"""Workflow registry: metadata, objective, and immutable config snapshots.

Everything here is discovered over HTTP; no workflow IDs, KPIs or policy
values are assumed. Config snapshots are immutable per (workflow, version):
the first copy stored wins, and a later fetch that disagrees is logged as a
conflict rather than overwriting history.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Literal

from controller.client import SimulatorClient
from controller.cohorts import CohortSet, config_version
from controller.database import get_state, set_state, transaction

log = logging.getLogger(__name__)

Kpi = Literal["pickup", "engagement"]
KPIS: tuple[Kpi, ...] = ("pickup", "engagement")


# Top-level keys that make a dict actual config content (vs a change record).
CONFIG_CONTENT_KEYS = ("cohorts", "allocations", "prompt_version", "schedule")


def history_config(item: Any) -> dict | None:
    """Config content inside a /config/history entry, or None.

    Observed entries are change records only:
      {"version": 1, "actor": "business_default", "applied_at": null,
       "changed_dims": [], "proposal_id": null, "approval_id": null}
    Such a record must never be stored as the config of that version.
    """
    if not isinstance(item, dict):
        return None
    inner = item.get("config")
    if isinstance(inner, dict):
        cfg = inner
    elif any(k in item for k in CONFIG_CONTENT_KEYS):
        cfg = item
    else:
        return None
    if config_version(cfg) is None and config_version(item) is not None:
        cfg = {**cfg, "version": config_version(item)}
    return cfg


def _canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"))


def parse_kpi(v: Any) -> Kpi | None:
    s = str(v or "").lower()
    if "pickup" in s or "connect" in s:
        return "pickup"
    if "engage" in s:
        return "engagement"
    return None


@dataclass(frozen=True)
class Objective:
    """Primary KPI to optimise and the guardrail KPI that must not degrade.

    ``guardrail_max_drop`` is in absolute rate units (0.02 = 2pp) unless
    ``guardrail_relative`` is set, in which case it's a fraction of baseline.
    None means the workflow didn't state a tolerance; callers must then treat
    any statistically significant degradation as a breach.
    """

    primary: Kpi
    guardrail: Kpi
    guardrail_max_drop: float | None
    guardrail_relative: bool
    raw: Any

    @classmethod
    def from_meta(cls, meta: dict) -> "Objective":
        obj = meta.get("objective") or {}
        if not isinstance(obj, dict):
            obj = {"primary": obj}
        primary = parse_kpi(obj.get("primary") or obj.get("kpi") or obj.get("metric"))
        if primary is None:
            raise ValueError(f"workflow {meta.get('workflow_id')}: unknown primary KPI in "
                             f"{obj!r}")
        g = obj.get("guardrail") or {}
        if not isinstance(g, dict):
            g = {"kpi": g}
        guardrail = parse_kpi(g.get("kpi") or g.get("metric")) or \
            ("engagement" if primary == "pickup" else "pickup")
        if guardrail == primary:
            raise ValueError(f"guardrail KPI equals primary ({primary}); refusing")
        drop, relative = None, False
        for key, val in g.items():
            if key in ("kpi", "metric") or not isinstance(val, (int, float)):
                continue
            k = key.lower()
            if any(w in k for w in ("drop", "degrad", "toler", "delta", "max")):
                drop, relative = float(val), "rel" in k or "pct" in k or "percent" in k
                if drop > 1:  # given in percentage points / percent
                    drop /= 100
                break
        return cls(primary, guardrail, drop, relative, obj)


@dataclass(frozen=True)
class Workflow:
    workflow_id: str
    tenant_id: str | None
    objective: Objective
    mid_flight: str | None
    meta: dict


class WorkflowRegistry:
    def __init__(self, conn: sqlite3.Connection, client: SimulatorClient) -> None:
        self.conn = conn
        self.client = client
        self.last_changes: list[dict] = []   # config_changed rows seen by the last refresh

    # ---------------------------------------------------------------- sync

    def sync_all(self) -> list[str]:
        ids = []
        for wf in self.client.list_workflows():
            wf_id = wf.get("workflow_id") or wf.get("id")
            if wf_id:
                self.refresh(wf_id)
                ids.append(wf_id)
        return ids

    def refresh(self, workflow_id: str) -> int | None:
        """Re-fetch metadata, current config and config history. Returns current version."""
        meta = self.client.get_workflow(workflow_id)
        current = self.client.get_config(workflow_id)
        try:
            history = self.client.get_config_history(workflow_id)
        except Exception as e:  # history is best-effort; current config is required
            log.warning("config history for %s unavailable: %s", workflow_id, e)
            history = []
        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO workflows(workflow_id, tenant_id, metadata) VALUES (?,?,?) "
                "ON CONFLICT(workflow_id) DO UPDATE SET tenant_id=excluded.tenant_id, "
                "metadata=excluded.metadata, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (workflow_id, meta.get("tenant_id"), _canon(meta)))
            # GET /config is the authoritative content for the current version:
            # snapshot it first so a history entry can never claim that version.
            self._snapshot(workflow_id, current, None, None, "api_current")
            for item in history:
                if not isinstance(item, dict):
                    continue
                actor = item.get("actor")
                ts = item.get("applied_at") or item.get("ts") or item.get("effective_at")
                cfg = history_config(item)
                if cfg is not None:
                    self._snapshot(workflow_id, cfg, actor, ts, "api_history")
                # Change-record fields only annotate an existing snapshot.
                hv = config_version(item) if config_version(item) is not None else (
                    config_version(cfg) if cfg else None)
                if hv is not None:
                    self.conn.execute(
                        "UPDATE workflow_config_snapshots SET actor = COALESCE(actor, ?), "
                        "effective_ts = COALESCE(effective_ts, ?) "
                        "WHERE workflow_id = ? AND version = ?",
                        (actor, ts, workflow_id, hv))
            v = config_version(current)
            set_state(self.conn, f"config_version:{workflow_id}", v)
        return v

    def refresh_changed(self) -> list[str]:
        """Refresh workflows that have new ``config_changed`` events since last sync."""
        last = int(get_state(self.conn, "config_sync_event_id", "0") or 0)
        changes = [dict(r) for r in self.conn.execute(
            "SELECT event_id, ts, workflow_id, new_version, actor, changed_dims, proposal_id, "
            "approval_id FROM config_changes WHERE event_id > ? ORDER BY event_id", (last,))]
        self.last_changes = changes
        for c in changes:
            if c["actor"] == "supervisor":
                log.warning("supervisor edit: %s v%s changed %s at %s (event %d); accepted as "
                            "authoritative, not reverted", c["workflow_id"], c["new_version"],
                            c["changed_dims"], c["ts"], c["event_id"])
            else:
                log.info("config change: %s v%s by %s changed %s (proposal %s, approval %s)",
                         c["workflow_id"], c["new_version"], c["actor"], c["changed_dims"],
                         c["proposal_id"], c["approval_id"])
        changed = sorted({c["workflow_id"] for c in changes})
        for wf_id in changed:
            self.refresh(wf_id)
        if changes:
            with transaction(self.conn):
                set_state(self.conn, "config_sync_event_id", changes[-1]["event_id"])
        return changed

    def _snapshot(self, workflow_id: str, cfg: dict, actor: str | None,
                  ts: str | None, source: str) -> None:
        v = config_version(cfg)
        if v is None:
            log.warning("config for %s has no version; not snapshotted", workflow_id)
            return
        body = _canon(cfg)
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO workflow_config_snapshots"
            "(workflow_id, version, actor, effective_ts, source, config) VALUES (?,?,?,?,?,?)",
            (workflow_id, v, actor, ts, source, body))
        if cur.rowcount == 0:
            existing = self.conn.execute(
                "SELECT config FROM workflow_config_snapshots WHERE workflow_id=? AND version=?",
                (workflow_id, v)).fetchone()[0]
            if existing != body:
                old = json.loads(existing)
                keys = sorted(k for k in set(old) | set(cfg) if old.get(k) != cfg.get(k))
                log.error("config %s v%s from %s differs from the stored snapshot in keys %s; "
                          "keeping first copy", workflow_id, v, source, keys)
            return
        try:
            cs = CohortSet.from_config(workflow_id, cfg)
        except ValueError as e:
            log.error("config %s v%s has invalid cohorts: %s", workflow_id, v, e)
            return
        self.conn.executemany(
            "INSERT OR IGNORE INTO cohort_snapshots(workflow_id, version, cohort_id, position, "
            "where_json) VALUES (?,?,?,?,?)",
            [(workflow_id, v, c.cohort_id, c.position, _canon(c.to_dict()["where"]))
             for c in cs.cohorts])

    # ---------------------------------------------------------------- reads

    def ids(self) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT workflow_id FROM workflows ORDER BY workflow_id")]

    def get(self, workflow_id: str) -> Workflow:
        row = self.conn.execute("SELECT tenant_id, metadata FROM workflows WHERE workflow_id=?",
                                (workflow_id,)).fetchone()
        if row is None:
            raise KeyError(workflow_id)
        meta = json.loads(row["metadata"])
        return Workflow(workflow_id, row["tenant_id"], Objective.from_meta(meta),
                        meta.get("mid_flight"), meta)

    def current_version(self, workflow_id: str) -> int | None:
        v = get_state(self.conn, f"config_version:{workflow_id}")
        return int(v) if v not in (None, "None") else None

    def config(self, workflow_id: str, version: int | None = None) -> dict | None:
        version = self.current_version(workflow_id) if version is None else version
        row = self.conn.execute(
            "SELECT config FROM workflow_config_snapshots WHERE workflow_id=? AND version=?",
            (workflow_id, version)).fetchone()
        return json.loads(row[0]) if row else None

    def cohort_set(self, workflow_id: str, version: int | None = None) -> CohortSet | None:
        cfg = self.config(workflow_id, version)
        return CohortSet.from_config(workflow_id, cfg) if cfg is not None else None
