"""Population-level incident detection. C5: caller-ID health.

Lifecycle (persisted in ``incidents``; every transition in ``incident_log``):

    detected ──> mitigated   (caller ID no longer allocated in the live config,
                              by an applied controller proposal OR a supervisor)

``detected`` carries a mitigation PROPOSAL (full config with the caller ID's
weight moved to the remaining healthy IDs). Submitting it through approval or
auto-apply is governance (M5); this module never changes a config.

Detection rule (all must hold; one bad call or a small sample never triggers):
* Window: the last ``window_hours`` hourly buckets that HAD calls for the
  workflow (overnight / outside the dialling window is skipped, not counted).
* Comparison: the caller ID vs all OTHER caller IDs of the same workflow in
  the same window. Caller ID is sampled independently per dial, so peers reach
  the same people at the same hours: a randomised comparison, not confounded
  by time of day or audience.
* Sample: >= ``min_dials`` in the window for the caller ID AND for its peers.
* Strength: one-sided pooled z <= -``z_threshold`` (default 4: p ~ 3e-5 per
  check, so ~24 checks/day/ID give a negligible false-alarm rate).
* Size: relative pickup drop >= ``min_relative_drop`` (default 30%).
* Persistence: ``sustain_checks`` consecutive hourly checks with enough data
  all breach. A check without enough data is "unknown": it neither confirms
  nor clears the breach (unknown is not healthy).

A workflow with a single caller ID has no peers; it is compared with its own
history window instead (weaker: not concurrent), using the same thresholds.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from controller import allocations as alloc
from controller.database import get_state, set_state, transaction
from controller.metrics import Rate, kpis
from controller.stats import compare
from controller.workflows import WorkflowRegistry

log = logging.getLogger(__name__)

KIND_CALLER_ID = "caller_id_health"
DETECTED, MITIGATED, ESCALATED = "detected", "mitigated", "escalated"
OPEN = (DETECTED, ESCALATED)    # escalated: a supervisor kept the caller ID; human decides


@dataclass(frozen=True)
class CallerIdPolicy:
    window_hours: float = 6.0
    min_dials: int = 300
    z_threshold: float = 4.0
    min_relative_drop: float = 0.30
    sustain_checks: int = 3


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def open_caller_id_incidents(conn: sqlite3.Connection, workflow_id: str) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT subject FROM incidents WHERE workflow_id=? AND kind=? AND status IN (?,?)",
        (workflow_id, KIND_CALLER_ID, *OPEN))}


def without_caller_ids(config: dict, bad: set[str], allowed: list[str]) -> tuple[dict, dict]:
    """Config with every cohort's caller_id weight on ``bad`` moved to healthy IDs.

    Weight goes to the cohort's remaining allocated IDs (renormalised); if none
    remain, to the other ALLOWED options uniformly. Returns (config, shifts)
    where shifts maps cohort -> total-variation distance of the change, the
    number governance's max_shift check needs. Unchanged cohorts are omitted.
    """
    new = dict(config)
    shifts: dict[str, float] = {}
    cohorts = set(alloc._by_cohort(config)) | {"default"}
    for cohort in sorted(cohorts):
        explicit = (alloc._by_cohort(new).get(cohort) or {}).get("caller_id")
        if explicit is None and cohort != "default":
            continue                       # inherits default, fixed there
        cur = alloc.distribution(new, cohort, "caller_id")
        if not cur or not (set(cur) & bad):
            continue
        healthy = {k: v for k, v in cur.items() if k not in bad}
        if not healthy:
            spare = [o for o in allowed if o not in bad]
            if not spare:
                raise ValueError(f"no healthy caller ID left for cohort {cohort}")
            healthy = {o: 1.0 for o in spare}
        total = sum(healthy.values())
        dist = {k: v / total for k, v in healthy.items()}
        shifts[cohort] = round(alloc.tv_distance(cur, dist), 4)
        new = alloc.with_distribution(new, cohort, "caller_id", dist)
    return new, shifts


class CallerIdMonitor:
    def __init__(self, conn: sqlite3.Connection, registry: WorkflowRegistry,
                 policy: CallerIdPolicy = CallerIdPolicy()) -> None:
        self.conn = conn
        self.registry = registry
        self.policy = policy

    def tick(self) -> list[tuple[str, str]]:
        """Check every workflow once. Returns (incident_id, new_status) changes."""
        changes: list[tuple[str, str]] = []
        for wf in self.registry.ids():
            data_now = self.conn.execute("SELECT MAX(ts) FROM calls WHERE workflow_id=?",
                                         (wf,)).fetchone()[0]
            sim_now = get_state(self.conn, "sim_time")
            # Records are stamped with SIMULATED time; the data clock (latest
            # call) only positions the window, and it stands still overnight.
            stamp = sim_now if sim_now not in (None, "None") else data_now
            if stamp is None:
                continue
            changes += self._check_mitigated(wf, stamp)
            if data_now:
                changes += self._detect(wf, data_now, stamp)
        return changes

    # -- detection -------------------------------------------------------------

    def evaluate(self, wf: str, now: str) -> dict[str, dict]:
        """Per caller ID: window counts, comparison and whether it breaches."""
        end = datetime.fromisoformat(now)
        hours = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT hour FROM metric_aggregates WHERE workflow_id=? AND hour<=? "
            "ORDER BY hour DESC LIMIT ?", (wf, now[:13], int(self.policy.window_hours)))]
        if not hours:
            return {}
        start = f"{hours[-1]}:00:00{now[19:]}"
        # metric_aggregates are hourly; [start, now] at hour granularity.
        rows = {r.key[0]: r for r in kpis(self.conn, wf, by=("caller_id",), start=start,
                                          end=_iso(end + timedelta(hours=1)))
                if r.key[0]}
        out: dict[str, dict] = {}
        for cid, r in rows.items():
            mine = r.pickup
            peers = Rate(sum(o.connected for c, o in rows.items() if c != cid),
                         sum(o.dials for c, o in rows.items() if c != cid))
            reference = "peers"
            if not peers.den:
                peers, reference = self._own_history(wf, cid), "own_history"
            result = {"window_start": start, "window_end": now, "window_hours": len(hours),
                      "reference": reference, "caller_id": mine.as_dict(),
                      "reference_rate": peers.as_dict(), "breach": False, "enough_data": False}
            if mine.den >= self.policy.min_dials and peers.den >= self.policy.min_dials:
                c = compare(peers, mine)
                drop = 1 - mine.value / peers.value if peers.value else 0.0
                result.update(z=round(c.z, 3), relative_drop=round(drop, 4), enough_data=True,
                              breach=c.z <= -self.policy.z_threshold
                              and drop >= self.policy.min_relative_drop)
            out[cid] = result
        return out

    def _own_history(self, wf: str, cid: str) -> Rate:
        end = self.conn.execute("SELECT MAX(window_end) FROM baselines WHERE workflow_id=? "
                                "AND label='history'", (wf,)).fetchone()[0]
        if end is None:
            return Rate(0, 0)
        rows = kpis(self.conn, wf, end=end, filters={"caller_id": cid})
        return rows[0].pickup if rows else Rate(0, 0)

    def _detect(self, wf: str, now: str, stamp: str) -> list[tuple[str, str]]:
        changes = []
        open_ids = open_caller_id_incidents(self.conn, wf)
        live = self._allocated(wf)
        for cid, ev in sorted(self.evaluate(wf, now).items()):
            key = f"cid_breach:{wf}:{cid}"
            state = json.loads(get_state(self.conn, key) or "{}")
            if cid not in live:
                # Not dialled any more: nothing to protect, and its old calls must
                # not reopen an incident after mitigation.
                if state:
                    with transaction(self.conn):
                        set_state(self.conn, key, "")
                continue
            if not ev["enough_data"]:
                continue                                  # unknown: keep state as is
            if not ev["breach"]:
                if state:
                    log.info("caller-ID check %s %s: breach cleared (z=%s, drop=%s)",
                             wf, cid, ev.get("z"), ev.get("relative_drop"))
                    with transaction(self.conn):
                        set_state(self.conn, key, "")
                continue
            if cid in open_ids or state.get("last") == now[:13]:
                continue                                  # one check per data hour
            state = {"since": state.get("since", now), "checks": state.get("checks", 0) + 1,
                     "last": now[:13]}
            with transaction(self.conn):
                set_state(self.conn, key, json.dumps(state))
            if state["checks"] == 1:
                log.info("caller-ID check %s %s: breach started, pickup %s vs %s %s "
                         "(z=%s, drop=%s); confirming over %d checks", wf, cid,
                         ev["caller_id"]["rate"], ev["reference"],
                         ev["reference_rate"]["rate"], ev["z"], ev["relative_drop"],
                         self.policy.sustain_checks)
            if state["checks"] >= self.policy.sustain_checks:
                changes.append(self._open(wf, cid, stamp, state["since"], ev))
                with transaction(self.conn):
                    set_state(self.conn, key, "")
        return changes

    def _open(self, wf: str, cid: str, now: str, since: str, ev: dict) -> tuple[str, str]:
        meta = self.registry.get(wf).meta
        cfg = self.registry.config(wf) or {}
        bad = open_caller_id_incidents(self.conn, wf) | {cid}
        try:
            proposed, shifts = without_caller_ids(cfg, bad, alloc.options(meta, "caller_id"))
            mitigation = {"action": "remove caller ID from allocation",
                          "base_version": self.registry.current_version(wf),
                          "tv_shift_by_cohort": shifts, "proposed_config": proposed,
                          "status": "awaiting governance"}
        except ValueError as e:
            mitigation = {"action": "none possible", "reason": str(e),
                          "status": "needs human decision"}
        inc_id = f"inc:{KIND_CALLER_ID}:{wf}:{cid}:{since}"
        evidence = {**ev, "breach_since": since, "policy": self.policy.__dict__}
        with transaction(self.conn):
            self.conn.execute(
                "INSERT OR IGNORE INTO incidents(incident_id, kind, workflow_id, subject, "
                "status, detected_ts, evidence, mitigation) VALUES (?,?,?,?,?,?,?,?)",
                (inc_id, KIND_CALLER_ID, wf, cid, DETECTED, now, json.dumps(evidence),
                 json.dumps(mitigation)))
            self._log(inc_id, now, None, DETECTED, {"evidence": evidence,
                                                    "mitigation": {k: v for k, v in
                                                                   mitigation.items()
                                                                   if k != "proposed_config"}})
        log.warning("CALLER-ID INCIDENT %s: %s %s pickup %s (n=%s) vs %s %s (n=%s), z=%s, "
                    "drop %.0f%%, sustained since %s. Mitigation: %s %s; %s",
                    inc_id, wf, cid, ev["caller_id"]["rate"], ev["caller_id"]["den"],
                    ev["reference"], ev["reference_rate"]["rate"], ev["reference_rate"]["den"],
                    ev["z"], 100 * ev["relative_drop"], since, mitigation["action"],
                    mitigation.get("tv_shift_by_cohort", ""), mitigation["status"])
        return inc_id, DETECTED

    def _allocated(self, wf: str) -> set[str]:
        cfg = self.registry.config(wf) or {}
        cohorts = set(alloc._by_cohort(cfg)) | {"default"}
        live = set()
        for c in cohorts:
            live |= {k for k, v in alloc.distribution(cfg, c, "caller_id").items() if v > 0}
        return live

    # -- mitigation check ----------------------------------------------------

    def _check_mitigated(self, wf: str, now: str) -> list[tuple[str, str]]:
        changes = []
        cfg = self.registry.config(wf)
        if cfg is None:
            return changes
        version = self.registry.current_version(wf)
        for inc_id, cid, status in self.conn.execute(
                "SELECT incident_id, subject, status FROM incidents WHERE workflow_id=? "
                "AND kind=? AND status IN (?,?)", (wf, KIND_CALLER_ID, *OPEN)).fetchall():
            cohorts = set(alloc._by_cohort(cfg)) | {"default"}
            if any(alloc.distribution(cfg, c, "caller_id").get(cid, 0) > 0 for c in cohorts):
                continue
            actor = self.conn.execute(
                "SELECT actor FROM config_changes WHERE workflow_id=? AND new_version=? "
                "ORDER BY event_id DESC LIMIT 1", (wf, version)).fetchone()
            detail = {"config_version": version, "by": actor[0] if actor else None}
            with transaction(self.conn):
                self.conn.execute(
                    "UPDATE incidents SET status=?, mitigated_ts=?, updated_at="
                    "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE incident_id=?",
                    (MITIGATED, now, inc_id))
                self._log(inc_id, now, status, MITIGATED, detail)
            log.warning("CALLER-ID INCIDENT %s mitigated: %s no longer allocated in %s v%s "
                        "(changed by %s)", inc_id, cid, wf, version, detail["by"])
            changes.append((inc_id, MITIGATED))
        return changes

    def _log(self, inc_id: str, now: str, frm: str | None, to: str, detail: dict) -> None:
        self.conn.execute(
            "INSERT INTO incident_log(incident_id, sim_ts, from_status, to_status, detail) "
            "VALUES (?,?,?,?,?)", (inc_id, now, frm, to, json.dumps(detail)))
