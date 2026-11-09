"""Governance (C9): the ONLY code path that changes a workflow config.

Each tick, per workflow:
1. Reconcile in-flight requests (GET /approvals/{id}); apply approved ones
   (POST /workflows/{id}/apply) within their window.
2. Collect intents: incident mitigation > rollback > promotion > experiment
   go-live (priority order; one intent per (cohort, dimension)).
3. Drop intents a SUPERVISOR has since edited (supervisor edits are
   authoritative even on auto-apply-eligible dimensions).
4. Build ONE batched proposal from the CURRENT config (fresh GET), never from
   an old full config, so nothing a supervisor changed is reverted.
5. Validate: options only, flagged caller IDs excluded, prompt changes need
   validation evidence >= min_validation_pass_rate.
6. Route: PUT auto-apply only if the live policy allows every changed dim and
   every shift <= max_shift; otherwise POST /approvals when the timing rules
   (approver hours vs TTL and latency, request limits, operator preferences)
   allow; otherwise wait and log why.

Proposal lifecycle (``proposals.status``):

    pending ──> approved ──> applied
       │            ├──────> lapsed   (apply window missed)  -> re-propose, NEW id
       │            └──────> stale    (base changed: 409)    -> re-propose, NEW id
       ├──────> rejected                                    -> intent dropped
       └──────> expired  (distinct from rejected)            -> re-propose, NEW id
    auto_applied | failed

Every transition writes ``audit_log`` and a ``GOVERNANCE`` log line.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from controller import allocations as alloc
from controller import experiments as ex
from controller import impact
from controller.client import (
    Conflict,
    Lapsed,
    SimulatorClient,
    SimulatorError,
    StaleBaseVersion,
)
from controller.cohorts import config_version
from controller.database import get_state, set_state, transaction
from controller.incidents import (
    DETECTED,
    ESCALATED,
    KIND_CALLER_ID,
    open_caller_id_incidents,
    without_caller_ids,
)
from controller.policy import GovernancePolicy, load_preferences
from controller.workflows import WorkflowRegistry

log = logging.getLogger(__name__)

IN_FLIGHT = ("pending", "approved")
RETRYABLE = ("expired", "lapsed", "stale", "failed")
MAX_ATTEMPTS = 3


@dataclass
class Intent:
    kind: str  # incident_mitigation | prompt_fix | rollback | promotion | experiment_golive
    ref: str                      # incident / experiment id
    workflow_id: str
    priority: int                 # lower = more important
    since: str                    # sim time the intent arose (supervisor-edit check)
    targets: list[tuple[str, str]]   # (cohort, dim) it changes
    transform: Callable[[dict], dict]
    summary: str
    rationale: str
    expected_pp: float            # expected primary/safety KPI change, percentage points
    confidence: float
    evidence: dict = field(default_factory=dict)


class GovernanceManager:
    def __init__(self, conn: sqlite3.Connection, client: SimulatorClient,
                 registry: WorkflowRegistry, preferences: dict | None = None) -> None:
        self.conn = conn
        self.client = client
        self.registry = registry
        self.preferences = load_preferences() if preferences is None else preferences

    # ---------------------------------------------------------------- helpers

    def now(self) -> str | None:
        v = get_state(self.conn, "sim_time")
        return None if v in (None, "None") else v

    def policy(self, wf: str) -> GovernancePolicy:
        return GovernancePolicy.from_meta(self.registry.get(wf).meta, self.preferences)

    def audit(self, wf: str | None, action: str, ref: str | None, detail: dict,
              now: str | None = None) -> None:
        self.conn.execute("INSERT INTO audit_log(sim_ts, workflow_id, action, ref, detail) "
                          "VALUES (?,?,?,?,?)",
                          (now or self.now(), wf, action, ref, json.dumps(detail, default=str)))

    def log_policies(self) -> None:
        for wf in self.registry.ids():
            p = self.policy(wf)
            obj = self.registry.get(wf).objective
            log.info("GOVERNANCE policy %s: primary=%s guardrail=%s max_drop=%s%s mid_flight=%s "
                     "%s", wf, obj.primary, obj.guardrail, obj.guardrail_max_drop,
                     " (relative)" if obj.guardrail_relative else " (absolute)",
                     self.registry.get(wf).mid_flight, json.dumps(p.describe()))

    # ---------------------------------------------------------------- tick

    def tick(self) -> list[tuple[str, str]]:
        now = self.now()
        if now is None:
            return []
        changes = self.reconcile(now)
        for wf in self.registry.ids():
            if self._in_flight(wf):
                continue
            new = self._propose(wf, now)
            if new:
                changes.append(new)
        return changes

    def _in_flight(self, wf: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM proposals WHERE workflow_id=? AND status IN (?,?)",
            (wf, *IN_FLIGHT)).fetchone() is not None

    # ---------------------------------------------------------------- reconcile

    def reconcile(self, now: str) -> list[tuple[str, str]]:
        changes = []
        rows = self.conn.execute(
            "SELECT * FROM proposals WHERE status IN (?,?) AND route='approval' "
            "ORDER BY submitted_ts", IN_FLIGHT).fetchall()
        for p in rows:
            try:
                remote = self.client.get_approval(p["approval_id"])
            except SimulatorError as e:
                log.warning("GOVERNANCE %s: approval %s unreadable (%s); will retry",
                            p["proposal_id"], p["approval_id"], e)
                continue
            status = str(remote.get("status", "pending")).lower()
            apply_by = remote.get("apply_by") or p["apply_by"]
            if status != p["status"] and status in ("approved", "rejected", "expired",
                                                     "lapsed", "applied"):
                self._set_status(p, status, now, reasons=remote.get("reasons")
                                 or remote.get("reason"), apply_by=apply_by,
                                 approver=remote.get("decided_by") or remote.get("approver"))
                changes.append((p["proposal_id"], status))
                if status == "approved":
                    changes += self._apply(dict(p) | {"apply_by": apply_by,
                                                      "status": "approved"}, now)
                else:
                    self._on_terminal(p, status, now)
            elif p["status"] == "approved":
                changes += self._apply(dict(p), now)       # approved last tick, apply failed
        return changes

    def _apply(self, p: dict, now: str) -> list[tuple[str, str]]:
        wf = p["workflow_id"]
        try:
            resp = self.client.apply_approval(wf, p["approval_id"])
        except StaleBaseVersion as e:
            self._set_status(p, "stale", now, reasons=str(e))
            self.registry.refresh(wf)
            log.warning("GOVERNANCE %s stale: %s changed since v%s (now v%s); will re-evaluate "
                        "and re-propose under a NEW id", p["proposal_id"], wf, p["base_version"],
                        self.registry.current_version(wf))
            return [(p["proposal_id"], "stale")]
        except Lapsed as e:
            self._set_status(p, "lapsed", now, reasons=str(e))
            return [(p["proposal_id"], "lapsed")]
        except Conflict as e:
            self._set_status(p, "failed", now, reasons=str(e))
            return [(p["proposal_id"], "failed")]
        except SimulatorError as e:
            log.warning("GOVERNANCE apply %s failed (%s); will retry next tick",
                        p["proposal_id"], e)
            return []
        version = config_version(resp) if isinstance(resp, dict) else None
        self.registry.refresh(wf)
        version = version or self.registry.current_version(wf)
        self._set_status(p, "applied", now, applied_version=version)
        self._on_applied(p, version, now)
        return [(p["proposal_id"], "applied")]

    def _set_status(self, p, status: str, now: str, *, reasons=None, apply_by=None,
                    approver=None, applied_version=None) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE proposals SET status=?, decided_ts=COALESCE(decided_ts, ?), "
                "reasons=COALESCE(?, reasons), apply_by=COALESCE(?, apply_by), "
                "applied_version=COALESCE(?, applied_version), updated_at="
                "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE proposal_id=?",
                (status, now, json.dumps(reasons) if reasons is not None else None, apply_by,
                 applied_version, p["proposal_id"]))
            self.audit(p["workflow_id"], f"proposal_{status}", p["proposal_id"],
                       {"from": p["status"], "reasons": reasons, "approver": approver,
                        "apply_by": apply_by, "applied_version": applied_version,
                        "approval_id": p["approval_id"]}, now)
        log.warning("GOVERNANCE %s %s -> %s%s%s", p["proposal_id"], p["status"], status,
                    f" by {approver}" if approver else "",
                    f" reasons={reasons}" if reasons else "")

    # ---------------------------------------------------------------- outcomes

    def _intents_of(self, p) -> list[dict]:
        return json.loads(p["intents"])

    def _on_applied(self, p, version: int | None, now: str) -> None:
        store = ex.ExperimentStore(self.conn)
        for it in self._intents_of(p):
            if it["kind"] == "promotion":
                with transaction(self.conn):
                    store.transition(it["ref"], ex.PROMOTED, now, {"version": version,
                                                                   "proposal": p["proposal_id"]},
                                     promoted_ts=now, promoted_version=version)
            elif it["kind"] == "rollback":
                with transaction(self.conn):
                    store.transition(it["ref"], ex.ROLLED_BACK, now,
                                     {"version": version, "proposal": p["proposal_id"]})
            # experiment_golive -> RUNNING and incident -> mitigated are detected
            # from the live config by their own monitors.

    def _on_terminal(self, p, status: str, now: str) -> None:
        if status != "rejected":
            return                                   # expired/lapsed: re-proposed later
        store = ex.ExperimentStore(self.conn)
        for it in self._intents_of(p):
            if it["kind"] == "prompt_fix":
                with transaction(self.conn):
                    self._escalate_incident(it["ref"], now, "prompt fix rejected by approver")
                log.warning("GOVERNANCE prompt fix for %s rejected by approver; incident "
                            "escalated to a human", it["ref"])
            if it["kind"] == "experiment_golive":
                exp = store.get(it["ref"])
                if exp is not None and exp["status"] == ex.PLANNED:
                    with transaction(self.conn):
                        store.transition(it["ref"], ex.CANCELLED, now,
                                         {"reason": "approval rejected",
                                          "proposal": p["proposal_id"]})

    # ---------------------------------------------------------------- intents

    def intents(self, wf: str, now: str) -> list[Intent]:
        out = [*self._incident_intents(wf), *self._prompt_intents(wf),
               *self._experiment_intents(wf)]
        # Exclude intents whose attempts are used up or that were rejected.
        keep = []
        for it in out:
            prior = [r[0] for r in self.conn.execute(
                "SELECT status FROM proposals WHERE workflow_id=? AND intents LIKE ?",
                (wf, f'%"ref": "{it.ref}"%'))]
            if "rejected" in prior and it.kind != "incident_mitigation":
                continue
            if sum(s in RETRYABLE or s == "rejected" for s in prior) >= MAX_ATTEMPTS:
                self._escalate_once(wf, it, now, f"{MAX_ATTEMPTS} attempts without success")
                continue
            keep.append(it)
        return sorted(keep, key=lambda i: i.priority)

    def _incident_intents(self, wf: str) -> list[Intent]:
        out = []
        meta = self.registry.get(wf).meta
        for inc in self.conn.execute(
                "SELECT * FROM incidents WHERE workflow_id=? AND kind=? AND status=?",
                (wf, KIND_CALLER_ID, DETECTED)).fetchall():
            cid = inc["subject"]
            ev = json.loads(inc["evidence"])
            drop_pp = 100 * ((ev.get("reference_rate") or {}).get("rate", 0)
                             - (ev.get("caller_id") or {}).get("rate", 0))
            out.append(Intent(
                "incident_mitigation", inc["incident_id"], wf, 0, inc["detected_ts"],
                [(c, "caller_id") for c in sorted(set(alloc._by_cohort(
                    self.registry.config(wf) or {})) | {"default"})],
                lambda cfg, cid=cid: without_caller_ids(
                    cfg, open_caller_id_incidents(self.conn, wf) | {cid},
                    alloc.options(meta, "caller_id"))[0],
                f"Remove caller ID {cid} (pickup incident) from {wf} allocations",
                f"{cid} pickup {ev.get('caller_id', {}).get('rate')} vs "
                f"{ev.get('reference')} {ev.get('reference_rate', {}).get('rate')} "
                f"(z={ev.get('z')}, drop {ev.get('relative_drop')}) sustained since "
                f"{ev.get('breach_since')}; incident {inc['incident_id']}",
                drop_pp, 0.9, {"incident": inc["incident_id"], "window": ev}))
        return out

    def _prompt_intents(self, wf: str) -> list[Intent]:
        from controller import prompt_incidents as pi
        out = []
        for inc in self.conn.execute(
                "SELECT * FROM incidents WHERE workflow_id=? AND kind=? AND status=?",
                (wf, pi.KIND, pi.FIX_PROPOSED)).fetchall():
            mit, ev = json.loads(inc["mitigation"]), json.loads(inc["evidence"])
            chosen = mit["chosen"]
            current = (self.registry.config(wf) or {}).get("prompt_version")
            sup = self.conn.execute(
                "SELECT ts FROM config_changes WHERE workflow_id=? AND actor='supervisor' AND "
                "new_version<=? AND changed_dims LIKE '%prompt_version%' ORDER BY event_id DESC "
                "LIMIT 1", (wf, self.registry.current_version(wf))).fetchone()
            note = (f" Replaces prompt {current} set by a SUPERVISOR at {sup[0]}; this needs "
                    f"your explicit approval and will not be applied otherwise." if sup else "")
            out.append(Intent(
                "prompt_fix", inc["incident_id"], wf, 0, inc["detected_ts"],
                [("*", "prompt_version")],
                lambda cfg, pv=chosen["prompt_version"]: {**cfg, "prompt_version": pv},
                f"Change prompt_version {current} -> {chosen['prompt_version']} "
                f"(hallucination incident)",
                f"Hallucination flag rate {ev['window']['rate']} vs history "
                f"{ev['baseline']['rate']} (+{100 * ev['abs_increase']:.1f}pp, z={ev['z']}). "
                f"{chosen['prompt_version']} passed validation run {chosen['validation_run_id']} "
                f"at {chosen['pass_rate']} (n={chosen['n']}, 95% lower bound "
                f"{chosen['lower_bound']}); engagement will be verified after apply.{note}",
                100 * ev["abs_increase"], 0.9,
                {"incident": inc["incident_id"],
                 "validation": {k: chosen[k] for k in ("validation_run_id", "pass_rate", "n",
                                                       "lower_bound", "prompt_version")},
                 "window": ev}))
        return out

    def _experiment_intents(self, wf: str) -> list[Intent]:
        out = []
        store = ex.ExperimentStore(self.conn)
        for e in self.conn.execute(
                "SELECT * FROM experiments WHERE workflow_id=? AND status IN (?,?,?)",
                (wf, ex.PLANNED, ex.DECIDED, ex.PROMOTED)).fetchall():
            cohort, dim = e["cohort_id"], e["dimension"]
            if e["status"] == ex.PLANNED:
                dist = json.loads(e["allocation"])
                out.append(Intent(
                    "experiment_golive", e["experiment_id"], wf, 3, e["created_ts"],
                    [(cohort, dim)],
                    lambda cfg, c=cohort, d=dim, x=dist: alloc.with_distribution(cfg, c, d, x),
                    f"Start experiment on {dim} in cohort {cohort}: {dist}",
                    f"Controlled test of {json.loads(e['candidates'])} vs control "
                    f"{e['control']} on {e['primary_kpi']}; design "
                    f"{json.dumps(json.loads(e['design']).get('sizing'))}",
                    0.0, 0.3, {"experiment": e["experiment_id"],
                               "design": json.loads(e["design"])}))
                continue
            decision = json.loads(e["decision"] or "{}")
            rec = decision.get("recommendation")
            if e["status"] == ex.DECIDED and decision.get("outcome") == "winner" and rec:
                test = decision["tests"][decision["winner"]]
                out.append(Intent(
                    "promotion", e["experiment_id"], wf, 2, e["decided_ts"], [(cohort, dim)],
                    lambda cfg, c=cohort, d=dim, x=rec["allocation"]:
                        alloc.with_distribution(cfg, c, d, x),
                    f"Promote {decision['winner']} on {dim}/{cohort} to "
                    f"{rec['allocation']} (control holdout kept)",
                    decision["reason"], 100 * test["primary"]["diff"], 0.9,
                    {"experiment": e["experiment_id"], "test": test}))
            elif e["status"] == ex.PROMOTED:
                harm = ex.promotion_harm(self.conn, self.registry, e)
                if harm:
                    store_dist = (json.loads(e["design"]).get("current_distribution")
                                  or {e["control"]: 1.0})
                    out.append(Intent(
                        "rollback", e["experiment_id"], wf, 1, e["promoted_ts"],
                        [(cohort, dim)],
                        lambda cfg, c=cohort, d=dim, x=store_dist:
                            alloc.with_distribution(cfg, c, d, x),
                        f"Roll back {dim}/{cohort} to pre-experiment {store_dist}",
                        harm["reason"], 100 * abs(harm["comparison"].get("diff", 0))
                        if harm.get("comparison") else 0.0, 0.5,
                        {"experiment": e["experiment_id"], "harm": harm}))
        return out

    def _supervisor_touched(self, wf: str, it: Intent) -> list[dict]:
        rows = self.conn.execute(
            "SELECT event_id, ts, new_version, changed_dims FROM config_changes "
            "WHERE workflow_id=? AND actor='supervisor' AND ts > ?", (wf, it.since)).fetchall()
        dims = {d for _, d in it.targets}
        extra = {"allocations", "cohorts"} if dims & set(alloc.ALLOCATION_DIMS) else set()
        hit = []
        for r in rows:
            changed = set(json.loads(r["changed_dims"] or "[]"))
            if changed & (dims | extra):
                hit.append(dict(r))
        return hit

    def _escalate_once(self, wf: str, it: Intent, now: str, why: str) -> None:
        key = f"escalated:{it.ref}"
        if get_state(self.conn, key):
            return
        with transaction(self.conn):
            set_state(self.conn, key, now)
            self.audit(wf, "intent_escalated", it.ref, {"kind": it.kind, "reason": why}, now)
            if it.kind in ("incident_mitigation", "prompt_fix"):
                self._escalate_incident(it.ref, now, why)
        log.warning("GOVERNANCE escalate %s %s: %s; needs a human decision", it.kind, it.ref, why)

    def _escalate_incident(self, inc_id: str, now: str, why: str) -> None:
        """Caller owns the transaction."""
        row = self.conn.execute("SELECT status FROM incidents WHERE incident_id=?",
                                (inc_id,)).fetchone()
        if row is None or row[0] in ("mitigated", "resolved", ESCALATED):
            return
        self.conn.execute("UPDATE incidents SET status=? WHERE incident_id=?", (ESCALATED, inc_id))
        self.conn.execute("INSERT INTO incident_log(incident_id, sim_ts, from_status, to_status, "
                          "detail) VALUES (?,?,?,?,?)",
                          (inc_id, now, row[0], ESCALATED, json.dumps({"reason": why})))

    # ---------------------------------------------------------------- propose

    def _propose(self, wf: str, now: str) -> tuple[str, str] | None:
        intents = self.intents(wf, now)
        if not intents:
            return None
        chosen, taken = [], set()
        for it in intents:
            touched = self._supervisor_touched(wf, it)
            if touched:
                self._escalate_once(wf, it, now, f"supervisor edited {sorted({d for _, d in it.targets})} "
                                    f"after this intent (events {[t['event_id'] for t in touched]}); "
                                    f"supervisor edits are authoritative")
                continue
            if taken & set(it.targets):
                continue                              # higher-priority intent owns this slot
            chosen.append(it)
            taken |= set(it.targets)
        if not chosen:
            return None

        policy = self.policy(wf)
        before = self.registry.current_version(wf)
        self.registry.refresh(wf)                     # exact current base
        base_version = self.registry.current_version(wf)
        if base_version != before and not self.conn.execute(
                "SELECT 1 FROM config_changes WHERE workflow_id=? AND new_version=?",
                (wf, base_version)).fetchone():
            # The config moved but we haven't ingested WHO moved it: never build on
            # a change whose author (possibly a supervisor) we can't check yet.
            self._defer(wf, now, f"config is now v{base_version} but its config_changed "
                        f"event is not ingested yet; waiting", chosen)
            return None
        current = self.registry.config(wf) or {}
        proposed = current
        for it in chosen:
            proposed = it.transform(proposed)
        proposed = ex._safe(self.conn, self.registry.get(wf), proposed)
        dims = impact.changed_dims(current, proposed)
        if not dims:
            return None
        evidence = {}
        for it in chosen:
            if "validation" in it.evidence:
                evidence["validation"] = it.evidence["validation"]
        problems = self.validate(wf, current, proposed, dims, policy, evidence)
        if problems:
            self._defer(wf, now, "invalid proposal: " + "; ".join(problems), chosen)
            return None

        auto_ok, auto_why = policy.auto_apply_check(current, proposed, dims)
        route = "auto" if auto_ok else "approval"
        if route == "approval":
            sent = self.conn.execute(
                "SELECT COUNT(*) FROM proposals WHERE workflow_id=? AND route='approval' AND "
                "submitted_ts > ?", (wf, (datetime.fromisoformat(now) - timedelta(hours=24))
                                     .isoformat(timespec="seconds"))).fetchone()[0]
            ok, why = policy.submission_check(datetime.fromisoformat(now), sent)
            if not ok:
                self._defer(wf, now, f"approval needed ({'; '.join(auto_why)}) but not "
                            f"sendable now: {why}", chosen)
                return None

        payload = self._payload(wf, now, base_version, current, proposed, dims, chosen,
                                policy, auto_why)
        return self._submit(wf, now, route, payload, chosen)

    def validate(self, wf: str, current: dict, proposed: dict, dims: list[str],
                 policy: GovernancePolicy, evidence: dict | None = None) -> list[str]:
        meta = self.registry.get(wf).meta
        problems = []
        for cohort, dim, _ in impact.allocation_shifts(current, proposed):
            allowed = set(alloc.options(meta, dim))
            bad = set(alloc.distribution(proposed, cohort, dim)) - allowed
            if bad:
                problems.append(f"{cohort}/{dim} uses values not in options: {sorted(bad)}")
        flagged = open_caller_id_incidents(self.conn, wf)
        for cohort in set(alloc._by_cohort(proposed)) | {"default"}:
            if set(alloc.distribution(proposed, cohort, "caller_id")) & flagged:
                problems.append(f"{cohort} still dials flagged caller IDs {sorted(flagged)}")
        if "prompt_version" in dims:
            opts = alloc.options(meta, "prompt_version") or alloc.options(meta, "prompt")
            if opts and proposed.get("prompt_version") not in opts:
                problems.append(f"prompt_version {proposed.get('prompt_version')} not in options")
            v = (evidence or {}).get("validation") or {}
            need = policy.min_validation_pass_rate
            if not v.get("validation_run_id"):
                problems.append("prompt change without validation evidence")
            elif need is not None and (v.get("pass_rate") or 0) < need:
                problems.append(f"validation pass rate {v.get('pass_rate')} < {need}")
        return problems

    def _payload(self, wf, now, base_version, current, proposed, dims, chosen, policy,
                 auto_why) -> dict:
        w = self.registry.get(wf)
        leads, basis = impact.affected_leads(self.conn, wf, now, w.mid_flight)
        share = impact.traffic_share_changed(current, proposed)
        lead = chosen[0]
        score = impact.impact_score(lead.expected_pp, share, lead.confidence)
        seq = self.conn.execute("SELECT COUNT(*) FROM proposals WHERE workflow_id=?",
                                (wf,)).fetchone()[0] + 1
        refs = "+".join(sorted(i.ref for i in chosen))
        pid = (f"prop-{wf}-{hashlib.sha1(refs.encode()).hexdigest()[:10]}"
               f"-v{base_version}-{seq}")
        return {
            "proposal_id": pid, "workflow_id": wf, "base_version": base_version,
            "proposed_config": proposed,
            "change_summary": " | ".join(i.summary for i in chosen),
            "diff": impact.diff(current, proposed),
            "affected_leads": int(leads),
            "dry_run": {
                "method": "replay of observed data in this workflow (no simulation)",
                "affected_leads_basis": basis,
                "traffic_share_changed": share,
                "mid_flight": w.mid_flight,
                "timing": ("applies to campaigns created after the change"
                           if w.mid_flight == "next_campaign_only"
                           else "applies to running campaigns from the next tick"),
                "objective": {"primary": w.objective.primary,
                              "guardrail": w.objective.guardrail,
                              "guardrail_max_drop": w.objective.guardrail_max_drop},
                "expected_primary_change_pp": round(lead.expected_pp, 3),
                "intents": [{"kind": i.kind, "ref": i.ref, "evidence": i.evidence}
                            for i in chosen],
                "impact_inputs": {"expected_pp": round(lead.expected_pp, 3), "share": share,
                                  "confidence": lead.confidence,
                                  "formula": "min(1,|pp|/5) * share * confidence"},
                "auto_apply_ineligible_because": auto_why,
            },
            "impact_score": score,
            "rationale": " | ".join(i.rationale for i in chosen),
            "evidence": {"intents": [i.ref for i in chosen],
                         **({"validation_run_id": v["validation_run_id"],
                             "validation_pass_rate": v["pass_rate"], "validation_n": v["n"]}
                            if (v := next((i.evidence["validation"] for i in chosen
                                           if "validation" in i.evidence), None)) else {})},
            "expires_in_hours": policy.proposal_ttl_hours,
        }

    def _submit(self, wf, now, route, payload, chosen) -> tuple[str, str] | None:
        pid = payload["proposal_id"]
        intents = json.dumps([{"kind": i.kind, "ref": i.ref} for i in chosen])
        approval_id, status, apply_by = None, None, None
        try:
            if route == "auto":
                resp = self.client.put_config(wf, payload)
                status = "auto_applied"
            else:
                try:
                    resp = self.client.submit_approval(payload)
                except Conflict as e:
                    if "stale" in (e.code or "").lower():
                        raise
                    resp = next((a for a in self.client.list_approvals(wf)
                                 if a.get("proposal_id") == pid), None)
                    if resp is None:
                        raise
                approval_id = resp.get("approval_id") or resp.get("id")
                status = str(resp.get("status", "pending")).lower()
                apply_by = resp.get("apply_by")
        except StaleBaseVersion as e:
            self.registry.refresh(wf)
            log.warning("GOVERNANCE %s not sent: base v%s already stale (%s)", pid,
                        payload["base_version"], e)
            return None
        except SimulatorError as e:
            if route == "auto" and (e.status or 0) < 500:
                log.warning("GOVERNANCE auto-apply of %s refused (%s); falling back to approval",
                            pid, e)
                self._defer(wf, now, f"auto-apply refused by simulator: {e}", chosen)
                return None
            log.error("GOVERNANCE %s submit failed: %s", pid, e)
            return None

        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO proposals(proposal_id, workflow_id, intents, route, base_version, "
                "payload, status, approval_id, apply_by, submitted_ts) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (pid, wf, intents, route, payload["base_version"], json.dumps(payload), status,
                 approval_id, apply_by, now))
            self.audit(wf, f"proposal_{'auto_applied' if route == 'auto' else 'submitted'}", pid,
                       {"route": route, "intents": json.loads(intents),
                        "base_version": payload["base_version"],
                        "change_summary": payload["change_summary"], "diff": payload["diff"],
                        "impact_score": payload["impact_score"],
                        "affected_leads": payload["affected_leads"],
                        "approval_id": approval_id}, now)
        log.warning("GOVERNANCE %s %s for %s: %s (impact %.3f, affected leads ~%d)", pid,
                    "AUTO-APPLIED" if route == "auto" else f"submitted for approval {approval_id}",
                    wf, payload["change_summary"], payload["impact_score"],
                    payload["affected_leads"])
        if route == "auto":
            self.registry.refresh(wf)
            p = self.conn.execute("SELECT * FROM proposals WHERE proposal_id=?", (pid,)).fetchone()
            self._on_applied(p, self.registry.current_version(wf), now)
        return pid, status

    def _defer(self, wf: str, now: str, why: str, chosen: list[Intent]) -> None:
        key = f"gov_deferred:{wf}"
        if get_state(self.conn, key) == why:
            return                                    # log each new reason once
        with transaction(self.conn):
            set_state(self.conn, key, why)
            self.audit(wf, "proposal_deferred", None,
                       {"reason": why, "intents": [i.ref for i in chosen]}, now)
        log.info("GOVERNANCE %s deferred %s: %s", wf, [i.kind for i in chosen], why)
