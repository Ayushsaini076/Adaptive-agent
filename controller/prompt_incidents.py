"""C8: hallucination / prompt incidents.

Lifecycle (``incidents`` rows with kind ``prompt_hallucination``; every step in
``incident_log`` and a ``PROMPT INCIDENT`` log line):

    detected ──> fix_proposed ──> fix_applied ──> resolved
       │              │                └───────> fix_failed ─> detected (next attempt)
       │              │                                     └> escalated (attempts used up)
       ├──> no_safe_fix (escalated: nothing passed validation / no credits)
       └──> superseded   (a supervisor changed the prompt; we don't override, we re-check)

Detection (one flagged call never triggers anything):
* Unit: connected calls with a QA verdict; signal: ``qa.hallucination_flag``.
* Window: the last ``window_hours`` hourly buckets that had QA'd calls.
* Reference: the workflow's own HISTORY flag rate. The QA detector has false
  positives and negatives; the history rate already contains its false-positive
  rate, so window vs history compares like with like, and false negatives only
  make the observed rise smaller (conservative).
* Trigger: the window has >= ``min_qa`` QA'd calls, one-sided z >= ``z_threshold``,
  absolute rise >= ``min_abs_increase`` and relative rise >= ``min_rel_increase``;
  AND the rise is SUSTAINED: each of the last ``sustain_checks`` hours on its own
  (>= ``min_qa_per_hour`` QA'd calls) is >= ``min_abs_increase`` above history.
  A single terrible hour inflates a 12h window for 12 hours, so overlapping
  window checks alone are not evidence of persistence; disjoint hours are.
* While an unresolved incident (no_safe_fix / escalated) exists for the prompt
  that is STILL live, no new incident is opened and no credits are re-spent:
  a human has to act. A different prompt can open a new incident.

Credits are spent only when the incident is confirmed, no fix is in flight,
no supervisor changed the prompt since detection, and the spend leaves the
reserve intact. Budget per attempt: suggest-fixes + up to ``max_validations``
validation runs of ``validation_n`` conversations.

Verification after the fix is live (both required):
* hallucination fixed: rate no longer meets the incident size (rise <
  ``min_abs_increase``) AND clearly below the incident window rate;
* engagement not harmed: not significantly below the history engagement
  baseline by >= the engagement tolerance (objective guardrail if engagement is
  the guardrail, else ``engagement_harm_pp``). A safer prompt that makes people
  hang up is a failed fix.
A failed fix moves to the next validated candidate, never back to the
hallucinating prompt.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import dataclass

from controller import allocations as alloc
from controller.client import InsufficientCredits, SimulatorClient, SimulatorError
from controller.database import get_state, set_state, transaction
from controller.metrics import Rate, Z95
from controller.stats import compare
from controller.workflows import WorkflowRegistry

log = logging.getLogger(__name__)

KIND = "prompt_hallucination"
DETECTED, FIX_PROPOSED, FIX_APPLIED, RESOLVED = (
    "detected", "fix_proposed", "fix_applied", "resolved")
FIX_FAILED, NO_SAFE_FIX, ESCALATED, SUPERSEDED = (
    "fix_failed", "no_safe_fix", "escalated", "superseded")
OPEN = (DETECTED, FIX_PROPOSED, FIX_APPLIED)


@dataclass(frozen=True)
class PromptPolicy:
    window_hours: int = 12
    min_qa: int = 300
    z_threshold: float = 4.0
    min_abs_increase: float = 0.02
    min_rel_increase: float = 0.5
    sustain_checks: int = 3
    min_qa_per_hour: int = 30
    validation_n: int = 300            # 3 credits; SE ~0.6pp at a 99% pass rate
    max_validations: int = 2
    suggest_cost: float = 3.0
    reserve_fraction: float = 0.2      # keep this share of the starting credits
    min_reserve: float = 10.0
    max_attempts: int = 2
    verify_min_qa: int = 300
    verify_min_connected: int = 300
    engagement_harm_pp: float = 0.02
    harm_z: float = 3.0

    @property
    def cost_per_attempt(self) -> float:
        return self.suggest_cost + self.max_validations * math.ceil(self.validation_n / 100)


def _rate_since(conn, wf, start_hour, prompt=None, end_hour=None):
    where, args = ["workflow_id = ?", "hour >= ?"], [wf, start_hour]
    if end_hour:
        where.append("hour < ?")
        args.append(end_hour)
    if prompt:
        where.append("prompt_version = ?")
        args.append(prompt)
    row = conn.execute(f"SELECT SUM(qa_n), SUM(hallucinations), SUM(connected), SUM(engaged) "
                       f"FROM metric_aggregates WHERE {' AND '.join(where)}", args).fetchone()
    qa, h, c, e = (int(x or 0) for x in row)
    return Rate(h, qa), Rate(e, c)


class HallucinationMonitor:
    def __init__(self, conn: sqlite3.Connection, client: SimulatorClient,
                 registry: WorkflowRegistry, policy: PromptPolicy = PromptPolicy()) -> None:
        self.conn = conn
        self.client = client
        self.registry = registry
        self.policy = policy

    # ---------------------------------------------------------------- helpers

    def _now(self, wf: str) -> tuple[str | None, str | None]:
        data = self.conn.execute("SELECT MAX(ts) FROM calls WHERE workflow_id=?",
                                 (wf,)).fetchone()[0]
        sim = get_state(self.conn, "sim_time")
        return data, (sim if sim not in (None, "None") else data)

    def baseline(self, wf: str) -> tuple[Rate, Rate] | None:
        end = self.conn.execute("SELECT MAX(window_end) FROM baselines WHERE workflow_id=? AND "
                                "label='history'", (wf,)).fetchone()[0]
        if end is None:
            return None
        return _rate_since(self.conn, wf, "0000", end_hour=end[:13])

    def _open(self, wf: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM incidents WHERE workflow_id=? AND kind=? AND status IN (?,?,?) "
            "ORDER BY detected_ts DESC LIMIT 1", (wf, KIND, *OPEN)).fetchone()

    def _transition(self, inc_id: str, frm: str, to: str, now: str, detail: dict,
                    **fields) -> None:
        sets = "".join(f", {k} = ?" for k in fields)
        with transaction(self.conn):
            self.conn.execute(f"UPDATE incidents SET status = ?{sets}, updated_at = "
                              f"strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE incident_id = ?",
                              (to, *fields.values(), inc_id))
            self.conn.execute("INSERT INTO incident_log(incident_id, sim_ts, from_status, "
                              "to_status, detail) VALUES (?,?,?,?,?)",
                              (inc_id, now, frm, to, json.dumps(detail, default=str)))
        level = logging.WARNING if to in (DETECTED, FIX_PROPOSED, FIX_FAILED, NO_SAFE_FIX,
                                          ESCALATED, SUPERSEDED, RESOLVED) else logging.INFO
        log.log(level, "PROMPT INCIDENT %s: %s -> %s %s", inc_id, frm, to,
                json.dumps(detail, default=str)[:400])

    # ---------------------------------------------------------------- tick

    def tick(self) -> list[tuple[str, str]]:
        changes = []
        for wf in self.registry.ids():
            data_now, now = self._now(wf)
            if now is None:
                continue
            inc = self._open(wf)
            if inc is None:
                if data_now:
                    new = self._detect(wf, data_now, now)
                    if new:
                        changes.append(new)
                continue
            step = {DETECTED: self._respond, FIX_PROPOSED: self._check_applied,
                    FIX_APPLIED: self._verify}[inc["status"]](wf, inc, now)
            if step:
                changes.append(step)
        return changes

    # ---------------------------------------------------------------- detection

    def evaluate(self, wf: str, data_now: str) -> dict | None:
        base = self.baseline(wf)
        if base is None:
            return None
        hours = [r[0] for r in self.conn.execute(
            "SELECT hour FROM metric_aggregates WHERE workflow_id=? AND hour<=? GROUP BY hour "
            "HAVING SUM(qa_n) > 0 ORDER BY hour DESC LIMIT ?",
            (wf, data_now[:13], self.policy.window_hours))]
        if not hours:
            return None
        window, _ = _rate_since(self.conn, wf, hours[-1])
        by_prompt = {r[0]: Rate(int(r[2] or 0), int(r[1] or 0)).as_dict() for r in self.conn.execute(
            "SELECT prompt_version, SUM(qa_n), SUM(hallucinations) FROM metric_aggregates "
            "WHERE workflow_id=? AND hour>=? GROUP BY prompt_version", (wf, hours[-1]))}
        ev = {"window_start_hour": hours[-1], "window_hours": len(hours),
              "window": window.as_dict(), "baseline": base[0].as_dict(),
              "by_prompt_version": by_prompt, "enough_data": False, "breach": False}
        if window.den >= self.policy.min_qa and base[0].den >= self.policy.min_qa:
            c = compare(base[0], window)
            rise = c.diff
            rel = rise / base[0].value if base[0].value else float("inf")
            recent = [(h, Rate(int(f or 0), int(q or 0))) for h, q, f in self.conn.execute(
                "SELECT hour, SUM(qa_n), SUM(hallucinations) FROM metric_aggregates "
                "WHERE workflow_id=? AND hour IN (%s) GROUP BY hour ORDER BY hour DESC"
                % ",".join("?" * self.policy.sustain_checks),
                (wf, *hours[: self.policy.sustain_checks]))]
            sustained = (len(recent) == self.policy.sustain_checks and all(
                r.den >= self.policy.min_qa_per_hour
                and r.value - base[0].value >= self.policy.min_abs_increase for _, r in recent))
            ev.update(enough_data=True, z=round(c.z, 3), abs_increase=round(rise, 4),
                      rel_increase=round(rel, 3) if rel != float("inf") else None,
                      recent_hours={h: r.as_dict() for h, r in recent}, sustained=sustained,
                      breach=(c.z >= self.policy.z_threshold
                              and rise >= self.policy.min_abs_increase
                              and rel >= self.policy.min_rel_increase and sustained))
        return ev

    def _detect(self, wf: str, data_now: str, now: str) -> tuple[str, str] | None:
        ev = self.evaluate(wf, data_now)
        if ev is None or not ev["breach"]:
            return None
        prompt = (self.registry.config(wf) or {}).get("prompt_version")
        blocked = self.conn.execute(
            "SELECT incident_id, status FROM incidents WHERE workflow_id=? AND kind=? AND "
            "subject=? AND status IN (?,?)", (wf, KIND, prompt or "", NO_SAFE_FIX, ESCALATED)
        ).fetchone()
        if blocked:
            key = f"halluc_blocked:{wf}:{blocked[0]}"
            if not get_state(self.conn, key):
                with transaction(self.conn):
                    set_state(self.conn, key, now)
                log.warning("PROMPT INCIDENT %s still unresolved (%s) and prompt %s is still "
                            "live: waiting for a human, not re-spending credits",
                            blocked[0], blocked[1], prompt)
            return None
        since = min(ev["recent_hours"])
        inc_id = f"inc:{KIND}:{wf}:{prompt}:{since}"
        with transaction(self.conn):
            self.conn.execute(
                "INSERT OR IGNORE INTO incidents(incident_id, kind, workflow_id, subject, status, "
                "detected_ts, evidence, mitigation) VALUES (?,?,?,?,?,?,?,?)",
                (inc_id, KIND, wf, prompt or "", DETECTED, now,
                 json.dumps({**ev, "breach_since": since, "prompt_at_detection": prompt,
                             "policy": self.policy.__dict__}),
                 json.dumps({"attempts": 0, "tried": [], "validated": []})))
            self.conn.execute("INSERT INTO incident_log(incident_id, sim_ts, from_status, "
                              "to_status, detail) VALUES (?,?,?,?,?)",
                              (inc_id, now, None, DETECTED, json.dumps(ev)))
        log.warning("PROMPT INCIDENT %s detected: %s hallucination flag rate %s (n=%s) vs history "
                    "%s, +%.1fpp (z=%s); last %d hours each elevated: %s; prompt %s", inc_id, wf,
                    ev["window"]["rate"], ev["window"]["den"], ev["baseline"]["rate"],
                    100 * ev["abs_increase"], ev["z"], self.policy.sustain_checks,
                    {h: r["rate"] for h, r in ev["recent_hours"].items()}, prompt)
        return inc_id, DETECTED

    # ---------------------------------------------------------------- respond

    def _supervisor_prompt_edit_since(self, wf: str, since: str) -> dict | None:
        row = self.conn.execute(
            "SELECT event_id, ts, new_version FROM config_changes WHERE workflow_id=? AND "
            "actor='supervisor' AND ts > ? AND changed_dims LIKE '%prompt_version%' "
            "ORDER BY event_id DESC LIMIT 1", (wf, since)).fetchone()
        return dict(row) if row else None

    def _credits(self) -> tuple[float | None, float]:
        state = self.client.state()
        credits = state.get("credits_remaining")
        credits = float(credits) if credits is not None else None
        start = get_state(self.conn, "credits_start")
        if start in (None, "None") and credits is not None:
            with transaction(self.conn):
                set_state(self.conn, "credits_start", credits)
            start = credits
        reserve = max(self.policy.min_reserve,
                      self.policy.reserve_fraction * float(start or 0))
        return credits, reserve

    def _spend(self, wf, inc_id, now, action, cost, before, detail) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO credit_ledger(sim_ts, workflow_id, incident_id, action, cost, "
                "credits_before, credits_after, detail) VALUES (?,?,?,?,?,?,?,?)",
                (now, wf, inc_id, action, cost, before,
                 None if before is None else before - cost, json.dumps(detail)))
        log.warning("CREDITS %s spent %.0f on %s for %s (remaining ~%s)", inc_id, cost, action,
                    wf, None if before is None else before - cost)

    def _respond(self, wf: str, inc, now: str) -> tuple[str, str] | None:
        inc_id = inc["incident_id"]
        mit = json.loads(inc["mitigation"])
        edit = self._supervisor_prompt_edit_since(wf, inc["detected_ts"])
        if edit:
            self._transition(inc_id, DETECTED, SUPERSEDED, now, {
                "reason": "supervisor changed prompt_version after detection; not overriding, "
                          "monitoring continues", "supervisor_event": edit})
            return inc_id, SUPERSEDED
        min_pass = self._min_pass(wf)
        current = (self.registry.config(wf) or {}).get("prompt_version")

        # Prefer a candidate already validated in an earlier attempt (no new spend).
        bad = json.loads(inc["evidence"]).get("prompt_at_detection")
        spare = [v for v in mit["validated"] if v["passed"] and v["prompt_version"] not in
                 mit["tried"] and v["prompt_version"] not in (current, bad)]
        if not spare:
            credits, reserve = self._credits()
            cost = self.policy.cost_per_attempt
            if credits is not None and credits - cost < reserve:
                self._transition(inc_id, DETECTED, NO_SAFE_FIX, now, {
                    "reason": f"credits {credits} - {cost} would breach the reserve {reserve}",
                    "escalated": True})
                return inc_id, NO_SAFE_FIX
            try:
                spare = self._suggest_and_validate(wf, inc, now, credits, current, min_pass, mit)
            except InsufficientCredits as e:
                self._transition(inc_id, DETECTED, NO_SAFE_FIX, now,
                                 {"reason": f"402 from simulator: {e}", "escalated": True})
                return inc_id, NO_SAFE_FIX
            except SimulatorError as e:
                log.warning("PROMPT INCIDENT %s: suggestion/validation call failed (%s); "
                            "will retry next tick", inc_id, e)
                return None
        if not spare:
            self._transition(inc_id, DETECTED, NO_SAFE_FIX, now, {
                "reason": f"no candidate reached min_validation_pass_rate {min_pass}",
                "validated": mit["validated"], "escalated": True})
            return inc_id, NO_SAFE_FIX
        best = max(spare, key=lambda v: (v["lower_bound"], -mit["validated"].index(v)))
        mit["chosen"] = best
        self._transition(inc_id, DETECTED, FIX_PROPOSED, now,
                         {"chosen": best, "replaces": current}, mitigation=json.dumps(mit))
        return inc_id, FIX_PROPOSED

    def _min_pass(self, wf: str) -> float | None:
        g = self.registry.get(wf).meta.get("governance") or {}
        v = g.get("min_validation_pass_rate")
        return float(v) if v is not None else None

    def _suggest_and_validate(self, wf, inc, now, credits, current, min_pass, mit) -> list[dict]:
        inc_id = inc["incident_id"]
        ev = json.loads(inc["evidence"])
        problem = (f"Hallucination flag rate rose to {ev['window']['rate']} from a history rate "
                   f"of {ev['baseline']['rate']} (+{100 * ev['abs_increase']:.1f}pp, "
                   f"z={ev['z']}) on prompt {current}. Need a prompt version that reduces "
                   f"hallucinations without hurting engagement.")
        suggestions = self.client.suggest_fixes(wf, problem)
        self._spend(wf, inc_id, now, "suggest_fixes", self.policy.suggest_cost, credits,
                    {"problem": problem, "suggestions": suggestions})
        credits = None if credits is None else credits - self.policy.suggest_cost
        options = (alloc.options(self.registry.get(wf).meta, "prompt_version")
                   or alloc.options(self.registry.get(wf).meta, "prompt"))
        tried = (set(mit["tried"]) | {current, ev.get("prompt_at_detection")}
                 | {v["prompt_version"] for v in mit["validated"]})
        candidates, skipped = [], []
        for s_ in suggestions:
            pv = s_["prompt_version"]
            if (options and pv not in options) or pv in tried:
                skipped.append(pv)
                continue
            candidates.append(s_)
        out = []
        for s_ in candidates[: self.policy.max_validations]:
            res = self.client.run_validation(wf, s_["prompt_version"], self.policy.validation_n)
            cost = math.ceil(res["n"] / 100)
            self._spend(wf, inc_id, now, "validation_run", cost, credits,
                        {"prompt_version": s_["prompt_version"], "result": res})
            credits = None if credits is None else credits - cost
            rate = res["pass_rate"]
            lb = Rate(round(rate * res["n"]), res["n"]).wilson(Z95)[0] if rate is not None else 0
            v = {"prompt_version": s_["prompt_version"], "summary": s_.get("summary"),
                 "validation_run_id": res["validation_run_id"], "pass_rate": rate,
                 "n": res["n"], "lower_bound": round(lb, 4),
                 "passed": rate is not None and (min_pass is None or rate >= min_pass)}
            mit["validated"].append(v)
            if v["passed"]:
                out.append(v)
        with transaction(self.conn):
            self.conn.execute("UPDATE incidents SET mitigation=? WHERE incident_id=?",
                              (json.dumps(mit), inc_id))
        log.info("PROMPT INCIDENT %s: suggestions %s, skipped %s (not in options or tried), "
                 "validated %s", inc_id, [s_["prompt_version"] for s_ in suggestions], skipped,
                 [(v["prompt_version"], v["pass_rate"], v["passed"]) for v in mit["validated"]])
        return out

    # ---------------------------------------------------------------- applied?

    def _check_applied(self, wf: str, inc, now: str) -> tuple[str, str] | None:
        mit = json.loads(inc["mitigation"])
        chosen = mit["chosen"]["prompt_version"]
        live = (self.registry.config(wf) or {}).get("prompt_version")
        edit = self._supervisor_prompt_edit_since(wf, inc["detected_ts"])
        if edit and live != chosen:
            self._transition(inc["incident_id"], FIX_PROPOSED, SUPERSEDED, now, {
                "reason": "supervisor changed prompt_version while the fix awaited approval",
                "supervisor_event": edit})
            return inc["incident_id"], SUPERSEDED
        if live != chosen:
            return None
        mit["tried"].append(chosen)
        mit["attempts"] += 1
        mit["applied_ts"] = now
        mit["applied_hour"] = now[:13]
        self._transition(inc["incident_id"], FIX_PROPOSED, FIX_APPLIED, now,
                         {"prompt_version": chosen, "version": self.registry.current_version(wf)},
                         mitigation=json.dumps(mit))
        return inc["incident_id"], FIX_APPLIED

    # ---------------------------------------------------------------- verify

    def _verify(self, wf: str, inc, now: str) -> tuple[str, str] | None:
        inc_id = inc["incident_id"]
        mit = json.loads(inc["mitigation"])
        ev = json.loads(inc["evidence"])
        chosen = mit["chosen"]["prompt_version"]
        halluc, engage = _rate_since(self.conn, wf, mit["applied_hour"], prompt=chosen)
        if halluc.den < self.policy.verify_min_qa or engage.den < self.policy.verify_min_connected:
            return None
        base_h, base_e = self.baseline(wf)
        incident_window = Rate(ev["window"]["num"], ev["window"]["den"])
        fixed = (halluc.value - base_h.value < self.policy.min_abs_increase
                 and halluc.wilson()[1] < incident_window.value)
        obj = self.registry.get(wf).objective
        tol = (obj.guardrail_max_drop if obj.guardrail == "engagement"
               and obj.guardrail_max_drop else self.policy.engagement_harm_pp)
        ce = compare(base_e, engage)
        harmed = ce.z <= -self.policy.harm_z and -ce.diff >= tol
        detail = {"prompt_version": chosen, "hallucination_after": halluc.as_dict(),
                  "hallucination_baseline": base_h.as_dict(),
                  "incident_window": incident_window.as_dict(), "fixed": fixed,
                  "engagement_after": engage.as_dict(), "engagement_baseline": base_e.as_dict(),
                  "engagement_z": round(ce.z, 3), "engagement_tolerance": tol, "harmed": harmed}
        if fixed and not harmed:
            self._transition(inc_id, FIX_APPLIED, RESOLVED, now, detail,
                             mitigated_ts=now, mitigation=json.dumps(mit))
            return inc_id, RESOLVED
        if not fixed and halluc.value - base_h.value < self.policy.min_abs_increase and not harmed:
            return None                                  # improving; not yet clearly separated
        reason = "engagement harmed" if harmed else "hallucination not fixed"
        if mit["attempts"] >= self.policy.max_attempts:
            self._transition(inc_id, FIX_APPLIED, ESCALATED, now,
                             {**detail, "reason": f"{reason}; {mit['attempts']} attempts used"},
                             mitigation=json.dumps(mit))
            return inc_id, ESCALATED
        mit.pop("chosen", None)
        self._transition(inc_id, FIX_APPLIED, FIX_FAILED, now, {**detail, "reason": reason},
                         mitigation=json.dumps(mit))
        self._transition(inc_id, FIX_FAILED, DETECTED, now,
                         {"reason": "next attempt (never back to the hallucinating prompt)"})
        return inc_id, FIX_FAILED
