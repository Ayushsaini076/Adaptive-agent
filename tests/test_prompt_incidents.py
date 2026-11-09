"""C8 prompt / hallucination incidents.

Scenario modelled on public_02_incidents: a supervisor switches ks_cancel to a
worse prompt; hallucination flags rise. The controller must detect it at
population level, spend credits only when credible, validate candidates, send
the fix through approval with validation evidence, and verify after apply that
hallucinations fell AND engagement held.
"""

import json
import logging

import pytest

from controller.database import connect, set_state, transaction
from controller.governance import GovernanceManager
from controller.ingestion import Ingester
from controller.main import run
from controller.metrics import snapshot_baselines
from controller.prompt_incidents import HallucinationMonitor, PromptPolicy
from controller.settings import Settings
from controller.workflows import WorkflowRegistry
from tests.fakes import FakeSimulator
from tests.test_governance import META, _config

HALLUC = {"P1": 0.03, "P2": 0.12, "P3": 0.03, "P4": 0.03, "P5": 0.03}
ENGAGE = {"P1": 0.5, "P2": 0.5, "P3": 0.5, "P4": 0.5, "P5": 0.5}


def make_activity(halluc=HALLUC, engage=ENGAGE, per_hour=200):
    def activity(sim, start, end):
        events = []
        cfg = sim.configs["ks_cancel"]
        pv = cfg["prompt_version"]
        for minute in range(start - start % 60, end, 60):
            if minute < start or not 9 <= (minute // 60) % 24 < 21:
                continue
            j = 0
            for i in range(per_hour):
                connected = i % 2 == 0
                flag = engaged = False
                if connected:
                    h, e = halluc[pv], engage[pv]
                    flag = (j * h) % 1 + h >= 1
                    engaged = ((j + 1) * e) % 1 + e >= 1
                    j += 1
                events.append({
                    "event_id": sim.next_event_id, "type": "call", "ts": sim._ts(minute + 5),
                    "workflow_id": "ks_cancel", "campaign_id": "c",
                    "lead_id": f"L{sim.next_event_id}", "person_id": f"p{sim.next_event_id}",
                    "attempt_no": 1, "config_version": cfg["version"], "cohort_id": "default",
                    "variant": {"welcome_message": "W1", "voice": "V1",
                                "caller_id": "D1" if (i // 2) % 2 else "D2",
                                "prompt_version": pv},
                    "connected": connected, "engaged": engaged,
                    "disposition": ("concluded_interested" if engaged else
                                    "short_hangup" if connected else "no_answer"),
                    "qa": {"hallucination_flag": flag, "flow_adherence": 0.9}
                    if connected else None})
                sim.next_event_id += 1
        return events
    return activity


def make_sim(activity, days=3, **kw):
    sim = FakeSimulator(workflows={"ks_cancel": json.loads(json.dumps(META["ks_cancel"]))},
                        configs={"ks_cancel": _config()}, days=days, activity=activity,
                        credits=200, **kw)
    sim.next_event_id = 10_000
    sim.history_chunks = {1: activity(sim, -1440, 0)}       # one day on P1
    return sim


# ------------------------------------------------------------------ end to end via run()


def test_bad_supervisor_prompt_detected_fixed_and_verified(tmp_path, caplog):
    sim = make_sim(make_activity(), suggestions=["P4", "P3", "P9"],
                   validation_pass={"P3": 0.99, "P4": 0.975},
                   scheduled_edits=[(12 * 60, "ks_cancel", {"prompt_version": "P2"})])
    conn = connect(tmp_path / "c.db")
    with caplog.at_level(logging.WARNING):
        run(Settings(sim_url="http://sim.test", advance_minutes=60), sim.client(), conn)

    inc = dict(conn.execute("SELECT * FROM incidents WHERE kind='prompt_hallucination'").fetchone())
    assert inc["subject"] == "P2" and inc["status"] == "resolved"
    steps = [r[0] for r in conn.execute("SELECT to_status FROM incident_log WHERE incident_id=? "
                                        "ORDER BY id", (inc["incident_id"],))]
    assert steps == ["detected", "fix_proposed", "fix_applied", "resolved"]

    mit = json.loads(inc["mitigation"])
    assert mit["chosen"]["prompt_version"] == "P3"            # higher lower bound than P4
    assert [v["prompt_version"] for v in mit["validated"]] == ["P4", "P3"]   # P9 not in options
    spent = [r[0] for r in conn.execute("SELECT action FROM credit_ledger")]
    assert spent == ["suggest_fixes", "validation_run", "validation_run"]
    assert sim.credits == 200 - 3 - 3 - 3

    assert conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='caller_id_health'"
                        ).fetchone()[0] == 0                   # equal caller IDs: no false alarm
    p = dict(conn.execute("SELECT * FROM proposals").fetchone())
    body = json.loads(p["payload"])
    assert p["route"] == "approval" and p["status"] == "applied" and sim.puts == []
    assert body["evidence"]["validation_run_id"] == mit["chosen"]["validation_run_id"]
    assert body["evidence"]["validation_pass_rate"] == 0.99
    assert "SUPERVISOR" in body["rationale"]                   # explicit, never silent
    assert int(p["submitted_ts"][11:13]) >= 11                 # Kestrel: none before 11:00
    assert sim.configs["ks_cancel"]["prompt_version"] == "P3"
    resolved = json.loads(conn.execute("SELECT detail FROM incident_log WHERE incident_id=? AND "
                                       "to_status='resolved'", (inc["incident_id"],)).fetchone()[0])
    assert resolved["fixed"] and not resolved["harmed"]
    assert "PROMPT INCIDENT" in caplog.text and "CREDITS" in caplog.text


def test_fix_that_hurts_engagement_fails_and_next_candidate_is_tried(tmp_path):
    engage = {**ENGAGE, "P3": 0.30}                            # safe but people hang up
    sim = make_sim(make_activity(engage=engage), days=4, suggestions=["P4", "P3"],
                   validation_pass={"P3": 0.99, "P4": 0.98},
                   scheduled_edits=[(12 * 60, "ks_cancel", {"prompt_version": "P2"})])
    conn = connect(tmp_path / "c.db")
    run(Settings(sim_url="http://sim.test", advance_minutes=60), sim.client(), conn)

    inc = dict(conn.execute("SELECT * FROM incidents WHERE kind='prompt_hallucination'").fetchone())
    steps = [r[0] for r in conn.execute("SELECT to_status FROM incident_log WHERE incident_id=? "
                                        "ORDER BY id", (inc["incident_id"],))]
    assert steps[:5] == ["detected", "fix_proposed", "fix_applied", "fix_failed", "detected"]
    failed = json.loads(conn.execute("SELECT detail FROM incident_log WHERE incident_id=? AND "
                                     "to_status='fix_failed'", (inc["incident_id"],)).fetchone()[0])
    assert failed["reason"] == "engagement harmed" and failed["fixed"]
    mit = json.loads(inc["mitigation"])
    assert mit["tried"][:2] == ["P3", "P4"]                    # never back to P2
    assert len([r for r in conn.execute("SELECT 1 FROM credit_ledger "
                                        "WHERE action='suggest_fixes'")]) == 1   # reused P4
    assert inc["status"] in ("resolved", "fix_proposed", "fix_applied")
    assert sim.configs["ks_cancel"]["prompt_version"] != "P2"


# ------------------------------------------------------------------ unit: detection & spending


class World:
    def __init__(self, tmp_path, halluc_live=0.12, credits=200, **kw):
        self.sim = make_sim(make_activity(halluc={**HALLUC, "P1": 0.03}), credits=credits, **kw) \
            if False else make_sim(make_activity(), **kw)
        self.sim.credits = credits
        self.conn = connect(tmp_path / "c.db")
        self.reg = WorkflowRegistry(self.conn, self.sim.client())
        self.reg.sync_all()
        self.ing = Ingester(self.conn, self.sim.client())
        self.ing.catch_up("history")
        with transaction(self.conn):
            snapshot_baselines(self.conn, "ks_cancel", "history",
                               end="2027-01-04T00:00:00+05:30")
        self.mon = HallucinationMonitor(self.conn, self.sim.client(), self.reg)
        self.gov = GovernanceManager(self.conn, self.sim.client(), self.reg, {})
        self.chunk = 100
        self.eid = 900_000

    def hours(self, day_hours, rate, n_connected=100, pv="P2"):
        """Ingest calls for the given hours with a hallucination rate; tick after each."""
        changes = []
        for h in day_hours:
            evs = []
            for j in range(n_connected):
                evs.append({"event_id": self.eid, "type": "call",
                            "ts": f"2027-01-04T{h:02d}:05:00+05:30", "workflow_id": "ks_cancel",
                            "lead_id": f"x{self.eid}", "person_id": f"x{self.eid}",
                            "config_version": 1, "cohort_id": "default",
                            "variant": {"prompt_version": pv, "caller_id": "D1"},
                            "connected": True, "engaged": j % 2 == 0,
                            "qa": {"hallucination_flag": (j * rate) % 1 + rate >= 1}})
                self.eid += 1
            self.chunk += 1
            self.sim.event_chunks[self.chunk] = evs
            self.ing.ingest_chunk(self.chunk, "events")
            with transaction(self.conn):
                set_state(self.conn, "sim_time", f"2027-01-04T{h:02d}:30:00+05:30")
            changes += self.mon.tick()
        return changes

    def incident(self):
        r = self.conn.execute("SELECT * FROM incidents WHERE kind='prompt_hallucination'"
                              ).fetchone()
        return dict(r) if r else None


@pytest.fixture
def w(tmp_path):
    return World(tmp_path, suggestions=["P3"], validation_pass={"P3": 0.99})


def test_one_bad_hour_or_small_sample_never_triggers(w):
    assert w.hours([9], 0.5, n_connected=20) == []             # tiny, extreme
    assert w.hours([10, 11, 12], 0.03) == []
    assert w.hours([13], 0.4) == []                            # one terrible hour
    assert w.hours(range(14, 21), 0.03) == []
    assert w.incident() is None and w.sim.paid_calls == []


def test_mild_rise_below_size_threshold_never_triggers(w):
    assert w.hours(range(9, 21), 0.045, n_connected=300) == []  # +1.5pp, significant but small
    assert w.incident() is None


def test_sustained_rise_detected_then_credits_spent_once(w):
    changes = w.hours(range(9, 21), 0.15)
    kinds = [c[1] for c in changes]
    assert kinds.count("detected") == 1 and "fix_proposed" in kinds
    assert w.sim.paid_calls == [("suggest", 3), ("validate:P3", 3)]
    ev = json.loads(w.incident()["evidence"])
    assert ev["z"] >= 4 and ev["abs_increase"] >= 0.02 and ev["rel_increase"] >= 0.5
    w.hours([21], 0.15)
    assert len(w.sim.paid_calls) == 2                          # no repeat spend while open


def test_low_credits_escalate_without_spending(tmp_path):
    w = World(tmp_path, credits=12, suggestions=["P3"], validation_pass={"P3": 0.99})
    w.hours(range(9, 21), 0.15)
    inc = w.incident()
    assert inc["status"] == "no_safe_fix" and w.sim.paid_calls == []
    detail = json.loads(w.conn.execute("SELECT detail FROM incident_log WHERE "
                                       "to_status='no_safe_fix'").fetchone()[0])
    assert "reserve" in detail["reason"]


def test_no_candidate_passing_validation_escalates(tmp_path):
    w = World(tmp_path, suggestions=["P3", "P4"], validation_pass={"P3": 0.95, "P4": 0.96})
    w.hours(range(9, 21), 0.15)
    assert w.incident()["status"] == "no_safe_fix"
    assert len(w.sim.paid_calls) == 3                          # spent, recorded, no fix
    assert w.conn.execute("SELECT COUNT(*) FROM credit_ledger").fetchone()[0] == 3


def test_prompt_at_detection_and_unknown_versions_are_never_candidates(tmp_path):
    w = World(tmp_path, suggestions=["P2", "P9", "P3"], validation_pass={"P2": 0.99, "P3": 0.98})
    w.sim.configs["ks_cancel"] = {**w.sim.configs["ks_cancel"], "prompt_version": "P2",
                                  "version": 2}
    w.reg.refresh("ks_cancel")
    w.hours(range(9, 21), 0.15)
    mit = json.loads(w.incident()["mitigation"])
    assert [v["prompt_version"] for v in mit["validated"]] == ["P3"]


def test_supervisor_prompt_change_after_detection_supersedes(w):
    for h in range(9, 21):                                     # stop at the detection hour
        if any(c[1] == "detected" for c in w.hours([h], 0.15)):
            break
    assert w.incident()["status"] == "detected" and w.sim.paid_calls == []
    w.sim.minute = 24 * 60 * 0 + h * 60 + 45                   # edit happens after detection
    w.sim.pending_events.append(w.sim.supervisor_edit("ks_cancel", prompt_version="P5"))
    w.ing.ingest_chunk(w.sim.flush_events(), "events")
    w.reg.refresh_changed()
    w.hours([h + 1], 0.15, pv="P5")
    assert w.incident()["status"] == "superseded" and w.sim.paid_calls == []   # no spend


def test_unresolved_incident_blocks_respending_on_same_prompt(tmp_path):
    w = World(tmp_path, suggestions=["P3"], validation_pass={"P3": 0.90})
    w.hours(range(9, 21), 0.15)
    w.hours(range(9, 21), 0.15)        # another day of the same bad prompt
    rows = w.conn.execute("SELECT status FROM incidents WHERE kind='prompt_hallucination'"
                          ).fetchall()
    assert [r[0] for r in rows] == ["no_safe_fix"] and len(w.sim.paid_calls) == 2


def test_prompt_fix_goes_to_approval_with_evidence_never_auto(w):
    w.hours(range(9, 21), 0.15)
    assert w.incident()["status"] == "fix_proposed"
    with transaction(w.conn):
        set_state(w.conn, "sim_time", "2027-01-05T11:00:00+05:30")
    (pid, status), = w.gov.tick()
    body = json.loads(w.conn.execute("SELECT payload FROM proposals").fetchone()[0])
    assert status == "pending" and w.sim.puts == []            # prompt not auto-eligible
    assert body["proposed_config"]["prompt_version"] == "P3"
    assert body["evidence"]["validation_run_id"] and body["evidence"]["validation_pass_rate"] == 0.99
    assert body["impact_score"] > 0 and body["dry_run"]["traffic_share_changed"] == 1.0


def test_rejected_prompt_fix_escalates_incident(w):
    w.hours(range(9, 21), 0.15)
    with transaction(w.conn):
        set_state(w.conn, "sim_time", "2027-01-05T11:00:00+05:30")
    (pid, _), = w.gov.tick()
    w.sim.decide_after_minutes, w.sim.approval_decision = 0, "rejected"
    assert (pid, "rejected") in w.gov.tick()
    assert w.incident()["status"] == "escalated"
    assert w.gov.tick() == []                                  # not re-sent


def test_402_from_simulator_escalates_instead_of_crashing(tmp_path):
    w = World(tmp_path, credits=200, suggestions=["P3"], validation_pass={"P3": 0.99})
    w.mon.policy = PromptPolicy(min_reserve=0, reserve_fraction=0)
    w.mon._credits = lambda: (None, 0.0)                       # credits unknown to controller
    w.sim.credits = 1                                          # simulator will refuse
    w.hours(range(9, 21), 0.15)
    inc = w.incident()
    assert inc["status"] == "no_safe_fix"
    assert "402" in json.loads(w.conn.execute(
        "SELECT detail FROM incident_log WHERE to_status='no_safe_fix'").fetchone()[0])["reason"]
