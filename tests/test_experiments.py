"""Experiment lifecycle through the fake simulator.

Go-live is simulated with a config edit carrying the experiment's
proposed_config, standing in for the M5 approval/apply path.
"""

import json

import pytest

from controller.database import connect, transaction
from controller.experiments import (
    ExperimentManager,
    ExperimentPolicy,
    decide,
    first_exposures,
    plan,
)
from controller.ingestion import Ingester
from controller.metrics import Rate, snapshot_baselines
from controller.workflows import WorkflowRegistry
from tests.fakes import FakeSimulator

HIST_END = "2027-01-04T00:00:00+05:30"
# History: 200 persons/day, pickup 0.5, engagement 0.8 (of connected).
# Primary (pickup, mde 0.2): 93/arm. Guardrail (engagement, tolerance 0.2):
# 63 connected/arm; at 0.5 connected per dial that is 126 dials -> n_per_arm 126.
POLICY = ExperimentPolicy(dimensions=("voice",), mde=0.2, min_practical_uplift=0.05,
                          max_candidates=1, min_history_units=50, max_duration_hours=72)


def make_call(eid, person, ts, voice, connected, engaged=None, version=1, cohort="default"):
    engaged = connected if engaged is None else engaged and connected
    return {"event_id": eid, "type": "call", "ts": ts, "workflow_id": "wf",
            "campaign_id": "camp", "lead_id": f"L{person}", "person_id": person,
            "attempt": 1, "config_version": version, "cohort_id": cohort,
            "variant": {"voice": voice, "welcome_message": "w1"},
            "connected": connected, "engaged": engaged,
            "disposition": "concluded_interested" if engaged else
            ("short_hangup" if connected else "no_answer")}


class World:
    """Fake sim + DB + registry + manager, bootstrapped with one day of history."""

    def __init__(self, tmp_path, policy=POLICY, max_drop=0.2):
        self.sim = FakeSimulator(
            workflows={"wf": {
                "tenant_id": "t", "mid_flight": "allowed",
                "objective": {"primary": "pickup",
                              "guardrail": {"kpi": "engagement", "max_drop": max_drop}},
                "options": {"voice": ["v1", "v2", "v3"], "welcome_message": ["w1"]}}},
            configs={"wf": {"version": 1, "cohorts": [],
                            "allocations": {"default": {"voice": {"v1": 1.0},
                                                        "welcome_message": {"w1": 1.0}}}}},
            history_chunks={1: [make_call(i, f"h{i}", "2027-01-03T10:00:00+05:30", "v1",
                                          connected=i % 2 == 0, engaged=i % 10 != 0)
                                for i in range(1, 201)]})
        self.sim.next_event_id = 10_000
        self.conn = connect(tmp_path / "c.db")
        self.reg = WorkflowRegistry(self.conn, self.sim.client())
        self.reg.sync_all()
        self.ing = Ingester(self.conn, self.sim.client())
        self.ing.catch_up("history")
        with transaction(self.conn):
            snapshot_baselines(self.conn, "wf", "history", end=HIST_END, min_n=50)
        self.mgr = ExperimentManager(self.conn, self.reg, policy)
        self.chunk = 100

    def ingest(self, events):
        self.chunk += 1
        self.sim.event_chunks[self.chunk] = events
        self.ing.ingest_chunk(self.chunk, "events")
        self.reg.refresh_changed()

    def edit(self, **changes):
        self.ingest([self.sim.supervisor_edit("wf", **changes)])

    def go_live(self, exp_id):
        cfg = json.loads(self.mgr.store.get(exp_id)["proposed_config"])
        self.edit(allocations=cfg["allocations"])

    def live_calls(self, voice, n, rate, *, start_eid, engaged_rate=1.0, version=2,
                   hour=10, prefix=None):
        out = []
        for i in range(n):
            connected = (i * rate) % 1 + rate >= 1          # exact share, spread evenly
            engaged = (i * engaged_rate) % 1 + engaged_rate >= 1
            out.append(make_call(start_eid + i, f"{prefix or voice}_{i}",
                                 f"2027-01-04T{hour:02d}:00:00+05:30", voice,
                                 connected, engaged, version))
        return out

    def exp(self, exp_id):
        return self.mgr.store.get(exp_id)


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def _planned(w):
    changes = w.mgr.tick()
    assert len(changes) == 1 and changes[0][1] == "planned"
    return changes[0][0]


def _running(w):
    exp_id = _planned(w)
    w.go_live(exp_id)
    assert w.mgr.tick() == [(exp_id, "running")]
    return exp_id


# ------------------------------------------------------------------ planning


def test_plan_is_deterministic_and_sized_from_history(w):
    design, _ = plan(w.conn, w.reg, "wf", POLICY)
    again, _ = plan(w.conn, w.reg, "wf", POLICY)
    assert design.experiment_id == again.experiment_id == "exp:wf:default:voice:v2:v1"
    assert (design.control, design.candidates) == ("v1", ["v2"])
    sizing = design.evidence["sizing"]
    assert (sizing["n_primary"], sizing["n_guardrail"]) == (93, 63)
    assert sizing["guardrail_units_per_primary_unit"] == 0.5
    assert design.n_per_arm == 126 and design.n_guardrail_per_arm == 63
    assert design.allocation == {"v1": 0.5, "v2": 0.5}
    assert design.evidence["baseline"]["rate"] == 0.5
    assert design.evidence["units_per_day"] == 200
    # proposed config changes only the cohort/dim under test
    assert design.proposed_config["allocations"]["default"]["welcome_message"] == {"w1": 1.0}


def test_plan_explains_infeasibility(w):
    strict = ExperimentPolicy(dimensions=("voice",), mde=0.01, min_history_units=50)
    design, reasons = plan(w.conn, w.reg, "wf", strict)
    assert design is None
    assert any("needs" in r and "72h" in r for r in reasons)


def test_plan_uses_more_candidates_when_affordable(w):
    two = ExperimentPolicy(dimensions=("voice",), mde=0.2, max_candidates=2,
                           min_history_units=50, max_duration_hours=72)
    design, _ = plan(w.conn, w.reg, "wf", two)
    assert design.candidates == ["v2", "v3"]
    assert design.evidence["sizing"]["alpha_per_comparison"] == pytest.approx(0.025)


def test_dimension_with_single_option_is_skipped(w):
    p = ExperimentPolicy(dimensions=("welcome_message",), mde=0.2, min_history_units=50)
    design, reasons = plan(w.conn, w.reg, "wf", p)
    assert design is None and any("fewer than 2 options" in r for r in reasons)


def test_replan_waits_for_interval(tmp_path):
    w = World(tmp_path, policy=ExperimentPolicy(dimensions=("voice",), mde=0.01,
                                                min_history_units=50))
    assert w.mgr.tick() == []                                  # infeasible, attempt recorded
    n = w.conn.execute("SELECT value FROM controller_state WHERE key='exp_plan_attempt:wf'"
                       ).fetchone()
    assert n is not None
    assert w.mgr.tick() == []


# ------------------------------------------------------------------ lifecycle


def test_winner_end_to_end(w):
    exp_id = _running(w)
    assert w.exp(exp_id)["start_version"] == 2
    w.ingest(w.live_calls("v1", 250, 0.40, start_eid=20_000)
             + w.live_calls("v2", 250, 0.75, start_eid=30_000))
    assert w.mgr.tick() == [(exp_id, "decided")]
    d = json.loads(w.exp(exp_id)["decision"])
    assert d["outcome"] == "winner" and d["winner"] == "v2"
    t = d["tests"]["v2"]
    assert t["significant"] and t["practical"] and t["guardrail"]["status"] == "pass"
    assert d["arms"]["v1"]["primary"]["den"] == 250
    rec = d["recommendation"]
    assert rec["allocation"] == {"v2": 0.9, "v1": 0.1}         # holdout kept for decay checks
    assert rec["proposed_config"]["allocations"]["default"]["voice"] == {"v1": 0.1, "v2": 0.9}
    log = [r[0] for r in w.conn.execute(
        "SELECT to_status FROM experiment_log WHERE experiment_id=? ORDER BY id", (exp_id,))]
    assert log == ["planned", "running", "decided"]
    rows = w.conn.execute("SELECT COUNT(*) FROM experiment_evaluations WHERE experiment_id=?",
                          (exp_id,)).fetchone()[0]
    assert rows == 4                                           # 2 arms x (primary, guardrail)


def test_waits_until_sample_reached(w):
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 50, 0.4, start_eid=20_000)
             + w.live_calls("v2", 50, 0.75, start_eid=30_000))
    assert w.mgr.tick() == []                                  # no peeking at p-values
    assert w.exp(exp_id)["status"] == "running"


def test_waits_for_guardrail_sample_too(w):
    """Primary sample reached, but low pickup leaves too few connected calls
    for the engagement guardrail: keep collecting rather than decide early."""
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 130, 0.3, start_eid=20_000)      # 39 connected < 63
             + w.live_calls("v2", 130, 0.75, start_eid=30_000))
    assert w.mgr.tick() == []
    assert w.exp(exp_id)["status"] == "running"


def test_guardrail_breach_blocks_winner(w):
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 250, 0.40, start_eid=20_000)
             + w.live_calls("v2", 250, 0.75, start_eid=30_000, engaged_rate=0.5))
    w.mgr.tick()
    d = json.loads(w.exp(exp_id)["decision"])
    assert d["outcome"] == "guardrail_breach" and d["winner"] is None
    assert d["tests"]["v2"]["guardrail"]["status"] == "breach"


def test_no_winner_when_arms_equal(w):
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 250, 0.5, start_eid=20_000)
             + w.live_calls("v2", 250, 0.5, start_eid=30_000))
    w.mgr.tick()
    assert json.loads(w.exp(exp_id)["decision"])["outcome"] == "no_winner"


def test_inconclusive_at_deadline_and_candidate_not_counted_as_tested(w):
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 250, 0.4, start_eid=20_000)
             + w.live_calls("v2", 10, 0.9, start_eid=30_000))
    assert w.mgr.tick() == []
    # Time passes beyond the 72h deadline (data clock moves with ingested calls).
    w.ingest([make_call(40_000, "late", "2027-01-07T12:00:00+05:30", "v1", True, version=2)])
    assert w.mgr.tick() == [(exp_id, "decided")]
    d = json.loads(w.exp(exp_id)["decision"])
    assert d["outcome"] == "inconclusive" and d["under_exposed"] == ["v2"]
    # v2 was never properly tested, so it can be planned again.
    design, _ = plan(w.conn, w.reg, "wf", POLICY)
    assert design.candidates == ["v2"]


def test_tested_candidates_are_not_replanned(w):
    exp_id = _running(w)
    w.ingest(w.live_calls("v1", 250, 0.5, start_eid=20_000)
             + w.live_calls("v2", 250, 0.5, start_eid=30_000))
    w.mgr.tick()
    assert w.exp(exp_id)["status"] == "decided"
    design, _ = plan(w.conn, w.reg, "wf", POLICY)
    assert design.candidates == ["v3"]


def test_supervisor_allocation_change_aborts_running_experiment(w):
    exp_id = _running(w)
    w.edit(allocations={"default": {"voice": {"v3": 1.0}}})
    assert w.mgr.tick() == [(exp_id, "aborted")]
    detail = json.loads(w.conn.execute(
        "SELECT detail FROM experiment_log WHERE experiment_id=? AND to_status='aborted'",
        (exp_id,)).fetchone()[0])
    assert detail["changed_by"] == "supervisor" and detail["version"] == 3


def test_unrelated_supervisor_change_keeps_experiment_running(w):
    exp_id = _running(w)
    cfg = w.reg.config("wf")
    w.edit(prompt_version="p2", allocations=cfg["allocations"])   # allocation unchanged
    assert w.mgr.tick() == []
    assert w.exp(exp_id)["status"] == "running"
    assert w.mgr.eligible_versions(w.exp(exp_id)) == [2, 3]


def test_base_change_before_go_live_cancels_plan(w):
    exp_id = _planned(w)
    w.edit(prompt_version="p2")
    assert w.mgr.tick() == [(exp_id, "cancelled")]


def test_planned_waits_while_config_unchanged(w):
    exp_id = _planned(w)
    assert w.mgr.tick() == []
    assert w.exp(exp_id)["status"] == "planned"


# ------------------------------------------------------------------ exposures


def test_first_exposure_counts_each_person_once(w):
    calls = [
        make_call(50_001, "p1", "2027-01-04T10:00:00+05:30", "v2", False, version=2),
        make_call(50_002, "p1", "2027-01-04T12:00:00+05:30", "v2", True, version=2),  # repeat
        make_call(50_003, "p1", "2027-01-04T13:00:00+05:30", "v1", True, version=2),  # repeat
        make_call(50_004, "p2", "2027-01-04T10:00:00+05:30", "v1", True, version=2),
        make_call(50_005, "p3", "2027-01-04T10:00:00+05:30", "v2", True, version=1),  # old cfg
    ]
    w.ingest(calls)
    kw = dict(versions=[2], start="2027-01-04T00:00:00+05:30")
    pickup = first_exposures(w.conn, "wf", "default", "voice", "pickup", **kw)
    assert pickup == {"v1": Rate(1, 1), "v2": Rate(0, 1)}     # p1 = first dial only
    engagement = first_exposures(w.conn, "wf", "default", "voice", "engagement", **kw)
    assert engagement == {"v1": Rate(1, 1), "v2": Rate(1, 1)}  # p1's first CONNECTED dial


def test_first_exposures_rejects_non_allocation_dimension(w):
    with pytest.raises(ValueError):
        first_exposures(w.conn, "wf", "default", "prompt_version", "pickup",
                        versions=[1], start=HIST_END)


# ------------------------------------------------------------------ decide (pure)


def _decide(primary, guardrail=None, candidates=("b", "c"), **kw):
    guardrail = guardrail or {a: Rate(9000, 10000) for a in ("a", *candidates)}
    args = dict(control="a", candidates=list(candidates), n_per_arm=100, primary=primary,
                guardrail=guardrail, alpha=0.05, min_practical_uplift=0.02, max_drop=0.03,
                relative=False, deadline_passed=False)
    return decide(**{**args, **kw})


def test_holm_controls_family_with_multiple_candidates():
    # b alone: p ~ 0.04 (would pass unadjusted); with 2 candidates Holm -> ~0.08.
    d = _decide({"a": Rate(500, 1000), "b": Rate(546, 1000), "c": Rate(500, 1000)},
                n_per_arm=1000)
    assert d["tests"]["b"]["primary"]["p_value"] < 0.05
    assert d["tests"]["b"]["holm_p"] > 0.05
    assert d["outcome"] == "no_winner"


def test_raw_leader_is_not_automatically_the_winner():
    # c has the higher raw rate but on fewer units -> wider CI; b has the higher lower bound.
    d = _decide({"a": Rate(500, 1000), "b": Rate(620, 1000), "c": Rate(66, 100)},
                n_per_arm=100)
    assert d["outcome"] == "winner"
    assert d["winner"] == "b"


def test_significant_but_not_practical_is_not_promoted():
    d = _decide({"a": Rate(5000, 10000), "b": Rate(5150, 10000)}, candidates=("b",),
                n_per_arm=10000, min_practical_uplift=0.02)
    assert d["tests"]["b"]["significant"] and not d["tests"]["b"]["practical"]
    assert d["outcome"] == "no_winner"


def test_relative_guardrail_tolerance():
    g = {"a": Rate(800, 1000), "b": Rate(770, 1000)}
    p = {"a": Rate(400, 1000), "b": Rate(500, 1000)}
    # -3pp drop, 95% CI ~[-6.7pp, +0.7pp]. Tolerance = rel * control rate (0.8).
    strict = _decide(p, g, candidates=("b",), n_per_arm=1000, max_drop=0.01, relative=True)
    loose = _decide(p, g, candidates=("b",), n_per_arm=1000, max_drop=0.15, relative=True)
    assert strict["outcome"] == "guardrail_unverified"    # CI straddles -0.008
    assert loose["outcome"] == "winner"                    # lower bound > -0.12
    big_drop = {"a": Rate(800, 1000), "b": Rate(700, 1000)}
    breach = _decide(p, big_drop, candidates=("b",), n_per_arm=1000, max_drop=0.01,
                     relative=True)
    assert breach["outcome"] == "guardrail_breach"         # upper bound < -0.008


def test_missing_guardrail_tolerance_fails_only_on_significant_drop():
    p = {"a": Rate(400, 1000), "b": Rate(500, 1000)}
    flat = _decide(p, {"a": Rate(800, 1000), "b": Rate(790, 1000)}, candidates=("b",),
                   n_per_arm=1000, max_drop=None)
    drop = _decide(p, {"a": Rate(800, 1000), "b": Rate(700, 1000)}, candidates=("b",),
                   n_per_arm=1000, max_drop=None)
    assert flat["outcome"] == "winner" and drop["outcome"] == "guardrail_breach"


def test_decide_is_deterministic():
    p = {"a": Rate(500, 1000), "b": Rate(600, 1000), "c": Rate(600, 1000)}
    assert _decide(p, n_per_arm=1000) == _decide(p, n_per_arm=1000)
    assert _decide(p, n_per_arm=1000)["winner"] == "b"           # tie -> declared order


def test_underpowered_guardrail_is_unverified_not_breach():
    p = {"a": Rate(400, 1000), "b": Rate(500, 1000)}
    g = {"a": Rate(90, 100), "b": Rate(90, 100)}          # identical but tiny sample
    d = _decide(p, g, candidates=("b",), n_per_arm=1000, max_drop=0.03)
    assert d["tests"]["b"]["guardrail"]["status"] == "unverified"
    assert d["outcome"] == "guardrail_unverified" and d["winner"] is None
