"""Experiment lifecycle through the fake simulator.

Go-live is simulated with a config edit carrying the experiment's
proposed_config, standing in for the M5 approval/apply path.
"""

import json
from datetime import datetime

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
                          max_candidates=1, min_history_units=50, max_duration_hours=72,
                          post_connect_dims_for_pickup=True)   # lifecycle tests only


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
    assert design.n_per_arm == 93 and design.n_guardrail_per_arm == 63
    assert sizing["design_mde"] == 0.2                      # target affordable: unchanged
    assert design.allocation == {"v1": 0.5, "v2": 0.5}
    assert design.evidence["baseline"]["rate"] == 0.5
    assert design.evidence["traffic"]["history_curve"] == [200]
    assert design.evidence["horizon_source"].startswith("fallback 72h")
    # proposed config changes only the cohort/dim under test
    assert design.proposed_config["allocations"]["default"]["welcome_message"] == {"w1": 1.0}


def test_plan_explains_infeasibility(w):
    strict = ExperimentPolicy(dimensions=("voice",), post_connect_dims_for_pickup=True, mde=0.01, min_history_units=50)
    design, reasons = plan(w.conn, w.reg, "wf", strict)
    assert design is None
    assert any("even the max acceptable MDE" in r and "horizon 72h" in r for r in reasons)


def test_plan_uses_more_candidates_when_affordable(w):
    two = ExperimentPolicy(dimensions=("voice",), post_connect_dims_for_pickup=True, mde=0.2, max_candidates=2,
                           min_history_units=50, max_duration_hours=72)
    design, _ = plan(w.conn, w.reg, "wf", two)
    assert design.candidates == ["v2", "v3"]
    assert design.evidence["sizing"]["alpha_per_comparison"] == pytest.approx(0.025)


def test_dimension_with_single_option_is_skipped(w):
    p = ExperimentPolicy(dimensions=("welcome_message",), mde=0.2, min_history_units=50,
                         post_connect_dims_for_pickup=True)
    design, reasons = plan(w.conn, w.reg, "wf", p)
    assert design is None and any("fewer than 2 options" in r for r in reasons)


def test_replan_waits_for_interval(tmp_path):
    w = World(tmp_path, policy=ExperimentPolicy(dimensions=("voice",), post_connect_dims_for_pickup=True, mde=0.01,
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



# ------------------------------------------------------------------ planner traffic & horizon

from controller.database import set_state  # noqa: E402
from controller.experiments import (  # noqa: E402
    design_mde,
    first_exposure_baseline,
    planning_horizon,
    unit_curve,
    units_within,
)


def _two_day_history(tmp_path):
    """Day 1: persons a0..a99. Day 2: same 100 re-dialled + 50 new b-persons."""
    w = World(tmp_path)
    calls = []
    eid = 70_000
    for i in range(100):
        calls.append(make_call(eid, f"a{i}", "2027-01-02T10:00:00+05:30", "v1", i % 2 == 0))
        eid += 1
    for i in range(100):
        calls.append(make_call(eid, f"a{i}", "2027-01-03T10:00:00+05:30", "v1", True))
        eid += 1
    for i in range(50):
        calls.append(make_call(eid, f"b{i}", "2027-01-03T11:00:00+05:30", "v1", False))
        eid += 1
    w.conn.execute("DELETE FROM calls")
    w.ingest(calls)
    return w


def test_unit_curve_counts_redialled_people_once(tmp_path):
    w = _two_day_history(tmp_path)
    curve = unit_curve(w.conn, "wf", "default", "pickup", HIST_END)
    assert curve == [100, 150]          # NOT 100 + 150 = 250 (old per-day overcount)
    assert units_within(curve, 1) == 100 and units_within(curve, 2) == 150
    assert units_within(curve, 4) == 150 + 2 * 50       # extends with new people/day
    assert units_within(curve, 3, new_only=True) == 150  # next_campaign_only: new only


def test_first_exposure_baseline_uses_first_dial_only(tmp_path):
    w = _two_day_history(tmp_path)
    # a-persons: first dial 50% connected (later re-dials all connected, ignored);
    # b-persons: 0%. -> 50/150, not the all-dials rate 150/250.
    assert first_exposure_baseline(w.conn, "wf", "default", "pickup", HIST_END) == Rate(50, 150)


def test_planning_horizon_uses_time_left_in_scenario(w):
    p = ExperimentPolicy()
    assert planning_horizon(w.conn, p, 0)[0] == 72                     # fallback
    with transaction(w.conn):
        set_state(w.conn, "end_minute", 14400)
        set_state(w.conn, "sim_minute", 0)
    hours, src = planning_horizon(w.conn, p, 24)
    assert hours == 240 - 24 - 48 - 24 and src.startswith("scenario: 240h left")
    capped = ExperimentPolicy(max_duration_hours=100)
    assert planning_horizon(w.conn, capped, 0)[0] == 100


def test_design_mde_prefers_target_then_relaxes_up_to_max():
    p = ExperimentPolicy(mde=0.03, max_acceptable_mde=0.05)
    assert design_mde(0.5, 5000, p, 1) == 0.03                       # target affordable
    relaxed = design_mde(0.5, 2500, p, 1)
    assert 0.03 < relaxed <= 0.05
    from controller.stats import sample_size_per_arm
    assert sample_size_per_arm(0.5, relaxed) <= 2500                  # alpha/power unchanged
    assert design_mde(0.5, 1000, p, 1) is None                       # 5pp needs ~1,565


def test_long_scenario_window_makes_design_feasible(tmp_path):
    """Infeasible within the 72h fallback, feasible within the real scenario window."""
    w = World(tmp_path)
    p = ExperimentPolicy(dimensions=("voice",), post_connect_dims_for_pickup=True, mde=0.1, max_acceptable_mde=0.1,
                         max_candidates=1, min_history_units=50)
    assert plan(w.conn, w.reg, "wf", p)[0] is None
    with transaction(w.conn):
        set_state(w.conn, "end_minute", 14400)
        set_state(w.conn, "sim_minute", 0)
    design, _ = plan(w.conn, w.reg, "wf", p)
    assert design is not None and design.horizon_hours == 168
    assert design.evidence["expected_hours"] <= 168


def test_mde_relaxed_within_max_is_recorded(tmp_path):
    w = World(tmp_path)
    p = ExperimentPolicy(dimensions=("voice",), post_connect_dims_for_pickup=True, mde=0.05, max_acceptable_mde=0.25,
                         max_candidates=1, min_history_units=50)
    design, _ = plan(w.conn, w.reg, "wf", p)       # 72h fallback: ~3 days of 200/day
    sizing = design.evidence["sizing"]
    assert 0.05 < sizing["design_mde"] <= 0.25
    exp_id = _planned_with(w, p)
    assert w.exp(exp_id)["mde"] == sizing["design_mde"]


def _planned_with(w, policy):
    w.mgr = ExperimentManager(w.conn, w.reg, policy)
    changes = w.mgr.tick()
    assert changes and changes[0][1] == "planned"
    return changes[0][0]


def test_guardrail_bound_rejection_explains_itself(tmp_path):
    """nw_kyc-like: a tight guardrail tolerance needs more units than the window has."""
    w = World(tmp_path, max_drop=0.01)
    design, reasons = plan(w.conn, w.reg, "wf", POLICY)
    assert design is None
    assert any("guardrail engagement non-inferiority needs" in r and "window gives" in r
               for r in reasons)


def test_deadline_leaves_room_for_post_decision(w):
    exp_id = _planned(w)                                   # planned with a 72h horizon
    with transaction(w.conn):                              # approval took long: 60h left
        set_state(w.conn, "end_minute", 14400)
        set_state(w.conn, "sim_minute", 14400 - 60 * 60)
    w.go_live(exp_id)
    w.mgr.tick()
    e = w.exp(exp_id)
    hours = (datetime.fromisoformat(e["deadline_ts"])
             - datetime.fromisoformat(e["start_ts"])).total_seconds() / 3600
    assert hours == 60 - 48                                # post-decision time kept free



def test_no_plan_when_scenario_too_short_for_post_decision(w):
    with transaction(w.conn):
        set_state(w.conn, "end_minute", 14400)
        set_state(w.conn, "sim_minute", 14400 - 60 * 60)
    design, reasons = plan(w.conn, w.reg, "wf", POLICY)
    assert design is None and "under one day" in reasons[0]


# ------------------------------------------------------------------ dimension applicability


def test_post_connect_dims_skipped_for_pickup_primary(w):
    p = ExperimentPolicy(mde=0.2, min_history_units=50)     # default: no opt-in
    design, reasons = plan(w.conn, w.reg, "wf", p)
    assert design is None
    assert any("voice: acts only after a call connects" in r for r in reasons)


def test_running_experiment_logs_continuation_across_unrelated_edit(w, caplog):
    import logging
    exp_id = _running(w)
    w.edit(prompt_version="p2", allocations=w.reg.config("wf")["allocations"])
    with caplog.at_level(logging.INFO):
        assert w.mgr.tick() == []
        assert w.mgr.tick() == []                              # logged once per version
    assert caplog.text.count("continues across wf v3 (changed by supervisor") == 1
    rows = w.conn.execute("SELECT from_status, to_status, detail FROM experiment_log "
                          "WHERE experiment_id=? ORDER BY id", (exp_id,)).fetchall()
    assert [(r[0], r[1]) for r in rows][-1] == ("running", "running")
    assert json.loads(rows[-1][2])["changed_dims"] == ["allocations", "prompt_version"]


def test_proposed_config_never_dials_caller_id_with_open_incident(w):
    w.conn.execute(
        "INSERT INTO incidents(incident_id, kind, workflow_id, subject, status, detected_ts, "
        "evidence, mitigation) VALUES ('i1','caller_id_health','wf','c_bad','detected',"
        "'2027-01-04T10:00:00+05:30','{}','{}')")
    cfg = w.reg.config("wf")
    w.reg.conn.execute("UPDATE workflow_config_snapshots SET config=? WHERE workflow_id='wf'",
                       (json.dumps({**cfg, "allocations": {**cfg["allocations"], "default": {
                           **cfg["allocations"]["default"],
                           "caller_id": {"c_ok": 0.5, "c_bad": 0.5}}}}),))
    w.sim.workflows["wf"]["options"]["caller_id"] = ["c_ok", "c_bad"]
    w.reg.refresh("wf")
    design, _ = plan(w.conn, w.reg, "wf", POLICY)
    assert design.proposed_config["allocations"]["default"]["caller_id"] == {"c_ok": 1.0}


# ------------------------------------------------------------------ option (b): carryover guardrail

from controller.experiments import (  # noqa: E402
    CARRYOVER,
    FIRST_EXPOSURE,
    carryover_baseline,
    carryover_exposures,
    guardrail_unit,
)


def test_guardrail_unit_choice():
    assert guardrail_unit("pickup", "welcome_message") == CARRYOVER
    assert guardrail_unit("pickup", "voice") == CARRYOVER
    assert guardrail_unit("pickup", "caller_id") == FIRST_EXPOSURE      # acts before connect
    assert guardrail_unit("engagement", "voice") == FIRST_EXPOSURE


def _call(eid, person, hour, voice, connected, version=2, day=4):
    return make_call(eid, person, f"2027-01-{day:02d}T{hour:02d}:00:00+05:30", voice,
                     connected, version=version)


def test_carryover_counts_next_dial_after_first_connected_exposure(w):
    w.ingest([
        _call(60_001, "a", 9, "v1", False),          # before exposure: ignored
        _call(60_002, "a", 10, "v2", True),          # a exposed to v2
        _call(60_003, "a", 11, "v1", True),          # a's next dial connected -> v2: 1/1
        _call(60_004, "b", 10, "v1", True),          # b exposed to v1
        _call(60_005, "b", 12, "v2", False),         # next dial not connected -> v1: 0/1
        _call(60_006, "c", 10, "v1", True),          # c never re-dialled: no observation
        _call(60_007, "d", 10, "v2", False),         # d never connected: no exposure
        _call(60_008, "e", 10, "v2", True),          # e exposed to v2 ...
        _call(60_009, "e", 13, "v2", True, version=1),   # ... next dial on old config: ignored
    ])
    got = carryover_exposures(w.conn, "wf", "default", "voice", versions=[2],
                              start="2027-01-04T00:00:00+05:30")
    assert got == {"v2": Rate(1, 1), "v1": Rate(0, 1)}


def test_carryover_baseline_from_history(w):
    w.ingest([_call(61_001, "x", 9, "v1", True, day=3, version=1),
              _call(61_002, "x", 12, "v1", True, day=3, version=1),
              _call(61_003, "y", 9, "v1", True, day=3, version=1),
              _call(61_004, "y", 12, "v1", False, day=3, version=1)])
    # history fixture: h-persons are dialled once (no next dial); x, y add 1/2
    assert carryover_baseline(w.conn, "wf", "default", HIST_END) == Rate(1, 2)


def test_engagement_primary_plan_sizes_pickup_guardrail_on_carryover(tmp_path):
    w = World(tmp_path)
    w.sim.workflows["wf"]["objective"] = {"primary": "engagement_rate",
                                          "guardrail": {"pickup_rate_max_drop": 0.02}}
    w.reg.refresh("wf")
    p = ExperimentPolicy(dimensions=("voice",), min_history_units=50)
    design, reasons = plan(w.conn, w.reg, "wf", p)
    assert design is None
    # history fixture has no re-dialled people: no carryover baseline -> refuse, don't skip
    assert any("guardrail pickup (unit: next dial after a connected exposure) has a tolerance "
               "but no history observations" in r for r in reasons), reasons


def test_carryover_guardrail_sized_from_history_redials(tmp_path):
    w = World(tmp_path)
    w.sim.workflows["wf"]["objective"] = {"primary": "engagement_rate",
                                          "guardrail": {"pickup_rate_max_drop": 0.02}}
    w.reg.refresh("wf")
    redials = [_call(62_000 + i, f"h{i}", 15, "v1", i % 4 == 0, day=3, version=1)
               for i in range(2, 201, 2)]               # connected h-persons re-dialled
    w.ingest(redials)
    p = ExperimentPolicy(dimensions=("voice",), min_history_units=50)
    design, reasons = plan(w.conn, w.reg, "wf", p)
    assert design is None                                # 2pp NI needs thousands; 100 exist
    msg = next(r for r in reasons if "next dial after a connected exposure" in r)
    assert "non-inferiority needs" in msg and "window gives" in msg
