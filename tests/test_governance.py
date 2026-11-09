"""Governance (M5) against the three real workflow policies of public_02_incidents.

Metadata below reproduces the policy values reported from the live simulator;
key names follow the spec contract. Nothing in controller code refers to these
workflow ids or values: they are read from metadata.
"""

import json
import logging
from datetime import datetime

import pytest

from controller import experiments as ex
from controller.database import connect, set_state, transaction
from controller.governance import GovernanceManager
from controller.incidents import CallerIdMonitor
from controller.ingestion import Ingester
from controller.policy import GovernancePolicy, load_preferences
from controller.workflows import WorkflowRegistry
from tests.fakes import FakeSimulator


def _gov(enabled, dims, shift, hours, latency, max_req, ttl, window=2):
    return {"auto_apply": {"enabled": enabled, "allowed_dims": dims, "max_shift": shift},
            "approver_hours": hours, "approver_latency_minutes": latency,
            "base_reject_rate": 0.1, "max_requests_per_24h": max_req,
            "min_validation_pass_rate": 0.97, "proposal_ttl_hours": ttl,
            "apply_window_hours": window}


META = {
    "ks_cancel": {
        "tenant_id": "kestrel_finance", "mid_flight": "allowed",
        "objective": {"primary": "engagement_rate", "guardrail": {"pickup_rate_max_drop": 0.02}},
        "governance": _gov(True, ["schedule", "caller_id", "welcome_message", "voice"], 0.2,
                           [10, 19], [20, 120], 4, 12),
        "options": {"caller_id": ["D1", "D2", "D3"],
                    "prompt_version": ["P1", "P2", "P3", "P4", "P5"],
                    "voice": ["V1", "V2", "V3", "V4"], "welcome_message": ["W1", "W2", "W3"]}},
    "nw_kyc": {
        "tenant_id": "northwind", "mid_flight": "next_campaign_only",
        "objective": {"primary": "pickup_rate", "guardrail": {"engagement_rate_max_drop": 0.02}},
        "governance": _gov(False, [], 0.0, [10, 18], [60, 480], 2, 24),
        "options": {"caller_id": ["D1", "D2", "D3"], "prompt_version": ["P1", "P3"],
                    "voice": ["V1", "V2"], "welcome_message": ["W1", "W2"]}},
    "nw_renewal": {
        "tenant_id": "northwind", "mid_flight": "allowed",
        "objective": {"primary": "engagement_rate", "guardrail": {"pickup_rate_max_drop": 0.02}},
        "governance": _gov(True, ["schedule", "caller_id"], 0.5, [10, 19], [30, 240], 3, 24),
        "options": {"caller_id": ["D1", "D2", "D3", "D4"],
                    "prompt_version": ["P1", "P3", "P4", "P5"],
                    "voice": ["V1", "V2", "V3", "V4"], "welcome_message": ["W1", "W2", "W3"]}},
}


def _config():
    return {"version": 1, "cohorts": [], "prompt_version": "P1",
            "allocations": {"default": {"caller_id": {"D1": 0.5, "D2": 0.5},
                                        "voice": {"V1": 1.0}, "welcome_message": {"W1": 1.0}}}}


class World:
    def __init__(self, tmp_path, prefs=None):
        self.sim = FakeSimulator(workflows=json.loads(json.dumps(META)),
                                 configs={w: _config() for w in META})
        self.sim.next_event_id = 500_000
        self.conn = connect(tmp_path / "c.db")
        self.reg = WorkflowRegistry(self.conn, self.sim.client())
        self.reg.sync_all()
        self.ing = Ingester(self.conn, self.sim.client())
        self.gov = GovernanceManager(self.conn, self.sim.client(), self.reg, prefs or {})
        self.clock("2027-01-06T12:00:00")

    def clock(self, ts):
        ts = ts if "+" in ts else ts + "+05:30"
        with transaction(self.conn):
            set_state(self.conn, "sim_time", ts)
        self.sim.minute = int((datetime.fromisoformat(ts) - datetime.fromisoformat(
            "2027-01-04T00:00:00+05:30")).total_seconds() // 60)

    def incident(self, wf, cid="D2", ts="2027-01-06T08:00:00+05:30"):
        ev = {"caller_id": {"rate": 0.09, "den": 700}, "reference": "peers",
              "reference_rate": {"rate": 0.25, "den": 700}, "z": -6.6, "relative_drop": 0.64,
              "breach_since": ts}
        self.conn.execute(
            "INSERT INTO incidents(incident_id, kind, workflow_id, subject, status, detected_ts, "
            "evidence, mitigation) VALUES (?,?,?,?,?,?,?,?)",
            (f"inc:{wf}:{cid}", "caller_id_health", wf, cid, "detected", ts, json.dumps(ev), "{}"))
        return f"inc:{wf}:{cid}"

    def sync_events(self):
        n = self.sim.flush_events()
        if n:
            self.ing.ingest_chunk(n, "events")
            self.reg.refresh_changed()

    def proposals(self, wf=None):
        q = "SELECT * FROM proposals" + (" WHERE workflow_id=?" if wf else "") + " ORDER BY rowid"
        return [dict(r) for r in self.conn.execute(q, (wf,) if wf else ())]

    def audit(self, ref=None):
        q = "SELECT action FROM audit_log" + (" WHERE ref=?" if ref else "") + " ORDER BY id"
        return [r[0] for r in self.conn.execute(q, (ref,) if ref else ())]


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def policy(w, wf):
    return w.gov.policy(wf)


# ------------------------------------------------------------------ 1-2: objectives


@pytest.mark.parametrize("wf,primary,guard", [("ks_cancel", "engagement", "pickup"),
                                              ("nw_kyc", "pickup", "engagement"),
                                              ("nw_renewal", "engagement", "pickup")])
def test_objective_read_per_workflow_as_absolute_pp(w, wf, primary, guard):
    o = w.reg.get(wf).objective
    assert (o.primary, o.guardrail, o.guardrail_max_drop, o.guardrail_relative) == (
        primary, guard, 0.02, False)


# ------------------------------------------------------------------ policy parsing


def test_policies_parsed_from_metadata(w):
    ks, kyc, ren = (policy(w, x) for x in ("ks_cancel", "nw_kyc", "nw_renewal"))
    assert ks.auto_apply_enabled and ks.max_shift == 0.2
    assert ks.allowed_dims == {"schedule", "caller_id", "welcome_message", "voice"}
    assert (ks.approver_hours, ks.max_requests_per_24h, ks.proposal_ttl_hours) == ((10, 19), 4, 12)
    assert not kyc.auto_apply_enabled and kyc.allowed_dims == frozenset()
    assert (kyc.approver_latency_minutes, kyc.max_requests_per_24h) == ((60, 480), 2)
    assert ren.allowed_dims == {"schedule", "caller_id"} and ren.max_shift == 0.5
    assert all(p.min_validation_pass_rate == 0.97 for p in (ks, kyc, ren))


def test_unreadable_policy_fails_safe():
    p = GovernancePolicy.from_meta({"workflow_id": "x", "governance": {
        "auto_apply": "yes please", "max_requests_per_24h": "lots"}})
    assert not p.auto_apply_enabled and p.max_requests_per_24h == 1


# ------------------------------------------------------------------ 3-5, 7: auto-apply


def _with(cfg, **alloc_changes):
    new = json.loads(json.dumps(cfg))
    for dim, dist in alloc_changes.items():
        new["allocations"]["default"][dim] = dist
    return new


@pytest.mark.parametrize("wf,change,dims,ok", [
    ("ks_cancel", {"caller_id": {"D1": 0.7, "D2": 0.3}}, ["caller_id"], True),     # shift 0.2
    ("ks_cancel", {"caller_id": {"D1": 1.0}}, ["caller_id"], False),               # shift 0.5
    ("ks_cancel", {"welcome_message": {"W1": 0.8, "W2": 0.2}}, ["welcome_message"], True),
    ("ks_cancel", {}, ["prompt_version"], False),                                  # never auto
    ("nw_renewal", {"caller_id": {"D1": 1.0}}, ["caller_id"], True),               # 0.5 <= 0.5
    ("nw_renewal", {"voice": {"V1": 0.9, "V2": 0.1}}, ["voice"], False),           # dim not allowed
    ("nw_kyc", {"caller_id": {"D1": 0.6, "D2": 0.4}}, ["caller_id"], False),       # disabled
])
def test_auto_apply_eligibility(w, wf, change, dims, ok):
    cur = w.reg.config(wf)
    new = _with(cur, **change)
    if "prompt_version" in dims:
        new["prompt_version"] = "P2"
    allowed, reasons = policy(w, wf).auto_apply_check(cur, new, dims)
    assert allowed is ok, reasons


def test_dimension_available_but_not_auto_eligible_is_still_proposable(w):
    """nw_renewal voice: in options (proposable) but not auto-apply eligible."""
    cur = w.reg.config("nw_renewal")
    new = _with(cur, voice={"V1": 0.5, "V2": 0.5})
    assert w.gov.validate("nw_renewal", cur, new, ["voice"], policy(w, "nw_renewal")) == []
    assert not policy(w, "nw_renewal").auto_apply_check(cur, new, ["voice"])[0]


# ------------------------------------------------------------------ 8: timing and limits


@pytest.mark.parametrize("wf,hhmm,sent,ok,why", [
    ("ks_cancel", "11:00", 0, True, "ok"),
    ("ks_cancel", "18:00", 0, False, "approver minutes"),     # 60 < 120 before the 12h TTL
    ("ks_cancel", "21:00", 0, False, "approver minutes"),     # TTL ends 09:00, before hours
    ("ks_cancel", "11:00", 4, False, "request limit"),
    ("nw_kyc", "20:00", 0, True, "ok"),                       # 24h TTL covers next day 10-18
    ("nw_kyc", "11:00", 2, False, "request limit"),
    ("nw_renewal", "19:30", 0, True, "ok"),
])
def test_submission_timing(w, wf, hhmm, sent, ok, why):
    allowed, reason = policy(w, wf).submission_check(
        datetime.fromisoformat(f"2027-01-06T{hhmm}:00+05:30"), sent)
    assert allowed is ok and why in reason


def test_operator_preferences_only_tighten(tmp_path):
    f = tmp_path / "prefs.json"
    f.write_text(json.dumps({"_note": "from the Kestrel supervisor memo",
                             "kestrel_finance": {"max_requests_per_24h": 2,
                                                 "earliest_submit_hour": 11},
                             "northwind": {"max_requests_per_24h": 10}}))
    prefs = load_preferences(f)
    ks = GovernancePolicy.from_meta(META["ks_cancel"], prefs)
    kyc = GovernancePolicy.from_meta(META["nw_kyc"], prefs)
    assert ks.max_requests_per_24h == 2 and ks.earliest_submit_hour == 11
    assert kyc.max_requests_per_24h == 2                       # 10 > 2: not loosened
    ok, why = ks.submission_check(datetime.fromisoformat("2027-01-06T10:30:00+05:30"), 0)
    assert not ok and "11:00" in why


# ------------------------------------------------------------------ lifecycle: approval path


def test_ks_incident_mitigation_approval_lifecycle(w, caplog):
    inc = w.incident("ks_cancel")
    w.clock("2027-01-06T21:00:00")                     # outside the sendable window
    assert w.gov.tick() == []
    assert w.proposals() == [] and "proposal_deferred" in w.audit()

    w.clock("2027-01-07T11:00:00")
    with caplog.at_level(logging.WARNING):
        (pid, status), = w.gov.tick()
    assert status == "pending" and "submitted for approval" in caplog.text
    p = w.proposals()[0]
    assert p["route"] == "approval" and p["base_version"] == 1
    body = json.loads(p["payload"])
    assert body["proposed_config"]["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert body["diff"][0]["path"] == "allocations.default.caller_id"
    assert body["diff"][0]["tv_shift"] == 0.5
    assert 0 < body["impact_score"] <= 1 and body["affected_leads"] >= 0
    assert body["dry_run"]["auto_apply_ineligible_because"] == [
        "default/caller_id shift 0.500 > max_shift 0.2"]
    assert w.sim.puts == []                                  # never auto-applied

    w.sim.decide_after_minutes = 0
    assert w.gov.tick() == [(pid, "approved"), (pid, "applied")]
    assert w.sim.configs["ks_cancel"]["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    w.sync_events()
    CallerIdMonitor(w.conn, w.reg).tick()
    status, = w.conn.execute("SELECT status FROM incidents WHERE incident_id=?", (inc,)).fetchone()
    assert status == "mitigated"
    by = json.loads(w.conn.execute("SELECT detail FROM incident_log WHERE incident_id=? AND "
                                   "to_status='mitigated'", (inc,)).fetchone()[0])["by"]
    assert by == "controller_approved"
    assert w.audit(pid) == ["proposal_submitted", "proposal_approved", "proposal_applied"]
    assert w.gov.tick() == [] and len(w.sim.applies) == 1     # no double apply


def test_nw_renewal_small_enough_shift_is_auto_applied(w):
    w.incident("nw_renewal")
    (pid, status), = w.gov.tick()
    assert status == "auto_applied" and len(w.sim.puts) == 1 and w.sim.approvals == {}
    assert w.sim.configs["nw_renewal"]["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert w.audit(pid) == ["proposal_auto_applied"]


def test_nw_kyc_never_auto_applies(w):
    w.incident("nw_kyc")
    (pid, status), = w.gov.tick()
    assert status == "pending" and w.sim.puts == []
    body = json.loads(w.proposals("nw_kyc")[0]["payload"])
    assert body["dry_run"]["auto_apply_ineligible_because"][0] == "auto-apply disabled for this workflow"
    assert "campaigns created after the change" in body["dry_run"]["timing"]   # next_campaign_only


# ------------------------------------------------------------------ stale / lapsed / expired / rejected


def test_stale_base_is_reproposed_with_new_id_and_fresh_base(w):
    w.incident("ks_cancel")
    (pid1, _), = w.gov.tick()
    w.sim.pending_events.append(w.sim.supervisor_edit("ks_cancel", prompt_version="P2"))
    w.sim.decide_after_minutes = 0
    w.sync_events()                                             # supervisor event ingested
    changes = w.gov.tick()
    # Same tick: stale -> config refreshed -> intent re-evaluated -> NEW proposal.
    assert changes[:2] == [(pid1, "approved"), (pid1, "stale")]
    pid2, status = changes[2]
    assert pid2 != pid1 and status == "pending"
    p2 = w.proposals()[1]
    assert p2["base_version"] == 2
    assert json.loads(p2["payload"])["proposed_config"]["prompt_version"] == "P2"  # supervisor kept


def test_lapsed_approval_is_not_applied_and_reproposed(w):
    w.incident("ks_cancel")
    (pid1, _), = w.gov.tick()
    w.sim.decide_after_minutes, w.sim.apply_window_minutes = 0, 0
    w.sim._decide_approvals()                                   # approved, apply_by = now
    w.sim.minute += 15                                          # window missed
    changes = w.gov.tick()
    assert changes[0] == (pid1, "lapsed") and w.sim.applies == []
    pid2, status = changes[1]                                   # re-proposed, new id
    assert pid2 != pid1 and status == "pending"


def test_expired_is_distinct_from_rejected_and_retried(w):
    w.incident("ks_cancel")
    (pid1, _), = w.gov.tick()
    w.sim.decide_after_minutes, w.sim.approval_decision = 0, "expired"
    changes = w.gov.tick()
    assert changes[0] == (pid1, "expired")
    assert w.proposals()[0]["status"] == "expired"             # not "rejected"
    pid2, _ = changes[1]                                        # expired is retried
    assert pid2 != pid1


def _planned_experiment(w, wf="ks_cancel", dim="welcome_message"):
    d = ex.Design(experiment_id=f"exp:{wf}:{dim}", workflow_id=wf, cohort_id="default",
                  dimension=dim, control="W1", candidates=["W2"], primary_kpi="engagement",
                  guardrail_kpi="pickup", n_per_arm=100, n_guardrail_per_arm=0,
                  horizon_hours=100, base_version=1, allocation={"W1": 0.5, "W2": 0.5},
                  proposed_config={}, evidence={"sizing": {"design_mde": 0.05}})
    with transaction(w.conn):
        ex.ExperimentStore(w.conn).create(d, ex.ExperimentPolicy(), w.reg.get(wf),
                                          "2027-01-06T10:00:00+05:30")
    return d.experiment_id


def test_rejected_experiment_golive_cancels_experiment(w):
    exp_id = _planned_experiment(w)
    (pid, _), = w.gov.tick()
    assert json.loads(w.proposals()[0]["intents"]) == [{"kind": "experiment_golive", "ref": exp_id}]
    w.sim.decide_after_minutes, w.sim.approval_decision = 0, "rejected"
    assert w.gov.tick() == [(pid, "rejected")]
    assert ex.ExperimentStore(w.conn).get(exp_id)["status"] == "cancelled"
    assert w.gov.tick() == []                                    # not re-proposed


def test_compatible_changes_batched_into_one_request(w):
    w.incident("ks_cancel")
    _planned_experiment(w)
    (pid, _), = w.gov.tick()
    p = w.proposals()[0]
    assert {i["kind"] for i in json.loads(p["intents"])} == {"incident_mitigation",
                                                             "experiment_golive"}
    cfg = json.loads(p["payload"])["proposed_config"]["allocations"]["default"]
    assert cfg["caller_id"] == {"D1": 1.0} and cfg["welcome_message"] == {"W1": 0.5, "W2": 0.5}
    assert w.gov.tick() == [] or len(w.proposals()) == 1         # one in flight per workflow


# ------------------------------------------------------------------ 9: supervisor authority


def test_supervisor_readding_caller_id_escalates_instead_of_overriding(w, caplog):
    inc = w.incident("ks_cancel")
    w.sim.pending_events.append(w.sim.supervisor_edit("ks_cancel", allocations={"default": {
        "caller_id": {"D1": 0.3, "D2": 0.7}, "voice": {"V1": 1.0}, "welcome_message": {"W1": 1.0}}}))
    w.sync_events()
    with caplog.at_level(logging.WARNING):
        assert w.gov.tick() == []
    assert w.proposals() == [] and w.sim.puts == []
    assert "supervisor edits are authoritative" in caplog.text
    status, = w.conn.execute("SELECT status FROM incidents WHERE incident_id=?", (inc,)).fetchone()
    assert status == "escalated"
    assert "intent_escalated" in w.audit()


def test_rollback_restores_only_experiment_dimension(w, monkeypatch):
    exp_id = _planned_experiment(w, dim="voice")
    w.conn.execute("UPDATE experiments SET status='promoted', control='V1', "
                   "design=?, decision=?, promoted_ts=?, promoted_version=1 WHERE experiment_id=?",
                   (json.dumps({"current_distribution": {"V1": 1.0}}),
                    json.dumps({"winner": "V2", "recommendation": {"allocation": {"V2": 0.9,
                                                                                 "V1": 0.1}}}),
                    "2027-01-06T10:00:00+05:30", exp_id))
    w.sim.configs["ks_cancel"]["allocations"]["default"]["voice"] = {"V2": 0.9, "V1": 0.1}
    w.sim.pending_events.append(                                # supervisor edit after promotion
        w.sim.supervisor_edit("ks_cancel", prompt_version="P3"))
    w.sync_events()
    monkeypatch.setattr(ex, "promotion_harm", lambda *a, **k: {
        "kpi": "engagement", "reason": "winner worse than holdout, z=-3.4",
        "comparison": {"diff": -0.04}})
    w.gov.tick()
    body = json.loads(w.proposals()[0]["payload"])
    assert body["proposed_config"]["allocations"]["default"]["voice"] == {"V1": 1.0}
    assert body["proposed_config"]["prompt_version"] == "P3"   # supervisor edit preserved
    assert [d["path"] for d in body["diff"]] == ["allocations.default.voice"]


# ------------------------------------------------------------------ 10 + validation negatives


def test_flagged_or_unknown_options_and_unvalidated_prompts_rejected(w):
    w.incident("ks_cancel")
    cur, p = w.reg.config("ks_cancel"), policy(w, "ks_cancel")
    flagged = _with(cur, caller_id={"D2": 1.0})
    assert any("flagged" in x for x in w.gov.validate("ks_cancel", cur, flagged,
                                                       ["caller_id"], p))
    unknown = _with(cur, caller_id={"D9": 1.0})
    assert any("not in options" in x for x in w.gov.validate("ks_cancel", cur, unknown,
                                                              ["caller_id"], p))
    cur = _with(cur, caller_id={"D1": 1.0})                     # D2 already removed
    prompt = {**cur, "prompt_version": "P2"}
    assert w.gov.validate("ks_cancel", cur, prompt, ["prompt_version"], p) == [
        "prompt change without validation evidence"]
    low = {"validation": {"validation_run_id": "v1", "pass_rate": 0.95}}
    assert "< 0.97" in w.gov.validate("ks_cancel", cur, prompt, ["prompt_version"], p, low)[0]
    good = {"validation": {"validation_run_id": "v1", "pass_rate": 0.98}}
    assert w.gov.validate("ks_cancel", cur, prompt, ["prompt_version"], p, good) == []
    bad_prompt = {**cur, "prompt_version": "P2"}
    assert any("not in options" in x for x in w.gov.validate(
        "nw_kyc", w.reg.config("nw_kyc"), bad_prompt, ["prompt_version"], policy(w, "nw_kyc"),
        good))


# ------------------------------------------------------------------ restart with pending approval


def test_restart_with_pending_approval_applies_exactly_once(w):
    w.incident("ks_cancel")
    (pid, _), = w.gov.tick()
    w.sim.decide_after_minutes = 0
    restarted = GovernanceManager(w.conn, w.sim.client(), w.reg, {})   # new process
    assert restarted.tick() == [(pid, "approved"), (pid, "applied")]
    again = GovernanceManager(w.conn, w.sim.client(), w.reg, {})
    assert again.tick() == []
    assert w.sim.applies == [w.proposals()[0]["approval_id"]]


def test_duplicate_submission_is_recovered_not_resent(w):
    w.incident("ks_cancel")
    (pid, _), = w.gov.tick()
    payload = json.loads(w.proposals()[0]["payload"])
    with pytest.raises(Exception):
        w.sim.client().submit_approval(payload)                  # same id -> 409 duplicate
    assert len(w.sim.approvals) == 1



def test_config_moved_without_ingested_event_waits(w):
    """Race guard: never build on a config change whose author isn't known yet."""
    w.incident("ks_cancel")
    w.sim.supervisor_edit("ks_cancel", prompt_version="P2")     # event NOT delivered yet
    assert w.gov.tick() == [] and w.proposals() == []
    assert "proposal_deferred" in w.audit()
