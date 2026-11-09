"""POST /approvals contract and failure handling.

Root cause being guarded: the controller stored the whole GET /config
response (metadata envelope) as the config, so proposed_config carried
workflow_id / version / actor / applied_at / config and the simulator
answered 422 on every tick.
"""

import json

from controller.workflows import split_config_response
from tests.test_governance import World, _config  # noqa: F401  (fixture world)

REQUIRED = ("proposal_id", "workflow_id", "base_version", "proposed_config", "change_summary",
            "diff", "affected_leads", "dry_run", "impact_score", "rationale")
METADATA = {"workflow_id", "version", "actor", "applied_at", "config"}


def posts(sim, path="/approvals"):
    return [r for r in sim.requests if r.method == "POST" and r.url.path == path]


# ------------------------------------------------------------------ extraction


def test_config_envelope_is_unwrapped(tmp_path):
    w = World(tmp_path)
    raw = w.sim.client().get_config("ks_cancel")
    assert set(raw) >= METADATA - {"config"} and "config" in raw          # real shape
    content, version = split_config_response(raw)
    assert version == 1 and "version" not in content
    assert w.reg.config("ks_cancel") == content                          # stored = inner only
    assert not METADATA & set(w.reg.config("ks_cancel"))
    assert w.reg.cohort_set("ks_cancel").config_version == 1


def test_flat_response_is_reduced_to_content_keys():
    content, version = split_config_response({"version": 4, "actor": "x", "workflow_id": "w",
                                              "prompt_version": "P1", "allocations": {}})
    assert content == {"prompt_version": "P1", "allocations": {}} and version == 4


# ------------------------------------------------------------------ payload contract


def test_payload_matches_simulator_contract(tmp_path):
    w = World(tmp_path)
    w.incident("ks_cancel")
    (pid, status), = w.gov.tick()
    assert status == "pending" and w.sim.invalid_requests == []           # reached simulator
    body = json.loads(posts(w.sim)[0].content)
    assert all(body.get(k) not in (None, "", [], {}) or k == "affected_leads" for k in REQUIRED)
    assert body["workflow_id"] == "ks_cancel" and body["base_version"] == 1
    assert isinstance(body["affected_leads"], int) and body["affected_leads"] >= 0
    assert 0 <= body["impact_score"] <= 1
    cfg = body["proposed_config"]
    assert set(cfg) <= {"cohorts", "allocations", "prompt_version", "schedule"}
    assert not METADATA & set(cfg)
    assert set(cfg["allocations"]["default"]) == {"welcome_message", "voice", "caller_id"}
    assert cfg["allocations"]["default"]["voice"] == {"V1": 1.0}            # untouched dims kept
    assert cfg["prompt_version"] == "P1"
    assert "validation_run_id" not in body.get("evidence", {})            # not a prompt change


def test_invalid_base_config_is_caught_locally_and_never_sent(tmp_path):
    w = World(tmp_path)
    del w.sim.configs["ks_cancel"]["allocations"]["default"]["voice"]     # broken live config
    w.sim.configs["ks_cancel"]["version"] = 2
    w.reg.refresh("ks_cancel")
    w.incident("ks_cancel")
    assert w.gov.tick() == []
    assert posts(w.sim) == []
    reason = json.loads(w.conn.execute("SELECT detail FROM audit_log WHERE "
                                       "action='proposal_deferred'").fetchone()[0])["reason"]
    assert "allocations.default.voice is required" in reason


# ------------------------------------------------------------------ 422 / 403 / lost reply


def test_422_is_permanent_and_not_resubmitted(tmp_path, caplog):
    w = World(tmp_path)
    w.sim.normalise_errors = lambda wf, cfg: ["simulated validation failure"]
    w.incident("ks_cancel")
    assert [s for _, s in w.gov.tick()] == ["invalid"]
    for _ in range(3):
        assert w.gov.tick() == []
    assert len(posts(w.sim)) == 1                                         # sent exactly once
    row = w.proposals()[0]
    assert row["status"] == "invalid"
    assert "simulated validation failure" in row["reasons"]
    assert "REJECTED AS INVALID" in caplog.text


def test_422_allows_a_new_attempt_once_the_change_differs(tmp_path):
    w = World(tmp_path)
    real = w.sim.normalise_errors
    w.sim.normalise_errors = lambda wf, cfg: ["temporary"]
    w.incident("ks_cancel")
    w.gov.tick()
    w.sim.normalise_errors = real
    w.sim.pending_events.append(w.sim.supervisor_edit("ks_cancel", prompt_version="P2"))
    w.sync_events()                                                       # base config moved
    (pid2, status), = w.gov.tick()
    assert status == "pending" and len(posts(w.sim)) == 2


def test_lost_reply_keeps_proposal_identity_and_adopts_remote_record(tmp_path):
    w = World(tmp_path)
    w.sim.fail_next_submit = 1                                            # stored, reply lost
    w.incident("ks_cancel")
    (pid, status), = w.gov.tick()
    assert status == "unconfirmed" and len(w.sim.approvals) == 1
    changes = w.gov.tick()
    assert (pid, "pending") in changes                                    # adopted, same id
    assert len(w.sim.approvals) == 1 and len(posts(w.sim)) == 1           # no duplicate
    assert w.proposals()[0]["approval_id"] == "apr_1"


def test_403_on_auto_apply_falls_back_to_approval(tmp_path):
    w = World(tmp_path)
    # Simulator refuses although the controller's policy snapshot allows it.
    w.sim.workflows["nw_renewal"]["governance"]["auto_apply"]["enabled"] = False
    w.incident("nw_renewal")
    assert [s for _, s in w.gov.tick()] == ["refused"]
    (pid2, status), = w.gov.tick()
    assert status == "pending" and len(w.sim.puts) == 1                   # PUT tried once


# ------------------------------------------------------------------ lifecycle


def test_approval_application_updates_controller_state(tmp_path):
    w = World(tmp_path)
    w.incident("ks_cancel")
    (pid, _), = w.gov.tick()
    w.sim.decide_after_minutes = 0
    assert w.gov.tick() == [(pid, "approved"), (pid, "applied")]
    apply_req = [r for r in w.sim.requests if r.url.path == "/workflows/ks_cancel/apply"]
    assert json.loads(apply_req[0].content) == {"approval_id": "apr_1"}
    w.sync_events()
    assert w.reg.current_version("ks_cancel") == 2
    change = w.conn.execute("SELECT actor, proposal_id, approval_id FROM config_changes "
                            "WHERE workflow_id='ks_cancel'").fetchone()
    assert tuple(change) == ("controller_approved", pid, "apr_1")
    statuses = [r[0] for r in w.conn.execute(
        "SELECT status FROM approval_decisions WHERE approval_id='apr_1' ORDER BY event_id")]
    assert statuses == ["approved", "applied"]                            # decided + applied
    assert w.reg.config("ks_cancel")["allocations"]["default"]["caller_id"] == {"D1": 1.0}


def test_stale_proposal_never_overwrites_supervisor_edit(tmp_path):
    w = World(tmp_path)
    w.incident("ks_cancel")
    (pid1, _), = w.gov.tick()
    w.sim.pending_events.append(w.sim.supervisor_edit("ks_cancel", prompt_version="P3"))
    w.sync_events()
    w.sim.decide_after_minutes = 0
    changes = w.gov.tick()
    assert (pid1, "stale") in changes and w.sim.applies == []             # old payload refused
    assert w.sim.configs["ks_cancel"]["prompt_version"] == "P3"
    new = json.loads(w.proposals()[-1]["payload"])
    assert new["base_version"] == 2 and new["proposed_config"]["prompt_version"] == "P3"
    assert new["proposal_id"] != pid1
