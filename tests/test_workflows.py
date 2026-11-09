import json

import pytest

from controller.database import connect
from controller.ingestion import Ingester
from controller.workflows import Objective, WorkflowRegistry
from tests.fakes import sample_simulator


@pytest.mark.parametrize("obj,expected", [
    ({"primary": "pickup", "guardrail": {"kpi": "engagement", "max_drop": 0.02}},
     ("pickup", "engagement", 0.02, False)),
    ({"primary": "engagement_rate", "guardrail": {"metric": "pickup", "max_drop_pp": 3}},
     ("engagement", "pickup", 0.03, False)),
    ({"primary": "pickup", "guardrail": {"kpi": "engagement", "max_relative_drop": 0.05}},
     ("pickup", "engagement", 0.05, True)),
    ({"primary": "engagement"}, ("engagement", "pickup", None, False)),
    ("pickup", ("pickup", "engagement", None, False)),
])
def test_objective_parsing(obj, expected):
    o = Objective.from_meta({"objective": obj})
    assert (o.primary, o.guardrail, o.guardrail_max_drop, o.guardrail_relative) == expected


@pytest.mark.parametrize("obj", [
    {"primary": "revenue"},
    {"primary": "pickup", "guardrail": {"kpi": "pickup"}},
])
def test_objective_rejects_bad_input(obj):
    with pytest.raises(ValueError):
        Objective.from_meta({"objective": obj})


@pytest.fixture
def sim():
    return sample_simulator()


@pytest.fixture
def reg(sim, tmp_path):
    return WorkflowRegistry(connect(tmp_path / "c.db"), sim.client())


def test_sync_all_snapshots_configs_and_cohorts(reg):
    assert reg.sync_all() == ["wf_a", "wf_b"]
    wf = reg.get("wf_a")
    assert wf.objective.primary == "pickup" and wf.mid_flight == "allowed"
    assert reg.current_version("wf_a") == 3
    assert reg.cohort_set("wf_a").ids == ["hi_t2", "default"]
    rows = reg.conn.execute("SELECT cohort_id, version FROM cohort_snapshots").fetchall()
    assert [tuple(r) for r in rows] == [("hi_t2", 3)]


def test_snapshots_are_immutable(sim, reg):
    reg.sync_all()
    sim.configs["wf_a"] = {**sim.configs["wf_a"], "prompt_version": "TAMPERED"}
    reg.refresh("wf_a")
    assert reg.config("wf_a", 3)["prompt_version"] == "p1"


def test_supervisor_change_refreshes_snapshot_and_keeps_old_version(sim, reg):
    reg.sync_all()
    event = sim.supervisor_edit("wf_a", prompt_version="p2",
                                cohorts=[{"cohort_id": "en", "where": {"lang": ["en"]}}])
    sim.event_chunks[10] = [event]
    Ingester(reg.conn, sim.client()).ingest_chunk(10, "events")

    assert reg.refresh_changed() == ["wf_a"]
    assert reg.current_version("wf_a") == 4
    assert reg.config("wf_a")["prompt_version"] == "p2"
    assert reg.cohort_set("wf_a", 3).ids == ["hi_t2", "default"]   # history preserved
    assert reg.cohort_set("wf_a", 4).ids == ["en", "default"]
    assert reg.refresh_changed() == []                              # already synced
    src = reg.conn.execute("SELECT config FROM workflow_config_snapshots WHERE version=4"
                           ).fetchone()[0]
    assert json.loads(src)["prompt_version"] == "p2"


# ---------------------------------------------------------------- F6: history = change records

import logging  # noqa: E402

from controller.workflows import history_config  # noqa: E402

REAL_HISTORY_ENTRY = {"actor": "business_default", "applied_at": None, "approval_id": None,
                      "changed_dims": [], "proposal_id": None, "version": 1}


def test_history_change_record_is_not_config():
    assert history_config(REAL_HISTORY_ENTRY) is None
    assert history_config({"version": 2, "config": {"prompt_version": "p"}}) == {
        "prompt_version": "p"}                          # content only; version is separate
    assert history_config({"version": 3, "allocations": {}, "actor": "x"}) == {"allocations": {}}


def test_real_history_shape_does_not_shadow_current_config(sim, reg, caplog):
    sim.config_history["wf_a"] = [{**REAL_HISTORY_ENTRY, "version": 3}]
    with caplog.at_level(logging.ERROR):
        reg.sync_all()
        reg.refresh("wf_a")
    assert "differs from the stored snapshot" not in caplog.text
    cfg = reg.config("wf_a", 3)
    assert cfg["prompt_version"] == "p1" and "actor" not in cfg
    assert reg.cohort_set("wf_a", 3).ids == ["hi_t2", "default"]
    row = reg.conn.execute("SELECT source, actor FROM workflow_config_snapshots "
                           "WHERE workflow_id='wf_a' AND version=3").fetchone()
    assert tuple(row) == ("api_current", "business_default")   # annotated, not replaced


def test_change_record_for_unseen_version_creates_no_snapshot(sim, reg):
    sim.config_history["wf_a"] = [{**REAL_HISTORY_ENTRY, "version": 1},
                                  {**REAL_HISTORY_ENTRY, "version": 3}]
    reg.sync_all()
    versions = [r[0] for r in reg.conn.execute(
        "SELECT version FROM workflow_config_snapshots WHERE workflow_id='wf_a'")]
    assert versions == [3]          # v1's content was never observed: not invented


def test_genuine_mismatch_logs_differing_keys(sim, reg, caplog):
    reg.sync_all()
    sim.configs["wf_a"] = {**sim.configs["wf_a"], "prompt_version": "TAMPERED"}
    with caplog.at_level(logging.ERROR):
        reg.refresh("wf_a")
    assert "keys ['prompt_version']" in caplog.text


# ---------------------------------------------------------------- F4: supervisor visibility


def test_supervisor_edit_is_logged_as_warning(sim, reg, caplog):
    reg.sync_all()
    sim.event_chunks[10] = [sim.supervisor_edit("wf_a", prompt_version="p_bad")]
    Ingester(reg.conn, sim.client()).ingest_chunk(10, "events")
    with caplog.at_level(logging.WARNING):
        reg.refresh_changed()
    assert "supervisor edit: wf_a v4 changed [\"prompt_version\"]" in caplog.text
    assert "not reverted" in caplog.text
    assert [c["actor"] for c in reg.last_changes] == ["supervisor"]
    reg.refresh_changed()
    assert reg.last_changes == []
    actor = reg.conn.execute("SELECT actor FROM workflow_config_snapshots "
                             "WHERE workflow_id='wf_a' AND version=4").fetchone()[0]
    assert actor == "supervisor"
