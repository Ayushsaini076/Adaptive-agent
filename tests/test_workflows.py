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
