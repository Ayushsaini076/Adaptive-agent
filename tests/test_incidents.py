"""C5 caller-ID health, replaying the pattern seen in public_02_incidents:
ks_cancel D2 pickup fell from ~0.25 to ~0.09 while D1 stayed at ~0.25."""

import json
import logging

import pytest

from controller.database import connect
from controller.incidents import CallerIdMonitor, open_caller_id_incidents, without_caller_ids
from controller.ingestion import Ingester
from controller.workflows import WorkflowRegistry
from tests.fakes import FakeSimulator


def dial(eid, person, hour, cid, connected, day=6):
    return {"event_id": eid, "type": "call", "ts": f"2027-01-{day:02d}T{hour:02d}:10:00+05:30",
            "workflow_id": "ks", "campaign_id": "c", "lead_id": f"L{person}",
            "person_id": person, "attempt_no": 1, "config_version": 1, "cohort_id": "default",
            "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": cid,
                        "prompt_version": "P1"},
            "connected": connected, "engaged": False,
            "disposition": "short_hangup" if connected else "no_answer"}


class World:
    def __init__(self, tmp_path):
        self.sim = FakeSimulator(
            workflows={"ks": {"tenant_id": "kestrel", "mid_flight": "allowed",
                              "objective": {"primary": "engagement",
                                            "guardrail": {"kpi": "pickup", "max_drop": 0.03}},
                              "options": {"caller_id": ["D1", "D2", "D3"]}}},
            configs={"ks": {"version": 1, "cohorts": [],
                            "allocations": {"default": {"caller_id": {"D1": 0.5, "D2": 0.5}}}}})
        self.sim.next_event_id = 900_000
        self.conn = connect(tmp_path / "c.db")
        self.reg = WorkflowRegistry(self.conn, self.sim.client())
        self.reg.sync_all()
        self.ing = Ingester(self.conn, self.sim.client())
        self.mon = CallerIdMonitor(self.conn, self.reg)
        self.chunk, self.eid = 0, 1

    def hour(self, hour, d2_rate, *, n=100, d1_rate=0.25, day=6):
        """n dials per caller ID in this hour, pickup at the given rates."""
        calls = []
        for cid, rate in (("D1", d1_rate), ("D2", d2_rate)):
            for i in range(n):
                calls.append(dial(self.eid, f"{cid}-{day}-{hour}-{i}", hour, cid,
                                  (i * rate) % 1 + rate >= 1, day))
                self.eid += 1
        self.ingest(calls)
        return self.mon.tick()

    def ingest(self, events):
        self.chunk += 1
        self.sim.event_chunks[self.chunk] = events
        self.ing.ingest_chunk(self.chunk, "events")
        self.reg.refresh_changed()

    def incidents(self):
        return [dict(r) for r in self.conn.execute("SELECT * FROM incidents")]


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def test_d2_collapse_detected_after_sustained_breach(w, caplog):
    for h in range(9, 15):                       # healthy morning
        assert w.hour(h, 0.25) == []
    detected_at = []
    with caplog.at_level(logging.INFO):
        for h in range(15, 23):                  # onset at 15:00
            if w.hour(h, 0.09):
                detected_at.append(h)
    # The 6-hour window is still mostly healthy right after onset: the breach
    # clears the thresholds at 18:00 and is confirmed on the 3rd check, 20:00.
    assert detected_at == [20]
    assert "breach started" in caplog.text and "CALLER-ID INCIDENT" in caplog.text

    inc = w.incidents()[0]
    assert (inc["workflow_id"], inc["subject"], inc["status"]) == ("ks", "D2", "detected")
    ev = json.loads(inc["evidence"])
    assert ev["reference"] == "peers" and ev["z"] <= -4 and ev["relative_drop"] >= 0.3
    mit = json.loads(inc["mitigation"])
    assert mit["proposed_config"]["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert mit["tv_shift_by_cohort"] == {"default": 0.5}
    assert mit["status"] == "awaiting governance"           # never applied by this module
    assert w.reg.config("ks")["allocations"]["default"]["caller_id"] == {"D1": 0.5, "D2": 0.5}
    log_rows = w.conn.execute("SELECT to_status FROM incident_log").fetchall()
    assert [r[0] for r in log_rows] == ["detected"]


def test_not_reopened_while_open(w):
    for h in range(9, 15):
        w.hour(h, 0.25)
    for h in range(15, 23):
        w.hour(h, 0.09)
    assert len(w.incidents()) == 1


def test_small_sample_never_triggers(w):
    for h in range(9, 20):
        assert w.hour(h, 0.0, n=20) == []       # 0% pickup but only 20 dials/hour
    assert w.incidents() == []


def test_one_bad_hour_does_not_trigger(w):
    for h in range(9, 15):
        w.hour(h, 0.25)
    w.hour(15, 0.0)                             # a single terrible hour
    for h in range(16, 23):
        assert w.hour(h, 0.25) == []
    assert w.incidents() == []


def test_mild_dip_below_relative_threshold_does_not_trigger(w):
    for h in range(9, 23):
        assert w.hour(h, 0.21, n=400) == []     # -16%, significant at n=400 but small
    assert w.incidents() == []


def test_removing_caller_id_marks_incident_mitigated(w, caplog):
    for h in range(9, 15):
        w.hour(h, 0.25)
    for h in range(15, 22):
        w.hour(h, 0.09)
    assert open_caller_id_incidents(w.conn, "ks") == {"D2"}
    w.ingest([w.sim.supervisor_edit("ks", allocations={"default": {"caller_id": {"D1": 1.0}}})])
    with caplog.at_level(logging.WARNING):
        changes = w.mon.tick()
    assert changes and changes[0][1] == "mitigated"
    assert "mitigated: D2 no longer allocated" in caplog.text and "supervisor" in caplog.text
    assert open_caller_id_incidents(w.conn, "ks") == set()
    detail = json.loads(w.conn.execute(
        "SELECT detail FROM incident_log WHERE to_status='mitigated'").fetchone()[0])
    assert detail == {"config_version": 2, "by": "supervisor"}


# ---------------------------------------------------------------- mitigation config


def test_without_caller_ids_renormalises_and_handles_cohorts():
    cfg = {"allocations": {"default": {"caller_id": {"D1": 0.5, "D2": 0.5}},
                           "vip": {"caller_id": {"D2": 1.0}},
                           "inherits": {"voice": {"V1": 1.0}}}}
    new, shifts = without_caller_ids(cfg, {"D2"}, ["D1", "D2", "D3"])
    assert new["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert new["allocations"]["vip"]["caller_id"] == {"D1": 0.5, "D3": 0.5}   # allowed spares
    assert "caller_id" not in new["allocations"]["inherits"]                 # inherits default
    assert shifts == {"default": 0.5, "vip": 1.0}
    assert cfg["allocations"]["vip"]["caller_id"] == {"D2": 1.0}             # not mutated


def test_without_caller_ids_refuses_when_nothing_healthy():
    with pytest.raises(ValueError, match="no healthy caller ID"):
        without_caller_ids({"allocations": {"default": {"caller_id": {"D2": 1}}}}, {"D2"}, ["D2"])



def test_overnight_gap_neither_clears_nor_confirms_breach(w):
    """Dialling stops overnight: thin morning windows are 'unknown', not healthy."""
    for h in range(9, 13):
        w.hour(h, 0.25)
    for h in range(13, 18):                       # breach starts at 16, 2nd check at 17
        assert w.hour(h, 0.09) == []
    changes = w.hour(9, 0.09, day=7)              # next morning: 3rd consecutive check
    assert len(changes) == 1 and w.incidents()[0]["detected_ts"].startswith("2027-01-07T09")
