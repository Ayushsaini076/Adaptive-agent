import json

import pytest

from controller import events as ev
from tests.fakes import call_event, lead_event


def test_call_row_extracts_variant_and_qa():
    row = ev.call_row(call_event(7, "L1", "c1"), 7)
    assert len(row) == 23
    assert row[0] == 7 and row[2] == "wf_a"
    assert row[11:15] == ("w1", "v1", "c1", "p1")   # variant dims
    assert row[15:17] == (1, 1)                    # connected, engaged
    assert row[20] == 0 and row[21] == 0.9         # hallucination_flag, flow_adherence


def test_unconnected_call_has_null_qa():
    row = ev.call_row(call_event(8, "L1", "c1", connected=False), 8)
    assert row[15:17] == (0, 0)
    assert row[20] is None and row[21] is None


def test_engaged_without_connected_is_invalid():
    raw = call_event(9, "L1", "c1")
    raw["connected"] = False
    with pytest.raises(ev.InvalidEvent, match="connected"):
        ev.call_row(raw, 9)


def test_call_requires_connected_flag():
    raw = call_event(9, "L1", "c1")
    del raw["connected"]
    with pytest.raises(ev.InvalidEvent):
        ev.call_row(raw, 9)


def test_lead_features_nested_or_top_level():
    nested = ev.lead_row(lead_event(1, "L1", "c1"), 1)
    flat_raw = {"lead_id": "L2", "lang": "en", "city_tier": 1}
    flat = ev.lead_row(flat_raw, 2)
    assert json.loads(nested[4])["lang"] == "hi"
    assert json.loads(flat[4]) == {"city_tier": 1, "lang": "en"}


def test_event_id_and_type_aliases():
    assert ev.event_id({"id": "12"}) == 12
    assert ev.event_type({"event_type": "call"}) == "call"
    with pytest.raises(ev.InvalidEvent):
        ev.event_id({"type": "call"})
    with pytest.raises(ev.InvalidEvent):
        ev.event_id({"event_id": "abc"})


def test_real_call_payload_maps_all_fields():
    """Shape observed on public_02_incidents (attempt is 'attempt_no')."""
    real = {"type": "call", "ts": "2027-01-13T20:45:00+05:30", "tenant_id": "kestrel_finance",
            "workflow_id": "ks_cancel", "campaign_id": "ks_cancel:c0024",
            "lead_id": "ks_cancel:l00035892", "person_id": "ks_cancel:p0025575",
            "attempt_no": 2, "campaign_day_index": 0, "config_version": 2,
            "cohort_id": "default",
            "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": "D1",
                        "prompt_version": "P2"},
            "connected": False, "engaged": False, "disposition": "no_answer",
            "duration_s": 0, "turns": 0, "qa": None, "event_id": 324324}
    row = ev.call_row(real, 324324)
    assert row[7] == 2 and row[8] == 0            # attempt, campaign_day
    assert row[11:15] == ("W1", "V1", "D1", "P2")
    assert row[19] == 0 and row[20] is None       # turns, no QA when not connected
