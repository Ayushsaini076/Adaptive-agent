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
