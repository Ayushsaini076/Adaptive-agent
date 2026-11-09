import json

import httpx
import pytest

from controller.client import (
    Conflict,
    InsufficientCredits,
    Lapsed,
    NotFound,
    ScenarioFinished,
    SimulatorClient,
    SimulatorError,
    SimulatorUnavailable,
    StaleBaseVersion,
    _raise_for_response,
)
from tests.fakes import FakeSimulator, sample_simulator


@pytest.fixture
def sim() -> FakeSimulator:
    return sample_simulator()


# ---------------------------------------------------------------- discovery


def test_health_and_state(sim):
    with sim.client() as c:
        assert c.health() == {"status": "ok"}
        st = c.state()
    assert st["ts"] == "2027-01-04T00:00:00+05:30" and st["sim_minute"] == 0
    assert st["credits_remaining"] == 100


def test_list_workflows_and_details(sim):
    with sim.client() as c:
        ids = [w["workflow_id"] for w in c.list_workflows()]
        assert ids == ["wf_a", "wf_b"]
        assert c.get_workflow("wf_b")["mid_flight"] == "next_campaign_only"
        assert c.get_config("wf_a")["version"] == 3
        assert c.get_config_history("wf_a")[0]["actor"] == "business_default"


def test_list_workflows_accepts_wrapped_and_bare_ids():
    def handler(req):
        return httpx.Response(200, json={"workflows": ["x", "y"]})
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler))
    assert c.list_workflows() == [{"workflow_id": "x"}, {"workflow_id": "y"}]


def test_unknown_workflow_raises_not_found(sim):
    with sim.client() as c, pytest.raises(NotFound):
        c.get_workflow("nope")


# ---------------------------------------------------------------- chunks


def test_list_chunks_with_cursor(sim):
    with sim.client() as c:
        assert c.list_chunks("history") == [1, 2]
        assert c.list_chunks("history", after=1) == [2]
        assert c.list_chunks("events") == []
    req = sim.requests[-1]
    assert req.url.params["kind"] == "events"


def test_list_chunks_rejects_bad_kind(sim):
    with sim.client() as c, pytest.raises(ValueError):
        c.list_chunks("bogus")


def test_list_chunks_accepts_bare_int_list():
    def handler(req):
        return httpx.Response(200, json=[5, 3, 4])
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler))
    assert c.list_chunks("events") == [3, 4, 5]


def test_iter_chunk_streams_ndjson_and_skips_blank_lines():
    body = '{"event_id": 1}\n\n{"event_id": 2}\n   \n'

    def handler(req):
        return httpx.Response(200, text=body)
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler))
    assert [e["event_id"] for e in c.iter_chunk(7)] == [1, 2]


def test_fetch_chunk(sim):
    with sim.client() as c:
        events = c.fetch_chunk(1)
    assert [e["event_id"] for e in events] == [1, 2, 3, 4, 5]


def test_iter_chunk_invalid_json_raises():
    def handler(req):
        return httpx.Response(200, text='{"event_id": 1}\n{not json\n')
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler))
    it = c.iter_chunk(1)
    assert next(it)["event_id"] == 1
    with pytest.raises(SimulatorError, match="line 2"):
        next(it)


def test_iter_chunk_missing_raises_not_found(sim):
    with sim.client() as c, pytest.raises(NotFound):
        c.fetch_chunk(999)


# ---------------------------------------------------------------- advance


def test_advance_returns_new_chunks_and_time(sim):
    with sim.client() as c:
        r = c.advance(60)
    assert r.new_chunks == [3]  # chunk numbers are global, after history 1-2
    assert r.sim_time == "2027-01-04T01:00:00+05:30" and r.sim_minute == 60
    assert r.finished is False
    req = sim.requests[-1]
    assert req.method == "POST" and json.loads(req.content) == {"minutes": 60}


@pytest.mark.parametrize("minutes", [0, -15, 10, 20])
def test_advance_validates_minutes_locally(sim, minutes):
    with sim.client() as c, pytest.raises(ValueError):
        c.advance(minutes)
    assert sim.requests == []  # never sent


def test_advance_until_finished_then_409(sim):
    sim.days = 1
    with sim.client() as c:
        assert c.advance(1440).finished is True
        with pytest.raises(ScenarioFinished):
            c.advance(15)


def test_advance_is_not_retried_on_5xx(sim):
    sim.faults["/sim/advance"] = [httpx.Response(503)]
    with sim.client(get_retries=3) as c, pytest.raises(SimulatorUnavailable):
        c.advance(15)
    assert len(sim.requests) == 1
    assert sim.minute == 0


def test_advance_transport_error_not_retried():
    calls = []

    def handler(req):
        calls.append(req)
        raise httpx.ReadTimeout("slow", request=req)
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler), get_retries=3)
    with pytest.raises(SimulatorUnavailable):
        c.advance(15)
    assert len(calls) == 1


# ---------------------------------------------------------------- GET retries


def test_get_retries_5xx_then_succeeds(sim):
    sim.faults["/sim/state"] = [httpx.Response(500), httpx.Response(502)]
    with sim.client(get_retries=3) as c:
        assert c.state()["sim_minute"] == 0
    assert len(sim.requests) == 3


def test_get_gives_up_after_retries(sim):
    sim.faults["/health"] = [httpx.Response(503)] * 5
    with sim.client(get_retries=2) as c, pytest.raises(SimulatorUnavailable):
        c.health()
    assert len(sim.requests) == 3


def test_get_retries_transport_errors():
    attempts = []

    def handler(req):
        attempts.append(req)
        if len(attempts) < 2:
            raise httpx.ConnectError("refused", request=req)
        return httpx.Response(200, json={"status": "ok"})
    c = SimulatorClient("http://s", transport=httpx.MockTransport(handler), backoff_s=0)
    assert c.health() == {"status": "ok"}
    assert len(attempts) == 2


def test_get_does_not_retry_4xx(sim):
    with sim.client(get_retries=3) as c, pytest.raises(NotFound):
        c.get_config("missing")
    assert len(sim.requests) == 1


# ---------------------------------------------------------------- error mapping


def _resp(status, body):
    req = httpx.Request("POST", "http://s/x")
    return httpx.Response(status, json=body, request=req)


@pytest.mark.parametrize("status,body,exc", [
    (402, {"detail": "insufficient_credits"}, InsufficientCredits),
    (409, {"detail": "finished"}, ScenarioFinished),
    (409, {"detail": {"error": "stale_base_version"}}, StaleBaseVersion),
    (409, {"detail": "duplicate_proposal_id"}, Conflict),
    (410, {"detail": "lapsed"}, Lapsed),
    (404, {"detail": "x"}, NotFound),
    (422, {"detail": [{"msg": "bad"}]}, SimulatorError),
    (500, {"detail": "boom"}, SimulatorUnavailable),
])
def test_error_mapping(status, body, exc):
    with pytest.raises(exc) as ei:
        _raise_for_response(_resp(status, body))
    assert ei.value.status == status
    assert ei.value.body == body


def test_duplicate_conflict_is_not_stale_or_finished():
    with pytest.raises(Conflict) as ei:
        _raise_for_response(_resp(409, {"detail": "duplicate"}))
    assert not isinstance(ei.value, (StaleBaseVersion, ScenarioFinished))
