import pytest

from controller.main import probe
from controller.settings import Settings
from tests.fakes import sample_simulator


def test_settings_defaults_and_trailing_slash(monkeypatch):
    monkeypatch.delenv("ADVANCE_MINUTES", raising=False)
    s = Settings.from_env("http://sim:8000/")
    assert s.sim_url == "http://sim:8000"
    assert s.advance_minutes == 60


@pytest.mark.parametrize("minutes", [0, 10, -15])
def test_settings_rejects_bad_advance(minutes):
    with pytest.raises(ValueError):
        Settings(sim_url="http://x", advance_minutes=minutes)


def test_settings_requires_url(monkeypatch):
    monkeypatch.delenv("SIM_URL", raising=False)
    with pytest.raises(ValueError):
        Settings.from_env(None)


def test_probe_is_read_only_and_summarises():
    sim = sample_simulator()
    with sim.client() as c:
        summary = probe(c)
    assert [w["workflow_id"] for w in summary["workflows"]] == ["wf_a", "wf_b"]
    assert summary["workflows"][0]["primary"] == "pickup"
    assert summary["workflows"][1]["config_version"] == 1
    assert summary["history_chunks"] == {"count": 2, "first": 1, "last": 2}
    assert all(r.method == "GET" for r in sim.requests)
    assert sim.minute == 0
