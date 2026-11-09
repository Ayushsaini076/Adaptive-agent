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


# ---------------------------------------------------------------- run loop

from controller.database import connect  # noqa: E402
from controller.main import run  # noqa: E402


def _settings(tmp_path):
    return Settings(sim_url="http://sim.test", advance_minutes=720,
                    db_path=str(tmp_path / "c.db"))


def test_run_bootstraps_then_advances_to_finish(tmp_path):
    sim = sample_simulator()  # 2 days
    conn = connect(tmp_path / "c.db")
    s = run(_settings(tmp_path), sim.client(), conn)
    assert s.finished and s.advances == 4 and s.sim_time == "D2 00:00"
    assert s.history.chunks == 2 and s.live.chunks == 4
    assert conn.execute("SELECT value FROM controller_state WHERE key='finished'"
                        ).fetchone()[0] == "1"


def test_bootstrap_only_does_not_advance(tmp_path):
    sim = sample_simulator()
    s = run(_settings(tmp_path), sim.client(), connect(tmp_path / "c.db"),
            bootstrap_only=True)
    assert s.advances == 0 and sim.minute == 0
    assert all(r.url.path != "/sim/advance" for r in sim.requests)


def test_restart_recovers_chunks_from_uningested_advance(tmp_path):
    """Process died after /sim/advance returned but before ingesting its chunk."""
    sim = sample_simulator()
    conn = connect(tmp_path / "c.db")
    run(_settings(tmp_path), sim.client(), conn, max_advances=1)
    sim.client().advance(720)  # the "lost" advance: chunk produced, never ingested
    lost = max(sim.event_chunks)

    s = run(_settings(tmp_path), sim.client(), conn, max_advances=0)
    assert s.live.chunks == 1 and s.advances == 0
    assert conn.execute("SELECT 1 FROM event_chunks WHERE chunk_number=?",
                        (lost,)).fetchone()


def test_restart_after_finish_does_not_advance(tmp_path):
    sim = sample_simulator()
    conn = connect(tmp_path / "c.db")
    run(_settings(tmp_path), sim.client(), conn)
    n = len(sim.requests)
    s = run(_settings(tmp_path), sim.client(), conn)
    assert s.finished and s.advances == 0
    assert all(r.url.path != "/sim/advance" for r in sim.requests[n:])
