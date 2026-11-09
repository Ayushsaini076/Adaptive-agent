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
    assert s.finished and s.advances == 4
    assert (s.sim_time, s.sim_minute) == ("2027-01-06T00:00:00+05:30", 2880)
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


def test_run_snapshots_configs_and_history_baselines_once(tmp_path):
    sim = sample_simulator()
    conn = connect(tmp_path / "c.db")
    s = run(_settings(tmp_path), sim.client(), conn, bootstrap_only=True)
    assert s.workflows == ["wf_a", "wf_b"]
    assert conn.execute("SELECT COUNT(*) FROM workflow_config_snapshots").fetchone()[0] == 2
    hist = conn.execute("SELECT den, window_end FROM baselines WHERE workflow_id='wf_a' "
                        "AND label='history' AND kpi='pickup' AND cohort_id='*'").fetchone()
    assert hist[0] == 5                                    # 5 history calls
    assert hist[1] == "2027-01-04T11:00:00+05:30"          # hour after last history event

    run(_settings(tmp_path), sim.client(), conn)           # full run incl. live data
    again = conn.execute("SELECT den FROM baselines WHERE workflow_id='wf_a' "
                         "AND label='history' AND kpi='pickup' AND cohort_id='*'").fetchone()
    assert again[0] == 5                                   # live data never leaks in


# ---------------------------------------------------------------- F5: simulated clock


def test_sim_clock_parses_real_state_payload():
    from controller.client import sim_clock
    real = {"scenario": "public_02_incidents", "sim_minute": 0,
            "ts": "2027-01-04T00:00:00+05:30", "end_minute": 14400, "finished": False}
    assert sim_clock(real) == ("2027-01-04T00:00:00+05:30", 0)
    assert sim_clock({}) == (None, None)


def test_run_records_sim_time_and_minute(tmp_path):
    sim = sample_simulator()
    conn = connect(tmp_path / "c.db")
    run(_settings(tmp_path), sim.client(), conn, max_advances=1)
    state = dict(conn.execute("SELECT key, value FROM controller_state").fetchall())
    assert state["sim_time"] == "2027-01-04T12:00:00+05:30"
    assert state["sim_minute"] == "720"


def test_advance_without_clock_falls_back_to_state(tmp_path):
    sim = sample_simulator()
    sim.advance_has_clock = False
    s = run(_settings(tmp_path), sim.client(), connect(tmp_path / "c.db"), max_advances=1)
    assert (s.sim_time, s.sim_minute) == ("2027-01-04T12:00:00+05:30", 720)


# ---------------------------------------------------------------- F4: per-advance line


def test_advance_log_line_has_counts(tmp_path, caplog):
    import logging
    sim = sample_simulator()
    with caplog.at_level(logging.INFO, logger="controller"):
        run(_settings(tmp_path), sim.client(), connect(tmp_path / "c.db"), max_advances=1)
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("advanced"))
    assert "(minute 720)" in line and "5 events (5 new, 0 duplicate, 0 rejected)" in line
    assert "config changes none" in line
