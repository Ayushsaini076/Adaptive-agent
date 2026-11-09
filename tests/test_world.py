"""F1: one database per simulator world; never mix worlds.

Reproduces the real incident: a database from a finished world reused
against a restarted simulator silently skipped every chunk whose number
already existed.
"""

import logging

import pytest

from controller.database import get_state
from controller.main import run
from controller.settings import Settings
from controller.world import WorldMismatch, auto_db_path, open_world_db
from tests.fakes import sample_simulator


def settings(tmp_path, db_path=None):
    return Settings(sim_url="http://sim.test", advance_minutes=720,
                    db_path=str(db_path) if db_path else None,
                    db_dir=str(tmp_path / "worlds"))


def open_db(sim, s):
    return open_world_db(s, sim.client().state())


def test_auto_path_is_per_scenario(tmp_path):
    a, b = sample_simulator(), sample_simulator()
    b.scenario = "public_02_incidents"
    _, pa = open_db(a, settings(tmp_path))
    _, pb = open_db(b, settings(tmp_path))
    assert pa.name == "fake_scenario.db" and pb.name == "public_02_incidents.db"
    assert auto_db_path("d", {"scenario": "a/b c"}).name == "a_b_c.db"


def test_identity_recorded_on_first_open(tmp_path):
    conn, _ = open_db(sample_simulator(), settings(tmp_path))
    assert get_state(conn, "world_scenario") == "fake_scenario"
    assert get_state(conn, "sim_minute") == "0"


def test_controller_restart_in_same_world_resumes_same_file(tmp_path):
    sim, s = sample_simulator(), settings(tmp_path)
    conn, path = open_db(sim, s)
    run(s, sim.client(), conn, max_advances=2)
    conn.close()

    conn2, path2 = open_db(sim, s)            # simulator kept running: minute 1440
    assert path2 == path
    assert conn2.execute("SELECT COUNT(*) FROM event_chunks WHERE kind='events'"
                         ).fetchone()[0] == 2
    assert list(path.parent.glob("*.db")) == [path]   # nothing archived


def test_simulator_restart_archives_old_db_and_starts_fresh(tmp_path, caplog):
    sim, s = sample_simulator(), settings(tmp_path)
    conn, path = open_db(sim, s)
    first = run(s, sim.client(), conn)        # full world A
    conn.close()

    sim.restart()                             # same scenario, NEW world, minute 0
    with caplog.at_level(logging.WARNING):
        conn2, path2 = open_db(sim, s)
    assert path2 == path
    assert "time went backwards" in caplog.text and "moved to" in caplog.text
    archived = [p for p in path.parent.glob("fake_scenario.*.db")]
    assert len(archived) == 1                 # moved aside, not deleted

    second = run(s, sim.client(), conn2)      # world B ingests EVERY chunk again
    assert second.live.chunks == first.live.chunks == 4
    assert second.live.skipped == 0 and second.live.duplicates == 0


def test_explicit_db_path_refuses_other_world(tmp_path):
    sim = sample_simulator()
    s = settings(tmp_path, db_path=tmp_path / "mine.db")
    conn, _ = open_db(sim, s)
    run(s, sim.client(), conn)
    conn.close()

    sim.restart()
    with pytest.raises(WorldMismatch, match="time went backwards"):
        open_db(sim, s)
    other = sample_simulator()
    other.scenario = "another"
    with pytest.raises(WorldMismatch, match="belongs to scenario 'fake_scenario'"):
        open_db(other, s)
    assert (tmp_path / "mine.db").exists()     # never moved


def test_legacy_db_without_identity_is_not_trusted(tmp_path):
    from controller.database import connect
    from controller.ingestion import Ingester
    legacy = tmp_path / "legacy.db"
    sim = sample_simulator()
    Ingester(connect(legacy), sim.client()).catch_up("history")   # pre-F1 database
    with pytest.raises(WorldMismatch, match="no recorded world identity"):
        open_db(sim, settings(tmp_path, db_path=legacy))


def test_advance_reporting_stored_chunk_stops_the_run(tmp_path):
    """Residual case: restarted world already advanced past our recorded minute,
    so the identity check passes; the ingestion backstop must catch it."""
    sim, s = sample_simulator(), settings(tmp_path)
    conn, _ = open_db(sim, s)
    run(s, sim.client(), conn, max_advances=2)        # chunks 3, 4; minute 1440
    conn.close()

    sim.restart()
    sim.client().advance(1455)                         # new world: chunk 3, minute 1455
    conn2, _ = open_db(sim, s)                         # passes: 1455 >= 1440
    with pytest.raises(WorldMismatch, match=r"chunk\(s\) \[4\] as new"):
        run(s, sim.client(), conn2, max_advances=1)
    assert conn2.execute(
        "SELECT COUNT(*) FROM event_chunks WHERE kind='events'").fetchone()[0] == 2


def test_main_exits_3_on_explicit_db_mismatch(tmp_path, monkeypatch):
    import controller.main as main_mod
    sim = sample_simulator()
    db = tmp_path / "mine.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setattr(main_mod, "SimulatorClient", lambda *a, **k: sim.client())
    assert main_mod.main(["http://sim.test", "--max-advances", "1"]) == 0
    sim.restart()
    assert main_mod.main(["http://sim.test", "--max-advances", "1"]) == 3
    assert sim.minute == 0            # refused before advancing the new world


def test_main_auto_mode_handles_restart_without_operator(tmp_path, monkeypatch):
    import controller.main as main_mod
    sim = sample_simulator()
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.setenv("DB_DIR", str(tmp_path / "worlds"))
    monkeypatch.setattr(main_mod, "SimulatorClient", lambda *a, **k: sim.client())
    assert main_mod.main(["http://sim.test", "--max-advances", "1"]) == 0
    sim.restart()
    assert main_mod.main(["http://sim.test", "--max-advances", "1"]) == 0
    assert len(list((tmp_path / "worlds").glob("fake_scenario*.db"))) == 2
