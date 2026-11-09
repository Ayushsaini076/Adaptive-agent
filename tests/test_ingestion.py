import pytest

from controller.client import SimulatorUnavailable
from controller.database import connect
from controller.ingestion import Ingester
from tests.fakes import call_event, chunk_of_activity, sample_simulator


def counts(conn):
    q = lambda t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]  # noqa: E731
    return {t: q(t) for t in ("events", "calls", "leads", "campaigns", "event_chunks",
                              "rejected_events")}


@pytest.fixture
def sim():
    return sample_simulator()


@pytest.fixture
def conn(tmp_path):
    return connect(tmp_path / "c.db")


def test_history_bootstrap_ingests_all_chunks(sim, conn):
    stats = Ingester(conn, sim.client()).catch_up("history")
    assert stats.chunks == 2 and stats.events == 5 + 7
    assert counts(conn) == {"events": 12, "calls": 5, "leads": 5, "campaigns": 2,
                            "event_chunks": 2, "rejected_events": 0}
    row = conn.execute("SELECT * FROM event_chunks WHERE chunk_number = 2").fetchone()
    assert (row["kind"], row["first_event_id"], row["last_event_id"]) == ("history", 100, 106)


def test_catch_up_twice_is_noop(sim, conn):
    ing = Ingester(conn, sim.client())
    ing.catch_up("history")
    before = counts(conn)
    n_requests = len(sim.requests)
    assert ing.catch_up("history").chunks == 0
    assert counts(conn) == before
    # Only a listing request; no chunk re-download.
    assert [r.url.path for r in sim.requests[n_requests:]] == ["/events/chunks"]


def test_reingesting_a_committed_chunk_is_skipped(sim, conn):
    ing = Ingester(conn, sim.client())
    ing.ingest_chunk(1, "history")
    r = ing.ingest_chunk(1, "history")
    assert r.skipped
    assert counts(conn)["events"] == 5


def test_duplicate_event_ids_across_chunks_are_not_double_counted(sim, conn):
    # Chunk 3 repeats chunk 1's events plus one genuinely new call.
    sim.event_chunks[3] = chunk_of_activity(1, "hist1") + [call_event(50, "hist1_L0", "hist1")]
    ing = Ingester(conn, sim.client())
    ing.catch_up("history")
    r = ing.ingest_chunk(3, "events")
    assert r.event_count == 6 and r.inserted == 1
    assert counts(conn)["calls"] == 6


def test_stream_failure_mid_chunk_rolls_back_everything(sim, conn):
    sim.broken_chunks[2] = 3  # serve 3 of 7 lines, then reset
    ing = Ingester(conn, sim.client(), batch_size=2)  # force flushes before failure
    with pytest.raises(SimulatorUnavailable):
        ing.catch_up("history")
    # Chunk 1 committed; chunk 2's partial rows (already flushed) rolled back.
    assert ing.cursor("history") == 1
    assert counts(conn)["events"] == 5
    assert conn.execute("SELECT COUNT(*) FROM events WHERE chunk_number=2").fetchone()[0] == 0
    # Retry ingests chunk 2 fully, exactly once.
    ing.catch_up("history")
    assert counts(conn)["events"] == 12 and ing.cursor("history") == 2


def test_failure_stops_before_later_chunks(sim, conn):
    sim.history_chunks[3] = chunk_of_activity(200, "hist3")
    sim.broken_chunks[2] = 1
    ing = Ingester(conn, sim.client())
    with pytest.raises(SimulatorUnavailable):
        ing.catch_up("history")
    committed = [r[0] for r in conn.execute("SELECT chunk_number FROM event_chunks")]
    assert committed == [1]  # no gap: chunk 3 not committed ahead of 2


def test_invalid_events_are_quarantined_not_dropped(sim, conn):
    bad_call = call_event(300, "L", "c")
    del bad_call["connected"]
    sim.history_chunks = {1: [bad_call, {"type": "lead"}, call_event(301, "L", "c")]}
    r = Ingester(conn, sim.client()).ingest_chunk(1, "history")
    assert (r.event_count, r.inserted, r.rejected) == (3, 1, 2)
    reasons = [row[0] for row in conn.execute(
        "SELECT reason FROM rejected_events ORDER BY line_index")]
    assert reasons == ["missing connected", "missing event_id"]


def test_unknown_event_type_kept_in_raw_log(sim, conn):
    sim.history_chunks = {1: [{"event_id": 1, "type": "something_new", "workflow_id": "w"}]}
    Ingester(conn, sim.client()).catch_up("history")
    assert counts(conn)["events"] == 1


def test_last_event_id_state_is_tracked(sim, conn):
    Ingester(conn, sim.client()).catch_up("history")
    v = conn.execute("SELECT value FROM controller_state WHERE key='last_event_id'").fetchone()
    assert v[0] == "106"


# ---------------------------------------------------------------- F2: duplicates vs collisions

import logging  # noqa: E402

from controller.ingestion import EventIdCollision  # noqa: E402


def test_identical_redelivery_counts_as_duplicate_and_warns(sim, conn, caplog):
    ing = Ingester(conn, sim.client())
    ing.catch_up("history")
    sim.event_chunks[3] = chunk_of_activity(1, "hist1") + [call_event(50, "x", "hist1")]
    with caplog.at_level(logging.WARNING):
        r = ing.ingest_chunk(3, "events")
    assert (r.event_count, r.inserted, r.duplicates) == (6, 1, 5)
    assert "5/6 events were exact duplicates" in caplog.text


def test_duplicate_within_one_chunk_is_counted(sim, conn):
    e = call_event(70, "x", "c")
    sim.history_chunks = {1: [e, e]}
    r = Ingester(conn, sim.client()).ingest_chunk(1, "history")
    assert (r.inserted, r.duplicates) == (1, 1)


def test_same_id_different_content_is_a_collision_and_rolls_back(sim, conn):
    """What actually happened: 144 events of a new world reused event_ids of
    stored old-world events and were silently dropped."""
    ing = Ingester(conn, sim.client())
    ing.catch_up("history")                      # stores event ids 1..5
    other_world = call_event(3, "someone_else", "other_campaign")
    sim.event_chunks[3] = [call_event(60, "y", "c"), other_world]
    with pytest.raises(EventIdCollision, match="event 3"):
        ing.ingest_chunk(3, "events")
    assert not ing.is_ingested(3)                # chunk rolled back entirely
    assert conn.execute("SELECT COUNT(*) FROM events WHERE event_id = 60").fetchone()[0] == 0


def test_skipped_chunks_are_counted(sim, conn):
    ing = Ingester(conn, sim.client())
    ing.ingest_chunk(1, "history")
    stats = ing.ingest_many([1, 2], "history")
    assert (stats.chunks, stats.skipped) == (1, 1)
