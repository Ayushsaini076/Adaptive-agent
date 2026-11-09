import pytest

from controller.database import (
    MIGRATIONS,
    connect,
    get_state,
    schema_version,
    set_state,
    transaction,
)


def test_connect_migrates_and_enables_wal(tmp_path):
    conn = connect(tmp_path / "c.db")
    assert schema_version(conn) == len(MIGRATIONS)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"events", "event_chunks", "calls", "leads", "campaigns",
            "config_changes", "approval_decisions", "controller_state"} <= tables


def test_reopen_is_idempotent(tmp_path):
    path = tmp_path / "c.db"
    connect(path).close()
    conn = connect(path)  # must not re-run migrations
    assert schema_version(conn) == len(MIGRATIONS)


def test_transaction_rolls_back_on_error(tmp_path):
    conn = connect(tmp_path / "c.db")
    with pytest.raises(RuntimeError):
        with transaction(conn):
            set_state(conn, "k", "v")
            raise RuntimeError("boom")
    assert get_state(conn, "k") is None
    with transaction(conn):
        set_state(conn, "k", 1)
        set_state(conn, "k", 2)
    assert get_state(conn, "k") == "2"
