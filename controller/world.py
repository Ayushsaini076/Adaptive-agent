"""Simulator-world identity: which database file belongs to which world.

The simulator keeps its world in memory; restarting it starts a NEW world
whose chunk numbers and event ids restart too. Mixing two worlds in one
database silently skips chunks (same chunk numbers) and drops events (same
event ids), so the controller must never do it.

The API exposes no world id, so identity is inferred from ``/sim/state``:
* ``scenario`` name: different name => different world.
* ``sim_minute``: time never goes backwards within one world, so a simulator
  reporting an earlier minute than the database last recorded has restarted.

Database selection:
* ``DB_PATH`` unset (default): one file per scenario under ``DB_DIR``. On a
  mismatch the old file is MOVED aside (never deleted) and a fresh one started.
* ``DB_PATH`` set: that exact file; on a mismatch the controller refuses to
  run rather than touch a file the operator chose.

Residual risk: a restarted simulator already advanced PAST the recorded
minute before the controller reconnects looks like the same world. The
ingestion backstop (an "already ingested" chunk reported as new by
/sim/advance, or an event_id collision) stops the run in that case.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from controller.client import sim_clock
from controller.database import connect, get_state, set_state, transaction
from controller.settings import Settings

log = logging.getLogger(__name__)

SQLITE_SUFFIXES = ("", "-wal", "-shm")


class WorldMismatch(RuntimeError):
    """The database and the simulator are not the same world."""


def scenario_of(state: dict) -> str:
    return str(state.get("scenario") or "unknown")


def auto_db_path(db_dir: str | Path, state: dict) -> Path:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", scenario_of(state))[:100] or "unknown"
    return Path(db_dir) / f"{name}.db"


def check_world(conn: sqlite3.Connection, state: dict) -> str | None:
    """Reason the database is NOT this simulator's world, or None if it is."""
    stored = get_state(conn, "world_scenario")
    if stored is None:
        has_data = conn.execute("SELECT 1 FROM event_chunks LIMIT 1").fetchone()
        return ("database holds ingested data but no recorded world identity "
                "(created before world tracking)") if has_data else None
    current = scenario_of(state)
    if stored != current:
        return f"database belongs to scenario {stored!r}, simulator is running {current!r}"
    stored_min = get_state(conn, "sim_minute")
    _, cur_min = sim_clock(state)
    if stored_min not in (None, "None") and cur_min is not None and cur_min < int(stored_min):
        return (f"simulator time went backwards (database at minute {stored_min}, "
                f"simulator at {cur_min}): the simulator was restarted, this is a new world")
    return None


def record_world(conn: sqlite3.Connection, state: dict) -> None:
    ts, minute = sim_clock(state)
    with transaction(conn):
        if get_state(conn, "world_scenario") is None:
            set_state(conn, "world_scenario", scenario_of(state))
            set_state(conn, "world_first_seen_minute", minute)
        if minute is not None:
            set_state(conn, "sim_minute", minute)
        if ts is not None:
            set_state(conn, "sim_time", ts)


def archive(path: Path) -> Path:
    """Move a closed database (and its WAL/SHM files) aside. Never deletes."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dest = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
    for suffix in SQLITE_SUFFIXES:
        src = Path(str(path) + suffix)
        if src.exists():
            src.rename(Path(str(dest) + suffix))
    return dest


def open_world_db(settings: Settings, state: dict) -> tuple[sqlite3.Connection, Path]:
    """Open the database for the simulator's current world. See module docstring."""
    explicit = settings.db_path is not None
    path = Path(settings.db_path) if explicit else auto_db_path(settings.db_dir, state)
    conn = connect(path)
    reason = check_world(conn, state)
    if reason:
        conn.close()
        if explicit:
            raise WorldMismatch(
                f"{path}: {reason}. Point DB_PATH at a new file, or unset DB_PATH to let "
                f"the controller choose one per scenario.")
        dest = archive(path)
        log.warning("%s: %s; previous database moved to %s, starting a fresh one",
                    path, reason, dest)
        conn = connect(path)
    record_world(conn, state)
    log.info("using database %s (scenario %s)", path, scenario_of(state))
    return conn, path
