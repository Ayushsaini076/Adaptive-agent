"""SQLite persistence: connection setup, schema migrations, transactions.

* WAL mode so readers (reports, audit queries) don't block the writer.
* Autocommit connection (isolation_level=None) with explicit
  ``BEGIN IMMEDIATE`` / ``COMMIT`` so every transaction boundary is visible
  in code rather than implied by the sqlite3 module.
* Schema versioned with ``PRAGMA user_version``; migrations are append-only.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

# Each entry migrates user_version i -> i+1. Never edit a shipped migration;
# append a new one.
MIGRATIONS: list[str] = [
    # 1: ingestion foundation
    """
    CREATE TABLE controller_state (
        key        TEXT PRIMARY KEY,
        value      TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- One row per fully ingested chunk. Written in the same transaction as
    -- the chunk's events, so its presence means "this chunk is committed".
    CREATE TABLE event_chunks (
        chunk_number   INTEGER PRIMARY KEY,
        kind           TEXT    NOT NULL CHECK (kind IN ('history','events')),
        event_count    INTEGER NOT NULL,
        inserted_count INTEGER NOT NULL,
        rejected_count INTEGER NOT NULL,
        first_event_id INTEGER,
        last_event_id  INTEGER,
        ingested_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- Raw event log: source of truth, append-only, unique by event_id.
    CREATE TABLE events (
        event_id     INTEGER PRIMARY KEY,
        ts           TEXT,
        type         TEXT NOT NULL,
        workflow_id  TEXT,
        tenant_id    TEXT,
        chunk_number INTEGER NOT NULL,
        payload      TEXT NOT NULL
    );
    CREATE INDEX idx_events_type_wf ON events(type, workflow_id, event_id);

    -- Events that parsed as JSON but failed validation. Kept for diagnosis,
    -- never silently dropped.
    CREATE TABLE rejected_events (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        chunk_number INTEGER NOT NULL,
        line_index   INTEGER NOT NULL,
        reason       TEXT NOT NULL,
        payload      TEXT NOT NULL,
        UNIQUE (chunk_number, line_index)
    );

    -- Typed projections used by metrics/cohorts. Keyed so replays are no-ops.
    CREATE TABLE campaigns (
        campaign_id    TEXT PRIMARY KEY,
        workflow_id    TEXT NOT NULL,
        tenant_id      TEXT,
        created_ts     TEXT,
        lead_count     INTEGER,
        config_version INTEGER,
        event_id       INTEGER NOT NULL
    );
    CREATE INDEX idx_campaigns_wf ON campaigns(workflow_id, created_ts);

    CREATE TABLE leads (
        lead_id     TEXT PRIMARY KEY,
        person_id   TEXT,
        campaign_id TEXT,
        workflow_id TEXT,
        features    TEXT NOT NULL,   -- JSON object: lang, city_tier, ...
        event_id    INTEGER NOT NULL
    );
    CREATE INDEX idx_leads_campaign ON leads(campaign_id);
    CREATE INDEX idx_leads_person ON leads(person_id);

    CREATE TABLE calls (
        event_id        INTEGER PRIMARY KEY,
        ts              TEXT,
        workflow_id     TEXT NOT NULL,
        tenant_id       TEXT,
        campaign_id     TEXT,
        lead_id         TEXT,
        person_id       TEXT,
        attempt         INTEGER,
        campaign_day    INTEGER,
        config_version  INTEGER,
        cohort_id       TEXT,
        welcome_message TEXT,
        voice           TEXT,
        caller_id       TEXT,
        prompt_version  TEXT,
        connected       INTEGER NOT NULL,
        engaged         INTEGER NOT NULL,
        disposition     TEXT,
        duration_s      REAL,
        turns           INTEGER,
        hallucination_flag INTEGER,   -- NULL when not connected / no QA
        flow_adherence  REAL,
        variant         TEXT          -- full variant JSON, for dims not columned
    );
    CREATE INDEX idx_calls_wf_ts ON calls(workflow_id, ts);
    CREATE INDEX idx_calls_wf_cohort ON calls(workflow_id, cohort_id, ts);
    CREATE INDEX idx_calls_person ON calls(workflow_id, person_id, attempt);

    CREATE TABLE config_changes (
        event_id       INTEGER PRIMARY KEY,
        ts             TEXT,
        workflow_id    TEXT NOT NULL,
        new_version    INTEGER,
        actor          TEXT,
        changed_dims   TEXT,          -- JSON list
        proposal_id    TEXT,
        approval_id    TEXT
    );
    CREATE INDEX idx_config_changes_wf ON config_changes(workflow_id, event_id);

    CREATE TABLE approval_decisions (
        event_id    INTEGER PRIMARY KEY,
        ts          TEXT,
        approval_id TEXT NOT NULL,
        workflow_id TEXT,
        status      TEXT,
        reasons     TEXT              -- JSON
    );
    CREATE INDEX idx_approval_decisions_id ON approval_decisions(approval_id);
    """,
]


def connect(path: str | Path) -> sqlite3.Connection:
    """Open the DB, apply pragmas and run pending migrations."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL is durable against process crashes in WAL mode; only an OS crash
    # / power loss can roll back the last few commits, which then replay.
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    migrate(conn)
    return conn


def schema_version(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA user_version").fetchone()[0]


def migrate(conn: sqlite3.Connection) -> None:
    current = schema_version(conn)
    for version in range(current, len(MIGRATIONS)):
        with transaction(conn):
            for stmt in _split_sql(MIGRATIONS[version]):
                conn.execute(stmt)
            conn.execute(f"PRAGMA user_version = {version + 1}")


def _split_sql(script: str) -> list[str]:
    # executescript() would COMMIT implicitly; run statements one by one so
    # each migration is atomic with its user_version bump.
    stmts, buf = [], []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmts.append("\n".join(buf))
            buf = []
    if buf:
        stmts.append("\n".join(buf))
    return stmts


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``BEGIN IMMEDIATE`` ... ``COMMIT``; ROLLBACK on any exception."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ------------------------------------------------------------- controller_state


def get_state(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM controller_state WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_state(conn: sqlite3.Connection, key: str, value: object) -> None:
    """Caller is responsible for the surrounding transaction."""
    conn.execute(
        "INSERT INTO controller_state(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        (key, str(value)),
    )
