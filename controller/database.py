"""SQLite persistence: connection setup, schema migrations, transactions.

* WAL mode so readers (reports, audit queries) don't block the writer.
* Autocommit connection (isolation_level=None) with explicit
  ``BEGIN IMMEDIATE`` / ``COMMIT`` so every transaction boundary is visible
  in code rather than implied by the sqlite3 module.
* Schema versioned with ``PRAGMA user_version``; migrations are append-only.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

# Each entry migrates user_version i -> i+1: either a SQL script or a function
# taking the connection (already inside the migration's transaction). Never
# edit a shipped migration; append a new one.
Migration = str | Callable[[sqlite3.Connection], None]

_M2_SQL = """
    CREATE INDEX idx_events_chunk ON events(chunk_number);

    CREATE TABLE workflows (
        workflow_id TEXT PRIMARY KEY,
        tenant_id   TEXT,
        metadata    TEXT NOT NULL,        -- latest GET /workflows/{id}, incl. objective/policy
        updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- Immutable: one row per (workflow, version); the first copy wins.
    CREATE TABLE workflow_config_snapshots (
        workflow_id  TEXT NOT NULL,
        version      INTEGER NOT NULL,
        actor        TEXT,
        effective_ts TEXT,
        source       TEXT NOT NULL,       -- api_current | api_history
        config       TEXT NOT NULL,
        fetched_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        PRIMARY KEY (workflow_id, version)
    );

    CREATE TABLE cohort_snapshots (
        workflow_id TEXT NOT NULL,
        version     INTEGER NOT NULL,
        cohort_id   TEXT NOT NULL,
        position    INTEGER NOT NULL,
        where_json  TEXT NOT NULL,
        PRIMARY KEY (workflow_id, version, cohort_id)
    );

    -- Hourly call counts keyed by the call's OWN recorded attribution.
    -- Unknown values are '' / -1 so the key is never NULL.
    CREATE TABLE metric_aggregates (
        workflow_id     TEXT NOT NULL,
        hour            TEXT NOT NULL,    -- 'YYYY-MM-DDTHH' in IST
        cohort_id       TEXT NOT NULL,
        config_version  INTEGER NOT NULL,
        welcome_message TEXT NOT NULL,
        voice           TEXT NOT NULL,
        caller_id       TEXT NOT NULL,
        prompt_version  TEXT NOT NULL,
        dials           INTEGER NOT NULL,
        connected       INTEGER NOT NULL,
        engaged         INTEGER NOT NULL,
        qa_n            INTEGER NOT NULL,
        hallucinations  INTEGER NOT NULL,
        PRIMARY KEY (workflow_id, hour, cohort_id, config_version, welcome_message,
                     voice, caller_id, prompt_version)
    );
    CREATE INDEX idx_agg_wf_cohort ON metric_aggregates(workflow_id, cohort_id, hour);

    CREATE TABLE baselines (
        workflow_id  TEXT NOT NULL,
        label        TEXT NOT NULL,       -- e.g. 'history'
        kpi          TEXT NOT NULL,
        cohort_id    TEXT NOT NULL,
        population   TEXT NOT NULL,       -- cohort used; '*' when pooled
        num          INTEGER NOT NULL,
        den          INTEGER NOT NULL,
        window_start TEXT,
        window_end   TEXT,
        computed_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        PRIMARY KEY (workflow_id, label, kpi, cohort_id)
    );
"""


def _m2_cohorts_and_metrics(conn: sqlite3.Connection) -> None:
    for stmt in _split_sql(_M2_SQL):
        conn.execute(stmt)
    from controller.metrics import rebuild_aggregates  # backfill from existing calls
    rebuild_aggregates(conn)


MIGRATIONS: list[Migration] = [
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
    # 2: workflows, config/cohort snapshots, aggregates, baselines
    _m2_cohorts_and_metrics,
    # 3: experiments
    """
    CREATE TABLE experiments (
        experiment_id        TEXT PRIMARY KEY,   -- deterministic: wf/cohort/dim/cands/version
        workflow_id          TEXT NOT NULL,
        cohort_id            TEXT NOT NULL,
        dimension            TEXT NOT NULL,
        control              TEXT NOT NULL,
        candidates           TEXT NOT NULL,      -- JSON list
        primary_kpi          TEXT NOT NULL,
        guardrail_kpi        TEXT NOT NULL,
        guardrail_max_drop   REAL,
        guardrail_relative   INTEGER NOT NULL,
        alpha                REAL NOT NULL,
        power                REAL NOT NULL,
        mde                  REAL NOT NULL,
        min_practical_uplift REAL NOT NULL,
        n_per_arm            INTEGER NOT NULL,   -- primary-KPI units per arm
        n_guardrail_per_arm  INTEGER NOT NULL,   -- guardrail units per arm (0 = no NI test)
        max_duration_hours   REAL NOT NULL,
        base_version         INTEGER NOT NULL,
        allocation           TEXT NOT NULL,      -- JSON {option: share} under test
        proposed_config      TEXT NOT NULL,      -- full config to make it live (M5)
        design               TEXT NOT NULL,      -- JSON evidence behind the design
        status               TEXT NOT NULL,
        created_ts           TEXT NOT NULL,      -- simulated time
        start_version        INTEGER,
        start_ts             TEXT,
        deadline_ts          TEXT,
        decided_ts           TEXT,
        decision             TEXT,               -- JSON, incl. per-arm tests
        updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_experiments_wf_status ON experiments(workflow_id, status);

    -- Append-only lifecycle log.
    CREATE TABLE experiment_log (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        experiment_id TEXT NOT NULL,
        sim_ts        TEXT NOT NULL,
        from_status   TEXT,
        to_status     TEXT NOT NULL,
        detail        TEXT NOT NULL,
        logged_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_experiment_log_exp ON experiment_log(experiment_id, id);

    -- Per-arm counts used at decision time (first exposure per person).
    CREATE TABLE experiment_evaluations (
        experiment_id TEXT NOT NULL,
        sim_ts        TEXT NOT NULL,
        arm           TEXT NOT NULL,
        kpi           TEXT NOT NULL,             -- 'primary' | 'guardrail'
        num           INTEGER NOT NULL,
        den           INTEGER NOT NULL,
        PRIMARY KEY (experiment_id, sim_ts, arm, kpi)
    );
    """,
    # 4: incidents
    """
    CREATE TABLE incidents (
        incident_id  TEXT PRIMARY KEY,     -- inc:<kind>:<workflow>:<subject>:<breach start>
        kind         TEXT NOT NULL,        -- caller_id_health | ...
        workflow_id  TEXT NOT NULL,
        subject      TEXT NOT NULL,        -- e.g. the caller ID
        status       TEXT NOT NULL,        -- detected | mitigated
        detected_ts  TEXT NOT NULL,        -- simulated time
        mitigated_ts TEXT,
        evidence     TEXT NOT NULL,        -- JSON: window counts, z, drop, policy
        mitigation   TEXT NOT NULL,        -- JSON: proposed config + TV shifts, or why none
        updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_incidents_wf ON incidents(workflow_id, kind, status);

    CREATE TABLE incident_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        incident_id TEXT NOT NULL,
        sim_ts      TEXT NOT NULL,
        from_status TEXT,
        to_status   TEXT NOT NULL,
        detail      TEXT NOT NULL,
        logged_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_incident_log_inc ON incident_log(incident_id, id);
    """,
    # 5: governance (proposals, audit log) + experiment promotion columns
    """
    ALTER TABLE experiments ADD COLUMN promoted_ts TEXT;
    ALTER TABLE experiments ADD COLUMN promoted_version INTEGER;

    -- One row per submission attempt. proposal_id is the idempotency key and is
    -- never reused: a stale/expired/lapsed request is re-proposed under a NEW id.
    CREATE TABLE proposals (
        proposal_id     TEXT PRIMARY KEY,
        workflow_id     TEXT NOT NULL,
        intents         TEXT NOT NULL,       -- JSON [{kind, ref}]
        route           TEXT NOT NULL,       -- auto | approval
        base_version    INTEGER NOT NULL,
        payload         TEXT NOT NULL,       -- exactly what was sent
        status          TEXT NOT NULL,       -- pending|approved|applied|auto_applied|rejected|expired|lapsed|stale|failed
        approval_id     TEXT,
        apply_by        TEXT,
        applied_version INTEGER,
        submitted_ts    TEXT NOT NULL,       -- simulated time
        decided_ts      TEXT,
        reasons         TEXT,                -- JSON from the approver / errors
        updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_proposals_wf ON proposals(workflow_id, status);
    CREATE INDEX idx_proposals_approval ON proposals(approval_id);

    -- Append-only record of every governance decision and change.
    CREATE TABLE audit_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        sim_ts      TEXT,
        workflow_id TEXT,
        action      TEXT NOT NULL,
        ref         TEXT,                    -- proposal / experiment / incident id
        detail      TEXT NOT NULL,           -- JSON
        logged_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_audit_wf ON audit_log(workflow_id, id);
    CREATE INDEX idx_audit_ref ON audit_log(ref, id);
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
        step = MIGRATIONS[version]
        with transaction(conn):
            if callable(step):
                step(conn)
            else:
                for stmt in _split_sql(step):
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
