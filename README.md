# Adaptive Agent

An outcome-first controller for a simulated outbound voice-agent platform. It talks to the simulator **only over HTTP**. See `ADAPTIVE_AGENT_BUILD_SPEC.md` for the full spec.

## Status

| Milestone | State |
|---|---|
| 1. Skeleton and simulator HTTP client | **Done**: unit-tested against a fake |
| 2. SQLite and safe ingestion | **Done**: unit- and hard-kill-tested against a fake |
| 3. Features, cohorts, KPI aggregation | Next |
| 4–7 | Not started |

Nothing has been run against a live simulator yet.

## Run

```bash
./run.sh http://localhost:8000      # simulator must already be running
```

`run.sh` ingests all history chunks, then advances time and ingests every new chunk until the scenario finishes. It makes **no decisions yet**, so a full run is a no-change baseline run.

```bash
.venv/bin/python -m controller.main <SIM_URL> --probe            # read-only discovery, no DB, no advance
.venv/bin/python -m controller.main <SIM_URL> --bootstrap-only   # ingest history only
.venv/bin/python -m controller.main <SIM_URL> --max-advances 4   # stop early
```

Settings come from environment variables; see `.env.example`.

## Test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

## Layout

```text
controller/
  client.py    HTTP client: typed methods, error mapping, GET retries, NDJSON streaming
  settings.py  env-based settings (advance step must be a multiple of 15 min)
  database.py  SQLite: WAL, explicit transactions, user_version migrations
  events.py    the only module that knows raw event field names (validate + normalise)
  ingestion.py chunk ingestion: streamed, batched, idempotent, atomic checkpoint
  main.py      entry point and main loop (bootstrap, advance, ingest)
tests/
  fakes.py     in-memory fake simulator served via httpx.MockTransport
```

## Client design notes

- **GETs are retried** on transport errors and 5xx, with exponential backoff.
- **POST/PUT are never retried automatically.** If `/sim/advance` were retried after a timeout, simulated time could move twice. Governance retries will rely on the `proposal_id` idempotency key.
- HTTP errors map to typed exceptions: `InsufficientCredits` (402), `ScenarioFinished` / `StaleBaseVersion` / `Conflict` (409), `Lapsed` (410), `NotFound` (404) and `SimulatorUnavailable` (5xx/transport).
- Chunks are **streamed** line by line. A failure mid-stream raises, and the whole chunk is replayed.
- The spec doesn't fully pin down some response shapes, such as chunk listings and the time field on advance. The client parses these defensively and keeps the raw payload. They still need confirming against the live `/docs`.

## Ingestion guarantees

- **One transaction per chunk.** Raw events, typed projections (`calls`, `leads`, `campaigns`, `config_changes`, `approval_decisions`) and the `event_chunks` checkpoint row commit together. A crash before commit means the chunk is replayed; a crash after commit means it is skipped.
- **Idempotent inserts.** Every table uses `INSERT OR IGNORE` on a natural key, so replays and duplicate `event_id`s across chunks never double-count.
- **Bounded memory.** Events stream from HTTP and flush in batches of 5,000 rows inside the chunk transaction.
- **No gaps.** Chunks are processed in ascending order and ingestion stops at the first failure.
- **Restart recovery.** On startup the controller lists and ingests any chunks after the committed cursor, including chunks from an advance whose ingestion never committed, before advancing again.
- **Invalid events are quarantined** in `rejected_events` with a reason, rather than blocking ingestion or vanishing. Unknown event types are kept in the raw `events` log.
- `tests/test_recovery.py` kills a real child process with `os._exit` mid-chunk, mid-transaction, then checks that the partial chunk left no rows and that resuming completes without duplicates.

**Local measurement (fake simulator, not the real load scenario):** 200k events ingested at about 49k events/s, about 0.8 KB/event on disk. The real load test is part of Milestone 7.
