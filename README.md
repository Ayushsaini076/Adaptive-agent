# Adaptive Agent

An outcome-first controller for a simulated outbound voice-agent platform. It talks to the simulator **only over HTTP**. See `ADAPTIVE_AGENT_BUILD_SPEC.md` for the full spec.

## Status

| Milestone | State |
|---|---|
| 1. Skeleton and simulator HTTP client | **Done**: unit-tested against a fake |
| 2. SQLite and safe ingestion | **Done**: unit- and hard-kill-tested against a fake |
| 3. Features, cohorts, KPI aggregation | **Done** |
| 4. First controlled experiment | Next |
| 5–7 | Not started |

All development and testing uses the in-repo fake simulator (`tests/fakes.py`).

## Run

```bash
./run.sh http://localhost:8000      # simulator must already be running
```

`run.sh` discovers workflows and snapshots their configs, ingests all history chunks and records historical baselines, then advances time and ingests every new chunk until the scenario finishes. It makes **no decisions yet**, so a full run is a no-change baseline run.

```bash
.venv/bin/python -m controller.main <SIM_URL> --probe            # read-only discovery, no DB, no advance
.venv/bin/python -m controller.main <SIM_URL> --bootstrap-only   # ingest history only
.venv/bin/python -m controller.main <SIM_URL> --max-advances 4   # stop early
```

KPI report from the controller's own database (no simulator needed):

```bash
.venv/bin/python -m controller.report --workflow <ID> --by cohort_id,voice
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
  cohorts.py   THE canonical cohort model (matching, versioned sets, small-sample rule)
  workflows.py registry: objective/guardrail parsing, immutable config + cohort snapshots
  metrics.py   pickup / engagement / business outcome, Wilson CIs, aggregates, baselines
  report.py    read-only KPI report CLI
  main.py      entry point and main loop (bootstrap, advance, ingest, refresh configs)
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

## Cohorts and KPIs

- **One cohort model** (`controller/cohorts.py`). Ordered cohorts, first match wins, AND across features, OR within a feature's values, unmatched leads go to `default`. Values compare as normalised strings, so `2` matches `"2"`.
- **Attribution uses the call's own record.** Call KPIs group by the `cohort_id`, `config_version` and variant written on each call event. Old calls are never re-assigned under today's definitions. Cohort sets are only used to assign leads with no call yet (business-outcome denominators), and then with the config version that was active when their campaign was created. Leads from a config version that was never snapshotted are reported as `unknown`, not guessed.
- **Small-sample rule.** A cohort with fewer than 200 observations falls back to the workflow-wide population (`*`). At p = 0.5, n = 200 gives a 95% CI of about ±7pp; below that, a cohort rate isn't informative. Experiments will add power-based sample sizes on top of this floor.
- **Three KPIs, never blended.**
  - Pickup = connected / dials.
  - Engagement = engaged / **connected**.
  - Business outcome = distinct leads with a `concluded_*` call / leads uploaded.
  - Each comes with its numerator, denominator and a Wilson 95% interval.
- **Objective per workflow.** The primary KPI and guardrail (KPI and tolerance) are parsed from live metadata, never assumed.
- **Immutable config snapshots** per (workflow, version). A `config_changed` event triggers a refresh. Older versions are kept, so historical attribution and dry runs can use the config that was actually live.
- **Aggregates.** `metric_aggregates` holds hourly counts, updated inside each chunk's ingestion transaction from exactly the calls that chunk newly inserted, so replays can't double-count. A test checks that incremental results equal a full rebuild.
- **Baselines.** Pre-D0 baselines are persisted once under the label `history`. Restarts never recompute them, so live data can't leak in.
- **Timestamps** are normalised to ISO-8601 IST at ingestion, so string order is time order and the first 13 characters give the hour bucket.

Ingestion with aggregates measured about 41k events/s on 200k fake events.
