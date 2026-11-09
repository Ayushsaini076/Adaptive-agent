# Adaptive Agent

An outcome-first controller for a simulated outbound voice-agent platform. It talks to the simulator **only over HTTP**. See `ADAPTIVE_AGENT_BUILD_SPEC.md` for the full spec.

## Status

| Milestone | State |
|---|---|
| 1. Skeleton and simulator HTTP client | **Done**: unit-tested against a fake |
| 2. SQLite and safe ingestion | **Done**: unit- and hard-kill-tested against a fake |
| 3. Features, cohorts, KPI aggregation | **Done** |
| 4. First controlled experiment | **Done**: design, exposure counting and decision; go-live waits for M5 |
| 5. Governance and audit | Next |
| 6–7 | Not started |

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

### Which database is used

- **By default**, each scenario gets its own file: `data/worlds/<scenario>.db`, where the name comes from `/sim/state`.
- **Controller restart:** it resumes the same file, because the simulator's `sim_minute` is at or ahead of the stored one.
- **Simulator restart:** this is a new world, detected because `sim_minute` went backwards. The old file is **moved** to `data/worlds/<scenario>.<UTC timestamp>.db` (never deleted) and a fresh one is started. No operator action is needed.
- **`DB_PATH` set:** that exact file is used. The controller **refuses to run** (exit code 3) if the file belongs to another world, rather than moving a file you chose.
- **Backstops:** if `/sim/advance` reports a "new" chunk that is already stored, or an incoming event reuses a stored `event_id` with different content, the run stops with exit code 3 instead of silently skipping or dropping data.

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
  stats.py     sample sizes, two-proportion test, Newcombe CI, Holm (pure functions)
  allocations.py  the only module that reads/edits config allocations and options
  experiments.py  experiment planning, first-exposure counts, decision, lifecycle
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

## Experiments (Milestone 4)

The controller plans one experiment per workflow at a time, detects when its allocation is live, and decides once the predeclared sample is reached. In this milestone nothing makes a design live; that's the governance path in Milestone 5. Every decision is a pure function of stored counts. No LLM is involved.

| Question the spec asks | Answer |
|---|---|
| **What is tested** | One dimension at a time: `welcome_message`, then `voice`. `caller_id` is left to caller-ID health (C5) and `prompt_version` to the prompt-incident path (C8), so attributions don't mix. |
| **Populations** | One cohort from the canonical model, searched in order of historical volume. Control is the cohort's current dominant option. Candidates are untested options from the workflow's live `options`. Traffic is split equally, so control keeps 1/(k+1). |
| **Unit and repeat dials** | One observation per person. Pickup uses each person's first dial in the window; engagement uses each person's first *connected* dial. The arm is the variant recorded on that call. Repeat dials stay in operational KPIs but not in the test. |
| **Which calls count** | Only calls from config versions whose allocation for (cohort, dimension) equals the design, from go-live on. A candidate counts as tested only if a statistical test actually ran on it. Under-exposed or never-exposed candidates can be planned again. |
| **Minimum sample per arm** | The larger of two numbers. (1) Primary KPI: two-proportion sample size from the historical baseline, powered at 80% for an absolute uplift of `mde` (default 3pp), with α/k for k candidates. (2) Guardrail: non-inferiority sample size for the stated tolerance, converted into primary units with the historical ratio (e.g. connected per dial). |
| **Feasibility** | The design must be expected to finish within `max_duration_hours` (default 72h), plus a 24h ramp for `next_campaign_only` workflows. Otherwise fewer candidates are tried, then the next cohort or dimension. The reasons for any rejection are logged. |
| **Stopping rule** | Fixed horizon. Interim ticks check only the counts; the test runs once, when every arm has its primary and guardrail sample or the deadline passes. There is no peeking. |
| **Test** | Pooled two-proportion z-test, two-sided, with a Newcombe 95% interval on the difference. |
| **Multiple comparisons** | Holm-Bonferroni across all planned candidates. Under-exposed candidates stay in the family with p = 1. |
| **Practical significance** | The uplift must be at least `min_practical_uplift` (default 1pp), as well as Holm-significant. |
| **Guardrail** | Non-inferiority on the 95% CI of (candidate − control). **Pass** if the lower bound ≥ −tolerance. **Breach** if the upper bound < −tolerance. Otherwise **unverified**, which is never promoted and never reported as a breach. The tolerance comes from the workflow objective (absolute, or relative to the control rate). With no stated tolerance, only a significant drop counts as a breach. |
| **Choosing among winners** | The highest lower confidence bound, not the highest raw rate. Ties go to declared option order. |
| **Outcomes** | `winner`, `no_winner`, `guardrail_breach`, `guardrail_unverified`, `inconclusive` (control or all candidates under-exposed at the deadline). |
| **Promotion** | A recommendation only. The winner gets 90% and control keeps a 10% holdout, so decay can still be measured (C7). Applying it is Milestone 5. |
| **False-positive trade-off** | Family-wise α = 5% per experiment and 80% power at the MDE. A smaller MDE means fewer false negatives but longer experiments; experiments that don't fit the deadline aren't started. |

**Lifecycle** (persisted in `experiments`; every transition appended to `experiment_log`):

- `planned` becomes `running` when the live config's allocation matches the design.
- `planned` becomes `cancelled` if the base config changes before go-live.
- `running` becomes `aborted` if the allocation under test is changed, e.g. by a supervisor; the actor is recorded.
- `running` becomes `decided` when the test runs. Per-arm counts at decision time are stored in `experiment_evaluations`.

**Clock.** Experiment timing uses each workflow's latest ingested call time (`data_clock`), not the `/sim/advance` response, which doesn't yet report time.
