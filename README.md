# Adaptive Agent

An outcome-first controller for a simulated outbound voice-agent platform. It talks to the simulator **only over HTTP**. See `ADAPTIVE_AGENT_BUILD_SPEC.md` for the full spec.

## Status

| Milestone | State |
|---|---|
| 1. Skeleton and simulator HTTP client | **Done**: unit-tested against a fake |
| 2. SQLite and safe ingestion | **Done**: unit- and hard-kill-tested against a fake |
| 3. Features, cohorts, KPI aggregation | **Done** |
| 4. First controlled experiment | **Done**: design, exposure counting and decision; go-live waits for M5 |
| 5. Governance and audit | **Done**: approval/auto-apply lifecycle, audit query; tested on the fake |
| 6. Incidents, decay, rollback | **Done**: caller-ID health (C5), post-promotion harm check and rollback (C7), prompt incidents (C8) |
| 7 | Not started |

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
  incidents.py    caller-ID health (C5): detection, incident record, mitigation proposal
  prompt_incidents.py  hallucination incidents (C8): detection, credits, validation, verification
  policy.py       per-workflow governance policy from metadata + operator preferences
  impact.py       proposal evidence: diff, affected leads, dry run, impact score
  governance.py   the ONLY path that changes a config: approvals, auto-apply, apply, reconcile
  audit.py        audit query CLI: why a config changed, who approved, before/after KPIs
config/
  operator_preferences.json   stricter client limits by tenant (can only tighten)
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
| **What is tested** | One dimension at a time: `welcome_message`, then `voice`. Both are heard only *after* a call connects, so they are tested only for workflows whose primary KPI is **engagement**; for pickup-primary workflows the planner skips them and logs why. `caller_id` is left to caller-ID health (C5) and `prompt_version` to the prompt-incident path (C8), so attributions don't mix. |
| **Populations** | One cohort from the canonical model, searched in order of historical volume. Control is the cohort's current dominant option. Candidates are untested options from the workflow's live `options`. Traffic is split equally, so control keeps 1/(k+1). |
| **Unit and repeat dials** | One observation per person. Pickup uses each person's first dial in the window; engagement uses each person's first *connected* dial. The arm is the variant recorded on that call. Repeat dials stay in operational KPIs but not in the test. |
| **Which calls count** | Only calls from config versions whose allocation for (cohort, dimension) equals the design, from go-live on. A candidate counts as tested only if a statistical test actually ran on it. Under-exposed or never-exposed candidates can be planned again. |
| **Minimum sample per arm** | Primary KPI: two-proportion sample size at α/k and 80% power. The baseline is each person's *first exposure* in history (the same unit the test uses). Guardrail: a non-inferiority sample size for the stated tolerance, in guardrail units (e.g. connected calls); the experiment waits for both. |
| **Uplift threshold (MDE)** | Powered for a target of 3pp. If the window can't support 3pp, the smallest uplift it *can* detect is used, up to a maximum acceptable 5pp (rounded up to 0.1pp) and recorded in the design. α and power never change, so a larger MDE means more missed small effects, never more false winners. Above 5pp the experiment isn't run. |
| **Traffic and horizon** | Traffic comes from the cumulative number of distinct people seen in the first 1..D days of history (a re-dialled person counts once), extended with the last day's *new* people. For `next_campaign_only` workflows only new people are counted, plus a 24h ramp. The horizon is the time left in the scenario minus 24h for approval and 48h for promotion and after-measurement (72h fallback if the end is unknown). The deadline at go-live is capped so the post-decision time still fits. Every rejection logs the horizon, units per arm in the window, and the primary and guardrail sample needs. |
| **Stopping rule** | Fixed horizon. Interim ticks check only the counts; the test runs once, when every arm has its primary and guardrail sample or the deadline passes. There is no peeking. |
| **Test** | Pooled two-proportion z-test, two-sided, with a Newcombe 95% interval on the difference. |
| **Multiple comparisons** | Holm-Bonferroni across all planned candidates. Under-exposed candidates stay in the family with p = 1. |
| **Practical significance** | The uplift must be at least `min_practical_uplift` (default 1pp), as well as Holm-significant. |
| **Guardrail** | Non-inferiority on the 95% CI of (candidate − control). **Pass** if the lower bound ≥ −tolerance, **breach** if the upper bound < −tolerance, otherwise **unverified** (never promoted, never reported as a breach). The tolerance comes from the workflow objective (absolute pp unless the key says relative). **Unit:** for a *pickup* guardrail on a post-connect setting (`welcome_message`, `voice`), first-dial pickup cannot differ between arms, so the guardrail uses the **carryover unit**: each person's first connected call in the window assigns their arm, and the outcome is whether their *next* dial connected. Otherwise each person's first exposure. A stated tolerance with no history for its unit makes the design infeasible: it is refused, never skipped. |
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

## Caller-ID health (C5)

The controller compares each caller ID's pickup with the other caller IDs of the same workflow in a rolling 6-hour window. Caller ID is sampled independently on every dial, so this is a randomised, same-hours, same-audience comparison. An incident opens only when **all** of these hold:

- at least 300 dials for the caller ID and for its peers in the window;
- a one-sided z ≤ −4 (about 3×10⁻⁵ per check);
- a relative drop of at least 30%;
- the breach persists for 3 hours.

One bad call, a small sample, a single bad hour or a mild dip never triggers it. The detection lag is about 6 hours after onset. A workflow with only one caller ID is compared with its own history instead.

**What happens on detection:**

- An `incidents` row is written (status `detected`) with the window counts, z, drop and policy, plus a `incident_log` entry and a `CALLER-ID INCIDENT` WARNING.
- A mitigation is **proposed**: the caller ID's weight is moved to the remaining healthy IDs (or to the other allowed options), recorded with the total-variation shift per cohort. Submitting it is governance (Milestone 5); this module never changes a config.
- While the incident is open, every experiment proposal and promotion leaves that caller ID out.
- When the live config no longer allocates the caller ID, whether by an applied proposal or a supervisor edit, the incident becomes `mitigated` and records who changed it.

## Governance (Milestone 5)

`controller/governance.py` is the only code that changes a workflow config. Each tick, per workflow:

1. **Reconcile** in-flight approvals (`GET /approvals/{id}`) and apply approved ones within their apply window.
2. **Collect intents** in priority order: incident mitigation, rollback, promotion, experiment go-live. Only one intent per (cohort, dimension).
3. **Respect supervisors.** An intent whose dimension a supervisor edited afterwards is dropped and escalated, never overridden, even if the dimension is auto-apply eligible. If the live config moved but its `config_changed` event hasn't been ingested yet, nothing is proposed until the author is known.
4. **Build one batched proposal** from a fresh `GET /config`, changing only the intended (cohort, dimension) slots, so a supervisor's edits are never reverted.
5. **Validate.** Every value must be in the workflow's `options`. No caller ID with an open incident may be included. A `prompt_version` change needs `validation_run_id` evidence with a pass rate ≥ `min_validation_pass_rate`.
6. **Route.**
   - `PUT /config` only if auto-apply is enabled **and** every changed dimension is in `allowed_dims` **and** every per-cohort TV shift is ≤ `max_shift`.
   - Otherwise `POST /approvals`, but only if worst-case approver latency still fits inside approver hours before the TTL expires, and the request limit (simulator, or tighter operator preference) allows it.
   - Otherwise wait, and log why once.

**Proposal lifecycle:**

- `pending` → `approved` → `applied`.
- `approved` → `lapsed` (apply window missed) or `stale` (409, base changed): re-proposed under a **new** id from fresh data.
- `expired` is distinct from `rejected` and is retried.
- `rejected` drops the intent; a rejected experiment go-live cancels the experiment.
- At most one request is in flight per workflow, and after 3 failed attempts the intent escalates to a human.
- Every step writes `audit_log` plus a `GOVERNANCE` WARNING line.
- The parsed policy of every workflow is logged at startup.

**Every proposal carries** a stable `proposal_id`, the exact `base_version`, the full config, a summary, a diff with per-cohort TV shift, `affected_leads` (with its basis), a dry run built from observed data, an `impact_score`, and a rationale. Impact score = `min(1, |expected pp| / 5) × traffic share changed × confidence` (0.9 measured, 0.5 rollback, 0.3 exploratory); the inputs are kept in the dry run.

**Audit query:**

```bash
.venv/bin/python -m controller.audit --workflow ks_cancel
```

For each config version it shows who changed it, the proposal and approval, the evidence and rationale, every governance step, and observational KPIs for 24h before and after (pickup and engagement with their own denominators, plus pickup per caller ID).

## Prompt / hallucination incidents (C8)

`controller/prompt_incidents.py` handles the full lifecycle:

`detected` → `fix_proposed` → `fix_applied` → `resolved`, with three exits:

- `fix_failed` → next attempt, up to 2, then `escalated`. It never goes back to the hallucinating prompt.
- `no_safe_fix`: nothing passed validation, or the credit reserve would be breached. This escalates to a human.
- `superseded`: a supervisor changed the prompt after detection. The controller doesn't override it and keeps monitoring.

**Detection.** The hallucination-flag rate on connected, QA'd calls in the last 12 hours with QA data is compared with the workflow's own history rate. The history rate already contains the detector's false positives, and false negatives only shrink the observed rise. An incident needs **all** of:

- at least 300 QA'd calls in the window;
- z ≥ 4;
- a rise of at least 2 points and at least 50%;
- **each of the last 3 hours on its own** at least 2 points above history.

The last rule is there because one terrible hour inflates a 12-hour window for 12 hours. While a `no_safe_fix` or `escalated` incident exists for the prompt that is still live, nothing reopens and no credits are re-spent.

**Credits.** Credits are spent only when the incident is confirmed, no fix is in flight, no supervisor changed the prompt since detection, and the spend leaves a reserve of max(10, 20% of the starting credits). One attempt costs at most 9 credits: suggestions (3) plus up to 2 validations at n = 300 (3 each). Every spend goes to `credit_ledger` and a `CREDITS` log line. A 402 escalates the incident instead of crashing.

**Choosing a fix.** Suggestions not in the workflow's prompt `options`, already tried, or equal to the prompt at detection are skipped. A candidate must reach `min_validation_pass_rate`; among those that do, the highest 95% lower bound wins. A validated runner-up is kept for a second attempt, so the second attempt may cost no credits at all.

**Approval.** The fix always goes through governance (`prompt_version` isn't auto-apply eligible), with `evidence.validation_run_id` and the pass rate attached. If it replaces a prompt a supervisor set, the rationale says so explicitly and needs approval.

**Verification after apply.** Both are required:

- the hallucination rise is now below 2 points **and** the upper bound is clearly below the incident window;
- engagement is not significantly below the history baseline by more than the tolerance (z ≤ −3), where the tolerance is the objective's engagement guardrail or 2 points.

A safer prompt that makes people hang up is a failed fix.
