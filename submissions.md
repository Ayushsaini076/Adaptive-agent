# Adaptive Agent — Submission

## 0. TL;DR

The Adaptive Agent is a deterministic, closed-loop controller for managing simulator workflows. It monitors operational KPIs, detects incidents, evaluates configuration changes, and routes changes through a governed approval and application process.

The implementation covers ingestion, workflow monitoring, cohort attribution, experimentation, incident detection, governance, auditability, and recovery.

Evaluation across the supplied scenarios demonstrates:

- **`public_01_dev`:** Clean baseline execution with no controller changes, incidents, or approval requests.
- **`public_02_incidents`:** Five changes applied, four beneficial and one harmful; overall conclusion rate improved by 8.1%, pickup by 5.4%, and engagement by 1.8% against the no-op baseline.
- **`public_03_load`:** Completed all 120 advances across 12 workflows, processing 9,737,015 events with zero rejected events, duplicates, or skipped chunks.

The controller uses deterministic logic for monitoring, decision-making, validation, and governance. An LLM fix call was used in the incident scenario to assist with prompt remediation; it does not act as the controller's decision-maker or bypass governance.

---

## 1. Assumptions

The system operates against a simulator that exposes workflow state, event history, live events, configuration, and governance-related operations.

The following assumptions guide the implementation:

1. **The simulator is the source of truth.** The controller reads simulator state and does not independently manufacture workflow outcomes.
2. **Configuration changes are versioned.** Proposed changes must be evaluated against the configuration version they were based on.
3. **Operational safety takes precedence over short-term KPI improvement.** A change that improves engagement but violates a guardrail must not be promoted.
4. **Changes require traceability.** Every proposal, approval, rejection, application, and relevant configuration transition should be auditable.
5. **Historical and live events serve different purposes.** Historical ingestion supports backfills and reconstruction, while live ingestion supports ongoing monitoring.
6. **A workflow may have multiple dimensions that affect performance.** Caller ID, prompt version, and other configuration dimensions must be evaluated without assuming that a single change explains every observed outcome.
7. **The no-op controller is the comparison baseline.** Reported outcome deltas are interpreted relative to the simulator's no-op baseline.
8. **Simulation time and wall-clock time are distinct.** Operational windows and incident durations are evaluated in simulated time unless explicitly stated otherwise.

---

## 2. Scope decisions

The implementation prioritises a reliable and auditable control loop over unrestricted autonomous configuration changes.

| Capability | Decision |
|---|---|
| Workflow monitoring | Included |
| Historical event ingestion | Included |
| Live event ingestion | Included |
| KPI and cohort evaluation | Included |
| Configuration experimentation | Included |
| Caller ID incident detection | Included |
| Prompt-quality incident detection | Included |
| Automated proposal generation | Included |
| Governance and approval workflow | Included |
| Configuration application | Included through the governed path |
| Rollback and mitigation handling | Included in the control architecture |
| Audit and reporting | Included |
| Unrestricted autonomous mutation | Excluded |
| LLM-controlled configuration decisions | Excluded |
| Real public load scenario | Completed at the tested workload |

The central design decision is to separate **detecting a problem**, **proposing a change**, **authorising it**, and **applying it**. This prevents a monitoring signal or generated suggestion from becoming an unreviewed production mutation.

---

## 3. Stakeholders and operating requirements

The controller is designed around the operational needs of the following stakeholder groups:

| Stakeholder | Primary concern | System response |
|---|---|---|
| Operations | Workflow performance and incident response | KPI monitoring and incident detection |
| Campaign/workflow owner | Business outcome improvement | Cohort and experiment analysis |
| Supervisor | Control over configuration changes | Approval, rejection, and edit handling |
| Governance | Safe and traceable mutation | Version checks, approval records, and audit history |
| Engineering | Reliability and recoverability | Idempotent ingestion, transactions, and recovery handling |
| Management | Measurable outcomes | Scenario reports and aggregate KPI comparisons |

The design aims to ensure that the controller can make recommendations without removing human authority over sensitive changes.

---

## 4. Architecture

The system is organised into distinct components with clear responsibilities.

### 4.1 Simulator client

Provides the interface to simulator endpoints for:

- Retrieving workflow and configuration state
- Advancing simulation time
- Fetching historical event data
- Fetching live event data
- Reading governance and approval state
- Submitting proposals and applying authorised changes

The client separates simulator communication from controller logic.

### 4.2 Ingestion and persistence

The ingestion pipeline retrieves event data and persists it in SQLite.

Its responsibilities include:

- Historical backfill
- Incremental live ingestion
- Event validation and normalisation
- Duplicate detection
- Chunk-level processing
- Checkpoint tracking
- Reconstructable event and projection state

SQLite WAL mode and transaction boundaries support concurrent reads and reliable writes. Chunk checkpoints allow completed ingestion work to be distinguished from unfinished work.

### 4.3 Event processing and KPI evaluation

Events are transformed into the data required for workflow monitoring and evaluation.

The system maintains workflow-level and cohort-level measurements, including:

- Pickup rate
- Engagement rate
- Conclusion rate
- Hallucinated-call counts
- Relevant configuration and variant attribution

Attribution uses the event's workflow and configuration context rather than assuming that the current configuration applies retrospectively to every event.

### 4.4 Cohort evaluation

Cohort rules support feature-based matching.

- Conditions across different features are combined using AND.
- Multiple accepted values within a feature are combined using OR.
- First-match-wins ordering determines the selected cohort.
- Events that do not match a configured cohort fall into the default cohort.

A minimum observation floor is used to avoid treating very small samples as reliable evidence.

### 4.5 Experiment manager

The experiment manager evaluates configuration alternatives and their effect on operational KPIs.

The design favours changing one configuration dimension at a time, with a fixed evaluation horizon and explicit guardrail checks.

### 4.6 Incident monitors

Dedicated monitors evaluate operational signals such as:

- Caller ID performance degradation
- Prompt-quality degradation
- Workflow KPI deterioration

Incident detection is separated from the mechanism that proposes or applies a remediation.

### 4.7 Governance manager

The governance manager is the controlled path for configuration mutation.

It handles:

- Approval requests
- Human approval and rejection
- Expiration
- Auto-apply eligibility
- Configuration version conflicts
- Lapsed proposals
- Application and audit records

A proposal is not equivalent to an applied change.

### 4.8 Audit and reporting

The reporting layer exposes scenario outcomes, applied changes, governance decisions, incidents, and ingestion statistics.

This makes it possible to evaluate both business outcomes and the behaviour of the controller itself.

---

## 5. Learning and experimentation loop

The control loop follows this sequence:

1. Ingest historical and live events.
2. Update workflow and cohort metrics.
3. Detect KPI changes and possible incidents.
4. Identify candidate configuration dimensions.
5. Evaluate candidate changes against available evidence.
6. Validate the candidate and its guardrails.
7. Submit the change through governance.
8. Apply only when the required authorisation and version conditions are satisfied.
9. Observe the result and record the outcome.
10. Use the resulting evidence in subsequent evaluations.

### Experiment design

The experiment design aims to avoid attributing an observed improvement to the wrong configuration dimension.

Key principles include:

- Prefer one configuration dimension per experiment.
- Use a fixed evaluation horizon.
- Evaluate against the appropriate baseline.
- Require sufficient observations before drawing conclusions.
- Consider statistical significance and practical significance.
- Apply multiple-comparison correction where appropriate.
- Evaluate guardrails independently of the primary KPI.

The implementation uses an observation floor of 200 for KPI evaluation. Experiment planning targets 80% statistical power and a 3 percentage-point minimum detectable effect where feasible; experiments with an estimated detectable effect above 5 percentage points are not treated as adequately powered.

A 1 percentage-point practical threshold is used when evaluating whether an observed change is meaningful. Holm correction and Newcombe confidence intervals are used in the relevant statistical comparisons.

These thresholds guide evaluation; they do not guarantee that every experiment will produce a conclusive result.

---

## 6. Governance

Governance is a core part of the architecture rather than an optional layer around configuration updates.

### 6.1 Approval path

The controller separates proposal creation from application.

A proposal can be:

- Approved
- Rejected
- Expired
- Applied after approval
- Eligible for auto-application when all configured conditions are satisfied

The incident scenario demonstrates that approval decisions are recorded and that approved changes can be applied through the governed path.

### 6.2 Mutation safety

The governance path accounts for:

- Base configuration versions
- Stale proposals
- Supervisor edits
- Expired approvals
- Application conflicts
- Lapsed proposals

A proposal based on an outdated configuration must not silently overwrite a newer supervisor or controller change.

### 6.3 Priority handling

The controller prioritises safety-sensitive operations. Incident mitigation and rollback take precedence over ordinary experiment promotion or new experiment activation.

### 6.4 Human authority

Human decisions remain authoritative. A rejected proposal is not applied, and a supervisor edit must not be silently overwritten by an older proposal.

### 6.5 Auto-application

Auto-application is restricted to changes satisfying the configured governance requirements. The incident scenario recorded zero auto-applied changes, demonstrating that the changes in that run went through approval rather than bypassing the approval path.

---

## 7. Incident handling

The incident scenario, `public_02_incidents`, exercises caller ID degradation, prompt changes, and supervisor edits.

### 7.1 Caller ID incident

The report records a `did_flagged` incident for `ks_cancel` at D2 10:00.

The flagged DID share by day was:

| Day | Flagged DID share |
|---|---:|
| D2 | 37.7% |
| D3 | 0.0% |
| D4 | 0.0% |
| D5 | 0.0% |
| D6 | 28.0% |
| D7 | 41.5% |
| D8 | 0.0% |
| D9 | 0.0% |

The report demonstrates that the incident signal was detected and that flagged DID exposure varied over the subsequent days. The later recurrence on D6 and D7 is important: the initial improvement should not be interpreted as proof that the underlying risk could never recur.

### 7.2 Prompt-quality incident and remediation

The applied prompt changes included:

| Change | Dimension | Engagement delta | DID delta | Assessment |
|---|---|---:|---:|---|
| `ks_cancel v4` | `prompt_version` | -0.051 | +0.000 | Harmful |
| `ks_cancel v5` | `prompt_version` | +0.219 | +0.000 | Beneficial |

The v4 prompt change was harmful, while the subsequent v5 change was beneficial.

This is a useful demonstration of why changes must be evaluated after application rather than assuming that a validated proposal will necessarily improve the outcome.

### 7.3 Supervisor edits

The incident report records two supervisor edits:

- `ks_cancel` at D3 11:30, with a reported MTTR of 1,650 simulated minutes and the fix attributed to `controller_approved`.
- `nw_renewal` at D5 15:00.

The report does not provide an MTTR value for the `nw_renewal` edit.

### 7.4 Incident scenario outcome

The incident run demonstrates detection, proposal generation, approval handling, application, and the measurement of both beneficial and harmful changes.

It also highlights that incident remediation is not a one-time action: subsequent monitoring remains necessary to identify recurrence or regressions.

---

## 8. Scale

The controller was run against the real public load scenario, completing all 120 advances across 12 workflows.

### Load test results

| Metric | History ingestion | Live ingestion |
|---|---:|---:|
| Chunks | 23 | 65 |
| Events | 5,536,955 | 4,200,060 |
| Inserted | 5,536,955 | 4,200,060 |
| Rejected | 0 | 0 |
| Duplicates | 0 | 0 |
| Skipped chunks | 0 | 0 |
| Ingestion time | 376.322s | 380.199s |
| Throughput | 14,713 events/sec | 11,047 events/sec |

The run processed **9,737,015 events** in **961.763 seconds** of total wall-clock time (approximately 16 minutes).

All 120 advances completed successfully, and the simulator reached its finished state at:

`2027-01-09T00:00:00+05:30`

The ingestion pipeline inserted every reported event, with zero rejected events, duplicate events, or skipped chunks in either history or live ingestion.

### Interpretation

History ingestion sustained approximately **14.7k events/sec**, while live ingestion sustained approximately **11.0k events/sec**.

The difference is consistent with live processing having additional work beyond historical ingestion; however, profiling would be required to attribute the difference to specific components.

This result demonstrates successful completion of the tested load scenario and clean ingestion at approximately 9.74 million events. It does not, by itself, establish peak memory consumption, p95 decision latency, behaviour beyond this workload, or recovery under a process crash during this particular run.

### Remaining scale work

At higher event volumes, likely pressure points include SQLite write contention, index and aggregate maintenance, Python validation and normalisation cost, and synchronous per-workflow governance/API work.

Further profiling should capture peak RSS, transaction latency, p95 decision lag, and wall-clock time per simulated day.

Potential improvements include a bounded ingestion queue with a single database writer, partitioning databases by tenant or world, and independent workflow evaluation workers. Configuration mutation should remain serialised per workflow to preserve base-version and supervisor-edit guarantees.

---

## 9. Reliability and recovery

Reliability is addressed through transactional ingestion, checkpoints, idempotency, and explicit handling of simulator API failures.

### 9.1 Transactional ingestion

Ingestion is performed in chunks with transaction boundaries and checkpoints.

A completed chunk can be recognised without reprocessing all earlier work, while unfinished work can be retried safely.

### 9.2 Idempotency

Natural event keys are used to prevent duplicate ingestion from creating duplicate persisted records.

The public load scenario provides additional evidence for this behaviour: both history and live ingestion reported zero duplicates and zero rejected events.

### 9.3 Recovery testing

The recovery test exercises process interruption using a child process terminated with `os._exit(137)`, allowing the system's persisted state and restart behaviour to be examined.

### 9.4 API retry behaviour

Read-only GET requests can be retried where appropriate.

Mutation requests such as simulator advancement must not be blindly retried because the first request may have succeeded even if the client did not receive its response. Governance proposal/application flows require reconciliation against simulator state and proposal identity before retrying.

### 9.5 Remaining reliability validation

The completed public load run did not itself establish crash recovery under peak load. A dedicated crash-and-restart test during a comparable high-volume ingestion run remains useful validation.

---

## 10. Audit and observability

The controller records enough information to understand both its operational behaviour and the decisions that led to configuration changes.

Relevant information includes:

- Ingested event and chunk counts
- Rejected and duplicate event counts
- Checkpoint progress
- Workflow and cohort metrics
- Experiment results
- Candidate configuration changes
- Approval requests and decisions
- Rejection and expiration reasons
- Application outcomes and conflicts
- Incident records
- Scenario-level reports

The grade reports provide a concise evaluation view containing outcome deltas against the no-op baseline, applied changes, governance decisions, incident information, and resource usage.

This is important because aggregate KPI improvement alone is not sufficient to establish that the controller behaved safely.

---

## 11. Results

The system was evaluated using three scenarios.

### 11.1 `public_01_dev`

<text>**Purpose:** Validate normal development execution against the no-op controller baseline.</text>

| Workflow | Conclusion rate | Δ | Pickup | Δ | Engagement | Δ | Engagement gap captured | Hallucinated calls (base) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `ks_cancel` | 0.342 | +0.0% | 0.240 | +0.0% | 0.500 | +0.0% | 0% | 52 (52) |
| `nw_kyc` | 0.377 | +0.0% | 0.242 | +0.0% | 0.612 | +0.0% | 0% | 21 (21) |
| `nw_renewal` | 0.420 | +0.0% | 0.262 | +0.0% | 0.547 | +0.0% | 0% | 39 (39) |
| **ALL** | **0.375** | **+0.0%** | **0.248** | **+0.0%** | **0.538** | **+0.0%** | **0%** | **112 (112)** |

**Controller activity**

- Changes applied: 0
- Approval requests: 0
- Approved, rejected, or expired decisions: 0
- Auto-applied changes: 0
- Apply conflicts: 0
- Incidents: 0
- Credits used: 0
- Validation runs: 0
- LLM fix calls: 0

**Assessment:** The controller left the baseline configuration unchanged. The outcome deltas were all zero, and no incident or governance activity was recorded. This is the expected no-intervention result for this scenario.

### 11.2 `public_02_incidents`

<text>**Purpose:** Evaluate the controller under incident conditions, including caller ID degradation, prompt changes, and supervisor edits.</text>

#### Outcomes against the no-op baseline

| Workflow | Conclusion rate | Δ | Pickup | Δ | Engagement | Δ | Engagement gap captured | Hallucinated calls (base) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `ks_cancel` | 0.309 | +26.7% | 0.214 | +17.3% | 0.497 | +5.4% | 30% | 202 (310) |
| `nw_kyc` | 0.374 | -2.8% | 0.241 | -2.7% | 0.601 | +0.2% | 0% | 27 (38) |
| `nw_renewal` | 0.430 | -0.1% | 0.260 | -1.3% | 0.555 | +1.2% | 5% | 55 (58) |
| **ALL** | **0.363** | **+8.1%** | **0.235** | **+5.4%** | **0.540** | **+1.8%** | **15%** | **284 (406)** |

The aggregate results improved against the no-op baseline, but improvements were not uniform across workflows. `nw_kyc` declined in conclusion rate and pickup, while `nw_renewal` had a small decline in conclusion rate and pickup. The overall result should therefore not be interpreted as every workflow improving.

Hallucinated calls decreased from 406 in the baseline to 284 in the controller run.

#### Applied changes

**5 changes were applied: 4 beneficial and 1 harmful.**

| Workflow / version | Change source | Dimension | Engagement Δ | DID Δ | Result |
|---|---|---|---:|---:|---|
| `ks_cancel v2` | `controller_approved` | `caller_id` | +0.000 | +0.481 | Beneficial |
| `ks_cancel v4` | `controller_approved` | `prompt_version` | -0.051 | +0.000 | Harmful |
| `ks_cancel v5` | `controller_approved` | `prompt_version` | +0.219 | +0.000 | Beneficial |
| `ks_cancel v6` | `controller_approved` | `caller_id` | +0.000 | +0.029 | Beneficial |
| `ks_cancel v7` | `controller_approved` | `caller_id` | +0.000 | +0.486 | Beneficial |

The harmful v4 prompt change is retained in the report rather than omitted. It demonstrates that the controller's changes are not assumed to be beneficial merely because they passed the proposal and approval process.

#### Governance results

| Metric | Result |
|---|---:|
| Approval requests | 7 |
| Approved | 5 |
| Rejected | 1 |
| Expired | 1 |
| Applied via approval | 5 |
| Auto-applied | 0 |
| Auto-apply rejected | 0 |
| Apply conflicts | 0 |
| Lapsed | 0 |
| Peak requests per 24h (`ks_cancel`) | 2 |

The rejection reason was `approver declined (judgement call)`.

All five applied changes went through approval. There were no auto-applied changes and no application conflicts.

#### Incident results

| Incident | Workflow | Time | Result |
|---|---|---|---|
| `did_flagged` | `ks_cancel` | D2 10:00 | Flagged DID share tracked across subsequent days |
| `supervisor_edit` | `ks_cancel` | D3 11:30 | MTTR: 1,650 minutes; fixed by `controller_approved` |
| `supervisor_edit` | `nw_renewal` | D5 15:00 | Supervisor edit recorded |

The report also recorded the following resource usage:

- Credits used: 9
- Validation runs: 2
- LLM fix calls: 1

The decaying-variants report was empty (`{}`), so no variant-decay conclusion is claimed from this run.

**Assessment:** The incident scenario demonstrates incident detection, governed configuration changes, human approval and rejection, and outcome measurement. It also exposes an important limitation: one applied prompt change was harmful. The subsequent beneficial prompt change and continued monitoring are therefore important parts of the result.

### 11.3 `public_03_load`

<text>**Purpose:** Validate ingestion and execution under a high-volume public workload.</text>

| Metric | Result |
|---|---:|
| Workflows | 12 |
| Advances | 120 |
| Finished | Yes |
| History events | 5,536,955 |
| Live events | 4,200,060 |
| Total events | **9,737,015** |
| History throughput | 14,713 events/sec |
| Live throughput | 11,047 events/sec |
| Total wall time | 961.763 seconds |
| History rejected / duplicates / skipped | 0 / 0 / 0 |
| Live rejected / duplicates / skipped | 0 / 0 / 0 |

**Assessment:** The scenario completed all advances and inserted all reported history and live events without rejected events, duplicates, or skipped chunks. This validates successful execution at the tested workload, not unlimited scalability.

### 11.4 Consolidated evaluation

| Scenario | Main evidence | Result |
|---|---|---|
| `public_01_dev` | Baseline behaviour and no-intervention path | No changes, incidents, or approvals; outcome deltas remained zero |
| `public_02_incidents` | Incident response and governance | 5 applied changes; 4 beneficial, 1 harmful; 7 approval requests |
| `public_03_load` | High-volume ingestion and simulation completion | 9.737 million events; zero rejects, duplicates, or skipped chunks |

---

## 12. Trade-offs and rejected alternatives

### 12.1 Deterministic control logic over LLM-led decisions

The controller's monitoring, KPI evaluation, proposal eligibility, governance, and mutation decisions are deterministic.

This improves reproducibility and makes it easier to explain why a decision was made. An LLM can assist with a bounded task such as prompt remediation, but it does not independently decide whether a configuration change should be applied.

The incident report records one LLM fix call. This should not be confused with an LLM-driven control architecture.

### 12.2 SQLite over a distributed database

SQLite provides a simple deployment and persistence model for the current workload. WAL mode, transactions, and checkpoints support the required ingestion and recovery behaviour.

A distributed database may become appropriate if tenant isolation, concurrent writers, or workload size exceed the current deployment model.

### 12.3 Governed mutation over unrestricted autonomy

Allowing the controller to apply every generated proposal would reduce approval latency but increase the risk of harmful or stale configuration changes.

The incident run provides concrete evidence for this trade-off: one of the five applied changes was harmful. Approval and post-application evaluation remain important safeguards.

### 12.4 Chunked ingestion over unbounded in-memory processing

Chunking and checkpointing make ingestion recoverable and bound the amount of work associated with a transaction.

The trade-off is additional bookkeeping and database overhead.

### 12.5 One configuration dimension at a time

Changing multiple dimensions simultaneously can make attribution difficult. Evaluating one dimension at a time improves interpretability, although it can take longer to explore interactions between configuration dimensions.

---

## 13. Known weaknesses and limitations

The current implementation has demonstrated successful execution across the development, incident, and public load scenarios. The following gaps remain:

1. **Uneven workflow outcomes:** Aggregate improvement in `public_02_incidents` did not translate into improvement across every workflow.
2. **Harmful changes remain possible:** The v4 prompt change was harmful, demonstrating that approval and validation do not guarantee a positive outcome.
3. **Incident recurrence:** Flagged DID share reappeared on D6 and D7 after earlier zero-share days. Continued monitoring is necessary.
4. **Load-test profiling:** The public load run did not record peak RSS, p95 decision latency, or detailed transaction latency.
5. **Beyond-tested scale:** The 9.74-million-event result establishes behaviour at the tested workload only. It does not establish capacity beyond that workload.
6. **Crash recovery under load:** Recovery testing exists, but crash recovery during a comparable high-volume public load run remains to be demonstrated.
7. **Variant decay evidence:** The decaying-variants output was empty in both supplied grade reports, so no claim about measured variant decay is made.
8. **Experiment conclusiveness:** Statistical thresholds and minimum observation requirements reduce weak conclusions, but sparse or noisy data can still leave an experiment inconclusive.
9. **Database write contention:** SQLite may become a bottleneck as concurrent ingestion and workflow evaluation increase.
10. **Synchronous orchestration:** Per-workflow simulator and governance operations may limit throughput at larger workflow counts.

---

## 14. Production plan

The next steps should focus on strengthening the evidence for reliability and scale rather than adding autonomous mutation capabilities prematurely.

### Phase 1 — Performance profiling

- Capture peak resident memory during the completed public load scenario.
- Measure transaction latency and database write contention.
- Record p50 and p95 decision lag.
- Break down wall-clock time by ingestion, evaluation, and governance operations.
- Repeat the load scenario to assess run-to-run consistency.

### Phase 2 — Recovery under load

- Trigger a controlled process interruption during high-volume ingestion.
- Verify that completed chunks are not reprocessed incorrectly.
- Verify that unfinished chunks can be recovered.
- Check event counts and projections after restart.
- Reconcile outstanding governance operations before retrying mutations.

### Phase 3 — Scale improvements

If profiling identifies bottlenecks:

- Introduce a bounded ingestion queue.
- Consider a single database writer with concurrent read/evaluation workers.
- Evaluate database partitioning by tenant or simulation world.
- Separate workflow evaluation workers where independent execution is safe.
- Preserve serialised configuration mutation per workflow.

### Phase 4 — Stronger incident evaluation

- Track whether caller ID and prompt incidents recur after remediation.
- Evaluate changes over longer observation windows.
- Improve visibility into variant decay and configuration exposure.
- Report workflow-level regressions alongside aggregate improvements.
- Continue recording harmful changes rather than excluding them from evaluation.

### Phase 5 — Production readiness

- Define operational SLOs for ingestion throughput and decision latency.
- Establish alerting for ingestion lag, rejected events, and stale checkpoints.
- Define approval and escalation policies for sensitive changes.
- Add repeatable scenario regression tests.
- Document deployment, backup, restore, and incident-response procedures.

The production objective is a controller that is measurable, recoverable, explainable, and safe to operate—not simply one that can generate and apply configuration changes autonomously.
