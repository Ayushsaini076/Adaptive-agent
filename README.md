# Adaptive Agent

An outcome-first controller for a simulated outbound voice-agent platform. It talks to the simulator **only over HTTP**. See `ADAPTIVE_AGENT_BUILD_SPEC.md` for the full spec.

## Status

| Milestone | State |
|---|---|
| 1. Skeleton and simulator HTTP client | **Done**: unit-tested against a fake; not yet run against a live simulator |
| 2. SQLite and safe ingestion | Next |
| 3–7 | Not started |

## Run

```bash
./run.sh http://localhost:8000      # simulator must already be running
```

Right now, `run.sh` only runs a **read-only probe**. It checks health, state, workflows, configs and chunk listings, then prints a JSON summary. It does not advance simulated time yet. Time only moves when the controller calls `/sim/advance`, so advancing before ingestion exists would let the world run without anything being recorded.

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
  main.py      entry point (Milestone 1: discovery probe only)
tests/
  fakes.py     in-memory fake simulator served via httpx.MockTransport
```

## Client design notes

- **GETs are retried** on transport errors and 5xx, with exponential backoff.
- **POST/PUT are never retried automatically.** If `/sim/advance` were retried after a timeout, simulated time could move twice. Governance retries will rely on the `proposal_id` idempotency key.
- HTTP errors map to typed exceptions: `InsufficientCredits` (402), `ScenarioFinished` / `StaleBaseVersion` / `Conflict` (409), `Lapsed` (410), `NotFound` (404) and `SimulatorUnavailable` (5xx/transport).
- Chunks are **streamed** line by line. A failure mid-stream raises, and the whole chunk is replayed. In Milestone 2, idempotent inserts and an atomic cursor commit will make that replay safe.
- The spec doesn't fully pin down some response shapes, such as chunk listings and the time field on advance. The client parses these defensively and keeps the raw payload. They still need confirming against the live `/docs`.
