"""Controller entry point and main loop.

Current scope (Milestones 1-3): discover workflows and snapshot their configs,
bootstrap from history and snapshot historical baselines, then advance time
and ingest every returned chunk until the scenario finishes, refreshing config
snapshots whenever a ``config_changed`` event arrives. No decisions are made
yet, so a full run is a "no-change baseline" run of the scenario.

Restart safety: on startup we ``catch_up`` both chunk kinds from the
committed cursor. If the previous process died after ``/sim/advance`` returned
but before its chunks were committed, those chunks are listed and ingested
here before time is advanced again.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass

from controller.client import ScenarioFinished, SimulatorClient, SimulatorError
from controller.database import connect, set_state, transaction
from controller.experiments import ExperimentManager
from controller.ingestion import Ingester, IngestStats
from controller.metrics import snapshot_baselines
from controller.settings import Settings
from controller.workflows import WorkflowRegistry

log = logging.getLogger("controller")


def probe(client: SimulatorClient) -> dict:
    """Read-only discovery pass. Returns a summary dict."""
    health = client.health()
    state = client.state()
    workflows = client.list_workflows()

    wf_summaries = []
    for wf in workflows:
        wf_id = wf.get("workflow_id") or wf.get("id")
        if not wf_id:
            log.warning("workflow without id: %r", wf)
            continue
        meta = client.get_workflow(wf_id)
        cfg = client.get_config(wf_id)
        objective = meta.get("objective", {})
        wf_summaries.append({
            "workflow_id": wf_id,
            "tenant_id": meta.get("tenant_id"),
            "primary": objective.get("primary") if isinstance(objective, dict) else objective,
            "mid_flight": meta.get("mid_flight"),
            "config_version": cfg.get("version"),
        })

    history = client.list_chunks("history")
    events = client.list_chunks("events")
    return {
        "health": health,
        "state": state,
        "workflows": wf_summaries,
        "history_chunks": {"count": len(history),
                           "first": history[0] if history else None,
                           "last": history[-1] if history else None},
        "event_chunks": {"count": len(events)},
    }


@dataclass
class RunSummary:
    advances: int = 0
    finished: bool = False
    sim_time: str | None = None
    history: IngestStats | None = None
    live: IngestStats | None = None
    wall_seconds: float = 0.0
    workflows: list[str] | None = None

    def as_dict(self) -> dict:
        def s(st: IngestStats | None) -> dict | None:
            if st is None:
                return None
            return {"chunks": st.chunks, "events": st.events, "inserted": st.inserted,
                    "rejected": st.rejected, "ingest_seconds": round(st.seconds, 3),
                    "events_per_sec": round(st.events_per_sec)}
        return {"workflows": self.workflows, "advances": self.advances, "finished": self.finished,
                "sim_time": self.sim_time, "history": s(self.history),
                "live": s(self.live), "wall_seconds": round(self.wall_seconds, 3)}


def history_window_end(conn: sqlite3.Connection) -> str | None:
    """Exclusive end (next hour) of the ingested history, or None if no history."""
    row = conn.execute(
        "SELECT MAX(e.ts) FROM events e JOIN event_chunks ch ON ch.chunk_number = "
        "e.chunk_number WHERE ch.kind = 'history'").fetchone()
    if not row or not row[0]:
        return None
    from datetime import datetime, timedelta
    last = datetime.fromisoformat(row[0]).replace(minute=0, second=0)
    return (last + timedelta(hours=1)).isoformat(timespec="seconds")


def snapshot_history_baselines(conn: sqlite3.Connection, workflow_ids: list[str]) -> None:
    """Persist pre-D0 baselines once per workflow. Restarts don't recompute them,
    so live data never leaks into the 'history' label."""
    end = history_window_end(conn)
    if end is None:
        return
    for wf in workflow_ids:
        if conn.execute("SELECT 1 FROM baselines WHERE workflow_id=? AND label='history'",
                        (wf,)).fetchone():
            continue
        with transaction(conn):
            bl = snapshot_baselines(conn, wf, "history", end=end)
        wide = {b.kpi: b.rate.as_dict() for b in bl if b.cohort_id == "*"}
        log.info("history baseline %s (calls before %s): pickup %s, engagement %s",
                 wf, end, wide.get("pickup"), wide.get("engagement"))


def _tick_experiments(manager: ExperimentManager) -> None:
    for exp_id, status in manager.tick():
        log.info("experiment %s -> %s", exp_id, status)


def _record_time(conn: sqlite3.Connection, sim_time: str | None, finished: bool) -> None:
    with transaction(conn):
        if sim_time is not None:
            set_state(conn, "sim_time", sim_time)
        set_state(conn, "finished", int(finished))


def run(settings: Settings, client: SimulatorClient, conn: sqlite3.Connection,
        *, max_advances: int | None = None, bootstrap_only: bool = False) -> RunSummary:
    t0 = time.perf_counter()
    summary = RunSummary()
    ingester = Ingester(conn, client)

    state = client.state()
    log.info("simulator state: %s", state)

    registry = WorkflowRegistry(conn, client)
    summary.workflows = registry.sync_all()
    log.info("discovered %d workflows: %s", len(summary.workflows), summary.workflows)

    summary.history = ingester.catch_up("history")
    log.info("history bootstrap: %d chunks, %d events (%d new)",
             summary.history.chunks, summary.history.events, summary.history.inserted)
    snapshot_history_baselines(conn, summary.workflows)
    experiments = ExperimentManager(conn, registry)
    _tick_experiments(experiments)

    # Recover chunks from an advance whose ingestion didn't commit before a crash.
    live = ingester.catch_up("events")
    if live.chunks:
        log.info("recovered %d un-ingested event chunks from a previous run", live.chunks)
    summary.live = live

    summary.finished = bool(state.get("finished"))
    summary.sim_time = state.get("sim_time")
    if bootstrap_only or summary.finished:
        _record_time(conn, summary.sim_time, summary.finished)
        summary.wall_seconds = time.perf_counter() - t0
        return summary

    while max_advances is None or summary.advances < max_advances:
        try:
            result = client.advance(settings.advance_minutes)
        except ScenarioFinished:
            summary.finished = True
            break
        summary.advances += 1
        summary.sim_time = result.sim_time
        for r in ingester.ingest_many(result.new_chunks, "events").results:
            live.add(r)
        changed = registry.refresh_changed()
        if changed:
            log.info("config changed for %s; snapshots refreshed", changed)
        _tick_experiments(experiments)
        # Time is recorded only after its chunks are committed.
        _record_time(conn, result.sim_time, result.finished)
        log.info("advanced to %s: %d chunks, cumulative live events %d",
                 result.sim_time, len(result.new_chunks), live.events)
        if result.finished:
            summary.finished = True
            break

    summary.wall_seconds = time.perf_counter() - t0
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Adaptive Agent controller")
    parser.add_argument("sim_url", nargs="?", help="simulator base URL (overrides SIM_URL)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--probe", action="store_true",
                      help="read-only discovery; touches neither time nor the DB")
    mode.add_argument("--bootstrap-only", action="store_true",
                      help="ingest history (and any pending chunks), do not advance")
    parser.add_argument("--max-advances", type=int, default=None,
                        help="stop after N advances (for testing)")
    args = parser.parse_args(argv)

    try:
        settings = Settings.from_env(args.sim_url)
    except ValueError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.info("connecting to simulator at %s", settings.sim_url)

    with SimulatorClient(settings.sim_url, timeout_s=settings.http_timeout_s,
                         get_retries=settings.http_get_retries) as client:
        try:
            if args.probe:
                print(json.dumps(probe(client), indent=2, default=str))
                return 0
            conn = connect(settings.db_path)
            try:
                summary = run(settings, client, conn, max_advances=args.max_advances,
                              bootstrap_only=args.bootstrap_only)
            finally:
                conn.close()
        except SimulatorError as e:
            log.error("simulator error: %s", e)
            return 1

    print(json.dumps(summary.as_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
