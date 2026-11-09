"""Controller entry point.

Milestone 1 scope: connect, check health, discover state/workflows/configs and
list available history chunks. It does NOT advance simulated time yet: time
only moves when we call /sim/advance, and advancing before ingestion exists
(Milestone 2) would let the world run without the controller recording it.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from controller.client import SimulatorClient, SimulatorError
from controller.settings import Settings

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Adaptive Agent controller")
    parser.add_argument("sim_url", nargs="?", help="simulator base URL (overrides SIM_URL)")
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
            summary = probe(client)
        except SimulatorError as e:
            log.error("simulator probe failed: %s", e)
            return 1

    print(json.dumps(summary, indent=2, default=str))
    log.info("milestone 1 probe complete; time was not advanced")
    return 0


if __name__ == "__main__":
    sys.exit(main())
