"""Read-only KPI reports from the controller's own database.

    python -m controller.report [--db PATH] [--workflow ID] [--by cohort_id,voice]
                                [--start ISO] [--end ISO]

Pickup, engagement and business outcome are always shown side by side with
their own numerators/denominators, never combined.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from controller.cohorts import MIN_COHORT_SAMPLE
from controller.database import connect
from controller.metrics import business_outcome, kpis, workflow_kpis


def _row(r, min_n: int) -> dict:
    return {
        "dials": r.dials,
        "pickup": r.pickup.as_dict(),
        "engagement": r.engagement.as_dict(),
        "hallucination_flag_rate": r.hallucination.as_dict(),
        "small_sample": r.dials < min_n,
    }


def workflow_report(conn: sqlite3.Connection, workflow_id: str, *,
                    by: tuple[str, ...] = ("cohort_id",), start: str | None = None,
                    end: str | None = None, min_n: int = MIN_COHORT_SAMPLE) -> dict:
    meta = conn.execute("SELECT metadata FROM workflows WHERE workflow_id = ?",
                        (workflow_id,)).fetchone()
    objective = json.loads(meta[0]).get("objective") if meta else None
    total = workflow_kpis(conn, workflow_id, start=start, end=end)
    bo = business_outcome(conn, workflow_id, start=start, end=end)["*"]
    return {
        "workflow_id": workflow_id,
        "objective": objective,
        "window": {"start": start, "end": end},
        "total": _row(total, min_n),
        "business_outcome": {"uploaded": bo.uploaded, "concluded": bo.concluded,
                             "interested": bo.interested, **bo.rate.as_dict()},
        "breakdown": {"by": list(by), "rows": [
            {"key": dict(zip(by, r.key)), **_row(r, min_n)}
            for r in kpis(conn, workflow_id, by=by, start=start, end=end)]} if by else None,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--db", default="data/controller.db")
    p.add_argument("--workflow", help="default: all workflows")
    p.add_argument("--by", default="cohort_id",
                   help="comma-separated: cohort_id,config_version,welcome_message,voice,"
                        "caller_id,prompt_version,hour (empty for totals only)")
    p.add_argument("--start")
    p.add_argument("--end")
    a = p.parse_args(argv)
    conn = connect(a.db)
    ids = [a.workflow] if a.workflow else [
        r[0] for r in conn.execute("SELECT workflow_id FROM workflows ORDER BY 1")]
    by = tuple(x for x in a.by.split(",") if x)
    try:
        out = [workflow_report(conn, w, by=by, start=a.start, end=a.end) for w in ids]
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
