"""Evidence attached to every proposal: diff, changed dimensions, affected
leads, dry run and impact score (C10).

Impact score (0..1), one formula for every proposal so scores are comparable:

    impact = min(1, |expected KPI change in pp| / 5pp) * share_of_traffic * confidence

* expected KPI change: from MEASURED data where it exists (incident window,
  experiment result); 0 for an exploratory go-live (no prior on the candidate).
* share_of_traffic: fraction of the workflow's dials whose variant changes.
* confidence: 0.9 measured effect, 0.5 rollback of a measured harm, 0.3
  exploratory. A higher score means more expected value, not a bigger change.
The inputs are stored in the dry run so the score can be recomputed.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from controller import allocations as alloc
from controller.policy import allocation_shifts

TOP_LEVEL_DIMS = ("prompt_version", "schedule", "cohorts")


def changed_dims(current: dict, proposed: dict) -> list[str]:
    dims = {dim for _, dim, _ in allocation_shifts(current, proposed)}
    dims |= {k for k in TOP_LEVEL_DIMS if current.get(k) != proposed.get(k)}
    return sorted(dims)


def diff(current: dict, proposed: dict) -> list[dict]:
    out = [{"path": f"allocations.{cohort}.{dim}",
            "from": alloc.distribution(current, cohort, dim),
            "to": alloc.distribution(proposed, cohort, dim), "tv_shift": tv}
           for cohort, dim, tv in allocation_shifts(current, proposed)]
    out += [{"path": k, "from": current.get(k), "to": proposed.get(k)}
            for k in TOP_LEVEL_DIMS if current.get(k) != proposed.get(k)]
    return out


def traffic_share_changed(current: dict, proposed: dict) -> float:
    """Upper bound on the share of dials whose variant changes: the largest TV
    shift over (cohort, dim). Conservative when cohorts differ in size."""
    return max((tv for _, _, tv in allocation_shifts(current, proposed)), default=0.0)


def affected_leads(conn: sqlite3.Connection, workflow_id: str, now: str,
                   mid_flight: str | None) -> tuple[int, str]:
    """Estimated leads in scope over the next day, from observed data."""
    since = (datetime.fromisoformat(now) - timedelta(hours=24)).isoformat(timespec="seconds")
    if mid_flight == "next_campaign_only":
        n = conn.execute(
            "SELECT COUNT(*) FROM leads l JOIN campaigns c ON c.campaign_id = l.campaign_id "
            "WHERE c.workflow_id = ? AND c.created_ts >= ?", (workflow_id, since)).fetchone()[0]
        return n, ("estimate: leads uploaded in the last 24h; allocations apply only to "
                   "campaigns created after the change (next_campaign_only)")
    n = conn.execute("SELECT COUNT(DISTINCT lead_id) FROM calls WHERE workflow_id = ? AND ts >= ?",
                     (workflow_id, since)).fetchone()[0]
    return n, "estimate: distinct leads dialled in the last 24h (running campaigns included)"


def impact_score(expected_pp: float, share: float, confidence: float) -> float:
    return round(min(1.0, abs(expected_pp) / 5.0) * max(0.0, min(1.0, share))
                 * max(0.0, min(1.0, confidence)), 4)
