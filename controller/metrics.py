"""KPI measurement. The three KPIs are NEVER blended:

* pickup      = connected / dials            (call level)
* engagement  = engaged   / connected        (call level; denominator is CONNECTED)
* business    = leads concluded / leads uploaded   (lead level, reported separately)

Call-level KPIs are served from ``metric_aggregates``: hourly counts keyed by
workflow, the call's own recorded cohort_id/config_version and variant. It is
updated inside the same transaction as each chunk's ingestion, from exactly
the calls that chunk newly inserted, so it stays consistent with ``calls``
under replay and crashes.

Experiments need person-level data (first exposure per person) and query
``calls`` directly; these aggregates are for operational reporting,
baselines and population-level monitoring.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from controller.cohorts import MIN_COHORT_SAMPLE, WORKFLOW_WIDE, CohortSet, features_from_json
from controller.workflows import KPIS, Kpi, WorkflowRegistry

Z95 = 1.959963984540054

DIMENSIONS = ("cohort_id", "config_version", "welcome_message", "voice", "caller_id",
              "prompt_version")

# Unknown values are stored as '' / -1 (not NULL) so they take part in the
# aggregate's primary key and ON CONFLICT merges them.
_AGG_SELECT = """
    SELECT c.workflow_id,
           substr(c.ts, 1, 13)            AS hour,
           COALESCE(c.cohort_id, '')      AS cohort_id,
           COALESCE(c.config_version, -1) AS config_version,
           COALESCE(c.welcome_message,'') AS welcome_message,
           COALESCE(c.voice, '')          AS voice,
           COALESCE(c.caller_id, '')      AS caller_id,
           COALESCE(c.prompt_version, '') AS prompt_version,
           COUNT(*)                       AS dials,
           SUM(c.connected)               AS connected,
           SUM(c.engaged)                 AS engaged,
           SUM(c.hallucination_flag IS NOT NULL) AS qa_n,
           COALESCE(SUM(c.hallucination_flag), 0) AS hallucinations
    FROM calls c {join} WHERE c.ts IS NOT NULL {where}
    GROUP BY 1,2,3,4,5,6,7,8
"""
_AGG_UPSERT = """
    INSERT INTO metric_aggregates(workflow_id, hour, cohort_id, config_version,
        welcome_message, voice, caller_id, prompt_version,
        dials, connected, engaged, qa_n, hallucinations)
    {select}
    ON CONFLICT(workflow_id, hour, cohort_id, config_version, welcome_message, voice,
                caller_id, prompt_version)
    DO UPDATE SET dials = dials + excluded.dials,
                  connected = connected + excluded.connected,
                  engaged = engaged + excluded.engaged,
                  qa_n = qa_n + excluded.qa_n,
                  hallucinations = hallucinations + excluded.hallucinations
"""


def update_aggregates_for_chunk(conn: sqlite3.Connection, chunk_number: int) -> None:
    """Fold calls first inserted by ``chunk_number`` into the aggregates.

    Must run inside the chunk's ingestion transaction. Duplicate event_ids
    from other chunks keep their ORIGINAL chunk_number in ``events``, so they
    are never counted twice.
    """
    select = _AGG_SELECT.format(join="JOIN events e ON e.event_id = c.event_id",
                                where="AND e.chunk_number = ?")
    conn.execute(_AGG_UPSERT.format(select=select), (chunk_number,))


def rebuild_aggregates(conn: sqlite3.Connection) -> None:
    """Recompute all aggregates from ``calls`` (migration / repair). Caller owns tx."""
    conn.execute("DELETE FROM metric_aggregates")
    conn.execute(_AGG_UPSERT.format(select=_AGG_SELECT.format(join="", where="")))


# --------------------------------------------------------------------- rates


@dataclass(frozen=True)
class Rate:
    num: int
    den: int

    @property
    def value(self) -> float | None:
        return self.num / self.den if self.den else None

    def wilson(self, z: float = Z95) -> tuple[float, float] | None:
        """Wilson score interval: well-behaved near 0/1 and at small n."""
        if not self.den:
            return None
        n, p = self.den, self.num / self.den
        denom = 1 + z * z / n
        centre = (p + z * z / (2 * n)) / denom
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
        return max(0.0, centre - half), min(1.0, centre + half)

    def as_dict(self) -> dict:
        ci = self.wilson()
        return {"num": self.num, "den": self.den,
                "rate": None if self.value is None else round(self.value, 4),
                "ci95": None if ci is None else [round(ci[0], 4), round(ci[1], 4)]}


@dataclass(frozen=True)
class KpiRow:
    key: tuple
    dials: int
    connected: int
    engaged: int
    qa_n: int
    hallucinations: int

    @property
    def pickup(self) -> Rate:
        return Rate(self.connected, self.dials)

    @property
    def engagement(self) -> Rate:
        return Rate(self.engaged, self.connected)

    @property
    def hallucination(self) -> Rate:
        return Rate(self.hallucinations, self.qa_n)

    def rate(self, kpi: Kpi) -> Rate:
        return self.pickup if kpi == "pickup" else self.engagement


def _hour(ts: str | None) -> str | None:
    return None if ts is None else ts[:13]


def kpis(conn: sqlite3.Connection, workflow_id: str, *, by: tuple[str, ...] = (),
         start: str | None = None, end: str | None = None,
         filters: dict[str, object] | None = None) -> list[KpiRow]:
    """Call-level KPI counts for a workflow, grouped by ``by``.

    ``start`` inclusive / ``end`` exclusive, compared at hour granularity on
    normalised ISO timestamps. ``filters`` restricts dimensions, e.g.
    ``{"cohort_id": "c1"}``. Only whitelisted dimension names are accepted.
    """
    for d in (*by, *(filters or {})):
        if d not in DIMENSIONS and d != "hour":
            raise ValueError(f"unknown dimension {d!r}")
    where, args = ["workflow_id = ?"], [workflow_id]
    if start:
        where.append("hour >= ?")
        args.append(_hour(start))
    if end:
        where.append("hour < ?")
        args.append(_hour(end))
    for d, v in (filters or {}).items():
        where.append(f"{d} = ?")
        args.append(v)
    cols = ", ".join(by)
    sql = (f"SELECT {cols + ',' if by else ''} SUM(dials), SUM(connected), SUM(engaged), "
           f"SUM(qa_n), SUM(hallucinations) FROM metric_aggregates "
           f"WHERE {' AND '.join(where)}"
           f"{' GROUP BY ' + cols + ' ORDER BY ' + cols if by else ''}")
    out = []
    for r in conn.execute(sql, args):
        r = tuple(r)
        counts = r[len(by):]
        if counts[0] is None:  # no rows at all
            continue
        out.append(KpiRow(r[: len(by)], *(int(x) for x in counts)))
    return out


def workflow_kpis(conn: sqlite3.Connection, workflow_id: str, **kw) -> KpiRow:
    rows = kpis(conn, workflow_id, **kw)
    return rows[0] if rows else KpiRow((), 0, 0, 0, 0, 0)


# --------------------------------------------------------------------- business outcome


@dataclass(frozen=True)
class BusinessOutcome:
    uploaded: int
    concluded: int
    interested: int

    @property
    def rate(self) -> Rate:
        return Rate(self.concluded, self.uploaded)


def business_outcome(conn: sqlite3.Connection, workflow_id: str, *,
                     start: str | None = None, end: str | None = None,
                     registry: WorkflowRegistry | None = None,
                     by_cohort: bool = False) -> dict[str, BusinessOutcome]:
    """Leads concluded / leads uploaded, for campaigns created in [start, end).

    A lead is concluded if ANY of its calls has a ``concluded_*`` disposition.
    Denominator is leads uploaded (lead events; falls back to the campaign's
    declared lead_count when lead events are missing).

    With ``by_cohort`` leads are assigned using the cohort set of the config
    version active when THEIR campaign was created (requires ``registry``).
    Leads whose campaign config isn't snapshotted are reported as
    ``"unknown"`` rather than guessed.
    """
    where, args = ["cp.workflow_id = ?"], [workflow_id]
    if start:
        where.append("cp.created_ts >= ?")
        args.append(start)
    if end:
        where.append("cp.created_ts < ?")
        args.append(end)
    w = " AND ".join(where)

    if not by_cohort:
        uploaded = 0
        for lead_rows, declared in conn.execute(
                f"SELECT (SELECT COUNT(*) FROM leads l WHERE l.campaign_id = cp.campaign_id), "
                f"cp.lead_count FROM campaigns cp WHERE {w}", args):
            uploaded += lead_rows or (declared or 0)
        concluded, interested = conn.execute(
            f"SELECT COUNT(DISTINCT c.lead_id), "
            f"COUNT(DISTINCT CASE WHEN c.disposition = 'concluded_interested' "
            f"THEN c.lead_id END) "
            f"FROM calls c JOIN campaigns cp ON cp.campaign_id = c.campaign_id "
            f"WHERE {w} AND c.disposition LIKE 'concluded%'", args).fetchone()
        return {WORKFLOW_WIDE: BusinessOutcome(uploaded, concluded or 0, interested or 0)}

    if registry is None:
        raise ValueError("by_cohort requires a WorkflowRegistry")
    status = {r[0]: (r[1], r[2]) for r in conn.execute(
        f"SELECT c.lead_id, MAX(c.disposition LIKE 'concluded%'), "
        f"MAX(c.disposition = 'concluded_interested') "
        f"FROM calls c JOIN campaigns cp ON cp.campaign_id = c.campaign_id "
        f"WHERE {w} GROUP BY c.lead_id", args)}
    sets: dict[int | None, CohortSet | None] = {}
    acc: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for lead_id, feats, version in conn.execute(
            f"SELECT l.lead_id, l.features, cp.config_version FROM leads l "
            f"JOIN campaigns cp ON cp.campaign_id = l.campaign_id WHERE {w}", args):
        if version not in sets:
            sets[version] = registry.cohort_set(workflow_id, version) \
                if version is not None else None
        cs = sets[version]
        cohort = cs.assign(features_from_json(feats)) if cs else "unknown"
        a = acc[cohort]
        a[0] += 1
        c, i = status.get(lead_id, (0, 0))
        a[1] += c or 0
        a[2] += i or 0
    return {k: BusinessOutcome(*v) for k, v in sorted(acc.items())}


# --------------------------------------------------------------------- baselines


@dataclass(frozen=True)
class Baseline:
    workflow_id: str
    kpi: Kpi
    cohort_id: str          # requested cohort
    population: str         # cohort actually used (itself or WORKFLOW_WIDE)
    rate: Rate


def baseline(conn: sqlite3.Connection, workflow_id: str, kpi: Kpi, cohort_id: str, *,
             start: str | None = None, end: str | None = None,
             min_n: int = MIN_COHORT_SAMPLE) -> Baseline:
    """Baseline rate for a cohort, pooled to workflow-wide when the cohort is small."""
    own = workflow_kpis(conn, workflow_id, start=start, end=end,
                        filters={"cohort_id": cohort_id}).rate(kpi)
    if own.den >= min_n:
        return Baseline(workflow_id, kpi, cohort_id, cohort_id, own)
    wide = workflow_kpis(conn, workflow_id, start=start, end=end).rate(kpi)
    return Baseline(workflow_id, kpi, cohort_id, WORKFLOW_WIDE, wide)


def snapshot_baselines(conn: sqlite3.Connection, workflow_id: str, label: str, *,
                       start: str | None = None, end: str | None = None,
                       min_n: int = MIN_COHORT_SAMPLE) -> list[Baseline]:
    """Compute and persist baselines for every observed cohort + workflow-wide.

    Caller owns the transaction. Re-running with the same label replaces it.
    """
    cohorts = [r.key[0] for r in kpis(conn, workflow_id, by=("cohort_id",),
                                      start=start, end=end)]
    out: list[Baseline] = []
    for kpi in KPIS:
        wide = workflow_kpis(conn, workflow_id, start=start, end=end).rate(kpi)
        out.append(Baseline(workflow_id, kpi, WORKFLOW_WIDE, WORKFLOW_WIDE, wide))
        out += [baseline(conn, workflow_id, kpi, c, start=start, end=end, min_n=min_n)
                for c in cohorts]
    conn.executemany(
        "INSERT INTO baselines(workflow_id, label, kpi, cohort_id, population, num, den, "
        "window_start, window_end) VALUES (?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(workflow_id, label, kpi, cohort_id) DO UPDATE SET "
        "population=excluded.population, num=excluded.num, den=excluded.den, "
        "window_start=excluded.window_start, window_end=excluded.window_end, "
        "computed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        [(b.workflow_id, label, b.kpi, b.cohort_id, b.population, b.rate.num, b.rate.den,
          start, end) for b in out])
    return out
