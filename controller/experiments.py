"""Controlled experiments on one allocation dimension in one cohort.

Lifecycle (persisted in ``experiments``; every transition in ``experiment_log``):

    planned ──> running ──> decided   (winner | no_winner | guardrail_breach | inconclusive)
       │           └──────> aborted   (allocation changed under us, e.g. supervisor edit)
       └──────> cancelled            (base config changed before the design went live)

M4 scope: plan, detect go-live, count exposures, decide. Getting a planned
design live (approval / auto-apply) and promoting a winner are governance
(M5); this module only produces the proposed config and the recommendation.

Design rules (see README "Experiments" for the rationale):
* One dimension at a time (welcome_message, then voice). caller_id is left
  to caller-ID health (C5) so attributions don't mix. prompt_version is
  workflow-wide and handled by the prompt-incident path (C8).
* One cohort from the canonical cohort model; control = current dominant
  option; 1..max_candidates untested options from the workflow's live
  ``options``; equal traffic split, so control keeps 1/(k+1).
* Sample size per arm from the historical baseline of the PRIMARY KPI,
  powered for ``mde`` at ``power`` with alpha / k (Bonferroni for planning;
  Holm at analysis is never less powerful).
* Unit of analysis: one observation per person. Pickup uses each person's
  first dial in the window; engagement uses each person's first CONNECTED
  dial. The arm is the variant recorded ON THAT CALL. Repeat dials are
  excluded from the test (still in operational KPIs).
* Exposures only count from config versions whose allocation for
  (cohort, dimension) equals the design, from go-live onwards.
* Fixed horizon: the test runs ONCE, when every arm has n_per_arm units or
  the deadline passes. Interim ticks only look at counts, never p-values,
  so there is no peeking.
* The guardrail is a non-inferiority test, so it is sized too: each arm also
  needs enough GUARDRAIL units for the 95% lower bound to clear -tolerance
  if the arms are truly equal. The primary n is raised when the guardrail
  needs more (e.g. engagement guardrail only counts connected calls).
* Winner = Holm-adjusted p <= alpha AND uplift >= min_practical_uplift AND
  guardrail PASS. Among several, the highest lower CI bound wins.
  Guardrail results are three-way: pass, breach (confidently worse than
  tolerance), unverified (not enough evidence either way): unverified is
  never promoted and never reported as a breach.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

from controller import allocations as alloc
from controller.cohorts import MIN_COHORT_SAMPLE
from controller.database import get_state, set_state, transaction
from controller.metrics import Rate
from controller.stats import (
    compare,
    holm_adjust,
    noninferiority_sample_size,
    sample_size_per_arm,
)
from controller.workflows import Kpi, Workflow, WorkflowRegistry

log = logging.getLogger(__name__)

POST_CONNECT_DIMS = ("welcome_message", "voice")
PLANNED, RUNNING, DECIDED, ABORTED, CANCELLED = (
    "planned", "running", "decided", "aborted", "cancelled")
PROMOTED, ROLLED_BACK = "promoted", "rolled_back"
ACTIVE = (PLANNED, RUNNING)
TRANSITIONS = {PLANNED: {RUNNING, CANCELLED}, RUNNING: {DECIDED, ABORTED},
               DECIDED: {PROMOTED}, PROMOTED: {ROLLED_BACK}}


@dataclass(frozen=True)
class ExperimentPolicy:
    dimensions: tuple[str, ...] = ("welcome_message", "voice")
    # welcome_message / voice are heard only AFTER a call connects, so they have
    # no causal path to first-dial pickup. Off by default for pickup-primary.
    post_connect_dims_for_pickup: bool = False
    alpha: float = 0.05
    power: float = 0.80
    mde: float = 0.03                    # TARGET uplift to power for, when the window allows
    max_acceptable_mde: float = 0.05     # largest MDE worth running (business decision)
    min_practical_uplift: float = 0.01   # smallest uplift worth promoting
    max_candidates: int = 2
    max_duration_hours: float | None = None  # optional hard cap on run time after go-live
    fallback_horizon_hours: float = 72.0     # used only when the scenario end is unknown
    approval_buffer_hours: float = 24.0      # time to get the design approved and live
    post_decision_hours: float = 48.0        # promotion approval + after-change measurement
    next_campaign_ramp_hours: float = 24.0
    replan_interval_hours: float = 6.0
    promotion_holdout: float = 0.10      # control share kept after promotion
    min_history_units: int = MIN_COHORT_SAMPLE


# ----------------------------------------------------------------- time helpers


def _dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def add_hours(ts: str, hours: float) -> str:
    return (_dt(ts) + timedelta(hours=hours)).isoformat(timespec="seconds")


def data_clock(conn: sqlite3.Connection, workflow_id: str) -> str | None:
    """Latest call timestamp for the workflow: the controller's notion of 'now'
    for that workflow, derived from data actually ingested."""
    return conn.execute("SELECT MAX(ts) FROM calls WHERE workflow_id = ?",
                        (workflow_id,)).fetchone()[0]


# ----------------------------------------------------------------- exposures


def first_exposures(conn: sqlite3.Connection, workflow_id: str, cohort_id: str, dim: str,
                    kpi: Kpi, *, versions: list[int], start: str,
                    end: str | None = None) -> dict[str, Rate]:
    """Per-arm counts with ONE observation per person (see module docstring)."""
    if dim not in alloc.ALLOCATION_DIMS:
        raise ValueError(f"not an allocation dimension: {dim!r}")
    if not versions:
        return {}
    qs = ",".join("?" * len(versions))
    outcome = "connected" if kpi == "pickup" else "engaged"
    only_connected = "AND connected = 1" if kpi == "engagement" else ""
    sql = f"""
        WITH f AS (
            SELECT {dim} AS arm, {outcome} AS y,
                   ROW_NUMBER() OVER (PARTITION BY COALESCE(person_id, lead_id)
                                      ORDER BY event_id) AS rn
            FROM calls
            WHERE workflow_id = ? AND cohort_id = ? AND ts >= ? AND ts < ?
              AND config_version IN ({qs}) {only_connected})
        SELECT arm, COUNT(*), SUM(y) FROM f WHERE rn = 1 AND arm IS NOT NULL GROUP BY arm"""
    rows = conn.execute(sql, (workflow_id, cohort_id, start, end or "9999",
                              *versions)).fetchall()
    return {arm: Rate(int(y or 0), int(n)) for arm, n, y in rows}


# Carryover unit (option b): for welcome_message / voice the variant is heard only
# once a call connects, so it can affect pickup only on LATER dials. Unit = a
# person whose first connected call in the window was in an arm; outcome =
# whether their NEXT dial (same window, eligible versions) connected.
CARRYOVER, FIRST_EXPOSURE = "carryover_next_dial", "first_exposure"


def guardrail_unit(guardrail_kpi: Kpi, dim: str) -> str:
    return CARRYOVER if guardrail_kpi == "pickup" and dim in POST_CONNECT_DIMS else FIRST_EXPOSURE


def _carryover_sql(dim_expr: str, extra: str) -> str:
    """Per arm: (people observed, people whose next dial connected).

    One ordered pass: LEAD() reads each person's next call. Parameters, in
    order: workflow_id, end, *version filter values, cohort_id, start.

    Candidate "next calls" are any call of the same person in the workflow
    before ``end`` (any cohort, same version filter); the EXPOSURE call is the
    person's first connected call in [start, end) in the cohort. One row per
    person (rn = 1), so repeat calls are never independent observations.
    (Replaces a correlated subquery that could not use an index: O(P x N).)
    """
    return f"""
        WITH seq AS (
            SELECT COALESCE(person_id, lead_id) AS p, event_id, ts, cohort_id, connected,
                   {dim_expr} AS arm,
                   LEAD(connected) OVER (PARTITION BY COALESCE(person_id, lead_id)
                                         ORDER BY event_id) AS y
            FROM calls WHERE workflow_id = ? AND ts < ? {extra}),
        fc AS (
            SELECT arm, y, ROW_NUMBER() OVER (PARTITION BY p ORDER BY event_id) AS rn
            FROM seq WHERE cohort_id = ? AND ts >= ? AND connected = 1)
        SELECT arm, COUNT(y), SUM(y) FROM fc WHERE rn = 1 AND y IS NOT NULL GROUP BY arm"""


def carryover_exposures(conn: sqlite3.Connection, workflow_id: str, cohort_id: str, dim: str,
                        *, versions: list[int], start: str,
                        end: str | None = None) -> dict[str, Rate]:
    """Per arm: pickup of the next dial after each person's first connected call."""
    if dim not in alloc.ALLOCATION_DIMS:
        raise ValueError(f"not an allocation dimension: {dim!r}")
    if not versions:
        return {}
    qs = ",".join("?" * len(versions))
    end = end or "9999"
    sql = _carryover_sql(dim, f"AND config_version IN ({qs})")
    rows = conn.execute(sql, (workflow_id, end, *versions, cohort_id, start)).fetchall()
    return {arm: Rate(int(y or 0), int(n)) for arm, n, y in rows if arm is not None}


def carryover_baseline(conn: sqlite3.Connection, workflow_id: str, cohort_id: str,
                       end: str) -> Rate:
    rows = conn.execute(_carryover_sql("'all'", ""),
                        (workflow_id, end, cohort_id, "0000")).fetchall()
    return Rate(int(rows[0][2] or 0), int(rows[0][1])) if rows else Rate(0, 0)


def carryover_curve(conn: sqlite3.Connection, workflow_id: str, cohort_id: str,
                    end: str) -> list[int]:
    """Cumulative carryover units in the first 1..D days of history."""
    first = conn.execute("SELECT MIN(ts) FROM calls WHERE workflow_id=? AND cohort_id=? AND "
                         "ts < ?", (workflow_id, cohort_id, end)).fetchone()[0]
    if first is None:
        return []
    day0 = _dt(first).replace(hour=0, minute=0, second=0, microsecond=0)
    days = math.ceil((_dt(end) - day0).total_seconds() / 86400)
    curve = []
    for d in range(1, days + 1):
        upto = min(day0 + timedelta(days=d), _dt(end)).isoformat(timespec="seconds")
        rows = conn.execute(_carryover_sql("'all'", ""),
                            (workflow_id, upto, cohort_id, first)).fetchall()
        curve.append(int(rows[0][1]) if rows else 0)
    return curve


def guardrail_exposures(conn: sqlite3.Connection, workflow_id: str, cohort_id: str, dim: str,
                        guardrail_kpi: Kpi, unit: str, **kw) -> dict[str, Rate]:
    if unit == CARRYOVER:
        return carryover_exposures(conn, workflow_id, cohort_id, dim, **kw)
    return first_exposures(conn, workflow_id, cohort_id, dim, guardrail_kpi, **kw)


def unit_curve(conn: sqlite3.Connection, workflow_id: str, cohort_id: str, kpi: Kpi,
               end: str) -> list[int]:
    """Cumulative distinct test units seen in the first 1, 2, ... D days of history.

    This is what an experiment window of d days actually yields: a person dialled
    on several days counts ONCE (one observation per person), unlike a per-day
    average, which counts re-dialled people every day and overstates traffic.
    Units: any dial for pickup, connected dials for engagement.
    """
    only_connected = "AND connected = 1" if kpi == "engagement" else ""
    first = conn.execute(
        f"SELECT MIN(ts) FROM calls WHERE workflow_id = ? AND cohort_id = ? AND ts < ? "
        f"{only_connected}", (workflow_id, cohort_id, end)).fetchone()[0]
    if first is None:
        return []
    day0 = _dt(first).replace(hour=0, minute=0, second=0, microsecond=0)
    days = math.ceil((_dt(end) - day0).total_seconds() / 86400)
    curve = []
    for d in range(1, days + 1):
        upto = min(day0 + timedelta(days=d), _dt(end)).isoformat(timespec="seconds")
        curve.append(conn.execute(
            f"SELECT COUNT(DISTINCT COALESCE(person_id, lead_id)) FROM calls "
            f"WHERE workflow_id = ? AND cohort_id = ? AND ts >= ? AND ts < ? {only_connected}",
            (workflow_id, cohort_id, first, upto)).fetchone()[0])
    return curve


def new_units_per_day(curve: list[int]) -> int:
    """Steady-state NEW units per day: the last history day's increment."""
    if not curve:
        return 0
    return curve[-1] - curve[-2] if len(curve) >= 2 else curve[-1]


def units_within(curve: list[int], days: int, *, new_only: bool = False) -> int:
    """Units an experiment of ``days`` whole days collects (pessimistic).

    Beyond the history length, extends with new units per day. ``new_only``
    (next_campaign_only workflows) counts only people arriving after go-live.
    """
    if days <= 0 or not curve:
        return 0
    per_day = new_units_per_day(curve)
    if new_only:
        return days * per_day
    if days <= len(curve):
        return curve[days - 1]
    return curve[-1] + (days - len(curve)) * per_day


def days_to_reach(curve: list[int], units: int, max_days: int, *,
                  new_only: bool = False) -> int | None:
    for d in range(1, max_days + 1):
        if units_within(curve, d, new_only=new_only) >= units:
            return d
    return None


def first_exposure_baseline(conn: sqlite3.Connection, workflow_id: str, cohort_id: str,
                            kpi: Kpi, end: str) -> Rate:
    """History rate on the SAME unit the test uses: each person's first dial
    (pickup) or first connected dial (engagement)."""
    outcome = "connected" if kpi == "pickup" else "engaged"
    only_connected = "AND connected = 1" if kpi == "engagement" else ""
    n, y = conn.execute(
        f"WITH f AS (SELECT {outcome} AS y, ROW_NUMBER() OVER ("
        f"PARTITION BY COALESCE(person_id, lead_id) ORDER BY event_id) rn FROM calls "
        f"WHERE workflow_id = ? AND cohort_id = ? AND ts < ? {only_connected}) "
        f"SELECT COUNT(*), SUM(y) FROM f WHERE rn = 1",
        (workflow_id, cohort_id, end)).fetchone()
    return Rate(int(y or 0), int(n or 0))


def planning_horizon(conn: sqlite3.Connection, policy: ExperimentPolicy,
                     ramp_hours: float) -> tuple[float, str]:
    """Hours an experiment may run after go-live, and where that number came from.

    = time left in the scenario - approval buffer - post-decision time - ramp,
    so a decided winner can still be promoted and measured before the end.
    """
    end, now = get_state(conn, "end_minute"), get_state(conn, "sim_minute")
    if end not in (None, "None") and now not in (None, "None"):
        hours = ((int(end) - int(now)) / 60 - policy.approval_buffer_hours
                 - policy.post_decision_hours - ramp_hours)
        source = (f"scenario: {(int(end) - int(now)) / 60:.0f}h left - "
                  f"{policy.approval_buffer_hours:.0f}h approval - "
                  f"{policy.post_decision_hours:.0f}h post-decision - {ramp_hours:.0f}h ramp")
    else:
        hours = policy.fallback_horizon_hours - ramp_hours
        source = f"fallback {policy.fallback_horizon_hours:.0f}h - {ramp_hours:.0f}h ramp"
    if policy.max_duration_hours is not None and hours > policy.max_duration_hours:
        hours, source = policy.max_duration_hours, f"cap {policy.max_duration_hours:.0f}h"
    return max(hours, 0.0), source


def design_mde(p0: float, per_arm_available: int, policy: ExperimentPolicy,
               k: int) -> float | None:
    """Smallest MDE the window can power, preferring the target.

    Returns the target if affordable, else the smallest affordable MDE up to
    ``max_acceptable_mde`` (rounded UP to 0.1pp), else None. Alpha and power
    never change: a larger MDE means more missed small effects, not more
    false winners.
    """
    alpha = policy.alpha / k

    def n(m: float) -> int:
        return sample_size_per_arm(p0, m, alpha=alpha, power=policy.power)

    try:
        if n(policy.mde) <= per_arm_available:
            return policy.mde
        hi = max(policy.max_acceptable_mde, policy.mde)
        if n(hi) > per_arm_available:
            return None
    except ValueError:
        return None
    lo = policy.mde
    for _ in range(40):
        mid = (lo + hi) / 2
        if n(mid) <= per_arm_available:
            hi = mid
        else:
            lo = mid
    return math.ceil(hi * 1000) / 1000


# ----------------------------------------------------------------- planning


@dataclass
class Design:
    experiment_id: str
    workflow_id: str
    cohort_id: str
    dimension: str
    control: str
    candidates: list[str]
    primary_kpi: Kpi
    guardrail_kpi: Kpi
    n_per_arm: int
    n_guardrail_per_arm: int
    horizon_hours: float
    base_version: int
    allocation: dict[str, float]
    proposed_config: dict
    evidence: dict = field(default_factory=dict)


def _tested(conn: sqlite3.Connection, wf: str, cohort: str, dim: str) -> set[str]:
    """Options that actually received a statistical test in a decided experiment.

    A candidate that was planned but under-exposed (or never exposed) is NOT
    tested and may be planned again.
    """
    out: set[str] = set()
    for (decision,) in conn.execute(
            "SELECT decision FROM experiments WHERE workflow_id=? AND cohort_id=? "
            "AND dimension=? AND status=?", (wf, cohort, dim, DECIDED)):
        out.update((json.loads(decision or "{}").get("tests") or {}).keys())
    return out


def _history_end(conn: sqlite3.Connection, wf: str) -> str | None:
    row = conn.execute("SELECT MAX(window_end) FROM baselines WHERE workflow_id=? AND "
                       "label='history'", (wf,)).fetchone()
    return row[0] if row else None


def _baseline_rate(conn: sqlite3.Connection, wf: str, cohort: str, kpi: Kpi) -> Rate | None:
    row = conn.execute("SELECT num, den FROM baselines WHERE workflow_id=? AND label='history' "
                       "AND kpi=? AND cohort_id=?", (wf, kpi, cohort)).fetchone()
    return Rate(row[0], row[1]) if row else None


def _observed_control(conn: sqlite3.Connection, wf: str, cohort: str, dim: str,
                      end: str) -> str | None:
    row = conn.execute(
        f"SELECT {dim}, COUNT(*) c FROM calls WHERE workflow_id=? AND cohort_id=? AND ts<? "
        f"AND {dim} IS NOT NULL GROUP BY {dim} ORDER BY c DESC, {dim} LIMIT 1",
        (wf, cohort, end)).fetchone()
    return row[0] if row else None


def plan(conn: sqlite3.Connection, registry: WorkflowRegistry, workflow_id: str,
         policy: ExperimentPolicy = ExperimentPolicy()) -> tuple[Design | None, list[str]]:
    """Deterministically pick the first feasible design, or explain why none.

    Search order: dimensions in policy order; cohorts by descending historical
    volume; most candidates first (fewer arms if the larger design can't
    finish before the deadline).
    """
    reasons: list[str] = []
    wf: Workflow = registry.get(workflow_id)
    cfg = registry.config(workflow_id)
    version = registry.current_version(workflow_id)
    cohort_set = registry.cohort_set(workflow_id)
    hist_end = _history_end(conn, workflow_id)
    if cfg is None or version is None or cohort_set is None or hist_end is None:
        return None, ["no config snapshot or history baseline yet"]
    kpi, guard = wf.objective.primary, wf.objective.guardrail
    new_only = wf.mid_flight == "next_campaign_only"
    ramp = policy.next_campaign_ramp_hours if new_only else 0.0
    horizon, horizon_src = planning_horizon(conn, policy, ramp)
    max_days = int(horizon // 24)
    if max_days < 1:
        return None, [f"horizon {horizon:.0f}h ({horizon_src}) is under one day"]

    volumes = []
    for cohort in cohort_set.ids:
        curve = unit_curve(conn, workflow_id, cohort, kpi, hist_end)
        total = curve[-1] if curve else 0
        if total < policy.min_history_units:
            reasons.append(f"{cohort}: {total} historical units < {policy.min_history_units}")
            continue
        volumes.append((units_within(curve, max_days, new_only=new_only), cohort, curve))
    volumes.sort(key=lambda v: (-v[0], v[1]))
    sizing_memo: dict[tuple[str, str], tuple] = {}

    for dim in policy.dimensions:
        if (kpi == "pickup" and dim in POST_CONNECT_DIMS
                and not policy.post_connect_dims_for_pickup):
            reasons.append(f"{dim}: acts only after a call connects, so it cannot move the "
                           f"primary KPI (pickup); not tested")
            continue
        opts = alloc.options(wf.meta, dim)
        if len(opts) < 2:
            reasons.append(f"{dim}: fewer than 2 options exposed")
            continue
        for window_units, cohort, curve in volumes:
            current = alloc.distribution(cfg, cohort, dim)
            control = (max(sorted(current), key=current.get) if current
                       else _observed_control(conn, workflow_id, cohort, dim, hist_end))
            if control is None or control not in opts:
                reasons.append(f"{dim}/{cohort}: control {control!r} not in options")
                continue
            done = _tested(conn, workflow_id, cohort, dim)
            untested = [o for o in opts if o != control and o not in done]
            if not untested:
                reasons.append(f"{dim}/{cohort}: all options already tested")
                continue
            base = first_exposure_baseline(conn, workflow_id, cohort, kpi, hist_end)
            if not base.value or base.value >= 1:
                reasons.append(f"{dim}/{cohort}: no usable first-exposure {kpi} baseline")
                continue
            g_unit = guardrail_unit(guard, dim)
            key = (cohort, g_unit)             # same for every dimension with this unit
            if key not in sizing_memo:
                sizing_memo[key] = _guardrail_sizing(conn, wf, cohort, hist_end, policy, g_unit)
            n_guard, g_base, g_curve = sizing_memo[key]
            if n_guard < 0:
                unit_txt = (" (unit: next dial after a connected exposure)"
                            if g_unit == CARRYOVER else "")
                reasons.append(f"{dim}/{cohort}: guardrail {guard}{unit_txt} has a tolerance "
                               f"but no history observations; cannot size non-inferiority")
                continue
            g_window = units_within(g_curve, max_days, new_only=new_only) if n_guard else 0
            for k in range(min(policy.max_candidates, len(untested)), 0, -1):
                arms_n = k + 1
                per_arm = window_units // arms_n
                g_per_arm = g_window // arms_n
                where = (f"{dim}/{cohort}/k={k}: horizon {horizon:.0f}h ({horizon_src}), "
                         f"window gives {per_arm} {kpi} units/arm")
                if n_guard and g_per_arm < n_guard:
                    unit_txt = (" (unit: next dial after a connected exposure)"
                                if g_unit == CARRYOVER else "")
                    reasons.append(f"{where}; guardrail {guard}{unit_txt} non-inferiority "
                                   f"needs {n_guard}/arm, window gives {g_per_arm}/arm")
                    continue
                mde = design_mde(base.value, per_arm, policy, k)
                if mde is None:
                    try:
                        need = sample_size_per_arm(
                            base.value, max(policy.max_acceptable_mde, policy.mde),
                            alpha=policy.alpha / k, power=policy.power)
                    except ValueError:
                        need = None
                    reasons.append(f"{where}; even the max acceptable MDE "
                                   f"{policy.max_acceptable_mde:.3f} needs {need}/arm")
                    continue
                n_primary = sample_size_per_arm(base.value, mde, alpha=policy.alpha / k,
                                                power=policy.power)
                d_primary = days_to_reach(curve, n_primary * arms_n, max_days,
                                          new_only=new_only)
                d_guard = (days_to_reach(g_curve, n_guard * arms_n, max_days,
                                         new_only=new_only) if n_guard else 0)
                expected_hours = max(d_primary or max_days, d_guard or 0) * 24 + ramp
                cands = untested[:k]
                arms = [control, *cands]
                split = {a: 1 / len(arms) for a in arms}
                exp_id = f"exp:{workflow_id}:{cohort}:{dim}:{'+'.join(cands)}:v{version}"
                design = Design(
                    experiment_id=exp_id, workflow_id=workflow_id, cohort_id=cohort,
                    dimension=dim, control=control, candidates=cands, primary_kpi=kpi,
                    guardrail_kpi=guard, n_per_arm=n_primary, n_guardrail_per_arm=n_guard,
                    horizon_hours=horizon, base_version=version, allocation=split,
                    proposed_config=_safe(conn, wf, alloc.with_distribution(
                        cfg, cohort, dim, split)),
                    evidence={
                        "baseline": {"kpi": kpi, "unit": "first exposure per person",
                                     **base.as_dict(), "window_end": hist_end},
                        "unit": ("first dial per person" if kpi == "pickup"
                                 else "first connected dial per person"),
                        "traffic": {"history_curve": curve,
                                    "new_units_per_day": new_units_per_day(curve),
                                    "new_only": new_only,
                                    "window_units": window_units, "window_days": max_days},
                        "sizing": {"target_mde": policy.mde, "design_mde": mde,
                                   "max_acceptable_mde": policy.max_acceptable_mde,
                                   "alpha_per_comparison": policy.alpha / k,
                                   "power": policy.power, "n_primary": n_primary,
                                   "n_guardrail": n_guard, "guardrail_baseline": g_base,
                                   "guardrail_window_units": g_window,
                                   "guardrail_unit": g_unit,
                                   "n_per_arm": n_primary},
                        "horizon_hours": round(horizon, 1), "horizon_source": horizon_src,
                        "expected_hours": round(expected_hours, 1), "ramp_hours": ramp,
                        "mid_flight": wf.mid_flight,
                        "current_distribution": current,
                    })
                return design, reasons
    return None, reasons


def _safe(conn: sqlite3.Connection, wf: Workflow, cfg: dict) -> dict:
    """Never propose a config that dials a caller ID with an open incident."""
    from controller.incidents import open_caller_id_incidents, without_caller_ids
    bad = open_caller_id_incidents(conn, wf.workflow_id)
    if not bad:
        return cfg
    safe, _ = without_caller_ids(cfg, bad, alloc.options(wf.meta, "caller_id"))
    return safe


def _guardrail_sizing(conn: sqlite3.Connection, wf: Workflow, cohort: str, hist_end: str,
                      policy: ExperimentPolicy, unit: str = FIRST_EXPOSURE
                      ) -> tuple[int, dict | None, list[int]]:
    """(guardrail units needed per arm, baseline evidence, guardrail unit curve).

    0 units when the workflow states no tolerance (then the guardrail check is
    'no significant drop', which needs no dedicated sizing).
    """
    obj = wf.objective
    if obj.guardrail_max_drop is None:
        return 0, None, []
    g_base = (carryover_baseline(conn, wf.workflow_id, cohort, hist_end) if unit == CARRYOVER
              else first_exposure_baseline(conn, wf.workflow_id, cohort, obj.guardrail, hist_end))
    if g_base.value is None:
        return -1, {"unit": unit}, []    # tolerance stated but nothing to size it from
    margin = obj.guardrail_max_drop * (g_base.value if obj.guardrail_relative else 1.0)
    if margin <= 0:
        return 0, None, []
    n = noninferiority_sample_size(g_base.value, margin, alpha=policy.alpha,
                                   power=policy.power)
    curve = (carryover_curve(conn, wf.workflow_id, cohort, hist_end) if unit == CARRYOVER
             else unit_curve(conn, wf.workflow_id, cohort, obj.guardrail, hist_end))
    return n, {**g_base.as_dict(), "margin": round(margin, 4), "unit": unit}, curve


# ----------------------------------------------------------------- evaluation


PASS, BREACH, UNVERIFIED = "pass", "breach", "unverified"


@dataclass(frozen=True)
class GuardrailCheck:
    status: str                    # pass | breach | unverified
    tolerance: float | None
    comparison: dict | None
    reason: str

    @property
    def passed(self) -> bool:
        return self.status == PASS


def guardrail_check(control: Rate, treatment: Rate, max_drop: float | None,
                    relative: bool) -> GuardrailCheck:
    """Non-inferiority on the guardrail KPI, with a 95% CI on (t - c).

    With a tolerance:
      pass       lower bound >= -tol   (confident the drop is within tolerance)
      breach     upper bound <  -tol   (confident the drop exceeds tolerance)
      unverified otherwise             (CI straddles -tol: not enough evidence)
    Without a stated tolerance:
      breach if upper bound < 0 (significant drop), else pass.
    No guardrail observations at all => unverified.
    """
    c = compare(control, treatment)
    if c is None:
        return GuardrailCheck(UNVERIFIED, max_drop, None, "no guardrail observations")
    lo, hi = c.ci
    if max_drop is None:
        status = BREACH if hi < 0 else PASS
        why = f"no tolerance configured; 95% CI [{lo:+.4f}, {hi:+.4f}] vs 0"
        return GuardrailCheck(status, None, c.as_dict(), why)
    tol = max_drop * control.value if relative else max_drop
    status = PASS if lo >= -tol else BREACH if hi < -tol else UNVERIFIED
    why = f"95% CI [{lo:+.4f}, {hi:+.4f}] vs -{tol:.4f}"
    return GuardrailCheck(status, tol, c.as_dict(), why)


def decide(*, control: str, candidates: list[str], n_per_arm: int,
           primary: dict[str, Rate], guardrail: dict[str, Rate], alpha: float,
           min_practical_uplift: float, max_drop: float | None, relative: bool,
           deadline_passed: bool) -> dict:
    """Pure decision function. Same inputs -> same decision."""
    empty = Rate(0, 0)
    arms = {a: {"primary": primary.get(a, empty).as_dict(),
                "guardrail": guardrail.get(a, empty).as_dict()} for a in [control, *candidates]}
    exposed = [c for c in candidates if primary.get(c, empty).den >= n_per_arm]
    under = [c for c in candidates if c not in exposed]
    base = {"arms": arms, "n_per_arm": n_per_arm, "under_exposed": under,
            "deadline_passed": deadline_passed}
    if primary.get(control, empty).den < n_per_arm or not exposed:
        return {**base, "outcome": "inconclusive", "winner": None,
                "reason": "control or all candidates below the required sample"}

    tests, pvals = {}, {}
    for cand in candidates:   # under-exposed arms stay in the family with p = 1
        cmp = compare(primary[control], primary[cand]) if cand in exposed else None
        tests[cand] = cmp
        pvals[cand] = cmp.p_value if cmp else 1.0
    adjusted = holm_adjust(pvals)

    results, winners, breaches, unverified = {}, [], [], []
    for cand in exposed:
        cmp = tests[cand]
        significant = adjusted[cand] <= alpha and cmp.diff > 0
        practical = cmp.diff >= min_practical_uplift
        g = guardrail_check(guardrail.get(control, empty), guardrail.get(cand, empty),
                            max_drop, relative)
        results[cand] = {"primary": cmp.as_dict(), "holm_p": adjusted[cand],
                         "significant": significant, "practical": practical,
                         "guardrail": asdict(g)}
        if significant and practical:
            {PASS: winners, BREACH: breaches, UNVERIFIED: unverified}[g.status].append(cand)

    base["tests"] = results
    if winners:
        best = max(winners, key=lambda c: (tests[c].ci[0], -candidates.index(c)))
        return {**base, "outcome": "winner", "winner": best,
                "reason": f"{best}: Holm p {adjusted[best]:.4g} <= {alpha}, uplift "
                          f"{tests[best].diff:+.4f} >= {min_practical_uplift}, guardrail ok"}
    if breaches:
        return {**base, "outcome": "guardrail_breach", "winner": None,
                "reason": f"significant primary uplift but guardrail breached: {breaches}"}
    if unverified:
        return {**base, "outcome": "guardrail_unverified", "winner": None,
                "reason": f"significant primary uplift but guardrail not verifiable: "
                          f"{unverified}"}
    return {**base, "outcome": "no_winner", "winner": None,
            "reason": "no candidate significant and practically meaningful"}


def promotion_harm(conn: sqlite3.Connection, registry: WorkflowRegistry, exp: sqlite3.Row,
                   *, min_n: int = 200, z_harm: float = 3.0) -> dict | None:
    """After promotion: is the winner now HARMING vs the control holdout?

    Harm-only monitoring (C7). One-sided: primary z <= -z_harm (p ~ 0.0013) with
    >= min_n first exposures per arm, or a guardrail BREACH (confidently worse
    than tolerance). It can only trigger a rollback, never a new promotion, so
    repeated looks don't inflate the winner false-positive rate.
    """
    decision = json.loads(exp["decision"] or "{}")
    rec = decision.get("recommendation") or {}
    winner, control = decision.get("winner"), exp["control"]
    if not winner or not exp["promoted_ts"]:
        return None
    wf, cohort, dim = exp["workflow_id"], exp["cohort_id"], exp["dimension"]
    versions = [r[0] for r in conn.execute(
        "SELECT version FROM workflow_config_snapshots WHERE workflow_id=? AND version>=? "
        "ORDER BY version", (wf, exp["promoted_version"]))
        if alloc.same_distribution(
            alloc.distribution(registry.config(wf, r[0]) or {}, cohort, dim),
            rec.get("allocation") or {})]
    args = (conn, wf, cohort, dim)
    primary = first_exposures(*args, exp["primary_kpi"], versions=versions,
                              start=exp["promoted_ts"])
    unit = json.loads(exp["design"]).get("sizing", {}).get("guardrail_unit", FIRST_EXPOSURE)
    guard = guardrail_exposures(*args, exp["guardrail_kpi"], unit, versions=versions,
                                start=exp["promoted_ts"])
    empty = Rate(0, 0)
    if min(primary.get(winner, empty).den, primary.get(control, empty).den) < min_n:
        return None
    c = compare(primary[control], primary[winner])
    if c.z <= -z_harm:
        return {"kpi": exp["primary_kpi"], "reason": f"winner worse than holdout, z={c.z:.2f}",
                "comparison": c.as_dict()}
    g = guardrail_check(guard.get(control, empty), guard.get(winner, empty),
                        exp["guardrail_max_drop"], bool(exp["guardrail_relative"]))
    if g.status == BREACH:
        return {"kpi": exp["guardrail_kpi"], "reason": f"guardrail breach: {g.reason}",
                "comparison": g.comparison}
    return None


# ----------------------------------------------------------------- persistence


class ExperimentStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def create(self, d: Design, policy: ExperimentPolicy, wf: Workflow, now: str) -> bool:
        """Insert a planned experiment. Returns False if the id already exists."""
        obj = wf.objective
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO experiments(experiment_id, workflow_id, cohort_id, "
            "dimension, control, candidates, primary_kpi, guardrail_kpi, guardrail_max_drop, "
            "guardrail_relative, alpha, power, mde, min_practical_uplift, n_per_arm, "
            "n_guardrail_per_arm, max_duration_hours, base_version, allocation, "
            "proposed_config, design, status, created_ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (d.experiment_id, d.workflow_id, d.cohort_id, d.dimension, d.control,
             json.dumps(d.candidates), d.primary_kpi, d.guardrail_kpi, obj.guardrail_max_drop,
             int(obj.guardrail_relative), policy.alpha, policy.power,
             d.evidence["sizing"]["design_mde"],
             policy.min_practical_uplift, d.n_per_arm, d.n_guardrail_per_arm,
             d.horizon_hours,
             d.base_version, json.dumps(d.allocation, sort_keys=True),
             json.dumps(d.proposed_config, sort_keys=True), json.dumps(d.evidence),
             PLANNED, now))
        if cur.rowcount:
            self._log(d.experiment_id, now, None, PLANNED, d.evidence)
        return bool(cur.rowcount)

    def transition(self, exp_id: str, to: str, now: str, detail: dict | None = None,
                   **fields: object) -> None:
        row = self.conn.execute("SELECT status FROM experiments WHERE experiment_id=?",
                                (exp_id,)).fetchone()
        if row is None:
            raise KeyError(exp_id)
        if to not in TRANSITIONS.get(row[0], set()):
            raise ValueError(f"{exp_id}: illegal transition {row[0]} -> {to}")
        sets = ", ".join(f"{k} = ?" for k in fields)
        self.conn.execute(
            f"UPDATE experiments SET status = ?{', ' + sets if sets else ''}, "
            f"updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE experiment_id = ?",
            (to, *fields.values(), exp_id))
        self._log(exp_id, now, row[0], to, detail)

    def _log(self, exp_id: str, now: str, frm: str | None, to: str,
             detail: dict | None) -> None:
        self.conn.execute(
            "INSERT INTO experiment_log(experiment_id, sim_ts, from_status, to_status, detail) "
            "VALUES (?,?,?,?,?)", (exp_id, now, frm, to, json.dumps(detail or {})))

    def active(self, workflow_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM experiments WHERE workflow_id=? AND status IN (?,?) "
            "ORDER BY created_ts LIMIT 1", (workflow_id, *ACTIVE)).fetchone()

    def get(self, exp_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM experiments WHERE experiment_id=?",
                                 (exp_id,)).fetchone()


# ----------------------------------------------------------------- manager


class ExperimentManager:
    """Drives the lifecycle once per tick. Every state change is one transaction."""

    def __init__(self, conn: sqlite3.Connection, registry: WorkflowRegistry,
                 policy: ExperimentPolicy = ExperimentPolicy()) -> None:
        self.conn = conn
        self.registry = registry
        self.policy = policy
        self.store = ExperimentStore(conn)

    def tick(self) -> list[tuple[str, str]]:
        """Advance every workflow's experiment by at most one step.
        Returns (experiment_id, new_status) for each change."""
        changes = []
        for wf_id in self.registry.ids():
            now = data_clock(self.conn, wf_id)
            if now is None:
                continue
            exp = self.store.active(wf_id)
            if exp is None:
                new = self._maybe_plan(wf_id, now)
            elif exp["status"] == PLANNED:
                new = self._maybe_start_or_cancel(exp, now)
            else:
                new = self._maybe_abort_or_decide(exp, now)
            if new:
                changes.append(new)
        return changes

    # -- planned -----------------------------------------------------------

    def _maybe_plan(self, wf_id: str, now: str) -> tuple[str, str] | None:
        key = f"exp_plan_attempt:{wf_id}"
        last = get_state(self.conn, key)
        if last and _dt(now) < _dt(add_hours(last, self.policy.replan_interval_hours)):
            return None
        design, reasons = plan(self.conn, self.registry, wf_id, self.policy)
        with transaction(self.conn):
            set_state(self.conn, key, now)
            if design is None:
                log.info("%s: no feasible experiment: %s", wf_id, " | ".join(reasons))
                return None
            if not self.store.create(design, self.policy, self.registry.get(wf_id), now):
                return None
        log.info("%s: planned %s (n=%d/arm, ~%sh)", wf_id, design.experiment_id,
                 design.n_per_arm, design.evidence["expected_hours"])
        return design.experiment_id, PLANNED

    def _live_matches(self, exp: sqlite3.Row, version: int | None) -> bool:
        cfg = self.registry.config(exp["workflow_id"], version)
        if cfg is None:
            return False
        return alloc.same_distribution(
            alloc.distribution(cfg, exp["cohort_id"], exp["dimension"]),
            json.loads(exp["allocation"]))

    def _maybe_start_or_cancel(self, exp: sqlite3.Row, now: str) -> tuple[str, str] | None:
        version = self.registry.current_version(exp["workflow_id"])
        if self._live_matches(exp, version):
            with transaction(self.conn):
                self.store.transition(
                    exp["experiment_id"], RUNNING, now, {"start_version": version},
                    start_version=version, start_ts=now,
                    deadline_ts=add_hours(now, self._run_hours(exp)))
            return exp["experiment_id"], RUNNING
        if version != exp["base_version"]:
            with transaction(self.conn):
                self.store.transition(exp["experiment_id"], CANCELLED, now, {
                    "reason": "base config changed before go-live",
                    "base_version": exp["base_version"], "current_version": version})
            return exp["experiment_id"], CANCELLED
        return None   # waiting for governance (M5) to make it live

    def _run_hours(self, exp: sqlite3.Row) -> float:
        """Run time from go-live: the planned horizon, but never past the point
        where the post-decision time (promotion + measurement) no longer fits."""
        hours = exp["max_duration_hours"]
        end, now = get_state(self.conn, "end_minute"), get_state(self.conn, "sim_minute")
        if end not in (None, "None") and now not in (None, "None"):
            left = (int(end) - int(now)) / 60 - self.policy.post_decision_hours
            hours = min(hours, max(left, 0.0))
        return hours

    # -- running -------------------------------------------------------------

    def eligible_versions(self, exp: sqlite3.Row) -> list[int]:
        rows = self.conn.execute(
            "SELECT version FROM workflow_config_snapshots WHERE workflow_id=? AND version>=? "
            "ORDER BY version", (exp["workflow_id"], exp["start_version"])).fetchall()
        return [r[0] for r in rows if self._live_matches(exp, r[0])]

    def _maybe_abort_or_decide(self, exp: sqlite3.Row, now: str) -> tuple[str, str] | None:
        exp_id = exp["experiment_id"]
        if not self._live_matches(exp, self.registry.current_version(exp["workflow_id"])):
            actor = self.conn.execute(
                "SELECT actor, new_version FROM config_changes WHERE workflow_id=? "
                "ORDER BY event_id DESC LIMIT 1", (exp["workflow_id"],)).fetchone()
            with transaction(self.conn):
                self.store.transition(exp_id, ABORTED, now, {
                    "reason": "allocation under test changed",
                    "changed_by": actor[0] if actor else None,
                    "version": actor[1] if actor else None})
            return exp_id, ABORTED

        self._note_continuation(exp, now)
        versions = self.eligible_versions(exp)
        args = (self.conn, exp["workflow_id"], exp["cohort_id"], exp["dimension"])
        primary = first_exposures(*args, exp["primary_kpi"], versions=versions,
                                  start=exp["start_ts"])
        unit = json.loads(exp["design"]).get("sizing", {}).get("guardrail_unit",
                                                               FIRST_EXPOSURE)
        guardrail = guardrail_exposures(*args, exp["guardrail_kpi"], unit, versions=versions,
                                    start=exp["start_ts"])
        candidates = json.loads(exp["candidates"])
        arms = [exp["control"], *candidates]
        empty = Rate(0, 0)
        enough = all(primary.get(a, empty).den >= exp["n_per_arm"]
                     and guardrail.get(a, empty).den >= exp["n_guardrail_per_arm"]
                     for a in arms)
        deadline_passed = _dt(now) >= _dt(exp["deadline_ts"])
        if not (enough or deadline_passed):
            return None   # interim ticks look at counts only, never at p-values
        decision = decide(
            control=exp["control"], candidates=candidates, n_per_arm=exp["n_per_arm"],
            primary=primary, guardrail=guardrail, alpha=exp["alpha"],
            min_practical_uplift=exp["min_practical_uplift"],
            max_drop=exp["guardrail_max_drop"], relative=bool(exp["guardrail_relative"]),
            deadline_passed=deadline_passed)
        decision["evaluated_at"] = now
        decision["versions"] = versions
        decision["other_arms"] = sorted(set(primary) - set(arms))
        if decision["outcome"] == "winner":
            decision["recommendation"] = self._promotion(exp, decision["winner"])
        with transaction(self.conn):
            self.conn.executemany(
                "INSERT INTO experiment_evaluations(experiment_id, sim_ts, arm, kpi, num, den) "
                "VALUES (?,?,?,?,?,?)",
                [(exp_id, now, arm, kpi, r.num, r.den)
                 for kpi, counts in (("primary", primary), ("guardrail", guardrail))
                 for arm, r in counts.items()])
            self.store.transition(exp_id, DECIDED, now, {"outcome": decision["outcome"]},
                                  decision=json.dumps(decision), decided_ts=now)
        log.info("%s decided: %s (%s)", exp_id, decision["outcome"], decision["reason"])
        return exp_id, DECIDED

    def _note_continuation(self, exp: sqlite3.Row, now: str) -> None:
        """Log (once per version) that a running experiment continues across a
        config change that left its allocation under test untouched."""
        version = self.registry.current_version(exp["workflow_id"])
        key = f"exp_seen_version:{exp['experiment_id']}"
        seen = get_state(self.conn, key)
        if seen in (None, "None"):
            seen = str(exp["start_version"])
        if version is None or str(version) == seen:
            return
        change = self.conn.execute(
            "SELECT actor, changed_dims FROM config_changes WHERE workflow_id=? AND "
            "new_version=? ORDER BY event_id DESC LIMIT 1",
            (exp["workflow_id"], version)).fetchone()
        detail = {"version": version, "changed_by": change[0] if change else None,
                  "changed_dims": json.loads(change[1]) if change else None,
                  "reason": "allocation under test unchanged; both arms see the change "
                            "concurrently, so the comparison stays randomised"}
        with transaction(self.conn):
            set_state(self.conn, key, version)
            self.store._log(exp["experiment_id"], now, RUNNING, RUNNING, detail)
        log.info("experiment %s continues across %s v%s (changed by %s: %s): %s",
                 exp["experiment_id"], exp["workflow_id"], version, detail["changed_by"],
                 detail["changed_dims"], detail["reason"])

    def _promotion(self, exp: sqlite3.Row, winner: str) -> dict:
        """Recommended (not applied) config: winner gets 1 - holdout, control keeps
        the holdout so the effect can keep being measured for decay (C7)."""
        h = self.policy.promotion_holdout
        dist = {winner: 1 - h, exp["control"]: h} if h > 0 else {winner: 1.0}
        cfg = self.registry.config(exp["workflow_id"]) or {}
        wf = self.registry.get(exp["workflow_id"])
        return {"cohort_id": exp["cohort_id"], "dimension": exp["dimension"],
                "allocation": dist, "holdout": h,
                "proposed_config": _safe(self.conn, wf, alloc.with_distribution(
                    cfg, exp["cohort_id"], exp["dimension"], dist))}
