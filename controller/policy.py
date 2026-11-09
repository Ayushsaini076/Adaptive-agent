"""Per-workflow governance policy, read from live workflow metadata.

Nothing here is hardcoded per workflow. Key names follow the spec
(``governance.auto_apply.enabled``, ``allowed_dims``, ``max_shift``,
``approver_hours``, ``max_requests_per_24h``, ``proposal_ttl_hours``,
``apply_window_hours``, ``min_validation_pass_rate``) with a few aliases.

Fail-safe parsing: anything unreadable resolves to the STRICTER choice
(auto-apply off, 1 request/24h, no approver hours known => submit any time
but treat latency as the full TTL). The parsed policy is logged at startup so
a run shows exactly how each workflow's metadata was read.

Operator preferences: stricter, client-requested limits (e.g. a supervisor's
"max 2 requests/day, none before 11:00") live in an optional JSON file keyed by
tenant_id, never in code. They can only TIGHTEN the simulator policy.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from controller import allocations as alloc

log = logging.getLogger(__name__)

PREFERENCES_PATH = os.environ.get("OPERATOR_PREFERENCES", "config/operator_preferences.json")


def _num(v: Any, default: float | None = None) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _hours(v: Any) -> tuple[int, int] | None:
    """[10, 19] | {"start": 10, "end": 19} | "10-19" -> (10, 19); end exclusive."""
    try:
        if isinstance(v, str) and "-" in v:
            a, b = v.split("-", 1)
            return int(a.split(":")[0]), int(b.split(":")[0])
        if isinstance(v, (list, tuple)) and len(v) == 2:
            return int(v[0]), int(v[1])
        if isinstance(v, dict):
            return int(v.get("start", v.get("from"))), int(v.get("end", v.get("to")))
    except (TypeError, ValueError):
        pass
    return None


def _range(v: Any) -> tuple[float, float] | None:
    if isinstance(v, (list, tuple)) and len(v) == 2:
        a, b = _num(v[0]), _num(v[1])
        return (a, b) if a is not None and b is not None else None
    if isinstance(v, dict):
        a, b = _num(v.get("min")), _num(v.get("max"))
        return (a, b) if a is not None and b is not None else None
    return None


@dataclass(frozen=True)
class GovernancePolicy:
    workflow_id: str
    tenant_id: str | None
    auto_apply_enabled: bool = False
    allowed_dims: frozenset[str] = frozenset()
    max_shift: float = 0.0
    approver_hours: tuple[int, int] | None = None
    approver_latency_minutes: tuple[float, float] | None = None
    max_requests_per_24h: int = 1
    proposal_ttl_hours: float = 12.0
    apply_window_hours: float = 2.0
    min_validation_pass_rate: float | None = None
    earliest_submit_hour: int | None = None       # operator preference only
    sources: dict = field(default_factory=dict)   # which value came from where

    @classmethod
    def from_meta(cls, meta: dict, preferences: dict | None = None) -> "GovernancePolicy":
        wf = meta.get("workflow_id", "?")
        g = meta.get("governance") or meta.get("policy") or {}
        aa = g.get("auto_apply")
        if not isinstance(aa, dict):
            aa = {}                     # unreadable => disabled (fail safe)
        enabled = aa.get("enabled") is True
        allowed = frozenset(str(d) for d in (aa.get("allowed_dims") or aa.get("dims") or [])
                            if enabled)
        max_shift = _num(aa.get("max_shift"), 0.0) if enabled else 0.0
        hours = _hours(g.get("approver_hours"))
        latency = _range(g.get("approver_latency_minutes") or g.get("approval_latency_minutes")
                         or g.get("approver_latency_min") or g.get("latency_minutes"))
        max_req = int(_num(g.get("max_requests_per_24h"), 1) or 1)
        ttl = _num(g.get("proposal_ttl_hours"), 12.0)
        window = _num(g.get("apply_window_hours"), 2.0)
        min_pass = _num(g.get("min_validation_pass_rate"))
        sources = {"max_requests_per_24h": "simulator", "earliest_submit_hour": None}

        earliest = None
        pref = (preferences or {}).get(meta.get("tenant_id") or "") or {}
        if pref.get("max_requests_per_24h") is not None:
            tighter = min(max_req, int(pref["max_requests_per_24h"]))
            if tighter < max_req:
                max_req, sources["max_requests_per_24h"] = tighter, "operator preference"
        if pref.get("earliest_submit_hour") is not None:
            earliest = int(pref["earliest_submit_hour"])
            sources["earliest_submit_hour"] = "operator preference"

        return cls(wf, meta.get("tenant_id"), enabled, allowed, max_shift or 0.0, hours,
                   latency, max_req, ttl, window, min_pass, earliest, sources)

    def describe(self) -> dict:
        return {"auto_apply": {"enabled": self.auto_apply_enabled,
                               "allowed_dims": sorted(self.allowed_dims),
                               "max_shift": self.max_shift},
                "approver_hours": self.approver_hours,
                "approver_latency_minutes": self.approver_latency_minutes,
                "max_requests_per_24h": self.max_requests_per_24h,
                "proposal_ttl_hours": self.proposal_ttl_hours,
                "apply_window_hours": self.apply_window_hours,
                "min_validation_pass_rate": self.min_validation_pass_rate,
                "earliest_submit_hour": self.earliest_submit_hour, "sources": self.sources}

    # -- auto-apply -------------------------------------------------------------

    def auto_apply_check(self, current: dict, proposed: dict,
                         changed: list[str]) -> tuple[bool, list[str]]:
        """Every condition of the auto-apply rule, with reasons for each failure."""
        reasons = []
        if not self.auto_apply_enabled:
            reasons.append("auto-apply disabled for this workflow")
        bad_dims = sorted(set(changed) - self.allowed_dims)
        if bad_dims:
            reasons.append(f"dimensions not auto-apply eligible: {bad_dims}")
        for cohort, dim, tv in allocation_shifts(current, proposed):
            if tv > self.max_shift + 1e-9:
                reasons.append(f"{cohort}/{dim} shift {tv:.3f} > max_shift {self.max_shift}")
        return not reasons, reasons

    # -- approval timing ----------------------------------------------------------

    def approver_minutes_within(self, start: datetime, hours: float) -> float:
        """Approver-working minutes in [start, start + hours)."""
        if self.approver_hours is None:
            return hours * 60
        a, b = self.approver_hours
        t, end, total = start, start + timedelta(hours=hours), 0.0
        while t < end:
            step = min(end, t + timedelta(minutes=15))
            if a <= t.hour < b:
                total += (step - t).total_seconds() / 60
            t = step
        return total

    def submission_check(self, now: datetime, sent_last_24h: int) -> tuple[bool, str]:
        """Can a request be sent NOW and still be decided before it expires?"""
        if sent_last_24h >= self.max_requests_per_24h:
            return False, (f"request limit reached: {sent_last_24h}/"
                           f"{self.max_requests_per_24h} in 24h "
                           f"({self.sources['max_requests_per_24h']})")
        if self.earliest_submit_hour is not None and now.hour < self.earliest_submit_hour:
            return False, f"before {self.earliest_submit_hour}:00 (operator preference)"
        need = (self.approver_latency_minutes[1] if self.approver_latency_minutes
                else self.proposal_ttl_hours * 60 / 2)
        have = self.approver_minutes_within(now, self.proposal_ttl_hours)
        if have < need:
            return False, (f"only {have:.0f} approver minutes before the {self.proposal_ttl_hours:.0f}h "
                           f"TTL expires; worst-case latency is {need:.0f}")
        return True, "ok"


def allocation_shifts(current: dict, proposed: dict) -> list[tuple[str, str, float]]:
    """(cohort, dim, TV distance) for every allocation distribution that differs."""
    out = []
    cohorts = set(alloc._by_cohort(current)) | set(alloc._by_cohort(proposed)) | {"default"}
    for cohort in sorted(cohorts):
        for dim in alloc.ALLOCATION_DIMS:
            p = alloc.distribution(current, cohort, dim)
            q = alloc.distribution(proposed, cohort, dim)
            tv = alloc.tv_distance(p, q)
            if tv > 1e-9:
                out.append((cohort, dim, round(tv, 6)))
    return out


def load_preferences(path: str | Path = PREFERENCES_PATH) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        log.error("operator preferences %s unreadable (%s); ignoring", p, e)
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_")}
