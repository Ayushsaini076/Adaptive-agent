"""Validation and normalisation of raw simulator events.

This module is the ONLY place that knows raw event field names. The spec
names the event types and their contents but not every exact key, so each
lookup accepts a small set of aliases. Confirm against the live ``/docs``
and tighten the aliases when a simulator is available.

``normalise`` returns plain tuples ready for ``executemany`` so the hot
ingestion path does no extra allocation beyond what it needs.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

JSON = dict[str, Any]

KNOWN_TYPES = {"campaign_created", "lead", "call", "config_changed", "approval_decided"}
VARIANT_DIMS = ("welcome_message", "voice", "caller_id", "prompt_version")


class InvalidEvent(ValueError):
    pass


def _get(d: JSON, *keys: str, default: Any = None) -> Any:
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


def _require(d: JSON, *keys: str) -> Any:
    v = _get(d, *keys)
    if v is None:
        raise InvalidEvent(f"missing {keys[0]}")
    return v


def _int_or_none(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        raise InvalidEvent(f"expected integer, got {v!r}") from None


def _bool01(v: Any, name: str) -> int:
    if isinstance(v, bool):
        return int(v)
    if v in (0, 1):
        return int(v)
    raise InvalidEvent(f"{name} must be boolean, got {v!r}")


def _dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True)


def event_id(raw: JSON) -> int:
    v = _require(raw, "event_id", "id")
    eid = _int_or_none(v)
    assert eid is not None
    return eid


def event_type(raw: JSON) -> str:
    t = _require(raw, "type", "event_type", "kind")
    if not isinstance(t, str):
        raise InvalidEvent(f"type must be string, got {t!r}")
    return t


IST = timezone(timedelta(hours=5, minutes=30))


def normalise_ts(v: Any) -> str | None:
    """Canonical ISO-8601 in IST, second precision: '2027-01-04T10:15:00+05:30'.

    All stored timestamps share this form, so string comparison is time order
    and ``substr(ts, 1, 13)`` is the IST hour bucket. Naive timestamps are
    taken as IST (the simulator's clock). Unparseable values are kept as-is.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, IST).isoformat(timespec="seconds")
    s = str(v)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST).isoformat(timespec="seconds")


def event_ts(raw: JSON) -> str | None:
    return normalise_ts(_get(raw, "ts", "timestamp", "time", "sim_time"))


# --------------------------------------------------------------- projections
# Each returns the row tuple for its typed table, matching the column order
# in ingestion.py's INSERT statements.


def campaign_row(raw: JSON, eid: int) -> tuple:
    return (
        _require(raw, "campaign_id"),
        _require(raw, "workflow_id"),
        _get(raw, "tenant_id"),
        event_ts(raw),
        _int_or_none(_get(raw, "lead_count", "n_leads", "leads")),
        _int_or_none(_get(raw, "config_version")),
        eid,
    )


def lead_row(raw: JSON, eid: int) -> tuple:
    features = _get(raw, "features")
    if features is None:
        # Features may be top-level rather than nested.
        features = {k: raw[k] for k in ("lang", "city_tier", "age_band", "value_band")
                    if k in raw}
    if not isinstance(features, dict):
        raise InvalidEvent("lead features must be an object")
    return (
        _require(raw, "lead_id"),
        _get(raw, "person_id"),
        _get(raw, "campaign_id"),
        _get(raw, "workflow_id"),
        _dumps(features),
        eid,
    )


def call_row(raw: JSON, eid: int) -> tuple:
    variant = _get(raw, "variant", default={})
    if not isinstance(variant, dict):
        raise InvalidEvent("variant must be an object")
    qa = _get(raw, "qa", default={}) or {}
    if not isinstance(qa, dict):
        raise InvalidEvent("qa must be an object")
    connected = _bool01(_require(raw, "connected"), "connected")
    engaged = _bool01(_get(raw, "engaged", default=False), "engaged")
    if engaged and not connected:
        raise InvalidEvent("engaged call must be connected")
    halluc = qa.get("hallucination_flag")
    flow = qa.get("flow_adherence")
    return (
        eid,
        event_ts(raw),
        _require(raw, "workflow_id"),
        _get(raw, "tenant_id"),
        _get(raw, "campaign_id"),
        _get(raw, "lead_id"),
        _get(raw, "person_id"),
        _int_or_none(_get(raw, "attempt_no", "attempt", "attempt_number")),
        _int_or_none(_get(raw, "campaign_day", "campaign_day_index", "day_index")),
        _int_or_none(_get(raw, "config_version")),
        _get(raw, "cohort_id"),
        *(variant.get(d) for d in VARIANT_DIMS),
        connected,
        engaged,
        _get(raw, "disposition"),
        _get(raw, "duration_s", "duration", "duration_sec"),
        _int_or_none(_get(raw, "turns")),
        None if halluc is None else _bool01(halluc, "qa.hallucination_flag"),
        None if flow is None else float(flow),
        _dumps(variant),
    )


def config_change_row(raw: JSON, eid: int) -> tuple:
    dims = _get(raw, "changed_dims", "changed_dimensions", default=[])
    return (
        eid,
        event_ts(raw),
        _require(raw, "workflow_id"),
        _int_or_none(_get(raw, "new_version", "version", "config_version")),
        _get(raw, "actor"),
        _dumps(dims),
        _get(raw, "proposal_id"),
        _get(raw, "approval_id"),
    )


def approval_decision_row(raw: JSON, eid: int) -> tuple:
    return (
        eid,
        event_ts(raw),
        _require(raw, "approval_id"),
        _get(raw, "workflow_id"),
        _get(raw, "status"),
        _dumps(_get(raw, "reasons", "reason", default=[])),
    )


def approval_applied_row(raw: JSON, eid: int) -> tuple:
    return (eid, event_ts(raw), _require(raw, "approval_id"), _get(raw, "workflow_id"),
            _get(raw, "status", default="applied"),
            _dumps({"proposal_id": _get(raw, "proposal_id"),
                    "new_version": _get(raw, "new_version", "version")}))


PROJECTIONS = {
    "campaign_created": ("campaigns", campaign_row),
    "lead": ("leads", lead_row),
    "call": ("calls", call_row),
    "config_changed": ("config_changes", config_change_row),
    "approval_decided": ("approval_decisions", approval_decision_row),
    "approval_applied": ("approval_decisions", approval_applied_row),
}
