"""Reading and editing per-cohort allocations in a workflow config.

The ONLY module that knows the config's ``allocations`` / ``options`` shape.
Assumed shapes (accepted defensively; tighten once confirmed against /docs):

    options:     {"voice": ["v1", "v2"], ...}   or  {"voice": [{"id": "v1"}, ...]}
    allocations: {"default": {"voice": {"v1": 1.0}, ...}, "<cohort>": {...}}
             or  [{"cohort_id": "default", "voice": {"v1": 1.0}}, ...]

A dimension value may be a weight map, a single option string (= 100%), or a
list of options (= uniform). Weights are normalised. A cohort that omits a
dimension inherits it from ``default`` (contract semantics).
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from controller.cohorts import DEFAULT

ALLOCATION_DIMS = ("welcome_message", "voice", "caller_id")
Dist = dict[str, float]


def options(meta: Mapping[str, Any], dim: str) -> list[str]:
    """Allowed values for ``dim`` from workflow metadata, in declared order."""
    opts = meta.get("options") or {}
    raw = opts.get(dim) if isinstance(opts, Mapping) else None
    out = []
    for o in raw or []:
        if isinstance(o, Mapping):
            o = o.get("id") or o.get("value") or o.get("name")
        if o is not None and str(o) not in out:
            out.append(str(o))
    return out


def normalise(value: Any) -> Dist:
    if value is None:
        return {}
    if isinstance(value, str):
        return {value: 1.0}
    if isinstance(value, (list, tuple)):
        return {str(v): 1.0 / len(value) for v in value} if value else {}
    if isinstance(value, Mapping):
        total = sum(float(w) for w in value.values() if float(w) > 0)
        if total <= 0:
            return {}
        return {str(k): float(w) / total for k, w in value.items() if float(w) > 0}
    raise ValueError(f"unrecognised allocation value {value!r}")


def _by_cohort(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    alloc = config.get("allocations") or {}
    if isinstance(alloc, Mapping):
        return {str(k): v for k, v in alloc.items() if isinstance(v, Mapping)}
    if isinstance(alloc, list):
        return {str(a.get("cohort_id") or a.get("cohort") or DEFAULT): a
                for a in alloc if isinstance(a, Mapping)}
    return {}


def distribution(config: Mapping[str, Any], cohort_id: str, dim: str) -> Dist:
    """Effective distribution for (cohort, dim), inheriting from default."""
    by = _by_cohort(config)
    for cid in (cohort_id, DEFAULT):
        d = normalise((by.get(cid) or {}).get(dim))
        if d:
            return d
    return {}


def with_distribution(config: Mapping[str, Any], cohort_id: str, dim: str,
                      dist: Dist) -> dict:
    """Deep copy of ``config`` with an explicit (cohort, dim) distribution.

    Preserves the config's allocation layout (map or list). Does not touch
    the version; governance sets base_version separately.
    """
    new = copy.deepcopy(dict(config))
    weights = {k: round(v, 6) for k, v in dist.items()}
    alloc = new.get("allocations")
    if isinstance(alloc, list):
        for a in alloc:
            if str(a.get("cohort_id") or a.get("cohort") or DEFAULT) == cohort_id:
                a[dim] = weights
                return new
        alloc.append({"cohort_id": cohort_id, dim: weights})
        return new
    alloc = dict(alloc or {})
    alloc[cohort_id] = {**(alloc.get(cohort_id) or {}), dim: weights}
    new["allocations"] = alloc
    return new


def tv_distance(p: Dist, q: Dist) -> float:
    """Total-variation distance 0.5 * sum |p - q| over the union of options."""
    keys = set(p) | set(q)
    return 0.5 * sum(abs(p.get(k, 0.0) - q.get(k, 0.0)) for k in keys)


def same_distribution(p: Dist, q: Dist, tol: float = 1e-4) -> bool:
    return tv_distance(p, q) <= tol
