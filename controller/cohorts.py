"""The ONE canonical cohort model.

Every module (metrics, experiments, incidents, impact, audit, reports) uses
``Cohort`` / ``CohortSet`` from here; nothing else re-implements matching.

Semantics (from the simulator contract):
* A workflow config holds up to 20 ordered cohorts. The first match wins.
* A cohort matches when EVERY feature key in ``where`` matches (AND), and a
  key matches when the lead's value is ANY of the listed values (OR).
* A lead matching no cohort belongs to ``default``.

Attribution rule: historical calls are attributed with the ``cohort_id``
recorded ON THE CALL EVENT, never by re-running today's definitions. A
``CohortSet`` is versioned by the config version it came from, and is used
only to assign leads that have no call yet (e.g. business-outcome
denominators), using the cohort set of the config active for that campaign.

Small-sample rule: a cohort below ``MIN_COHORT_SAMPLE`` observations is not
reported or decided on by itself; callers fall back to the workflow-wide
population (``WORKFLOW_WIDE``). ``default`` is NOT a fallback: it is a real
cohort (the unmatched leads) with its own population.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DEFAULT = "default"
WORKFLOW_WIDE = "*"          # pseudo-cohort: every call/lead in the workflow
MAX_COHORTS = 20
FEATURE_KEYS = ("lang", "city_tier", "age_band", "value_band")

# Minimum denominator before a cohort-level rate is used on its own.
# At p = 0.5 the 95% Wilson half-width is ~±6.9pp at n = 200 and ~±9.8pp at
# n = 100. Below ~200 a cohort rate can't distinguish anything useful from
# the workflow rate, so we pool instead. Experiments compute their own,
# larger, power-based sample sizes on top of this floor.
MIN_COHORT_SAMPLE = 200


class CohortError(ValueError):
    pass


def _norm(v: Any) -> str:
    """Compare feature values as strings so 2 == "2" (configs vs events)."""
    return str(v).strip().lower()


@dataclass(frozen=True)
class Cohort:
    cohort_id: str
    where: tuple[tuple[str, frozenset[str]], ...]   # sorted, normalised
    position: int = 0

    @classmethod
    def from_dict(cls, d: Mapping[str, Any], position: int = 0) -> "Cohort":
        cid = d.get("cohort_id") or d.get("id") or d.get("name")
        if not cid or not isinstance(cid, str):
            raise CohortError(f"cohort without id: {d!r}")
        if cid in (DEFAULT, WORKFLOW_WIDE):
            raise CohortError(f"{cid!r} is reserved")
        raw_where = d.get("where") or d.get("conditions") or d.get("match") or {}
        if not isinstance(raw_where, Mapping) or not raw_where:
            raise CohortError(f"cohort {cid}: 'where' must be a non-empty object")
        where = []
        for key, vals in raw_where.items():
            vals = vals if isinstance(vals, (list, tuple, set)) else [vals]
            if not vals:
                raise CohortError(f"cohort {cid}: empty value list for {key}")
            where.append((key, frozenset(_norm(v) for v in vals)))
        return cls(cid, tuple(sorted(where)), position)

    def matches(self, features: Mapping[str, Any]) -> bool:
        for key, allowed in self.where:
            v = features.get(key)
            if v is None or _norm(v) not in allowed:
                return False
        return True

    def to_dict(self) -> dict:
        return {"cohort_id": self.cohort_id,
                "where": {k: sorted(v) for k, v in self.where}}


@dataclass(frozen=True)
class CohortSet:
    """Ordered cohort definitions as of one workflow config version."""

    workflow_id: str
    config_version: int | None
    cohorts: tuple[Cohort, ...] = ()

    def __post_init__(self) -> None:
        if len(self.cohorts) > MAX_COHORTS:
            raise CohortError(f"{len(self.cohorts)} cohorts > max {MAX_COHORTS}")
        ids = [c.cohort_id for c in self.cohorts]
        if len(ids) != len(set(ids)):
            raise CohortError(f"duplicate cohort ids: {ids}")

    @classmethod
    def from_config(cls, workflow_id: str, config: Mapping[str, Any],
                    version: int | None = None) -> "CohortSet":
        raw = config.get("cohorts") or []
        if isinstance(raw, Mapping):  # {"id": {where...}} form
            raw = [{"cohort_id": k, **({"where": v} if "where" not in v else v)}
                   for k, v in raw.items()]
        cohorts = tuple(Cohort.from_dict(c, i) for i, c in enumerate(raw)
                        if (c.get("cohort_id") or c.get("id") or c.get("name")) != DEFAULT)
        return cls(workflow_id, version if version is not None else config_version(config),
                   cohorts)

    @property
    def ids(self) -> list[str]:
        return [c.cohort_id for c in self.cohorts] + [DEFAULT]

    def assign(self, features: Mapping[str, Any]) -> str:
        for c in self.cohorts:
            if c.matches(features):
                return c.cohort_id
        return DEFAULT

    def get(self, cohort_id: str) -> Cohort | None:
        return next((c for c in self.cohorts if c.cohort_id == cohort_id), None)

    def to_config(self) -> list[dict]:
        return [c.to_dict() for c in self.cohorts]


def config_version(config: Mapping[str, Any]) -> int | None:
    v = config.get("version", config.get("config_version"))
    return int(v) if v is not None else None


def population_for(cohort_id: str, n: int, min_n: int = MIN_COHORT_SAMPLE) -> str:
    """Cohort to report/decide on: itself if large enough, else workflow-wide."""
    return cohort_id if n >= min_n else WORKFLOW_WIDE


def features_from_json(s: str) -> dict:
    return json.loads(s) if s else {}
