"""Deterministic statistics for experiment decisions. Pure functions, stdlib only.

Choices (all documented so they can be defended in review):

* Sample size: classic two-proportion formula (pooled variance under H0,
  unpooled under H1), two-sided. Reproduces the spec's illustration: at
  p0 = 0.55, +10pp needs ~376/arm, +5pp ~1,533/arm.
* Test: two-proportion pooled z-test, two-sided. Adequate because we only
  evaluate at sample sizes in the hundreds+ per arm (np, n(1-p) >> 5).
* Effect interval: Newcombe hybrid-score interval for p_t - p_c (built from
  Wilson intervals). Better coverage than Wald near 0/1 and at unequal n.
* Multiple comparisons: Holm-Bonferroni step-down on the candidates'
  p-values. Controls family-wise error at alpha, uniformly more powerful
  than plain Bonferroni.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist

from controller.metrics import Rate

_N = NormalDist()


def z_quantile(p: float) -> float:
    return _N.inv_cdf(p)


def sample_size_per_arm(p0: float, mde: float, *, alpha: float = 0.05,
                        power: float = 0.8) -> int:
    """Observations per arm to detect an absolute change ``mde`` from ``p0``.

    Two-sided at ``alpha``. If p0 + mde would exceed 1, sizes for p0 - mde
    (symmetric detection); the formula is symmetric enough for planning.
    """
    if not 0 < p0 < 1:
        raise ValueError(f"baseline rate must be in (0,1), got {p0}")
    if mde <= 0:
        raise ValueError("mde must be positive")
    p1 = p0 + mde if p0 + mde < 1 else p0 - mde
    if not 0 < p1 < 1:
        raise ValueError(f"mde {mde} too large for baseline {p0}")
    za, zb = z_quantile(1 - alpha / 2), z_quantile(power)
    pbar = (p0 + p1) / 2
    num = za * math.sqrt(2 * pbar * (1 - pbar)) + zb * math.sqrt(p0 * (1 - p0) + p1 * (1 - p1))
    return math.ceil(num ** 2 / mde ** 2)


def noninferiority_sample_size(p: float, margin: float, *, alpha: float = 0.05,
                               power: float = 0.8) -> int:
    """Per-arm n so that, if the arms are truly equal at rate ``p``, the 95%
    lower bound of the difference clears ``-margin`` with probability ``power``.

    One-sided at alpha/2 (= the lower end of the two-sided 95% interval we use
    at analysis). ``p`` is clipped to [0.05, 0.95] so a 0%/100% historical rate
    doesn't produce a zero-variance (n = 0) plan.
    """
    if margin <= 0:
        raise ValueError("margin must be positive")
    p = min(max(p, 0.05), 0.95)
    z = z_quantile(1 - alpha / 2) + z_quantile(power)
    return math.ceil(z * z * 2 * p * (1 - p) / margin ** 2)


@dataclass(frozen=True)
class Comparison:
    """Treatment vs control on one KPI."""

    control: Rate
    treatment: Rate
    diff: float                     # p_t - p_c
    ci: tuple[float, float]         # Newcombe 95% for diff
    z: float
    p_value: float                  # two-sided

    def as_dict(self) -> dict:
        return {"control": self.control.as_dict(), "treatment": self.treatment.as_dict(),
                "diff": round(self.diff, 5), "ci95": [round(self.ci[0], 5), round(self.ci[1], 5)],
                "z": round(self.z, 4), "p_value": self.p_value}


def compare(control: Rate, treatment: Rate) -> Comparison | None:
    """Pooled two-proportion z-test + Newcombe interval. None if either arm is empty."""
    if not control.den or not treatment.den:
        return None
    pc, pt = control.num / control.den, treatment.num / treatment.den
    diff = pt - pc
    pooled = (control.num + treatment.num) / (control.den + treatment.den)
    se = math.sqrt(pooled * (1 - pooled) * (1 / control.den + 1 / treatment.den))
    if se == 0:
        z, p = 0.0, 1.0       # both arms all-0 or all-1: no evidence of a difference
    else:
        z = diff / se
        p = 2 * (1 - _N.cdf(abs(z)))
    lc, uc = control.wilson()
    lt, ut = treatment.wilson()
    lo = diff - math.sqrt((pt - lt) ** 2 + (uc - pc) ** 2)
    hi = diff + math.sqrt((ut - pt) ** 2 + (pc - lc) ** 2)
    return Comparison(control, treatment, diff, (lo, hi), z, p)


def holm_adjust(p_values: dict[str, float]) -> dict[str, float]:
    """Holm-adjusted p-values. Reject H_i at level alpha iff adjusted <= alpha.

    Ties are broken by key so the result is deterministic.
    """
    m = len(p_values)
    ordered = sorted(p_values.items(), key=lambda kv: (kv[1], kv[0]))
    out: dict[str, float] = {}
    running = 0.0
    for i, (k, p) in enumerate(ordered):
        running = max(running, min(1.0, (m - i) * p))
        out[k] = running
    return out
