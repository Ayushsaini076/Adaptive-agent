import pytest

from controller.metrics import Rate
from controller.stats import (
    compare,
    holm_adjust,
    noninferiority_sample_size,
    sample_size_per_arm,
)


def test_sample_size_matches_spec_illustration():
    # Spec: ~390/arm for +10pp and ~1,550/arm for +5pp at p0 = 0.55.
    assert sample_size_per_arm(0.55, 0.10) == 376
    assert sample_size_per_arm(0.55, 0.05) == 1534


def test_sample_size_grows_with_stricter_alpha_and_smaller_mde():
    base = sample_size_per_arm(0.5, 0.05)
    assert sample_size_per_arm(0.5, 0.05, alpha=0.025) > base
    assert sample_size_per_arm(0.5, 0.03) > base
    assert sample_size_per_arm(0.5, 0.05, power=0.9) > base


def test_sample_size_near_one_sizes_downward_change():
    assert sample_size_per_arm(0.97, 0.05) > 0


@pytest.mark.parametrize("p0,mde", [(0, 0.1), (1, 0.1), (0.5, 0), (0.5, 0.6)])
def test_sample_size_rejects_bad_input(p0, mde):
    with pytest.raises(ValueError):
        sample_size_per_arm(p0, mde)


def test_compare_z_test_known_values():
    c = compare(Rate(50, 100), Rate(60, 100))
    assert c.diff == pytest.approx(0.10)
    assert c.z == pytest.approx(1.4213, abs=1e-4)
    assert c.p_value == pytest.approx(0.1552, abs=1e-4)


def test_newcombe_interval_matches_published_example():
    # Newcombe (1998), Statistics in Medicine 17:873, example: 56/70 vs 48/80.
    c = compare(Rate(48, 80), Rate(56, 70))
    assert c.ci[0] == pytest.approx(0.0524, abs=1e-4)
    assert c.ci[1] == pytest.approx(0.3339, abs=1e-4)


def test_compare_degenerate_cases():
    assert compare(Rate(0, 0), Rate(5, 10)) is None
    same = compare(Rate(10, 10), Rate(20, 20))
    assert same.p_value == 1.0 and same.diff == 0


def test_holm_adjustment():
    adj = holm_adjust({"a": 0.01, "b": 0.04})
    assert adj == {"a": pytest.approx(0.02), "b": pytest.approx(0.04)}
    adj = holm_adjust({"a": 0.03, "b": 0.04})
    assert adj["a"] == pytest.approx(0.06) and adj["b"] == pytest.approx(0.06)  # monotone
    assert holm_adjust({"x": 0.03}) == {"x": 0.03}
    assert holm_adjust({"a": 0.6, "b": 0.7, "c": 0.9})["c"] == 1.0              # capped


def test_noninferiority_sample_size():
    # (1.96 + 0.8416)^2 * 2 * 0.8 * 0.2 / 0.2^2 = 62.8 -> 63
    assert noninferiority_sample_size(0.8, 0.2) == 63
    assert noninferiority_sample_size(0.8, 0.05) > noninferiority_sample_size(0.8, 0.1)
    assert noninferiority_sample_size(1.0, 0.1) == noninferiority_sample_size(0.95, 0.1)
    with pytest.raises(ValueError):
        noninferiority_sample_size(0.5, 0)
