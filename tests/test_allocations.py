import pytest

from controller import allocations as alloc

CFG = {"version": 2, "allocations": {
    "default": {"voice": {"v1": 3, "v2": 1}, "welcome_message": "w1"},
    "vip": {"voice": ["v2", "v3"]},
}}


def test_options_parsing():
    meta = {"options": {"voice": ["v1", {"id": "v2"}, {"name": "v3"}, "v1"]}}
    assert alloc.options(meta, "voice") == ["v1", "v2", "v3"]
    assert alloc.options(meta, "caller_id") == []
    assert alloc.options({}, "voice") == []


def test_distribution_normalises_and_inherits_default():
    assert alloc.distribution(CFG, "default", "voice") == {"v1": 0.75, "v2": 0.25}
    assert alloc.distribution(CFG, "vip", "voice") == {"v2": 0.5, "v3": 0.5}
    assert alloc.distribution(CFG, "vip", "welcome_message") == {"w1": 1.0}   # inherited
    assert alloc.distribution(CFG, "other", "voice") == {"v1": 0.75, "v2": 0.25}
    assert alloc.distribution({}, "x", "voice") == {}


def test_list_form_allocations():
    cfg = {"allocations": [{"cohort_id": "default", "voice": {"v1": 1}},
                           {"cohort_id": "c1", "voice": {"v2": 1}}]}
    assert alloc.distribution(cfg, "c1", "voice") == {"v2": 1.0}
    new = alloc.with_distribution(cfg, "c2", "voice", {"v3": 1.0})
    assert isinstance(new["allocations"], list)
    assert alloc.distribution(new, "c2", "voice") == {"v3": 1.0}
    assert len(cfg["allocations"]) == 2                     # original untouched


def test_with_distribution_only_touches_target():
    new = alloc.with_distribution(CFG, "vip", "voice", {"v1": 0.5, "v3": 0.5})
    assert alloc.distribution(new, "vip", "voice") == {"v1": 0.5, "v3": 0.5}
    assert alloc.distribution(new, "default", "voice") == {"v1": 0.75, "v2": 0.25}
    assert alloc.distribution(CFG, "vip", "voice") == {"v2": 0.5, "v3": 0.5}   # no mutation
    assert new["version"] == 2


def test_tv_distance():
    assert alloc.tv_distance({"a": 1.0}, {"a": 0.5, "b": 0.5}) == pytest.approx(0.5)
    assert alloc.tv_distance({"a": 1.0}, {"b": 1.0}) == pytest.approx(1.0)
    assert alloc.same_distribution({"a": 0.5, "b": 0.5}, {"b": 0.5, "a": 0.5})


def test_zero_and_negative_weights_ignored():
    assert alloc.normalise({"a": 0, "b": 2}) == {"b": 1.0}
    assert alloc.normalise({"a": 0}) == {}
