import pytest

from controller.cohorts import (
    DEFAULT,
    MAX_COHORTS,
    WORKFLOW_WIDE,
    Cohort,
    CohortError,
    CohortSet,
    population_for,
)

CFG = {"version": 4, "cohorts": [
    {"cohort_id": "hi_metro", "where": {"lang": ["hi"], "city_tier": [1]}},
    {"cohort_id": "hi_any", "where": {"lang": ["hi"]}},
    {"cohort_id": "high_value", "where": {"value_band": ["high", "premium"]}},
]}


@pytest.fixture
def cs():
    return CohortSet.from_config("wf", CFG)


def test_and_across_keys_or_within_values(cs):
    assert cs.assign({"lang": "hi", "city_tier": 1}) == "hi_metro"
    assert cs.assign({"lang": "en", "value_band": "premium"}) == "high_value"
    assert cs.assign({"lang": "en", "value_band": "low"}) == DEFAULT


def test_first_match_wins(cs):
    # Matches hi_any AND high_value; hi_any is earlier.
    assert cs.assign({"lang": "hi", "city_tier": 3, "value_band": "high"}) == "hi_any"


def test_values_compare_as_normalised_strings(cs):
    assert cs.assign({"lang": "HI", "city_tier": "1"}) == "hi_metro"


def test_missing_feature_does_not_match(cs):
    assert cs.assign({"city_tier": 1}) == DEFAULT


def test_ids_and_version(cs):
    assert cs.ids == ["hi_metro", "hi_any", "high_value", DEFAULT]
    assert cs.config_version == 4


def test_empty_config_is_all_default():
    cs = CohortSet.from_config("wf", {"version": 1})
    assert cs.assign({"lang": "hi"}) == DEFAULT and cs.ids == [DEFAULT]


def test_explicit_default_entry_is_ignored():
    cs = CohortSet.from_config("wf", {"version": 1, "cohorts": [
        {"cohort_id": "default", "where": {"lang": ["x"]}}]})
    assert cs.cohorts == ()


def test_mapping_form_is_accepted():
    cs = CohortSet.from_config("wf", {"version": 1, "cohorts": {"t1": {"city_tier": [1]}}})
    assert cs.assign({"city_tier": 1}) == "t1"


def test_round_trip_to_config(cs):
    again = CohortSet.from_config("wf", {"version": 4, "cohorts": cs.to_config()})
    assert again == cs


@pytest.mark.parametrize("bad", [
    {"where": {"lang": ["hi"]}},                         # no id
    {"cohort_id": "x", "where": {}},                     # empty where
    {"cohort_id": "x", "where": {"lang": []}},           # empty value list
    {"cohort_id": "*", "where": {"lang": ["hi"]}},       # reserved
])
def test_invalid_cohorts(bad):
    with pytest.raises(CohortError):
        Cohort.from_dict(bad)


def test_limits_and_duplicates():
    many = [{"cohort_id": f"c{i}", "where": {"lang": [str(i)]}} for i in range(MAX_COHORTS + 1)]
    with pytest.raises(CohortError):
        CohortSet.from_config("wf", {"cohorts": many})
    dup = [{"cohort_id": "a", "where": {"lang": ["x"]}}] * 2
    with pytest.raises(CohortError):
        CohortSet.from_config("wf", {"cohorts": dup})


def test_population_for_falls_back_to_workflow_wide():
    assert population_for("c1", 500, min_n=200) == "c1"
    assert population_for("c1", 199, min_n=200) == WORKFLOW_WIDE
