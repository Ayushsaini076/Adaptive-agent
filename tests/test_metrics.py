import pytest

from controller import database
from controller.cohorts import WORKFLOW_WIDE
from controller.database import connect, transaction
from controller.ingestion import Ingester
from controller.metrics import (
    Rate,
    baseline,
    business_outcome,
    kpis,
    rebuild_aggregates,
    snapshot_baselines,
    workflow_kpis,
)
from controller.workflows import WorkflowRegistry
from tests.fakes import FakeSimulator, call_event, campaign_event, lead_event, sample_simulator


def call(eid, lead, camp, *, hour=10, cohort="default", voice="v1", connected=True,
         engaged=True, disposition=None, halluc=False, wf="wf_a"):
    e = call_event(eid, lead, camp, wf, connected=connected, engaged=engaged)
    e["ts"] = f"2027-01-04T{hour:02d}:05:00+05:30"
    e["cohort_id"] = cohort
    e["variant"]["voice"] = voice
    if disposition:
        e["disposition"] = disposition
    if connected:
        e["qa"]["hallucination_flag"] = halluc
    return e


@pytest.fixture
def conn(tmp_path):
    return connect(tmp_path / "c.db")


def ingest(conn, chunks, sim=None):
    sim = sim or FakeSimulator()
    sim.history_chunks.update(chunks)
    Ingester(conn, sim.client()).catch_up("history")
    return sim


# --------------------------------------------------------------- denominators


def test_pickup_and_engagement_use_their_own_denominators(conn):
    # 4 dials, 2 connected, 1 engaged.
    ingest(conn, {1: [
        call(1, "a", "c", connected=True, engaged=True),
        call(2, "b", "c", connected=True, engaged=False),
        call(3, "c", "c", connected=False),
        call(4, "d", "c", connected=False),
    ]})
    r = workflow_kpis(conn, "wf_a")
    assert (r.pickup.num, r.pickup.den) == (2, 4)
    assert (r.engagement.num, r.engagement.den) == (1, 2)       # NOT 1/4
    assert (r.hallucination.num, r.hallucination.den) == (0, 2)  # QA only on connected


def test_rate_with_zero_denominator_is_none():
    assert Rate(0, 0).value is None and Rate(0, 0).wilson() is None


def test_wilson_interval_known_values():
    lo, hi = Rate(50, 100).wilson()
    assert lo == pytest.approx(0.4038, abs=1e-4) and hi == pytest.approx(0.5962, abs=1e-4)
    lo, hi = Rate(0, 20).wilson()
    assert lo == 0.0 and hi == pytest.approx(0.1611, abs=1e-4)


# --------------------------------------------------------------- aggregates


def test_grouping_filters_and_time_window(conn):
    ingest(conn, {1: [
        call(1, "a", "c", hour=9, cohort="x", voice="v1"),
        call(2, "b", "c", hour=10, cohort="x", voice="v2", connected=False),
        call(3, "c", "c", hour=11, cohort="y", voice="v1"),
    ]})
    by_voice = {r.key: r.dials for r in kpis(conn, "wf_a", by=("voice",))}
    assert by_voice == {("v1",): 2, ("v2",): 1}
    xs = kpis(conn, "wf_a", by=("cohort_id", "voice"), filters={"cohort_id": "x"})
    assert [(r.key, r.dials) for r in xs] == [(("x", "v1"), 1), (("x", "v2"), 1)]
    win = workflow_kpis(conn, "wf_a", start="2027-01-04T10:00:00+05:30",
                        end="2027-01-04T11:00:00+05:30")
    assert win.dials == 1


def test_unknown_dimension_rejected(conn):
    with pytest.raises(ValueError):
        kpis(conn, "wf_a", by=("1; DROP TABLE calls",))


def test_aggregates_not_double_counted_on_duplicates(conn):
    sim = ingest(conn, {1: [call(1, "a", "c"), call(2, "b", "c")]})
    sim.history_chunks[2] = [call(1, "a", "c"), call(3, "c", "c")]  # event 1 repeated
    Ingester(conn, sim.client()).catch_up("history")
    assert workflow_kpis(conn, "wf_a").dials == 3


def test_incremental_equals_full_rebuild(conn):
    ingest(conn, {
        1: [call(i, f"l{i}", "c", hour=9 + i % 3, cohort="xy"[i % 2], connected=i % 3 > 0)
            for i in range(1, 40)],
        2: [call(i, f"l{i}", "c", hour=9 + i % 3, voice="v2", halluc=i % 5 == 0)
            for i in range(40, 70)],
    })
    q = "SELECT * FROM metric_aggregates ORDER BY 1,2,3,4,5,6,7,8"
    incremental = [tuple(r) for r in conn.execute(q)]
    with transaction(conn):
        rebuild_aggregates(conn)
    assert [tuple(r) for r in conn.execute(q)] == incremental


def test_attribution_uses_cohort_recorded_on_call(tmp_path):
    """Old calls stay attributed to the cohort they were made under even
    after the config's cohort definitions change."""
    sim = sample_simulator()
    conn = connect(tmp_path / "c.db")
    reg = WorkflowRegistry(conn, sim.client())
    reg.sync_all()
    ingest(conn, {9: [call(5000, "a", "c", cohort="hi_t2")]}, sim)
    sim.event_chunks[20] = [sim.supervisor_edit(
        "wf_a", cohorts=[{"cohort_id": "en", "where": {"lang": ["en"]}}])]
    Ingester(conn, sim.client()).ingest_chunk(20, "events")
    reg.refresh_changed()
    assert reg.cohort_set("wf_a").ids == ["en", "default"]
    by_cohort = {r.key[0]: r.dials for r in kpis(conn, "wf_a", by=("cohort_id",))}
    assert by_cohort["hi_t2"] == 1 and "en" not in by_cohort   # not reassigned


def test_migration_from_v1_backfills_aggregates(tmp_path):
    path = tmp_path / "old.db"
    conn = database.sqlite3.connect(path, isolation_level=None)
    for stmt in database._split_sql(database.MIGRATIONS[0]):
        conn.execute(stmt)
    conn.execute("PRAGMA user_version = 1")
    conn.execute("INSERT INTO calls(event_id, ts, workflow_id, connected, engaged) VALUES "
                 "(1, '2027-01-04T10:00:00+05:30', 'w', 1, 0), "
                 "(2, '2027-01-04T10:30:00+05:30', 'w', 0, 0)")
    conn.close()
    r = workflow_kpis(connect(path), "w")
    assert (r.dials, r.connected) == (2, 1)


# --------------------------------------------------------------- business outcome


def _campaign(eid, camp, version, leads):
    out = [campaign_event(eid, camp, n=len(leads)) | {"config_version": version,
                                                     "ts": "2027-01-04T08:00:00+05:30"}]
    for i, (lead, lang) in enumerate(leads):
        e = lead_event(eid + 1 + i, lead, camp)
        e["features"]["lang"] = lang
        out.append(e)
    return out


def test_business_outcome_counts_distinct_concluded_leads(conn):
    ingest(conn, {1: _campaign(1, "camp", 3, [("a", "hi"), ("b", "hi"), ("c", "en")]) + [
        call(10, "a", "camp", connected=False),
        call(11, "a", "camp", disposition="concluded_interested"),
        call(12, "a", "camp", disposition="concluded_not_interested"),  # same lead again
        call(13, "b", "camp", disposition="concluded_not_interested"),
        call(14, "c", "camp", engaged=False, disposition="short_hangup"),
    ]})
    bo = business_outcome(conn, "wf_a")[WORKFLOW_WIDE]
    assert (bo.uploaded, bo.concluded, bo.interested) == (3, 2, 1)
    assert bo.rate.value == pytest.approx(2 / 3)


def test_business_outcome_uses_declared_lead_count_without_lead_events(conn):
    ingest(conn, {1: [campaign_event(1, "camp", n=50) | {"ts": "2027-01-04T08:00:00+05:30"}]})
    assert business_outcome(conn, "wf_a")[WORKFLOW_WIDE].uploaded == 50


def test_business_outcome_by_cohort_uses_campaign_config_version(tmp_path):
    sim = sample_simulator()   # wf_a v3 cohorts: hi_t2 = lang hi AND city_tier 2
    conn = connect(tmp_path / "c.db")
    reg = WorkflowRegistry(conn, sim.client())
    reg.sync_all()
    sim.supervisor_edit("wf_a", cohorts=[{"cohort_id": "en", "where": {"lang": ["en"]}}])
    reg.refresh("wf_a")        # v4 snapshotted; v3 kept
    ingest(conn, {
        1: _campaign(1, "old", 3, [("a", "hi"), ("b", "en")]),
        2: _campaign(10, "new", 4, [("c", "hi"), ("d", "en")])
           + [call(20, "d", "new", disposition="concluded_interested")],
        3: _campaign(30, "orphan", 99, [("e", "hi")]),
    }, sim)
    out = business_outcome(conn, "wf_a", registry=reg, by_cohort=True)
    assert {k: (v.uploaded, v.concluded) for k, v in out.items()} == {
        "hi_t2": (1, 0),       # lead a under v3 (features: hi, tier 2)
        "default": (2, 0),     # lead b under v3, lead c under v4
        "en": (1, 1),          # lead d under v4
        "unknown": (1, 0),     # v99 never snapshotted: not guessed
    }


# --------------------------------------------------------------- baselines


def test_baseline_pools_small_cohorts(conn):
    ingest(conn, {1: [call(i, f"l{i}", "c", cohort="big" if i < 300 else "tiny",
                           connected=i % 2 == 0) for i in range(1, 310)]})
    big = baseline(conn, "wf_a", "pickup", "big", min_n=200)
    tiny = baseline(conn, "wf_a", "pickup", "tiny", min_n=200)
    assert big.population == "big" and big.rate.den == 299
    assert tiny.population == WORKFLOW_WIDE and tiny.rate.den == 309


def test_snapshot_baselines_persists_each_kpi_and_cohort(conn):
    ingest(conn, {1: [call(i, f"l{i}", "c", cohort="x") for i in range(1, 11)]})
    with transaction(conn):
        snapshot_baselines(conn, "wf_a", "history", min_n=5)
        snapshot_baselines(conn, "wf_a", "history", min_n=5)   # idempotent
    rows = conn.execute("SELECT kpi, cohort_id, population, num, den FROM baselines "
                        "ORDER BY kpi, cohort_id").fetchall()
    assert [tuple(r) for r in rows] == [
        ("engagement", "*", "*", 10, 10), ("engagement", "x", "x", 10, 10),
        ("pickup", "*", "*", 10, 10), ("pickup", "x", "x", 10, 10)]
