"""The LEAD()-based carryover query must give EXACTLY the results of the original
correlated-subquery version (kept below as the reference), without its
quadratic cost."""

import random
import time

import pytest

from controller import experiments as ex
from controller.database import connect, transaction
from controller.experiments import (
    ExperimentPolicy,
    _carryover_sql,
    carryover_baseline,
    carryover_curve,
    carryover_exposures,
    plan,
)


def reference_sql(dim_expr: str, extra: str) -> str:
    """The original implementation (correct but O(P x N)). Parameters: wf, cohort,
    start, end, *versions, wf, end, *versions."""
    return f"""
        WITH fc AS (
            SELECT p, eid, arm FROM (
                SELECT COALESCE(person_id, lead_id) AS p, event_id AS eid, {dim_expr} AS arm,
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(person_id, lead_id)
                                          ORDER BY event_id) AS rn
                FROM calls
                WHERE workflow_id = ? AND cohort_id = ? AND ts >= ? AND ts < ?
                  AND connected = 1 {extra})
            WHERE rn = 1),
        nxt AS (
            SELECT fc.arm,
                   (SELECT c.connected FROM calls c
                    WHERE c.workflow_id = ? AND COALESCE(c.person_id, c.lead_id) = fc.p
                      AND c.event_id > fc.eid AND c.ts < ? {extra.replace("config_version", "c.config_version")}
                    ORDER BY c.event_id LIMIT 1) AS y
            FROM fc)
        SELECT arm, COUNT(y), SUM(y) FROM nxt WHERE y IS NOT NULL GROUP BY arm"""


def _rows(cur):
    return sorted((tuple(r) for r in cur), key=str)


def adversarial_db(tmp_path, seed):
    """Cohort changes per person, NULL person_id (lead fallback), several config
    versions, another workflow interleaved, people never re-dialled."""
    random.seed(seed)
    conn = connect(tmp_path / f"e{seed}.db")
    rows = []
    for eid in range(1, 600):
        p = random.randint(1, 60)
        rows.append((eid, f"2027-01-{random.randint(1, 9):02d}T{random.randint(9, 20):02d}:00:00"
                          f"+05:30", random.choice(["w", "w", "other"]),
                     None if p % 7 == 0 else f"p{p}", f"l{p}", random.choice([1, 2, 3]),
                     random.choice(["c1", "c1", "c2"]), random.choice(["v1", "v2"]),
                     int(random.random() < 0.3)))
    with transaction(conn):
        conn.executemany("INSERT INTO calls(event_id, ts, workflow_id, person_id, lead_id, "
                         "config_version, cohort_id, voice, connected, engaged) "
                         "VALUES (?,?,?,?,?,?,?,?,?,0)", rows)
    return conn


@pytest.mark.parametrize("seed", range(12))
def test_lead_query_equals_reference(tmp_path, seed):
    conn = adversarial_db(tmp_path, seed)
    windows = (("0000", "9999"), ("2027-01-03", "2027-01-07"), ("2027-01-05", "2027-01-06"))
    filters = (("'all'", "", []), ("voice", "AND config_version IN (?,?)", [2, 3]),
               ("voice", "AND config_version IN (?)", [1]))
    for start, end in windows:
        for dim_expr, extra, vers in filters:
            for cohort in ("c1", "c2"):
                old = _rows(conn.execute(reference_sql(dim_expr, extra),
                                         ("w", cohort, start, end, *vers, "w", end, *vers)))
                new = _rows(conn.execute(_carryover_sql(dim_expr, extra),
                                         ("w", end, *vers, cohort, start)))
                assert new == old, (start, end, dim_expr, vers, cohort)


def test_public_functions_equal_reference(tmp_path):
    conn = adversarial_db(tmp_path, 99)
    end = "2027-01-08T00:00:00+05:30"
    old = conn.execute(reference_sql("'all'", ""), ("w", "c1", "0000", end, "w", end)).fetchone()
    assert carryover_baseline(conn, "w", "c1", end) == ex.Rate(int(old[2] or 0), int(old[1]))

    got = carryover_exposures(conn, "w", "c1", "voice", versions=[2, 3], start="2027-01-02")
    ref = {r[0]: ex.Rate(int(r[2] or 0), int(r[1])) for r in conn.execute(
        reference_sql("voice", "AND config_version IN (?,?)"),
        ("w", "c1", "2027-01-02", "9999", 2, 3, "w", "9999", 2, 3)) if r[0] is not None}
    assert got == ref

    first = conn.execute("SELECT MIN(ts) FROM calls WHERE workflow_id='w' AND cohort_id='c1' "
                         "AND ts < ?", (end,)).fetchone()[0]
    curve = carryover_curve(conn, "w", "c1", end)
    for d, value in enumerate(curve, start=1):
        upto = min(f"2027-01-{int(first[8:10]) + d:02d}T00:00:00+05:30", end)
        r = conn.execute(reference_sql("'all'", ""),
                         ("w", "c1", first, upto, "w", upto)).fetchone()
        assert value == (int(r[1]) if r else 0), d


def test_query_plan_has_no_correlated_subquery(tmp_path):
    conn = adversarial_db(tmp_path, 0)
    plan_rows = [r[-1] for r in conn.execute("EXPLAIN QUERY PLAN " + _carryover_sql("'all'", ""),
                                             ("w", "9999", "c1", "0000"))]
    assert not any("CORRELATED" in x for x in plan_rows), plan_rows


def test_carryover_sizing_is_fast_at_history_scale(tmp_path):
    """~50k dials (ks_cancel-like history). The old query took minutes here."""
    conn = connect(tmp_path / "big.db")
    random.seed(1)
    rows, eid, person = [], 1, 0
    for day in range(10):
        for _ in range(2200):
            person += 1
            for a in range(3):
                if day + a > 9:
                    break
                c = int(random.random() < 0.25)
                rows.append((eid, f"2026-12-{25 + day + a:02d}T{9 + person % 12:02d}:00:00+05:30",
                             "ks", f"p{person}", f"l{person}", 1, "default", c))
                eid += 1
                if c and random.random() < 0.5:
                    break
    with transaction(conn):
        conn.executemany("INSERT INTO calls(event_id, ts, workflow_id, person_id, lead_id, "
                         "config_version, cohort_id, connected, engaged) "
                         "VALUES (?,?,?,?,?,?,?,?,0)", rows)
    assert len(rows) > 45_000
    end = "2027-01-04T00:00:00+05:30"
    t = time.perf_counter()
    carryover_baseline(conn, "ks", "default", end)
    carryover_curve(conn, "ks", "default", end)
    assert time.perf_counter() - t < 3.0


def test_guardrail_sizing_computed_once_per_cohort_not_per_dimension(tmp_path, monkeypatch):
    from tests.test_experiments import World, _call
    w = World(tmp_path)
    w.sim.workflows["wf"]["objective"] = {"primary": "engagement_rate",
                                          "guardrail": {"pickup_rate_max_drop": 0.02}}
    w.sim.workflows["wf"]["options"]["welcome_message"] = ["w1", "w2"]
    w.reg.refresh("wf")
    w.ingest([_call(62_000 + i, f"h{i}", 15, "v1", i % 4 == 0, day=3, version=1)
              for i in range(2, 201, 2)])
    calls = []
    real = ex.carryover_baseline
    monkeypatch.setattr(ex, "carryover_baseline",
                        lambda *a, **k: calls.append(a[2]) or real(*a, **k))
    design, reasons = plan(w.conn, w.reg, "wf",
                           ExperimentPolicy(dimensions=("welcome_message", "voice"),
                                            min_history_units=50))
    assert sum("next dial after a connected exposure" in r for r in reasons) >= 2   # both dims
    assert calls == ["default"]                                   # sized once, reused
