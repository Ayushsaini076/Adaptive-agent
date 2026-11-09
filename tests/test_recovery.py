"""Crash recovery with a REAL hard kill.

A child process ingests history into a file-backed DB and calls ``os._exit``
in the middle of a chunk, after some rows have been flushed inside the open
transaction. ``os._exit`` skips all Python cleanup (no rollback, no close),
which is what SIGKILL / OOM looks like to SQLite. The parent then reopens the
DB and checks nothing from the partial chunk survived and that a fresh
ingester completes the job without duplicates.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

from controller.database import connect
from controller.ingestion import Ingester
from tests.fakes import sample_simulator

ROOT = Path(__file__).resolve().parents[1]

CHILD = textwrap.dedent("""
    import os, sys
    from controller.database import connect
    from controller.ingestion import Ingester
    from tests.fakes import sample_simulator

    sim = sample_simulator()
    client = sim.client()
    real_iter = client.iter_chunk

    def dying_iter(n):
        for i, e in enumerate(real_iter(n)):
            if n == 2 and i == 4:
                os._exit(137)      # hard kill mid-chunk, mid-transaction
            yield e

    client.iter_chunk = dying_iter
    Ingester(connect(sys.argv[1]), client, batch_size=2).catch_up("history")
""")


def test_hard_kill_mid_chunk_then_resume(tmp_path):
    db = tmp_path / "c.db"
    proc = subprocess.run([sys.executable, "-c", CHILD, str(db)], cwd=ROOT,
                          capture_output=True, text=True)
    assert proc.returncode == 137, proc.stderr

    conn = connect(db)
    q = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
    assert q("SELECT COUNT(*) FROM event_chunks") == 1          # only chunk 1 committed
    assert q("SELECT COUNT(*) FROM events WHERE chunk_number = 2") == 0
    assert q("SELECT COUNT(*) FROM events") == 5

    stats = Ingester(conn, sample_simulator().client()).catch_up("history")
    assert stats.chunks == 1                                    # resumed at chunk 2 only
    assert q("SELECT COUNT(*) FROM events") == 12
    assert q("SELECT COUNT(*) FROM calls") == 5
    assert q("SELECT COUNT(DISTINCT event_id) FROM events") == 12
