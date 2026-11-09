"""Chunk ingestion: history bootstrap and live chunks, idempotent and atomic.

Guarantees
----------
* A chunk is ingested in ONE transaction: raw events, typed projections,
  rejected-event records and the ``event_chunks`` checkpoint row commit
  together or not at all. A crash before COMMIT means the chunk is fetched
  and replayed from scratch; a crash after COMMIT means it is skipped.
* Every insert is ``INSERT OR IGNORE`` on a natural key (event_id, lead_id,
  campaign_id), so replaying a chunk or seeing a duplicate event_id across
  chunks never double-counts.
* Events are streamed and flushed in batches of ``batch_size`` rows, so peak
  memory is bounded by the batch size, not the chunk size.
* ``metric_aggregates`` are folded in from the chunk's newly inserted calls
  in the same transaction, so they can never disagree with ``calls``.
* Chunks are processed strictly in ascending order and ingestion stops at the
  first failure, so the committed set never has gaps below the cursor.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass, field

from controller import events as ev
from controller.metrics import update_aggregates_for_chunk
from controller.client import SimulatorClient
from controller.database import set_state, transaction

log = logging.getLogger(__name__)

INSERTS: dict[str, str] = {
    "events": "INSERT OR IGNORE INTO events"
              "(event_id, ts, type, workflow_id, tenant_id, chunk_number, payload) "
              "VALUES (?,?,?,?,?,?,?)",
    "campaigns": "INSERT OR IGNORE INTO campaigns VALUES (?,?,?,?,?,?,?)",
    "leads": "INSERT OR IGNORE INTO leads VALUES (?,?,?,?,?,?)",
    "calls": "INSERT OR IGNORE INTO calls VALUES "
             "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    "config_changes": "INSERT OR IGNORE INTO config_changes VALUES (?,?,?,?,?,?,?,?)",
    "approval_decisions": "INSERT OR IGNORE INTO approval_decisions VALUES (?,?,?,?,?,?)",
    "rejected_events": "INSERT OR IGNORE INTO rejected_events"
                       "(chunk_number, line_index, reason, payload) VALUES (?,?,?,?)",
}


@dataclass
class ChunkResult:
    chunk_number: int
    kind: str
    skipped: bool = False
    event_count: int = 0
    inserted: int = 0
    rejected: int = 0
    seconds: float = 0.0


@dataclass
class IngestStats:
    chunks: int = 0
    events: int = 0
    inserted: int = 0
    rejected: int = 0
    seconds: float = 0.0
    results: list[ChunkResult] = field(default_factory=list)

    def add(self, r: ChunkResult) -> None:
        self.results.append(r)
        if r.skipped:
            return
        self.chunks += 1
        self.events += r.event_count
        self.inserted += r.inserted
        self.rejected += r.rejected
        self.seconds += r.seconds

    @property
    def events_per_sec(self) -> float:
        return self.events / self.seconds if self.seconds else 0.0


class Ingester:
    def __init__(self, conn: sqlite3.Connection, client: SimulatorClient,
                 batch_size: int = 5000) -> None:
        self.conn = conn
        self.client = client
        self.batch_size = batch_size

    # ---------------------------------------------------------------- cursors

    def cursor(self, kind: str) -> int | None:
        """Highest committed chunk number of ``kind`` (None if none yet)."""
        row = self.conn.execute(
            "SELECT MAX(chunk_number) FROM event_chunks WHERE kind = ?", (kind,)).fetchone()
        return row[0]

    def is_ingested(self, chunk_number: int) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM event_chunks WHERE chunk_number = ?", (chunk_number,)
        ).fetchone() is not None

    # ---------------------------------------------------------------- entry points

    def catch_up(self, kind: str) -> IngestStats:
        """Ingest every listed chunk of ``kind`` after the committed cursor.

        Used for the history bootstrap and, after a restart, to pick up event
        chunks produced by an advance whose chunks were never ingested.
        Loops until the listing is empty in case the API paginates.
        """
        stats = IngestStats()
        while True:
            pending = self.client.list_chunks(kind, after=self.cursor(kind))
            if not pending:
                return stats
            for r in self.ingest_many(pending, kind).results:
                stats.add(r)

    def ingest_many(self, chunk_numbers: Iterable[int], kind: str) -> IngestStats:
        stats = IngestStats()
        for n in sorted(set(chunk_numbers)):
            stats.add(self.ingest_chunk(n, kind))
        return stats

    def ingest_chunk(self, chunk_number: int, kind: str) -> ChunkResult:
        if self.is_ingested(chunk_number):
            log.debug("chunk %d already ingested; skipping", chunk_number)
            return ChunkResult(chunk_number, kind, skipped=True)

        t0 = time.perf_counter()
        res = ChunkResult(chunk_number, kind)
        buffers: dict[str, list[tuple]] = {t: [] for t in INSERTS}
        buffered = 0
        first_id: int | None = None
        last_id: int | None = None
        before = self.conn.total_changes

        with transaction(self.conn):
            # Re-check inside the write lock: another process may have won.
            if self.is_ingested(chunk_number):
                return ChunkResult(chunk_number, kind, skipped=True)

            for idx, raw in enumerate(self.client.iter_chunk(chunk_number)):
                res.event_count += 1
                try:
                    eid = ev.event_id(raw)
                    etype = ev.event_type(raw)
                    projection = ev.PROJECTIONS.get(etype)
                    proj_row = projection[1](raw, eid) if projection else None
                except ev.InvalidEvent as e:
                    res.rejected += 1
                    buffers["rejected_events"].append(
                        (chunk_number, idx, str(e), json.dumps(raw)))
                    buffered += 1
                else:
                    if last_id is not None and eid <= last_id:
                        log.warning("chunk %d: non-increasing event_id %d after %d",
                                    chunk_number, eid, last_id)
                    first_id = eid if first_id is None else min(first_id, eid)
                    last_id = eid if last_id is None else max(last_id, eid)
                    buffers["events"].append((
                        eid, ev.event_ts(raw), etype, raw.get("workflow_id"),
                        raw.get("tenant_id"), chunk_number,
                        json.dumps(raw, separators=(",", ":")),
                    ))
                    if projection:
                        buffers[projection[0]].append(proj_row)
                    buffered += 1

                if buffered >= self.batch_size:
                    res.inserted += self._flush(buffers)
                    buffered = 0

            res.inserted += self._flush(buffers)
            update_aggregates_for_chunk(self.conn, chunk_number)
            self.conn.execute(
                "INSERT INTO event_chunks(chunk_number, kind, event_count, inserted_count, "
                "rejected_count, first_event_id, last_event_id) VALUES (?,?,?,?,?,?,?)",
                (chunk_number, kind, res.event_count, res.inserted, res.rejected,
                 first_id, last_id),
            )
            if last_id is not None:
                set_state(self.conn, "last_event_id",
                          max(last_id, int(self._state_int("last_event_id") or 0)))

        res.seconds = time.perf_counter() - t0
        if res.rejected:
            log.warning("chunk %d: %d/%d events rejected (see rejected_events)",
                        chunk_number, res.rejected, res.event_count)
        log.debug("chunk %d (%s): %d events, %d new rows, %.3fs (total_changes +%d)",
                  chunk_number, kind, res.event_count, res.inserted, res.seconds,
                  self.conn.total_changes - before)
        return res

    # ---------------------------------------------------------------- helpers

    def _flush(self, buffers: dict[str, list[tuple]]) -> int:
        """Write buffered rows. Returns how many NEW raw events were inserted."""
        new_events = 0
        for table, rows in buffers.items():
            if not rows:
                continue
            before = self.conn.total_changes
            self.conn.executemany(INSERTS[table], rows)
            if table == "events":
                new_events = self.conn.total_changes - before
            rows.clear()
        return new_events

    def _state_int(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM controller_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None
