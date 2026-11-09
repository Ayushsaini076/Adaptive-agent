"""Audit queries (C12): explain every config change from the controller's own DB.

    python -m controller.audit [--db PATH] [--workflow ID] [--hours 24]

For each config version: who changed it (supervisor / controller_approved /
controller_auto), the controller proposal and approval behind it, the
evidence and rationale, every governance step, and the KPIs in the
``hours`` before vs after the change (pickup and engagement with their own
denominators, plus pickup per caller ID). Before/after are observational
comparisons, not controlled estimates; they're labelled as such.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta

from controller.database import connect
from controller.metrics import kpis, workflow_kpis


def _shift(ts: str, hours: float) -> str:
    return (datetime.fromisoformat(ts) + timedelta(hours=hours)).isoformat(timespec="seconds")


def _window(conn: sqlite3.Connection, wf: str, start: str, end: str) -> dict:
    t = workflow_kpis(conn, wf, start=start, end=end)
    return {"start": start, "end": end, "dials": t.dials,
            "pickup": t.pickup.as_dict(), "engagement": t.engagement.as_dict(),
            "pickup_by_caller_id": {r.key[0]: r.pickup.as_dict() for r in
                                    kpis(conn, wf, by=("caller_id",), start=start, end=end)}}


def explain(conn: sqlite3.Connection, workflow_id: str, hours: float = 24) -> list[dict]:
    out = []
    for c in conn.execute("SELECT * FROM config_changes WHERE workflow_id=? ORDER BY event_id",
                          (workflow_id,)).fetchall():
        entry = {"version": c["new_version"], "at": c["ts"], "actor": c["actor"],
                 "changed_dims": json.loads(c["changed_dims"] or "[]"),
                 "proposal_id": c["proposal_id"], "approval_id": c["approval_id"]}
        p = None
        if c["proposal_id"]:
            p = conn.execute("SELECT * FROM proposals WHERE proposal_id=?",
                             (c["proposal_id"],)).fetchone()
        if p is None and c["approval_id"]:
            p = conn.execute("SELECT * FROM proposals WHERE approval_id=?",
                             (c["approval_id"],)).fetchone()
        if p is not None:
            body = json.loads(p["payload"])
            entry["proposal"] = {
                "route": p["route"], "status": p["status"], "base_version": p["base_version"],
                "submitted": p["submitted_ts"], "decided": p["decided_ts"],
                "intents": json.loads(p["intents"]), "change_summary": body["change_summary"],
                "rationale": body["rationale"], "diff": body["diff"],
                "impact_score": body["impact_score"], "affected_leads": body["affected_leads"],
                "approver_reasons": json.loads(p["reasons"]) if p["reasons"] else None,
                "governance_steps": [dict(r) for r in conn.execute(
                    "SELECT sim_ts, action, detail FROM audit_log WHERE ref=? ORDER BY id",
                    (p["proposal_id"],))]}
        elif c["actor"] == "supervisor":
            entry["note"] = "supervisor edit: authoritative, accepted, not reverted"
        if c["ts"]:
            entry["observed"] = {
                "note": "observational before/after windows, not a controlled estimate",
                "before": _window(conn, workflow_id, _shift(c["ts"], -hours), c["ts"]),
                "after": _window(conn, workflow_id, c["ts"], _shift(c["ts"], hours))}
        out.append(entry)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", default=None, help="database file (default: newest in data/worlds)")
    ap.add_argument("--workflow")
    ap.add_argument("--hours", type=float, default=24)
    a = ap.parse_args(argv)
    path = a.db
    if path is None:
        from pathlib import Path
        dbs = sorted(Path("data/worlds").glob("*.db"), key=lambda p: p.stat().st_mtime)
        if not dbs:
            print("no database found; pass --db", file=sys.stderr)
            return 2
        path = str(dbs[-1])
    conn = connect(path)
    wfs = [a.workflow] if a.workflow else [r[0] for r in conn.execute(
        "SELECT workflow_id FROM workflows ORDER BY 1")]
    print(json.dumps({wf: explain(conn, wf, a.hours) for wf in wfs}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
