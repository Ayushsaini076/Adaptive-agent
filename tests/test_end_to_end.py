"""Full loop through main.run(): a caller ID degrades, the controller detects
it, proposes a mitigation within approver timing rules, the (fake) approver
approves, the controller applies it, the incident is marked mitigated, and the
audit query shows the change with before/after pickup.

The fake dials with whatever caller-ID allocation is LIVE, so the mitigation
really changes subsequent traffic.
"""

import json

from controller import audit
from controller.database import connect
from controller.main import run
from controller.settings import Settings
from tests.fakes import FakeSimulator
from tests.test_governance import META, _config

ONSET_MINUTE = 15 * 60          # D0 15:00


def activity(sim, start, end):
    """240 dials per hour 09:00-21:00 for ks_cancel, caller ID by live allocation.
    D2 pickup collapses from 0.25 to 0.08 at ONSET_MINUTE; D1 stays 0.25."""
    events = []
    alloc = sim.configs["ks_cancel"]["allocations"]["default"]["caller_id"]
    ids = [c for c, wgt in sorted(alloc.items()) if wgt > 0]
    shares = [alloc[c] for c in ids]
    for minute in range(start, end, 60):
        hour = (minute // 60) % 24
        if not 9 <= hour < 21:
            continue
        for i in range(240):
            # deterministic split by live weights
            acc, cid = 0.0, ids[-1]
            for c, sh in zip(ids, shares):
                acc += sh
                if (i + 0.5) / 240 < acc:
                    cid = c
                    break
            rate = 0.08 if cid == "D2" and minute >= ONSET_MINUTE else 0.25
            connected = (i * rate) % 1 + rate >= 1
            events.append({
                "event_id": sim.next_event_id, "type": "call", "ts": sim._ts(minute + 10),
                "workflow_id": "ks_cancel", "tenant_id": "kestrel_finance",
                "campaign_id": "c", "lead_id": f"L{sim.next_event_id}",
                "person_id": f"p{sim.next_event_id}", "attempt_no": 1,
                "config_version": sim.configs["ks_cancel"]["version"], "cohort_id": "default",
                "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": cid,
                            "prompt_version": "P1"},
                "connected": connected, "engaged": connected and (i // 4) % 2 == 0,
                "disposition": "concluded_interested" if connected and (i // 4) % 2 == 0
                else ("short_hangup" if connected else "no_answer")})
            sim.next_event_id += 1
    return events


def test_caller_id_incident_full_lifecycle(tmp_path):
    sim = FakeSimulator(workflows={"ks_cancel": json.loads(json.dumps(META["ks_cancel"]))},
                        configs={"ks_cancel": _config()}, days=3, activity=activity)
    sim.next_event_id = 10_000
    conn = connect(tmp_path / "c.db")
    s = run(Settings(sim_url="http://sim.test", advance_minutes=60), sim.client(), conn)
    assert s.finished

    incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents")]
    assert len(incidents) == 1                                     # no re-open after mitigation
    inc = incidents[0]
    assert (inc["subject"], inc["status"]) == ("D2", "mitigated")
    assert inc["detected_ts"] < "2027-01-05"                       # same day as onset

    p = dict(conn.execute("SELECT * FROM proposals").fetchone())
    assert p["route"] == "approval" and p["status"] == "applied"   # shift 0.5 > max_shift 0.2
    assert len(sim.applies) == 1 and sim.puts == []
    assert sim.configs["ks_cancel"]["allocations"]["default"]["caller_id"] == {"D1": 1.0}

    p_applied = conn.execute("SELECT decided_ts FROM proposals").fetchone()[0]
    assert inc["mitigated_ts"] >= p_applied >= inc["detected_ts"]  # simulated-time order
    steps = [r[0] for r in conn.execute(
        "SELECT action FROM audit_log WHERE ref=? ORDER BY id", (p["proposal_id"],))]
    assert steps == ["proposal_submitted", "proposal_approved", "proposal_applied"]
    from datetime import datetime
    from controller.policy import GovernancePolicy
    ok, why = GovernancePolicy.from_meta(META["ks_cancel"]).submission_check(
        datetime.fromisoformat(p["submitted_ts"]), 0)
    assert ok, why                                                 # sent only when decidable

    change, = audit.explain(conn, "ks_cancel")
    assert change["actor"] == "controller_approved"
    assert change["proposal"]["intents"][0]["kind"] == "incident_mitigation"
    before = change["observed"]["before"]["pickup"]["rate"]
    after = change["observed"]["after"]["pickup"]["rate"]
    assert after > before                                          # pickup recovered
    assert "D2" not in change["observed"]["after"]["pickup_by_caller_id"]
