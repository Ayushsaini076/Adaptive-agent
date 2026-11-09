"""In-memory fake simulator served through httpx.MockTransport.

It speaks only HTTP to the client under test, mirroring the real contract,
so tests exercise the same code path as production.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any

import httpx

from controller.client import SimulatorClient

Handler = Callable[[httpx.Request], httpx.Response]


def _change_record(version: int, actor: str, dims: list[str]) -> dict:
    return {"version": version, "actor": actor, "applied_at": None, "approval_id": None,
            "changed_dims": dims, "proposal_id": None}


class BrokenStream(httpx.SyncByteStream):
    """Yields ``good_lines`` NDJSON lines, then fails like a dropped connection."""

    def __init__(self, lines: list[str], good_lines: int) -> None:
        self.lines, self.good = lines, good_lines

    def __iter__(self):
        for line in self.lines[: self.good]:
            yield line.encode()
        raise httpx.ReadError("connection reset mid-chunk")


def campaign_event(eid: int, campaign: str, wf: str = "wf_a", n: int = 2) -> dict:
    return {"event_id": eid, "type": "campaign_created", "ts": "2027-01-04T09:00:00+05:30",
            "tenant_id": "t1", "workflow_id": wf, "campaign_id": campaign,
            "lead_count": n, "config_version": 3}


def lead_event(eid: int, lead: str, campaign: str, wf: str = "wf_a") -> dict:
    return {"event_id": eid, "type": "lead", "ts": "2027-01-04T09:00:00+05:30",
            "workflow_id": wf, "campaign_id": campaign, "lead_id": lead,
            "person_id": "p_" + lead,
            "features": {"lang": "hi", "city_tier": 2, "age_band": "30-45", "value_band": "mid"}}


def call_event(eid: int, lead: str, campaign: str, wf: str = "wf_a", *,
               connected: bool = True, engaged: bool = True, attempt: int = 1) -> dict:
    return {"event_id": eid, "type": "call", "ts": "2027-01-04T10:00:00+05:30",
            "tenant_id": "t1", "workflow_id": wf, "campaign_id": campaign,
            "lead_id": lead, "person_id": "p_" + lead, "attempt": attempt,
            "campaign_day": 0, "config_version": 3, "cohort_id": "default",
            "variant": {"welcome_message": "w1", "voice": "v1", "caller_id": "c1",
                        "prompt_version": "p1"},
            "connected": connected, "engaged": engaged and connected,
            "disposition": ("concluded_interested" if engaged and connected
                            else "short_hangup" if connected else "no_answer"),
            "duration_s": 42.0, "turns": 6,
            "qa": {"hallucination_flag": False, "flow_adherence": 0.9} if connected else None}


def chunk_of_activity(first_eid: int, campaign: str, n_leads: int = 2) -> list[dict]:
    """campaign_created, n leads, one call per lead; event_ids consecutive."""
    out = [campaign_event(first_eid, campaign, n=n_leads)]
    eid = first_eid + 1
    for i in range(n_leads):
        out.append(lead_event(eid, f"{campaign}_L{i}", campaign)); eid += 1
    for i in range(n_leads):
        out.append(call_event(eid, f"{campaign}_L{i}", campaign, connected=i % 2 == 0))
        eid += 1
    return out


@dataclass
class FakeSimulator:
    workflows: dict[str, dict] = field(default_factory=dict)
    configs: dict[str, dict] = field(default_factory=dict)
    history_chunks: dict[int, list[dict]] = field(default_factory=dict)
    event_chunks: dict[int, list[dict]] = field(default_factory=dict)
    minute: int = 0
    days: int = 2
    credits: int = 100
    requests: list[httpx.Request] = field(default_factory=list)
    # path -> list of responses to return before normal handling (fault injection)
    faults: dict[str, list[httpx.Response]] = field(default_factory=dict)
    # chunk_number -> number of lines to serve before the stream breaks (one-shot)
    broken_chunks: dict[int, int] = field(default_factory=dict)
    next_event_id: int = 1000
    # /config/history returns CHANGE RECORDS (no config content), as observed
    # on the real simulator.
    config_history: dict[str, list[dict]] = field(default_factory=dict)
    scenario: str = "fake_scenario"
    advance_has_clock: bool = True   # False: advance response carries no time
    # Governance endpoints. Pending approvals are decided on the next advance
    # (after decide_after_minutes) with ``approval_decision``.
    approvals: dict[str, dict] = field(default_factory=dict)
    approval_decision: str = "approved"
    decide_after_minutes: int = 60
    apply_window_minutes: int = 120
    pending_events: list[dict] = field(default_factory=list)
    applies: list[str] = field(default_factory=list)
    puts: list[dict] = field(default_factory=list)
    # Optional generator replacing the default activity: (sim, first_minute,
    # last_minute) -> events. Lets a test dial with whatever config is LIVE.
    activity: Callable | None = None

    @property
    def finished(self) -> bool:
        return self.minute >= self.days * 1440

    def _sim_time(self) -> str:
        base = datetime.fromisoformat("2027-01-04T00:00:00+05:30")
        return (base + timedelta(minutes=self.minute)).isoformat()

    def restart(self) -> None:
        """Simulator process restart: a NEW world with the same scenario."""
        self.minute = 0
        self.event_chunks = {}
        self.next_event_id = 1000

    def _ts(self, minute: int) -> str:
        base = datetime.fromisoformat("2027-01-04T00:00:00+05:30")
        return (base + timedelta(minutes=minute)).isoformat()

    def _event(self, **fields) -> dict:
        e = {"event_id": self.next_event_id, "ts": self._ts(self.minute), **fields}
        self.next_event_id += 1
        self.pending_events.append(e)
        return e

    def _set_config(self, wf: str, cfg: dict, actor: str, dims: list[str],
                    proposal_id=None, approval_id=None) -> dict:
        new = {**cfg, "version": self.configs[wf]["version"] + 1}
        self.config_history.setdefault(wf, self._history_records(wf)).append(
            {**_change_record(new["version"], actor, dims), "proposal_id": proposal_id,
             "approval_id": approval_id})
        self.configs[wf] = new
        self._event(type="config_changed", workflow_id=wf, new_version=new["version"],
                    actor=actor, changed_dims=dims, proposal_id=proposal_id,
                    approval_id=approval_id)
        return new

    @staticmethod
    def _dims(old: dict, new: dict) -> list[str]:
        dims = set()
        for k in ("prompt_version", "schedule", "cohorts"):
            if old.get(k) != new.get(k):
                dims.add(k)
        oa, na = old.get("allocations") or {}, new.get("allocations") or {}
        for cohort in set(oa) | set(na):
            for d in ("welcome_message", "voice", "caller_id"):
                if (oa.get(cohort) or {}).get(d) != (na.get(cohort) or {}).get(d):
                    dims.add(d)
        return sorted(dims)

    def flush_events(self) -> int | None:
        """Put pending events into a new chunk now (tests that don't advance)."""
        if not self.pending_events:
            return None
        n = max([*self.event_chunks, *self.history_chunks], default=0) + 1
        self.event_chunks[n] = self.pending_events
        self.pending_events = []
        return n

    def _decide_approvals(self) -> None:
        for a in self.approvals.values():
            if a["status"] == "pending" and self.minute - a["submitted_minute"] >= \
                    self.decide_after_minutes:
                a["status"] = self.approval_decision
                a["decided_minute"] = self.minute
                if a["status"] == "approved":
                    a["apply_by_minute"] = self.minute + self.apply_window_minutes
                    a["apply_by"] = self._ts(a["apply_by_minute"])
                self._event(type="approval_decided", approval_id=a["approval_id"],
                            workflow_id=a["workflow_id"], status=a["status"],
                            reasons=["scripted"])
            if (a["status"] == "approved"
                    and self.minute > a.get("apply_by_minute", 10**9)):
                a["status"] = "lapsed"

    def _history_records(self, wf: str) -> list[dict]:
        return self.config_history.get(wf) or [_change_record(
            self.configs[wf]["version"], "business_default", [])]

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if self.faults.get(path):
            return self.faults[path].pop(0)

        m, parts = request.method, path.strip("/").split("/")
        if (m, path) == ("GET", "/health"):
            return httpx.Response(200, json={"status": "ok"})
        if (m, path) == ("GET", "/sim/state"):
            return httpx.Response(200, json={"scenario": self.scenario,
                                             "sim_minute": self.minute,
                                             "ts": self._sim_time(),
                                             "end_minute": self.days * 1440,
                                             "finished": self.finished,
                                             "tick_minutes": 15,
                                             "credits_remaining": self.credits})
        if (m, path) == ("POST", "/sim/advance"):
            minutes = json.loads(request.content)["minutes"]
            if minutes <= 0 or minutes % 15:
                return httpx.Response(422, json={"detail": "minutes must be multiple of 15"})
            if self.finished:
                return httpx.Response(409, json={"detail": "finished"})
            start = self.minute
            self.minute += minutes
            n = max([*self.event_chunks, *self.history_chunks], default=0) + 1
            new = [n]
            if self.activity is not None:
                self.event_chunks[n] = self.activity(self, start, self.minute)
            else:
                self.event_chunks[n] = chunk_of_activity(self.next_event_id, f"camp{n}")
                self.next_event_id += 100
            self._decide_approvals()
            if self.pending_events:
                self.event_chunks[n] = self.event_chunks[n] + self.pending_events
                self.pending_events = []
            body = {"new_chunks": new, "finished": self.finished}
            if self.advance_has_clock:
                body |= {"sim_minute": self.minute, "ts": self._sim_time()}
            return httpx.Response(200, json=body)
        if (m, path) == ("POST", "/approvals"):
            body = json.loads(request.content)
            if any(a["proposal_id"] == body["proposal_id"] for a in self.approvals.values()):
                return httpx.Response(409, json={"detail": "duplicate_proposal_id"})
            for k in ("proposal_id", "workflow_id", "base_version", "proposed_config",
                      "change_summary", "diff", "dry_run", "rationale"):
                if not body.get(k) and body.get(k) != 0:
                    return httpx.Response(422, json={"detail": f"missing {k}"})
            if not 0 <= body.get("impact_score", -1) <= 1 or body.get("affected_leads", -1) < 0:
                return httpx.Response(422, json={"detail": "bad impact/affected_leads"})
            aid = f"apr_{len(self.approvals) + 1}"
            self.approvals[aid] = {"approval_id": aid, "status": "pending",
                                   "submitted_minute": self.minute, **body}
            return httpx.Response(200, json={"approval_id": aid, "status": "pending"})
        if m == "GET" and parts[0] == "approvals" and len(parts) == 2:
            a = self.approvals.get(parts[1])
            if a is None:
                return httpx.Response(404, json={"detail": "unknown approval"})
            self._decide_approvals()
            return httpx.Response(200, json={k: v for k, v in a.items()
                                             if k != "proposed_config"})
        if (m, path) == ("GET", "/approvals"):
            wf = request.url.params.get("workflow_id")
            return httpx.Response(200, json=[{k: v for k, v in a.items()
                                              if k != "proposed_config"}
                                             for a in self.approvals.values()
                                             if a["workflow_id"] == wf])
        if m == "POST" and parts[0] == "workflows" and parts[2:] == ["apply"]:
            wf, aid = parts[1], json.loads(request.content)["approval_id"]
            a = self.approvals[aid]
            self._decide_approvals()
            if a["status"] == "lapsed":
                return httpx.Response(410, json={"detail": "lapsed"})
            if a["status"] != "approved":
                return httpx.Response(409, json={"detail": f"not_approved:{a['status']}"})
            if a["base_version"] != self.configs[wf]["version"]:
                return httpx.Response(409, json={"detail": "stale_base_version"})
            new = self._set_config(wf, a["proposed_config"], "controller_approved",
                                   self._dims(self.configs[wf], a["proposed_config"]),
                                   a["proposal_id"], aid)
            a["status"] = "applied"
            self.applies.append(aid)
            return httpx.Response(200, json={"version": new["version"]})
        if m == "PUT" and parts[0] == "workflows" and parts[2:] == ["config"]:
            wf, body = parts[1], json.loads(request.content)
            self.puts.append(body)
            aa = (self.workflows[wf].get("governance") or {}).get("auto_apply") or {}
            dims = self._dims(self.configs[wf], body["proposed_config"])
            if not aa.get("enabled") or set(dims) - set(aa.get("allowed_dims") or []):
                return httpx.Response(403, json={"detail": "auto_apply_not_permitted"})
            if body["base_version"] != self.configs[wf]["version"]:
                return httpx.Response(409, json={"detail": "stale_base_version"})
            new = self._set_config(wf, body["proposed_config"], "controller_auto", dims,
                                   body["proposal_id"])
            return httpx.Response(200, json={"version": new["version"]})
        if (m, path) == ("GET", "/workflows"):
            return httpx.Response(200, json=[{"workflow_id": w, **v}
                                             for w, v in self.workflows.items()])
        if m == "GET" and parts[0] == "workflows" and len(parts) >= 2:
            wf = parts[1]
            if wf not in self.workflows:
                return httpx.Response(404, json={"detail": "unknown workflow"})
            if len(parts) == 2:
                return httpx.Response(200, json={"workflow_id": wf, **self.workflows[wf]})
            if parts[2:] == ["config"]:
                return httpx.Response(200, json=self.configs[wf])
            if parts[2:] == ["config", "history"]:
                return httpx.Response(200, json=self._history_records(wf))
        if (m, path) == ("GET", "/events/chunks"):
            kind = request.url.params["kind"]
            after = int(request.url.params.get("after", -1))
            src = self.history_chunks if kind == "history" else self.event_chunks
            return httpx.Response(200, json={"chunks": [
                {"chunk_number": n, "kind": kind} for n in sorted(src) if n > after]})
        if m == "GET" and parts[:2] == ["events", "chunks"] and len(parts) == 3:
            n = int(parts[2])
            events = self.history_chunks.get(n) or self.event_chunks.get(n)
            if events is None:
                return httpx.Response(404, json={"detail": "no such chunk"})
            lines = [json.dumps(e) + "\n" for e in events]
            if n in self.broken_chunks:
                return httpx.Response(200, stream=BrokenStream(lines, self.broken_chunks.pop(n)))
            body = "".join(lines)
            return httpx.Response(200, text=body,
                                  headers={"content-type": "application/x-ndjson"})
        return httpx.Response(404, json={"detail": f"no route {m} {path}"})

    def supervisor_edit(self, wf: str, **changes: Any) -> dict:
        """Change a config the way a client supervisor would; returns the event."""
        old = self.configs[wf]
        new = {**old, **changes, "version": old["version"] + 1}
        self.config_history.setdefault(wf, self._history_records(wf)).append(
            _change_record(new["version"], "supervisor", sorted(changes)))
        self.configs[wf] = new
        eid = self.next_event_id
        self.next_event_id += 1
        return {"event_id": eid, "type": "config_changed",
                "ts": self._ts(max(self.minute, 720)),
                "workflow_id": wf, "new_version": new["version"], "actor": "supervisor",
                "changed_dims": sorted(changes)}

    def client(self, **kwargs: Any) -> SimulatorClient:
        kwargs.setdefault("backoff_s", 0)
        return SimulatorClient("http://sim.test", transport=httpx.MockTransport(self.handler),
                               **kwargs)


def sample_simulator() -> FakeSimulator:
    return FakeSimulator(
        workflows={
            "wf_a": {"tenant_id": "t1", "objective": {"primary": "pickup",
                                                      "guardrail": {"kpi": "engagement"}},
                     "mid_flight": "allowed"},
            "wf_b": {"tenant_id": "t2", "objective": {"primary": "engagement"},
                     "mid_flight": "next_campaign_only"},
        },
        configs={"wf_a": {"version": 3, "prompt_version": "p1",
                          "cohorts": [{"cohort_id": "hi_t2",
                                       "where": {"lang": ["hi"], "city_tier": [2]}}]},
                 "wf_b": {"version": 1, "prompt_version": "p9", "cohorts": []}},
        history_chunks={1: chunk_of_activity(1, "hist1"),
                        2: chunk_of_activity(100, "hist2", n_leads=3)},
    )
