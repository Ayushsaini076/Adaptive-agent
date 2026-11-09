"""In-memory fake simulator served through httpx.MockTransport.

It speaks only HTTP to the client under test, mirroring the real contract,
so tests exercise the same code path as production.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from controller.client import SimulatorClient

Handler = Callable[[httpx.Request], httpx.Response]


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
            "disposition": "concluded_interested" if engaged else "no_answer",
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

    @property
    def finished(self) -> bool:
        return self.minute >= self.days * 1440

    def _sim_time(self) -> str:
        d, rem = divmod(self.minute, 1440)
        return f"D{d} {rem // 60:02d}:{rem % 60:02d}"

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if self.faults.get(path):
            return self.faults[path].pop(0)

        m, parts = request.method, path.strip("/").split("/")
        if (m, path) == ("GET", "/health"):
            return httpx.Response(200, json={"status": "ok"})
        if (m, path) == ("GET", "/sim/state"):
            return httpx.Response(200, json={"sim_time": self._sim_time(),
                                             "finished": self.finished,
                                             "credits_remaining": self.credits})
        if (m, path) == ("POST", "/sim/advance"):
            minutes = json.loads(request.content)["minutes"]
            if minutes <= 0 or minutes % 15:
                return httpx.Response(422, json={"detail": "minutes must be multiple of 15"})
            if self.finished:
                return httpx.Response(409, json={"detail": "finished"})
            self.minute += minutes
            n = max([*self.event_chunks, *self.history_chunks], default=0) + 1
            new = [n]
            self.event_chunks[n] = chunk_of_activity(self.next_event_id, f"camp{n}")
            self.next_event_id += 100
            return httpx.Response(200, json={"new_chunks": new, "sim_time": self._sim_time(),
                                             "finished": self.finished})
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
                return httpx.Response(200, json=[self.configs[wf]])
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
        configs={"wf_a": {"version": 3, "prompt_version": "p1"},
                 "wf_b": {"version": 1, "prompt_version": "p9"}},
        history_chunks={1: chunk_of_activity(1, "hist1"),
                        2: chunk_of_activity(100, "hist2", n_leads=3)},
    )
