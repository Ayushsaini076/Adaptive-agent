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
            start = max(self.event_chunks, default=0) + 1
            new = [start]
            self.event_chunks[start] = [{"event_id": start * 10, "type": "call"}]
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
            body = "".join(json.dumps(e) + "\n" for e in events)
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
        history_chunks={1: [{"event_id": 1, "type": "campaign_created"},
                            {"event_id": 2, "type": "lead"}],
                        2: [{"event_id": 3, "type": "call"}]},
    )
