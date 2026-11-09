"""HTTP client for the simulator.

This is the ONLY way the controller talks to the simulator. Design choices:

* GET requests are idempotent, so they are retried on transport errors and 5xx
  with exponential backoff.
* POST/PUT requests are NOT retried automatically. Retrying ``/sim/advance``
  after a timeout could advance simulated time twice; retrying governance calls
  is left to callers that hold an idempotency key (``proposal_id``).
* HTTP errors are mapped to typed exceptions so callers can handle
  402 (credits), 409 (finished / stale base / duplicate) and 410 (lapsed)
  deliberately instead of string-matching.
* Response shapes not pinned down by the spec are parsed defensively and the
  raw payload is always kept alongside the normalised fields.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx

from controller.settings import TICK_MINUTES

log = logging.getLogger(__name__)

JSON = dict[str, Any]


# --------------------------------------------------------------------------- errors


class SimulatorError(Exception):
    """Any non-success response or unrecoverable transport failure."""

    def __init__(self, message: str, status: int | None = None,
                 code: str | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.body = body


class SimulatorUnavailable(SimulatorError):
    """Transport error or 5xx that persisted after retries."""


class NotFound(SimulatorError):
    """404."""


class InsufficientCredits(SimulatorError):
    """402: credits exhausted. Treated as a controller failure upstream."""


class Conflict(SimulatorError):
    """409 that is not one of the more specific cases below."""


class ScenarioFinished(Conflict):
    """409 ``finished``: the scenario has ended; no further advances."""


class StaleBaseVersion(Conflict):
    """409 ``stale_base_version``: config changed after the proposal was made."""


class Lapsed(SimulatorError):
    """410: an approved request was not applied before ``apply_by``."""


def _error_code(body: Any) -> str | None:
    """Extract a machine-readable error code from a FastAPI-style error body.

    Handles ``{"detail": "finished"}``, ``{"detail": {"error": "..."}}``,
    ``{"error": "..."}`` and ``{"code": "..."}``.
    """
    if not isinstance(body, dict):
        return None
    detail = body.get("detail", body)
    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        for key in ("error", "code", "reason", "status"):
            if isinstance(detail.get(key), str):
                return detail[key]
    return None


def _raise_for_response(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    try:
        body: Any = resp.json()
    except ValueError:
        body = resp.text
    code = _error_code(body)
    msg = f"{resp.request.method} {resp.request.url.path} -> {resp.status_code} {code or body!r}"
    status = resp.status_code
    code_l = (code or "").lower()

    if status == 402:
        raise InsufficientCredits(msg, status, code, body)
    if status == 404:
        raise NotFound(msg, status, code, body)
    if status == 409:
        if "stale" in code_l:
            raise StaleBaseVersion(msg, status, code, body)
        if "finished" in code_l:
            raise ScenarioFinished(msg, status, code, body)
        raise Conflict(msg, status, code, body)
    if status == 410:
        raise Lapsed(msg, status, code, body)
    if status >= 500:
        raise SimulatorUnavailable(msg, status, code, body)
    raise SimulatorError(msg, status, code, body)


# --------------------------------------------------------------------------- results


@dataclass(frozen=True)
class AdvanceResult:
    new_chunks: list[int]
    sim_time: str | None
    finished: bool
    raw: JSON = field(repr=False)
    sim_minute: int | None = None


# Keys the simulator uses for simulated time. Observed on /sim/state:
# {"sim_minute": 0, "ts": "2027-01-04T00:00:00+05:30", ...}. Older aliases kept
# as fallbacks.
TIME_KEYS = ("ts", "sim_time", "current_time", "now", "time")


def sim_clock(payload: JSON) -> tuple[str | None, int | None]:
    """(timestamp, sim_minute) from a /sim/state or /sim/advance payload."""
    ts = _first(payload, *TIME_KEYS)
    minute = payload.get("sim_minute")
    try:
        minute = int(minute) if minute is not None else None
    except (TypeError, ValueError):
        minute = None
    return (str(ts) if ts is not None else None), minute


def _chunk_number(item: Any) -> int:
    if isinstance(item, int):
        return item
    if isinstance(item, dict):
        for key in ("chunk_number", "chunk", "number", "id"):
            if key in item:
                return int(item[key])
    raise ValueError(f"cannot read chunk number from {item!r}")


def _chunk_numbers(payload: Any) -> list[int]:
    """Normalise a chunk listing to a sorted list of chunk numbers."""
    items = payload
    if isinstance(payload, dict):
        for key in ("chunks", "new_chunks", "items"):
            if key in payload:
                items = payload[key]
                break
        else:
            raise ValueError(f"unrecognised chunk listing: {payload!r}")
    if not isinstance(items, list):
        raise ValueError(f"unrecognised chunk listing: {payload!r}")
    return sorted(_chunk_number(i) for i in items)


def _first(d: JSON, *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d:
            return d[k]
    return default


# --------------------------------------------------------------------------- client


class SimulatorClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 30.0,
        get_retries: int = 3,
        backoff_s: float = 0.5,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._get_retries = get_retries
        self._backoff_s = backoff_s
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
            transport=transport,
            headers={"User-Agent": "adaptive-agent-controller/0.1"},
        )

    # -- plumbing ---------------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "SimulatorClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _sleep_before_retry(self, attempt: int) -> None:
        time.sleep(self._backoff_s * (2 ** attempt))

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        last: Exception | None = None
        for attempt in range(self._get_retries + 1):
            try:
                resp = self._http.get(path, params=params)
            except httpx.TransportError as e:
                last = e
                log.warning("GET %s transport error (attempt %d): %s", path, attempt + 1, e)
            else:
                if resp.status_code < 500:
                    _raise_for_response(resp)
                    return resp.json()
                last = SimulatorUnavailable(
                    f"GET {path} -> {resp.status_code}", resp.status_code, body=resp.text)
                log.warning("GET %s -> %d (attempt %d)", path, resp.status_code, attempt + 1)
            if attempt < self._get_retries:
                self._sleep_before_retry(attempt)
        if isinstance(last, SimulatorError):
            raise last
        raise SimulatorUnavailable(f"GET {path} failed: {last}") from last

    def _send(self, method: str, path: str, body: JSON) -> Any:
        """Non-idempotent request: exactly one attempt."""
        try:
            resp = self._http.request(method, path, json=body)
        except httpx.TransportError as e:
            raise SimulatorUnavailable(f"{method} {path} failed: {e}") from e
        _raise_for_response(resp)
        return resp.json() if resp.content else {}

    # -- discovery / state ------------------------------------------------------

    def health(self) -> JSON:
        return self._get("/health")

    def state(self) -> JSON:
        return self._get("/sim/state")

    def list_workflows(self) -> list[JSON]:
        payload = self._get("/workflows")
        if isinstance(payload, dict):
            payload = _first(payload, "workflows", "items", default=payload)
        if not isinstance(payload, list):
            raise ValueError(f"unrecognised /workflows payload: {payload!r}")
        # Some APIs return bare IDs; normalise to dicts with workflow_id.
        return [w if isinstance(w, dict) else {"workflow_id": w} for w in payload]

    def get_workflow(self, workflow_id: str) -> JSON:
        return self._get(f"/workflows/{workflow_id}")

    def get_config(self, workflow_id: str) -> JSON:
        return self._get(f"/workflows/{workflow_id}/config")

    def get_config_history(self, workflow_id: str) -> list[JSON]:
        payload = self._get(f"/workflows/{workflow_id}/config/history")
        if isinstance(payload, dict):
            payload = _first(payload, "history", "versions", "items", default=[])
        return list(payload)

    # -- events -----------------------------------------------------------------

    def list_chunks(self, kind: str, after: int | None = None) -> list[int]:
        """Chunk numbers of ``kind`` ("history" or "events") strictly after ``after``."""
        if kind not in ("history", "events"):
            raise ValueError(f"kind must be 'history' or 'events', got {kind!r}")
        params: dict[str, Any] = {"kind": kind}
        if after is not None:
            params["after"] = after
        return _chunk_numbers(self._get("/events/chunks", params=params))

    def iter_chunk(self, chunk_number: int) -> Iterator[JSON]:
        """Stream one chunk's NDJSON events without loading it all into memory.

        Not retried mid-stream: a partial read raises, and the ingestion layer
        replays the whole chunk (its commit is atomic, inserts are idempotent).
        """
        path = f"/events/chunks/{chunk_number}"
        try:
            with self._http.stream("GET", path) as resp:
                if not resp.is_success:
                    resp.read()
                    _raise_for_response(resp)
                for lineno, line in enumerate(resp.iter_lines(), start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as e:
                        raise SimulatorError(
                            f"chunk {chunk_number} line {lineno}: invalid JSON: {e}") from e
                    if not isinstance(event, dict):
                        raise SimulatorError(
                            f"chunk {chunk_number} line {lineno}: expected object")
                    yield event
        except httpx.TransportError as e:
            raise SimulatorUnavailable(f"GET {path} failed: {e}") from e

    def fetch_chunk(self, chunk_number: int) -> list[JSON]:
        """Convenience for small chunks / tests. Prefer ``iter_chunk`` at scale."""
        return list(self.iter_chunk(chunk_number))

    # -- governance -------------------------------------------------------------
    # Never retried automatically (see module docstring). Approval submission is
    # idempotent on proposal_id; callers handle a 409 duplicate by looking it up.

    def submit_approval(self, payload: JSON) -> JSON:
        return self._send("POST", "/approvals", payload)

    def get_approval(self, approval_id: str) -> JSON:
        return self._get(f"/approvals/{approval_id}")

    def list_approvals(self, workflow_id: str, status: str | None = None) -> list[JSON]:
        params = {"workflow_id": workflow_id}
        if status:
            params["status"] = status
        payload = self._get("/approvals", params=params)
        if isinstance(payload, dict):
            payload = _first(payload, "approvals", "items", default=[])
        return list(payload)

    def apply_approval(self, workflow_id: str, approval_id: str) -> JSON:
        return self._send("POST", f"/workflows/{workflow_id}/apply",
                          {"approval_id": approval_id})

    def put_config(self, workflow_id: str, body: JSON) -> JSON:
        return self._send("PUT", f"/workflows/{workflow_id}/config", body)

    # -- time -------------------------------------------------------------------

    def advance(self, minutes: int) -> AdvanceResult:
        if minutes <= 0 or minutes % TICK_MINUTES:
            raise ValueError(f"minutes must be a positive multiple of {TICK_MINUTES}")
        raw = self._send("POST", "/sim/advance", {"minutes": minutes})
        ts, minute = sim_clock(raw)
        return AdvanceResult(
            new_chunks=_chunk_numbers(raw.get("new_chunks", [])),
            sim_time=ts,
            finished=bool(raw.get("finished", False)),
            raw=raw,
            sim_minute=minute,
        )
