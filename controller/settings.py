"""Controller settings, read from environment variables.

Kept deliberately small: a frozen dataclass validated at construction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

TICK_MINUTES = 15


@dataclass(frozen=True)
class Settings:
    sim_url: str
    advance_minutes: int = 60
    http_timeout_s: float = 30.0
    http_get_retries: int = 3
    db_path: str | None = None        # explicit file; None = one file per scenario
    db_dir: str = "data/worlds"       # where per-scenario files live when db_path is None
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not self.sim_url:
            raise ValueError("sim_url is required")
        if self.advance_minutes <= 0 or self.advance_minutes % TICK_MINUTES:
            raise ValueError(
                f"advance_minutes must be a positive multiple of {TICK_MINUTES}, "
                f"got {self.advance_minutes}"
            )
        if self.http_timeout_s <= 0:
            raise ValueError("http_timeout_s must be positive")
        if self.http_get_retries < 0:
            raise ValueError("http_get_retries must be >= 0")

    @classmethod
    def from_env(cls, sim_url: str | None = None) -> "Settings":
        env = os.environ
        return cls(
            sim_url=(sim_url or env.get("SIM_URL", "")).rstrip("/"),
            advance_minutes=int(env.get("ADVANCE_MINUTES", "60")),
            http_timeout_s=float(env.get("HTTP_TIMEOUT_S", "30")),
            http_get_retries=int(env.get("HTTP_GET_RETRIES", "3")),
            db_path=env.get("DB_PATH") or None,
            db_dir=env.get("DB_DIR", "data/worlds"),
            log_level=env.get("LOG_LEVEL", "INFO"),
        )
