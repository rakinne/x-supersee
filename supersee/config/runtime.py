"""Runtime configuration: pool sizes, concurrency caps, retention, logging.

Most values are tunable via env vars prefixed `SUPERSEE_RUNTIME_`. A few
keep their conventional bare names (DATABASE_URL, LOG_LEVEL, SUPERSEE_MOCK_LLM,
SUPERSEE_XRPL_WS_URL) and are wired via `validation_alias`.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SUPERSEE_RUNTIME_",
        extra="ignore",
    )

    # --- Database ---
    # DATABASE_URL is the standard env var name across the Python ecosystem.
    database_url: str = Field(..., validation_alias="DATABASE_URL")
    pool_min_size: int = Field(default=1, gt=0)
    pool_max_size: int = Field(default=10, gt=0)

    # --- Concurrency ---
    # Semaphore size around graph.ainvoke. Prevents the crash-recovery sweep
    # from stampeding the LLM API on cold start (see eng-review hardening #1B).
    langgraph_concurrency: int = Field(default=4, gt=0)

    # --- Background-task supervisor ---
    restart_max_backoff_seconds: float = Field(default=60.0, gt=0.0)
    # Cap on the recovery sweep batch so a long outage doesn't pile up at boot.
    recovery_sweep_limit: int = Field(default=1000, gt=0)

    # --- Retention ---
    case_artifact_retention_days: int = Field(default=90, gt=0)

    # --- HTTP ---
    http_host: str = Field(default="0.0.0.0")
    http_port: int = Field(default=8000, gt=0, le=65535)

    # --- XRPL ingestor ---
    xrpl_ws_url: str = Field(
        default="wss://xrplcluster.com/",
        validation_alias="SUPERSEE_XRPL_WS_URL",
    )
    # Beyond ~13 minutes the public cluster may not retain history reliably.
    websocket_backfill_cap_ledgers: int = Field(default=256, gt=0)

    # --- Logging ---
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    # --- LLM mock for offline runs and CI ---
    mock_llm: bool = Field(default=False, validation_alias="SUPERSEE_MOCK_LLM")
