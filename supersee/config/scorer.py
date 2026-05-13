"""Scorer configuration: weights, thresholds, risk-bands, memo regexes.

Tunable via env vars prefixed `SUPERSEE_SCORER_`. Nested fields use the
`env_nested_delimiter='__'`, e.g.:

    SUPERSEE_SCORER_WEIGHTS__LARGE_PAYMENT=0.25
    SUPERSEE_SCORER_THRESHOLDS__LARGE_PAYMENT_FLOOR_XRP=75000
    SUPERSEE_SCORER_BANDS__LOW_MAX=0.15
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuleWeights(BaseModel):
    """Per-rule contribution to the case risk score (clipped to [0,1])."""

    large_payment: float = Field(default=0.20, ge=0.0, le=1.0)
    velocity_burst: float = Field(default=0.15, ge=0.0, le=1.0)
    new_counterparty: float = Field(default=0.10, ge=0.0, le=1.0)
    memo_anomaly: float = Field(default=0.10, ge=0.0, le=1.0)
    watchlist_hit: float = Field(default=0.20, ge=0.0, le=1.0)
    # Calibrated so a single OFAC hit clears the high-band threshold alone.
    ofac_sdn_match: float = Field(default=0.50, ge=0.0, le=1.0)
    mixer_direct: float = Field(default=0.30, ge=0.0, le=1.0)


class RuleThresholds(BaseModel):
    """XRP-denominated thresholds (no USD oracle in MVP)."""

    # At ~$0.50 XRP, 50k XRP ≈ $25k. Floor used when account has no history.
    large_payment_floor_xrp: float = Field(default=50_000.0, gt=0.0)
    new_counterparty_min_xrp: float = Field(default=5_000.0, gt=0.0)
    velocity_burst_window_minutes: int = Field(default=5, gt=0)
    velocity_burst_min_count: int = Field(default=10, gt=0)


class RiskBands(BaseModel):
    """Risk-score banding boundaries.

    score < low_max → 'low' (auto_close)
    score < medium_max → 'medium' (hitl_pause path)
    else → 'high' (escalate flag set)
    """

    low_max: float = Field(default=0.20, ge=0.0, le=1.0)
    medium_max: float = Field(default=0.50, ge=0.0, le=1.0)


# Memo regex defaults. Heuristic — false positives are fine because the
# memo_anomaly weight is low and the LLM narrative downstream can dismiss
# noise. Tunable in code; rarely overridden via env.
DEFAULT_MEMO_PATTERNS: list[str] = [
    r"(?i)\btornado\s*cash\b",
    r"(?i)\bmixer\b",
    r"(?i)\bwasabi\b|\bsamourai\b",
    r"(?i)\bransom\b|\bdarkmarket\b",
    r"(?i)\bsanction(?:ed|s)?\b",
    r"(?i)\bofac\b",
    r"^[a-f0-9]{64,}$",
]


class ScorerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SUPERSEE_SCORER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    weights: RuleWeights = Field(default_factory=RuleWeights)
    thresholds: RuleThresholds = Field(default_factory=RuleThresholds)
    bands: RiskBands = Field(default_factory=RiskBands)
    memo_patterns: list[str] = Field(default_factory=lambda: list(DEFAULT_MEMO_PATTERNS))

    # Lazy P99 recompute window: refresh if cached value is older than this.
    p99_lookback_days: int = Field(default=7, gt=0)
    p99_recompute_age_minutes: int = Field(default=60, gt=0)

    # Optional static rate kept for callers that want USD framing in
    # rationale strings. Unused by the scorer's rule logic.
    xrp_usd_rate: float | None = Field(default=None, gt=0.0)
