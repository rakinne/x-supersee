"""Enrichment configuration: API URLs, retry budgets, refresh cadence.

Tunable via env vars prefixed `SUPERSEE_ENRICHMENT_`. ANTHROPIC_API_KEY
keeps its conventional bare name via `validation_alias`.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnrichmentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SUPERSEE_ENRICHMENT_",
        extra="ignore",
        populate_by_name=True,
    )

    # --- OFAC SDN crypto address feed ---
    # OpenSanctions models crypto wallets as their own `CryptoWallet`
    # schema entities. The broad `sanctions` collection contains none of
    # these at the moment; crypto wallets live in narrower sanctions-
    # adjacent datasets. We default to the FBI Lazarus Group dataset
    # because it (a) reliably returns CryptoWallet entries and (b) maps
    # to a real, defensible compliance use case. Operators running this
    # in production should set SUPERSEE_ENRICHMENT_OPENSANCTIONS_URL to
    # whatever consolidated feed they trust. See ofac.py module docstring.
    opensanctions_url: str = Field(
        default="https://data.opensanctions.org/datasets/latest/us_fbi_lazarus_crypto/entities.ftm.json",
    )
    # Refresh-on-startup-if-stale: triggers when fetched_at is older than this.
    ofac_refresh_age_hours: int = Field(default=25, gt=0)
    # On terminal 429/5xx, give up after this many seconds and keep the
    # existing table; log `ofac_refresh_failed` at WARNING.
    ofac_refresh_max_retry_seconds: int = Field(default=3600, gt=0)

    # --- Anthropic LLM ---
    # Optional: when None and SUPERSEE_MOCK_LLM is true, the system uses
    # canned narratives. When None and mock_llm is false, build_narrative
    # falls back to the low-confidence canned narrative.
    anthropic_api_key: str | None = Field(
        default=None,
        validation_alias="ANTHROPIC_API_KEY",
    )
    anthropic_model: str = Field(default="claude-haiku-4-5")
    # tenacity-driven transport retries for transient 429/5xx/timeout.
    anthropic_max_retries: int = Field(default=3, gt=0)
    anthropic_retry_min_seconds: float = Field(default=1.0, gt=0.0)
    anthropic_retry_max_seconds: float = Field(default=10.0, gt=0.0)

    @field_validator("anthropic_api_key", mode="before")
    @classmethod
    def _empty_str_to_none(cls, v: str | None) -> str | None:
        # compose.yaml may pass `${ANTHROPIC_API_KEY:-}` which substitutes
        # an empty string when the host doesn't have one set. Treat that
        # as absent so `if settings.enrichment.anthropic_api_key:` works.
        if isinstance(v, str) and not v.strip():
            return None
        return v
