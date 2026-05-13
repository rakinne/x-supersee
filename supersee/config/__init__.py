"""Configuration facade.

`from supersee.config import settings` returns a process-singleton with
three sub-objects loaded once from env vars at import time:

    settings.runtime.database_url               # required (DATABASE_URL)
    settings.scorer.weights.large_payment       # 0.20
    settings.scorer.bands.medium_max            # 0.50
    settings.enrichment.anthropic_model         # 'claude-haiku-4-5'

Each sub-object is its own pydantic-settings model with a `SUPERSEE_<area>_`
env prefix. Industry-convention names (DATABASE_URL, LOG_LEVEL,
ANTHROPIC_API_KEY) keep their bare names via `validation_alias`.

Tests can construct a fresh `Settings(...)` with overrides instead of
relying on the module singleton.
"""

from __future__ import annotations

from supersee.config.enrichment import EnrichmentSettings
from supersee.config.runtime import RuntimeSettings
from supersee.config.scorer import ScorerSettings


class Settings:
    """Composite settings facade. One instance per process under `settings`."""

    def __init__(
        self,
        runtime: RuntimeSettings | None = None,
        scorer: ScorerSettings | None = None,
        enrichment: EnrichmentSettings | None = None,
    ) -> None:
        self.runtime = runtime or RuntimeSettings()
        self.scorer = scorer or ScorerSettings()
        self.enrichment = enrichment or EnrichmentSettings()


settings = Settings()

__all__ = [
    "EnrichmentSettings",
    "RuntimeSettings",
    "ScorerSettings",
    "Settings",
    "settings",
]
