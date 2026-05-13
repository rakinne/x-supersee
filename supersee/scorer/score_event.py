"""Risk-score aggregator.

Takes a normalized event plus precomputed scoring context and runs all
seven rules from `supersee.scorer.rules`. Returns the weighted-sum risk
score (clipped to [0, 1]), the risk band, and the list of firing rule
hits ready to serialize into `cases.rule_hits`.

Non-firing rule rationales are emitted at DEBUG level and not persisted;
firing rules end up in `cases.rule_hits` as JSON for the LLM and analyst.

No I/O happens here. Fetching `ScoringContext` (account_history row,
OFAC/watchlist/mixer sets, recent_outbound_count) is the caller's job
and lives in a sibling module that will land with the DB-backed
history upsert.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from supersee.config.scorer import RiskBands, ScorerSettings
from supersee.scorer import rules

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventData:
    """The fields of a normalized event any rule might read."""

    amount_xrp: float | None
    account_src: str
    account_dst: str | None
    memo_decoded: str | None


@dataclass(frozen=True)
class ScoringContext:
    """Precomputed lookup state the aggregator needs to score one event."""

    rolling_p99_7d: float | None
    known_counterparties: frozenset[str]
    recent_outbound_count: int
    ofac_addresses: frozenset[str]
    watchlist_addresses: frozenset[str]
    mixer_addresses: frozenset[str]


@dataclass(frozen=True)
class RuleHit:
    rule: str
    score: float
    rationale: str


@dataclass(frozen=True)
class ScoringResult:
    risk_score: float
    risk_band: str  # "low" | "medium" | "high"
    rule_hits: list[RuleHit]

    def to_jsonb(self) -> list[dict[str, str | float]]:
        """Shape suitable for JSON-encoding into the `cases.rule_hits` column."""
        return [
            {"rule": h.rule, "score": h.score, "rationale": h.rationale}
            for h in self.rule_hits
        ]


def compute_band(score: float, bands: RiskBands) -> str:
    """Map a clipped risk score to its band.

    Boundaries match the design-doc truth table:
        score < low_max         → 'low'    (auto_close)
        score < medium_max      → 'medium' (hitl_pause path)
        score >= medium_max     → 'high'   (escalate flag set)
    """
    if score < bands.low_max:
        return "low"
    if score < bands.medium_max:
        return "medium"
    return "high"


def score_event(
    event: EventData,
    ctx: ScoringContext,
    cfg: ScorerSettings,
) -> ScoringResult:
    """Run all seven rules over the event + context and band the result."""
    # Each entry: (rule_name, score, rationale). Order matches the design's
    # rule list so rule_hits has deterministic ordering for snapshot tests.
    raw_results: list[tuple[str, float, str]] = [
        ("large_payment", *rules.large_payment(event.amount_xrp, ctx.rolling_p99_7d, cfg)),
        ("velocity_burst", *rules.velocity_burst(ctx.recent_outbound_count, cfg)),
        (
            "new_counterparty",
            *rules.new_counterparty(
                event.amount_xrp, event.account_dst, ctx.known_counterparties, cfg
            ),
        ),
        ("memo_anomaly", *rules.memo_anomaly(event.memo_decoded, cfg)),
        (
            "watchlist_hit",
            *rules.watchlist_hit(
                event.account_src, event.account_dst, ctx.watchlist_addresses, cfg
            ),
        ),
        (
            "ofac_sdn_match",
            *rules.ofac_sdn_match(
                event.account_src, event.account_dst, ctx.ofac_addresses, cfg
            ),
        ),
        (
            "mixer_direct",
            *rules.mixer_direct(event.account_dst, ctx.mixer_addresses, cfg),
        ),
    ]

    hits: list[RuleHit] = []
    for name, score, rationale in raw_results:
        if score > 0.0:
            hits.append(RuleHit(rule=name, score=score, rationale=rationale))
        else:
            logger.debug("rule %s no-fire: %s", name, rationale)

    raw_total = sum(hit.score for hit in hits)
    risk_score = min(raw_total, 1.0)
    band = compute_band(risk_score, cfg.bands)

    return ScoringResult(risk_score=risk_score, risk_band=band, rule_hits=hits)
