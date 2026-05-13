"""Unit tests for the risk-score aggregator.

Verifies: weighted-sum + clip semantics, band boundaries (which use `<`
on both edges so equal-to-threshold falls into the upper band), the OFAC
calibration invariant at the aggregator level (lone OFAC hit → high),
rule_hits filtering (non-firing rules excluded), ordering determinism,
and the JSON-serializable shape of `to_jsonb()`.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from supersee.config.scorer import ScorerSettings
from supersee.scorer.score_event import (
    EventData,
    RuleHit,
    ScoringContext,
    ScoringResult,
    compute_band,
    score_event,
)


@pytest.fixture
def cfg() -> ScorerSettings:
    return ScorerSettings()


@pytest.fixture
def base_event() -> EventData:
    """Tiny payment to a known destination; triggers no rules by itself."""
    return EventData(
        amount_xrp=100.0,
        account_src="rSrc",
        account_dst="rKnownDst",
        memo_decoded=None,
    )


@pytest.fixture
def base_ctx() -> ScoringContext:
    """Empty curated sets; destination is in known_counterparties."""
    return ScoringContext(
        rolling_p99_7d=None,
        known_counterparties=frozenset({"rKnownDst"}),
        recent_outbound_count=0,
        ofac_addresses=frozenset(),
        watchlist_addresses=frozenset(),
        mixer_addresses=frozenset(),
    )


# ===========================================================================
# compute_band — direct test of the banding logic
# ===========================================================================

class TestComputeBand:
    @pytest.mark.parametrize(
        "score, expected",
        [
            (0.0, "low"),
            (0.10, "low"),
            (0.19, "low"),
            (0.20, "medium"),   # boundary: >= low_max → medium
            (0.30, "medium"),
            (0.49, "medium"),
            (0.50, "high"),     # boundary: >= medium_max → high
            (0.75, "high"),
            (1.0, "high"),
        ],
    )
    def test_banding(self, cfg: ScorerSettings, score: float, expected: str) -> None:
        assert compute_band(score, cfg.bands) == expected


# ===========================================================================
# score_event — happy path and boundary tests
# ===========================================================================

class TestScoreEvent:
    def test_no_rules_fire_lands_in_low_band(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        result = score_event(base_event, base_ctx, cfg)
        assert result.risk_score == 0.0
        assert result.risk_band == "low"
        assert result.rule_hits == []

    def test_lone_ofac_hit_lands_in_high_band(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        """OFAC calibration at the aggregator level: one hit, escalation."""
        ctx = replace(base_ctx, ofac_addresses=frozenset({"rSrc"}))
        result = score_event(base_event, ctx, cfg)
        assert result.risk_score == cfg.weights.ofac_sdn_match
        assert result.risk_band == "high"
        assert len(result.rule_hits) == 1
        assert result.rule_hits[0].rule == "ofac_sdn_match"

    def test_lone_watchlist_hit_lands_in_medium(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        """watchlist_hit alone = 0.20 = low_max boundary → medium band."""
        ctx = replace(base_ctx, watchlist_addresses=frozenset({"rSrc"}))
        result = score_event(base_event, ctx, cfg)
        assert result.risk_score == cfg.weights.watchlist_hit
        assert result.risk_score == cfg.bands.low_max
        assert result.risk_band == "medium"

    def test_lone_velocity_burst_lands_in_low_band(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        """Documented calibration: velocity weight 0.15 < low_max 0.20.

        A lone burst does not trigger an investigation; it must combine
        with another signal to reach the medium band. If this changes,
        update the design doc and this test together.
        """
        ctx = replace(base_ctx, recent_outbound_count=15)
        result = score_event(base_event, ctx, cfg)
        assert result.risk_score == cfg.weights.velocity_burst
        assert result.risk_band == "low"

    def test_combined_signals_can_reach_medium(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        """memo_anomaly (0.10) + watchlist_hit (0.20) = 0.30 → medium."""
        event = replace(base_event, memo_decoded="tornado cash deposit")
        ctx = replace(base_ctx, watchlist_addresses=frozenset({"rSrc"}))
        result = score_event(event, ctx, cfg)
        expected = cfg.weights.memo_anomaly + cfg.weights.watchlist_hit
        assert result.risk_score == pytest.approx(expected)
        assert result.risk_band == "medium"
        assert {h.rule for h in result.rule_hits} == {"memo_anomaly", "watchlist_hit"}

    def test_score_is_clipped_to_one(self, cfg: ScorerSettings) -> None:
        """Construct an event/ctx that fires all seven rules and verify clipping."""
        event = EventData(
            amount_xrp=100_000.0,
            account_src="rOFAC",
            account_dst="rNewMixer",
            memo_decoded="tornado cash",
        )
        ctx = ScoringContext(
            rolling_p99_7d=None,
            known_counterparties=frozenset(),  # rNewMixer is new
            recent_outbound_count=25,
            ofac_addresses=frozenset({"rOFAC"}),
            watchlist_addresses=frozenset({"rNewMixer"}),
            mixer_addresses=frozenset({"rNewMixer"}),
        )
        result = score_event(event, ctx, cfg)

        raw_sum = sum(cfg.weights.model_dump().values())
        assert raw_sum > 1.0, "test premise: raw weights must sum to > 1.0"
        assert result.risk_score == 1.0
        assert result.risk_band == "high"
        assert len(result.rule_hits) == 7

    def test_rule_hits_exclude_non_firing(
        self,
        cfg: ScorerSettings,
        base_event: EventData,
        base_ctx: ScoringContext,
    ) -> None:
        ctx = replace(base_ctx, watchlist_addresses=frozenset({"rSrc"}))
        result = score_event(base_event, ctx, cfg)
        rule_names = {h.rule for h in result.rule_hits}
        assert rule_names == {"watchlist_hit"}, (
            "only the firing rule should appear in rule_hits"
        )

    def test_rule_hits_preserve_evaluation_order(
        self,
        cfg: ScorerSettings,
    ) -> None:
        """rule_hits ordering follows the rules list, not alphabetical."""
        # Fire large_payment + watchlist_hit + ofac_sdn_match in that order.
        event = EventData(
            amount_xrp=100_000.0,
            account_src="rWatchSrc",
            account_dst="rOFACDst",
            memo_decoded=None,
        )
        ctx = ScoringContext(
            rolling_p99_7d=None,
            known_counterparties=frozenset({"rOFACDst"}),  # suppress new_counterparty
            recent_outbound_count=0,
            ofac_addresses=frozenset({"rOFACDst"}),
            watchlist_addresses=frozenset({"rWatchSrc"}),
            mixer_addresses=frozenset(),
        )
        result = score_event(event, ctx, cfg)
        order = [h.rule for h in result.rule_hits]
        assert order == ["large_payment", "watchlist_hit", "ofac_sdn_match"]


# ===========================================================================
# ScoringResult.to_jsonb — JSON shape contract
# ===========================================================================

class TestToJsonb:
    def test_to_jsonb_shape(self, cfg: ScorerSettings) -> None:
        result = ScoringResult(
            risk_score=0.30,
            risk_band="medium",
            rule_hits=[
                RuleHit(rule="memo_anomaly", score=0.10, rationale="memo matches pattern 'tornado'"),
                RuleHit(rule="watchlist_hit", score=0.20, rationale="source rX in watchlist"),
            ],
        )
        out = result.to_jsonb()
        assert isinstance(out, list)
        for entry in out:
            assert set(entry.keys()) == {"rule", "score", "rationale"}
            assert isinstance(entry["rule"], str)
            assert isinstance(entry["score"], float)
            assert isinstance(entry["rationale"], str)
        assert out[0]["rule"] == "memo_anomaly"
        assert out[1]["score"] == 0.20

    def test_to_jsonb_empty_when_no_hits(self) -> None:
        result = ScoringResult(risk_score=0.0, risk_band="low", rule_hits=[])
        assert result.to_jsonb() == []
