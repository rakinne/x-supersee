"""Unit tests for the deterministic scoring rules.

The rules are pure functions; tests use a fresh `ScorerSettings()`
instance so each test reads the same defaults regardless of env vars.
No database or network is touched.
"""

from __future__ import annotations

import pytest

from supersee.config.scorer import ScorerSettings
from supersee.scorer import rules


@pytest.fixture
def cfg() -> ScorerSettings:
    return ScorerSettings()


# ===========================================================================
# large_payment
# ===========================================================================

class TestLargePayment:
    @pytest.mark.parametrize(
        "amount, p99, should_fire, in_rationale",
        [
            # Floor binds when P99 is absent
            (75_000.0, None, True, "floor"),
            # Boundary: amount == floor fires (>= semantics)
            (50_000.0, None, True, "floor"),
            # Below floor, no P99 → no fire
            (49_999.0, None, False, "below threshold"),
            # P99 > floor → P99 binds
            (75_000.0, 60_000.0, True, "P99"),
            # Amount > floor but < P99 → no fire (P99 binds)
            (55_000.0, 60_000.0, False, "below threshold"),
            # P99 below floor → floor still binds
            (60_000.0, 40_000.0, True, "floor"),
            # Non-XRP tx → no fire regardless of P99
            (None, None, False, "non-XRP"),
            (None, 100_000.0, False, "non-XRP"),
        ],
    )
    def test_large_payment(
        self,
        cfg: ScorerSettings,
        amount: float | None,
        p99: float | None,
        should_fire: bool,
        in_rationale: str,
    ) -> None:
        score, rationale = rules.large_payment(amount, p99, cfg)
        if should_fire:
            assert score == cfg.weights.large_payment
        else:
            assert score == 0.0
        assert in_rationale in rationale


# ===========================================================================
# velocity_burst
# ===========================================================================

class TestVelocityBurst:
    @pytest.mark.parametrize(
        "count, should_fire",
        [
            (10, True),   # boundary fires (>= semantics)
            (12, True),
            (50, True),
            (9, False),
            (1, False),
            (0, False),
        ],
    )
    def test_velocity_burst(
        self,
        cfg: ScorerSettings,
        count: int,
        should_fire: bool,
    ) -> None:
        score, rationale = rules.velocity_burst(count, cfg)
        if should_fire:
            assert score == cfg.weights.velocity_burst
            assert ">= threshold" in rationale
        else:
            assert score == 0.0
            assert "< threshold" in rationale
        assert str(count) in rationale


# ===========================================================================
# new_counterparty
# ===========================================================================

class TestNewCounterparty:
    KNOWN = frozenset({"rKnown1", "rKnown2"})

    def test_fires_for_new_dst_above_threshold(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.new_counterparty(7_500.0, "rNew", self.KNOWN, cfg)
        assert score == cfg.weights.new_counterparty
        assert "rNew" in rationale
        assert ">=" in rationale

    def test_fires_at_exact_threshold(self, cfg: ScorerSettings) -> None:
        score, _ = rules.new_counterparty(
            cfg.thresholds.new_counterparty_min_xrp, "rNew", self.KNOWN, cfg
        )
        assert score == cfg.weights.new_counterparty

    def test_no_fire_for_known_dst(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.new_counterparty(7_500.0, "rKnown1", self.KNOWN, cfg)
        assert score == 0.0
        assert "previously seen" in rationale

    def test_no_fire_when_below_threshold(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.new_counterparty(100.0, "rNew", self.KNOWN, cfg)
        assert score == 0.0
        assert "<" in rationale

    def test_no_fire_when_no_destination(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.new_counterparty(7_500.0, None, self.KNOWN, cfg)
        assert score == 0.0
        assert "no destination" in rationale

    def test_no_fire_when_non_xrp(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.new_counterparty(None, "rNew", self.KNOWN, cfg)
        assert score == 0.0
        assert "non-XRP" in rationale


# ===========================================================================
# memo_anomaly
# ===========================================================================

class TestMemoAnomaly:
    @pytest.mark.parametrize(
        "memo, should_fire",
        [
            ("Sending to Tornado Cash account", True),
            ("MIXER service deposit", True),
            ("wasabi wallet", True),
            ("OFAC evasion test", True),
            # Long hex blob triggers the ^[a-f0-9]{64,}$ catch-all
            ("a" * 70, True),
            ("normal invoice memo 12345", False),
            ("", False),
            (None, False),
        ],
    )
    def test_memo_anomaly(
        self,
        cfg: ScorerSettings,
        memo: str | None,
        should_fire: bool,
    ) -> None:
        score, rationale = rules.memo_anomaly(memo, cfg)
        if should_fire:
            assert score == cfg.weights.memo_anomaly
            assert "matches pattern" in rationale
        else:
            assert score == 0.0

    def test_case_insensitive_match(self, cfg: ScorerSettings) -> None:
        # Patterns use (?i); both casings should fire.
        score_upper, _ = rules.memo_anomaly("TORNADO CASH", cfg)
        score_lower, _ = rules.memo_anomaly("tornado cash", cfg)
        assert score_upper == score_lower == cfg.weights.memo_anomaly


# ===========================================================================
# watchlist_hit
# ===========================================================================

class TestWatchlistHit:
    WL = frozenset({"rW1", "rW2"})

    def test_source_in_watchlist(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.watchlist_hit("rW1", "rOther", self.WL, cfg)
        assert score == cfg.weights.watchlist_hit
        assert "source rW1" in rationale

    def test_destination_in_watchlist(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.watchlist_hit("rOther", "rW2", self.WL, cfg)
        assert score == cfg.weights.watchlist_hit
        assert "destination rW2" in rationale

    def test_both_in_watchlist(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.watchlist_hit("rW1", "rW2", self.WL, cfg)
        assert score == cfg.weights.watchlist_hit
        assert "both" in rationale

    def test_neither_in_watchlist(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.watchlist_hit("rA", "rB", self.WL, cfg)
        assert score == 0.0
        assert "no watchlist match" in rationale

    def test_no_destination(self, cfg: ScorerSettings) -> None:
        score, _ = rules.watchlist_hit("rA", None, self.WL, cfg)
        assert score == 0.0

    def test_no_destination_but_source_match(self, cfg: ScorerSettings) -> None:
        # Source-only hit must still fire even without a destination.
        score, rationale = rules.watchlist_hit("rW1", None, self.WL, cfg)
        assert score == cfg.weights.watchlist_hit
        assert "source rW1" in rationale


# ===========================================================================
# ofac_sdn_match
# ===========================================================================

class TestOfacSdnMatch:
    OFAC = frozenset({"rSDN1", "rSDN2"})

    def test_source_match(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.ofac_sdn_match("rSDN1", "rOther", self.OFAC, cfg)
        assert score == cfg.weights.ofac_sdn_match
        assert "source rSDN1" in rationale

    def test_destination_match(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.ofac_sdn_match("rOther", "rSDN2", self.OFAC, cfg)
        assert score == cfg.weights.ofac_sdn_match
        assert "destination rSDN2" in rationale

    def test_both_match(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.ofac_sdn_match("rSDN1", "rSDN2", self.OFAC, cfg)
        assert score == cfg.weights.ofac_sdn_match
        assert "both" in rationale

    def test_no_match(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.ofac_sdn_match("rA", "rB", self.OFAC, cfg)
        assert score == 0.0
        assert "no OFAC SDN match" in rationale

    def test_no_destination(self, cfg: ScorerSettings) -> None:
        score, _ = rules.ofac_sdn_match("rOther", None, self.OFAC, cfg)
        assert score == 0.0

    def test_calibration_single_hit_clears_high_band(
        self, cfg: ScorerSettings
    ) -> None:
        """A single OFAC hit must clear the high-band threshold on its own.

        This is the calibration invariant from the design doc: OFAC's
        weight is set so that a lone OFAC match routes straight to
        escalation regardless of what else fired.
        """
        score, _ = rules.ofac_sdn_match("rSDN1", None, self.OFAC, cfg)
        assert score >= cfg.bands.medium_max, (
            f"OFAC weight {score} must be >= medium_max {cfg.bands.medium_max} "
            f"so a single hit lands in the high band"
        )


# ===========================================================================
# mixer_direct
# ===========================================================================

class TestMixerDirect:
    MIX = frozenset({"rMixer1", "rMixer2"})

    def test_fires_on_direct_match(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.mixer_direct("rMixer1", self.MIX, cfg)
        assert score == cfg.weights.mixer_direct
        assert "rMixer1" in rationale

    def test_no_fire_on_miss(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.mixer_direct("rOther", self.MIX, cfg)
        assert score == 0.0
        assert "not in mixer" in rationale

    def test_no_fire_when_dst_none(self, cfg: ScorerSettings) -> None:
        score, rationale = rules.mixer_direct(None, self.MIX, cfg)
        assert score == 0.0
        assert "no destination" in rationale

    def test_empty_mixer_set(self, cfg: ScorerSettings) -> None:
        # Cold start: no curated mixers yet → never fires.
        score, _ = rules.mixer_direct("rAny", frozenset(), cfg)
        assert score == 0.0
