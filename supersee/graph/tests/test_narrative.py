"""Tests for the Narrative schema, the canned generator, and the factory.

The Anthropic client isn't tested here — that requires a real API key
and is exercised via manual integration tests.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from supersee.config.enrichment import EnrichmentSettings
from supersee.config.runtime import RuntimeSettings
from supersee.graph.narrative import (
    AnthropicNarrativeClient,
    MockNarrativeClient,
    Narrative,
    NarrativeContext,
    NarrativeEvidenceItem,
    _canned_narrative,
    _format_user_prompt,
    get_narrative_client,
)


def _ctx(**overrides: Any) -> NarrativeContext:
    defaults: dict[str, Any] = dict(
        case_id="case-1",
        account_src="rSrc",
        account_dst="rDst",
        amount_xrp=100.0,
        memo_decoded=None,
        risk_band="low",
        rule_hits=[],
        history_summary={},
        enrichment={},
    )
    defaults.update(overrides)
    return NarrativeContext(**defaults)


# ===========================================================================
# Pydantic schema validation
# ===========================================================================

class TestNarrativeSchema:
    def test_valid_narrative_constructs(self) -> None:
        n = Narrative(
            summary="ok",
            evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
            risk_factors=["routine"],
            confidence=0.5,
            recommended_action="monitor",
        )
        assert n.confidence == 0.5

    def test_rejects_too_long_summary(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x" * 601,
                evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
                risk_factors=["routine"],
                confidence=0.5,
                recommended_action="monitor",
            )

    def test_rejects_empty_evidence(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x",
                evidence=[],
                risk_factors=["routine"],
                confidence=0.5,
                recommended_action="monitor",
            )

    def test_rejects_too_many_evidence_items(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x",
                evidence=[
                    NarrativeEvidenceItem(fact=f"f{i}", source="scorer")
                    for i in range(11)
                ],
                risk_factors=["routine"],
                confidence=0.5,
                recommended_action="monitor",
            )

    def test_rejects_out_of_range_confidence(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x",
                evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
                risk_factors=["routine"],
                confidence=1.5,
                recommended_action="monitor",
            )

    def test_rejects_unknown_recommended_action(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x",
                evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
                risk_factors=["routine"],
                confidence=0.5,
                recommended_action="ignore",  # type: ignore[arg-type]
            )

    def test_rejects_unknown_evidence_source(self) -> None:
        with pytest.raises(ValidationError):
            Narrative(
                summary="x",
                evidence=[NarrativeEvidenceItem(fact="x", source="blockchain")],  # type: ignore[arg-type]
                risk_factors=["routine"],
                confidence=0.5,
                recommended_action="monitor",
            )


# ===========================================================================
# Canned generator (MockNarrativeClient backend)
# ===========================================================================

class TestCannedNarrative:
    def test_ofac_hit_escalates_with_high_confidence(self) -> None:
        ctx = _ctx(
            risk_band="high",
            rule_hits=[
                {"rule": "ofac_sdn_match", "score": 0.5,
                 "rationale": "destination rXXX matches OFAC SDN"}
            ],
        )
        n = _canned_narrative(ctx)
        assert n.recommended_action == "escalate"
        assert n.confidence >= 0.9
        assert "sanctions" in n.risk_factors

    def test_mixer_hit_routes_to_analyst(self) -> None:
        ctx = _ctx(
            risk_band="medium",
            rule_hits=[
                {"rule": "mixer_direct", "score": 0.3,
                 "rationale": "destination rMixer in mixer addresses"}
            ],
        )
        n = _canned_narrative(ctx)
        assert n.recommended_action == "request_analyst_review"
        assert "mixer-proximity" in n.risk_factors

    def test_no_rules_low_band_recommends_no_action(self) -> None:
        ctx = _ctx(risk_band="low", rule_hits=[])
        n = _canned_narrative(ctx)
        assert n.recommended_action == "no_action"
        assert n.confidence < 0.5

    def test_evidence_falls_back_when_no_rationale(self) -> None:
        ctx = _ctx(rule_hits=[{"rule": "memo_anomaly", "score": 0.1, "rationale": ""}])
        n = _canned_narrative(ctx)
        # The rule's rationale was empty so we fall back to baseline evidence.
        assert len(n.evidence) >= 1

    def test_summary_truncated_to_600_chars(self) -> None:
        # Construct a context whose generated summary would be long.
        ctx = _ctx(
            risk_band="high",
            account_src="r" + "x" * 1000,
            account_dst="r" + "y" * 1000,
            rule_hits=[
                {"rule": "ofac_sdn_match", "score": 0.5, "rationale": "z" * 200}
            ],
        )
        n = _canned_narrative(ctx)
        assert len(n.summary) <= 600

    def test_risk_factors_capped_at_eight(self) -> None:
        rule_hits = [
            {"rule": r, "score": 0.1, "rationale": "x"}
            for r in [
                "large_payment", "velocity_burst", "new_counterparty",
                "memo_anomaly", "watchlist_hit", "ofac_sdn_match",
                "mixer_direct", "large_payment", "velocity_burst",
            ]
        ]
        ctx = _ctx(rule_hits=rule_hits)
        n = _canned_narrative(ctx)
        assert len(n.risk_factors) <= 8


# ===========================================================================
# MockNarrativeClient async behavior
# ===========================================================================

class TestMockNarrativeClient:
    async def test_generate_returns_canned(self) -> None:
        ctx = _ctx(
            risk_band="high",
            rule_hits=[{"rule": "ofac_sdn_match", "score": 0.5,
                         "rationale": "OFAC match"}],
        )
        n = await MockNarrativeClient().generate(ctx)
        assert n.recommended_action == "escalate"


# ===========================================================================
# Factory selection
# ===========================================================================

class TestGetNarrativeClient:
    def test_returns_mock_when_mock_llm_true(self) -> None:
        rt = RuntimeSettings(database_url="postgresql://x", mock_llm=True)
        en = EnrichmentSettings(anthropic_api_key="sk-real")
        client = get_narrative_client(rt, en)
        assert isinstance(client, MockNarrativeClient)

    def test_returns_mock_when_no_api_key(self) -> None:
        rt = RuntimeSettings(database_url="postgresql://x", mock_llm=False)
        en = EnrichmentSettings(anthropic_api_key=None)
        client = get_narrative_client(rt, en)
        assert isinstance(client, MockNarrativeClient)

    def test_returns_anthropic_when_real_mode(self) -> None:
        rt = RuntimeSettings(database_url="postgresql://x", mock_llm=False)
        en = EnrichmentSettings(anthropic_api_key="sk-test")
        client = get_narrative_client(rt, en)
        assert isinstance(client, AnthropicNarrativeClient)


# ===========================================================================
# Prompt formatter
# ===========================================================================

class TestFormatUserPrompt:
    def test_includes_all_sections(self) -> None:
        ctx = _ctx(
            risk_band="medium",
            rule_hits=[{"rule": "memo_anomaly", "score": 0.1, "rationale": "tornado"}],
            history_summary={"total_seen": 50},
            enrichment={"ofac_matches": []},
        )
        out = _format_user_prompt(ctx)
        assert "Risk band: medium" in out
        assert "memo_anomaly" in out
        assert "total_seen: 50" in out
        assert "Produce a Narrative." in out

    def test_handles_empty_context_blocks(self) -> None:
        ctx = _ctx(rule_hits=[], history_summary={}, enrichment={})
        out = _format_user_prompt(ctx)
        assert "(none)" in out
