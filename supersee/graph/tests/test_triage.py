"""Parametric truth-table tests for the triage dispatcher.

Mirrors the table from the design doc + the truth table comment in
`supersee.graph.triage`. Every row exists as a separate assertion.
"""

from __future__ import annotations

import pytest

from supersee.graph.triage import decide


class TestTriageDecision:
    @pytest.mark.parametrize(
        "risk_band, action, confidence, exp_path, exp_status",
        [
            # high band variants (confidence >= 0.5)
            ("high", "escalate",               0.95, "hitl_pause", "escalated"),
            ("high", "request_analyst_review", 0.8,  "hitl_pause", "pending_hitl"),
            ("high", "monitor",                0.7,  "hitl_pause", "pending_hitl"),
            ("high", "no_action",              0.6,  "hitl_pause", "pending_hitl"),

            # medium band variants (confidence >= 0.5)
            ("medium", "request_analyst_review", 0.6, "hitl_pause", "pending_hitl"),
            ("medium", "monitor",                0.6, "hitl_pause", "pending_hitl"),
            ("medium", "escalate",               0.6, "hitl_pause", "pending_hitl"),
            ("medium", "no_action",              0.9, "auto_close", "auto_closed"),

            # low-confidence override: any combination routes to analyst
            ("high",   "escalate",  0.49, "hitl_pause", "pending_hitl"),
            ("medium", "no_action", 0.49, "hitl_pause", "pending_hitl"),
            ("low",    "no_action", 0.1,  "hitl_pause", "pending_hitl"),

            # unknown band falls through to safe default
            ("???",    "no_action", 0.99, "hitl_pause", "pending_hitl"),
        ],
    )
    def test_truth_table(
        self,
        risk_band: str,
        action: str,
        confidence: float,
        exp_path: str,
        exp_status: str,
    ) -> None:
        d = decide(risk_band, action, confidence)
        assert d.triage_path == exp_path, f"{risk_band}/{action}/{confidence}"
        assert d.case_status == exp_status, f"{risk_band}/{action}/{confidence}"
        assert isinstance(d.rationale, str) and d.rationale

    def test_high_escalate_explains_vp_review_in_rationale(self) -> None:
        d = decide("high", "escalate", 0.95)
        assert "VP review" in d.rationale

    def test_low_confidence_rationale_calls_out_confidence(self) -> None:
        d = decide("high", "escalate", 0.3)
        assert "low confidence" in d.rationale.lower()
