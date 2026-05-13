"""Deterministic dispatch from (band, narrative) to triage_path + case_status.

The truth table mirrors the design doc's spec. It's pure: same inputs
always produce the same outputs. The graph's `triage_branch` node calls
`decide` and writes the path into state for the conditional edge router.

Truth table (from the design doc):

    risk_band   recommended_action          confidence    -> triage_path   case_status
    ----------  --------------------------- ------------  --------------- ---------------
    high        escalate (any)              >= 0.5        hitl_pause      escalated
    high        (anything else)             >= 0.5        hitl_pause      pending_hitl
    medium      request_analyst_review      >= 0.5        hitl_pause      pending_hitl
    medium      monitor                     >= 0.5        hitl_pause      pending_hitl
    medium      no_action                   >= 0.5        auto_close      auto_closed
    (any)       (any)                       <  0.5        hitl_pause      pending_hitl   (LOW CONFIDENCE)

In words:
- High-band cases always reach a human.
- Medium-band cases reach a human unless the LLM is confident the event is benign.
- Low-confidence narratives always reach a human, regardless of band.
- The graph's `record_outcome` writes the case_status into the DB.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from supersee.graph.state import TriagePath

CaseStatus = Literal[
    "auto_closed",
    "pending_hitl",
    "escalated",
]


@dataclass(frozen=True)
class TriageDecision:
    triage_path: TriagePath
    case_status: CaseStatus
    rationale: str  # one-line explanation for the audit_log


def decide(
    risk_band: str,
    recommended_action: str,
    confidence: float,
) -> TriageDecision:
    """Map (band, action, confidence) to the triage_path + case_status pair.

    Default-safe: unknown bands fall through to hitl_pause / pending_hitl
    so we never auto-close a case the graph doesn't fully understand.
    """
    # Low-confidence overrides everything else.
    if confidence < 0.5:
        return TriageDecision(
            triage_path="hitl_pause",
            case_status="pending_hitl",
            rationale=(
                f"low confidence ({confidence:.2f}) — routing to analyst regardless "
                f"of band='{risk_band}' / action='{recommended_action}'"
            ),
        )

    if risk_band == "high":
        if recommended_action == "escalate":
            return TriageDecision(
                triage_path="hitl_pause",
                case_status="escalated",
                rationale="high band + LLM escalation; routing for VP review",
            )
        return TriageDecision(
            triage_path="hitl_pause",
            case_status="pending_hitl",
            rationale=f"high band ({recommended_action}); routing to analyst",
        )

    if risk_band == "medium":
        if recommended_action == "no_action":
            return TriageDecision(
                triage_path="auto_close",
                case_status="auto_closed",
                rationale=(
                    "medium band but LLM judged benign with high confidence; "
                    "auto-closing"
                ),
            )
        return TriageDecision(
            triage_path="hitl_pause",
            case_status="pending_hitl",
            rationale=f"medium band ({recommended_action}); routing to analyst",
        )

    # Unknown band: default to safe (analyst review).
    return TriageDecision(
        triage_path="hitl_pause",
        case_status="pending_hitl",
        rationale=f"unknown risk_band='{risk_band}'; defaulting to analyst review",
    )
