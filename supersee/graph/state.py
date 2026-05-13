"""The InvestigationState contract.

Every LangGraph node reads from this state and returns a partial dict
that gets merged in. The `audit_log` field uses the `add` reducer so
each node can append events without clobbering the previous ones.

`total=False` because LangGraph state updates are merge-style: any node
may set any subset of keys, and missing keys are NOT errors. The full
TypedDict is the union of what all nodes might write.

`AuditLogEntry` is the unit of trace: one per node invocation (and
sometimes more if a node retries internally). It's the source of truth
for "what did the agent do, in what order, with what evidence" — which
is the load-bearing artifact for an enterprise-style compliance demo.
"""

from __future__ import annotations

from operator import add
from typing import Annotated, Any, Literal, TypedDict

TriagePath = Literal["auto_close", "hitl_pause", "escalate"]


class AuditLogEntry(TypedDict, total=False):
    """One row in the per-investigation audit trail.

    Nodes append to `state["audit_log"]` (concatenated via the `add`
    reducer). At record_outcome time the whole list is serialized into
    `case_artifacts` so a reviewer can replay the agent's reasoning
    step-by-step.
    """

    node: str               # which node emitted this row
    at: str                 # ISO 8601 UTC, from clock.now()
    event: str              # short event tag (e.g., "fetch_context.ok")
    detail: dict[str, Any]            # node-specific payload
    attempt: int            # which attempt within the node (1-indexed)


class InvestigationState(TypedDict, total=False):
    """The full state contract across the six-node graph.

    Lifecycle of the keys:

    Set by the caller (the future scorer service, or the investigate CLI):
        case_id, event_id, account_src, account_dst, amount_xrp,
        memo_decoded, risk_band, rule_hits

    Set by fetch_context:
        history_summary

    Set by enrich_external:
        enrichment, enrichment_attempts

    Set by build_narrative:
        narrative, narrative_attempts

    Set by triage_branch:
        triage_path

    Set by hitl_pause (from Command(resume=...)):
        analyst_decision

    Set by record_outcome:
        final_status

    Appended by every node:
        audit_log
    """

    # Identity (caller-supplied)
    case_id: str
    event_id: int

    # Event payload (caller-supplied)
    account_src: str
    account_dst: str | None
    amount_xrp: float | None
    memo_decoded: str | None
    risk_band: str          # "low" | "medium" | "high"
    rule_hits: list[dict[str, Any]]

    # Per-node outputs
    history_summary: dict[str, Any] | None
    enrichment: dict[str, Any] | None
    enrichment_attempts: int
    narrative: dict[str, Any] | None
    narrative_attempts: int
    triage_path: TriagePath | None
    case_status_target: str | None     # written by triage_branch, read by hitl_pause
    analyst_decision: dict[str, Any] | None
    final_status: str       # "approved" | "rejected" | "escalated" | "auto_closed" | "errored"

    # Cross-node accumulator (reducer: concatenation)
    audit_log: Annotated[list[AuditLogEntry], add]
