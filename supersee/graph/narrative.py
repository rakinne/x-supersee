"""LLM narrative contract: pydantic schema + two backends (Mock, Anthropic).

The Narrative schema is the load-bearing LLM contract for the project.
Both backends (canned for demo/CI, real for production) must produce
narratives that pass this schema. `build_narrative` (in nodes.py) reads
that guarantee and routes to triage based on the structured output.

Backends:

    MockNarrativeClient
        Deterministic. Generates a plausible narrative from rule_hits +
        history_summary using a small dispatch table. Used for tests,
        for --mock-llm runs, and as the fallback when no Anthropic API
        key is configured.

    AnthropicNarrativeClient
        Real Anthropic via langchain-anthropic with tool-calling
        structured output. Transport-error retries via tenacity
        (3 attempts with exponential backoff). Schema-validation
        failures are NOT retried here — that's the node's responsibility
        (one validation retry, then canned fallback).

`get_narrative_client(settings)` is the factory the lifespan calls.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from supersee.config import EnrichmentSettings, RuntimeSettings

logger = logging.getLogger(__name__)


# ===========================================================================
# Pydantic schema (the LLM output contract)
# ===========================================================================

EvidenceSource = Literal["scorer", "enrichment", "history", "ofac", "watchlist", "mixer"]
RecommendedAction = Literal["no_action", "monitor", "request_analyst_review", "escalate"]


class NarrativeEvidenceItem(BaseModel):
    fact: str = Field(
        ...,
        description="One concrete observation backed by enrichment or rule data.",
    )
    source: EvidenceSource


class Narrative(BaseModel):
    summary: str = Field(..., max_length=600)
    evidence: list[NarrativeEvidenceItem] = Field(..., min_length=1, max_length=10)
    risk_factors: list[str] = Field(..., max_length=8)
    confidence: float = Field(..., ge=0.0, le=1.0)
    recommended_action: RecommendedAction


# ===========================================================================
# NarrativeContext: input shape the clients work from
# ===========================================================================

@dataclass(frozen=True)
class NarrativeContext:
    """All inputs build_narrative will hand to a NarrativeClient."""

    case_id: str
    account_src: str
    account_dst: str | None
    amount_xrp: float | None
    memo_decoded: str | None
    risk_band: str
    rule_hits: list[dict[str, Any]]          # JSONB-shaped: [{rule, score, rationale}, ...]
    history_summary: dict[str, Any]          # whatever fetch_context wrote
    enrichment: dict[str, Any]               # whatever enrich_external wrote


# ===========================================================================
# Client protocol
# ===========================================================================

class NarrativeClient(Protocol):
    """The interface build_narrative depends on."""

    async def generate(self, ctx: NarrativeContext) -> Narrative: ...


# ===========================================================================
# Mock backend: deterministic, no LLM, used for CI and demos without a key
# ===========================================================================

# Each fired rule maps to a short risk-factor label that's safe to show
# in the UI. The mock client uses this to populate Narrative.risk_factors.
_RULE_TO_FACTOR: dict[str, str] = {
    "large_payment": "high-value",
    "velocity_burst": "burst-activity",
    "new_counterparty": "new-counterparty",
    "memo_anomaly": "suspicious-memo",
    "watchlist_hit": "watchlist",
    "ofac_sdn_match": "sanctions",
    "mixer_direct": "mixer-proximity",
}

# Each rule's most appropriate evidence source for Narrative.evidence.
_RULE_TO_SOURCE: dict[str, EvidenceSource] = {
    "large_payment": "scorer",
    "velocity_burst": "history",
    "new_counterparty": "history",
    "memo_anomaly": "scorer",
    "watchlist_hit": "watchlist",
    "ofac_sdn_match": "ofac",
    "mixer_direct": "mixer",
}


def _short_addr(addr: str | None) -> str:
    if not addr:
        return "n/a"
    return f"{addr[:8]}...{addr[-4:]}" if len(addr) > 16 else addr


def _canned_narrative(ctx: NarrativeContext) -> Narrative:
    """Generate a deterministic Narrative from ctx without calling any LLM.

    The dispatch table below picks `recommended_action` + `confidence`
    based on what the scorer flagged. This is plausible enough to demo
    the agentic flow end-to-end without an API key, and it's pinned
    behavior for tests.
    """
    fired: list[str] = [
        str(h.get("rule"))
        for h in ctx.rule_hits
        if h.get("score", 0) > 0 and h.get("rule")
    ]
    fired_set: set[str] = set(fired)

    # Recommendation logic (mirrors the design's truth-table spirit).
    if "ofac_sdn_match" in fired_set:
        action: RecommendedAction = "escalate"
        confidence = 0.95
        summary = (
            f"Source {_short_addr(ctx.account_src)} -> destination "
            f"{_short_addr(ctx.account_dst)} matches an OFAC SDN crypto address. "
            "Immediate escalation required."
        )
    elif "mixer_direct" in fired_set:
        action = "request_analyst_review"
        confidence = 0.85
        summary = (
            f"Destination {_short_addr(ctx.account_dst)} is a known mixer. "
            "Recommend analyst review before allowing."
        )
    elif "watchlist_hit" in fired_set:
        action = "request_analyst_review"
        confidence = 0.75
        summary = (
            f"Address pair touches the curated watchlist on a "
            f"{ctx.amount_xrp or 'non-XRP'} XRP payment. "
            "Recommend analyst review."
        )
    elif ctx.risk_band == "medium":
        action = "request_analyst_review"
        confidence = 0.6
        summary = (
            f"Medium-band signals fired ({', '.join(sorted(fired_set)) or 'none'}). "
            "Recommend analyst review."
        )
    else:
        action = "no_action"
        confidence = 0.3
        summary = "Low-band signals; auto-close recommended."

    # Evidence: one item per firing rule, with a fallback if nothing fired.
    evidence: list[NarrativeEvidenceItem] = []
    for hit in ctx.rule_hits[:10]:
        rationale = str(hit.get("rationale") or "").strip()
        if not rationale:
            continue
        evidence.append(
            NarrativeEvidenceItem(
                fact=rationale,
                source=_RULE_TO_SOURCE.get(str(hit.get("rule") or ""), "scorer"),
            )
        )
    if not evidence:
        evidence = [
            NarrativeEvidenceItem(
                fact="No rules fired; low-confidence baseline.",
                source="scorer",
            )
        ]

    risk_factors = [
        _RULE_TO_FACTOR[r] for r in fired if r in _RULE_TO_FACTOR
    ][:8]
    if not risk_factors:
        risk_factors = ["routine"]

    return Narrative(
        summary=summary[:600],
        evidence=evidence,
        risk_factors=risk_factors,
        confidence=confidence,
        recommended_action=action,
    )


class MockNarrativeClient:
    """Canned-narrative backend. Deterministic; no I/O; suitable for tests/CI."""

    async def generate(self, ctx: NarrativeContext) -> Narrative:
        return _canned_narrative(ctx)


class StaticNarrativeClient:
    """Test helper: returns a single hardcoded Narrative regardless of input."""

    def __init__(self, narrative: Narrative) -> None:
        self._narrative = narrative

    async def generate(self, ctx: NarrativeContext) -> Narrative:
        return self._narrative


class FailingNarrativeClient:
    """Test helper: raises every time (drives the canned-fallback path)."""

    def __init__(self, exc_factory: Callable[[], Exception] | None = None) -> None:
        self._exc_factory = exc_factory or (lambda: RuntimeError("llm boom"))

    async def generate(self, ctx: NarrativeContext) -> Narrative:
        raise self._exc_factory()


# ===========================================================================
# Anthropic backend: real LLM via langchain-anthropic + tool-calling
# ===========================================================================

# System prompt: defines the agent's role + the output schema constraints
# in natural language. langchain-anthropic.with_structured_output also
# enforces the schema via tool-calling, so this prompt is belt-and-suspenders.
_SYSTEM_PROMPT = """You are a compliance analyst reviewing a suspicious cryptocurrency transaction in real-time at an asset-management firm.

A deterministic rules engine has already pre-scored the event and flagged specific risk indicators. Your job is to produce a structured narrative that:
- Summarizes the risk concisely (1-2 sentences, max 600 chars).
- Cites 1-10 concrete evidence items, each tagged with its source.
- Lists up to 8 short risk-factor labels.
- Reports your confidence that the recommended action is correct (0.0 to 1.0).
- Picks one recommended action.

Recommended actions:
- no_action: The event is benign; the scorer's flags don't constitute real risk.
- monitor: Add to a watch queue; no immediate action required.
- request_analyst_review: A human analyst should examine this case.
- escalate: Treat as confirmed risk; route to senior analyst / VP review.

Be concise and specific. Cite amounts, addresses (truncated is fine), rule names, history aggregates. Do not pad the summary with corporate language."""


def _format_user_prompt(ctx: NarrativeContext) -> str:
    """Render NarrativeContext into a single user-prompt string."""
    rule_lines = "\n".join(
        f"  - {h.get('rule')}: score={h.get('score')}, {h.get('rationale')}"
        for h in ctx.rule_hits
    ) or "  (none)"
    enrichment_lines = (
        "\n".join(f"  {k}: {v}" for k, v in (ctx.enrichment or {}).items())
        or "  (none)"
    )
    history_lines = (
        "\n".join(f"  {k}: {v}" for k, v in (ctx.history_summary or {}).items())
        or "  (none)"
    )

    return (
        f"Case: {ctx.case_id}\n"
        f"Event:\n"
        f"  source: {ctx.account_src}\n"
        f"  destination: {ctx.account_dst}\n"
        f"  amount_xrp: {ctx.amount_xrp}\n"
        f"  memo: {ctx.memo_decoded}\n"
        f"\nRisk band: {ctx.risk_band}\n"
        f"\nRule hits:\n{rule_lines}\n"
        f"\nAccount history summary:\n{history_lines}\n"
        f"\nEnrichment:\n{enrichment_lines}\n"
        f"\nProduce a Narrative."
    )


class AnthropicNarrativeClient:
    """Real Anthropic backend. Uses tool-calling for structured output.

    Transport-error retries (timeouts, 5xx, rate limits) via tenacity.
    Validation failures are caller's responsibility: the node retries
    once with the error fed back, then falls back to canned.
    """

    def __init__(self, enrichment: EnrichmentSettings, runtime: RuntimeSettings):
        # Import locally so unit tests that don't touch the Anthropic
        # backend don't pay the langchain import cost.
        from langchain_anthropic import ChatAnthropic  # noqa: PLC0415
        from pydantic import SecretStr  # noqa: PLC0415

        self._enrichment = enrichment
        self._runtime = runtime
        # langchain-anthropic 0.3.x uses `model_name=` and accepts SecretStr
        # for the api_key. The kwargs below match those expectations; if
        # you upgrade langchain, double-check this constructor signature.
        api_key = enrichment.anthropic_api_key
        if not api_key:
            raise RuntimeError(
                "AnthropicNarrativeClient requires ANTHROPIC_API_KEY; "
                "use get_narrative_client() to pick the right backend."
            )
        self._llm = ChatAnthropic(
            model_name=enrichment.anthropic_model,
            api_key=SecretStr(api_key),
            timeout=60.0,
            stop=None,
        ).with_structured_output(Narrative, method="function_calling")

    async def generate(self, ctx: NarrativeContext) -> Narrative:
        from anthropic import (  # noqa: PLC0415
            APIConnectionError,
            APITimeoutError,
            RateLimitError,
        )
        from langchain_core.messages import (  # noqa: PLC0415
            HumanMessage,
            SystemMessage,
        )

        e = self._enrichment

        @retry(
            stop=stop_after_attempt(e.anthropic_max_retries),
            wait=wait_exponential(
                min=e.anthropic_retry_min_seconds,
                max=e.anthropic_retry_max_seconds,
            ),
            retry=retry_if_exception_type(
                (APIConnectionError, APITimeoutError, RateLimitError)
            ),
            reraise=True,
        )
        async def _call() -> Narrative:
            msgs = [
                SystemMessage(content=_SYSTEM_PROMPT),
                HumanMessage(content=_format_user_prompt(ctx)),
            ]
            result = await self._llm.ainvoke(msgs)
            # with_structured_output(method="function_calling") returns the
            # pydantic model directly when the schema is a BaseModel subclass.
            if isinstance(result, Narrative):
                return result
            # Defensive: some langchain versions return a dict that needs
            # pydantic validation; accept both shapes.
            return Narrative.model_validate(result)

        return await _call()


# ===========================================================================
# Factory
# ===========================================================================

def get_narrative_client(
    runtime: RuntimeSettings,
    enrichment: EnrichmentSettings,
) -> NarrativeClient:
    """Pick the LLM backend based on runtime config.

    Returns MockNarrativeClient when SUPERSEE_MOCK_LLM=true OR no
    ANTHROPIC_API_KEY is configured. Otherwise returns the real
    Anthropic client. The mock path is the default for CI and for
    reviewers who don't want to provision a key.
    """
    if runtime.mock_llm or not enrichment.anthropic_api_key:
        logger.info("narrative client: mock (mock_llm=%s, key_set=%s)",
                    runtime.mock_llm, bool(enrichment.anthropic_api_key))
        return MockNarrativeClient()
    logger.info("narrative client: anthropic (%s)", enrichment.anthropic_model)
    return AnthropicNarrativeClient(enrichment, runtime)
