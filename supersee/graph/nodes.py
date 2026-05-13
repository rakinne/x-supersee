"""The six investigation nodes.

Pattern:
- Each node is `async def name(state: InvestigationState) -> dict`.
- Each node reads what it needs from state, does its work, and returns a
  partial state dict (LangGraph merges into the persisted state via the
  checkpointer).
- Every node appends one or more AuditLogEntry to `audit_log`; LangGraph's
  `add` reducer concatenates them across the run.
- Nodes that touch the DB use `db.get_pool()` — the same module-level pool
  the rest of the app shares.
- Nodes that need an LLM client receive it via the closure created in
  `app.build_graph(narrative_client=...)` so build_narrative is testable
  with a Mock backend.

Triage routes to `record_outcome` (auto_close path) or `hitl_pause`
(both human-review paths). `hitl_pause` calls `interrupt()`; the only
way out is `Command(resume=<analyst_decision>)` which the FastAPI
decision endpoint (next commit) will issue.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langgraph.types import interrupt
from psycopg.errors import OperationalError
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from supersee import clock, db
from supersee.graph.narrative import (
    MockNarrativeClient,
    Narrative,
    NarrativeClient,
    NarrativeContext,
)
from supersee.graph.state import AuditLogEntry, InvestigationState
from supersee.graph.triage import TriageDecision, decide

logger = logging.getLogger(__name__)


def _audit(node: str, event: str, detail: dict[str, Any], attempt: int = 1) -> AuditLogEntry:
    """Construct one AuditLogEntry with a tz-aware timestamp."""
    return AuditLogEntry(
        node=node,
        at=clock.now().isoformat(),
        event=event,
        detail=detail,
        attempt=attempt,
    )


# ===========================================================================
# fetch_context
# ===========================================================================

async def fetch_context(state: InvestigationState) -> dict[str, Any]:
    """Pull aggregates that the LLM should see beyond the firing-rule rationale.

    Reads `account_history` for the source account (totals, last_seen,
    counterparty count) and computes a few extra event-count windows
    for narrative color. Writes a `history_summary` dict to state.
    """
    account_src = state["account_src"]
    case_id = state.get("case_id", "?")
    pool = db.get_pool()

    history_summary: dict[str, Any] = {}
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT total_seen, last_seen_at, rolling_p99_7d,
                       rolling_p99_at, array_length(known_counterparties, 1)
                FROM account_history
                WHERE account = %s
                """,
                (account_src,),
            )
            row = await cur.fetchone()
    if row is not None:
        history_summary = {
            "total_seen": int(row[0] or 0),
            "last_seen_at": row[1].isoformat() if row[1] else None,
            "rolling_p99_7d": float(row[2]) if row[2] is not None else None,
            "rolling_p99_at": row[3].isoformat() if row[3] else None,
            "known_counterparties_count": int(row[4] or 0),
        }
    else:
        history_summary = {
            "total_seen": 0,
            "last_seen_at": None,
            "rolling_p99_7d": None,
            "rolling_p99_at": None,
            "known_counterparties_count": 0,
        }

    logger.debug("fetch_context case=%s history=%s", case_id, history_summary)
    return {
        "history_summary": history_summary,
        "audit_log": [
            _audit("fetch_context", "ok", {"history_summary": history_summary})
        ],
    }


# ===========================================================================
# enrich_external
# ===========================================================================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=8),
    retry=retry_if_exception_type(OperationalError),
    reraise=True,
)
async def _query_enrichment(pool: AsyncConnectionPool, state: InvestigationState) -> dict[str, Any]:
    """One enrichment round-trip; tenacity-wrapped on transient DB errors."""
    fired_rules = {h.get("rule") for h in state.get("rule_hits", [])}
    account_src = state["account_src"]
    account_dst = state.get("account_dst")

    enrichment: dict[str, Any] = {
        "ofac_matches": [],
        "watchlist_matches": [],
        "mixer_matches": [],
        "recent_events_src": [],
        "recent_events_dst": [],
    }

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            if "ofac_sdn_match" in fired_rules:
                addrs = [a for a in [account_src, account_dst] if a]
                await cur.execute(
                    "SELECT address, asset, sdn_uid, source_url, fetched_at "
                    "FROM ofac_sdn_crypto WHERE address = ANY(%s)",
                    (addrs,),
                )
                for r in await cur.fetchall():
                    enrichment["ofac_matches"].append({
                        "address": r[0],
                        "asset": r[1],
                        "sdn_uid": r[2],
                        "source_url": r[3],
                        "fetched_at": r[4].isoformat() if r[4] else None,
                    })

            if "watchlist_hit" in fired_rules:
                addrs = [a for a in [account_src, account_dst] if a]
                await cur.execute(
                    "SELECT address, label, risk_tier, tags FROM watchlist "
                    "WHERE address = ANY(%s)",
                    (addrs,),
                )
                for r in await cur.fetchall():
                    enrichment["watchlist_matches"].append({
                        "address": r[0],
                        "label": r[1],
                        "risk_tier": r[2],
                        "tags": list(r[3] or []),
                    })

            if "mixer_direct" in fired_rules and account_dst:
                await cur.execute(
                    "SELECT address, label, source FROM mixer_addresses "
                    "WHERE address = %s",
                    (account_dst,),
                )
                for r in await cur.fetchall():
                    enrichment["mixer_matches"].append({
                        "address": r[0],
                        "label": r[1],
                        "source": r[2],
                    })

            # A few recent events for narrative color (both src and dst).
            for key, addr in (("recent_events_src", account_src),
                              ("recent_events_dst", account_dst)):
                if not addr:
                    continue
                await cur.execute(
                    """
                    SELECT tx_hash, validated_at, tx_type, amount_xrp
                    FROM events
                    WHERE account_src = %s OR account_dst = %s
                    ORDER BY validated_at DESC
                    LIMIT 5
                    """,
                    (addr, addr),
                )
                for r in await cur.fetchall():
                    enrichment[key].append({
                        "tx_hash": r[0],
                        "validated_at": r[1].isoformat() if r[1] else None,
                        "tx_type": r[2],
                        "amount_xrp": float(r[3]) if r[3] is not None else None,
                    })
    return enrichment


async def enrich_external(state: InvestigationState) -> dict[str, Any]:
    """Pull DB-side metadata for the rules that fired + recent-event color."""
    case_id = state.get("case_id", "?")
    prior_attempts = state.get("enrichment_attempts", 0)
    attempt = prior_attempts + 1
    pool = db.get_pool()

    try:
        enrichment = await _query_enrichment(pool, state)
    except Exception as e:
        logger.exception("enrich_external case=%s failed: %s", case_id, e)
        return {
            "enrichment": {"error": str(e), "ofac_matches": [], "watchlist_matches": [],
                           "mixer_matches": [], "recent_events_src": [],
                           "recent_events_dst": []},
            "enrichment_attempts": attempt,
            "audit_log": [_audit("enrich_external", "failed",
                                 {"error": str(e)}, attempt=attempt)],
        }

    return {
        "enrichment": enrichment,
        "enrichment_attempts": attempt,
        "audit_log": [
            _audit(
                "enrich_external",
                "ok",
                {
                    "ofac": len(enrichment["ofac_matches"]),
                    "watchlist": len(enrichment["watchlist_matches"]),
                    "mixer": len(enrichment["mixer_matches"]),
                    "recent_src": len(enrichment["recent_events_src"]),
                    "recent_dst": len(enrichment["recent_events_dst"]),
                },
                attempt=attempt,
            )
        ],
    }


# ===========================================================================
# build_narrative (closure-bound to a NarrativeClient via app.build_graph)
# ===========================================================================

def _canned_low_confidence_narrative(reason: str) -> Narrative:
    """Final-fallback narrative when both LLM call and validation retry fail."""
    return Narrative(
        summary=f"Narrative generation failed; routing to analyst review. ({reason})"[:600],
        evidence=[
            {  # type: ignore[list-item]
                "fact": f"Narrative generation failed: {reason}",
                "source": "scorer",
            }
        ],
        risk_factors=["narrative-failure"],
        confidence=0.0,
        recommended_action="request_analyst_review",
    )


def make_build_narrative(
    narrative_client: NarrativeClient,
) -> Callable[[InvestigationState], Awaitable[dict[str, Any]]]:
    """Bind the LLM client to a node function via closure."""

    async def build_narrative(state: InvestigationState) -> dict[str, Any]:
        case_id = state.get("case_id", "?")
        prior_attempts = state.get("narrative_attempts", 0)
        attempt = prior_attempts + 1

        ctx = NarrativeContext(
            case_id=state.get("case_id", ""),
            account_src=state.get("account_src", ""),
            account_dst=state.get("account_dst"),
            amount_xrp=state.get("amount_xrp"),
            memo_decoded=state.get("memo_decoded"),
            risk_band=state.get("risk_band", "low"),
            rule_hits=list(state.get("rule_hits") or []),
            history_summary=dict(state.get("history_summary") or {}),
            enrichment=dict(state.get("enrichment") or {}),
        )

        # First call. Transport-error retries happen INSIDE the client
        # (tenacity in AnthropicNarrativeClient); validation retries are
        # the node's job (one retry, then canned fallback).
        narrative: Narrative
        try:
            narrative = await narrative_client.generate(ctx)
        except ValidationError as ve:
            logger.warning("build_narrative case=%s validation fail (try 1): %s",
                           case_id, ve)
            try:
                narrative = await narrative_client.generate(ctx)
                attempt += 1
            except Exception as e2:
                logger.warning("build_narrative case=%s validation fail (try 2): %s",
                               case_id, e2)
                narrative = _canned_low_confidence_narrative(f"validation: {e2}")
                attempt += 1
                return {
                    "narrative": narrative.model_dump(),
                    "narrative_attempts": attempt,
                    "audit_log": [
                        _audit("build_narrative", "canned_fallback",
                               {"reason": "validation"}, attempt=attempt)
                    ],
                }
        except Exception as e:
            logger.warning("build_narrative case=%s transport fail: %s", case_id, e)
            narrative = _canned_low_confidence_narrative(f"transport: {e}")
            return {
                "narrative": narrative.model_dump(),
                "narrative_attempts": attempt,
                "audit_log": [
                    _audit("build_narrative", "canned_fallback",
                           {"reason": "transport"}, attempt=attempt)
                ],
            }

        return {
            "narrative": narrative.model_dump(),
            "narrative_attempts": attempt,
            "audit_log": [
                _audit(
                    "build_narrative",
                    "ok",
                    {
                        "recommended_action": narrative.recommended_action,
                        "confidence": narrative.confidence,
                        "client": type(narrative_client).__name__,
                    },
                    attempt=attempt,
                )
            ],
        }

    return build_narrative


# ===========================================================================
# triage_branch
# ===========================================================================

async def triage_branch(state: InvestigationState) -> dict[str, Any]:
    """Read narrative + band; write triage_path. Pure dispatch — no I/O."""
    narrative = state.get("narrative") or {}
    decision: TriageDecision = decide(
        risk_band=state.get("risk_band", "low"),
        recommended_action=str(narrative.get("recommended_action") or "request_analyst_review"),
        confidence=float(narrative.get("confidence") or 0.0),
    )
    return {
        "triage_path": decision.triage_path,
        "case_status_target": decision.case_status,
        "audit_log": [
            _audit(
                "triage_branch",
                "decided",
                {
                    "triage_path": decision.triage_path,
                    "case_status_target": decision.case_status,
                    "rationale": decision.rationale,
                },
            )
        ],
    }


# ===========================================================================
# hitl_pause
# ===========================================================================

async def hitl_pause(state: InvestigationState) -> dict[str, Any]:
    """Block until an analyst sends a Command(resume=...).

    The resume value is expected to be a dict with at least:
        {"decision": "approve" | "reject" | "escalate", "note": "...", "actor": "..."}
    The /cases/{id}/decision endpoint (next commit) constructs that dict.
    Until then, the CLI investigate subcommand or tests can drive resumes
    manually.
    """
    # Persist a status flip BEFORE blocking so the analyst UI can see the
    # case waiting. The case_status_target was set by triage_branch and
    # captures whether this is a plain pending_hitl or an escalation.
    target_status = state.get("case_status_target") or "pending_hitl"

    case_id = state.get("case_id")
    if case_id:
        pool = db.get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE cases SET status = %s WHERE id = %s",
                    (target_status, case_id),
                )

    # interrupt() suspends the graph; resume value populates 'analyst_decision'.
    decision = interrupt({"status": target_status, "case_id": case_id})

    # Normalize the decision payload (Command(resume=...) can pass any JSON).
    if not isinstance(decision, dict):
        decision = {"decision": str(decision)}

    return {
        "analyst_decision": decision,
        "audit_log": [
            _audit(
                "hitl_pause",
                "resumed",
                {"target_status": target_status, "decision": decision},
            )
        ],
    }


# ===========================================================================
# record_outcome
# ===========================================================================

def _final_status_from_state(state: InvestigationState) -> str:
    """Compute the terminal cases.status value at the end of the graph."""
    decision = state.get("analyst_decision") or {}
    triage_path = state.get("triage_path")

    if triage_path == "auto_close":
        return "auto_closed"

    # HITL completed: derive from analyst decision.
    raw = str(decision.get("decision") or "").lower()
    if raw == "approve":
        return "approved"
    if raw == "reject":
        return "rejected"
    if raw == "escalate":
        return "escalated"
    # Fallback if the resume payload was unrecognizable.
    return "errored"


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=30),
    retry=retry_if_exception_type(OperationalError),
    reraise=True,
)
async def _write_outcome(pool: AsyncConnectionPool, case_id: str,
                          final_status: str, payload: dict[str, Any]) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE cases SET status = %s WHERE id = %s",
                (final_status, case_id),
            )
            await cur.execute(
                """
                INSERT INTO case_artifacts (case_id, kind, payload)
                VALUES (%s, 'analyst_decision', %s::jsonb)
                """,
                (case_id, _to_json(payload)),
            )


def _to_json(payload: dict[str, Any]) -> str:
    import json
    return json.dumps(payload, default=str)


async def record_outcome(state: InvestigationState) -> dict[str, Any]:
    """Persist the final case status + analyst decision artifact."""
    case_id = state.get("case_id")
    final_status = _final_status_from_state(state)
    decision = state.get("analyst_decision") or {}
    narrative = state.get("narrative") or {}

    audit_payload = {
        "final_status": final_status,
        "analyst_decision": decision,
        "narrative_recommendation": narrative.get("recommended_action"),
        "narrative_confidence": narrative.get("confidence"),
    }

    if case_id:
        pool = db.get_pool()
        try:
            await _write_outcome(pool, case_id, final_status, audit_payload)
        except Exception as e:
            # TODO: dead-letter file on terminal failure (eng-review hardening).
            logger.exception("record_outcome case=%s WRITE FAILED: %s", case_id, e)
            return {
                "final_status": "errored",
                "audit_log": [
                    _audit("record_outcome", "write_failed",
                           {"error": str(e), "intended_status": final_status})
                ],
            }

    return {
        "final_status": final_status,
        "audit_log": [
            _audit("record_outcome", "ok",
                   {"final_status": final_status, "case_id": case_id})
        ],
    }


# ===========================================================================
# Conditional-edge router (referenced by app.build_graph)
# ===========================================================================

def triage_router(state: InvestigationState) -> str:
    """Map triage_path to the next node name. Default: hitl_pause."""
    path = state.get("triage_path") or "hitl_pause"
    if path == "auto_close":
        return "record_outcome"
    return "hitl_pause"


# ===========================================================================
# Default narrative client (used when app.build_graph() isn't given one)
# ===========================================================================

_default_narrative_client: NarrativeClient = MockNarrativeClient()
