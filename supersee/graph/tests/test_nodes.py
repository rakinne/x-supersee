"""Per-node tests against the live Postgres.

Skipped when DATABASE_URL is the fake DSN unit-tests use. Each test
truncates the relevant tables before running and re-initializes the
db pool so the singleton is consistent.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from supersee import clock, db
from supersee.graph.narrative import (
    FailingNarrativeClient,
    Narrative,
    NarrativeEvidenceItem,
    StaticNarrativeClient,
)
from supersee.graph.nodes import (
    enrich_external,
    fetch_context,
    make_build_narrative,
    record_outcome,
    triage_branch,
)


_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"
pytestmark = pytest.mark.skipif(
    _IS_FAKE_DSN, reason="node tests need a real Postgres DSN"
)


@pytest.fixture
async def pool() -> AsyncIterator[AsyncConnectionPool]:
    # Use db.init_pool so nodes that call db.get_pool() find it.
    await db.close_pool()  # reset between tests
    p = await db.init_pool(_DSN)
    try:
        yield p
    finally:
        await db.close_pool()


@pytest.fixture(autouse=True)
async def clean_db(pool: AsyncConnectionPool) -> AsyncIterator[None]:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                TRUNCATE events, account_history, cases, case_artifacts,
                         ofac_sdn_crypto, watchlist, mixer_addresses,
                         exchange_labels, ingestor_cursor, enrichment_cache,
                         rate_limits
                RESTART IDENTITY CASCADE
                """
            )
    yield


async def _insert_event(
    pool: AsyncConnectionPool,
    account_src: str,
    account_dst: str | None,
    amount_xrp: float | None,
    validated_at: datetime,
    tx_type: str = "Payment",
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO events (tx_hash, ledger_index, validated_at, tx_type,
                                    account_src, account_dst, amount_xrp, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (
                    uuid.uuid4().hex,
                    1,
                    validated_at,
                    tx_type,
                    account_src,
                    account_dst,
                    amount_xrp,
                ),
            )


async def _insert_case(
    pool: AsyncConnectionPool,
    case_id: str,
    event_id: int,
    risk_band: str = "medium",
    status: str = "pending",
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits)
                VALUES (%s, %s, %s, 0.3, %s, '[]'::jsonb)
                """,
                (case_id, event_id, status, risk_band),
            )


# ===========================================================================
# fetch_context
# ===========================================================================

class TestFetchContext:
    async def test_fresh_account_returns_zero_summary(
        self, pool: AsyncConnectionPool
    ) -> None:
        state = {"case_id": "c-1", "account_src": "rNoHistory"}
        out = await fetch_context(state)
        hs = out["history_summary"]
        assert hs["total_seen"] == 0
        assert hs["rolling_p99_7d"] is None
        assert hs["known_counterparties_count"] == 0
        # Audit log appended
        assert any(e["event"] == "ok" for e in out["audit_log"])

    async def test_existing_account_returns_aggregates(
        self, pool: AsyncConnectionPool
    ) -> None:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (
                        account, total_seen, last_seen_at,
                        rolling_p99_7d, rolling_p99_at, known_counterparties
                    )
                    VALUES (%s, 42, NOW(), 12345.0, NOW(), ARRAY['rA', 'rB', 'rC'])
                    """,
                    ("rRich",),
                )
        out = await fetch_context({"case_id": "c-2", "account_src": "rRich"})
        hs = out["history_summary"]
        assert hs["total_seen"] == 42
        assert hs["rolling_p99_7d"] == 12345.0
        assert hs["known_counterparties_count"] == 3


# ===========================================================================
# enrich_external
# ===========================================================================

class TestEnrichExternal:
    async def test_ofac_match_pulled_when_rule_fired(
        self, pool: AsyncConnectionPool
    ) -> None:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at)
                    VALUES ('rOFAC', 'XRP', 'NK-1', 'http://x', NOW())
                    """
                )

        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "account_dst": "rOFAC",
            "rule_hits": [{"rule": "ofac_sdn_match", "score": 0.5}],
        }
        out = await enrich_external(state)
        e = out["enrichment"]
        assert len(e["ofac_matches"]) == 1
        assert e["ofac_matches"][0]["address"] == "rOFAC"
        assert e["ofac_matches"][0]["sdn_uid"] == "NK-1"
        assert out["enrichment_attempts"] == 1

    async def test_no_lookup_when_rule_did_not_fire(
        self, pool: AsyncConnectionPool
    ) -> None:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at)
                    VALUES ('rOFAC', 'XRP', 'NK-1', 'http://x', NOW())
                    """
                )

        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "account_dst": "rOFAC",
            "rule_hits": [],  # no rule fired
        }
        out = await enrich_external(state)
        assert out["enrichment"]["ofac_matches"] == []

    async def test_recent_events_populated_when_src_known(
        self, pool: AsyncConnectionPool
    ) -> None:
        await _insert_event(pool, "rSrc", "rDst1", 100.0, datetime(2026, 5, 1, tzinfo=UTC))
        await _insert_event(pool, "rSrc", "rDst2", 200.0, datetime(2026, 5, 2, tzinfo=UTC))

        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "account_dst": None,
            "rule_hits": [],
        }
        out = await enrich_external(state)
        recent = out["enrichment"]["recent_events_src"]
        assert len(recent) == 2
        # Sorted DESC by validated_at — most recent first
        assert recent[0]["validated_at"].startswith("2026-05-02")


# ===========================================================================
# build_narrative
# ===========================================================================

class TestBuildNarrative:
    async def test_success_path_writes_narrative_and_increments_attempts(
        self,
    ) -> None:
        fixed = Narrative(
            summary="static test narrative",
            evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
            risk_factors=["test"],
            confidence=0.7,
            recommended_action="monitor",
        )
        client = StaticNarrativeClient(fixed)
        build = make_build_narrative(client)

        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "rule_hits": [],
            "history_summary": {},
            "enrichment": {},
        }
        out = await build(state)
        assert out["narrative"]["summary"] == "static test narrative"
        assert out["narrative"]["confidence"] == 0.7
        assert out["narrative_attempts"] == 1
        assert any(e["event"] == "ok" for e in out["audit_log"])

    async def test_transport_failure_falls_back_to_canned(
        self,
    ) -> None:
        client = FailingNarrativeClient(lambda: RuntimeError("network"))
        build = make_build_narrative(client)

        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "rule_hits": [],
            "history_summary": {},
            "enrichment": {},
        }
        out = await build(state)
        assert out["narrative"]["recommended_action"] == "request_analyst_review"
        assert out["narrative"]["confidence"] == 0.0
        assert any(
            e["event"] == "canned_fallback" for e in out["audit_log"]
        )

    async def test_validation_failure_then_canned_fallback(self) -> None:
        # A client that ALWAYS raises ValidationError to drive the canned path.
        # We have to construct a ValidationError; use Narrative's own validation
        # by trying to build an invalid one.
        try:
            Narrative(
                summary="x",
                evidence=[],
                risk_factors=[],
                confidence=0.5,
                recommended_action="monitor",
            )
        except ValidationError as ve:
            saved_ve = ve
        else:
            pytest.fail("expected ValidationError")

        client = FailingNarrativeClient(lambda: saved_ve)
        build = make_build_narrative(client)
        state = {
            "case_id": "c-1",
            "account_src": "rSrc",
            "rule_hits": [],
            "history_summary": {},
            "enrichment": {},
        }
        out = await build(state)
        # Both call attempts raised ValidationError → canned fallback
        assert out["narrative"]["confidence"] == 0.0
        assert any(
            e["event"] == "canned_fallback" for e in out["audit_log"]
        )


# ===========================================================================
# triage_branch
# ===========================================================================

class TestTriageBranchNode:
    async def test_writes_triage_path_from_narrative(self) -> None:
        state = {
            "risk_band": "high",
            "narrative": {
                "recommended_action": "escalate",
                "confidence": 0.95,
            },
        }
        out = await triage_branch(state)
        assert out["triage_path"] == "hitl_pause"
        # Audit log carries the case_status target
        assert out["audit_log"][0]["detail"]["case_status_target"] == "escalated"

    async def test_low_confidence_routes_to_hitl_even_if_action_is_no_action(self) -> None:
        state = {
            "risk_band": "medium",
            "narrative": {"recommended_action": "no_action", "confidence": 0.3},
        }
        out = await triage_branch(state)
        assert out["triage_path"] == "hitl_pause"


# ===========================================================================
# record_outcome
# ===========================================================================

class TestRecordOutcome:
    async def test_auto_close_path_writes_status(
        self, pool: AsyncConnectionPool
    ) -> None:
        case_id = str(uuid.uuid4())
        await _insert_event(pool, "rSrc", "rDst", 100.0, datetime(2026, 5, 1, tzinfo=UTC))
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM events LIMIT 1")
                event_id = (await cur.fetchone())[0]
        await _insert_case(pool, case_id, event_id, status="in_progress")

        state = {
            "case_id": case_id,
            "triage_path": "auto_close",
            "analyst_decision": None,
            "narrative": {"recommended_action": "no_action", "confidence": 0.9},
        }
        out = await record_outcome(state)
        assert out["final_status"] == "auto_closed"

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
                row = await cur.fetchone()
                assert row[0] == "auto_closed"

                # Artifact written
                await cur.execute(
                    "SELECT kind, payload FROM case_artifacts WHERE case_id = %s",
                    (case_id,),
                )
                arows = await cur.fetchall()
                assert any(r[0] == "analyst_decision" for r in arows)

    async def test_approve_decision_sets_approved(
        self, pool: AsyncConnectionPool
    ) -> None:
        case_id = str(uuid.uuid4())
        await _insert_event(pool, "rSrc", "rDst", 100.0, datetime(2026, 5, 1, tzinfo=UTC))
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM events LIMIT 1")
                event_id = (await cur.fetchone())[0]
        await _insert_case(pool, case_id, event_id, status="pending_hitl")

        state = {
            "case_id": case_id,
            "triage_path": "hitl_pause",
            "analyst_decision": {"decision": "approve", "note": "ok", "actor": "test"},
            "narrative": {"recommended_action": "request_analyst_review", "confidence": 0.7},
        }
        out = await record_outcome(state)
        assert out["final_status"] == "approved"

    async def test_reject_decision_sets_rejected(
        self, pool: AsyncConnectionPool
    ) -> None:
        case_id = str(uuid.uuid4())
        await _insert_event(pool, "rSrc", "rDst", 100.0, datetime(2026, 5, 1, tzinfo=UTC))
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM events LIMIT 1")
                event_id = (await cur.fetchone())[0]
        await _insert_case(pool, case_id, event_id, status="pending_hitl")

        state = {
            "case_id": case_id,
            "triage_path": "hitl_pause",
            "analyst_decision": {"decision": "reject", "note": "false positive", "actor": "test"},
            "narrative": {"recommended_action": "request_analyst_review", "confidence": 0.7},
        }
        out = await record_outcome(state)
        assert out["final_status"] == "rejected"

    async def test_unknown_decision_lands_in_errored(
        self, pool: AsyncConnectionPool
    ) -> None:
        case_id = str(uuid.uuid4())
        await _insert_event(pool, "rSrc", "rDst", 100.0, datetime(2026, 5, 1, tzinfo=UTC))
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM events LIMIT 1")
                event_id = (await cur.fetchone())[0]
        await _insert_case(pool, case_id, event_id, status="pending_hitl")

        state = {
            "case_id": case_id,
            "triage_path": "hitl_pause",
            "analyst_decision": {"decision": "smile-and-wave"},
            "narrative": {"recommended_action": "request_analyst_review", "confidence": 0.7},
        }
        out = await record_outcome(state)
        assert out["final_status"] == "errored"
