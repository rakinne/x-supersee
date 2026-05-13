"""End-to-end graph integration: full pipeline, HITL pause/resume, restart recovery.

Exercises the compiled graph against real Postgres + AsyncPostgresSaver.
Skipped when DATABASE_URL is the fake unit-test DSN.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from langgraph.types import Command
from psycopg_pool import AsyncConnectionPool

from supersee import db
from supersee.graph.app import close_graph, init_graph
from supersee.graph.narrative import (
    Narrative,
    NarrativeEvidenceItem,
    StaticNarrativeClient,
)

_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"
pytestmark = pytest.mark.skipif(
    _IS_FAKE_DSN, reason="graph integration needs a real Postgres DSN"
)


@pytest.fixture
async def pool() -> AsyncIterator[AsyncConnectionPool]:
    await db.close_pool()
    await close_graph()
    p = await db.init_pool(_DSN)
    try:
        yield p
    finally:
        await close_graph()
        await db.close_pool()


@pytest.fixture(autouse=True)
async def clean_db(pool: AsyncConnectionPool) -> AsyncIterator[None]:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                TRUNCATE events, account_history, cases, case_artifacts,
                         ofac_sdn_crypto, watchlist, mixer_addresses,
                         exchange_labels, ingestor_cursor, enrichment_cache,
                         rate_limits
                RESTART IDENTITY CASCADE
                """
        )
        # Drop checkpointer tables so each integration test starts clean.
        # AsyncPostgresSaver.setup() recreates them as needed.
        await cur.execute("DROP TABLE IF EXISTS checkpoint_writes CASCADE")
        await cur.execute("DROP TABLE IF EXISTS checkpoint_blobs CASCADE")
        await cur.execute("DROP TABLE IF EXISTS checkpoints CASCADE")
        await cur.execute("DROP TABLE IF EXISTS checkpoint_migrations CASCADE")
    yield


async def _seed_event_and_case(
    pool: AsyncConnectionPool,
    *,
    risk_band: str = "high",
    rule_hits: list[dict] | None = None,
) -> tuple[str, int]:
    """Insert a fresh event + case row. Returns (case_id, event_id)."""
    case_id = str(uuid.uuid4())
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO events (
                    tx_hash, ledger_index, validated_at, tx_type,
                    account_src, account_dst, amount_xrp, raw_json
                )
                VALUES (%s, %s, %s, 'Payment', 'rSrc', 'rDst', 75000.0, '{}'::jsonb)
                RETURNING id
                """,
            (
                uuid.uuid4().hex,
                1,
                datetime.now(UTC),
            ),
        )
        event_id = (await cur.fetchone())[0]
        await cur.execute(
            """
                INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits)
                VALUES (%s, %s, 'pending', %s, %s, %s::jsonb)
                """,
            (
                case_id,
                event_id,
                0.9 if risk_band == "high" else 0.3,
                risk_band,
                __import__("json").dumps(rule_hits or []),
            ),
        )
    return case_id, event_id


def _state_for_case(
    case_id: str,
    event_id: int,
    risk_band: str,
    rule_hits: list[dict],
) -> dict:
    return {
        "case_id": case_id,
        "event_id": event_id,
        "account_src": "rSrc",
        "account_dst": "rDst",
        "amount_xrp": 75000.0,
        "memo_decoded": None,
        "risk_band": risk_band,
        "rule_hits": rule_hits,
    }


# ===========================================================================
# Happy path: auto-close (medium band + no_action narrative)
# ===========================================================================

class TestAutoCloseFlow:
    async def test_medium_band_with_no_action_narrative_auto_closes(
        self, pool: AsyncConnectionPool
    ) -> None:
        # Force a no_action high-confidence narrative.
        narrative = Narrative(
            summary="benign on closer look",
            evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
            risk_factors=["false-positive"],
            confidence=0.92,
            recommended_action="no_action",
        )
        graph = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))

        case_id, event_id = await _seed_event_and_case(
            pool,
            risk_band="medium",
            rule_hits=[{"rule": "new_counterparty", "score": 0.1, "rationale": "new dst"}],
        )

        config = {"configurable": {"thread_id": case_id}}
        result = await graph.ainvoke(
            _state_for_case(case_id, event_id, "medium",
                            [{"rule": "new_counterparty", "score": 0.1, "rationale": "new dst"}]),
            config,
        )

        assert result["final_status"] == "auto_closed"
        assert result["triage_path"] == "auto_close"

        # Audit log contains all expected node entries
        nodes = [e["node"] for e in result["audit_log"]]
        assert "fetch_context" in nodes
        assert "enrich_external" in nodes
        assert "build_narrative" in nodes
        assert "triage_branch" in nodes
        assert "record_outcome" in nodes
        # hitl_pause was NOT visited (auto_close path)
        assert "hitl_pause" not in nodes

        # cases row updated
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
            assert (await cur.fetchone())[0] == "auto_closed"


# ===========================================================================
# HITL path: high band → interrupt → resume with decision
# ===========================================================================

class TestHITLFlow:
    async def test_high_band_escalate_pauses_then_resumes_to_approved(
        self, pool: AsyncConnectionPool
    ) -> None:
        narrative = Narrative(
            summary="confirmed risk",
            evidence=[NarrativeEvidenceItem(fact="OFAC match", source="ofac")],
            risk_factors=["sanctions"],
            confidence=0.95,
            recommended_action="escalate",
        )
        graph = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))

        case_id, event_id = await _seed_event_and_case(
            pool,
            risk_band="high",
            rule_hits=[{"rule": "ofac_sdn_match", "score": 0.5, "rationale": "OFAC"}],
        )

        config = {"configurable": {"thread_id": case_id}}

        # First invocation: runs until hitl_pause interrupt.
        await graph.ainvoke(
            _state_for_case(case_id, event_id, "high",
                            [{"rule": "ofac_sdn_match", "score": 0.5, "rationale": "OFAC"}]),
            config,
        )

        # The case row should be in 'escalated' (high band + escalate action).
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
            assert (await cur.fetchone())[0] == "escalated"

        # The checkpointer should have a pending state for this thread.
        state_snapshot = await graph.aget_state(config)
        # An interrupted graph has `next` pointing at the interrupted node.
        # In LangGraph 0.4 the interrupt is exposed via state.tasks[].interrupts.
        assert state_snapshot.next, "graph should be suspended at an interrupt"

        # Resume with an analyst approve decision.
        final = await graph.ainvoke(
            Command(resume={"decision": "approve", "note": "confirmed",
                            "actor": "test-analyst"}),
            config,
        )

        assert final["final_status"] == "approved"
        nodes = [e["node"] for e in final["audit_log"]]
        assert "hitl_pause" in nodes
        assert "record_outcome" in nodes

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
            assert (await cur.fetchone())[0] == "approved"

    async def test_resume_with_reject_marks_rejected(
        self, pool: AsyncConnectionPool
    ) -> None:
        narrative = Narrative(
            summary="needs review",
            evidence=[NarrativeEvidenceItem(fact="watchlist hit", source="watchlist")],
            risk_factors=["watchlist"],
            confidence=0.7,
            recommended_action="request_analyst_review",
        )
        graph = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))

        case_id, event_id = await _seed_event_and_case(
            pool,
            risk_band="medium",
            rule_hits=[{"rule": "watchlist_hit", "score": 0.2,
                        "rationale": "source on watchlist"}],
        )
        config = {"configurable": {"thread_id": case_id}}
        await graph.ainvoke(
            _state_for_case(case_id, event_id, "medium",
                            [{"rule": "watchlist_hit", "score": 0.2,
                              "rationale": "source on watchlist"}]),
            config,
        )
        final = await graph.ainvoke(
            Command(resume={"decision": "reject", "note": "false positive"}),
            config,
        )
        assert final["final_status"] == "rejected"


# ===========================================================================
# Restart recovery: simulate process restart between pause and resume
# ===========================================================================

class TestRestartRecovery:
    async def test_resume_works_after_fresh_graph_instance(
        self, pool: AsyncConnectionPool
    ) -> None:
        """The checkpointer persists state across init_graph() calls so a
        process restart can resume any pending HITL case."""
        narrative = Narrative(
            summary="needs review",
            evidence=[NarrativeEvidenceItem(fact="watchlist hit", source="watchlist")],
            risk_factors=["watchlist"],
            confidence=0.8,
            recommended_action="request_analyst_review",
        )

        # ---- First "process": start the investigation, hit pause ----
        graph_a = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))
        case_id, event_id = await _seed_event_and_case(
            pool,
            risk_band="high",
            rule_hits=[{"rule": "watchlist_hit", "score": 0.2, "rationale": "wl"}],
        )
        config = {"configurable": {"thread_id": case_id}}
        await graph_a.ainvoke(
            _state_for_case(case_id, event_id, "high",
                            [{"rule": "watchlist_hit", "score": 0.2, "rationale": "wl"}]),
            config,
        )

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
            assert (await cur.fetchone())[0] in ("pending_hitl", "escalated")

        # ---- Simulate restart: tear down and rebuild ----
        await close_graph()
        graph_b = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))

        # The new graph picks up where graph_a left off via the persisted checkpointer.
        final = await graph_b.ainvoke(
            Command(resume={"decision": "approve", "actor": "post-restart"}),
            config,
        )
        assert final["final_status"] == "approved"


# ===========================================================================
# Idempotency: re-initializing the graph is safe
# ===========================================================================

class TestInitGraphIdempotent:
    async def test_double_init_returns_same_singleton(
        self, pool: AsyncConnectionPool
    ) -> None:
        narrative = Narrative(
            summary="x",
            evidence=[NarrativeEvidenceItem(fact="x", source="scorer")],
            risk_factors=["x"],
            confidence=0.5,
            recommended_action="monitor",
        )
        g1 = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))
        g2 = await init_graph(pool, narrative_client=StaticNarrativeClient(narrative))
        assert g1 is g2
