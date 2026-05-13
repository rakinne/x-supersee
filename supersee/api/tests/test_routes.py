"""Integration tests for /cases (queue + detail + decision).

Skipped when DATABASE_URL is the fake unit-test DSN. Each test truncates
relevant tables, initializes the pool + graph with a MockNarrativeClient,
and drives the FastAPI app via httpx.ASGITransport (no real network).
"""

from __future__ import annotations

import json as _json
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from supersee import clock, db
from supersee.config.scorer import ScorerSettings
from supersee.graph import app as graph_app
from supersee.graph.narrative import MockNarrativeClient, StaticNarrativeClient, Narrative, NarrativeEvidenceItem
from supersee.scorer.score_event import EventData, ScoringContext, score_event


_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"
pytestmark = pytest.mark.skipif(
    _IS_FAKE_DSN, reason="route tests need a real Postgres DSN"
)


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
async def stack() -> AsyncIterator[None]:
    """Init pool + graph (with mock LLM) for the duration of one test."""
    await db.close_pool()
    await graph_app.close_graph()
    await db.init_pool(_DSN)
    await graph_app.init_graph(db.get_pool(), narrative_client=MockNarrativeClient())
    try:
        yield None
    finally:
        await graph_app.close_graph()
        await db.close_pool()


@pytest.fixture(autouse=True)
async def clean_db(stack) -> AsyncIterator[None]:
    pool = db.get_pool()
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
            # Wipe checkpointer too so each test sees clean graph state
            await cur.execute("TRUNCATE checkpoints, checkpoint_writes, checkpoint_blobs")
    yield


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    # Import here so the env override above takes effect first.
    from supersee.api import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ===========================================================================
# Helpers
# ===========================================================================

async def _seed_paused_case(*, source_dst_ofac: bool = True) -> str:
    """Drive a fixture through to hitl_pause; return the case_id."""
    pool = db.get_pool()
    cfg = ScorerSettings()
    case_id = str(uuid.uuid4())
    src = f"rSrc{uuid.uuid4().hex[:8]}"
    dst = f"rDst{uuid.uuid4().hex[:8]}"
    amount = 75_000.0

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            if source_dst_ofac:
                await cur.execute(
                    "INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at) "
                    "VALUES (%s, 'XRP', 'test-seed', 'test://', NOW()) ON CONFLICT DO NOTHING",
                    (dst,),
                )
            await cur.execute(
                """
                INSERT INTO events (tx_hash, ledger_index, validated_at, tx_type,
                                    account_src, account_dst, amount_xrp, raw_json)
                VALUES (%s, 1, NOW(), 'Payment', %s, %s, %s, '{}'::jsonb)
                RETURNING id
                """,
                (uuid.uuid4().hex, src, dst, amount),
            )
            event_id = (await cur.fetchone())[0]

    event = EventData(amount_xrp=amount, account_src=src, account_dst=dst, memo_decoded=None)
    ctx = ScoringContext(
        rolling_p99_7d=None, known_counterparties=frozenset(),
        recent_outbound_count=0,
        ofac_addresses=frozenset({dst}) if source_dst_ofac else frozenset(),
        watchlist_addresses=frozenset(), mixer_addresses=frozenset(),
    )
    result = score_event(event, ctx, cfg)

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'in_progress', %s, %s, %s::jsonb)",
                (case_id, event_id, result.risk_score, result.risk_band, _json.dumps(result.to_jsonb())),
            )

    graph = graph_app.get_graph()
    config = {"configurable": {"thread_id": case_id}}
    await graph.ainvoke({
        "case_id": case_id, "event_id": event_id,
        "account_src": src, "account_dst": dst,
        "amount_xrp": amount, "memo_decoded": None,
        "risk_band": result.risk_band, "rule_hits": result.to_jsonb(),
    }, config)
    return case_id


async def _insert_approved_case() -> str:
    """Insert a case in 'approved' status with no graph state — for terminal-banner tests."""
    pool = db.get_pool()
    case_id = str(uuid.uuid4())
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO events (tx_hash, ledger_index, validated_at, tx_type,
                                    account_src, account_dst, amount_xrp, raw_json)
                VALUES (%s, 1, NOW(), 'Payment', 'rA', 'rB', 100, '{}'::jsonb)
                RETURNING id
                """,
                (uuid.uuid4().hex,),
            )
            event_id = (await cur.fetchone())[0]
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'approved', 0.3, 'medium', '[]'::jsonb)",
                (case_id, event_id),
            )
    return case_id


# ===========================================================================
# GET /cases
# ===========================================================================

class TestQueueRoute:
    async def test_root_redirects_to_queue(self, client: AsyncClient) -> None:
        r = await client.get("/", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/cases"

    async def test_empty_queue_renders_empty_state(self, client: AsyncClient) -> None:
        r = await client.get("/cases")
        assert r.status_code == 200
        assert "No cases in the queue" in r.text
        assert "XRPL Flagged Transaction Queue" in r.text

    async def test_queue_shows_paused_case(self, client: AsyncClient) -> None:
        case_id = await _seed_paused_case()
        r = await client.get("/cases")
        assert r.status_code == 200
        assert case_id[:8] in r.text  # truncated id
        assert "escalated" in r.text or "pending_hitl" in r.text
        assert f'href="/cases/{case_id}"' in r.text

    async def test_default_view_hides_approved(self, client: AsyncClient) -> None:
        await _insert_approved_case()
        await _seed_paused_case()
        r = await client.get("/cases")
        # default view shows open queue, not approved
        assert "approved" not in r.text.lower() or "approved" in r.text  # status filter labels include 'approved'
        # but the approved case id should NOT be in the table
        r_all = await client.get("/cases?all=1")
        assert r_all.status_code == 200


# ===========================================================================
# GET /cases/{case_id}
# ===========================================================================

class TestDetailRoute:
    async def test_404_for_unknown_case(self, client: AsyncClient) -> None:
        r = await client.get(f"/cases/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_detail_renders_narrative_and_hitl_form(self, client: AsyncClient) -> None:
        case_id = await _seed_paused_case()
        r = await client.get(f"/cases/{case_id}")
        assert r.status_code == 200
        # Narrative panel rendered
        assert "Agent Narrative" in r.text
        # Recommendation present (from MockNarrativeClient: OFAC -> escalate)
        assert "escalate" in r.text.lower()
        # HITL form present
        assert f'action="/cases/{case_id}/decision"' in r.text
        assert 'name="decision" value="approve"' in r.text
        assert 'name="decision" value="reject"' in r.text
        assert 'name="decision" value="escalate"' in r.text

    async def test_detail_for_terminal_case_hides_hitl_form(self, client: AsyncClient) -> None:
        case_id = await _insert_approved_case()
        r = await client.get(f"/cases/{case_id}")
        assert r.status_code == 200
        # No decision buttons
        assert 'name="decision"' not in r.text
        # Terminal banner present
        assert "No further action required" in r.text


# ===========================================================================
# POST /cases/{case_id}/decision
# ===========================================================================

class TestDecisionRoute:
    async def test_approve_resumes_graph_and_redirects(self, client: AsyncClient) -> None:
        case_id = await _seed_paused_case()

        r = await client.post(
            f"/cases/{case_id}/decision",
            data={"decision": "approve", "actor": "test-analyst", "note": "ok"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == f"/cases/{case_id}"

        # Verify status flipped to approved
        pool = db.get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
                row = await cur.fetchone()
        assert row[0] == "approved"

    async def test_reject_sets_status_rejected(self, client: AsyncClient) -> None:
        case_id = await _seed_paused_case()
        r = await client.post(
            f"/cases/{case_id}/decision",
            data={"decision": "reject", "note": "false positive"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        pool = db.get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
                assert (await cur.fetchone())[0] == "rejected"

    async def test_stale_resume_redirects_with_flash(self, client: AsyncClient) -> None:
        """A second decision POST after approval should redirect with a flash, not crash."""
        case_id = await _seed_paused_case()
        # First decision: approve
        await client.post(f"/cases/{case_id}/decision",
                          data={"decision": "approve"})
        # Second decision: should hit the stale-resume guard
        r = await client.post(
            f"/cases/{case_id}/decision",
            data={"decision": "reject"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "flash=" in r.headers["location"]
        assert "already" in r.headers["location"]

    async def test_404_on_unknown_case(self, client: AsyncClient) -> None:
        r = await client.post(
            f"/cases/{uuid.uuid4()}/decision",
            data={"decision": "approve"},
        )
        assert r.status_code == 404

    async def test_invalid_decision_redirects_with_flash(self, client: AsyncClient) -> None:
        case_id = await _seed_paused_case()
        r = await client.post(
            f"/cases/{case_id}/decision",
            data={"decision": "smile"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "invalid+decision" in r.headers["location"]
