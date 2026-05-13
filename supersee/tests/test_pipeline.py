"""Integration tests for `supersee.pipeline` — scorer loop + recovery sweep."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from psycopg_pool import AsyncConnectionPool

from supersee import db
from supersee.graph import app as graph_app
from supersee.graph.narrative import MockNarrativeClient
from supersee.pipeline import (
    _process_one_event,
    recovery_sweep,
    run_scorer_loop,
)

_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"
pytestmark = pytest.mark.skipif(
    _IS_FAKE_DSN, reason="pipeline tests need a real Postgres DSN"
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
async def stack() -> AsyncIterator[None]:
    """Init pool + graph (with mock LLM)."""
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
        await cur.execute("TRUNCATE checkpoints, checkpoint_writes, checkpoint_blobs")
    from supersee.scorer.history import invalidate_lookups
    invalidate_lookups()
    yield


async def _insert_event(
    pool: AsyncConnectionPool, *, src: str, dst: str | None, amount: float | None,
    memo: str | None = None,
) -> int:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO events (tx_hash, ledger_index, validated_at, tx_type,
                                    account_src, account_dst, amount_xrp, memo_decoded, raw_json)
                VALUES (%s, 1, NOW(), 'Payment', %s, %s, %s, %s, '{}'::jsonb)
                RETURNING id
                """,
            (uuid.uuid4().hex, src, dst, amount, memo),
        )
        return int((await cur.fetchone())[0])


async def _case_status(pool: AsyncConnectionPool, case_id: str) -> str | None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT status FROM cases WHERE id = %s", (case_id,))
        r = await cur.fetchone()
    return r[0] if r else None


# ===========================================================================
# _process_one_event
# ===========================================================================


class TestProcessOneEvent:
    async def test_low_band_event_creates_auto_closed_case(self) -> None:
        pool = db.get_pool()
        # 100 XRP, no OFAC, no watchlist → low band; no rules fire above thresholds
        event_id = await _insert_event(pool, src="rA", dst="rB", amount=100.0)
        graph_queue: asyncio.Queue[str] = asyncio.Queue()

        result = await _process_one_event(pool, event_id, graph_queue)
        assert result is None  # low band → no graph push
        assert graph_queue.empty()

        # Case row should exist in auto_closed
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT status, risk_band FROM cases WHERE event_id = %s",
                (event_id,),
            )
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == "auto_closed"
        assert row[1] == "low"

    async def test_high_band_event_creates_pending_case_and_enqueues(self) -> None:
        pool = db.get_pool()
        # Seed an OFAC address so the dst hits → high band
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at) "
                "VALUES ('rOFAC', 'XRP', 'test', 'test://', NOW())"
            )

        event_id = await _insert_event(pool, src="rSrc", dst="rOFAC", amount=75_000.0)
        graph_queue: asyncio.Queue[str] = asyncio.Queue()

        result = await _process_one_event(pool, event_id, graph_queue)
        assert result is not None
        assert graph_queue.qsize() == 0  # we return the case_id; loop pushes

        # Case row in pending
        case_status = await _case_status(pool, result)
        assert case_status == "pending"

    async def test_already_scored_event_is_skipped(self) -> None:
        pool = db.get_pool()
        event_id = await _insert_event(pool, src="rA", dst="rB", amount=100.0)
        graph_queue: asyncio.Queue[str] = asyncio.Queue()

        # First pass: creates case
        await _process_one_event(pool, event_id, graph_queue)
        # Second pass: should skip (case already exists)
        result = await _process_one_event(pool, event_id, graph_queue)
        assert result is None

        # Only one case row
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM cases WHERE event_id = %s", (event_id,)
            )
            count = int((await cur.fetchone())[0])
        assert count == 1


# ===========================================================================
# Scorer loop
# ===========================================================================


class TestScorerLoop:
    async def test_loop_processes_queued_events(self) -> None:
        pool = db.get_pool()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at) "
                "VALUES ('rOFAC', 'XRP', 'test', 'test://', NOW())"
            )

        event_queue: asyncio.Queue[int] = asyncio.Queue()
        graph_queue: asyncio.Queue[str] = asyncio.Queue()

        # Queue two events: one low-band, one high-band
        low_id = await _insert_event(pool, src="rLow", dst="rOther", amount=100.0)
        high_id = await _insert_event(pool, src="rHigh", dst="rOFAC", amount=75_000.0)
        await event_queue.put(low_id)
        await event_queue.put(high_id)

        # Run the loop for a moment
        task = asyncio.create_task(run_scorer_loop(pool, event_queue, graph_queue))
        await asyncio.wait_for(event_queue.join(), timeout=5.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # graph_queue should contain ONE entry (the high-band case)
        assert graph_queue.qsize() == 1

        # Verify final status
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT event_id, status FROM cases ORDER BY event_id")
            rows = await cur.fetchall()
        assert len(rows) == 2
        # The low-band case is auto_closed; the high-band one is pending
        statuses = {r[0]: r[1] for r in rows}
        assert statuses[low_id] == "auto_closed"
        assert statuses[high_id] == "pending"


# ===========================================================================
# Recovery sweep
# ===========================================================================


class TestRecoverySweep:
    async def test_enqueues_events_without_cases(self) -> None:
        pool = db.get_pool()
        # Two events: one with a case row, one without
        e1 = await _insert_event(pool, src="rA", dst="rB", amount=100.0)
        e2 = await _insert_event(pool, src="rC", dst="rD", amount=200.0)
        case_id = str(uuid.uuid4())
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'auto_closed', 0.1, 'low', '[]'::jsonb)",
                (case_id, e1),
            )

        event_queue: asyncio.Queue[int] = asyncio.Queue()
        graph_queue: asyncio.Queue[str] = asyncio.Queue()
        events_n, cases_n = await recovery_sweep(pool, event_queue, graph_queue)
        assert events_n == 1
        assert cases_n == 0
        # The orphaned event id should be in the queue
        assert event_queue.get_nowait() == e2

    async def test_enqueues_pending_and_in_progress_cases(self) -> None:
        pool = db.get_pool()
        e1 = await _insert_event(pool, src="rA", dst="rB", amount=100.0)
        e2 = await _insert_event(pool, src="rC", dst="rD", amount=200.0)
        e3 = await _insert_event(pool, src="rE", dst="rF", amount=300.0)

        pending = str(uuid.uuid4())
        in_progress = str(uuid.uuid4())
        hitl = str(uuid.uuid4())
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'pending', 0.3, 'medium', '[]'::jsonb)",
                (pending, e1),
            )
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'in_progress', 0.4, 'medium', '[]'::jsonb)",
                (in_progress, e2),
            )
            await cur.execute(
                "INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits) "
                "VALUES (%s, %s, 'pending_hitl', 0.9, 'high', '[]'::jsonb)",
                (hitl, e3),
            )

        event_queue: asyncio.Queue[int] = asyncio.Queue()
        graph_queue: asyncio.Queue[str] = asyncio.Queue()
        events_n, cases_n = await recovery_sweep(pool, event_queue, graph_queue)
        # No orphan events
        assert events_n == 0
        # pending + in_progress are re-enqueued; pending_hitl is NOT (suspended)
        assert cases_n == 2
        ids = sorted([graph_queue.get_nowait(), graph_queue.get_nowait()])
        assert sorted([pending, in_progress]) == ids

    async def test_respects_limit(self) -> None:
        pool = db.get_pool()
        # Insert 5 orphaned events
        for i in range(5):
            await _insert_event(pool, src=f"r{i}", dst="rDst", amount=100.0)
        event_queue: asyncio.Queue[int] = asyncio.Queue()
        graph_queue: asyncio.Queue[str] = asyncio.Queue()
        events_n, _ = await recovery_sweep(
            pool, event_queue, graph_queue, event_limit=3, case_limit=1000,
        )
        assert events_n == 3
