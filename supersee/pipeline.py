"""In-process pipeline: scorer loop, graph loop, crash-recovery sweep.

The handoff diagram lives in ARCHITECTURE.md. Brief version:

    ingestor → event_queue → scorer_loop → graph_queue → graph_loop
                              │
                              └→ (low band) → cases.status = 'auto_closed'

`scorer_loop` reads `event_id` ints, builds a `ScoringContext` from the
DB-backed `history` module, scores, and either:
  - writes a case row with status='auto_closed' (low band; done)
  - writes a case row with status='pending' and pushes case_id onto the
    graph queue.

`graph_loop` reads `case_id` strings and drives the LangGraph
investigation. The bounded `Semaphore` prevents a startup-burst from
stampeding the LLM API. Each drive uses `Command(resume=...)`-free
ainvoke from the start; for cases that reach `hitl_pause`, the graph
suspends in the checkpointer and the analyst UI (/cases/{id}/decision)
issues the resume out-of-band.

`recovery_sweep` runs once at lifespan startup. It re-enqueues:
  - events with no matching case row (scorer dropped them)
  - cases in pending / in_progress (graph dropped them or never started)
It does NOT re-enqueue cases in pending_hitl — those are correctly
suspended in the checkpointer waiting on an analyst, no new ainvoke
should fire for them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

from psycopg_pool import AsyncConnectionPool

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig

from supersee.config import settings
from supersee.graph import app as graph_app
from supersee.scorer import history
from supersee.scorer.score_event import EventData, score_event

logger = logging.getLogger(__name__)


# ===========================================================================
# Scorer loop
# ===========================================================================


async def _load_event(pool: AsyncConnectionPool, event_id: int) -> tuple[Any, ...] | None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, account_src, account_dst, amount_xrp, memo_decoded
                FROM events WHERE id = %s
                """,
                (event_id,),
            )
            return await cur.fetchone()


async def _has_case_for_event(pool: AsyncConnectionPool, event_id: int) -> bool:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM cases WHERE event_id = %s LIMIT 1", (event_id,)
            )
            return (await cur.fetchone()) is not None


async def _write_case_row(
    pool: AsyncConnectionPool,
    case_id: str,
    event_id: int,
    risk_score: float,
    risk_band: str,
    rule_hits_json: str,
    status: str,
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (case_id, event_id, status, risk_score, risk_band, rule_hits_json),
            )


async def _process_one_event(
    pool: AsyncConnectionPool,
    event_id: int,
    graph_queue: asyncio.Queue[str],
) -> str | None:
    """Score one event; write a case row; return case_id if graph-bound, else None."""
    row = await _load_event(pool, event_id)
    if row is None:
        logger.debug("scorer: event %d not found (deleted?)", event_id)
        return None

    if await _has_case_for_event(pool, event_id):
        # Already scored (e.g., recovery sweep race with normal flow)
        return None

    event = EventData(
        amount_xrp=float(row[3]) if row[3] is not None else None,
        account_src=row[1],
        account_dst=row[2],
        memo_decoded=row[4],
    )
    cfg = settings.scorer
    lookups = await history.get_lookups(pool)
    ctx = await history.build_context(pool, event, lookups, cfg)
    result = score_event(event, ctx, cfg)

    case_id = str(uuid.uuid4())
    rule_hits_json = json.dumps(result.to_jsonb())
    if result.risk_band == "low":
        await _write_case_row(
            pool, case_id, event_id, result.risk_score, result.risk_band,
            rule_hits_json, status="auto_closed",
        )
        # Still record_event so account_history reflects the activity.
        await history.record_event(pool, event)
        return None

    # Medium / high → hand to the graph.
    await _write_case_row(
        pool, case_id, event_id, result.risk_score, result.risk_band,
        rule_hits_json, status="pending",
    )
    await history.record_event(pool, event)
    return case_id


async def run_scorer_loop(
    pool: AsyncConnectionPool,
    event_queue: asyncio.Queue[int],
    graph_queue: asyncio.Queue[str],
) -> None:
    """Pull from event_queue; score; push to graph_queue. Forever."""
    logger.info("scorer loop started")
    while True:
        event_id = await event_queue.get()
        try:
            case_id = await _process_one_event(pool, event_id, graph_queue)
            if case_id is not None:
                try:
                    graph_queue.put_nowait(case_id)
                except asyncio.QueueFull:
                    logger.warning(
                        "scorer: graph queue full (size=%d); dropping case %s — "
                        "recovery sweep will re-pick on next restart",
                        graph_queue.qsize(), case_id,
                    )
        except asyncio.CancelledError:
            event_queue.task_done()
            raise
        except Exception:
            logger.exception("scorer: failed on event_id=%d", event_id)
        finally:
            event_queue.task_done()


# ===========================================================================
# Graph loop
# ===========================================================================


async def _load_case_for_graph(
    pool: AsyncConnectionPool, case_id: str
) -> dict[str, Any] | None:
    """Hydrate the initial InvestigationState dict for a case_id."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT c.event_id, c.risk_band, c.rule_hits,
                       e.account_src, e.account_dst, e.amount_xrp, e.memo_decoded
                FROM cases c
                JOIN events e ON e.id = c.event_id
                WHERE c.id = %s
                """,
                (case_id,),
            )
            row = await cur.fetchone()
    if row is None:
        return None
    return {
        "case_id": case_id,
        "event_id": int(row[0]),
        "risk_band": row[1],
        "rule_hits": list(row[2] or []),
        "account_src": row[3],
        "account_dst": row[4],
        "amount_xrp": float(row[5]) if row[5] is not None else None,
        "memo_decoded": row[6],
    }


async def _mark_case_in_progress(pool: AsyncConnectionPool, case_id: str) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE cases SET status = 'in_progress' WHERE id = %s AND status = 'pending'",
                (case_id,),
            )


async def _drive_case(pool: AsyncConnectionPool, case_id: str) -> None:
    """Run the graph for one case from its initial state."""
    state = await _load_case_for_graph(pool, case_id)
    if state is None:
        logger.debug("graph: case %s not found in DB; skipping", case_id)
        return

    await _mark_case_in_progress(pool, case_id)
    graph = graph_app.get_graph()
    config: RunnableConfig = {"configurable": {"thread_id": case_id}}
    await graph.ainvoke(state, config)


async def run_graph_loop(
    pool: AsyncConnectionPool,
    graph_queue: asyncio.Queue[str],
    semaphore: asyncio.Semaphore,
) -> None:
    """Drain graph_queue under bounded concurrency. Forever."""
    logger.info(
        "graph loop started (concurrency=%d)", semaphore._value  # noqa: SLF001
    )

    async def _worker(case_id: str) -> None:
        async with semaphore:
            try:
                await _drive_case(pool, case_id)
            except Exception:
                logger.exception("graph: case %s failed", case_id)

    while True:
        case_id = await graph_queue.get()
        try:
            # Spawn the worker as a fire-and-forget task so the loop can keep
            # draining the queue even when one case takes a while. The
            # semaphore bounds total in-flight work.
            asyncio.create_task(_worker(case_id), name=f"graph:{case_id[:8]}")
        finally:
            graph_queue.task_done()


# ===========================================================================
# Crash-recovery sweep
# ===========================================================================


async def recovery_sweep(
    pool: AsyncConnectionPool,
    event_queue: asyncio.Queue[int],
    graph_queue: asyncio.Queue[str],
    *,
    event_limit: int | None = None,
    case_limit: int | None = None,
) -> tuple[int, int]:
    """Re-enqueue work the previous process didn't finish.

    Returns `(events_enqueued, cases_enqueued)`.
    """
    event_limit = event_limit or settings.runtime.recovery_sweep_limit
    case_limit = case_limit or settings.runtime.recovery_sweep_limit

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT e.id FROM events e
                WHERE NOT EXISTS (
                    SELECT 1 FROM cases c WHERE c.event_id = e.id
                )
                ORDER BY e.id
                LIMIT %s
                """,
                (event_limit,),
            )
            event_rows = await cur.fetchall()

            await cur.execute(
                """
                SELECT id FROM cases
                WHERE status IN ('pending', 'in_progress')
                ORDER BY created_at
                LIMIT %s
                """,
                (case_limit,),
            )
            case_rows = await cur.fetchall()

    events_enqueued = 0
    for r in event_rows:
        try:
            event_queue.put_nowait(int(r[0]))
            events_enqueued += 1
        except asyncio.QueueFull:
            logger.warning(
                "recovery_sweep: event_queue full at %d items; remaining backlog left in DB",
                events_enqueued,
            )
            break

    cases_enqueued = 0
    for r in case_rows:
        try:
            graph_queue.put_nowait(str(r[0]))
            cases_enqueued += 1
        except asyncio.QueueFull:
            logger.warning(
                "recovery_sweep: graph_queue full at %d items; remaining backlog left in DB",
                cases_enqueued,
            )
            break

    logger.info(
        "recovery_sweep: enqueued %d events, %d cases",
        events_enqueued, cases_enqueued,
    )
    return events_enqueued, cases_enqueued
