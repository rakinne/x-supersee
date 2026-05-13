"""Supersee CLI.

Subcommands:
    supersee serve                            # uvicorn supersee.api:app
    supersee score <fixture>                  # run the scorer over a fixture, print JSON
    supersee investigate <fixture> [--decision approve|reject|escalate]
                                              # full pipeline: scorer + LangGraph + HITL
                                              # (auto-resumes with --decision; default approve)

Subcommands (planned):
    supersee migrate            # apply pending migrations from migrations/
    supersee refresh-ofac       # force-refresh ofac_sdn_crypto table
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig
    from psycopg_pool import AsyncConnectionPool

    from supersee.scorer.score_event import EventData, ScoringResult

app = typer.Typer(
    no_args_is_help=True,
    help="Supersee compliance investigation CLI.",
    add_completion=False,
)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Host to bind."),
    port: int = typer.Option(8000, help="Port to bind."),
    reload: bool = typer.Option(
        False, "--reload", help="Enable uvicorn auto-reload (dev only)."
    ),
) -> None:
    """Run the FastAPI app and the four background tasks via uvicorn."""
    import uvicorn

    uvicorn.run(
        "supersee.api:app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=["supersee"] if reload else None,
    )


@app.command()
def score(
    fixture: Path = typer.Argument(
        ...,
        help="Path to a fixture JSON file with `event` and `context` keys.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print the output."),
) -> None:
    """Run the deterministic scorer over a fixture event and print the result.

    The fixture is self-contained (no DB needed) so reviewers can iterate
    on rule weights and thresholds locally without spinning up the full
    pipeline. The output JSON matches the `cases` row shape: `risk_score`,
    `risk_band`, and `rule_hits` (only firing rules).
    """
    # Local imports so `supersee --help` and `supersee serve` don't pay the
    # scorer/config import cost.
    from supersee.config.scorer import ScorerSettings
    from supersee.scorer.score_event import EventData, ScoringContext, score_event

    data = json.loads(fixture.read_text())
    event_raw = data["event"]
    event = EventData(
        amount_xrp=event_raw.get("amount_xrp"),
        account_src=event_raw["account_src"],
        account_dst=event_raw.get("account_dst"),
        memo_decoded=event_raw.get("memo_decoded"),
    )
    ctx_raw = data.get("context", {})
    ctx = ScoringContext(
        rolling_p99_7d=ctx_raw.get("rolling_p99_7d"),
        known_counterparties=frozenset(ctx_raw.get("known_counterparties", [])),
        recent_outbound_count=ctx_raw.get("recent_outbound_count", 0),
        ofac_addresses=frozenset(ctx_raw.get("ofac_addresses", [])),
        watchlist_addresses=frozenset(ctx_raw.get("watchlist_addresses", [])),
        mixer_addresses=frozenset(ctx_raw.get("mixer_addresses", [])),
    )

    result = score_event(event, ctx, ScorerSettings())
    output: dict[str, object] = {
        "risk_score": result.risk_score,
        "risk_band": result.risk_band,
        "rule_hits": result.to_jsonb(),
    }
    typer.echo(json.dumps(output, indent=2 if pretty else None))


@app.command()
def investigate(
    fixture: Path = typer.Argument(
        ...,
        help="Path to a fixture JSON file with `event` and `context` keys.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    decision: str = typer.Option(
        "approve",
        "--decision",
        help="Analyst decision to auto-resume the graph if it pauses at HITL.",
    ),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print the output."),
) -> None:
    """Drive a fixture event through the full pipeline (scorer + LangGraph + HITL).

    Requires the docker compose stack running (Postgres reachable via
    DATABASE_URL). Inserts the fixture's lookup data, scores the event,
    creates a case row, drives the LangGraph investigation. If the graph
    pauses for HITL, auto-resumes with `--decision`. Prints the final
    state + narrative + audit log.
    """
    import asyncio

    asyncio.run(_run_investigate(fixture, decision, pretty))


async def _run_investigate(fixture: Path, decision: str, pretty: bool) -> None:
    import uuid

    from langgraph.types import Command

    from supersee import db
    from supersee.config.scorer import ScorerSettings
    from supersee.graph.app import close_graph, init_graph
    from supersee.scorer.score_event import (
        EventData,
        ScoringContext,
        score_event,
    )

    raw = json.loads(fixture.read_text())
    event_raw = raw["event"]
    ctx_raw = raw.get("context", {})

    # 1) Boot the stack: pool + migrations + graph (uses default narrative client).
    await db.init_pool()
    await db.apply_migrations()
    pool = db.get_pool()
    await _seed_fixture_lookups(pool, ctx_raw)
    graph = await init_graph(pool)

    try:
        # 2) Score the fixture in-process (the live scorer would do this on
        # the in-process queue handoff; here we run it synchronously).
        event = EventData(
            amount_xrp=event_raw.get("amount_xrp"),
            account_src=event_raw["account_src"],
            account_dst=event_raw.get("account_dst"),
            memo_decoded=event_raw.get("memo_decoded"),
        )
        ctx = ScoringContext(
            rolling_p99_7d=ctx_raw.get("rolling_p99_7d"),
            known_counterparties=frozenset(ctx_raw.get("known_counterparties", [])),
            recent_outbound_count=ctx_raw.get("recent_outbound_count", 0),
            ofac_addresses=frozenset(ctx_raw.get("ofac_addresses", [])),
            watchlist_addresses=frozenset(ctx_raw.get("watchlist_addresses", [])),
            mixer_addresses=frozenset(ctx_raw.get("mixer_addresses", [])),
        )
        score = score_event(event, ctx, ScorerSettings())

        # 3) Insert the event + case row.
        event_id = await _insert_event(pool, event)
        case_id = str(uuid.uuid4())
        await _insert_case(pool, case_id, event_id, score)

        # 4) Drive the graph.
        config: RunnableConfig = {"configurable": {"thread_id": case_id}}
        initial_state = {
            "case_id": case_id,
            "event_id": event_id,
            "account_src": event.account_src,
            "account_dst": event.account_dst,
            "amount_xrp": event.amount_xrp,
            "memo_decoded": event.memo_decoded,
            "risk_band": score.risk_band,
            "rule_hits": score.to_jsonb(),
        }
        result = await graph.ainvoke(initial_state, config)

        # 5) If paused at HITL, resume with the supplied decision.
        snapshot = await graph.aget_state(config)
        if snapshot.next:
            result = await graph.ainvoke(
                Command(
                    resume={
                        "decision": decision,
                        "actor": "supersee-cli",
                        "note": f"auto-resumed via investigate --decision {decision}",
                    }
                ),
                config,
            )

        # 6) Output.
        output = {
            "case_id": case_id,
            "risk_score": score.risk_score,
            "risk_band": score.risk_band,
            "triage_path": result.get("triage_path"),
            "final_status": result.get("final_status"),
            "narrative": result.get("narrative"),
            "audit_log": result.get("audit_log"),
        }
        typer.echo(json.dumps(output, indent=2 if pretty else None, default=str))
    finally:
        await close_graph()
        await db.close_pool()


async def _seed_fixture_lookups(
    pool: AsyncConnectionPool, ctx_raw: dict[str, Any]
) -> None:
    """Insert the fixture's curated addresses so the live graph nodes see them."""
    from supersee import clock

    now = clock.now()
    async with pool.connection() as conn, conn.cursor() as cur:
        for addr in ctx_raw.get("ofac_addresses", []):
            await cur.execute(
                """
                    INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at)
                    VALUES (%s, 'XRP', 'demo-fixture', 'cli://investigate', %s)
                    ON CONFLICT (address) DO NOTHING
                    """,
                (addr, now),
            )
        for addr in ctx_raw.get("watchlist_addresses", []):
            await cur.execute(
                """
                    INSERT INTO watchlist (address, label, risk_tier, tags)
                    VALUES (%s, 'demo-fixture', 'info', ARRAY['demo'])
                    ON CONFLICT (address) DO NOTHING
                    """,
                (addr,),
            )
        for addr in ctx_raw.get("mixer_addresses", []):
            await cur.execute(
                """
                    INSERT INTO mixer_addresses (address, label, source)
                    VALUES (%s, 'demo-fixture', 'cli://investigate')
                    ON CONFLICT (address) DO NOTHING
                    """,
                (addr,),
            )


async def _insert_event(
    pool: AsyncConnectionPool, event: EventData
) -> int:
    import uuid as _uuid

    from supersee import clock

    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO events (
                    tx_hash, ledger_index, validated_at, tx_type,
                    account_src, account_dst, amount_xrp, memo_decoded, raw_json
                )
                VALUES (%s, 1, %s, 'Payment', %s, %s, %s, %s, '{}'::jsonb)
                RETURNING id
                """,
            (
                _uuid.uuid4().hex,
                clock.now(),
                event.account_src,
                event.account_dst,
                event.amount_xrp,
                event.memo_decoded,
            ),
        )
        row = await cur.fetchone()
        assert row is not None  # RETURNING id always yields a row
        return int(row[0])


async def _insert_case(
    pool: AsyncConnectionPool,
    case_id: str,
    event_id: int,
    score: ScoringResult,
) -> None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO cases (id, event_id, status, risk_score, risk_band, rule_hits)
                VALUES (%s, %s, 'in_progress', %s, %s, %s::jsonb)
                """,
            (
                case_id,
                event_id,
                score.risk_score,
                score.risk_band,
                json.dumps(score.to_jsonb()),
            ),
        )


if __name__ == "__main__":
    app()
