"""Graph compile + lifecycle.

Public surface:

    build_graph(narrative_client)        -> CompiledStateGraph (no checkpointer)
    await init_graph(pool, narrative_client=None) -> CompiledStateGraph
    await close_graph()
    get_graph()                          -> the module-singleton compiled graph
    get_checkpointer()                   -> the AsyncPostgresSaver instance

`init_graph` is what the FastAPI lifespan calls after `db.init_pool()`
and `db.apply_migrations()`. It wires `AsyncPostgresSaver` against the
shared connection pool so the checkpointer and the rest of the app
share one pool (no double-pool).

`build_graph` is exposed for tests that want to inject a non-default
NarrativeClient (e.g., StaticNarrativeClient for deterministic outcomes,
FailingNarrativeClient to drive the canned-fallback path).
"""

from __future__ import annotations

import logging

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from psycopg_pool import AsyncConnectionPool

from supersee.config import settings
from supersee.graph.narrative import NarrativeClient, get_narrative_client
from supersee.graph.nodes import (
    enrich_external,
    fetch_context,
    hitl_pause,
    make_build_narrative,
    record_outcome,
    triage_branch,
    triage_router,
)
from supersee.graph.state import InvestigationState

logger = logging.getLogger(__name__)


def build_graph(narrative_client: NarrativeClient) -> StateGraph:
    """Build (but don't compile) the six-node investigation graph.

    The narrative client is closure-bound into build_narrative so tests
    can swap in StaticNarrativeClient / FailingNarrativeClient without
    monkeypatching module state.
    """
    g = StateGraph(InvestigationState)
    g.add_node("fetch_context", fetch_context)
    g.add_node("enrich_external", enrich_external)
    g.add_node("build_narrative", make_build_narrative(narrative_client))
    g.add_node("triage_branch", triage_branch)
    g.add_node("hitl_pause", hitl_pause)
    g.add_node("record_outcome", record_outcome)

    g.add_edge(START, "fetch_context")
    g.add_edge("fetch_context", "enrich_external")
    g.add_edge("enrich_external", "build_narrative")
    g.add_edge("build_narrative", "triage_branch")
    g.add_conditional_edges(
        "triage_branch",
        triage_router,
        {
            "record_outcome": "record_outcome",
            "hitl_pause": "hitl_pause",
        },
    )
    g.add_edge("hitl_pause", "record_outcome")
    g.add_edge("record_outcome", END)
    return g


# ===========================================================================
# Module-level singletons (driven by the FastAPI lifespan)
# ===========================================================================

_compiled_graph: CompiledStateGraph | None = None
_checkpointer: AsyncPostgresSaver | None = None
_narrative_client: NarrativeClient | None = None


async def init_graph(
    pool: AsyncConnectionPool,
    narrative_client: NarrativeClient | None = None,
) -> CompiledStateGraph:
    """Open the AsyncPostgresSaver against `pool`, compile, store singletons.

    Idempotent: calling twice is a no-op. The shared pool means we never
    spawn a second pool just for the checkpointer.
    """
    global _compiled_graph, _checkpointer, _narrative_client

    if _compiled_graph is not None:
        return _compiled_graph

    nc = narrative_client or get_narrative_client(
        runtime=settings.runtime, enrichment=settings.enrichment
    )

    saver = AsyncPostgresSaver(pool)  # type: ignore[arg-type]
    await saver.setup()  # idempotent; creates checkpoints + checkpoint_writes + checkpoint_blobs

    graph = build_graph(nc).compile(checkpointer=saver)

    _checkpointer = saver
    _compiled_graph = graph
    _narrative_client = nc

    logger.info("graph compiled (narrative_client=%s)", type(nc).__name__)
    return graph


async def close_graph() -> None:
    """Tear down singletons. The pool is owned by db.py — do not close it here."""
    global _compiled_graph, _checkpointer, _narrative_client
    _compiled_graph = None
    _checkpointer = None
    _narrative_client = None


def get_graph() -> CompiledStateGraph:
    if _compiled_graph is None:
        raise RuntimeError("graph not initialized; call init_graph() first")
    return _compiled_graph


def get_checkpointer() -> AsyncPostgresSaver:
    if _checkpointer is None:
        raise RuntimeError("checkpointer not initialized; call init_graph() first")
    return _checkpointer


def get_narrative_client_singleton() -> NarrativeClient:
    if _narrative_client is None:
        raise RuntimeError("narrative client not initialized; call init_graph() first")
    return _narrative_client
