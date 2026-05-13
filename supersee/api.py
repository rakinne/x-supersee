"""FastAPI application entrypoint.

The full implementation wires the `lifespan` hook to open the Postgres pool,
apply pending migrations, compile the LangGraph app against an
`AsyncPostgresSaver`, and launch the four background tasks (ingestor, scorer,
langgraph_app, scheduler) under a `run_with_restart` supervisor.

This stub exposes only `/healthz` so the Dockerfile's HEALTHCHECK passes and
`docker compose up` reports the container as healthy while the rest of the
package is scaffolded.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from supersee import __version__, db, scheduler
from supersee.config import settings
from supersee.graph import app as graph_app

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=settings.runtime.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # TODO: launch ingestor / scorer under run_with_restart with bounded
    # concurrency via asyncio.Semaphore(settings.runtime.langgraph_concurrency).
    _configure_logging()
    logger.info("supersee %s starting", __version__)
    await db.init_pool()
    await db.apply_migrations()
    pool = db.get_pool()

    # Compile the LangGraph investigation app against the shared pool.
    # The checkpointer (AsyncPostgresSaver) creates its own tables via
    # .setup() on first call; idempotent thereafter.
    await graph_app.init_graph(pool)

    # Background scheduler: hourly tick for OFAC refresh + artifact retention.
    # Wired directly here for now; the per-task restart supervisor (eng-review
    # hardening) will wrap all four background tasks once the others land.
    scheduler_task = asyncio.create_task(
        scheduler.run_scheduler(pool), name="scheduler"
    )

    try:
        yield
    finally:
        logger.info("supersee shutting down")
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        await graph_app.close_graph()
        await db.close_pool()


app = FastAPI(title="Supersee", version=__version__, lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    # TODO: gate on actual liveness of the four background tasks once they
    # exist. For now, container-up == healthy.
    return {"status": "ok", "version": __version__}
