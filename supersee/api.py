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

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from supersee import __version__

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # TODO: open Postgres pool, run migrations, init LangGraph checkpointer,
    # launch ingestor / scorer / langgraph_app / scheduler under
    # run_with_restart with bounded concurrency via asyncio.Semaphore(4).
    logger.info("supersee %s starting", __version__)
    try:
        yield
    finally:
        logger.info("supersee shutting down")


app = FastAPI(title="Supersee", version=__version__, lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    # TODO: gate on actual liveness of the four background tasks once they
    # exist. For now, container-up == healthy.
    return {"status": "ok", "version": __version__}
