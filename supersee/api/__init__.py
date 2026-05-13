"""FastAPI application entrypoint.

The lifespan opens the Postgres pool, applies pending migrations,
compiles the LangGraph investigation app against the shared
AsyncPostgresSaver, and launches the background scheduler. The case
routes (`/cases`, `/cases/{id}`, `/cases/{id}/decision`) live in
`supersee.api.routes`.

The future ingestor + per-task restart supervisor (eng-review hardening)
will plug into this lifespan once they land.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from supersee import __version__, db, scheduler
from supersee.config import settings
from supersee.graph import app as graph_app

logger = logging.getLogger(__name__)

# Templates directory lives alongside this package so the import works in
# both the bind-mounted dev container and a frozen production image.
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


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
    # TODO: gate on actual liveness of the background tasks once they exist.
    return {"status": "ok", "version": __version__}


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    """Redirect the bare host to the case queue — the only meaningful UI."""
    return RedirectResponse(url="/cases", status_code=303)


# Route registration. Done after `app` exists so route modules can
# import `app` if they want — but for clarity, routes.py uses a Router.
from supersee.api import routes  # noqa: E402

app.include_router(routes.router)
