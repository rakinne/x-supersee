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

from supersee import __version__, db, ingestor, pipeline, scheduler, supervisor
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
    _configure_logging()
    logger.info("supersee %s starting", __version__)

    await db.init_pool()
    await db.apply_migrations()
    pool = db.get_pool()
    await graph_app.init_graph(pool)

    # In-process queues. Bounded so backpressure manifests as a log
    # warning instead of unbounded memory growth.
    event_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=10_000)
    graph_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1_000)
    graph_sem = asyncio.Semaphore(settings.runtime.langgraph_concurrency)

    # Crash-recovery: re-enqueue work the previous process didn't finish.
    # Runs BEFORE we start the consumers so the backlog is ready.
    events_recovered, cases_recovered = await pipeline.recovery_sweep(
        pool, event_queue, graph_queue
    )
    if events_recovered or cases_recovered:
        logger.info(
            "recovery_sweep done: %d events + %d cases re-enqueued",
            events_recovered, cases_recovered,
        )

    # Four background tasks under the restart supervisor (eng-review hardening).
    tasks = [
        asyncio.create_task(
            supervisor.run_with_restart(
                lambda: ingestor.run_ingestor(pool, event_queue),
                "ingestor",
            ),
            name="ingestor",
        ),
        asyncio.create_task(
            supervisor.run_with_restart(
                lambda: pipeline.run_scorer_loop(pool, event_queue, graph_queue),
                "scorer",
            ),
            name="scorer",
        ),
        asyncio.create_task(
            supervisor.run_with_restart(
                lambda: pipeline.run_graph_loop(pool, graph_queue, graph_sem),
                "graph",
            ),
            name="graph",
        ),
        asyncio.create_task(
            supervisor.run_with_restart(
                lambda: scheduler.run_scheduler(pool),
                "scheduler",
            ),
            name="scheduler",
        ),
    ]

    try:
        yield
    finally:
        logger.info("supersee shutting down; cancelling background tasks")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
