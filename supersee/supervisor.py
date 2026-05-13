"""Per-task restart supervisor for the live pipeline.

Each long-running background task (ingestor, scorer loop, graph loop,
scheduler) is wrapped by `run_with_restart`. If the task raises, it logs
the traceback and restarts with exponential backoff (capped at
`settings.runtime.restart_max_backoff_seconds`). A clean return is also
treated as a crash for infinite-loop tasks — the supervisor restarts
immediately.

CancelledError propagates so `asyncio.gather(*tasks, return_exceptions=True)`
in the lifespan can shut down cleanly.

Eng-review hardening: this is the pattern that prevents one blown
coroutine from wedging the whole process.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from supersee.config import settings

logger = logging.getLogger(__name__)


async def run_with_restart(
    task_factory: Callable[[], Awaitable[None]],
    name: str,
    *,
    max_backoff: float | None = None,
    initial_backoff: float = 1.0,
) -> None:
    """Run `task_factory()` forever, restarting on any non-cancel error.

    `task_factory` is a no-arg async callable that returns a coroutine.
    We call it fresh each restart so any setup it does (open sockets,
    construct clients) happens cleanly per attempt.
    """
    max_backoff = max_backoff or settings.runtime.restart_max_backoff_seconds
    delay = initial_backoff
    logger.info("supervisor: %s starting", name)

    while True:
        try:
            await task_factory()
        except asyncio.CancelledError:
            logger.info("supervisor: %s cancelled", name)
            raise
        except Exception:
            logger.exception(
                "supervisor: %s crashed; restarting in %.1fs", name, delay
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            delay = min(delay * 2, max_backoff)
            continue

        # Clean return: unusual for an infinite-loop task. Treat as a
        # crash and restart immediately with the backoff floor.
        logger.warning("supervisor: %s returned cleanly; restarting", name)
        delay = initial_backoff
