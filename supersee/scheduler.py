"""Background scheduler: periodic OFAC refresh + case_artifacts retention.

Runs as one of the four async tasks in the FastAPI lifespan. Two jobs:

1. **OFAC refresh** — if `MAX(fetched_at) FROM ofac_sdn_crypto` is older
   than `enrichment.ofac_refresh_age_hours`, kick off a fetch. The fetch
   itself is retry-bounded; on terminal failure it logs and leaves the
   existing rows intact.
2. **Artifact retention** — delete `case_artifacts` older than
   `runtime.case_artifact_retention_days`. The hosted-demo case where
   this matters is in TODOS.md; on the MVP timescale it's a no-op.

The loop ticks once an hour. Each iteration is wrapped so a transient
failure in one job doesn't kill the loop or take down the other job.
The future per-task restart supervisor (eng-review hardening) will
replace the direct `asyncio.create_task` wiring in `api.py`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from psycopg_pool import AsyncConnectionPool

from supersee import clock
from supersee.config import settings
from supersee.enrichment.ofac import get_last_fetched_at, refresh_ofac

logger = logging.getLogger(__name__)

_TICK_INTERVAL_SECONDS = 60 * 60  # hourly cadence; the work is age-gated below


async def maybe_refresh_ofac(pool: AsyncConnectionPool) -> bool:
    """Trigger an OFAC refresh if the cached data is older than the threshold.

    Returns True if a refresh actually ran, False if we skipped because
    the data was fresh.
    """
    last = await get_last_fetched_at(pool)
    threshold = clock.now() - timedelta(hours=settings.enrichment.ofac_refresh_age_hours)
    if last is not None and last > threshold:
        return False
    logger.info(
        "ofac stale (last_fetched_at=%s, threshold=%s); refreshing",
        last.isoformat() if last else "never",
        threshold.isoformat(),
    )
    await refresh_ofac(pool, settings.enrichment)
    return True


async def maybe_cleanup_artifacts(pool: AsyncConnectionPool) -> int:
    """Delete `case_artifacts` rows older than the retention window."""
    cutoff = clock.now() - timedelta(days=settings.runtime.case_artifact_retention_days)
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM case_artifacts WHERE created_at < %s",
            (cutoff,),
        )
        deleted = cur.rowcount
    if deleted and deleted > 0:
        logger.info(
            "artifact retention: deleted %d rows older than %s",
            deleted,
            cutoff.isoformat(),
        )
    return deleted or 0


async def run_scheduler(pool: AsyncConnectionPool) -> None:
    """Forever loop. Cancellation on app shutdown returns cleanly."""
    logger.info(
        "scheduler started (tick=%ds, ofac_age_threshold=%dh, artifact_retention=%dd)",
        _TICK_INTERVAL_SECONDS,
        settings.enrichment.ofac_refresh_age_hours,
        settings.runtime.case_artifact_retention_days,
    )
    try:
        while True:
            try:
                await maybe_refresh_ofac(pool)
            except Exception:
                logger.exception("scheduler: maybe_refresh_ofac failed")
            try:
                await maybe_cleanup_artifacts(pool)
            except Exception:
                logger.exception("scheduler: maybe_cleanup_artifacts failed")
            await asyncio.sleep(_TICK_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("scheduler stopping")
        raise
