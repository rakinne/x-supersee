"""DB-backed scoring context: lookup snapshots, per-event context, P99.

Public surface:

    await get_lookups(pool)                  -> Lookups   (TTL-cached singleton)
    await build_context(pool, event, lookups, cfg) -> ScoringContext
    await record_event(pool, event)          -> None      (post-scoring upsert)
    await recompute_p99_if_stale(pool, ...)  -> None      (lazy + single-flighted)
    invalidate_lookups()                                   (test helper)

`Lookups` holds the curated OFAC / watchlist / mixer sets. The cache is
process-singleton with a TTL (default 5 minutes) — small enough that
operator updates land quickly, large enough that we don't hammer the DB
on every event.

`build_context` is the seam between "the scorer pipeline" and the DB:
it reads `account_history`, counts recent outbound Payments via
`clock.now()` (so fixture replays are deterministic), and packages
everything into a `ScoringContext` that the rules consume.

`record_event` runs AFTER scoring so this event's stats don't influence
its own score. Order matters.

`recompute_p99_if_stale` is lazy — only refires when the cached P99 is
older than `p99_recompute_age_minutes`. Single-flighted via
`pg_try_advisory_xact_lock(hashtext(account))` so concurrent scorer
workers don't race on the same account.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from psycopg_pool import AsyncConnectionPool

from supersee import clock
from supersee.config.scorer import ScorerSettings
from supersee.scorer.score_event import EventData, ScoringContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Lookups:
    """Snapshot of the curated address sets used by the rules."""

    ofac_addresses: frozenset[str]
    watchlist_addresses: frozenset[str]
    mixer_addresses: frozenset[str]
    loaded_at: datetime


# Module-level TTL cache. Singleton per process, guarded by an asyncio lock
# so concurrent first-callers don't both hit the DB.
_lookups_cache: Lookups | None = None
_lookups_lock = asyncio.Lock()
_DEFAULT_LOOKUPS_TTL_SECONDS = 300


def invalidate_lookups() -> None:
    """Drop the cached lookups snapshot. Tests and forced-refresh callers use this."""
    global _lookups_cache
    _lookups_cache = None


async def _load_lookups_from_db(pool: AsyncConnectionPool) -> Lookups:
    """Single round-trip per curated table. All sets are small (<<10k rows)."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT address FROM ofac_sdn_crypto")
        ofac = frozenset(row[0] for row in await cur.fetchall())

        await cur.execute("SELECT address FROM watchlist")
        watch = frozenset(row[0] for row in await cur.fetchall())

        await cur.execute("SELECT address FROM mixer_addresses")
        mixer = frozenset(row[0] for row in await cur.fetchall())

    return Lookups(
        ofac_addresses=ofac,
        watchlist_addresses=watch,
        mixer_addresses=mixer,
        loaded_at=clock.now(),
    )


async def get_lookups(
    pool: AsyncConnectionPool,
    *,
    ttl_seconds: int = _DEFAULT_LOOKUPS_TTL_SECONDS,
) -> Lookups:
    """Return a cached Lookups snapshot, refreshing if it's older than `ttl_seconds`."""
    global _lookups_cache
    cached = _lookups_cache
    if cached is not None and (clock.now() - cached.loaded_at).total_seconds() < ttl_seconds:
        return cached

    async with _lookups_lock:
        # Double-check under the lock — another coroutine may have refreshed.
        cached = _lookups_cache
        if cached is not None and (clock.now() - cached.loaded_at).total_seconds() < ttl_seconds:
            return cached
        fresh = await _load_lookups_from_db(pool)
        _lookups_cache = fresh
        logger.debug(
            "lookups refreshed: ofac=%d watchlist=%d mixer=%d",
            len(fresh.ofac_addresses),
            len(fresh.watchlist_addresses),
            len(fresh.mixer_addresses),
        )
        return fresh


async def build_context(
    pool: AsyncConnectionPool,
    event: EventData,
    lookups: Lookups,
    cfg: ScorerSettings,
) -> ScoringContext:
    """Assemble per-event ScoringContext from DB state + caller-supplied lookups."""
    velocity_window_start = clock.now() - timedelta(
        minutes=cfg.thresholds.velocity_burst_window_minutes
    )

    async with pool.connection() as conn, conn.cursor() as cur:
        # account_history for the source account
        await cur.execute(
            """
                SELECT rolling_p99_7d, known_counterparties
                FROM account_history
                WHERE account = %s
                """,
            (event.account_src,),
        )
        row = await cur.fetchone()
        if row is None:
            rolling_p99: float | None = None
            known_counterparties: frozenset[str] = frozenset()
        else:
            rolling_p99 = float(row[0]) if row[0] is not None else None
            known_counterparties = frozenset(row[1] or ())

        # recent outbound Payment count over the velocity window
        await cur.execute(
            """
                SELECT COUNT(*) FROM events
                WHERE account_src = %s
                  AND tx_type = 'Payment'
                  AND validated_at >= %s
                """,
            (event.account_src, velocity_window_start),
        )
        count_row = await cur.fetchone()
        recent_outbound_count = int(count_row[0]) if count_row is not None else 0

    return ScoringContext(
        rolling_p99_7d=rolling_p99,
        known_counterparties=known_counterparties,
        recent_outbound_count=recent_outbound_count,
        ofac_addresses=lookups.ofac_addresses,
        watchlist_addresses=lookups.watchlist_addresses,
        mixer_addresses=lookups.mixer_addresses,
    )


async def record_event(pool: AsyncConnectionPool, event: EventData) -> None:
    """Upsert account_history for the source account. Call AFTER scoring.

    Increments `total_seen`, sets `last_seen_at` to `clock.now()`. If the
    event has a destination, appends it to `known_counterparties` via
    `array_remove(arr, dst) || ARRAY[dst]` — that pattern dedupes the
    destination and moves it to the most-recent position. The cap-at-1000
    trigger from migration 001 handles overflow.
    """
    now = clock.now()
    async with pool.connection() as conn, conn.cursor() as cur:
        # Single statement: insert-or-update with conditional counterparty append.
        # EXCLUDED.known_counterparties is either '{}' (no dst) or ARRAY[dst].
        await cur.execute(
            """
                INSERT INTO account_history (
                    account, total_seen, last_seen_at, known_counterparties
                )
                VALUES (
                    %s,
                    1,
                    %s,
                    CASE
                        WHEN %s::text IS NULL THEN '{}'::text[]
                        ELSE ARRAY[%s::text]
                    END
                )
                ON CONFLICT (account) DO UPDATE SET
                    total_seen = account_history.total_seen + 1,
                    last_seen_at = EXCLUDED.last_seen_at,
                    known_counterparties = CASE
                        WHEN array_length(EXCLUDED.known_counterparties, 1) IS NULL THEN
                            account_history.known_counterparties
                        ELSE
                            array_remove(
                                account_history.known_counterparties,
                                EXCLUDED.known_counterparties[1]
                            ) || EXCLUDED.known_counterparties
                    END
                """,
            (event.account_src, now, event.account_dst, event.account_dst),
        )


async def recompute_p99_if_stale(
    pool: AsyncConnectionPool,
    account: str,
    cfg: ScorerSettings,
) -> bool:
    """Refresh `account_history.rolling_p99_7d` if older than the configured TTL.

    Returns True if a recompute actually ran, False if we skipped (fresh
    enough OR another worker holds the advisory lock).
    """
    staleness_cutoff = clock.now() - timedelta(minutes=cfg.p99_recompute_age_minutes)

    # Cheap check: if the cached value is fresh, no need to acquire the lock.
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT rolling_p99_at FROM account_history WHERE account = %s",
            (account,),
        )
        row = await cur.fetchone()
        if row is not None and row[0] is not None and row[0] > staleness_cutoff:
            return False

    # Stale (or missing). Try to take the per-account advisory lock and recompute.
    async with pool.connection() as conn:
        await conn.set_autocommit(False)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                    (account,),
                )
                lock_row = await cur.fetchone()
                if not (lock_row and lock_row[0]):
                    await conn.rollback()
                    return False  # another worker is doing it

                # Re-check under the lock to avoid redundant work.
                await cur.execute(
                    "SELECT rolling_p99_at FROM account_history WHERE account = %s",
                    (account,),
                )
                row = await cur.fetchone()
                if row is not None and row[0] is not None and row[0] > staleness_cutoff:
                    await conn.rollback()
                    return False

                lookback_start = clock.now() - timedelta(days=cfg.p99_lookback_days)
                await cur.execute(
                    """
                    SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY amount_xrp)
                    FROM events
                    WHERE account_src = %s
                      AND validated_at >= %s
                      AND amount_xrp IS NOT NULL
                    """,
                    (account, lookback_start),
                )
                p99_row = await cur.fetchone()
                new_p99 = p99_row[0] if p99_row is not None else None

                await cur.execute(
                    """
                    INSERT INTO account_history (account, rolling_p99_7d, rolling_p99_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (account) DO UPDATE SET
                        rolling_p99_7d = EXCLUDED.rolling_p99_7d,
                        rolling_p99_at = EXCLUDED.rolling_p99_at
                    """,
                    (account, new_p99, clock.now()),
                )
                await conn.commit()
                return True
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.set_autocommit(True)
