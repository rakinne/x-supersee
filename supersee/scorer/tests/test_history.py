"""Integration tests for the DB-backed scoring context.

These tests touch the real Postgres provided by `docker compose` (the
container that runs them has the same DATABASE_URL the app uses). Each
test truncates the relevant tables before running and invalidates the
`Lookups` cache so the module-level singleton doesn't bleed state.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from psycopg_pool import AsyncConnectionPool

from supersee import clock
from supersee.config.scorer import ScorerSettings
from supersee.scorer import history
from supersee.scorer.score_event import EventData

# These tests need a real database. Skip if DATABASE_URL is the fake one
# the unit-test conftest sets (i.e., we're not running inside a stack).
_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"
pytestmark = pytest.mark.skipif(
    _IS_FAKE_DSN, reason="integration tests need a real Postgres DSN"
)


@pytest.fixture
async def pool() -> AsyncIterator[AsyncConnectionPool]:
    p = AsyncConnectionPool(
        conninfo=_DSN, min_size=1, max_size=4, kwargs={"autocommit": True}, open=False
    )
    await p.open()
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture(autouse=True)
async def clean_db(pool: AsyncConnectionPool) -> AsyncIterator[None]:
    """Truncate per-test state. RESTART IDENTITY keeps sequences predictable."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                TRUNCATE events, account_history, cases, case_artifacts,
                         ofac_sdn_crypto, watchlist, mixer_addresses,
                         exchange_labels, ingestor_cursor, enrichment_cache,
                         rate_limits
                RESTART IDENTITY CASCADE
                """
        )
    history.invalidate_lookups()
    yield


@pytest.fixture
def cfg() -> ScorerSettings:
    return ScorerSettings()


async def _insert_event(
    pool: AsyncConnectionPool,
    account_src: str,
    account_dst: str | None,
    amount_xrp: float | None,
    validated_at: datetime,
    tx_type: str = "Payment",
) -> None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO events (
                    tx_hash, ledger_index, validated_at, tx_type,
                    account_src, account_dst, amount_xrp, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
            (
                uuid.uuid4().hex,
                1,
                validated_at,
                tx_type,
                account_src,
                account_dst,
                amount_xrp,
            ),
        )


# ===========================================================================
# Lookups TTL cache
# ===========================================================================

class TestLookups:
    async def test_initial_load_is_empty(self, pool: AsyncConnectionPool) -> None:
        lookups = await history.get_lookups(pool, ttl_seconds=0)
        assert lookups.ofac_addresses == frozenset()
        assert lookups.watchlist_addresses == frozenset()
        assert lookups.mixer_addresses == frozenset()

    async def test_load_after_seeding(self, pool: AsyncConnectionPool) -> None:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at) "
                    "VALUES ('rOFAC1', 'XRP', 'src-1', 'http://x', NOW())"
                )
                await cur.execute(
                    "INSERT INTO watchlist (address, label, risk_tier) VALUES ('rWatch1', 'demo', 'info')"
                )
                await cur.execute(
                    "INSERT INTO mixer_addresses (address, label, source) VALUES ('rMix1', 'demo', 'curated')"
                )

        lookups = await history.get_lookups(pool, ttl_seconds=0)
        assert "rOFAC1" in lookups.ofac_addresses
        assert "rWatch1" in lookups.watchlist_addresses
        assert "rMix1" in lookups.mixer_addresses

    async def test_cache_hit_within_ttl(self, pool: AsyncConnectionPool) -> None:
        first = await history.get_lookups(pool, ttl_seconds=300)
        # Mutate the DB; the cached snapshot should NOT pick it up.
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO watchlist (address, label) VALUES ('rNew', 'after-cache')"
            )
        second = await history.get_lookups(pool, ttl_seconds=300)
        assert second is first, "should return identical cached object"
        assert "rNew" not in second.watchlist_addresses

    async def test_invalidate_forces_reload(self, pool: AsyncConnectionPool) -> None:
        await history.get_lookups(pool, ttl_seconds=300)
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO watchlist (address, label) VALUES ('rNew', 'post-invalidate')"
            )
        history.invalidate_lookups()
        refreshed = await history.get_lookups(pool, ttl_seconds=300)
        assert "rNew" in refreshed.watchlist_addresses


# ===========================================================================
# build_context
# ===========================================================================

class TestBuildContext:
    async def test_fresh_account_returns_empty_state(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        event = EventData(amount_xrp=100.0, account_src="rNew", account_dst="rDst", memo_decoded=None)
        lookups = await history.get_lookups(pool, ttl_seconds=0)
        ctx = await history.build_context(pool, event, lookups, cfg)

        assert ctx.rolling_p99_7d is None
        assert ctx.known_counterparties == frozenset()
        assert ctx.recent_outbound_count == 0

    async def test_existing_account_history_loaded(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                    INSERT INTO account_history (
                        account, total_seen, last_seen_at, rolling_p99_7d,
                        rolling_p99_at, known_counterparties
                    )
                    VALUES (%s, 50, NOW(), 80000.0, NOW(), ARRAY['rKnown1', 'rKnown2'])
                    """,
                ("rExisting",),
            )

        event = EventData(amount_xrp=100.0, account_src="rExisting", account_dst="rNewDst", memo_decoded=None)
        lookups = await history.get_lookups(pool, ttl_seconds=0)
        ctx = await history.build_context(pool, event, lookups, cfg)

        assert ctx.rolling_p99_7d == 80000.0
        assert ctx.known_counterparties == frozenset({"rKnown1", "rKnown2"})

    async def test_recent_outbound_count_respects_window(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        # Three events inside the 5-minute window, two outside.
        await _insert_event(pool, "rSrc", "rA", 100.0, anchor - timedelta(minutes=1))
        await _insert_event(pool, "rSrc", "rB", 100.0, anchor - timedelta(minutes=3))
        await _insert_event(pool, "rSrc", "rC", 100.0, anchor - timedelta(minutes=4, seconds=59))
        await _insert_event(pool, "rSrc", "rD", 100.0, anchor - timedelta(minutes=5, seconds=1))
        await _insert_event(pool, "rSrc", "rE", 100.0, anchor - timedelta(minutes=10))

        with clock.freeze_time(anchor):
            event = EventData(amount_xrp=100.0, account_src="rSrc", account_dst="rNew", memo_decoded=None)
            lookups = await history.get_lookups(pool, ttl_seconds=0)
            ctx = await history.build_context(pool, event, lookups, cfg)

        assert ctx.recent_outbound_count == 3

    async def test_recent_count_filters_to_payments(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        # Two Payments + one OfferCreate within window; only Payments count.
        await _insert_event(pool, "rSrc", "rA", 100.0, anchor - timedelta(minutes=1), tx_type="Payment")
        await _insert_event(pool, "rSrc", "rB", 100.0, anchor - timedelta(minutes=2), tx_type="Payment")
        await _insert_event(pool, "rSrc", None, None, anchor - timedelta(minutes=1), tx_type="OfferCreate")

        with clock.freeze_time(anchor):
            event = EventData(amount_xrp=100.0, account_src="rSrc", account_dst="rNew", memo_decoded=None)
            lookups = await history.get_lookups(pool, ttl_seconds=0)
            ctx = await history.build_context(pool, event, lookups, cfg)

        assert ctx.recent_outbound_count == 2


# ===========================================================================
# record_event
# ===========================================================================

class TestRecordEvent:
    async def test_new_account_creates_row(self, pool: AsyncConnectionPool) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        event = EventData(amount_xrp=100.0, account_src="rNew", account_dst="rDst", memo_decoded=None)

        with clock.freeze_time(anchor):
            await history.record_event(pool, event)

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT total_seen, last_seen_at, known_counterparties "
                "FROM account_history WHERE account = %s",
                ("rNew",),
            )
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == anchor
        assert row[2] == ["rDst"]

    async def test_existing_account_increments_and_appends(
        self, pool: AsyncConnectionPool
    ) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (account, total_seen, last_seen_at, known_counterparties)
                    VALUES (%s, 5, %s, ARRAY['rOld1', 'rOld2'])
                    """,
                    ("rExisting", anchor - timedelta(hours=1)),
                )

        event = EventData(amount_xrp=100.0, account_src="rExisting", account_dst="rNew", memo_decoded=None)
        with clock.freeze_time(anchor):
            await history.record_event(pool, event)

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT total_seen, last_seen_at, known_counterparties "
                "FROM account_history WHERE account = %s",
                ("rExisting",),
            )
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 6
        assert row[1] == anchor
        assert row[2] == ["rOld1", "rOld2", "rNew"]

    async def test_repeat_destination_moves_to_end(
        self, pool: AsyncConnectionPool
    ) -> None:
        """The array_remove + append pattern dedupes and refreshes recency."""
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (account, total_seen, last_seen_at, known_counterparties)
                    VALUES (%s, 3, NOW(), ARRAY['rA', 'rB', 'rC'])
                    """,
                    ("rExisting",),
                )

        event = EventData(amount_xrp=100.0, account_src="rExisting", account_dst="rA", memo_decoded=None)
        await history.record_event(pool, event)

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT known_counterparties FROM account_history WHERE account = %s",
                ("rExisting",),
            )
            row = await cur.fetchone()
        # rA was first; after record, it's last (most recent).
        assert row is not None
        assert row[0] == ["rB", "rC", "rA"]

    async def test_no_destination_leaves_counterparties_alone(
        self, pool: AsyncConnectionPool
    ) -> None:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (account, total_seen, last_seen_at, known_counterparties)
                    VALUES (%s, 1, NOW(), ARRAY['rA', 'rB'])
                    """,
                    ("rExisting",),
                )

        event = EventData(amount_xrp=None, account_src="rExisting", account_dst=None, memo_decoded=None)
        await history.record_event(pool, event)

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT total_seen, known_counterparties FROM account_history WHERE account = %s",
                ("rExisting",),
            )
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 2
        assert row[1] == ["rA", "rB"]


# ===========================================================================
# recompute_p99_if_stale
# ===========================================================================

class TestRecomputeP99:
    async def test_skips_when_fresh(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (account, total_seen, rolling_p99_7d, rolling_p99_at)
                    VALUES (%s, 100, 50000.0, %s)
                    """,
                    ("rFresh", anchor - timedelta(minutes=5)),
                )

        with clock.freeze_time(anchor):
            did_recompute = await history.recompute_p99_if_stale(pool, "rFresh", cfg)
        assert did_recompute is False

    async def test_recomputes_when_stale(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        # Stale P99: cached 24h ago.
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_history (account, total_seen, rolling_p99_7d, rolling_p99_at)
                    VALUES (%s, 100, 999.0, %s)
                    """,
                    ("rStale", anchor - timedelta(hours=24)),
                )

        # Seed events to drive a real P99 computation.
        for amount in [100.0, 200.0, 500.0, 1000.0, 9000.0]:
            await _insert_event(
                pool, "rStale", "rDst", amount, anchor - timedelta(days=1)
            )

        with clock.freeze_time(anchor):
            did_recompute = await history.recompute_p99_if_stale(pool, "rStale", cfg)

        assert did_recompute is True
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT rolling_p99_7d, rolling_p99_at FROM account_history WHERE account = %s",
                ("rStale",),
            )
            row = await cur.fetchone()
        assert row is not None
        # P99 of {100, 200, 500, 1000, 9000} via percentile_cont is ~7400.
        assert row[0] is not None
        assert float(row[0]) > 5000.0
        assert row[1] == anchor

    async def test_missing_account_creates_row(
        self, pool: AsyncConnectionPool, cfg: ScorerSettings
    ) -> None:
        """No events, no prior row: P99 ends up NULL, but rolling_p99_at is set."""
        anchor = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        with clock.freeze_time(anchor):
            did_recompute = await history.recompute_p99_if_stale(pool, "rGhost", cfg)
        assert did_recompute is True

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT rolling_p99_7d, rolling_p99_at FROM account_history WHERE account = %s",
                ("rGhost",),
            )
            row = await cur.fetchone()
        assert row is not None
        assert row[0] is None  # No events → percentile_cont returns NULL
        assert row[1] == anchor
