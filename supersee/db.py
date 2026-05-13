"""Async Postgres pool + migrations runner.

The pool is a module-level singleton opened by the FastAPI lifespan in
`supersee.api`. Background tasks and the LangGraph checkpointer share it.
Migrations are plain `.sql` files in `migrations/` applied in lexicographic
order at startup; applied versions are recorded in `schema_migrations`.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from psycopg_pool import AsyncConnectionPool

from supersee.config import settings

logger = logging.getLogger(__name__)

# Located at <repo>/migrations/, next to the supersee package. Overridable
# via env so tests can point at a fixture directory.
_DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
MIGRATIONS_DIR = Path(os.environ.get("SUPERSEE_MIGRATIONS_DIR", str(_DEFAULT_MIGRATIONS_DIR)))

# Bootstrap DDL for the migrations-tracking table itself. Not part of any
# migration file because the runner needs it to exist before it can read it.
_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Arbitrary process-wide lock key. Held per-transaction during each migration
# apply so two booting processes serialize cleanly instead of racing.
_ADVISORY_LOCK_KEY = 4242

_pool: AsyncConnectionPool | None = None


async def init_pool(
    dsn: str | None = None,
    *,
    min_size: int | None = None,
    max_size: int | None = None,
) -> AsyncConnectionPool:
    """Open the module-level pool. Idempotent.

    `autocommit=True` matches what `langgraph-checkpoint-postgres` expects
    from connections it borrows. Migration apply flips autocommit off for
    the duration of each migration's transaction. Sizes default to
    `settings.runtime.pool_min_size` / `pool_max_size`.
    """
    global _pool
    if _pool is not None:
        return _pool
    pool = AsyncConnectionPool(
        conninfo=dsn or settings.runtime.database_url,
        min_size=min_size if min_size is not None else settings.runtime.pool_min_size,
        max_size=max_size if max_size is not None else settings.runtime.pool_max_size,
        kwargs={"autocommit": True},
        open=False,
    )
    await pool.open()
    _pool = pool
    logger.info("postgres pool opened (max=%d)", pool.max_size)
    return pool


async def close_pool() -> None:
    """Close the module-level pool. Idempotent."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None
    logger.info("postgres pool closed")


def get_pool() -> AsyncConnectionPool:
    """Return the initialized pool, or raise if init_pool() hasn't run."""
    if _pool is None:
        raise RuntimeError("Postgres pool not initialized; call init_pool() first")
    return _pool


def _discover_migrations() -> list[Path]:
    """Return migration files in version order (filenames starting with a digit)."""
    if not MIGRATIONS_DIR.is_dir():
        return []
    return sorted(p for p in MIGRATIONS_DIR.glob("*.sql") if p.stem[:1].isdigit())


async def apply_migrations(pool: AsyncConnectionPool | None = None) -> list[str]:
    """Apply any unapplied migrations from `MIGRATIONS_DIR` in version order.

    Returns the list of versions newly applied (empty if nothing to do).
    Safe to call repeatedly. Uses a Postgres transactional advisory lock so
    two processes booting against the same database serialize correctly.
    """
    pool = pool or get_pool()
    files = _discover_migrations()
    if not files:
        logger.warning("no migrations found at %s", MIGRATIONS_DIR)
        return []

    newly_applied: list[str] = []

    async with pool.connection() as conn:
        await conn.set_autocommit(False)
        try:
            # Bootstrap schema_migrations + read applied versions in one tx.
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_ADVISORY_LOCK_KEY,))
                await cur.execute(_SCHEMA_MIGRATIONS_DDL)
                await cur.execute("SELECT version FROM schema_migrations")
                rows = await cur.fetchall()
                already = {row[0] for row in rows}
            await conn.commit()

            for path in files:
                version = path.stem
                if version in already:
                    continue
                logger.info("applying migration %s", version)
                sql = path.read_text()
                async with conn.cursor() as cur:
                    # Re-take the lock for each migration's transaction.
                    await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_ADVISORY_LOCK_KEY,))
                    await cur.execute(sql)
                    await cur.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (version,),
                    )
                await conn.commit()
                newly_applied.append(version)
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.set_autocommit(True)

    if newly_applied:
        logger.info(
            "applied %d migration(s): %s",
            len(newly_applied),
            ", ".join(newly_applied),
        )
    else:
        logger.info("schema up to date (%d migration(s) on file)", len(files))
    return newly_applied
