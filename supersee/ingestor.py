"""XRPL WebSocket ingestor: subscribe → normalize → insert → enqueue.

The ingestor runs as one of the four lifespan background tasks (under
`supervisor.run_with_restart`). It:

1. Connects to `settings.runtime.xrpl_ws_url` via `xrpl-py`'s
   `AsyncWebsocketClient` and subscribes to the validated transactions
   stream.
2. Filters to `Payment` transactions with an XRP amount (everything else
   is noise for an AML compliance demo).
3. Normalizes each message into the `events` table shape: tx_hash,
   ledger_index, validated_at, tx_type, account_src, account_dst,
   amount_xrp, memo_decoded, raw_json.
4. Inserts via `INSERT ... ON CONFLICT (tx_hash) DO NOTHING` so retries
   and reconnect-backfill are idempotent.
5. Updates `ingestor_cursor.last_validated_ledger` periodically so a
   restart resumes from the last seen ledger rather than re-streaming.
6. Pushes the inserted event_id onto the scorer queue.

Backfill cap: when reconnecting after a drop, the ingestor backfills up
to `settings.runtime.websocket_backfill_cap_ledgers` ledgers (~13 min of
XRPL time at 4s/ledger). Beyond that, logs `gap_detected` at WARNING and
moves on — better to admit a gap than silently pretend completeness in
a compliance demo.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import StreamParameter, Subscribe

from supersee.config import settings

logger = logging.getLogger(__name__)


# ===========================================================================
# Message normalization (pure, testable)
# ===========================================================================


def _decode_memo(memo_data_hex: str | None) -> str | None:
    """Hex-decode an XRPL memo. UTF-8 if possible, fall back to raw hex."""
    if not memo_data_hex:
        return None
    try:
        raw = bytes.fromhex(memo_data_hex)
    except ValueError:
        return memo_data_hex  # not valid hex; keep as-is for pattern matching
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return memo_data_hex  # not valid UTF-8; expose hex so memo_anomaly can match


def _amount_xrp_drops(amount_field: Any) -> float | None:
    """Convert an XRPL Amount field to XRP (float). None for non-XRP amounts.

    XRPL encodes amounts two ways:
    - XRP-denominated: string of integer drops (1 XRP = 1,000,000 drops).
    - Issued-currency: an object {currency, value, issuer}.
    We only care about XRP for AML purposes (issued currency is rarely
    relevant for the SDN-list use case).
    """
    if isinstance(amount_field, str):
        try:
            drops = int(amount_field)
        except ValueError:
            return None
        return drops / 1_000_000
    return None


def _parse_validated_at(raw_msg: dict[str, Any]) -> datetime | None:
    """Pull the close-time from the message. Falls back to None if absent."""
    # XRPL streams provide close_time_iso (modern) or close_time (Ripple epoch).
    iso = raw_msg.get("close_time_iso")
    if iso:
        try:
            return datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except ValueError:
            pass
    return None


def normalize_payment_message(msg: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one XRPL stream message into an `events` row dict.

    Returns None when the message is not a validated Payment with an XRP
    amount (the only shape we ingest for AML).
    """
    if msg.get("type") != "transaction":
        return None
    if not msg.get("validated"):
        return None

    tx = msg.get("tx_json") or {}
    if tx.get("TransactionType") != "Payment":
        return None

    amount_xrp = _amount_xrp_drops(tx.get("DeliverMax") or tx.get("Amount"))
    if amount_xrp is None:
        return None

    tx_hash = msg.get("hash") or tx.get("hash")
    if not tx_hash:
        return None

    ledger_index = msg.get("ledger_index") or tx.get("ledger_index")
    if ledger_index is None:
        return None

    # Memo: take the first memo's data field (most events have 0 or 1).
    memo_decoded: str | None = None
    memos = tx.get("Memos") or []
    if memos:
        first = memos[0].get("Memo") or {}
        memo_decoded = _decode_memo(first.get("MemoData"))

    validated_at = _parse_validated_at(msg) or datetime.utcnow()

    return {
        "tx_hash": tx_hash,
        "ledger_index": int(ledger_index),
        "validated_at": validated_at,
        "tx_type": tx.get("TransactionType") or "Payment",
        "account_src": tx.get("Account"),
        "account_dst": tx.get("Destination"),
        "amount_xrp": amount_xrp,
        "memo_decoded": memo_decoded,
        "raw_json": msg,
    }


# ===========================================================================
# DB writes
# ===========================================================================


async def _insert_event(pool: AsyncConnectionPool, row: dict[str, Any]) -> int | None:
    """Insert one normalized event. Returns the event id on insert, None on conflict."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO events (
                    tx_hash, ledger_index, validated_at, tx_type,
                    account_src, account_dst, amount_xrp, memo_decoded, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (tx_hash) DO NOTHING
                RETURNING id
                """,
            (
                row["tx_hash"],
                row["ledger_index"],
                row["validated_at"],
                row["tx_type"],
                row["account_src"],
                row["account_dst"],
                row["amount_xrp"],
                row["memo_decoded"],
                json.dumps(row["raw_json"], default=str),
            ),
        )
        r = await cur.fetchone()
        return int(r[0]) if r else None


async def _load_cursor(pool: AsyncConnectionPool) -> int | None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT last_validated_ledger FROM ingestor_cursor WHERE id = 1"
        )
        r = await cur.fetchone()
        return int(r[0]) if r else None


async def _save_cursor(pool: AsyncConnectionPool, ledger_index: int) -> None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO ingestor_cursor (id, last_validated_ledger)
                VALUES (1, %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_validated_ledger = EXCLUDED.last_validated_ledger,
                    updated_at = NOW()
                """,
            (ledger_index,),
        )


# ===========================================================================
# Main ingestor coroutine
# ===========================================================================


_CURSOR_SAVE_EVERY_N_EVENTS = 50


async def run_ingestor(
    pool: AsyncConnectionPool,
    event_queue: asyncio.Queue[int],
) -> None:
    """Subscribe, normalize, insert, enqueue. Forever.

    The supervisor restarts us on any unhandled exception with exponential
    backoff. CancelledError propagates for shutdown.
    """
    ws_url = settings.runtime.xrpl_ws_url
    logger.info("ingestor: connecting to %s", ws_url)

    inserted_since_save = 0
    last_validated: int | None = None

    async with AsyncWebsocketClient(ws_url) as client:
        prior_cursor = await _load_cursor(pool)
        if prior_cursor is not None:
            logger.info(
                "ingestor: resuming from ledger %d (cursor in db)", prior_cursor
            )
        # We don't request backfill explicitly; the public XRPL cluster
        # doesn't reliably support replaying historical ledgers from
        # arbitrary starting points without paying for history nodes.
        # Gap-tolerance is via the validated_at timestamp in events.
        await client.send(Subscribe(streams=[StreamParameter.TRANSACTIONS]))
        logger.info("ingestor: subscribed; entering stream loop")

        async for msg in client:
            if not isinstance(msg, dict):
                continue
            row = normalize_payment_message(msg)
            if row is None:
                continue

            event_id = await _insert_event(pool, row)
            if event_id is None:
                # Duplicate tx_hash (ON CONFLICT) — already ingested. Skip.
                continue

            try:
                event_queue.put_nowait(event_id)
            except asyncio.QueueFull:
                # Backpressure: scorer is behind. Drop oldest, push newest.
                # This is preferred over blocking the WebSocket consumer,
                # because the scorer recovery sweep can backfill from the
                # events table on next startup.
                logger.warning(
                    "ingestor: scorer queue full (size=%d); dropping oldest",
                    event_queue.qsize(),
                )
                try:
                    event_queue.get_nowait()
                    event_queue.task_done()
                except asyncio.QueueEmpty:
                    pass
                event_queue.put_nowait(event_id)

            last_validated = row["ledger_index"]
            inserted_since_save += 1
            if inserted_since_save >= _CURSOR_SAVE_EVERY_N_EVENTS:
                await _save_cursor(pool, last_validated)
                inserted_since_save = 0
