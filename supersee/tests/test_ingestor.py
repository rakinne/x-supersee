"""Tests for `supersee.ingestor` — message normalization + DB writes.

The WebSocket subscribe loop is exercised live in the running container; we
test the pure normalization + DB-insert pieces here.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from psycopg_pool import AsyncConnectionPool

from supersee.ingestor import (
    _amount_xrp_drops,
    _decode_memo,
    _insert_event,
    normalize_payment_message,
)

# ===========================================================================
# Pure helpers
# ===========================================================================


class TestDecodeMemo:
    def test_decodes_utf8(self) -> None:
        # "tornado cash" in hex
        assert _decode_memo("746f726e61646f2063617368") == "tornado cash"

    def test_returns_raw_hex_when_not_utf8(self) -> None:
        # \xff\xff is not valid UTF-8 in isolation
        out = _decode_memo("ffff")
        # Either returns the hex string OR a string with replacement chars,
        # depending on Python's decode error mode. Our implementation falls
        # back to the raw hex when UTF-8 fails.
        assert out == "ffff"

    def test_returns_none_for_empty(self) -> None:
        assert _decode_memo(None) is None
        assert _decode_memo("") is None

    def test_handles_invalid_hex(self) -> None:
        # Not hex; should not raise, should return the string as-is.
        assert _decode_memo("not-hex-at-all") == "not-hex-at-all"


class TestAmountXrpDrops:
    def test_converts_drops_to_xrp(self) -> None:
        assert _amount_xrp_drops("1000000") == 1.0
        assert _amount_xrp_drops("75000000000") == 75_000.0

    def test_returns_none_for_issued_currency(self) -> None:
        # Issued currency Amount is a dict, not a string
        assert _amount_xrp_drops({"currency": "USD", "value": "100", "issuer": "rX"}) is None

    def test_returns_none_for_non_numeric_string(self) -> None:
        assert _amount_xrp_drops("not a number") is None

    def test_returns_none_for_none(self) -> None:
        assert _amount_xrp_drops(None) is None


# ===========================================================================
# normalize_payment_message
# ===========================================================================


def _payment_msg(**overrides) -> dict:
    """A minimal valid validated-Payment stream message."""
    msg = {
        "type": "transaction",
        "validated": True,
        "hash": "0xDEADBEEF",
        "ledger_index": 91_000_000,
        "close_time_iso": "2026-05-13T20:00:00Z",
        "tx_json": {
            "TransactionType": "Payment",
            "Account": "rSrcXxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "Destination": "rDstXxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "Amount": "75000000000",  # 75,000 XRP in drops
        },
    }
    msg.update(overrides)
    return msg


class TestNormalizePaymentMessage:
    def test_valid_payment_normalizes(self) -> None:
        row = normalize_payment_message(_payment_msg())
        assert row is not None
        assert row["tx_hash"] == "0xDEADBEEF"
        assert row["ledger_index"] == 91_000_000
        assert row["amount_xrp"] == 75_000.0
        assert row["account_src"] == "rSrcXxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        assert row["account_dst"] == "rDstXxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        assert row["tx_type"] == "Payment"
        assert row["memo_decoded"] is None

    def test_skips_non_validated(self) -> None:
        assert normalize_payment_message(_payment_msg(validated=False)) is None

    def test_skips_non_transaction_type(self) -> None:
        assert normalize_payment_message(_payment_msg(type="ledgerClosed")) is None

    def test_skips_non_payment(self) -> None:
        msg = _payment_msg()
        msg["tx_json"]["TransactionType"] = "OfferCreate"
        assert normalize_payment_message(msg) is None

    def test_skips_issued_currency_amount(self) -> None:
        msg = _payment_msg()
        msg["tx_json"]["Amount"] = {
            "currency": "USD", "value": "1000", "issuer": "rIssuerXxx"
        }
        assert normalize_payment_message(msg) is None

    def test_decodes_memo(self) -> None:
        msg = _payment_msg()
        msg["tx_json"]["Memos"] = [
            {"Memo": {"MemoData": "746f726e61646f2063617368"}}  # "tornado cash"
        ]
        row = normalize_payment_message(msg)
        assert row is not None
        assert row["memo_decoded"] == "tornado cash"

    def test_handles_missing_hash(self) -> None:
        msg = _payment_msg()
        del msg["hash"]
        msg["tx_json"].pop("hash", None)
        assert normalize_payment_message(msg) is None

    def test_handles_missing_ledger_index(self) -> None:
        msg = _payment_msg()
        del msg["ledger_index"]
        msg["tx_json"].pop("ledger_index", None)
        assert normalize_payment_message(msg) is None

    def test_uses_deliver_max_when_amount_missing(self) -> None:
        msg = _payment_msg()
        del msg["tx_json"]["Amount"]
        msg["tx_json"]["DeliverMax"] = "1000000"  # 1 XRP
        row = normalize_payment_message(msg)
        assert row is not None
        assert row["amount_xrp"] == 1.0


# ===========================================================================
# DB writes (integration; skipped on fake DSN)
# ===========================================================================


_DSN = os.environ.get("DATABASE_URL", "")
_IS_FAKE_DSN = "fake" in _DSN or _DSN == "postgresql://test@localhost/test"


@pytest.mark.skipif(_IS_FAKE_DSN, reason="DB integration needs a real DSN")
class TestInsertEvent:
    @pytest.fixture
    async def pool(self) -> AsyncIterator[AsyncConnectionPool]:
        p = AsyncConnectionPool(
            conninfo=_DSN, min_size=1, max_size=2,
            kwargs={"autocommit": True}, open=False,
        )
        await p.open()
        async with p.connection() as conn, conn.cursor() as cur:
            await cur.execute("TRUNCATE events RESTART IDENTITY CASCADE")
        try:
            yield p
        finally:
            await p.close()

    async def test_insert_returns_new_id(self, pool: AsyncConnectionPool) -> None:
        row = normalize_payment_message(_payment_msg(hash=uuid.uuid4().hex))
        assert row is not None
        event_id = await _insert_event(pool, row)
        assert event_id is not None and event_id > 0

    async def test_duplicate_tx_hash_returns_none(self, pool: AsyncConnectionPool) -> None:
        msg = _payment_msg(hash=uuid.uuid4().hex)
        row = normalize_payment_message(msg)
        assert row is not None
        first_id = await _insert_event(pool, row)
        second_id = await _insert_event(pool, row)
        assert first_id is not None
        assert second_id is None  # ON CONFLICT DO NOTHING
