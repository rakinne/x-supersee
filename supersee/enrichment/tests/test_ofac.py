"""Unit tests for OFAC ingestion: parser, dedupe, retry behavior.

Parser tests are pure-Python over NDJSON literals shaped like real
OpenSanctions `CryptoWallet` entities. Retry/fetch tests use
`httpx.MockTransport` to inject canned responses; no real network or
DB is touched.
"""

from __future__ import annotations

import httpx
import pytest

from supersee.config.enrichment import EnrichmentSettings
from supersee.enrichment.ofac import (
    SDNAddress,
    _normalize_currency,
    fetch_sdn_addresses,
    fetch_sdn_addresses_retrying,
    parse_opensanctions_ndjson,
)


# Helper to build NDJSON CryptoWallet entries that look like the real feed.
def _wallet_line(
    wallet_id: str,
    *,
    public_key: str,
    currency: str,
    holder: str | None = None,
) -> str:
    import json

    props: dict[str, object] = {
        "publicKey": [public_key],
        "currency": [currency],
    }
    if holder is not None:
        props["holder"] = [holder]
    return json.dumps(
        {"id": wallet_id, "schema": "CryptoWallet", "properties": props}
    )


# ===========================================================================
# _normalize_currency
# ===========================================================================

class TestNormalizeCurrency:
    @pytest.mark.parametrize(
        "input_value, expected",
        [
            ("Bitcoin", "BTC"),
            ("bitcoin", "BTC"),
            ("BTC", "BTC"),
            ("Ethereum", "ETH"),
            ("Ripple", "XRP"),
            ("  XRP  ", "XRP"),
            ("Tether", "USDT"),
            ("Dogecoin", "DOGE"),
        ],
    )
    def test_known_currencies_map_to_short_codes(
        self, input_value: str, expected: str
    ) -> None:
        assert _normalize_currency(input_value) == expected

    def test_unknown_currency_falls_through_uppercased(self) -> None:
        assert _normalize_currency("Helium") == "HELIUM"
        assert _normalize_currency("zkSync") == "ZKSYNC"


# ===========================================================================
# parse_opensanctions_ndjson
# ===========================================================================

class TestParseNDJSON:
    def test_extracts_addresses_from_cryptowallet_entities(self) -> None:
        lines = [
            _wallet_line(
                "fbi-lazarus-abc",
                public_key="bc1qqvpjgaurtnhc8smkmdtwhx9c8207m0prsyxyjx",
                currency="Bitcoin",
                holder="NK-Xv8CnM8sgddxx7QenotGtb",
            ),
            _wallet_line(
                "fbi-lazarus-def",
                public_key="0xdef0000000000000000000000000000000000000",
                currency="Ethereum",
                holder="NK-Xv8CnM8sgddxx7QenotGtb",
            ),
            _wallet_line(
                "tornado-001",
                public_key="rTornadoXxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                currency="Ripple",
                holder="NK-OtherSanctionedEntity",
            ),
        ]
        out = list(parse_opensanctions_ndjson(lines))
        addresses = {sdn.address: sdn for sdn in out}
        assert (
            addresses["bc1qqvpjgaurtnhc8smkmdtwhx9c8207m0prsyxyjx"]
            == SDNAddress(
                address="bc1qqvpjgaurtnhc8smkmdtwhx9c8207m0prsyxyjx",
                asset="BTC",
                source_id="NK-Xv8CnM8sgddxx7QenotGtb",
            )
        )
        assert addresses["rTornadoXxxxxxxxxxxxxxxxxxxxxxxxxxxxx"].asset == "XRP"
        assert len(out) == 3

    def test_skips_non_cryptowallet_schemas(self) -> None:
        import json

        lines = [
            json.dumps(
                {
                    "id": "NK-Org",
                    "schema": "Organization",
                    "properties": {"name": ["Lazarus Group"]},
                }
            ),
            _wallet_line("real-wallet", public_key="addr1", currency="Bitcoin", holder="NK-Org"),
        ]
        out = list(parse_opensanctions_ndjson(lines))
        assert len(out) == 1
        assert out[0].address == "addr1"

    def test_falls_back_to_wallet_id_when_no_holder(self) -> None:
        import json

        lines = [
            json.dumps(
                {
                    "id": "lone-wallet",
                    "schema": "CryptoWallet",
                    "properties": {
                        "publicKey": ["addrZ"],
                        "currency": ["Bitcoin"],
                    },
                }
            )
        ]
        out = list(parse_opensanctions_ndjson(lines))
        assert len(out) == 1
        assert out[0].source_id == "lone-wallet"

    def test_skips_blank_and_malformed_lines(self) -> None:
        lines = [
            "",
            "   ",
            "not json at all",
            _wallet_line("good", public_key="addrG", currency="Bitcoin", holder="NK-1"),
            "{broken json",
        ]
        out = list(parse_opensanctions_ndjson(lines))
        assert len(out) == 1
        assert out[0].address == "addrG"

    def test_skips_entries_missing_required_props(self) -> None:
        import json

        lines = [
            # No publicKey
            json.dumps(
                {
                    "id": "no-key",
                    "schema": "CryptoWallet",
                    "properties": {"currency": ["Bitcoin"]},
                }
            ),
            # No currency
            json.dumps(
                {
                    "id": "no-currency",
                    "schema": "CryptoWallet",
                    "properties": {"publicKey": ["abc"]},
                }
            ),
        ]
        assert list(parse_opensanctions_ndjson(lines)) == []

    def test_string_source_id_preserved(self) -> None:
        """Schema migration 002 lets us store strings like 'NK-Xv8C...' verbatim."""
        lines = [
            _wallet_line(
                "x",
                public_key="addr",
                currency="Bitcoin",
                holder="NK-LongStringIDFromOpenSanctions",
            )
        ]
        out = list(parse_opensanctions_ndjson(lines))
        assert out[0].source_id == "NK-LongStringIDFromOpenSanctions"


# ===========================================================================
# fetch_sdn_addresses (single HTTP call) via MockTransport
# ===========================================================================

class TestFetchSDNAddresses:
    async def test_dedupe_last_wins_per_address(self) -> None:
        body = "\n".join(
            [
                _wallet_line("w1", public_key="dupAddr", currency="Bitcoin", holder="NK-A"),
                _wallet_line("w2", public_key="dupAddr", currency="Bitcoin", holder="NK-B"),
                _wallet_line("w3", public_key="uniqueAddr", currency="Ethereum", holder="NK-A"),
            ]
        )
        transport = httpx.MockTransport(lambda req: httpx.Response(200, content=body))
        async with httpx.AsyncClient(transport=transport) as client:
            out = await fetch_sdn_addresses("https://example.com/x", client=client)

        by_addr = {sdn.address: sdn for sdn in out}
        assert by_addr["dupAddr"].source_id == "NK-B", "last entry wins"
        assert "uniqueAddr" in by_addr
        assert len(out) == 2

    async def test_raises_on_404(self) -> None:
        transport = httpx.MockTransport(lambda req: httpx.Response(404))
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await fetch_sdn_addresses("https://example.com/x", client=client)


# ===========================================================================
# fetch_sdn_addresses_retrying: retry behavior via MockTransport
# ===========================================================================

class TestRetryBehavior:
    async def test_retries_on_5xx_then_succeeds(self) -> None:
        attempts: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 3:
                return httpx.Response(503)
            body = _wallet_line(
                "x", public_key="addrOK", currency="Bitcoin", holder="NK-1"
            )
            return httpx.Response(200, content=body.encode())

        # Tighten the retry budget so the test runs fast.
        cfg = EnrichmentSettings(ofac_refresh_max_retry_seconds=30)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            out = await fetch_sdn_addresses_retrying(
                "https://example.com/x", cfg, client=client
            )

        assert len(attempts) == 3
        assert len(out) == 1
        assert out[0].address == "addrOK"

    async def test_terminal_failure_reraises(self) -> None:
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        # Short retry budget — give up almost immediately.
        cfg = EnrichmentSettings(ofac_refresh_max_retry_seconds=3)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await fetch_sdn_addresses_retrying(
                    "https://example.com/x", cfg, client=client
                )
