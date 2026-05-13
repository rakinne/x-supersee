"""OFAC SDN crypto address ingestion via OpenSanctions.

OpenSanctions exposes its data as FollowTheMoney NDJSON. Crypto wallets
are modeled as their own `CryptoWallet` schema entities (NOT as a
property on the sanctioned entity), with `publicKey`, `currency`, and
`holder` properties. We stream the feed, filter to `CryptoWallet`
entities, normalize the currency to a short asset code, and upsert
into `ofac_sdn_crypto`.

Coverage caveat: the broad OpenSanctions "sanctions" feed currently
contains no `CryptoWallet` entities. Crypto wallets live in narrower
sanctions-adjacent datasets (e.g., `us_fbi_lazarus_crypto`,
`il_mod_crypto`). The default URL points at one such dataset; operators
running this in production should override
`SUPERSEE_ENRICHMENT_OPENSANCTIONS_URL` with whatever consolidated feed
they trust. The synthetic-OFAC fixture path (see design doc, Open
Question #1) covers the demo regardless of feed sparsity.

Public surface:

    fetch_sdn_addresses(url, *, client=None)       -> list[SDNAddress]
    fetch_sdn_addresses_retrying(url, settings, *) -> list[SDNAddress]
    upsert_sdn_addresses(pool, url, addresses)     -> int
    refresh_ofac(pool, settings)                   -> int  (top-level entry point)
    get_last_fetched_at(pool)                      -> datetime | None

`refresh_ofac` is the one the scheduler calls. On terminal HTTP failure
(retries exhausted) it logs `ofac_refresh_failed` at WARNING and returns
0 — the existing `ofac_sdn_crypto` rows stay in place; stale beats empty.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime

import httpx
from psycopg_pool import AsyncConnectionPool
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)

from supersee import clock
from supersee.config.enrichment import EnrichmentSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SDNAddress:
    address: str
    asset: str        # "XRP", "BTC", "ETH", ...
    source_id: str    # OpenSanctions holder entity id, e.g., "NK-Xv8...", or wallet id


# OpenSanctions records the crypto network as a human-readable name
# ("Bitcoin", "Ripple", "Ethereum"). Map common ones to short asset
# codes; unknowns fall through to upper-cased input.
_CURRENCY_TO_ASSET: dict[str, str] = {
    "bitcoin": "BTC",
    "bitcoin cash": "BCH",
    "btc": "BTC",
    "dogecoin": "DOGE",
    "eth": "ETH",
    "ethereum": "ETH",
    "litecoin": "LTC",
    "monero": "XMR",
    "ripple": "XRP",
    "solana": "SOL",
    "tether": "USDT",
    "tron": "TRX",
    "trx": "TRX",
    "usd coin": "USDC",
    "usdc": "USDC",
    "usdt": "USDT",
    "xrp": "XRP",
}


def _normalize_currency(currency: str) -> str:
    """Map an OpenSanctions currency name to a short asset code."""
    return _CURRENCY_TO_ASSET.get(currency.lower().strip(), currency.upper().strip())


def parse_opensanctions_ndjson(lines: Iterable[str]) -> Iterator[SDNAddress]:
    """Stream-parse NDJSON, yield SDN crypto addresses.

    Filters to entities whose `schema` is `CryptoWallet`; pulls `publicKey`
    as the address, `currency` (normalized) as the asset, and `holder`
    (FK to the sanctioned entity) as `source_id`. Falls back to the
    wallet's own id when no holder is recorded.
    """
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        try:
            entity = json.loads(line)
        except json.JSONDecodeError:
            logger.debug("opensanctions: skipping malformed NDJSON line")
            continue
        if not isinstance(entity, dict):
            continue
        if entity.get("schema") != "CryptoWallet":
            continue
        props = entity.get("properties") or {}

        public_keys = props.get("publicKey") or []
        if not public_keys:
            continue
        address = str(public_keys[0]).strip()
        if not address:
            continue

        currencies = props.get("currency") or []
        if not currencies:
            continue
        asset = _normalize_currency(str(currencies[0]))
        if not asset:
            continue

        holders = props.get("holder") or []
        source_id = (
            str(holders[0]).strip()
            if holders
            else str(entity.get("id") or "").strip()
        )
        if not source_id:
            continue

        yield SDNAddress(address=address, asset=asset, source_id=source_id)


async def fetch_sdn_addresses(
    url: str,
    *,
    timeout: float = 60.0,
    client: httpx.AsyncClient | None = None,
) -> list[SDNAddress]:
    """One HTTP fetch + parse. Returns deduped addresses (last-wins per address)."""
    own_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        text = resp.text
    finally:
        if own_client:
            await client.aclose()

    seen: dict[str, SDNAddress] = {}
    for sdn in parse_opensanctions_ndjson(text.splitlines()):
        seen[sdn.address] = sdn  # last-wins dedup
    return list(seen.values())


async def fetch_sdn_addresses_retrying(
    url: str,
    settings: EnrichmentSettings,
    *,
    client: httpx.AsyncClient | None = None,
) -> list[SDNAddress]:
    """Fetch with exponential-backoff retries on httpx.HTTPError.

    `reraise=True` means the final exception is the actual HTTP error,
    not a tenacity RetryError. Caller in `refresh_ofac` catches and logs.
    """
    async for attempt in AsyncRetrying(
        stop=stop_after_delay(settings.ofac_refresh_max_retry_seconds),
        wait=wait_exponential(min=2, max=120),
        retry=retry_if_exception_type(httpx.HTTPError),
        before_sleep=before_sleep_log(logger, logging.INFO),
        reraise=True,
    ):
        with attempt:
            return await fetch_sdn_addresses(url, client=client)
    return []  # unreachable; AsyncRetrying with reraise=True raises on exhaustion


async def upsert_sdn_addresses(
    pool: AsyncConnectionPool,
    url: str,
    addresses: list[SDNAddress],
) -> int:
    """Upsert into ofac_sdn_crypto. Returns the count of rows touched."""
    if not addresses:
        return 0
    now = clock.now()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # executemany is fine here; the table is small (<10k rows total) and
            # daily refresh is not a hot path. If volume grows, switch to COPY.
            await cur.executemany(
                """
                INSERT INTO ofac_sdn_crypto (address, asset, sdn_uid, source_url, fetched_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (address) DO UPDATE SET
                    asset = EXCLUDED.asset,
                    sdn_uid = EXCLUDED.sdn_uid,
                    source_url = EXCLUDED.source_url,
                    fetched_at = EXCLUDED.fetched_at
                """,
                [(sdn.address, sdn.asset, sdn.source_id, url, now) for sdn in addresses],
            )
    return len(addresses)


async def refresh_ofac(pool: AsyncConnectionPool, settings: EnrichmentSettings) -> int:
    """Top-level entry point: fetch (with retries) and upsert.

    On terminal failure (e.g., OpenSanctions down for the full retry
    window), log a warning and return 0. The existing rows in
    `ofac_sdn_crypto` are left untouched — stale beats empty.
    """
    try:
        addresses = await fetch_sdn_addresses_retrying(
            settings.opensanctions_url, settings
        )
    except Exception as e:  # httpx.HTTPError, network errors, etc.
        logger.warning(
            "ofac_refresh_failed: %s: %s (kept existing rows)",
            type(e).__name__,
            e,
        )
        return 0
    count = await upsert_sdn_addresses(pool, settings.opensanctions_url, addresses)
    logger.info("ofac refresh: upserted %d addresses", count)
    return count


async def get_last_fetched_at(pool: AsyncConnectionPool) -> datetime | None:
    """Return the most recent `fetched_at` in `ofac_sdn_crypto`, or None if empty."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT MAX(fetched_at) FROM ofac_sdn_crypto")
            row = await cur.fetchone()
    return row[0] if row else None
