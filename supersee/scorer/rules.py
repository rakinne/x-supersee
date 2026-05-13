"""Deterministic scoring rules.

Each rule is a pure function that takes only the data it needs (from the
event itself plus precomputed context like account history or curated
lookup sets) plus a `ScorerSettings` instance, and returns
`(score, rationale)`.

When a rule fires, the score equals its weight in `cfg.weights`. When it
doesn't fire, the score is 0.0 and the rationale describes why. The
aggregator (`supersee.scorer.score_event`, future) sums weights, clips to
[0, 1], and bands the result.

Rationales are concise and informative. The ones from firing rules end
up in `cases.rule_hits` and feed the LLM narrative downstream; the ones
from non-firing rules support debugging and case audit.

No I/O happens in this module — every dependency is an argument. That
keeps the rules unit-testable without a database or network.
"""

from __future__ import annotations

import re

from supersee.config.scorer import ScorerSettings

RuleResult = tuple[float, str]


def large_payment(
    amount_xrp: float | None,
    rolling_p99_7d: float | None,
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when amount_xrp >= max(rolling_p99_7d, floor) for the source account."""
    if amount_xrp is None:
        return 0.0, "non-XRP tx (amount_xrp is None)"
    floor = cfg.thresholds.large_payment_floor_xrp
    threshold = max(rolling_p99_7d, floor) if rolling_p99_7d is not None else floor
    if amount_xrp >= threshold:
        binding = (
            "P99"
            if rolling_p99_7d is not None and rolling_p99_7d > floor
            else "floor"
        )
        return (
            cfg.weights.large_payment,
            f"amount {amount_xrp:.2f} XRP >= {binding} {threshold:.2f} XRP",
        )
    return (
        0.0,
        f"amount {amount_xrp:.2f} XRP below threshold {threshold:.2f} XRP",
    )


def velocity_burst(
    recent_outbound_count: int,
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when the outbound count over the configured window meets the threshold.

    Time-windowing is the aggregator's job: it counts events in the last
    `velocity_burst_window_minutes` (using `clock.now()` so fixture replays
    are deterministic) and passes the count in.
    """
    threshold = cfg.thresholds.velocity_burst_min_count
    window = cfg.thresholds.velocity_burst_window_minutes
    if recent_outbound_count >= threshold:
        return (
            cfg.weights.velocity_burst,
            f"{recent_outbound_count} outbound payments in last {window} minutes "
            f"(>= threshold {threshold})",
        )
    return (
        0.0,
        f"{recent_outbound_count} outbound payments in last {window} minutes "
        f"(< threshold {threshold})",
    )


def new_counterparty(
    amount_xrp: float | None,
    account_dst: str | None,
    known_counterparties: frozenset[str],
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire for a previously-unseen destination at or above the amount floor."""
    if account_dst is None:
        return 0.0, "no destination (not a Payment-like tx)"
    if amount_xrp is None:
        return 0.0, "non-XRP tx (amount_xrp is None)"
    threshold = cfg.thresholds.new_counterparty_min_xrp
    if account_dst in known_counterparties:
        return 0.0, f"destination {account_dst} previously seen"
    if amount_xrp < threshold:
        return (
            0.0,
            f"new destination {account_dst} but amount {amount_xrp:.2f} XRP "
            f"< threshold {threshold:.2f} XRP",
        )
    return (
        cfg.weights.new_counterparty,
        f"new destination {account_dst}; amount {amount_xrp:.2f} XRP "
        f">= threshold {threshold:.2f} XRP",
    )


def memo_anomaly(
    memo_decoded: str | None,
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when the memo matches any of the configured suspicious patterns.

    `memo_decoded` is either the UTF-8 decode of the on-chain memo hex,
    or the raw hex when UTF-8 decode fails. Both are valid inputs; the
    pattern list includes both natural-language regexes and a long-hex
    catch-all.
    """
    if not memo_decoded:
        return 0.0, "no memo"
    for pattern in cfg.memo_patterns:
        if re.search(pattern, memo_decoded):
            return (
                cfg.weights.memo_anomaly,
                f"memo matches pattern {pattern!r}",
            )
    return 0.0, "memo did not match any suspicious pattern"


def watchlist_hit(
    account_src: str,
    account_dst: str | None,
    watchlist_addresses: frozenset[str],
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when source or destination address is in the curated watchlist."""
    src_hit = account_src in watchlist_addresses
    dst_hit = account_dst is not None and account_dst in watchlist_addresses
    if src_hit and dst_hit:
        return (
            cfg.weights.watchlist_hit,
            f"both source {account_src} and destination {account_dst} in watchlist",
        )
    if src_hit:
        return cfg.weights.watchlist_hit, f"source {account_src} in watchlist"
    if dst_hit:
        return cfg.weights.watchlist_hit, f"destination {account_dst} in watchlist"
    return 0.0, "no watchlist match"


def ofac_sdn_match(
    account_src: str,
    account_dst: str | None,
    ofac_addresses: frozenset[str],
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when source or destination matches an OFAC SDN crypto address.

    Weight is calibrated so a single hit clears the high-band threshold
    on its own (see `bands.medium_max` in ScorerSettings).
    """
    src_hit = account_src in ofac_addresses
    dst_hit = account_dst is not None and account_dst in ofac_addresses
    if src_hit and dst_hit:
        return (
            cfg.weights.ofac_sdn_match,
            f"both source {account_src} and destination {account_dst} match OFAC SDN",
        )
    if src_hit:
        return cfg.weights.ofac_sdn_match, f"source {account_src} matches OFAC SDN"
    if dst_hit:
        return cfg.weights.ofac_sdn_match, f"destination {account_dst} matches OFAC SDN"
    return 0.0, "no OFAC SDN match"


def mixer_direct(
    account_dst: str | None,
    mixer_addresses: frozenset[str],
    cfg: ScorerSettings,
) -> RuleResult:
    """Fire when destination is directly listed in the curated mixer table.

    The 1-hop neighborhood check was trimmed during /plan-eng-review;
    real graph-based mixer detection lives in TODOS.md.
    """
    if account_dst is None:
        return 0.0, "no destination"
    if account_dst in mixer_addresses:
        return cfg.weights.mixer_direct, f"destination {account_dst} in mixer addresses"
    return 0.0, "destination not in mixer addresses"
