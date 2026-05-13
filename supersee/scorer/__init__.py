"""Deterministic scoring layer.

`supersee.scorer.rules` is the canonical module: 7 pure rule functions
that take an event's data plus precomputed context and return
`(score, rationale)`. The aggregator (`score_event`) and DB-backed
account-history upsert + P99 single-flight live in sibling modules
that will land in subsequent commits.
"""

from supersee.scorer import rules

__all__ = ["rules"]
