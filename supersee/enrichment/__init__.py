"""External enrichment sources (OFAC SDN, etc.).

This package owns the code that reaches outside the local DB to refresh
sanction lists and other compliance feeds. The scheduler (see
`supersee.scheduler`) drives these on a daily cadence; the per-event
scorer consumes the cached results via `supersee.scorer.history.Lookups`.
"""
