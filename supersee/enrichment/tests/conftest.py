"""Pytest setup for the enrichment test suite.

The parser tests are pure-Python; the retry tests use `httpx.MockTransport`
to inject responses; no test reaches a real DB or network. Still need a
fallback DATABASE_URL so `from supersee.config import settings` doesn't
fail at import time.
"""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test@localhost/test")
