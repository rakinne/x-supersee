"""Pytest setup for the graph test suite.

Unit tests for state/narrative/triage are pure-Python. Node and integration
tests need the real Postgres provided by `docker compose`. The fallback
DATABASE_URL only matters when running outside the container.
"""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test@localhost/test")
