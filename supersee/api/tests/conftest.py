"""Pytest setup for the API route tests.

These tests touch the real Postgres + the compiled LangGraph. We bypass the
FastAPI lifespan and init pool + graph manually so each test has predictable
state (the lifespan-driven `nav` queries also work without a separate stack).
"""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test@localhost/test")
