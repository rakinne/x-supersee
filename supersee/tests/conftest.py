"""Pytest setup for the top-level module tests (supervisor, ingestor, pipeline)."""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test@localhost/test")
