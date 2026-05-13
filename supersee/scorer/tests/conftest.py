"""Pytest setup for the scorer test suite.

The rule functions are pure, but they import `ScorerSettings` from
`supersee.config.scorer`, which triggers loading of `supersee.config`'s
package init. That init constructs the module-level `settings` singleton,
which constructs `RuntimeSettings()`, which requires `DATABASE_URL`.

These are unit tests that touch no database, so we set a fallback DSN
here. Real-integration tests will use `pytest-postgresql` to provide
a real ephemeral database.
"""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test@localhost/test")
