"""Deterministic time indirection for fixture replays and tests.

The scorer's `velocity_burst` rule queries "events in the last 5 minutes,"
which requires fixed-relative time semantics. Fixture replays from a date
in the past would never fire the rule under a real wall clock, so all
scorer code reads `clock.now()` instead of `datetime.now(UTC)`.

Tests and the replay CLI override the clock via `freeze_time()`. The
override is held in a `ContextVar`, so async tests running in parallel
(via `asyncio.gather`, for example) don't bleed time into each other.

Example
-------
    from datetime import UTC, datetime
    from supersee import clock

    with clock.freeze_time(datetime(2026, 5, 13, 10, 0, tzinfo=UTC)):
        process(event_1)         # clock.now() == 10:00:00
        clock.tick(seconds=30)
        process(event_2)         # clock.now() == 10:00:30
        clock.tick(minutes=4)
        process(event_3)         # clock.now() == 10:04:30
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta

_OVERRIDE: ContextVar[datetime | None] = ContextVar(
    "supersee_clock_override", default=None
)


def now() -> datetime:
    """Return the current time (UTC, tz-aware).

    If a `freeze_time()` context is active in this task, returns the frozen
    time (advanced by any `tick()` calls). Otherwise returns wall-clock time.
    """
    override = _OVERRIDE.get()
    return override if override is not None else datetime.now(UTC)


@contextmanager
def freeze_time(t: datetime) -> Iterator[None]:
    """Freeze the clock at `t` for the duration of the block.

    `t` must be tz-aware. Nested freezes restore the outer time on exit.
    `tick()` and `set_time()` inside the block adjust the frozen value.
    """
    if t.tzinfo is None:
        raise ValueError("freeze_time requires a tz-aware datetime")
    token = _OVERRIDE.set(t)
    try:
        yield
    finally:
        _OVERRIDE.reset(token)


def set_time(t: datetime) -> None:
    """Set the frozen time to an absolute value. Requires active `freeze_time`."""
    if _OVERRIDE.get() is None:
        raise RuntimeError("set_time() requires an active freeze_time() context")
    if t.tzinfo is None:
        raise ValueError("set_time requires a tz-aware datetime")
    _OVERRIDE.set(t)


def tick(
    seconds: float = 0,
    *,
    minutes: float = 0,
    hours: float = 0,
    days: float = 0,
) -> None:
    """Advance the frozen clock by the given duration. Requires active `freeze_time`."""
    current = _OVERRIDE.get()
    if current is None:
        raise RuntimeError("tick() requires an active freeze_time() context")
    delta = timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)
    _OVERRIDE.set(current + delta)
