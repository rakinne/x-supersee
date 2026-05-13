"""Tests for `supersee.supervisor.run_with_restart`."""

from __future__ import annotations

import asyncio

import pytest

from supersee.supervisor import run_with_restart


class TestRunWithRestart:
    async def test_restarts_on_exception(self) -> None:
        """A task that raises is restarted; we check restart count + cleanup on cancel."""
        attempts = 0

        async def flaky() -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError(f"boom #{attempts}")
            # On the third attempt, hang until cancelled
            await asyncio.Future()

        task = asyncio.create_task(
            run_with_restart(flaky, "flaky", initial_backoff=0.01, max_backoff=0.05)
        )
        # Wait long enough for at least 3 attempts (2 crashes + 1 ongoing).
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert attempts >= 3

    async def test_propagates_cancellation(self) -> None:
        """Cancel signals must escape the supervisor cleanly."""
        async def long_running() -> None:
            await asyncio.Future()

        task = asyncio.create_task(
            run_with_restart(long_running, "long", initial_backoff=0.01)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_restarts_on_clean_return(self) -> None:
        """An infinite-loop task that returns cleanly should restart."""
        attempts = 0

        async def returns_quickly() -> None:
            nonlocal attempts
            attempts += 1
            if attempts >= 3:
                # On the third attempt, hang so the test can cancel
                await asyncio.Future()
            # Otherwise return cleanly — supervisor should restart us

        task = asyncio.create_task(
            run_with_restart(
                returns_quickly, "quick", initial_backoff=0.01, max_backoff=0.05
            )
        )
        await asyncio.sleep(0.3)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert attempts >= 3

    async def test_cancel_during_backoff_sleep(self) -> None:
        """Cancellation during the inter-attempt backoff sleep should still propagate."""
        async def always_crash() -> None:
            raise RuntimeError("persistent")

        task = asyncio.create_task(
            run_with_restart(
                always_crash, "crashy", initial_backoff=10.0, max_backoff=10.0
            )
        )
        # Let one crash happen, supervisor enters 10s backoff sleep
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)
