"""M6.0 Phase 1 — runtime tests.

`LongRunningService` is the abstract base every M6 asyncio service
inherits. These tests assert the loop semantics work as the M3.3 +
M5.1 precedents do:

  - `max_ticks` terminates cleanly after N ticks.
  - `stop_event` set mid-run drains the current tick, persists state,
    and exits with no exception.
  - `make_workflow_pool` constructs with `statement_cache_size=0`
    (sixth such activation; pgbouncer transaction mode per M1.3 ADR Q1).

The real-subprocess SIGTERM test lives in Phase 2 with the first
concrete service (`FeelsOnboardedMonitor` + `__main__.py`). Phase 1's
in-process stop_event test exercises the SAME loop logic the
subprocess handler will trigger; the subprocess test adds OS-signal
plumbing on top.
"""
from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from services.ingestion.workflows.runtime import (
    LongRunningService,
    make_workflow_pool,
)


pytestmark = [pytest.mark.timeout(20)]


# =====================================================================
# Fake subclasses for testing.
# =====================================================================

class _CountingService(LongRunningService):
    """Records the tick count; exposes a knob to stop mid-run."""

    def __init__(
        self, *, tick_interval: float = 0.01,
        stop_at_tick: int | None = None,
    ) -> None:
        self.tick_count = 0
        self._stop_at_tick = stop_at_tick
        self._tick_interval = tick_interval
        self._external_stop: asyncio.Event | None = None

    @property
    def tick_interval_seconds(self) -> float:
        return self._tick_interval

    def bind_stop_event(self, event: asyncio.Event) -> None:
        self._external_stop = event

    async def tick(self) -> None:
        self.tick_count += 1
        if (
            self._stop_at_tick is not None
            and self.tick_count >= self._stop_at_tick
            and self._external_stop is not None
        ):
            self._external_stop.set()


class _RaisingService(LongRunningService):
    """A service whose tick raises — used to verify exceptions propagate
    (the substrate does NOT swallow tick errors; the supervisor
    restarts on failure)."""

    @property
    def tick_interval_seconds(self) -> float:
        return 0.01

    async def tick(self) -> None:
        raise RuntimeError("synthetic tick failure")


# =====================================================================
# 1. max_ticks terminates after N ticks.
# =====================================================================

async def test_long_running_service_max_ticks_terminates() -> None:
    svc = _CountingService(tick_interval=0.001)
    ticks = await svc.run(max_ticks=5)
    assert ticks == 5
    assert svc.tick_count == 5


async def test_long_running_service_max_ticks_zero_terminates_immediately() -> None:
    """A max_ticks=0 caller should get zero ticks (no business logic
    invoked). Tests the boundary at the top of the loop."""
    svc = _CountingService()
    ticks = await svc.run(max_ticks=0)
    assert ticks == 0
    assert svc.tick_count == 0


# =====================================================================
# 2. stop_event set mid-run exits cleanly.
#    Same loop logic the SIGTERM handler will trigger in Phase 2's
#    real-subprocess test.
# =====================================================================

async def test_long_running_service_handles_stop_event_cleanly() -> None:
    """Simulate SIGTERM: the service decides at tick 3 to stop. The
    current tick completes, the loop sees stop_event is set, exit
    cleanly without exception or extra ticks."""
    stop_event = asyncio.Event()
    svc = _CountingService(tick_interval=0.001, stop_at_tick=3)
    svc.bind_stop_event(stop_event)

    ticks = await svc.run(stop_event=stop_event, max_ticks=1000)
    # Exits AT tick 3 (sets stop_event during tick 3; loop checks
    # stop_event at the top, exits).
    assert ticks == 3
    assert svc.tick_count == 3


# =====================================================================
# 3. tick() exceptions propagate (supervisor-restart contract).
# =====================================================================

async def test_long_running_service_tick_exception_propagates() -> None:
    """Tick errors propagate to the run() caller. The CLI entry's
    SIGTERM handler is the ONLY clean-exit path; bugs in tick logic
    crash the service and let the supervisor restart it. This matches
    M3.3's run_backlog_service / M5.1's run_circuit_breaker behaviour.
    """
    svc = _RaisingService()
    with pytest.raises(RuntimeError, match="synthetic tick failure"):
        await svc.run(max_ticks=5)


# =====================================================================
# 4. make_workflow_pool uses pgbouncer-compatible config.
# =====================================================================

async def test_make_workflow_pool_uses_pgbouncer_compatible_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`make_workflow_pool` MUST set `statement_cache_size=0` — the
    sixth activation after M3.1, M3.3, M4.2, M5.1, M5.2. M1.3 ADR Q1.
    """
    captured: dict[str, Any] = {}

    async def _spy(dsn: str, **kwargs: Any) -> Any:
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(asyncpg, "create_pool", _spy)
    await make_workflow_pool("postgresql://x@y/z")

    assert captured["kwargs"]["statement_cache_size"] == 0, (
        f"make_workflow_pool did NOT set statement_cache_size=0; "
        f"got {captured['kwargs'].get('statement_cache_size')}. "
        f"M6 workflow pool will NOT be pgbouncer-compatible in "
        f"transaction mode."
    )
    assert "min_size" in captured["kwargs"]
    assert "max_size" in captured["kwargs"]
