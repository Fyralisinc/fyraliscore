"""services/ingestion/workflows/runtime.py
   — Long-running asyncio service skeleton for M6 workflows.

Per [04-implementation-plan.md §M6 pattern-alignment requirements]:
this module provides `LongRunningService`, the abstract base every
M6 asyncio orchestration service inherits, plus `make_workflow_pool`,
the pgbouncer-compatible asyncpg pool factory.

============================================================
PATTERN-ALIGNMENT EXEMPTION
============================================================
This module is one of three substrate modules (state.py, signals.py,
runtime.py) that may import `asyncpg` directly. Every concrete
workflow service (subclass of `LongRunningService`) MUST go through
the substrate modules — `state.load_state`, `signals.poll_signals`,
etc. — instead of importing asyncpg itself. The pattern-alignment
static analyzer (M6.0 Phase 3) enforces this.

============================================================
M3.3 + M5.1 PRECEDENT
============================================================
This base class generalises the shape `run_backlog_service` (M3.3)
and `run_circuit_breaker` (M5.1) had as free functions:
  - long while-loop with `stop_event.is_set()` exit.
  - `max_ticks` parameter for tests.
  - SIGTERM-handled clean exit at the CLI entry.
  - asyncpg pool with `statement_cache_size=0` (pgbouncer transaction
    mode; M1.3 ADR Q1).

Concrete subclasses implement `tick()` and a `tick_interval_seconds`
property. The base class owns the loop, the stop logic, and the
sleep-between-ticks. This keeps the orchestration code (the
subclass) thin and the substrate code (this module) consistent.

============================================================
PGBOUNCER POOL — SIXTH ACTIVATION
============================================================
`make_workflow_pool` is the sixth `statement_cache_size=0` activation:
  1. M3.1 DLQ writer
  2. M3.3 backlog drainer
  3. M4.2 session-state pool
  4. M5.1 circuit-breaker pool
  5. M5.2 writer full-mode pool
  6. M6.0 workflow substrate pool (this file)

Constants are M1.3 ADR Q1.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

import asyncpg


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Pool helper — pgbouncer-compatible. Sixth activation after M3.1,
# M3.3, M4.2, M5.1, M5.2. M1.3 ADR Q1.
# ---------------------------------------------------------------------
async def make_workflow_pool(
    dsn: str,
    *,
    min_size: int = 1,
    max_size: int = 5,
    command_timeout: float = 30.0,
) -> asyncpg.Pool:
    """Construct the asyncpg pool used by M6 workflow services.

    `statement_cache_size=0` per the M1.3 ADR Q1 pgbouncer-transaction-
    mode contract (same shape as
    `services.ingestion.feature_flags.circuit_breaker.make_breaker_pool`
    and `services.ingestion.writers.observation_writer.make_writer_pool`).

    Default sizing is small (min=1, max=5) because M6 services are
    cursor-style — at most one tick in flight per service. Per-source
    services that fan out internally should pass a larger `max_size`.
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        statement_cache_size=0,  # pgbouncer transaction mode (M1.3 ADR Q1)
    )


# ---------------------------------------------------------------------
# Long-running service base.
# ---------------------------------------------------------------------
class LongRunningService(ABC):
    """Abstract base for M6 asyncio workflow services.

    Subclasses MUST implement:
      - `tick()` — one iteration. Idempotent: SIGTERM mid-loop is
        clean because the next start replays from the persisted state
        row anyway.
      - `tick_interval_seconds` property — seconds between ticks.
        The base class sleeps `await asyncio.wait_for(stop_event.wait(),
        timeout=tick_interval_seconds)` so SIGTERM interrupts the
        sleep cleanly.

    Subclasses MUST honour the [M6 pattern-alignment requirements]
    (../../../docs/ingestion/04-implementation-plan.md):
      1. Orchestration separated from side effects — `tick()` calls
         named functions (state.py / signals.py / per-source modules)
         instead of doing DB/Kafka I/O inline.
      2. State in Postgres, not memory — no instance attributes
         survive a SIGTERM-restart; all progress-bearing state lives
         in `workflow_states.state_data`.
      3. Retry logic in named functions — `tick()` MAY call helpers
         from `retry.py` but MUST NOT contain inline `try/except`
         retry loops.
      4. Signals via Postgres polling — cross-service handoffs go
         through `signals.poll_signals`, not `asyncio.Queue`.
      5. No cross-workflow shared in-process state — no module-level
         dicts, no class-level mutable defaults, no shared singletons
         between services.

    The static analyzer (M6.0 Phase 3,
    `tests/test_pattern_alignment.py`) enforces these as gate tests.
    """

    @property
    @abstractmethod
    def tick_interval_seconds(self) -> float:
        """Seconds to sleep between consecutive ticks."""

    @abstractmethod
    async def tick(self) -> None:
        """One iteration of the service's main loop.

        Idempotent under SIGTERM-restart: the substrate persists
        state via `state.persist_state` /
        `state.advance_cursor_atomic_with_kafka_publish` BEFORE
        returning, so a SIGTERM right after `tick()` returns does not
        lose work.
        """

    async def run(
        self,
        *,
        max_ticks: int | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> int:
        """Main loop. Returns the number of ticks executed.

        Stops on:
          - `stop_event.set()` (the CLI entry installs this via
            SIGTERM/SIGINT handlers).
          - `max_ticks` reached (tests pass a finite count; production
            passes None for forever).

        Between ticks, sleeps for `tick_interval_seconds` via
        `asyncio.wait_for(stop_event.wait(), ...)` so a SIGTERM
        interrupts the sleep cleanly.
        """
        stop_event = stop_event or asyncio.Event()
        ticks = 0
        while not stop_event.is_set():
            if max_ticks is not None and ticks >= max_ticks:
                break
            ticks += 1
            await self.tick()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.tick_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
        return ticks


__all__ = [
    "LongRunningService",
    "make_workflow_pool",
]
