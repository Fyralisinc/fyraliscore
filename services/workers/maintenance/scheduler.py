"""services/workers/maintenance/scheduler.py — in-process scheduler
for Wave 4-D maintenance jobs.

Each ``JobDescriptor`` has:
- a ``name`` (used for the advisory-lock key hash)
- a ``fn`` awaitable callable taking ``(pool,)``
- an ``interval`` timedelta between successive runs

The scheduler uses ``pg_advisory_lock`` (session-scoped) with a
per-job integer key so only one instance in the deployment can run a
given job at a time. This satisfies BUILD-PLAN §5 Prompt 4.D "each
maintenance worker has a lock preventing two instances from running
concurrently".

Spec note: production would use Kubernetes CronJobs or an external
scheduler; this in-process version is a correctness gate for Wave 4 and
is documented as a deviation.

Lifecycle:
- ``start()`` — spawn one ``asyncio.Task`` per JobDescriptor.
- ``stop()`` — cancel + await every spawned task.

Per-job semantics:
- Acquire ``pg_advisory_lock(key)`` on a fresh connection. If another
  process holds it, wait up to ``lock_timeout`` seconds (default 5),
  then skip this tick. Release after run or error.
- On exception, log and sleep ``interval`` before next attempt. Never
  crash the scheduler.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable

import asyncpg

from lib.shared.db import get_pool


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Advisory-lock key derivation
# ---------------------------------------------------------------------


def advisory_lock_key(job_name: str) -> int:
    """Derive a positive 31-bit int from the job name via SHA-256.

    ``pg_advisory_lock(int)`` accepts a bigint, but we clip to 31 bits so
    the value fits in Python's ``int`` without surprising negative-sign
    behavior in psql logs.
    """
    digest = hashlib.sha256(job_name.encode("utf-8")).digest()
    # Keep one sign bit free — positive 31-bit signed int range.
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


# ---------------------------------------------------------------------
# Job descriptor
# ---------------------------------------------------------------------


@dataclass
class JobDescriptor:
    """One scheduled job.

    ``fn`` receives the pool (positional). Return value is logged but
    not stored — each job is expected to write its own durable trail.

    ``initial_delay`` is the number of seconds to wait before the first
    run (useful for staggering a daily job away from process boot).
    """

    name: str
    fn: Callable[[asyncpg.Pool], Awaitable[Any]]
    interval: timedelta
    initial_delay: timedelta = timedelta(seconds=0)
    lock_timeout_seconds: float = 5.0
    enabled: bool = True


@dataclass
class _JobRuntime:
    descriptor: JobDescriptor
    task: asyncio.Task | None = None
    last_run_at: float | None = None
    runs: int = 0
    errors: int = 0


# ---------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------


class MaintenanceScheduler:
    """In-process asyncio scheduler with per-job advisory locks."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None = None,
        descriptors: list[JobDescriptor] | None = None,
    ) -> None:
        self._pool = pool
        self._jobs: dict[str, _JobRuntime] = {}
        self._stop = asyncio.Event()
        for d in descriptors or []:
            self.register(d)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, descriptor: JobDescriptor) -> None:
        if descriptor.name in self._jobs:
            raise ValueError(f"duplicate job name: {descriptor.name}")
        self._jobs[descriptor.name] = _JobRuntime(descriptor=descriptor)

    def stats(self) -> dict[str, dict[str, Any]]:
        out = {}
        for name, rt in self._jobs.items():
            out[name] = {
                "runs": rt.runs,
                "errors": rt.errors,
                "last_run_at": rt.last_run_at,
                "enabled": rt.descriptor.enabled,
            }
        return out

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        for rt in self._jobs.values():
            if not rt.descriptor.enabled:
                continue
            rt.task = asyncio.create_task(self._run_job_loop(rt))

    async def stop(self) -> None:
        """Cancel every task; await them. Idempotent."""
        self._stop.set()
        tasks = [rt.task for rt in self._jobs.values() if rt.task is not None]
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        for rt in self._jobs.values():
            rt.task = None

    # ------------------------------------------------------------------
    # Execution loops
    # ------------------------------------------------------------------

    async def _run_job_loop(self, rt: _JobRuntime) -> None:
        d = rt.descriptor
        # Initial delay — interruptible by shutdown.
        try:
            await asyncio.wait_for(
                self._stop.wait(),
                timeout=max(0.0, d.initial_delay.total_seconds()),
            )
            return  # stop was set during the initial delay
        except asyncio.TimeoutError:
            pass

        while not self._stop.is_set():
            try:
                await self._run_once_locked(rt)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                rt.errors += 1
                log.warning("scheduler job %s errored: %s", d.name, e)
            # Sleep until next tick OR shutdown.
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=max(0.1, d.interval.total_seconds()),
                )
                return
            except asyncio.TimeoutError:
                continue

    async def _run_once_locked(self, rt: _JobRuntime) -> None:
        """Acquire advisory lock (non-blocking variant with polling),
        run ``fn`` once, release the lock. If the lock isn't acquired
        within ``lock_timeout``, skip this tick.
        """
        d = rt.descriptor
        the_pool = self._pool or get_pool()
        key = advisory_lock_key(d.name)

        async with the_pool.acquire() as conn:
            got = await self._try_advisory_lock(
                conn, key, d.lock_timeout_seconds
            )
            if not got:
                log.info("scheduler: lock busy for %s — skipping tick", d.name)
                return
            try:
                await d.fn(the_pool)
                rt.runs += 1
                rt.last_run_at = asyncio.get_event_loop().time()
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", key)

    async def _try_advisory_lock(
        self,
        conn: asyncpg.Connection,
        key: int,
        timeout_seconds: float,
    ) -> bool:
        """Poll pg_try_advisory_lock up to timeout_seconds."""
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while True:
            got = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", key
            )
            if got:
                return True
            if asyncio.get_event_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.05)

    # ------------------------------------------------------------------
    # One-shot for tests
    # ------------------------------------------------------------------

    async def run_job_now(self, name: str) -> None:
        """Run a single registered job immediately under the advisory
        lock. Used by tests and by operators performing a manual one-off.
        """
        rt = self._jobs.get(name)
        if rt is None:
            raise KeyError(name)
        await self._run_once_locked(rt)


__all__ = [
    "advisory_lock_key",
    "JobDescriptor",
    "MaintenanceScheduler",
]
