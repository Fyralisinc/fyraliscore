"""services/think/tests/test_region_locks.py — advisory-lock semantics.

Verifies:
  * pg_advisory_xact_lock is released on COMMIT.
  * pg_advisory_xact_lock is released on ROLLBACK.
  * Two runs on overlapping regions serialize.
  * Two runs on disjoint regions do NOT serialize.
"""
from __future__ import annotations

import asyncio
import time
from uuid import uuid4

import pytest

from services.think.region_locks import (
    acquire_region_lock, region_lock_key,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_lock_released_on_commit(fresh_db, tenant):
    """
    Acquire + commit. A second acquisition on the same key MUST NOT
    block (would block if _xact_ variant was misused).
    """
    entities = [("commitment", uuid4())]
    async with fresh_db.acquire() as c1:
        async with c1.transaction():
            await acquire_region_lock(c1, tenant, entities)
        # tx committed — second conn should acquire immediately.
    async with fresh_db.acquire() as c2:
        async with c2.transaction():
            t0 = time.monotonic()
            await acquire_region_lock(c2, tenant, entities)
            elapsed_ms = (time.monotonic() - t0) * 1000
            assert elapsed_ms < 100, (
                f"expected fast acquire after commit; took {elapsed_ms}ms"
            )


async def test_lock_released_on_rollback(fresh_db, tenant):
    """
    This is the test that catches the session-variant bug. If we
    mistakenly used `pg_advisory_lock` (session-scoped), the lock
    would survive rollback and the second acquire would hang
    indefinitely.
    """
    entities = [("commitment", uuid4())]
    async with fresh_db.acquire() as c1:
        try:
            async with c1.transaction():
                await acquire_region_lock(c1, tenant, entities)
                raise RuntimeError("intentional abort")
        except RuntimeError:
            pass
    # Lock should have released at rollback.
    async with fresh_db.acquire() as c2:
        async with c2.transaction():
            t0 = time.monotonic()
            await acquire_region_lock(c2, tenant, entities)
            elapsed_ms = (time.monotonic() - t0) * 1000
            assert elapsed_ms < 100, (
                f"lock did not release on rollback (_xact_ bug); {elapsed_ms}ms"
            )


async def test_overlapping_region_serializes(fresh_db, tenant):
    """
    Two concurrent transactions on the SAME region-key pair. The
    second MUST wait for the first. We prove this by holding the
    first lock for 150ms and measuring that the second acquires at
    roughly the 150ms mark.
    """
    entities = [("commitment", uuid4())]
    barrier_released = asyncio.Event()

    async def first_holder():
        async with fresh_db.acquire() as c:
            async with c.transaction():
                await acquire_region_lock(c, tenant, entities)
                await asyncio.sleep(0.15)
                barrier_released.set()

    async def second_waiter() -> float:
        # Give the first holder a head start.
        await asyncio.sleep(0.01)
        t0 = time.monotonic()
        async with fresh_db.acquire() as c:
            async with c.transaction():
                await acquire_region_lock(c, tenant, entities)
                return (time.monotonic() - t0) * 1000

    first_task = asyncio.create_task(first_holder())
    elapsed_ms = await second_waiter()
    await first_task
    assert elapsed_ms > 100, (
        f"expected second acquire to wait for first; elapsed={elapsed_ms}ms"
    )


async def test_disjoint_region_parallel(fresh_db, tenant):
    """
    Two runs on DIFFERENT region keys don't contend.
    """
    e1 = [("commitment", uuid4())]
    e2 = [("commitment", uuid4())]

    async def holder(entities, delay):
        async with fresh_db.acquire() as c:
            async with c.transaction():
                await acquire_region_lock(c, tenant, entities)
                await asyncio.sleep(delay)

    t0 = time.monotonic()
    await asyncio.gather(holder(e1, 0.15), holder(e2, 0.15))
    elapsed_ms = (time.monotonic() - t0) * 1000
    # Total should be ~150ms (parallel), not ~300ms (serialized).
    assert elapsed_ms < 280, (
        f"disjoint regions serialized incorrectly; elapsed={elapsed_ms}ms"
    )


async def test_region_key_deterministic(fresh_db, tenant):
    entities = [("commitment", uuid4()), ("goal", uuid4())]
    k1 = region_lock_key(tenant, entities)
    k2 = region_lock_key(tenant, entities)
    assert k1 == k2


async def test_region_key_differs_by_tenant():
    entities = [("commitment", uuid4())]
    k1 = region_lock_key(uuid4(), entities)
    k2 = region_lock_key(uuid4(), entities)
    assert k1 != k2
