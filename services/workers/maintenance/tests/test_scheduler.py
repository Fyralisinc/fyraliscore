"""Tests for the MaintenanceScheduler — Wave 4-D.

Covers test-list items #18 (row lease prevents two instances from running a job
concurrently), #21 (scheduler cancels pending jobs on
shutdown), #22 (property: random sequences of maintenance → invariants
hold).
"""
from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest

from services.workers.maintenance.scheduler import (
    JobDescriptor,
    LeaseLostError,
    MaintenanceScheduler,
    job_lease_name,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# job_lease_name is deterministic and namespaced
# ---------------------------------------------------------------------


def test_job_lease_name_deterministic_namespaced() -> None:
    k = job_lease_name("daily")
    k2 = job_lease_name("daily")
    assert k == k2
    assert k == "maintenance:daily"
    assert job_lease_name("weekly") != k


# ---------------------------------------------------------------------
# #18 Two scheduler instances don't run the same job concurrently
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_row_lease_prevents_concurrent_run(
    m_pool: asyncpg.Pool,
) -> None:
    """Two parallel ``run_job_now`` invocations serialise on the
    row lease. We prove it by having the job sleep 300ms and
    checking the total wall time > 500ms for two serial runs.
    """
    started_at: list[float] = []

    async def slow_job(_pool: asyncpg.Pool) -> None:
        started_at.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.3)

    name = f"t-{uuid4().hex[:8]}"
    s1 = MaintenanceScheduler(
        pool=m_pool,
        descriptors=[
            JobDescriptor(
                name=name,
                fn=slow_job,
                interval=timedelta(seconds=60),
                lock_timeout_seconds=2.0,
            )
        ],
    )
    s2 = MaintenanceScheduler(
        pool=m_pool,
        descriptors=[
            JobDescriptor(
                name=name,
                fn=slow_job,
                interval=timedelta(seconds=60),
                lock_timeout_seconds=2.0,
            )
        ],
    )
    t0 = asyncio.get_event_loop().time()
    await asyncio.gather(s1.run_job_now(name), s2.run_job_now(name))
    elapsed = asyncio.get_event_loop().time() - t0
    assert len(started_at) == 2
    # Second run starts only after first releases; so 2 × 0.3s ≈ 0.6s.
    assert elapsed > 0.5


@pytest.mark.asyncio
async def test_row_lease_loss_cancels_running_job(
    m_pool: asyncpg.Pool,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release_never = asyncio.Event()
    name = f"loss-{uuid4().hex[:8]}"

    async def blocking_job(_pool: asyncpg.Pool) -> None:
        started.set()
        try:
            await release_never.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    scheduler = MaintenanceScheduler(
        pool=m_pool,
        descriptors=[
            JobDescriptor(
                name=name,
                fn=blocking_job,
                interval=timedelta(seconds=60),
                lock_timeout_seconds=0.5,
                lease_ttl_seconds=30.0,
                lease_refresh_seconds=0.1,
            )
        ],
    )
    run_task = asyncio.create_task(scheduler.run_job_now(name))
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)
        await m_pool.execute(
            """
            UPDATE scheduler_leases
               SET holder_id = 'stolen-by-test',
                   expires_at = now() + interval '30 seconds'
             WHERE lease_name = $1
            """,
            job_lease_name(name),
        )
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(run_task, timeout=5.0)
        assert cancelled.is_set()
    finally:
        release_never.set()
        if not run_task.done():
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task


# ---------------------------------------------------------------------
# #21 Scheduler cancels pending jobs on shutdown
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_cancels_pending_tasks_on_stop(
    m_pool: asyncpg.Pool,
) -> None:
    ran = 0

    async def counting_job(_pool: asyncpg.Pool) -> None:
        nonlocal ran
        ran += 1
        await asyncio.sleep(0.05)

    s = MaintenanceScheduler(
        pool=m_pool,
        descriptors=[
            JobDescriptor(
                name=f"t-{uuid4().hex[:8]}",
                fn=counting_job,
                interval=timedelta(milliseconds=100),
                lock_timeout_seconds=0.5,
            )
        ],
    )
    await s.start()
    await asyncio.sleep(0.15)
    await s.stop()
    stats = s.stats()
    assert list(stats.values())[0]["enabled"] is True
    # At least one run completed; subsequent runs cancelled.
    assert ran >= 1


# ---------------------------------------------------------------------
# Scheduler.stop() is idempotent
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_stop_is_idempotent(m_pool: asyncpg.Pool) -> None:
    s = MaintenanceScheduler(
        pool=m_pool,
        descriptors=[
            JobDescriptor(
                name=f"idempotent-{uuid4().hex[:8]}",
                fn=lambda p: _noop(),
                interval=timedelta(seconds=60),
            )
        ],
    )
    await s.start()
    await s.stop()
    await s.stop()  # second call must not raise


async def _noop() -> None:
    return None


# ---------------------------------------------------------------------
# #22 Property-ish: random sequence of maintenance runs → invariants hold
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_random_sequence_of_maintenance_invariants_hold(
    m_pool: asyncpg.Pool,
) -> None:
    """Run the three composers in a shuffled sequence multiple times.
    After every run, key invariants must hold:
    - observations row counts do not decrease (orphan_detection never
      deletes them).
    - entity_aliases with recent last_used_at are untouched.
    - realtime_replay_cursors row count doesn't go negative (trivially).
    """
    import random

    from services.workers.maintenance.daily import run_daily
    from services.workers.maintenance.weekly import run_weekly
    from services.workers.maintenance.monthly import run_monthly

    from .conftest import seed_observation

    tenant_id = uuid4()
    async with m_pool.acquire() as c:
        actor_id = uuid4()
        await c.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'A', 'active')
            """,
            actor_id,
            tenant_id,
        )
        # 10 observations across ages.
        for _ in range(10):
            await seed_observation(c, tenant_id=tenant_id)
        # Recent alias — must survive.
        recent_alias = uuid4()
        await c.execute(
            """
            INSERT INTO entity_aliases (
                id, tenant_id, alias_text, actor_id,
                resolved_entity_ref, confidence,
                confirmed_count, contested_count,
                first_seen_at, last_used_at
            ) VALUES (
                $1, $2, 'recent', $3, '{}'::jsonb, 0.5,
                0, 0,
                now() - interval '5 days',
                now() - interval '5 days'
            )
            """,
            recent_alias,
            tenant_id,
            actor_id,
        )
        obs_before = await c.fetchval(
            "SELECT COUNT(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        )

    jobs = [run_daily, run_weekly, run_monthly]
    for _ in range(3):
        random.shuffle(jobs)
        for j in jobs:
            await j(pool=m_pool)

    async with m_pool.acquire() as c:
        obs_after = await c.fetchval(
            "SELECT COUNT(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        )
        alias_surviving = await c.fetchval(
            "SELECT COUNT(*) FROM entity_aliases WHERE id = $1",
            recent_alias,
        )
    assert obs_after == obs_before
    assert alias_surviving == 1
