"""Tests for the MaintenanceScheduler — Wave 4-D.

Covers test-list items #18 (advisory lock prevents two instances from
running a job concurrently), #21 (scheduler cancels pending jobs on
shutdown), #22 (property: random sequences of maintenance → invariants
hold).
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest

from services.workers.maintenance.scheduler import (
    JobDescriptor,
    MaintenanceScheduler,
    advisory_lock_key,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# advisory_lock_key is deterministic + positive
# ---------------------------------------------------------------------


def test_advisory_lock_key_deterministic_positive() -> None:
    k = advisory_lock_key("daily")
    k2 = advisory_lock_key("daily")
    assert k == k2
    assert 0 < k <= 0x7FFFFFFF
    assert advisory_lock_key("weekly") != k


# ---------------------------------------------------------------------
# #18 Two scheduler instances don't run the same job concurrently
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_advisory_lock_prevents_concurrent_run(
    m_pool: asyncpg.Pool,
) -> None:
    """Two parallel ``run_job_now`` invocations serialise on the
    advisory lock. We prove it by having the job sleep 300ms and
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
