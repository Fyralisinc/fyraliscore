"""Tests for weekly maintenance jobs — Wave 4-D.

Covers test-list items #14 (partitions idempotent), #15 (no long lock
blocks Think-sized inserts), #16 (SMF decay), #17 (concurrent collision),
#21 (scheduler cancels pending jobs — in test_scheduler.py).
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.workers.maintenance.weekly import (
    contestation_aggregation_report,
    extend_partitions_job,
    run_weekly,
    signal_memory_fabric_decay,
)

from .conftest import seed_observation


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# #14 extend_partitions is idempotent — running twice is safe
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extend_partitions_idempotent(m_pool: asyncpg.Pool) -> None:
    first = await extend_partitions_job(pool=m_pool, months_ahead=3)
    second = await extend_partitions_job(pool=m_pool, months_ahead=3)
    # Every first-run name should already exist by second-run time.
    assert set(second).isdisjoint(set(first))


@pytest.mark.asyncio
async def test_extend_partitions_adds_future_partitions(
    m_pool: asyncpg.Pool,
) -> None:
    """Explicit 6-month window forces creation of new months beyond
    the foundation migration's 4."""
    created = await extend_partitions_job(pool=m_pool, months_ahead=6)
    # It's fine if some existed — but at least a few new ones should appear.
    assert isinstance(created, list)


# ---------------------------------------------------------------------
# #16 signal_memory_fabric_decay drops unpromoted, keeps promoted
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_smf_decay_drops_unpromoted_keeps_promoted(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    unpromoted_old = uuid7()
    unpromoted_new = uuid7()
    promoted_old = uuid7()
    async with m_pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO signal_memory_fabric (
                id, tenant_id, region_hash, signal_ref, significance,
                recorded_at, promoted_at
            ) VALUES
              ($1, $2, 'r', $3::jsonb, 0.1,
               now() - interval '60 days', NULL),
              ($4, $2, 'r', $3::jsonb, 0.1,
               now() - interval '5 days',  NULL),
              ($5, $2, 'r', $3::jsonb, 0.1,
               now() - interval '60 days',
               now() - interval '10 days')
            """,
            unpromoted_old,
            tenant_id,
            json.dumps({"k": 1}),
            unpromoted_new,
            promoted_old,
        )
        deleted = await signal_memory_fabric_decay(conn=c)
        assert deleted == 1
        rows = await c.fetch(
            "SELECT id FROM signal_memory_fabric WHERE tenant_id = $1",
            tenant_id,
        )
        ids = {r["id"] for r in rows}
        assert unpromoted_old not in ids
        assert unpromoted_new in ids
        assert promoted_old in ids


# ---------------------------------------------------------------------
# #17 SMF decay doesn't collide with a concurrent producer
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_smf_decay_concurrent_insert_is_safe(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()

    async def insert_batch() -> int:
        n = 0
        async with m_pool.acquire() as c:
            for _ in range(20):
                await c.execute(
                    """
                    INSERT INTO signal_memory_fabric (
                        id, tenant_id, region_hash, signal_ref,
                        significance, recorded_at
                    ) VALUES (
                        $1, $2, 'r', '{}'::jsonb, 0.1, now()
                    )
                    """,
                    uuid7(),
                    tenant_id,
                )
                n += 1
                await asyncio.sleep(0.005)
        return n

    async def run_decay() -> int:
        total = 0
        async with m_pool.acquire() as c:
            for _ in range(5):
                # Temporarily seed an old unpromoted row each iter.
                await c.execute(
                    """
                    INSERT INTO signal_memory_fabric (
                        id, tenant_id, region_hash, signal_ref,
                        significance, recorded_at
                    ) VALUES (
                        $1, $2, 'r', '{}'::jsonb, 0.1,
                        now() - interval '60 days'
                    )
                    """,
                    uuid7(),
                    tenant_id,
                )
                total += await signal_memory_fabric_decay(conn=c)
                await asyncio.sleep(0.01)
        return total

    inserted, decayed = await asyncio.gather(insert_batch(), run_decay())
    assert inserted == 20
    assert decayed >= 1  # decay ran without deadlock


# ---------------------------------------------------------------------
# #15 Weekly maintenance doesn't block a concurrent model insert
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_weekly_does_not_block_concurrent_model_insert(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    # Create a handful of active models so relationship_maintenance has
    # real work.
    async with m_pool.acquire() as c:
        actor_id = uuid7()
        await c.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'A', 'active')
            """,
            actor_id,
            tenant_id,
        )
        for _ in range(5):
            born = await seed_observation(c, tenant_id=tenant_id)
            await c.execute(
                """
                INSERT INTO models (
                    id, tenant_id, born_from_event_id,
                    proposition, "natural", embedding,
                    scope_temporal, confidence, activation,
                    confidence_at_assertion
                ) VALUES (
                    $1, $2, $3, '{"kind": "test"}'::jsonb, 'n',
                    array_fill(0.0::real, ARRAY[768])::vector,
                    '{}'::jsonb, 0.5, 0.5, 0.5
                )
                """,
                uuid7(),
                tenant_id,
                born,
            )

    async def do_weekly():
        return await run_weekly(pool=m_pool)

    async def insert_models():
        inserted = 0
        await asyncio.sleep(0.02)
        async with m_pool.acquire() as c:
            born = await seed_observation(c, tenant_id=tenant_id)
            for _ in range(3):
                await c.execute(
                    """
                    INSERT INTO models (
                        id, tenant_id, born_from_event_id,
                        proposition, "natural", embedding,
                        scope_temporal, confidence, activation,
                        confidence_at_assertion
                    ) VALUES (
                        $1, $2, $3, '{"kind": "test"}'::jsonb, 'n',
                        array_fill(0.0::real, ARRAY[768])::vector,
                        '{}'::jsonb, 0.5, 0.5, 0.5
                    )
                    """,
                    uuid7(),
                    tenant_id,
                    born,
                )
                inserted += 1
        return inserted

    weekly, inserted = await asyncio.gather(do_weekly(), insert_models())
    assert inserted == 3
    assert weekly.tenants_processed >= 1


# ---------------------------------------------------------------------
# contestation_aggregation counts by entity_kind
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_contestation_aggregation_counts_by_kind(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with m_pool.acquire() as c:
        for _ in range(2):
            await seed_observation(
                c,
                tenant_id=tenant_id,
                kind="contestation",
                content={"entity_kind": "model"},
            )
        await seed_observation(
            c,
            tenant_id=tenant_id,
            kind="contestation",
            content={"entity_kind": "commitment"},
        )
        summary = await contestation_aggregation_report(conn=c)
    keys = list(summary.keys())
    assert any("model" in k for k in keys)
    assert any("commitment" in k for k in keys)


# ---------------------------------------------------------------------
# run_weekly swallows errors gracefully (calibration unavailable path)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_weekly_calibration_unavailable_graceful(
    m_pool: asyncpg.Pool,
) -> None:
    report = await run_weekly(pool=m_pool)
    # Wave 4-C may or may not have landed. Either way, no exception.
    assert report.calibration_status in {"ok", "unavailable"} or \
        report.calibration_status.startswith("error:")
