"""Tests for monthly maintenance jobs — Wave 4-D.

Covers test-list item #20 (vacuum-analyze on empty partition doesn't
throw) + activation histogram + uncontested-high-confidence report.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.workers.maintenance.monthly import (
    activation_histogram_report,
    old_partition_migration_notes,
    run_monthly,
    uncontested_high_confidence_report,
    vacuum_analyze_foundation,
)

from .conftest import seed_observation


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# #20 vacuum_analyze completes without throwing on empty tables
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_vacuum_analyze_empty_ok(m_pool: asyncpg.Pool) -> None:
    vacuumed = await vacuum_analyze_foundation(pool=m_pool)
    # Expect every foundation table to be present.
    assert "actors" in vacuumed
    assert any(v.startswith("observations_") for v in vacuumed)


# ---------------------------------------------------------------------
# activation_histogram_report returns percentiles per kind
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_activation_histogram_report_buckets_by_kind(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with m_pool.acquire() as c:
        born = await seed_observation(c, tenant_id=tenant_id)
        for i in range(5):
            await c.execute(
                """
                INSERT INTO models (
                    id, tenant_id, born_from_event_id,
                    proposition, "natural", embedding,
                    scope_temporal, confidence, activation,
                    confidence_at_assertion
                ) VALUES (
                    $1, $2, $3,
                    $4::jsonb, 'n',
                    array_fill(0.0::real, ARRAY[768])::vector,
                    '{}'::jsonb, 0.5, $5, 0.5
                )
                """,
                uuid7(),
                tenant_id,
                born,
                '{"kind": "prediction"}',
                float(0.1 * (i + 1)),
            )
        buckets = await activation_histogram_report(conn=c)
    assert any(k.endswith(":prediction") for k in buckets.keys())


# ---------------------------------------------------------------------
# uncontested_high_confidence_report lists qualifying ids
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uncontested_high_confidence_report(
    m_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with m_pool.acquire() as c:
        born = await seed_observation(c, tenant_id=tenant_id)
        qualifying = uuid7()
        non_qualifying_new = uuid7()
        non_qualifying_contested = uuid7()
        # Old, high confidence, no contest.
        await c.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal, confidence, activation,
                confidence_at_assertion,
                contested_count, created_at
            ) VALUES (
                $1, $2, $3, '{"kind": "test"}'::jsonb, 'n',
                array_fill(0.0::real, ARRAY[768])::vector,
                '{}'::jsonb, 0.9, 1.0, 0.9,
                0, now() - interval '120 days'
            )
            """,
            qualifying,
            tenant_id,
            born,
        )
        # Too young.
        await c.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal, confidence, activation,
                confidence_at_assertion,
                contested_count
            ) VALUES (
                $1, $2, $3, '{"kind": "test"}'::jsonb, 'n',
                array_fill(0.0::real, ARRAY[768])::vector,
                '{}'::jsonb, 0.9, 1.0, 0.9, 0
            )
            """,
            non_qualifying_new,
            tenant_id,
            born,
        )
        # Contested.
        await c.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal, confidence, activation,
                confidence_at_assertion,
                contested_count, created_at
            ) VALUES (
                $1, $2, $3, '{"kind": "test"}'::jsonb, 'n',
                array_fill(0.0::real, ARRAY[768])::vector,
                '{}'::jsonb, 0.9, 1.0, 0.9, 2,
                now() - interval '120 days'
            )
            """,
            non_qualifying_contested,
            tenant_id,
            born,
        )
        ids = await uncontested_high_confidence_report(conn=c)
    assert str(qualifying) in ids
    assert str(non_qualifying_new) not in ids
    assert str(non_qualifying_contested) not in ids


# ---------------------------------------------------------------------
# old_partition_migration_notes returns [] on a fresh DB
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_old_partition_notes_empty_by_default(
    m_pool: asyncpg.Pool,
) -> None:
    async with m_pool.acquire() as c:
        old = await old_partition_migration_notes(conn=c, old_days=365)
    # No partition more than a year old by default.
    assert old == []


# ---------------------------------------------------------------------
# run_monthly composes cleanly on empty tables
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_monthly_empty_ok(m_pool: asyncpg.Pool) -> None:
    report = await run_monthly(pool=m_pool)
    assert not report.errors
    assert len(report.vacuumed_tables) > 0
