"""Tests for services/resources/partitions.py."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from services.resources.partitions import (
    compute_partitions,
    ensure_next_n_months,
    ensure_partition_for,
    ensure_partitions,
    list_existing_partitions,
    partition_name,
)


def test_partition_name_format():
    assert partition_name("resource_transactions", date(2026, 4, 1)) == (
        "resource_transactions_2026_04"
    )


def test_compute_partitions_current_plus_three():
    specs = compute_partitions(as_of=date(2026, 4, 15), months_ahead=3)
    names = [s.name for s in specs]
    assert names == [
        "resource_transactions_2026_04",
        "resource_transactions_2026_05",
        "resource_transactions_2026_06",
        "resource_transactions_2026_07",
    ]


def test_compute_partitions_year_wrap():
    specs = compute_partitions(as_of=date(2026, 11, 1), months_ahead=3)
    names = [s.name for s in specs]
    assert names == [
        "resource_transactions_2026_11",
        "resource_transactions_2026_12",
        "resource_transactions_2027_01",
        "resource_transactions_2027_02",
    ]


@pytest.mark.asyncio
async def test_ensure_partitions_idempotent(resources_db):
    # Wave 0 migration already attached current + 3, so the first call
    # here should return empty (everything exists) for the default window.
    created_1 = await ensure_partitions(resources_db, months_ahead=3)
    assert created_1 == []
    # Running the same call again is a no-op.
    created_2 = await ensure_partitions(resources_db, months_ahead=3)
    assert created_2 == []


@pytest.mark.asyncio
async def test_ensure_next_n_months_extends_window(resources_db):
    # Extending to 5 months ahead should attach up to 2 new partitions
    # (months 4 and 5 ahead of current). A prior test or migration may
    # have already attached one or both; we assert idempotency of the
    # final list instead.
    await ensure_next_n_months(resources_db, 5)
    async with resources_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE p.relname = 'resource_transactions'
            """
        )
    names = {r["relname"] for r in rows}
    assert len(names) >= 6  # at least current + 5 ahead


@pytest.mark.asyncio
async def test_ensure_partition_for_specific_date(resources_db):
    # Date outside the Wave-0 window (say, 11 months ahead) forces creation.
    future = datetime.now(timezone.utc).replace(day=1)
    # Pick a date far in the future to guarantee a new partition.
    far = future.replace(year=future.year + 1)  # +12 months from today
    name = await ensure_partition_for(far, pool_or_conn=resources_db)
    existing = await list_existing_partitions(resources_db)
    assert name in existing


@pytest.mark.asyncio
async def test_list_existing_partitions_nonempty(resources_db):
    existing = await list_existing_partitions(resources_db)
    assert len(existing) >= 4  # Wave 0 created at minimum 4
    for e in existing:
        assert e.startswith("resource_transactions_")
