"""services/forecasts/tests/test_accuracy.py — bin + summary sanity."""
from __future__ import annotations

from uuid import UUID

import asyncpg
import pytest

from services.forecasts import accuracy as accuracy_mod

from .conftest import seed_prediction


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_accuracy_bins_returns_all_five_buckets(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    async with gateway_pool.acquire() as conn:
        bins = await accuracy_mod.accuracy_bins(conn, registered_tenant)
    assert [b.bin_label for b in bins] == ["50-60", "60-70", "70-80",
                                            "80-90", "90-100"]
    # No resolved samples → every bin should be None observed_hit_rate.
    for b in bins:
        assert b.n_resolved == 0
        assert b.observed_hit_rate is None


@pytest.mark.asyncio
async def test_accuracy_bins_computes_hit_rate_when_enough_samples(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    # Seed 4 resolved predictions in the 70-80 bin:
    # 3 outcome=true, 1 outcome=false → expected rate = 0.75.
    for _ in range(3):
        await seed_prediction(
            gateway_pool, tenant=registered_tenant,
            statement="hit", confidence=0.74, status="resolved",
            resolution_days=-3, resolved_days_ago=3,
            outcome="true", timeliness="on_time",
        )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="miss", confidence=0.78, status="resolved",
        resolution_days=-3, resolved_days_ago=3,
        outcome="false", timeliness="late",
    )

    async with gateway_pool.acquire() as conn:
        bins = await accuracy_mod.accuracy_bins(conn, registered_tenant)
    bin_70 = next(b for b in bins if b.bin_label == "70-80")
    assert bin_70.n_resolved == 4
    assert bin_70.observed_hit_rate == pytest.approx(0.75)
    # Other bins still empty.
    for b in bins:
        if b.bin_label != "70-80":
            assert b.n_resolved == 0


@pytest.mark.asyncio
async def test_accuracy_bin_below_min_samples_returns_none(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    # 2 samples (below MIN_BIN_SAMPLES=3) → observed_hit_rate is None
    # but n_resolved is still reported honestly.
    for _ in range(2):
        await seed_prediction(
            gateway_pool, tenant=registered_tenant,
            confidence=0.82, status="resolved",
            resolution_days=-3, resolved_days_ago=3,
            outcome="true", timeliness="on_time",
        )
    async with gateway_pool.acquire() as conn:
        bins = await accuracy_mod.accuracy_bins(conn, registered_tenant)
    bin_80 = next(b for b in bins if b.bin_label == "80-90")
    assert bin_80.n_resolved == 2
    assert bin_80.observed_hit_rate is None


@pytest.mark.asyncio
async def test_partial_outcomes_count_as_half(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    # 3 partials in 60-70 bin → rate = 0.5.
    for _ in range(3):
        await seed_prediction(
            gateway_pool, tenant=registered_tenant,
            confidence=0.65, status="resolved",
            resolution_days=-1, resolved_days_ago=1,
            outcome="partial", timeliness="on_time",
        )
    async with gateway_pool.acquire() as conn:
        bins = await accuracy_mod.accuracy_bins(conn, registered_tenant)
    bin_60 = next(b for b in bins if b.bin_label == "60-70")
    assert bin_60.observed_hit_rate == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_recent_resolutions_orders_newest_first(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    older = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="older", confidence=0.7, status="resolved",
        resolution_days=-20, resolved_days_ago=20,
        outcome="true", timeliness="on_time",
    )
    newer = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="newer", confidence=0.7, status="resolved",
        resolution_days=-2, resolved_days_ago=2,
        outcome="false", timeliness="late",
    )
    async with gateway_pool.acquire() as conn:
        rows = await accuracy_mod.recent_resolutions(conn, registered_tenant)
    assert [r.id for r in rows] == [newer, older]


@pytest.mark.asyncio
async def test_calibration_summary_with_no_data(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    async with gateway_pool.acquire() as conn:
        s = await accuracy_mod.calibration_summary(conn, registered_tenant)
    assert s.value is None
    assert s.delta_vs_last_week is None
    assert s.n_resolved_total == 0


@pytest.mark.asyncio
async def test_calibration_summary_with_resolved_rows(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    # Confidence 0.7, outcome true → |0.7 - 1| = 0.3. Score = 0.7.
    for _ in range(3):
        await seed_prediction(
            gateway_pool, tenant=registered_tenant,
            confidence=0.7, status="resolved",
            resolution_days=-2, resolved_days_ago=2,
            outcome="true", timeliness="on_time",
        )
    async with gateway_pool.acquire() as conn:
        s = await accuracy_mod.calibration_summary(conn, registered_tenant)
    assert s.value is not None
    assert s.value == pytest.approx(0.7)
    assert s.n_resolved_total == 3
