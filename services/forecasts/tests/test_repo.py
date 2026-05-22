"""services/forecasts/tests/test_repo.py — direct asyncpg-level
tests for the repo functions."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from services.forecasts import repo as repo_mod

from .conftest import seed_prediction, seed_signal


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_list_predictions_filters_by_status_and_category(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="active customer risk", category="customer_risk",
        confidence=0.7, status="active",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="active capacity", category="capacity",
        confidence=0.6, status="active",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="resolved risk", category="customer_risk",
        confidence=0.8, status="resolved",
        resolution_days=-5, resolved_days_ago=2,
        outcome="true", timeliness="on_time",
    )

    async with gateway_pool.acquire() as conn:
        actives = await repo_mod.list_predictions(
            conn, registered_tenant, status="active",
        )
        assert len(actives) == 2
        assert {a.category for a in actives} == {"customer_risk", "capacity"}

        risks = await repo_mod.list_predictions(
            conn, registered_tenant,
            status="active", category="customer_risk",
        )
        assert len(risks) == 1
        assert risks[0].statement == "active customer risk"


@pytest.mark.asyncio
async def test_list_predictions_sorts_by_earliest_resolution(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    pid_far = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="far",
        confidence=0.6, resolution_days=20,
    )
    pid_near = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="near",
        confidence=0.7, resolution_days=3,
    )
    async with gateway_pool.acquire() as conn:
        rows = await repo_mod.list_predictions(
            conn, registered_tenant, sort="earliest_resolution",
        )
    assert [r.id for r in rows] == [pid_near, pid_far]


@pytest.mark.asyncio
async def test_get_prediction_returns_signals(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="row with signals",
    )
    await seed_signal(gateway_pool, prediction_id=pid,
                      title="signal A", ordinal=0)
    await seed_signal(gateway_pool, prediction_id=pid,
                      title="signal B", ordinal=1)

    async with gateway_pool.acquire() as conn:
        detail = await repo_mod.get_prediction(
            conn, registered_tenant, pid,
        )
    assert detail is not None
    assert detail.prediction.id == pid
    assert [s.title for s in detail.signals] == ["signal A", "signal B"]


@pytest.mark.asyncio
async def test_get_prediction_returns_none_on_miss(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    from uuid import uuid4
    async with gateway_pool.acquire() as conn:
        detail = await repo_mod.get_prediction(
            conn, registered_tenant, uuid4(),
        )
    assert detail is None


@pytest.mark.asyncio
async def test_create_prediction_inserts_row_and_signals(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    payload = {
        "tenant_id": registered_tenant,
        "statement": "Created via repo",
        "category": "pricing",
        "confidence": 0.55,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=10)
        ).isoformat(),
        "impact": {"arr_at_risk": 50000},
        "key_drivers": [{"label": "x", "delta_label": "+1", "direction": "up"}],
        "signals": [
            {"source": "slack", "title": "thread",
             "ts": datetime.now(timezone.utc).isoformat()},
            {"source": "email", "title": "ping",
             "ts": datetime.now(timezone.utc).isoformat(),
             "ordinal": 1, "weight": 0.6, "trust_tier": "observed"},
        ],
    }
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            row = await repo_mod.create_prediction(conn, payload)

    assert row.statement == "Created via repo"
    assert row.category == "pricing"

    async with gateway_pool.acquire() as conn:
        detail = await repo_mod.get_prediction(conn, registered_tenant, row.id)
    assert detail is not None
    assert len(detail.signals) == 2
    assert detail.signals[0].source in ("slack", "email")


@pytest.mark.asyncio
async def test_create_prediction_rejects_invalid_category(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    from lib.shared.errors import ValidationError
    payload = {
        "tenant_id": registered_tenant,
        "statement": "bad",
        "category": "not_a_category",
        "confidence": 0.5,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=1)
        ).isoformat(),
    }
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError):
                await repo_mod.create_prediction(conn, payload)


@pytest.mark.asyncio
async def test_resolve_prediction_sets_outcome_and_timeliness(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant, statement="to resolve",
    )
    async with gateway_pool.acquire() as conn:
        row = await repo_mod.resolve_prediction(
            conn, pid, "true", "early",
        )
    assert row.status == "resolved"
    assert row.outcome == "true"
    assert row.resolution_timeliness == "early"
    assert row.resolved_at is not None


@pytest.mark.asyncio
async def test_upcoming_resolutions_only_returns_active_in_window(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    near = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="near", resolution_days=5,
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="far",  resolution_days=30,
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="resolved", status="resolved",
        resolution_days=3, resolved_days_ago=1,
        outcome="true", timeliness="on_time",
    )

    async with gateway_pool.acquire() as conn:
        rows = await repo_mod.upcoming_resolutions(
            conn, registered_tenant, days=14,
        )
    assert [r.id for r in rows] == [near]


@pytest.mark.asyncio
async def test_summary_counters(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="hi conf",
        confidence=0.85, resolution_days=5,
        impact={"arr_at_risk": 1_000_000},
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="lo conf",
        confidence=0.55, resolution_days=8,
        impact={"arr_at_risk": 200_000},
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="outside window",
        confidence=0.9, resolution_days=60,
        impact={"arr_at_risk": 50_000},
    )

    async with gateway_pool.acquire() as conn:
        s = await repo_mod.summary_counters(conn, registered_tenant)
    assert s["active_count"] == 3
    # 1M + 200k + 50k.
    assert s["at_risk_arr"] == pytest.approx(1_250_000)
    assert s["high_confidence_count"] == 2
    assert s["upcoming_resolutions_count_14d"] == 2


@pytest.mark.asyncio
async def test_risk_exposure_series_builds_contiguous_weeks(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID,
):
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        resolution_days=3,
        impact={"arr_at_risk": 100_000},
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        resolution_days=10,
        impact={"arr_at_risk": 250_000},
    )
    async with gateway_pool.acquire() as conn:
        series = await repo_mod.risk_exposure_series(
            conn, registered_tenant,
            metric="arr_at_risk", range_days=28,
        )
    assert len(series) >= 4
    total = sum(b["value"] for b in series)
    # Two predictions with combined 350k should be summed across the
    # weekly buckets that overlap them.
    assert total == pytest.approx(350_000)


@pytest.mark.asyncio
async def test_tenant_isolation(
    gateway_pool: asyncpg.Pool, registered_tenant: UUID, tenant_id_b: UUID,
):
    # Register tenant B too.
    await gateway_pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tenant_id_b, "forecasts_test_b",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant, statement="mine",
    )
    await seed_prediction(
        gateway_pool, tenant=tenant_id_b, statement="theirs",
    )
    async with gateway_pool.acquire() as conn:
        mine = await repo_mod.list_predictions(conn, registered_tenant)
    assert [m.statement for m in mine] == ["mine"]
