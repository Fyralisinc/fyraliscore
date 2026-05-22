"""services/forecasts/tests/test_router.py — integration tests for the
Forecasts HTTP surface.

Builds a FastAPI test app from the gateway factory and mounts the
forecasts router so the BearerAuthMiddleware + GatewayDeps wiring is
identical to production.
"""
from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

from services.forecasts.router import build_router
from services.gateway.main import build_app

from .conftest import seed_prediction, seed_signal


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def forecasts_client(app_deps) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Build the app exactly as production does, then attach the
    forecasts router."""
    app = build_app(
        pool=app_deps.pool,
        actor_repo=app_deps.actor_repo,
        alias_repo=app_deps.alias_repo,
        embedder=app_deps.embedder,
        rate_limiter=app_deps.rate_limiter,
        slack_signing_secret=app_deps.slack_signing_secret,
        configure_logging=False,
    )
    app.include_router(build_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test",
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_list_endpoint_rejects_unauthenticated(
    forecasts_client: httpx.AsyncClient,
):
    resp = await forecasts_client.get("/v1/forecasts")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_list_endpoint_returns_active_only_by_default(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="active 1", confidence=0.7, resolution_days=3,
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="resolved 1", confidence=0.7, status="resolved",
        resolution_days=-3, resolved_days_ago=3,
        outcome="true", timeliness="on_time",
    )
    resp = await forecasts_client.get(
        "/v1/forecasts",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["statement"] == "active 1"


@pytest.mark.asyncio
async def test_list_endpoint_filter_by_category(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="risk", category="customer_risk",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="cap", category="capacity",
    )
    resp = await forecasts_client.get(
        "/v1/forecasts?category=customer_risk",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["category"] == "customer_risk"


@pytest.mark.asyncio
async def test_summary_endpoint(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        confidence=0.85, resolution_days=4,
        impact={"arr_at_risk": 500_000},
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        confidence=0.55, resolution_days=8,
        impact={"arr_at_risk": 100_000},
    )
    resp = await forecasts_client.get(
        "/v1/forecasts/summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["active_count"] == 2
    assert body["at_risk_arr"] == pytest.approx(600_000)
    assert body["high_confidence_count"] == 1
    assert body["upcoming_resolutions_count_14d"] == 2
    # No resolved data → calibration is None.
    assert body["model_calibration"] is None
    assert body["calibration_delta"] is None


@pytest.mark.asyncio
async def test_detail_endpoint_returns_row_and_signals(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant, statement="inspect me",
    )
    await seed_signal(gateway_pool, prediction_id=pid, title="evidence 1")
    resp = await forecasts_client.get(
        f"/v1/forecasts/{pid}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["prediction"]["id"] == str(pid)
    assert len(body["signals"]) == 1


@pytest.mark.asyncio
async def test_detail_endpoint_returns_404_for_missing(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    resp = await forecasts_client.get(
        f"/v1/forecasts/{uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_accuracy_endpoint_shape(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    for _ in range(3):
        await seed_prediction(
            gateway_pool, tenant=registered_tenant,
            confidence=0.72, status="resolved",
            resolution_days=-2, resolved_days_ago=2,
            outcome="true", timeliness="on_time",
        )
    resp = await forecasts_client.get(
        "/v1/forecasts/accuracy",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert {b["bin_label"] for b in body["bins"]} == {
        "50-60", "60-70", "70-80", "80-90", "90-100",
    }
    bin_70 = next(b for b in body["bins"] if b["bin_label"] == "70-80")
    assert bin_70["n_resolved"] == 3
    assert bin_70["observed_hit_rate"] == pytest.approx(1.0)
    assert len(body["recent_resolutions"]) == 3
    assert body["calibration_summary"]["n_resolved_total"] == 3


@pytest.mark.asyncio
async def test_upcoming_endpoint(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="near", resolution_days=5,
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="far", resolution_days=40,
    )
    resp = await forecasts_client.get(
        "/v1/forecasts/upcoming?days=14",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["statement"] == "near"


@pytest.mark.asyncio
async def test_risk_exposure_endpoint(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        resolution_days=3, impact={"arr_at_risk": 100_000},
    )
    resp = await forecasts_client.get(
        "/v1/forecasts/risk_exposure?days=28&metric=arr_at_risk",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["metric"] == "arr_at_risk"
    assert body["range_days"] == 28
    assert len(body["buckets"]) >= 4
    total = sum(b["value"] for b in body["buckets"])
    assert total == pytest.approx(100_000)


@pytest.mark.asyncio
async def test_create_endpoint(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    payload = {
        "statement": "New scenario from CEO",
        "category": "strategy",
        "confidence": 0.6,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=14)
        ).isoformat(),
        "rationale": "ad hoc",
        "impact": {"arr_at_risk": 100_000},
    }
    resp = await forecasts_client.post(
        "/v1/forecasts",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["statement"] == "New scenario from CEO"
    assert body["tenant_id"] == str(registered_tenant)


@pytest.mark.asyncio
async def test_create_endpoint_validates_category(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    payload = {
        "statement": "bad",
        "category": "not_real",
        "confidence": 0.5,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=2)
        ).isoformat(),
    }
    resp = await forecasts_client.post(
        "/v1/forecasts",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 400
