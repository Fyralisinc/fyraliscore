"""services/product/forecasts/tests/test_router.py — integration tests for the
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

from services.app.gateway.main import build_app
from services.app.gateway.tests.test_dashboard_router import (
    _auth,
    _seed_commitment,
)
from services.product.forecasts.router import build_router

from .conftest import seed_prediction, seed_signal


pytestmark = pytest.mark.integration


async def _seed_actor_prediction(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID,
    **kwargs,
) -> UUID:
    return await seed_prediction(
        pool,
        tenant=tenant,
        created_by_actor_id=actor_id,
        scope_actors=[actor_id],
        **kwargs,
    )


async def _seed_forecast_actor(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    display_name: str,
) -> UUID:
    actor_id = uuid4()
    await pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', $3, 'active')
        """,
        actor_id,
        tenant,
        display_name,
    )
    return actor_id


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
    token, actor_id = valid_session
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
        statement="active 1", confidence=0.7, resolution_days=3,
    )
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
    token, actor_id = valid_session
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
        statement="risk", category="customer_risk",
    )
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
async def test_forecast_page_routes_filter_hidden_targeted_predictions(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    visible_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Visible forecast target",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Hidden forecast target",
    )
    visible_prediction = await seed_prediction(
        gateway_pool,
        tenant=registered_tenant,
        statement="visible targeted forecast",
        confidence=0.82,
        resolution_days=3,
        impact={"arr_at_risk": 100_000},
        key_drivers=[{"label": "Visible signal", "direction": "up"}],
        target_node_kind="commitment",
        target_node_id=visible_commitment,
    )
    hidden_prediction = await seed_prediction(
        gateway_pool,
        tenant=registered_tenant,
        statement="hidden targeted forecast",
        confidence=0.91,
        resolution_days=4,
        impact={"arr_at_risk": 800_000},
        key_drivers=[{"label": "Hidden signal", "direction": "up"}],
        target_node_kind="commitment",
        target_node_id=hidden_commitment,
    )

    list_resp = await forecasts_client.get(
        "/v1/forecasts",
        headers=_auth(token),
    )
    page_resp = await forecasts_client.get(
        "/v1/forecasts/page",
        headers=_auth(token),
    )
    patterns_resp = await forecasts_client.get(
        "/v1/forecasts/patterns",
        headers=_auth(token),
    )
    hidden_detail_resp = await forecasts_client.get(
        f"/v1/forecasts/{hidden_prediction}",
        headers=_auth(token),
    )
    hidden_detail_v2_resp = await forecasts_client.get(
        f"/v1/forecasts/detail/{hidden_prediction}",
        headers=_auth(token),
    )

    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()["items"]
    assert [item["id"] for item in items] == [str(visible_prediction)]

    assert page_resp.status_code == 200, page_resp.text
    page = page_resp.json()
    assert page["header"]["active_forecast_count"] == 1
    assert page["selected_forecast_id"] == str(visible_prediction)
    assert set(page["forecast_details_by_id"]) == {str(visible_prediction)}

    assert patterns_resp.status_code == 200, patterns_resp.text
    pattern_forecast_ids = {
        forecast_id
        for pattern in patterns_resp.json()["patterns"]
        for forecast_id in pattern["related_forecast_ids"]
    }
    assert str(visible_prediction) in pattern_forecast_ids
    assert str(hidden_prediction) not in pattern_forecast_ids

    assert hidden_detail_resp.status_code == 404
    assert hidden_detail_v2_resp.status_code == 404


@pytest.mark.asyncio
async def test_forecast_routes_filter_hidden_targetless_predictions(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    other_actor_id = await _seed_forecast_actor(
        gateway_pool,
        tenant=registered_tenant,
        display_name="Hidden Forecast Owner",
    )
    visible_prediction = await _seed_actor_prediction(
        gateway_pool,
        tenant=registered_tenant,
        actor_id=actor_id,
        statement="visible targetless forecast",
        confidence=0.7,
    )
    hidden_prediction = await seed_prediction(
        gateway_pool,
        tenant=registered_tenant,
        statement="hidden targetless forecast",
        confidence=0.8,
        created_by_actor_id=other_actor_id,
        scope_actors=[other_actor_id],
    )

    list_resp = await forecasts_client.get(
        "/v1/forecasts",
        headers=_auth(token),
    )
    hidden_detail_resp = await forecasts_client.get(
        f"/v1/forecasts/{hidden_prediction}",
        headers=_auth(token),
    )

    assert list_resp.status_code == 200, list_resp.text
    assert [item["id"] for item in list_resp.json()["items"]] == [
        str(visible_prediction)
    ]
    assert hidden_detail_resp.status_code == 404


@pytest.mark.asyncio
async def test_summary_endpoint(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
        confidence=0.85, resolution_days=4,
        impact={"arr_at_risk": 500_000},
    )
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
async def test_forecast_summary_upcoming_and_risk_scope_targeted_predictions(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    visible_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Visible risk target",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Hidden risk target",
    )
    await seed_prediction(
        gateway_pool,
        tenant=registered_tenant,
        statement="visible risk",
        confidence=0.85,
        resolution_days=5,
        impact={"arr_at_risk": 100_000},
        target_node_kind="commitment",
        target_node_id=visible_commitment,
    )
    await seed_prediction(
        gateway_pool,
        tenant=registered_tenant,
        statement="hidden risk",
        confidence=0.95,
        resolution_days=6,
        impact={"arr_at_risk": 400_000},
        target_node_kind="commitment",
        target_node_id=hidden_commitment,
    )

    summary_resp = await forecasts_client.get(
        "/v1/forecasts/summary",
        headers=_auth(token),
    )
    upcoming_resp = await forecasts_client.get(
        "/v1/forecasts/upcoming?days=14",
        headers=_auth(token),
    )
    risk_resp = await forecasts_client.get(
        "/v1/forecasts/risk_exposure?days=28&metric=arr_at_risk",
        headers=_auth(token),
    )

    assert summary_resp.status_code == 200, summary_resp.text
    summary = summary_resp.json()
    assert summary["active_count"] == 1
    assert summary["at_risk_arr"] == pytest.approx(100_000)
    assert summary["high_confidence_count"] == 1
    assert summary["upcoming_resolutions_count_14d"] == 1

    assert upcoming_resp.status_code == 200, upcoming_resp.text
    upcoming = upcoming_resp.json()
    assert upcoming["count"] == 1
    assert upcoming["items"][0]["statement"] == "visible risk"

    assert risk_resp.status_code == 200, risk_resp.text
    risk = risk_resp.json()
    assert sum(bucket["value"] for bucket in risk["buckets"]) == pytest.approx(
        100_000
    )


@pytest.mark.asyncio
async def test_detail_endpoint_returns_row_and_signals(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    pid = await _seed_actor_prediction(
        gateway_pool,
        tenant=registered_tenant,
        actor_id=actor_id,
        statement="inspect me",
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
    token, actor_id = valid_session
    for _ in range(3):
        await _seed_actor_prediction(
            gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
async def test_accuracy_endpoint_scopes_targeted_predictions(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    visible_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Visible accuracy target",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Hidden accuracy target",
    )
    for _ in range(3):
        await seed_prediction(
            gateway_pool,
            tenant=registered_tenant,
            confidence=0.72,
            status="resolved",
            resolution_days=-2,
            resolved_days_ago=2,
            outcome="true",
            timeliness="on_time",
            target_node_kind="commitment",
            target_node_id=visible_commitment,
        )
        await seed_prediction(
            gateway_pool,
            tenant=registered_tenant,
            confidence=0.72,
            status="resolved",
            resolution_days=-2,
            resolved_days_ago=2,
            outcome="false",
            timeliness="on_time",
            target_node_kind="commitment",
            target_node_id=hidden_commitment,
        )

    resp = await forecasts_client.get(
        "/v1/forecasts/accuracy",
        headers=_auth(token),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
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
    token, actor_id = valid_session
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
        statement="near", resolution_days=5,
    )
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
    token, actor_id = valid_session
    await _seed_actor_prediction(
        gateway_pool, tenant=registered_tenant, actor_id=actor_id,
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
async def test_create_endpoint_allows_visible_target(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, actor_id = valid_session
    visible_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Visible forecast target",
        owner_id=actor_id,
    )
    payload = {
        "statement": "Forecast tied to visible commitment",
        "category": "strategy",
        "target_node_kind": "commitment",
        "target_node_id": str(visible_commitment),
        "confidence": 0.6,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=14)
        ).isoformat(),
    }

    resp = await forecasts_client.post(
        "/v1/forecasts",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["target_node_kind"] == "commitment"
    assert body["target_node_id"] == str(visible_commitment)


@pytest.mark.asyncio
async def test_create_endpoint_denies_hidden_target(
    forecasts_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        registered_tenant,
        title="Hidden forecast target",
    )
    payload = {
        "statement": "Forecast tied to hidden commitment",
        "category": "strategy",
        "target_node_kind": "commitment",
        "target_node_id": str(hidden_commitment),
        "confidence": 0.6,
        "resolution_at": (
            datetime.now(timezone.utc) + timedelta(days=14)
        ).isoformat(),
    }

    resp = await forecasts_client.post(
        "/v1/forecasts",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 403, resp.text
    assert resp.json() == {
        "error": "forbidden",
        "reason": "commitment_out_of_scope",
    }
    rows = await gateway_pool.fetchval(
        """
        SELECT COUNT(*)
        FROM predictions
        WHERE tenant_id = $1
          AND statement = 'Forecast tied to hidden commitment'
        """,
        registered_tenant,
    )
    assert rows == 0


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
