"""services/forecasts/tests/test_page.py — integration tests for the
spec v1.0 Forecasts page endpoints (/page, /detail/{id}, /patterns,
/ask). The existing test_router.py covers the legacy endpoints; this
file only exercises the new surface.
"""
from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio

from services.forecasts.router import build_router
from services.gateway.main import build_app

from .conftest import seed_prediction, seed_signal


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def fc_client(app_deps) -> AsyncGenerator[httpx.AsyncClient, None]:
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
async def test_page_endpoint_returns_full_payload(
    fc_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    # Seed three active rows across two domains.
    p1 = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="Beacon renewal at risk", category="customer_risk",
        confidence=0.78, resolution_days=2,
        impact={"arr_at_risk": 1_200_000},
        key_drivers=[
            {"label": "Open sync errors", "delta_label": "+42%", "direction": "up"},
            {"label": "Champion replies", "delta_label": "-3", "direction": "down"},
        ],
        target_label="Beacon",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="Engineering capacity will exceed 90%", category="capacity",
        confidence=0.72, resolution_days=6,
        impact={"capacity_pct": 92},
        key_drivers=[{"label": "Active commitments", "delta_label": "+3",
                      "direction": "up"}],
        target_label="Engineering",
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="Q3 delivery commitments at risk", category="delivery",
        confidence=0.65, resolution_days=19,
        impact={"arr_at_risk": 480_000},
        key_drivers=[],
    )

    resp = await fc_client.get(
        "/v1/forecasts/page",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Header.
    h = body["header"]
    assert h["active_forecast_count"] == 3
    assert h["resolving_soon_count"] >= 2
    assert h["horizon_days"] == 90

    # Brief.
    assert isinstance(body["foresight_brief"]["statement"], str)
    assert body["foresight_brief"]["statement"]
    assert isinstance(body["foresight_brief"]["resolves_soon"], list)

    # Horizon matrix shape.
    horizon = body["horizon"]
    assert {d["id"] for d in horizon["domains"]} >= {
        "customers_revenue", "commitments_delivery", "systems_capacity",
    }
    assert [h["id"] for h in horizon["horizons"]] == [
        "next_14_days", "days_15_45", "days_46_90",
    ]
    # Beacon row lands in the customers_revenue × next_14_days cell.
    cust = next(d for d in horizon["domains"] if d["id"] == "customers_revenue")
    near = next(c for c in cust["cells"] if c["horizon_id"] == "next_14_days")
    assert any(f["id"] == str(p1) for f in near["forecasts"])

    # Default selection should be the highest-impact near-term row.
    assert body["selected_forecast_id"] == str(p1)
    detail = body["forecast_details_by_id"][str(p1)]
    assert detail["statement"] == "Beacon renewal at risk"
    assert detail["driving_patterns"]
    assert detail["leading_indicators"]
    assert detail["intervention_levers"]
    assert detail["would_change_if"]

    # Patterns present.
    assert isinstance(body["patterns"], list)
    assert body["patterns"]


@pytest.mark.asyncio
async def test_detail_v2_returns_spec_shape(
    fc_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="X happens", category="customer_risk",
        confidence=0.7, resolution_days=4,
        key_drivers=[{"label": "Driver A", "delta_label": "+10%",
                      "direction": "up"}],
        target_label="Acme",
    )
    await seed_signal(gateway_pool, prediction_id=pid, title="ev one")
    resp = await fc_client.get(
        f"/v1/forecasts/detail/{pid}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == str(pid)
    assert body["domain"] == "customers_revenue"
    assert body["confidence"] == pytest.approx(0.7)
    assert body["confidence_series"]["points"]
    assert body["leading_indicators"]
    assert body["evidence_summary"]["signal_count"] == 1


@pytest.mark.asyncio
async def test_detail_v2_returns_404_for_missing(
    fc_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    from uuid import uuid4
    resp = await fc_client.get(
        f"/v1/forecasts/detail/{uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_patterns_endpoint(
    fc_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        category="customer_risk", confidence=0.7,
    )
    await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        category="capacity", confidence=0.6,
    )
    resp = await fc_client.get(
        "/v1/forecasts/patterns",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] >= 1
    assert all("id" in p and "title" in p for p in body["patterns"])


@pytest.mark.asyncio
async def test_ask_returns_scenario_for_what_if(
    fc_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        statement="Beacon renewal at risk", category="customer_risk",
        confidence=0.78,
    )
    resp = await fc_client.post(
        "/v1/forecasts/ask",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mode": "horizon",
            "selected_forecast_id": str(pid),
            "prompt": "What if we assign an owner today?",
            "visible_forecast_ids": [str(pid)],
            "horizon_days": 90,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["type"] == "scenario_analysis"
    assert "scenario" in body["title"].lower()
    assert body["body"]
    assert isinstance(body["actions"], list)


@pytest.mark.asyncio
async def test_ask_returns_explanation_for_why(
    fc_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    registered_tenant: UUID,
):
    token, _ = valid_session
    pid = await seed_prediction(
        gateway_pool, tenant=registered_tenant,
        category="customer_risk", confidence=0.6,
    )
    resp = await fc_client.post(
        "/v1/forecasts/ask",
        headers={"Authorization": f"Bearer {token}"},
        json={"prompt": "Why did this move?", "selected_forecast_id": str(pid)},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "forecast_explanation"


@pytest.mark.asyncio
async def test_ask_rejects_missing_prompt(
    fc_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    resp = await fc_client.post(
        "/v1/forecasts/ask",
        headers={"Authorization": f"Bearer {token}"},
        json={"prompt": ""},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_page_endpoint_empty_state(
    fc_client: httpx.AsyncClient,
    valid_session,
    registered_tenant: UUID,
):
    token, _ = valid_session
    resp = await fc_client.get(
        "/v1/forecasts/page",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["header"]["active_forecast_count"] == 0
    assert body["selected_forecast_id"] is None
    assert body["forecast_details_by_id"] == {}
