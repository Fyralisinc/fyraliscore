"""Week-5 integration: SIM router mounts cleanly on the gateway.

Item 3 from the Week-5 Stabilization brief. Week-4 integration left
`GATEWAY_MOUNT_SIM=1` opt-in because `simulation/server.py` owned its
own pool + lifespan — mounting it inside the gateway would have
double-created an asyncpg pool. Week-5 extracts `build_sim_router(deps)`
from `simulation/server.py`; the gateway now constructs a `SimDeps`
using its own pool and includes the router, with no second lifespan.

These smoke tests boot the gateway app (lifespan up) and assert the
SIM routes respond from the same host.

Skipped automatically when DATABASE_URL is absent (see conftest.py).
"""
from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.integration


@pytest.fixture
def _sim_mount_env(monkeypatch):
    """Force GATEWAY_MOUNT_SIM=1 regardless of the ambient env, and
    pin the dogfood tenant + run id so assertions are stable."""
    monkeypatch.setenv("GATEWAY_MOUNT_SIM", "1")
    monkeypatch.setenv(
        "SIMULATION_TENANT_ID", "00000000-0000-7000-8000-000000000dd1"
    )
    monkeypatch.setenv("SIMULATION_RUN_ID", "sim-gateway-mount-smoke")
    # Keep the GRT scheduler off — not under test here, avoids
    # background-task noise on the test loop.
    monkeypatch.setenv("GATEWAY_START_GRT_SCHEDULER", "0")
    # services.synthetic refuses to run in prod; this test harness
    # is a dev-equivalent.
    monkeypatch.setenv("COMPANY_OS_ENV", "test")
    yield


@pytest.mark.asyncio
async def test_sim_health_responds_through_gateway(fresh_db, _sim_mount_env):
    """Boot the gateway app with the SIM router mounted; hit
    `/simulation/health` through the same ASGI transport.
    """
    import httpx
    from services.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get("/simulation/health")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["tenant_id"] == "00000000-0000-7000-8000-000000000dd1"
    assert body["run_id"] == "sim-gateway-mount-smoke"
    assert body["channel_count"] > 0
    assert body["persona_count"] > 0


@pytest.mark.asyncio
async def test_sim_channels_responds_through_gateway(fresh_db, _sim_mount_env):
    """Smoke-check another SIM route to prove the router is fully
    attached (not just the health endpoint).
    """
    import httpx
    from services.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get("/simulation/channels")
    assert r.status_code == 200, r.text
    channels = r.json()["channels"]
    handles = {c["handle"] for c in channels}
    # The fixed channel list includes these authoring defaults.
    assert "leadership" in handles
    assert "eng" in handles


@pytest.mark.asyncio
async def test_sim_personas_responds_through_gateway(fresh_db, _sim_mount_env):
    """Personas are loaded from the YAML registry at import; this
    proves the route is wired on the gateway mount path (no in-db
    seeding required for a GET, though the mount does seed actors).
    """
    import httpx
    from services.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get("/simulation/personas")
    assert r.status_code == 200, r.text
    personas = r.json()["personas"]
    assert isinstance(personas, list)
    assert len(personas) > 0
    # Shape sanity: each persona has the required keys.
    for p in personas:
        assert "id" in p
        assert "name" in p
        assert "slack_handle" in p


@pytest.mark.asyncio
async def test_gateway_does_not_double_create_pool(fresh_db, _sim_mount_env):
    """Regression for the Week-4 caveat: mounting SIM must not create
    a second pool. We assert the gateway deps.pool and the
    `app.state.sim_deps.pool` are the same object.
    """
    from services.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        deps = app.state.deps
        sim_deps = getattr(app.state, "sim_deps", None)
        assert sim_deps is not None, "sim_deps not attached to gateway state"
        assert sim_deps.pool is deps.pool, (
            "SIM mount created a second pool; should share the gateway pool"
        )
