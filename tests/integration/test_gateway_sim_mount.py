"""Integration checks for a simulation gateway extension.

Core no longer imports the simulation overlay directly. When an installed
gateway extension contributes ``/simulation`` routes, these smoke tests boot
the gateway app and assert those routes share the gateway runtime pool.

Skipped automatically when DATABASE_URL is absent (see conftest.py), or when
no simulation gateway extension is installed in the current checkout.
"""
from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.app.gateway.auth import create_session


pytestmark = pytest.mark.integration


def _extension_contributes_simulation() -> bool:
    from services.app.gateway.extensions import discovered_extensions, reset_for_tests

    reset_for_tests()
    for ext in discovered_extensions():
        if "sim" in ext.name.lower() or "demo" in ext.name.lower():
            return True
        if any(prefix.startswith("/simulation") for prefix in ext.public_path_prefixes):
            return True
        for router in ext.routers:
            for route in router.routes:
                if str(getattr(route, "path", "")).startswith("/simulation"):
                    return True
    return False


@pytest.fixture
def _require_simulation_gateway_extension(_sim_mount_env):
    if not _extension_contributes_simulation():
        pytest.skip(
            "simulation gateway extension is not installed; core no longer "
            "mounts /simulation directly"
        )


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
    # services.ingest.synthetic refuses to run in prod; this test harness
    # is a dev-equivalent.
    monkeypatch.setenv("COMPANY_OS_ENV", "test")
    yield


@pytest.fixture
async def _gateway_auth_headers(fresh_db, _require_simulation_gateway_extension):
    tenant_id = UUID("00000000-0000-7000-8000-000000000dd1")
    actor_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name, is_demo) VALUES ($1, $2, TRUE) "
            "ON CONFLICT (id) DO NOTHING",
            tenant_id,
            "simulation smoke tenant",
        )
        await conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, status, metadata
            ) VALUES ($1, $2, 'human_internal', 'Simulation Smoke Actor',
                      'active', '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            """,
            actor_id,
            tenant_id,
        )
    token, _ = await create_session(
        fresh_db,
        actor_id=actor_id,
        tenant_id=tenant_id,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_sim_health_responds_through_gateway(
    fresh_db,
    _require_simulation_gateway_extension,
    _gateway_auth_headers,
):
    """Boot the gateway app with the SIM router mounted; hit
    `/simulation/health` through the same ASGI transport.
    """
    import httpx
    from services.app.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get(
                "/simulation/health",
                headers=_gateway_auth_headers,
            )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["tenant_id"] == "00000000-0000-7000-8000-000000000dd1"
    assert body["run_id"] == "sim-gateway-mount-smoke"
    assert body["channel_count"] > 0
    assert body["persona_count"] > 0


@pytest.mark.asyncio
async def test_sim_channels_responds_through_gateway(
    fresh_db,
    _require_simulation_gateway_extension,
    _gateway_auth_headers,
):
    """Smoke-check another SIM route to prove the router is fully
    attached (not just the health endpoint).
    """
    import httpx
    from services.app.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get(
                "/simulation/channels",
                headers=_gateway_auth_headers,
            )
    assert r.status_code == 200, r.text
    channels = r.json()["channels"]
    handles = {c["handle"] for c in channels}
    # The fixed channel list includes these authoring defaults.
    assert "leadership" in handles
    assert "eng" in handles


@pytest.mark.asyncio
async def test_sim_personas_responds_through_gateway(
    fresh_db,
    _require_simulation_gateway_extension,
    _gateway_auth_headers,
):
    """Personas are loaded from the YAML registry at import; this
    proves the route is wired on the gateway mount path (no in-db
    seeding required for a GET, though the mount does seed actors).
    """
    import httpx
    from services.app.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ) as client:
            r = await client.get(
                "/simulation/personas",
                headers=_gateway_auth_headers,
            )
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
async def test_gateway_does_not_double_create_pool(
    fresh_db,
    _require_simulation_gateway_extension,
):
    """Regression for the Week-4 caveat: mounting SIM must not create
    a second pool. We assert the gateway deps.pool and the
    `app.state.sim_deps.pool` are the same object.
    """
    from services.app.gateway.main import build_app

    app = build_app(pool=fresh_db)
    async with app.router.lifespan_context(app):
        deps = app.state.deps
        sim_deps = getattr(app.state, "sim_deps", None)
        assert sim_deps is not None, "sim_deps not attached to gateway state"
        assert sim_deps.pool is deps.pool, (
            "SIM mount created a second pool; should share the gateway pool"
        )
