"""Integration: governance end-to-end (M5) against a live Postgres.

kill-switch (token + authz hard-off), audit log (reads/writes recorded), consent
flow (manifest -> grant intersection), provenance (third-party-driven model).
Skips without DATABASE_URL.
"""
from __future__ import annotations

import os
import pathlib
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

pytestmark = pytest.mark.integration

_SERVER_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://company_os:company_os@localhost:5434/postgres"
)


async def _make_db() -> str:
    if "://" not in _SERVER_DSN:
        pytest.skip("no DATABASE_URL")
    admin = await asyncpg.connect(_SERVER_DSN)
    try:
        await admin.execute('DROP DATABASE IF EXISTS fyralis_m5_test WITH (FORCE)')
        await admin.execute('CREATE DATABASE fyralis_m5_test')
    finally:
        await admin.close()
    return _SERVER_DSN.rsplit("/", 1)[0] + "/fyralis_m5_test"


@pytest.fixture
async def wired():
    import lib
    from fastapi import FastAPI
    from lib.shared.migrations import apply_migrations_dir, schema_bootstrap_lock
    from lib.extensions.host_api.v1 import Capabilities
    from services.app.gateway.db_bootstrap import create_gateway_pool
    from services.app.gateway.extension_router import build_extension_router
    from services.platform.extensions.identity import ExtensionOAuthClientsRepo
    from services.platform.extensions.grants import ExtensionGrantsRepo

    dsn = await _make_db()
    conn = await asyncpg.connect(dsn)
    try:
        core = pathlib.Path(lib.__file__).resolve().parents[1] / "db" / "migrations"
        async with schema_bootstrap_lock(conn):
            await apply_migrations_dir(conn, core)
    finally:
        await conn.close()

    pool = await create_gateway_pool(dsn)
    tenant_id = uuid4()
    ext_id = "gov_ext"
    async with pool.acquire() as c:
        await c.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tenant_id, "m5")
        await c.execute(
            "INSERT INTO observations (id, tenant_id, occurred_at, kind, source_channel, "
            "content, content_text, trust_tier, external_id) "
            "VALUES ($1,$2,now(),'k','github:webhook',$3::jsonb,'t','inferential_external','g1')",
            uuid4(), tenant_id, '{"author":"o"}')
    creds = await ExtensionOAuthClientsRepo(pool).register(
        extension_id=ext_id, created_by="test", environment="sandbox")
    await ExtensionGrantsRepo(pool).grant(
        tenant_id=tenant_id, extension_id=ext_id, granted_version="1.0.0",
        capabilities=Capabilities(read_channels=("github:webhook",),
                                  substrate_read=frozenset({"observation"})),
        granted_by="test")

    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.include_router(build_extension_router())
    try:
        yield SimpleNamespace(app=app, pool=pool, tenant_id=tenant_id, ext_id=ext_id, creds=creds)
    finally:
        await pool.close()


async def _token(c, creds):
    r = await c.post("/ext/oauth/token", data={
        "grant_type": "client_credentials",
        "client_id": creds.client_id, "client_secret": creds.client_secret})
    return r


async def test_killswitch_blocks_token_and_access(wired):
    import httpx
    from services.platform.extensions.killswitch import KillSwitch

    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = (await _token(c, wired.creds)).json()["access_token"]
        auth = {"authorization": f"Bearer {token}", "x-fyralis-tenant": str(wired.tenant_id)}
        assert (await c.get("/ext/v1/observations", headers=auth)).status_code == 200

        # global kill: existing tokens are rejected at authz AND new tokens refused
        await KillSwitch(wired.pool).disable(wired.ext_id, disabled_by="ops", reason="abuse")
        r = await c.get("/ext/v1/observations", headers=auth)
        assert r.status_code == 403 and r.json()["error"] == "extension_disabled"
        rt = await _token(c, wired.creds)
        assert rt.status_code == 403 and rt.json()["error"] == "extension_disabled"

        # re-enable restores access
        assert await KillSwitch(wired.pool).enable(wired.ext_id) is True
        assert (await _token(c, wired.creds)).status_code == 200


async def test_audit_log_records_reads(wired):
    import httpx
    from services.platform.extensions.audit import AuditLog

    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = (await _token(c, wired.creds)).json()["access_token"]
        auth = {"authorization": f"Bearer {token}", "x-fyralis-tenant": str(wired.tenant_id)}
        await c.get("/ext/v1/observations", headers=auth)

    rows = await AuditLog(wired.pool).recent(extension_id=wired.ext_id)
    assert any(r["action"] == "read_observations" for r in rows)


async def test_consent_flow_creates_grant(wired):
    from lib.extensions.manifest import ExtensionManifest
    from lib.extensions.host_api.v1 import Capabilities
    from services.platform.extensions import consent
    from services.platform.extensions.grants import ExtensionGrantsRepo

    m = ExtensionManifest(
        id="consent_ext", trust_tier="third_party",
        capabilities={"read_channels": ["github:webhook"], "substrate_read": ["observation"],
                      "write_observations": True})
    screen = consent.consent_screen(m)
    assert screen["requests"]["write_observations"] is True
    # admin approves a NARROWER set (no write) -> intersection drops write
    await consent.approve(
        wired.pool, tenant_id=wired.tenant_id, manifest=m,
        approved=Capabilities(read_channels=("github:webhook",),
                              substrate_read=frozenset({"observation"}),
                              write_observations=False),
        granted_by="admin")
    grant = await ExtensionGrantsRepo(wired.pool).get(
        tenant_id=wired.tenant_id, extension_id="consent_ext")
    assert grant is not None
    assert grant.capabilities.write_observations is False  # narrowed by the admin
    assert grant.capabilities.allows_channel("github:webhook")


async def test_provenance_third_party_driven(wired):
    from services.platform.extensions.provenance import ModelProvenanceRepo
    repo = ModelProvenanceRepo(wired.pool)
    model_id = uuid4()
    await repo.record(model_id=model_id, tenant_id=wired.tenant_id,
                      source_channels=["github:webhook", "ext:gov_ext:risk"])
    assert await repo.is_third_party_driven(model_id) is True
    sources = await repo.sources_for(model_id)
    idents = {s["source_identity"] for s in sources}
    assert "extension:gov_ext" in idents and "channel:github:webhook" in idents

    clean = uuid4()
    await repo.record(model_id=clean, tenant_id=wired.tenant_id,
                      source_channels=["github:webhook"])
    assert await repo.is_third_party_driven(clean) is False
