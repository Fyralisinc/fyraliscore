"""Integration: edge-ingest end-to-end (M4 / E3.2) against a live Postgres.

POST /ext/v1/ingest persists a derived observation via the real ingest pipeline,
host-namespaced + trust-capped; rejects over-ceiling/unreachable tiers (not
downgrade); enforces the write_observations grant; dedups. Skips without DATABASE_URL.
"""
from __future__ import annotations

import json
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
        await admin.execute('DROP DATABASE IF EXISTS fyralis_m4_test WITH (FORCE)')
        await admin.execute('CREATE DATABASE fyralis_m4_test')
    finally:
        await admin.close()
    return _SERVER_DSN.rsplit("/", 1)[0] + "/fyralis_m4_test"


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
    ext_id = "writer_ext"
    async with pool.acquire() as c:
        await c.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tenant_id, "m4")
    creds = await ExtensionOAuthClientsRepo(pool).register(
        extension_id=ext_id, created_by="test", environment="sandbox")
    grants = ExtensionGrantsRepo(pool)
    await grants.grant(
        tenant_id=tenant_id, extension_id=ext_id, granted_version="1.0.0",
        capabilities=Capabilities(write_observations=True),
        trust_ceiling="inferential_external", granted_by="test")

    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.include_router(build_extension_router())
    try:
        yield SimpleNamespace(app=app, pool=pool, tenant_id=tenant_id, ext_id=ext_id,
                              creds=creds, grants=grants)
    finally:
        await pool.close()


async def _token(c, creds):
    r = await c.post("/ext/oauth/token", data={
        "grant_type": "client_credentials",
        "client_id": creds.client_id, "client_secret": creds.client_secret})
    return r.json()["access_token"]


async def test_edge_ingest_end_to_end(wired):
    import httpx
    from lib.extensions.host_api.v1 import Capabilities

    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = await _token(c, wired.creds)
        hdr = {"authorization": f"Bearer {token}", "x-fyralis-tenant": str(wired.tenant_id),
               "content-type": "application/json"}

        # default tier, host-namespaced channel, persisted
        r = await c.post("/ext/v1/ingest", headers=hdr, content=json.dumps({
            "channel": "risk", "content": {"score": 9}, "content_text": "risk up",
            "external_id": "r-1"}))
        assert r.status_code == 200, r.text
        ack = r.json()
        assert ack["source_channel"] == f"ext:{wired.ext_id}:risk"
        assert ack["trust_tier"] == "inferential_external"
        # the row really landed in the SoR
        async with wired.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT source_channel, trust_tier, content FROM observations "
                "WHERE tenant_id=$1 AND external_id=$2", wired.tenant_id, "r-1")
        assert row is not None and row["source_channel"] == f"ext:{wired.ext_id}:risk"
        assert row["trust_tier"] == "inferential_external"

        # over-ceiling request is REJECTED, not downgraded
        r = await c.post("/ext/v1/ingest", headers=hdr, content=json.dumps({
            "channel": "risk", "content": {}, "content_text": "x", "trust_tier": "attested_agent"}))
        assert r.status_code == 403 and r.json()["error"] == "trust_tier_over_ceiling"

        # authoritative is unreachable
        r = await c.post("/ext/v1/ingest", headers=hdr, content=json.dumps({
            "channel": "risk", "content": {}, "content_text": "x", "trust_tier": "authoritative"}))
        assert r.status_code == 403 and r.json()["error"] == "trust_tier_unreachable"

        # dedup on (channel, external_id)
        r = await c.post("/ext/v1/ingest", headers=hdr, content=json.dumps({
            "channel": "risk", "content": {"score": 9}, "content_text": "risk up",
            "external_id": "r-1"}))
        assert r.status_code == 200 and r.json()["deduped"] is True

        # revoke write grant -> ingest forbidden
        await wired.grants.grant(
            tenant_id=wired.tenant_id, extension_id=wired.ext_id, granted_version="1.0.0",
            capabilities=Capabilities(write_observations=False),
            trust_ceiling="inferential_external", granted_by="test")
        r = await c.post("/ext/v1/ingest", headers=hdr, content=json.dumps({
            "channel": "risk", "content": {}, "content_text": "x"}))
        assert r.status_code == 403 and r.json()["error"] == "write_observations_not_granted"
