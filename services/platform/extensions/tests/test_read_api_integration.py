"""Integration: the /ext read-API end-to-end (M2) against a live Postgres.

Proves the whole read plane: register an OAuth client -> mint a token via
/ext/oauth/token -> read observations via /ext/v1/observations under the
fyralis_ext_readonly role + RLS, capability- and grant-scoped. Also proves the
negative paths (no token -> 401, no tenant header -> 400, no grant -> 403,
ungranted channel filtered out).

Self-contained: creates a throwaway DB, applies the full core migration set,
seeds a tenant + observations + client + grant. Skips if DATABASE_URL is unset.
"""
from __future__ import annotations

import os
import pathlib
from datetime import datetime, timezone
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
    from services.platform.extensions.tests._migration_test_helpers import (
        require_pgvector_server_privilege_or_skip,
    )

    await require_pgvector_server_privilege_or_skip(
        _SERVER_DSN,
        feature="extension read-API",
    )
    admin = await asyncpg.connect(_SERVER_DSN)
    try:
        await admin.execute('DROP DATABASE IF EXISTS fyralis_m2_test WITH (FORCE)')
        await admin.execute('CREATE DATABASE fyralis_m2_test')
    finally:
        await admin.close()
    return _SERVER_DSN.rsplit("/", 1)[0] + "/fyralis_m2_test"


@pytest.fixture
async def wired():
    import lib
    from fastapi import FastAPI
    from services.app.gateway.db_bootstrap import create_gateway_pool
    from services.app.gateway.extension_router import build_extension_router
    from services.platform.extensions.identity import ExtensionOAuthClientsRepo
    from services.platform.extensions.grants import ExtensionGrantsRepo
    from lib.extensions.host_api.v1 import Capabilities
    from services.platform.extensions.tests._migration_test_helpers import (
        apply_core_migrations_or_skip,
    )

    dsn = await _make_db()
    conn = await asyncpg.connect(dsn)
    try:
        core = pathlib.Path(lib.__file__).resolve().parents[1] / "db" / "migrations"
        await apply_core_migrations_or_skip(conn, core, feature="extension read-API")
    finally:
        await conn.close()

    pool = await create_gateway_pool(dsn)
    tenant_id = uuid4()
    ext_id = "test_ext"
    async with pool.acquire() as c:
        await c.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tenant_id, "m2")
        await c.execute(
            "INSERT INTO observations (id, tenant_id, occurred_at, kind, source_channel, "
            "content, content_text, trust_tier, external_id) "
            "VALUES ($1,$2,$3,'github','github:webhook',$4::jsonb,$5,'inferential_external',$6)",
            uuid4(), tenant_id, datetime(2026, 6, 10, tzinfo=timezone.utc),
            '{"event_type":"push","_raw":{"secret":"leak"}}', "a push", "ext-1",
        )
        # an observation in a channel the extension is NOT granted
        await c.execute(
            "INSERT INTO observations (id, tenant_id, occurred_at, kind, source_channel, "
            "content, content_text, trust_tier, external_id) "
            "VALUES ($1,$2,$3,'slack','slack:message',$4::jsonb,$5,'inferential_external',$6)",
            uuid4(), tenant_id, datetime(2026, 6, 10, tzinfo=timezone.utc),
            '{}', "a slack msg", "ext-2",
        )

    client_creds = await ExtensionOAuthClientsRepo(pool).register(
        extension_id=ext_id, created_by="test", environment="sandbox"
    )
    await ExtensionGrantsRepo(pool).grant(
        tenant_id=tenant_id, extension_id=ext_id, granted_version="1.0.0",
        capabilities=Capabilities(read_channels=("github:webhook",),
                                  substrate_read=frozenset({"observation"})),
        granted_by="test",
    )

    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.include_router(build_extension_router())
    try:
        yield SimpleNamespace(app=app, pool=pool, tenant_id=tenant_id,
                              ext_id=ext_id, creds=client_creds)
    finally:
        await pool.close()


async def _token(client, creds) -> str:
    r = await client.post("/ext/oauth/token", data={
        "grant_type": "client_credentials",
        "client_id": creds.client_id, "client_secret": creds.client_secret})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


async def test_read_api_end_to_end(wired):
    import httpx
    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = await _token(c, wired.creds)
        auth = {"authorization": f"Bearer {token}", "x-fyralis-tenant": str(wired.tenant_id)}

        # granted channel returns the observation, with _raw redacted out
        r = await c.get("/ext/v1/observations", headers=auth)
        assert r.status_code == 200, r.text
        obs = r.json()["observations"]
        assert len(obs) == 1
        assert obs[0]["source_channel"] == "github:webhook"
        assert "_raw" not in obs[0]["content"]  # baseline redaction

        # requesting the ungranted channel is capability-denied
        r = await c.get("/ext/v1/observations", headers=auth, params={"channel": "slack:message"})
        assert r.status_code == 403

        # negative paths
        assert (await c.get("/ext/v1/observations")).status_code == 401          # no token
        assert (await c.get("/ext/v1/observations",
                            headers={"authorization": f"Bearer {token}"})).status_code == 400  # no tenant


async def test_no_grant_is_forbidden(wired):
    import httpx
    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = await _token(c, wired.creds)
        other_tenant = str(uuid4())
        r = await c.get("/ext/v1/observations", headers={
            "authorization": f"Bearer {token}", "x-fyralis-tenant": other_tenant})
        assert r.status_code == 403
