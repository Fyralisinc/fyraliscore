"""Integration: the egress plane end-to-end (M3) against a live Postgres.

projector (tails observations, capability-filters + redacts) -> outbox ->
  (a) cursor PULL via GET /ext/v1/stream
  (b) opt-in webhook PUSH (HMAC-signed, verifiable with the client's webhook secret)

Skips if DATABASE_URL is unset; creates a throwaway DB with the full migration set.
"""
from __future__ import annotations

import json
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
        feature="extension egress",
    )
    admin = await asyncpg.connect(_SERVER_DSN)
    try:
        await admin.execute('DROP DATABASE IF EXISTS fyralis_m3_test WITH (FORCE)')
        await admin.execute('CREATE DATABASE fyralis_m3_test')
    finally:
        await admin.close()
    return _SERVER_DSN.rsplit("/", 1)[0] + "/fyralis_m3_test"


@pytest.fixture
async def wired():
    import lib
    from fastapi import FastAPI
    from lib.extensions.host_api.v1 import Capabilities
    from services.app.gateway.db_bootstrap import create_gateway_pool
    from services.app.gateway.extension_router import build_extension_router
    from services.platform.extensions.identity import ExtensionOAuthClientsRepo
    from services.platform.extensions.grants import ExtensionGrantsRepo
    from services.platform.extensions.tests._migration_test_helpers import (
        apply_core_migrations_or_skip,
    )

    dsn = await _make_db()
    conn = await asyncpg.connect(dsn)
    try:
        core = pathlib.Path(lib.__file__).resolve().parents[1] / "db" / "migrations"
        await apply_core_migrations_or_skip(conn, core, feature="extension egress")
    finally:
        await conn.close()

    pool = await create_gateway_pool(dsn)
    tenant_id = uuid4()
    ext_id = "egress_ext"
    async with pool.acquire() as c:
        await c.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tenant_id, "m3")
        for i, (chan, content) in enumerate([
            ("github:webhook", '{"event_type":"push","author":"octocat","author_email":"o@e.com","_raw":{"x":1}}'),
            ("slack:message", '{"text":"hi"}'),  # ungranted channel — must NOT be projected
        ]):
            await c.execute(
                "INSERT INTO observations (id, tenant_id, occurred_at, kind, source_channel, "
                "content, content_text, trust_tier, external_id) "
                "VALUES ($1,$2,$3,'k',$4,$5::jsonb,$6,'inferential_external',$7)",
                uuid4(), tenant_id, datetime(2026, 6, 10, 12, i, tzinfo=timezone.utc),
                chan, content, f"text{i}", f"e{i}",
            )

    creds = await ExtensionOAuthClientsRepo(pool).register(
        extension_id=ext_id, created_by="test", environment="sandbox",
        callback_url="https://ext.example/hook",
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
        yield SimpleNamespace(app=app, pool=pool, tenant_id=tenant_id, ext_id=ext_id, creds=creds)
    finally:
        await pool.close()


async def _token(c, creds):
    r = await c.post("/ext/oauth/token", data={
        "grant_type": "client_credentials",
        "client_id": creds.client_id, "client_secret": creds.client_secret})
    return r.json()["access_token"]


async def test_projection_then_pull(wired):
    import httpx
    from services.platform.extensions.egress.projector import run_projection_pass

    n = await run_projection_pass(wired.pool)
    assert n == 2  # both observations scanned...

    transport = httpx.ASGITransport(app=wired.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        token = await _token(c, wired.creds)
        auth = {"authorization": f"Bearer {token}", "x-fyralis-tenant": str(wired.tenant_id)}
        r = await c.get("/ext/v1/stream", headers=auth)
        assert r.status_code == 200, r.text
        body = r.json()
        items = body["items"]
        # ...but only the GRANTED github channel was projected, redacted
        assert len(items) == 1
        assert items[0]["source_channel"] == "github:webhook"
        assert items[0]["content"]["author"] == "octocat"      # signal kept
        assert "author_email" not in items[0]["content"]        # email redacted
        assert "_raw" not in items[0]["content"]
        cursor = body["cursor"]
        assert cursor > 0
        # paging forward from the cursor is empty (caught up)
        r2 = await c.get("/ext/v1/stream", headers=auth, params={"cursor": cursor})
        assert r2.json()["items"] == []

    # projector is idempotent: a second pass adds nothing
    assert await run_projection_pass(wired.pool) == 0


async def test_webhook_push_is_signed_and_delivered(wired):
    from services.platform.extensions.egress.projector import run_projection_pass
    from services.platform.extensions.egress.delivery import run_webhook_pass
    from fyralis_ext.webhooks import verify as verify_sig
    from services.platform.extensions.egress.webhook import SIGNATURE_HEADER

    await run_projection_pass(wired.pool)
    captured = {}

    async def fake_post(url, body, headers):
        captured["url"] = url
        captured["body"] = body
        captured["sig"] = headers.get(SIGNATURE_HEADER)
        return 200

    result = await run_webhook_pass(wired.pool, http_post=fake_post)
    assert result["delivered"] == 1 and result["failed"] == 0
    assert captured["url"] == "https://ext.example/hook"
    # the SDK-side verify accepts the host-signed body with the client's webhook secret
    assert verify_sig(captured["body"], captured["sig"], wired.creds.webhook_secret)
    event = json.loads(captured["body"])
    assert event["type"] == "observation"
    assert event["observation"]["source_channel"] == "github:webhook"

    # already delivered -> next pass has nothing pending
    assert (await run_webhook_pass(wired.pool, http_post=fake_post))["total"] == 0
