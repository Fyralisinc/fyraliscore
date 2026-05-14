"""End-to-end test: verified webhook → observations row.

Spec: US1 / FR-013 / SC-001.

Requires live Postgres. Uses the project-wide `db_pool` fixture from
the top-level conftest. The test exercises the full path: build the
FastAPI app with real DB-backed dependencies, send a properly-signed
Slack request, and observe an `observations` row created under the
resolved tenant_id with the right `source_channel` and `trust_tier`.
"""
from __future__ import annotations

import json
import time
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.webhooks.tests.conftest import slack_sign


pytestmark = pytest.mark.integration


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    """Insert a tenant row so the FK in `observations.tenant_id`
    (migration 0037) can resolve.
    """
    tenant_id = uuid4()
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            f"webhook-e2e-{tenant_id.hex[:8]}",
        )
    return tenant_id


@pytest.fixture
async def _app_and_env(
    fresh_db: asyncpg.Pool,
    _tenant: UUID,
    monkeypatch: pytest.MonkeyPatch,
):
    secret = "e2e-slack-secret"
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", secret)
    monkeypatch.setenv("WEBHOOK_TENANT_SLACK_T0001E2E", str(_tenant))

    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,  # embedding marked pending — fine for the test
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    return app, secret


@pytest.mark.asyncio
async def test_verified_slack_becomes_observation(
    fresh_db: asyncpg.Pool, _tenant: UUID, _app_and_env
) -> None:
    app, secret = _app_and_env

    body = json.dumps({
        "team_id": "T0001E2E",
        "event": {
            "type": "message",
            "text": "shipped the rate limiter fix",
            "ts": str(time.time()),
            "channel": "C123",
            "user": "U_ALICE",
        },
    }).encode("utf-8")
    ts = int(time.time())
    sig = slack_sign(secret, body, ts)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
                "Content-Type": "application/json",
            },
        )

    assert r.status_code in (200, 201), r.text
    payload = r.json()
    obs_id = UUID(payload["observation_id"])

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, source_channel, trust_tier, content_text
            FROM observations WHERE id = $1
            """,
            obs_id,
        )
    assert row is not None
    assert row["tenant_id"] == _tenant
    assert row["source_channel"] == "slack:message"
    assert row["trust_tier"] == "attested_agent"
    assert "rate limiter" in (row["content_text"] or "")


@pytest.mark.asyncio
async def test_spoofed_request_creates_no_observation(
    fresh_db: asyncpg.Pool, _tenant: UUID, _app_and_env
) -> None:
    app, _ = _app_and_env

    body = b'{"team_id":"T0001E2E","event":{"type":"message","text":"x","ts":"1","channel":"C","user":"U"}}'
    ts = int(time.time())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": "v0=" + ("ff" * 32),
            },
        )
    assert r.status_code == 401
    assert r.json()["context"]["reason"] == "signature_mismatch"

    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM observations WHERE tenant_id = $1",
            _tenant,
        )
    assert n == 0
