"""Gateway: bearer auth + rate limit + tenant isolation + request-id log.

Tests the middleware stack end-to-end via httpx.AsyncClient.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import httpx
import pytest

from lib.shared.ids import uuid7
from services.app.gateway.auth import (
    create_session,
    hash_token,
    new_token,
    revoke_session,
)
from services.app.gateway.rate_limit import RateLimiter, RateTier


# ========================================================================
# Bearer-token auth
# ========================================================================


@pytest.mark.asyncio
async def test_valid_token_returns_200_on_observations(
    client: httpx.AsyncClient, valid_session
):
    token, _actor = valid_session
    resp = await client.get(
        "/observations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["stub"] is False
    assert body["source"] == "substrate"


@pytest.mark.asyncio
async def test_missing_authorization_returns_401(client: httpx.AsyncClient):
    resp = await client.get("/observations")
    assert resp.status_code == 401
    assert resp.json()["error"] == "unauthorized"


@pytest.mark.asyncio
async def test_malformed_bearer_prefix_returns_401(client: httpx.AsyncClient):
    resp = await client.get(
        "/observations", headers={"Authorization": "Token abcdef"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_invalid_token_returns_401(client: httpx.AsyncClient):
    resp = await client.get(
        "/observations", headers={"Authorization": "Bearer nonexistent-token"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_expired_token_returns_401(
    client: httpx.AsyncClient, gateway_pool, seeded_actor, tenant_id
):
    # Mint a session that's already expired.
    token, ctx = await create_session(
        gateway_pool,
        actor_id=seeded_actor,
        tenant_id=tenant_id,
        ttl=timedelta(seconds=-1),
    )
    resp = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_revoked_token_returns_401(
    client: httpx.AsyncClient, gateway_pool, seeded_actor, tenant_id
):
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor, tenant_id=tenant_id
    )
    ok = await revoke_session(gateway_pool, ctx.session_id)
    assert ok
    resp = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 401


# ========================================================================
# Rate limiting
# ========================================================================


@pytest.mark.asyncio
async def test_rate_limit_allows_up_to_capacity_then_429(
    client: httpx.AsyncClient, valid_session, rate_limiter: RateLimiter
):
    token, _actor = valid_session
    headers = {"Authorization": f"Bearer {token}"}
    rate_limiter.clock = lambda: 0.0  # type: ignore[assignment]
    capacity, _ = rate_limiter.budget(RateTier.DEFAULT)
    # Burst the whole bucket on GET /observations (DEFAULT tier).
    ok_count = 0
    for _ in range(int(capacity)):
        r = await client.get("/observations", headers=headers)
        if r.status_code == 200:
            ok_count += 1
    assert ok_count == int(capacity)
    # Next request should be 429 — bucket empty, no appreciable refill.
    r2 = await client.get("/observations", headers=headers)
    assert r2.status_code == 429


@pytest.mark.asyncio
async def test_rate_limit_refills_via_virtual_clock(
    client: httpx.AsyncClient, valid_session, rate_limiter: RateLimiter
):
    """Inject a virtual clock; advance past refill time; verify 200."""
    token, _actor = valid_session
    headers = {"Authorization": f"Bearer {token}"}
    virtual_now = [0.0]
    rate_limiter.clock = lambda: virtual_now[0]  # type: ignore[assignment]
    capacity, refill_per_s = rate_limiter.budget(RateTier.DEFAULT)
    for _ in range(int(capacity)):
        r = await client.get("/observations", headers=headers)
        assert r.status_code == 200
    r = await client.get("/observations", headers=headers)
    assert r.status_code == 429
    # Advance the clock enough to refill to full.
    virtual_now[0] = capacity / refill_per_s + 1.0
    r = await client.get("/observations", headers=headers)
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_rate_limit_signal_ingest_tier_higher_than_default(
    rate_limiter: RateLimiter,
):
    default_cap, _ = rate_limiter.budget(RateTier.DEFAULT)
    signal_cap, _ = rate_limiter.budget(RateTier.SIGNAL_INGEST)
    assert signal_cap >= default_cap


# ========================================================================
# Tenant isolation
# ========================================================================


@pytest.mark.asyncio
async def test_tenant_a_cannot_see_tenant_b_observations(
    client: httpx.AsyncClient,
    gateway_pool,
    valid_session,
    valid_session_b,
    tenant_id,
    tenant_id_b,
):

    from lib.shared.ids import uuid7

    token_a, actor_a = valid_session
    token_b, actor_b = valid_session_b
    actor_c = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Charlie', 'active')
        """,
        actor_c,
        tenant_id,
    )

    # Insert visible and hidden observations directly. Actor A should see only
    # their own same-tenant row; tenant B should see only tenant B's authored row.
    for tid, actor_id, content_text in [
        (tenant_id, actor_a, "alpha own"),
        (tenant_id, actor_c, "alpha same tenant hidden"),
        (tenant_id_b, actor_b, "beta only"),
    ]:
        await gateway_pool.execute(
            """
            INSERT INTO observations (
                id, tenant_id, actor_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier
            ) VALUES ($1, $2, $3, $4, 'signal', 'test:harness',
                      '{}'::jsonb, $5, 'authoritative')
            """,
            uuid7(),
            tid,
            actor_id,
            datetime.now(timezone.utc),
            content_text,
        )

    r_a = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token_a}"}
    )
    r_b = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token_b}"}
    )
    assert r_a.status_code == 200 and r_b.status_code == 200
    texts_a = [o["content_text"] for o in r_a.json()["items"]]
    texts_b = [o["content_text"] for o in r_b.json()["items"]]
    assert "alpha own" in texts_a
    assert "alpha same tenant hidden" not in texts_a
    assert "beta only" not in texts_a
    assert "beta only" in texts_b
    assert "alpha own" not in texts_b

    r_a_limited = await client.get(
        "/observations?limit=1",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    assert r_a_limited.status_code == 200
    assert [o["content_text"] for o in r_a_limited.json()["items"]] == ["alpha own"]


@pytest.mark.asyncio
async def test_observations_can_be_filtered_by_source_prefix(
    client: httpx.AsyncClient,
    gateway_pool,
    valid_session,
    tenant_id,
):
    token, actor_id = valid_session
    for channel, text in [
        ("slack:message", "slack historical backfill"),
        ("github:event", "github live event"),
    ]:
        await gateway_pool.execute(
            """
            INSERT INTO observations (
                id, tenant_id, actor_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier
            ) VALUES ($1, $2, $3, $4, 'signal', $5,
                      '{}'::jsonb, $6, 'authoritative')
            """,
            uuid7(),
            tenant_id,
            actor_id,
            datetime.now(timezone.utc),
            channel,
            text,
        )

    response = await client.get(
        "/observations?source=slack",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    texts = [item["content_text"] for item in response.json()["items"]]
    assert texts == ["slack historical backfill"]


@pytest.mark.asyncio
async def test_mismatched_tenant_header_returns_403(
    client: httpx.AsyncClient, valid_session, tenant_id_b
):
    token, _ = valid_session
    resp = await client.get(
        "/observations",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Tenant-Id": str(tenant_id_b),
        },
    )
    assert resp.status_code == 403


# ========================================================================
# Session creation + request-id propagation
# ========================================================================


@pytest.mark.asyncio
async def test_post_auth_session_mints_and_validates(
    client: httpx.AsyncClient, gateway_pool, seeded_actor, tenant_id
):
    resp = await client.post(
        "/auth/session",
        json={
            "actor_id": str(seeded_actor),
            "tenant_id": str(tenant_id),
            "ttl_seconds": 60,
        },
    )
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["token"] and data["expires_at"]
    # Use it immediately.
    r = await client.get(
        "/observations",
        headers={"Authorization": f"Bearer {data['token']}"},
    )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_post_auth_session_wrong_actor_404(
    client: httpx.AsyncClient, tenant_id
):
    resp = await client.post(
        "/auth/session",
        json={
            "actor_id": str(uuid7()),
            "tenant_id": str(tenant_id),
        },
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_post_auth_session_rejects_tenant_header_mismatch(
    client: httpx.AsyncClient,
    seeded_actor,
    tenant_id,
    tenant_id_b,
):
    resp = await client.post(
        "/auth/session",
        headers={"X-Tenant-Id": str(tenant_id_b)},
        json={
            "actor_id": str(seeded_actor),
            "tenant_id": str(tenant_id),
        },
    )
    assert resp.status_code == 400
    assert resp.json() == {"error": "tenant_id_mismatch"}


@pytest.mark.asyncio
async def test_post_auth_session_rejects_invalid_tenant_header(
    client: httpx.AsyncClient,
    seeded_actor,
    tenant_id,
):
    resp = await client.post(
        "/auth/session",
        headers={"X-Tenant-Id": "not-a-uuid"},
        json={
            "actor_id": str(seeded_actor),
            "tenant_id": str(tenant_id),
        },
    )
    assert resp.status_code == 400
    assert resp.json() == {"error": "invalid_tenant_header"}


@pytest.mark.asyncio
async def test_request_id_header_echoed_on_response(
    client: httpx.AsyncClient, valid_session
):
    token, _ = valid_session
    r = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token}"}
    )
    assert "X-Request-Id" in r.headers
    # Must parse as UUID v7 per BUILD-PLAN §0 non-negotiable #7.
    UUID(r.headers["X-Request-Id"])


@pytest.mark.asyncio
async def test_structlog_binds_request_id_and_tenant(
    client: httpx.AsyncClient, valid_session, capsys
):
    """The Gateway emits a structured 'request' log line with
    request_id, tenant_id, actor_id bound. We assert on the stdout
    JSON emitted via structlog's PrintLoggerFactory.
    """
    token, actor_id = valid_session
    # Clear captured output (pytest captures everything to-date).
    capsys.readouterr()
    r = await client.get(
        "/observations", headers={"Authorization": f"Bearer {token}"}
    )
    assert r.status_code == 200
    out = capsys.readouterr().out
    # Parse line-by-line; look for the 'request' access-log entry.
    access_lines = [
        json.loads(line)
        for line in out.splitlines()
        if line.strip().startswith("{") and '"request"' in line
    ]
    assert access_lines, f"expected an access log line, got: {out!r}"
    last = access_lines[-1]
    assert last["event"] == "request"
    assert "request_id" in last
    assert last.get("actor_id") == str(actor_id)
    assert "tenant_id" in last


# ========================================================================
# Healthcheck
# ========================================================================


@pytest.mark.asyncio
async def test_healthz_unauthenticated(client: httpx.AsyncClient):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ========================================================================
# auth helpers — unit tests
# ========================================================================


def test_hash_token_is_hex_and_stable():
    t = "some-opaque-token"
    h1 = hash_token(t)
    h2 = hash_token(t)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex
    int(h1, 16)  # valid hex


def test_new_token_is_uuid_v7():
    t = new_token()
    parsed = UUID(t)
    assert parsed.version == 7
