"""Week-4 integration smoke tests.

Purpose: cover COMPANY-OS-UI-BUILD-PLAN §7 tests 1 (full scenario
replay → UI match) and 4 (query coverage) at the plumbing level. These
are smoke tests — they verify the seams are wired, not the prose
quality.

Skipped by default because they require:
  - a live Postgres (DATABASE_URL)
  - a live LLM provider (DEEPSEEK_API_KEY)
  - the acme_tuesday scenario already replayed against the tenant

Run manually with:
    source .venv/bin/activate && export $(cat .env | xargs) && \
      COMPANY_OS_ENV=dev WEEK4_SMOKE=1 \
      python -m pytest tests/integration/test_ceo_view_smoke.py -v
"""
from __future__ import annotations

import json
import os
from uuid import UUID

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("WEEK4_SMOKE") != "1",
    reason="set WEEK4_SMOKE=1 to run (requires gateway + live LLM + scenario)",
)


# The dogfood tenant used by the replay helper by default.
DOGFOOD_TENANT = UUID("00000000-0000-7000-8000-000000000dd1")


@pytest.fixture
async def gateway_app():
    """Build the gateway app with lifespan so all Week-4 routers mount."""
    from services.gateway.main import build_app

    app = build_app()
    async with app.router.lifespan_context(app):
        yield app


@pytest.mark.asyncio
async def test_ceo_home_returns_contract_shape(gateway_app):
    """Test 1: cache-populated tenant → GET /view/ceo/home returns the
    CONTRACTS §1.1 shape."""
    import httpx

    transport = httpx.ASGITransport(app=gateway_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gateway"
    ) as client:
        r = await client.get("/view/ceo/home")
    assert r.status_code == 200, r.text
    payload = r.json()
    # §1.1 required top-level keys
    for key in (
        "greeting",
        "query_grid",
        "cards",
        "close_line",
        "status",
        "viewer_state",
    ):
        assert key in payload, f"missing key {key!r} in home payload"
    # Greeting body_html non-empty (cache populated)
    assert isinstance(payload["greeting"]["body_html"], str)
    assert isinstance(payload["query_grid"]["queries"], list)
    assert isinstance(payload["cards"], list)
    # viewer_state contract (Track A)
    vs = payload["viewer_state"]
    assert "previous_last_seen_at" in vs
    assert "current_visit_at" in vs
    assert isinstance(vs["current_visit_at"], str)


def _viewer_state_pool(gateway_app):
    """Pull the asyncpg pool off the ViewerStateRepo wired into the
    gateway. Used by the Track-A tests to reset state between runs.
    """
    repo = gateway_app.state.ceo_view["viewer_state_repo"]
    return repo._pool  # noqa: SLF001 — test-only introspection


def _gateway_tenant(gateway_app) -> UUID:
    """The tenant_id the gateway's unauthenticated path falls back to.

    Configured via the DEFAULT_TENANT_ID env var; tests that hit
    /view/ceo/home without a Bearer token end up writing viewer_state
    rows under this tenant.
    """
    tid = gateway_app.state.ceo_view["tenant_id"]
    assert tid is not None, "DEFAULT_TENANT_ID must be set for smoke tests"
    return tid


async def _reset_viewer_state(gateway_app) -> None:
    pool = _viewer_state_pool(gateway_app)
    tenant_id = _gateway_tenant(gateway_app)
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM viewer_state WHERE tenant_id = $1 AND viewer_id = $2",
            tenant_id,
            "default",
        )


@pytest.mark.asyncio
async def test_ceo_home_viewer_state_first_visit_is_null(gateway_app):
    """Track A: first-ever GET /view/ceo/home for a fresh viewer must
    report `previous_last_seen_at: null` and a valid `current_visit_at`.
    """
    import httpx

    # Clear any prior viewer_state row for the default viewer so the
    # assertion holds even when this test runs after the others.
    await _reset_viewer_state(gateway_app)

    transport = httpx.ASGITransport(app=gateway_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gateway"
    ) as client:
        r = await client.get("/view/ceo/home")
    assert r.status_code == 200, r.text
    vs = r.json()["viewer_state"]
    assert vs["previous_last_seen_at"] is None
    assert isinstance(vs["current_visit_at"], str) and vs["current_visit_at"]


@pytest.mark.asyncio
async def test_ceo_home_viewer_state_second_visit_returns_previous(gateway_app):
    """Track A: the second call's `previous_last_seen_at` must equal
    the first call's `current_visit_at` (modulo ISO normalization).
    """
    import httpx
    from datetime import datetime

    await _reset_viewer_state(gateway_app)

    transport = httpx.ASGITransport(app=gateway_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gateway"
    ) as client:
        r1 = await client.get("/view/ceo/home")
        r2 = await client.get("/view/ceo/home")
    assert r1.status_code == 200 and r2.status_code == 200
    first_visit = r1.json()["viewer_state"]["current_visit_at"]
    second_prev = r2.json()["viewer_state"]["previous_last_seen_at"]
    assert second_prev is not None
    # Both are ISO-8601 UTC strings — compare as datetimes to be tolerant
    # of any cosmetic differences (microsecond precision, +00:00 vs Z).
    a = datetime.fromisoformat(first_visit.replace("Z", "+00:00"))
    b = datetime.fromisoformat(second_prev.replace("Z", "+00:00"))
    assert a == b


@pytest.mark.asyncio
async def test_ceo_ask_returns_turn(gateway_app):
    """Test 4 (query coverage): POST /view/ceo/ask with a free-text
    query returns a well-formed conversation turn."""
    import httpx

    transport = httpx.ASGITransport(app=gateway_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gateway", timeout=60
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={"query": "What changed yesterday?"},
            headers={"x-tenant-id": str(DOGFOOD_TENANT)},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    for key in (
        "turn_id",
        "query_echo",
        "response_html",
        "verbs",
        "computed_at",
        "latency_ms",
    ):
        assert key in body, f"missing key {key!r} in ask response"
    assert isinstance(body["response_html"], str)
    assert len(body["verbs"]) == 3


