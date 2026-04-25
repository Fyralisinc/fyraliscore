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
    for key in ("greeting", "query_grid", "cards", "close_line", "status"):
        assert key in payload, f"missing key {key!r} in home payload"
    # Greeting body_html non-empty (cache populated)
    assert isinstance(payload["greeting"]["body_html"], str)
    assert isinstance(payload["query_grid"]["queries"], list)
    assert isinstance(payload["cards"], list)


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


