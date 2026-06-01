"""IN-08 US3: install-handler tests.

GET /integrations/slack/install:
  - Bearer-authed.
  - Issues an oauth_install_states row.
  - Returns 302 to Slack's oauth/v2/authorize with state in the query.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest

from fastapi import FastAPI

from services.integrations.router import build_integrations_router
from services.integrations.slack import oauth as slack_oauth


pytestmark = pytest.mark.integration


class _Auth:
    """Stand-in for the Bearer middleware's AuthContext."""
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id
        self.actor_id = uuid4()


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"in08-us3-{tid.hex[:8]}",
    )
    return tid


def _make_app(pool: asyncpg.Pool, auth: _Auth | None) -> FastAPI:
    """Build an isolated FastAPI app with only the integrations router
    mounted. We inject `app.state.pool` directly (no full build_app)
    so the install handler has what it needs without the lifespan."""
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        # Stand in for the Bearer middleware: attach a synthetic auth
        # context so the install handler can read tenant_id from it.
        if auth is not None:
            request.state.auth = auth
        return await call_next(request)

    return app


async def test_install_redirect_to_slack(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLACK_CLIENT_ID", "1234.5678")
    monkeypatch.setenv(
        "SLACK_REDIRECT_URI", "https://app.fyralis.test/integrations/slack/callback",
    )
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-please")

    app = _make_app(fresh_db, _Auth(_tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/slack/install", follow_redirects=False)

    assert r.status_code == 302
    location = r.headers["location"]
    parsed = urlparse(location)
    assert parsed.scheme == "https"
    assert parsed.netloc == "slack.com"
    assert parsed.path == "/oauth/v2/authorize"
    qs = parse_qs(parsed.query)
    assert qs["client_id"] == ["1234.5678"]
    assert "channels:history" in qs["scope"][0]
    assert "team:read" in qs["scope"][0]
    assert "state" in qs and "." in qs["state"][0]

    # oauth_install_states has exactly one new row for this tenant.
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, provider, consumed_at "
        "FROM oauth_install_states WHERE tenant_id = $1",
        _tenant,
    )
    assert row is not None
    assert row["provider"] == "slack"
    assert row["consumed_at"] is None


async def test_install_requires_bearer(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLACK_CLIENT_ID", "1234.5678")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://x/y")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key")

    app = _make_app(fresh_db, auth=None)  # no auth
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/slack/install", follow_redirects=False)

    assert r.status_code == 401
    assert r.json()["code"] == "missing_bearer"


async def test_install_fails_when_slack_client_unconfigured(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SLACK_CLIENT_ID", raising=False)
    monkeypatch.delenv("SLACK_REDIRECT_URI", raising=False)
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key")

    app = _make_app(fresh_db, _Auth(_tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/slack/install", follow_redirects=False)

    assert r.status_code == 500
    assert r.json()["code"] == "slack_client_unconfigured"


async def test_short_team_hash_is_deterministic_16_hex() -> None:
    """SC: the redirect's `?team=` hash is `blake2b(team_id, digest_size=8)`."""
    h = slack_oauth.short_team_hash("T_FYRALIS_DEV")
    assert isinstance(h, str)
    assert len(h) == 16
    # Deterministic.
    assert slack_oauth.short_team_hash("T_FYRALIS_DEV") == h
    # Different inputs → different outputs.
    assert slack_oauth.short_team_hash("T_OTHER") != h
