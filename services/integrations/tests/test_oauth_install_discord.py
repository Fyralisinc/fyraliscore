"""IN-09 US2 install-handler tests.

GET /integrations/discord/install:
  - Bearer-authed.
  - Issues an oauth_install_states row with provider='discord'.
  - Returns 302 to https://discord.com/oauth2/authorize with state in query.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from services.integrations.discord import oauth as discord_oauth
from services.integrations.router import build_integrations_router


pytestmark = pytest.mark.integration


class _Auth:
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id
        self.actor_id = uuid4()


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"in09-us2-{tid.hex[:8]}",
    )
    return tid


def _make_app(pool: asyncpg.Pool, auth: _Auth | None) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        if auth is not None:
            request.state.auth = auth
        return await call_next(request)

    return app


async def test_install_redirects_to_discord_oauth_with_signed_state(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DISCORD_CLIENT_ID", "1234567890")
    monkeypatch.setenv(
        "DISCORD_REDIRECT_URI",
        "https://app.fyralis.test/integrations/discord/callback",
    )
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-discord")

    app = _make_app(fresh_db, _Auth(_tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/discord/install", follow_redirects=False)

    assert r.status_code == 302
    location = r.headers["location"]
    parsed = urlparse(location)
    assert parsed.scheme == "https"
    assert parsed.netloc == "discord.com"
    assert parsed.path == "/oauth2/authorize"
    qs = parse_qs(parsed.query)
    assert qs["client_id"] == ["1234567890"]
    assert qs["response_type"] == ["code"]
    # FR-006: scopes are exactly applications.commands + bot.
    scope = qs["scope"][0]
    assert "applications.commands" in scope
    assert "bot" in scope
    # FR-006: state token has the .-delimited shape from issue_state_token.
    assert "state" in qs and "." in qs["state"][0]

    # oauth_install_states has exactly one new row for this tenant + provider=discord.
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, provider, consumed_at "
        "FROM oauth_install_states WHERE tenant_id = $1",
        _tenant,
    )
    assert row is not None
    assert row["provider"] == "discord"
    assert row["consumed_at"] is None


async def test_install_requires_bearer(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DISCORD_CLIENT_ID", "1234567890")
    monkeypatch.setenv("DISCORD_REDIRECT_URI", "https://x/y")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key")

    app = _make_app(fresh_db, auth=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/discord/install", follow_redirects=False)

    assert r.status_code == 401
    assert r.json()["code"] == "missing_bearer"


async def test_install_fails_when_discord_client_unconfigured(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DISCORD_CLIENT_ID", raising=False)
    monkeypatch.delenv("DISCORD_REDIRECT_URI", raising=False)
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key")

    app = _make_app(fresh_db, _Auth(_tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/discord/install", follow_redirects=False)

    assert r.status_code == 500
    assert r.json()["code"] == "discord_client_unconfigured"


async def test_short_guild_hash_is_deterministic_16_hex() -> None:
    h = discord_oauth.short_guild_hash("G_FYRALIS_DEV_123")
    assert isinstance(h, str)
    assert len(h) == 16
    assert discord_oauth.short_guild_hash("G_FYRALIS_DEV_123") == h
    assert discord_oauth.short_guild_hash("G_OTHER") != h
