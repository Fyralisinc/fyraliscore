"""Contract test: the OAuth token-refresh core exchanges + parses the REAL
documented token-endpoint shapes for QBO / Ramp / Gusto / Carta.

Guards the Phase-3 CRITICAL blocker (findings #24/#26/#38/#40): poll installs
stopped fetching once their ~1h access token expired because no refresh exchange
was implemented. `oauth_refresh.refresh_access_token` now performs the documented
exchange; this test pins each provider's request shape (grant, client-auth
placement, refresh_token rotation) and response parsing against doc-sourced
fixtures, using an httpx MockTransport (no network, no real credentials).

Carta is special: NO refresh grant — it re-mints via `client_credentials` and
returns no refresh token.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone

import httpx
import pytest

from services.ingest.integrations.oauth_refresh import (
    REFRESH_CONFIGS,
    OAuthRefreshError,
    refresh_access_token,
)
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_CLIENT_ID = "test-client-id"
_CLIENT_SECRET = "test-client-secret"

# (provider, fixture_stem, client_creds_in_basic_header, rotates_refresh_token)
_CASES = [
    ("quickbooks", "refresh", True, True),
    ("ramp", "refresh", True, True),
    ("gusto", "refresh", False, True),
    ("carta", "client_credentials", False, False),
]


def _mock_http(status: int, body: dict, captured: dict) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        captured["form"] = dict(httpx.QueryParams(request.content.decode("utf-8")))
        return httpx.Response(status, json=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("provider,stem,basic,rotated", _CASES)
async def test_oauth_refresh_request_and_response_contract(
    provider, stem, basic, rotated,
):
    fixture = load_fixture(provider, "oauth_token", stem)
    config = REFRESH_CONFIGS[provider]
    resp_body = fixture.response["body"]
    old_refresh = fixture.request.get("form", {}).get("refresh_token")
    captured: dict = {}

    async with _mock_http(int(fixture.response["status"]), resp_body, captured) as http:
        token = await refresh_access_token(
            http,
            config,
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            refresh_token=old_refresh,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    # --- response parsing matches the documented shape ---
    assert token.access_token == resp_body["access_token"]
    assert token.expires_in == resp_body["expires_in"]
    assert token.expires_at > token.obtained_at
    if rotated:
        assert token.refresh_token == resp_body["refresh_token"]
    else:
        # client_credentials (Carta) returns no refresh token.
        assert token.refresh_token is None

    # --- the request our code SENT matches the documented contract ---
    sent = captured["request"]
    form = captured["form"]
    assert sent.method == "POST"
    assert str(sent.url) == config.token_url
    assert form["grant_type"] == config.grant_type
    if config.grant_type == "refresh_token":
        assert form["refresh_token"] == old_refresh
    if basic:
        expected = base64.b64encode(
            f"{_CLIENT_ID}:{_CLIENT_SECRET}".encode("utf-8")
        ).decode("ascii")
        assert sent.headers["Authorization"] == f"Basic {expected}"
        assert "client_secret" not in form
    else:
        assert form.get("client_id") == _CLIENT_ID
        assert form.get("client_secret") == _CLIENT_SECRET
        assert "Authorization" not in sent.headers


async def test_carta_uses_client_credentials_not_refresh_grant():
    """Carta has no refresh grant — the config must re-mint via client_credentials
    and require no refresh_token input."""
    config = REFRESH_CONFIGS["carta"]
    assert config.grant_type == "client_credentials"
    assert config.rotates_refresh_token is False


async def test_oauth_refresh_non_2xx_raises_degraded_signal():
    """A 400 invalid_grant (revoked / stale rotated refresh token) raises
    OAuthRefreshError carrying the HTTP status — the caller marks the shard
    degraded rather than crashing or silently dropping data."""
    config = REFRESH_CONFIGS["quickbooks"]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid_grant"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(OAuthRefreshError) as exc:
            await refresh_access_token(
                http, config, client_id="x", client_secret="y",
                refresh_token="stale-rotated-token",
            )
    assert exc.value.status == 400
    assert exc.value.provider == "quickbooks"


async def test_quickbooks_refresh_rotation_persists_new_token():
    """Intuit rotates the refresh token; if a refresh response echoes a new
    refresh_token we must surface it (persisting it is the caller's job)."""
    config = REFRESH_CONFIGS["quickbooks"]
    captured: dict = {}
    body = {
        "access_token": "new-access",
        "refresh_token": "ROTATED-refresh",
        "expires_in": 3600,
    }
    async with _mock_http(200, body, captured) as http:
        token = await refresh_access_token(
            http, config, client_id="c", client_secret="s",
            refresh_token="OLD-refresh",
        )
    assert token.refresh_token == "ROTATED-refresh"
    assert token.refresh_token != "OLD-refresh"
