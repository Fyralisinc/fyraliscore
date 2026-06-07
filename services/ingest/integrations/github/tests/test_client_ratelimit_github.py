"""Regression: GitHub rate-limit / transient errors are classified
`recoverable` so the backfill parks (retries) the shard instead of
terminal-failing it.

Covers:
  - `_api_error_from_response` recoverability classification (primary 403,
    secondary 429, 5xx vs. terminal 401/404/4xx).
  - `_get_with_rl_retry` does NOT sleep-loop on the PRIMARY limit (403 +
    X-RateLimit-Remaining: 0); the reset can be ≤1h away, so it returns for
    the caller to park the shard rather than blocking the worker.
"""
from __future__ import annotations

import httpx
import pytest

from datetime import datetime, timedelta, timezone

from services.ingest.integrations.github.client import (
    CachedInstallationToken,
    GithubClient,
    _api_error_from_response,
)


def _resp(status: int, headers: dict | None = None, body: dict | None = None):
    return httpx.Response(
        status, headers=headers or {}, json=body or {"message": "x"},
        request=httpx.Request("GET", "http://t/repos/o/r/pulls"),
    )


def test_primary_rate_limit_403_is_recoverable():
    err = _api_error_from_response(
        _resp(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "999"})
    )
    assert err.code == "github_api_rate_limited"
    assert err.recoverable is True


def test_secondary_rate_limit_429_is_recoverable():
    err = _api_error_from_response(_resp(429, {"Retry-After": "60"}))
    assert err.code == "github_api_rate_limited"
    assert err.recoverable is True


def test_5xx_is_recoverable():
    assert _api_error_from_response(_resp(502)).recoverable is True
    assert _api_error_from_response(_resp(503)).recoverable is True


def test_auth_and_notfound_are_terminal():
    # 401/404 are genuine auth/config failures (the chokepoint handles real
    # revocation by disabling the install) — fail fast, not recoverable.
    assert _api_error_from_response(_resp(401)).recoverable is False
    assert _api_error_from_response(_resp(404)).recoverable is False
    # A plain 403 WITHOUT the budget-exhausted header is not a rate limit.
    assert _api_error_from_response(_resp(403)).recoverable is False


@pytest.mark.asyncio
async def test_primary_limit_does_not_sleep_loop(monkeypatch):
    """The primary limit returns after ONE GET (no retry sleeps); a bounded
    in-call retry is reserved for the short secondary limit."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "999"},
            json={"message": "API rate limit exceeded"},
        )

    slept = {"total": 0.0}

    async def _no_sleep(d):
        slept["total"] += d

    monkeypatch.setattr("asyncio.sleep", _no_sleep)

    client = GithubClient(
        pool=None,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        api_base_url="http://t",
    )
    resp = await client._get_with_rl_retry("http://t/repos/o/r/pulls", {})
    assert resp.status_code == 403
    assert calls["n"] == 1, "primary limit must not retry-loop"
    assert slept["total"] == 0.0, "must not sleep the worker on the primary limit"
    await client.aclose()


@pytest.mark.asyncio
async def test_stale_cached_token_401_invalidates_and_remints(monkeypatch):
    """REGRESSION (P2 token-cache interaction): the process-wide token cache
    can hold a token that's rejected server-side (revoked / upstream re-issue)
    before its client-side expiry. A 401 must invalidate the cached token and
    re-mint ONCE, not wedge every read with 'Bad credentials'."""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization", ""))
        if request.headers.get("Authorization") == "token STALE":
            return httpx.Response(401, json={"message": "Bad credentials"})
        return httpx.Response(
            200, json=[{"id": 1, "updated_at": "2026-01-01T00:00:00Z"}],
            headers={"ETag": 'W/"x"'},
        )

    client = GithubClient(
        pool=None,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        api_base_url="http://t", backfill_installation_id="42",
    )
    # Seed a STALE (but not-yet-client-expired) cached token.
    client._installation_tokens["42"] = CachedInstallationToken(
        token="STALE", expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    async def fake_mint(inst):
        cached = client._installation_tokens.get(inst)
        if cached is not None:
            return cached.token
        fresh = CachedInstallationToken(
            token="FRESH", expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        client._installation_tokens[inst] = fresh
        return fresh.token

    monkeypatch.setattr(client, "mint_installation_token", fake_mint)

    records, _etag, _next = await client.list_repo_events(
        owner="o", repo="r", event_type="issues", page=1, per_page=30,
    )
    assert seen == ["token STALE", "token FRESH"], (
        "must use the cached token, then re-mint after the 401"
    )
    assert len(records) == 1, "the re-minted retry must succeed (200)"
    await client.aclose()
