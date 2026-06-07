"""Regression: webhook routes must accept the bare `/webhooks/{provider}`
path WITHOUT a 307 redirect.

GitHub (and most webhook senders) do NOT follow 3xx on delivery — they treat
a redirect as a failed delivery. Before the fix the router only registered
`/{provider}/{subpath:path}`, so `POST /webhooks/github` (no trailing slash,
the form a sender is often configured with) 307-redirected to
`/webhooks/github/` and silently failed. The handler is now registered on both
`/{provider}` and `/{provider}/{subpath:path}`.

These tests build a minimal app with only the webhook router mounted and use
httpx with redirects OFF, so a 307 would surface as status 307.
"""
from __future__ import annotations

import httpx
import pytest

from services.app.webhooks.router import build_webhooks_router


def _app():
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(build_webhooks_router())
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/webhooks/twilio", "/webhooks/twilio/"])
async def test_bare_and_slash_paths_route_without_redirect(path: str) -> None:
    """Both the bare and trailing-slash forms route straight to the handler
    (here an unknown provider → 404), never a 307 redirect."""
    transport = httpx.ASGITransport(app=_app())
    async with httpx.AsyncClient(
        transport=transport, base_url="http://t", follow_redirects=False,
    ) as c:
        r = await c.post(path, content=b"{}")
    assert r.status_code != 307, f"{path} should not 307-redirect"
    assert r.status_code == 404
    assert r.json()["code"] == "unknown_provider"


@pytest.mark.asyncio
async def test_github_bare_path_reaches_handler_not_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`POST /webhooks/github` (bare) reaches the verifier — an unsigned body
    is rejected at verification (401), NOT bounced with a 307."""
    monkeypatch.setenv("WEBHOOK_SECRET_GITHUB", "slash-test-secret")
    transport = httpx.ASGITransport(app=_app())
    async with httpx.AsyncClient(
        transport=transport, base_url="http://t", follow_redirects=False,
    ) as c:
        r = await c.post(
            "/webhooks/github",
            content=b'{"installation":{"id":1}}',
            headers={"X-GitHub-Event": "push"},
        )
    # The regression: bare path must NOT 307-redirect, and must reach the
    # github handler (not fall through as an unknown route / 404). It runs the
    # handler body — here 503 because this minimal app wires no tenant_resolver
    # (a real gateway does); the point is it ROUTED, not redirected.
    assert r.status_code != 307, "bare /webhooks/github must not 307-redirect"
    assert r.status_code != 404, "bare /webhooks/github must reach the handler"
