"""Gmail Pub/Sub push-ingress mount + readiness tests.

The ingress is now mounted UNCONDITIONALLY (so Google's pushes hit a real
endpoint, never a silent 404) and reports its readiness explicitly: when the
OIDC env (`GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE` + `GMAIL_PUBSUB_PUSH_OIDC_SA`) is
absent it returns `503 not_configured` rather than crashing (500) or silently
disappearing. These tests pin that contract — no DB / auth needed because the
not-configured guard short-circuits before either.
"""
from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI


pytestmark = pytest.mark.asyncio


def _app() -> FastAPI:
    from services.app.webhooks.gmail_pubsub import router

    app = FastAPI()
    app.include_router(router)
    return app


async def test_unconfigured_push_returns_503_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_ENDPOINT", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_SA", raising=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app()), base_url="http://t",
    ) as c:
        # No bearer token, no body: the not-configured guard must fire FIRST,
        # so the endpoint exists (not 404) and is explicit (not 500/401).
        r = await c.post("/webhooks/gmail/pubsub")
    assert r.status_code == 503, r.text
    body = r.json()
    assert body["status"] == "not_configured"
    assert body["reason"] == "gmail_pubsub_oidc_env_missing"


async def test_partial_config_still_reports_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Audience set but push-SA missing → still not fully configured.
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", "https://app.test/webhooks/gmail/pubsub")
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_SA", raising=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app()), base_url="http://t",
    ) as c:
        r = await c.post("/webhooks/gmail/pubsub")
    assert r.status_code == 503
    assert r.json()["status"] == "not_configured"


async def test_is_pubsub_configured_reflects_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.app.webhooks import gmail_pubsub

    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_ENDPOINT", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_SA", raising=False)
    assert gmail_pubsub.is_pubsub_configured() is False

    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", "https://app.test/webhooks/gmail/pubsub")
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_SA", "push-sa@proj.iam.gserviceaccount.com")
    assert gmail_pubsub.is_pubsub_configured() is True


async def test_configured_but_missing_bearer_is_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When configured, the missing-bearer path is reached again (401) — proving
    the not-configured guard only short-circuits when the env is absent."""
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", "https://app.test/webhooks/gmail/pubsub")
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_SA", "push-sa@proj.iam.gserviceaccount.com")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app()), base_url="http://t",
    ) as c:
        r = await c.post("/webhooks/gmail/pubsub")
    assert r.status_code == 401


async def test_configured_but_malformed_bearer_is_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", "https://app.test/webhooks/gmail/pubsub")
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_SA", "push-sa@proj.iam.gserviceaccount.com")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app()), base_url="http://t",
    ) as c:
        r = await c.post(
            "/webhooks/gmail/pubsub",
            headers={"Authorization": "Bearer malformed"},
        )

    assert r.status_code == 401
    assert r.json()["detail"] == "oidc_invalid"
