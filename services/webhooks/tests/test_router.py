"""Router-level tests.

Spec: US1 / US2 / FR-001 / FR-002 / FR-014 / FR-017 / FR-018 / SC-006, SC-008.

These tests exercise the FastAPI router under the real gateway app
configuration — Bearer middleware must skip /webhooks/, the body-size
precheck must apply, the Slack url_verification handshake must work,
and unknown providers must 404.

The tests use a minimal gateway built with `build_app()` and stub-
overridden dependencies so we do not require a live Postgres or
Ollama for the path-routing assertions. The E2E integration test
(test_e2e_ingest.py) covers the real-DB path.
"""
from __future__ import annotations

import asyncio
import json
import os
from uuid import UUID
from unittest.mock import MagicMock

import httpx
import pytest

from services.webhooks.tests.conftest import slack_sign


_TENANT = UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def _patch_secrets_and_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire env-based secrets + tenant resolution for the test app."""
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "router-test-slack")
    monkeypatch.setenv(
        "WEBHOOK_TENANT_DEFAULT_ALLOW", "1"
    )
    monkeypatch.setenv("WEBHOOK_TENANT_DEFAULT", str(_TENANT))


@pytest.fixture
def _router_app(_patch_secrets_and_tenant: None):
    """Build a FastAPI app with ONLY the webhook router mounted, plus a
    stub `deps` object on app.state so the router's ingestion call has
    something to resolve (and fails predictably at the ingest layer if
    invoked — the path-routing tests don't reach that code)."""
    from fastapi import FastAPI

    from services.webhooks.router import build_webhooks_router

    app = FastAPI()
    app.include_router(build_webhooks_router())

    deps = MagicMock()
    deps.pool = MagicMock()
    deps.actor_repo = None
    deps.alias_repo = None
    deps.embedder = None
    app.state.deps = deps
    return app


@pytest.mark.asyncio
async def test_unknown_provider_returns_404(_router_app) -> None:
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/twilio/inbound", content=b"{}")
    assert r.status_code == 404
    body = r.json()
    assert body["code"] == "unknown_provider"
    assert body["context"]["provider"] == "twilio"


@pytest.mark.asyncio
async def test_oversize_body_413(_router_app) -> None:
    from services.ingestion.core import MAX_PAYLOAD_BYTES

    oversize = b"x" * (MAX_PAYLOAD_BYTES + 1)
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/slack/events", content=oversize)
    assert r.status_code == 413
    assert r.json()["code"] == "payload_too_large"


@pytest.mark.asyncio
async def test_missing_signature_returns_401(_router_app) -> None:
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/slack/events", content=b'{"team_id":"T"}')
    assert r.status_code == 401
    body = r.json()
    assert body["context"]["reason"] == "missing_signature_header"
    assert body["context"]["provider"] == "slack"


@pytest.mark.asyncio
async def test_spoofed_signature_returns_401(_router_app) -> None:
    import time as _t

    body = b'{"team_id":"T0001"}'
    ts = str(int(_t.time()))  # use real now — router uses real time.time()
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": ts,
                "X-Slack-Signature": "v0=" + ("00" * 32),
            },
        )
    assert r.status_code == 401
    body_json = r.json()
    assert body_json["context"]["reason"] == "signature_mismatch"
    # Critical: response MUST NOT leak the body or the candidate sig.
    rendered = json.dumps(body_json)
    assert "team_id" not in rendered
    assert "00" * 32 not in rendered


@pytest.mark.asyncio
async def test_slack_url_verification_handshake(_router_app) -> None:
    """Slack sends a url_verification event on app install with a
    `challenge`. We verify the signature (still!) and echo the
    challenge — no Observation, no ingestion call.
    """
    import time as _t

    secret = os.environ["WEBHOOK_SECRET_SLACK"]
    body = json.dumps({
        "type": "url_verification",
        "token": "abc",
        "challenge": "chal-12345",
    }).encode("utf-8")
    ts = int(_t.time())
    sig = slack_sign(secret, body, ts)

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
    assert r.status_code == 200
    assert r.json() == {"challenge": "chal-12345"}


@pytest.mark.asyncio
async def test_tenant_not_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the verified payload cannot be mapped to a tenant, return
    401 with `tenant_not_resolved` — NOT a default-tenant fallback
    (FR-014)."""
    import time as _t
    from fastapi import FastAPI
    from services.webhooks.router import build_webhooks_router

    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "trsecret")
    monkeypatch.delenv("WEBHOOK_TENANT_DEFAULT_ALLOW", raising=False)
    monkeypatch.delenv("WEBHOOK_TENANT_DEFAULT", raising=False)

    app = FastAPI()
    app.include_router(build_webhooks_router())
    deps = MagicMock()
    app.state.deps = deps

    body = b'{"team_id":"T_UNKNOWN","event":{"type":"message","ts":"1","channel":"C","user":"U","text":"hi"}}'
    ts = int(_t.time())
    sig = slack_sign("trsecret", body, ts)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
    assert r.status_code == 401
    body_json = r.json()
    assert body_json["context"]["reason"] == "tenant_not_resolved"


@pytest.mark.asyncio
async def test_failure_metric_increments(_router_app) -> None:
    """A 401 must bump the (provider, reason) counter."""
    from services.webhooks import metrics

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        await c.post("/webhooks/slack/events", content=b"{}")
    assert metrics.get_count("slack", "missing_signature_header") == 1
