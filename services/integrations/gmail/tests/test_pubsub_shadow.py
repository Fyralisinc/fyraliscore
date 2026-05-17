"""M2.2 — Gmail Pub/Sub shadow-write tests.

Verifies the shadow block added to `services/webhooks/gmail_pubsub.py`:
  - After handle_push() returns a tenant-resolved result, the
    Pub/Sub notification payload is shadow-written with
    ingress_kind="pubsub".
  - Shadow failure does NOT break the response — Pub/Sub still
    receives 200 and the inline handle_push() call ran.
  - The shadow body is the request's raw bytes (not a re-parsed
    dict); content_hash determinism is preserved.

`verify_pubsub_oidc_token` and `handle_push` are patched at the
gmail_pubsub module level so the tests don't need real Google
infrastructure.
"""
from __future__ import annotations

import base64
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from services.ingestion import shadow_write as shadow_write_module


_TENANT = UUID("44444444-4444-4444-4444-444444444444")


def _build_envelope(email: str = "alice@acme.com", history_id: str = "12345") -> dict:
    inner = {"emailAddress": email, "historyId": history_id}
    return {
        "message": {
            "data": base64.b64encode(json.dumps(inner).encode("utf-8")).decode("ascii"),
            "messageId": "msg-id-12345",
            "publishTime": "2026-05-17T10:00:00Z",
        },
        "subscription": "projects/fyralis-test/subscriptions/gmail-tenant-sub",
    }


@pytest.fixture
def _env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", "test-audience")
    monkeypatch.setenv("GMAIL_PUBSUB_PUSH_OIDC_SA", "push@fyralis.iam.gserviceaccount.com")


@pytest.fixture
def _patched_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the OIDC verifier with a no-op so the request reaches
    the shadow block without needing a real Google-signed JWT.
    """
    monkeypatch.setattr(
        "services.webhooks.gmail_pubsub.verify_pubsub_oidc_token",
        AsyncMock(return_value=None),
    )


@pytest.fixture
def _handle_push_returns_tenant(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Patch handle_push to return a tenant-resolved result dict.
    Returns the AsyncMock so tests can adjust per-test."""
    mock = AsyncMock(return_value={
        "status": "drained",
        "tenant_id": str(_TENANT),
    })
    monkeypatch.setattr(
        "services.webhooks.gmail_pubsub.handle_push",
        mock,
    )
    return mock


@pytest.fixture
def _shadow_app(
    _env_vars: None,
    _patched_oidc: None,
    _handle_push_returns_tenant: AsyncMock,
):
    """FastAPI app with the gmail_pubsub router + shadow-path deps
    wired. handle_push is patched to return a tenant-resolved result
    so the shadow block fires.
    """
    from fastapi import FastAPI

    from services.webhooks.gmail_pubsub import router as pubsub_router

    app = FastAPI()
    app.include_router(pubsub_router)

    deps = MagicMock()
    deps.pool = MagicMock()
    app.state.deps = deps

    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    app.state.s3_raw_client = s3

    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    app.state.kafka_producer = kafka

    flags = MagicMock()
    flags.get_bool = AsyncMock(return_value=True)
    app.state.tenant_flags = flags

    shadow_write_module.reset_metrics()
    return app


async def _post_pubsub(app, envelope: dict) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    body = json.dumps(envelope).encode("utf-8")
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post(
            "/webhooks/gmail/pubsub",
            content=body,
            headers={
                "Authorization": "Bearer fake-jwt-token",
                "Content-Type": "application/json",
            },
        )


# ---------------------------------------------------------------------
# 1. Happy path — Pub/Sub push triggers inline handle_push AND shadow
# writes the notification payload.
# ---------------------------------------------------------------------

async def test_pubsub_notification_writes_shadow(_shadow_app, _handle_push_returns_tenant):
    envelope = _build_envelope()
    r = await _post_pubsub(_shadow_app, envelope)
    assert r.status_code == 200, r.text

    # Inline path: handle_push was called with the parsed envelope.
    assert _handle_push_returns_tenant.await_count == 1

    # Shadow path: exactly one S3 PUT, exactly one Kafka publish.
    s3 = _shadow_app.state.s3_raw_client
    kafka = _shadow_app.state.kafka_producer
    assert s3.put_if_absent.await_count == 1
    assert kafka.produce.await_count == 1

    _, kafka_kwargs = kafka.produce.await_args
    assert kafka_kwargs["topic"] == "ingestion.raw"
    assert kafka_kwargs["key"] == str(_TENANT).encode("utf-8")

    out_envelope = json.loads(kafka_kwargs["value"])
    assert out_envelope["source"] == "gmail"
    assert out_envelope["ingress_kind"] == "pubsub"
    assert out_envelope["tenant_id"] == str(_TENANT)
    assert out_envelope["ingress_metadata"]["event_type"] == "pubsub.notification"
    assert out_envelope["ingress_metadata"]["messageId"] == "msg-id-12345"
    assert out_envelope["ingress_metadata"]["subscription"].endswith(
        "subscriptions/gmail-tenant-sub"
    )

    # Response body must not leak the internal tenant_id key.
    body = r.json()
    assert "tenant_id" not in body
    assert body.get("status") == "drained"


# ---------------------------------------------------------------------
# 2. LOAD-BEARING SAFETY TEST.
# Shadow failure must NOT break the Pub/Sub response. Pub/Sub treats
# anything but 200 as a delivery retry signal — a shadow-path bug
# turning the response into 500 would cause storm-level retries.
# ---------------------------------------------------------------------

async def test_pubsub_shadow_failure_does_not_break_fetch(
    _shadow_app, _handle_push_returns_tenant,
):
    _shadow_app.state.s3_raw_client.put_if_absent = AsyncMock(
        side_effect=RuntimeError("simulated S3 timeout"),
    )

    envelope = _build_envelope()
    r = await _post_pubsub(_shadow_app, envelope)

    # Response is unaffected — handle_push's result returned.
    assert r.status_code == 200, r.text
    assert _handle_push_returns_tenant.await_count == 1

    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.failure.s3"] == 1
    assert metrics["shadow_write.success"] == 0


# ---------------------------------------------------------------------
# 3. handle_push returns no tenant_id (e.g. "unknown_subscription")
# — no shadow write attempted.
# ---------------------------------------------------------------------

async def test_pubsub_no_tenant_resolution_skips_shadow(
    _shadow_app, _handle_push_returns_tenant,
):
    _handle_push_returns_tenant.return_value = {
        "status": "skipped",
        "reason": "unknown_subscription",
    }

    envelope = _build_envelope()
    r = await _post_pubsub(_shadow_app, envelope)
    assert r.status_code == 200

    # No shadow side effects.
    assert _shadow_app.state.s3_raw_client.put_if_absent.await_count == 0
    assert _shadow_app.state.kafka_producer.produce.await_count == 0


# ---------------------------------------------------------------------
# 4. Flag-disabled path.
# ---------------------------------------------------------------------

async def test_pubsub_shadow_disabled_by_flag(
    _shadow_app, _handle_push_returns_tenant,
):
    _shadow_app.state.tenant_flags.get_bool = AsyncMock(return_value=False)

    envelope = _build_envelope()
    r = await _post_pubsub(_shadow_app, envelope)
    assert r.status_code == 200

    assert _shadow_app.state.s3_raw_client.put_if_absent.await_count == 0
    assert _shadow_app.state.kafka_producer.produce.await_count == 0


# ---------------------------------------------------------------------
# 5. Shadow body is the raw request bytes (NOT a re-serialised dict).
# Confirms the work order's "content_hash" contract — two byte-identical
# Pub/Sub pushes hash to the same S3 key.
# ---------------------------------------------------------------------

async def test_pubsub_shadow_body_is_raw_request_bytes(
    _shadow_app, _handle_push_returns_tenant,
):
    envelope = _build_envelope()
    await _post_pubsub(_shadow_app, envelope)

    s3 = _shadow_app.state.s3_raw_client
    args, _ = s3.put_if_absent.await_args
    # put_if_absent(key, body) — body is positional arg 1.
    assert len(args) >= 2
    shadow_body = args[1]
    # The shadow body must be the bytes httpx sent (== json.dumps(envelope).encode).
    expected = json.dumps(envelope).encode("utf-8")
    assert shadow_body == expected, (
        "shadow body must be byte-identical to the request body so "
        "content_hash dedup at S3 works across Pub/Sub retransmissions"
    )
