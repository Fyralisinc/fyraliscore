"""Linear verifier tests. Spec: US3 / FR-003..FR-008 / SC-002, SC-003."""
from __future__ import annotations

import json

import pytest

from services.webhooks.signatures.linear import verifier
from services.webhooks.tests.conftest import linear_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_happy_path(linear_secret: str, now: float) -> None:
    body = json.dumps({
        "type": "Issue",
        "action": "create",
        "data": {"id": "iss_1", "title": "x", "team": {"id": "t1"}},
        "webhookTimestamp": int(now * 1000),
    }).encode("utf-8")
    sig = linear_sign(linear_secret, body)
    ctx = await verifier.verify(
        body=body,
        headers={"Linear-Signature": sig},
        secrets=[Secret("linear", linear_secret, label="primary")],
        now=now,
    )
    assert ctx.provider == "linear"
    assert ctx.signed_timestamp == int(now)


@pytest.mark.asyncio
async def test_tampered_body(linear_secret: str) -> None:
    body = b'{"type":"Issue","action":"create","data":{}}'
    sig = linear_sign(linear_secret, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"type":"Issue","action":"update","data":{}}',
            headers={"Linear-Signature": sig},
            secrets=[Secret("linear", linear_secret)],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_replay_via_webhook_timestamp(linear_secret: str, now: float) -> None:
    """Linear's webhookTimestamp drives the replay-window check."""
    body = json.dumps({
        "type": "Issue", "action": "create", "data": {},
        "webhookTimestamp": int((now - 400) * 1000),  # 400s old
    }).encode("utf-8")
    sig = linear_sign(linear_secret, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"Linear-Signature": sig},
            secrets=[Secret("linear", linear_secret)],
            now=now,
        )
    assert exc.value.reason == "expired_timestamp"


@pytest.mark.asyncio
async def test_missing_header(linear_secret: str) -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={},
            secrets=[Secret("linear", linear_secret)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_body_without_timestamp_passes(linear_secret: str, now: float) -> None:
    """If the body has no webhookTimestamp, we accept without a window
    check (matches Linear's older deliveries)."""
    body = b'{"type":"Issue","action":"create","data":{}}'
    sig = linear_sign(linear_secret, body)
    ctx = await verifier.verify(
        body=body,
        headers={"Linear-Signature": sig},
        secrets=[Secret("linear", linear_secret)],
        now=now,
    )
    assert ctx.signed_timestamp is None
