"""Stripe verifier tests. Spec: US3 / FR-003..FR-008 / SC-002, SC-003."""
from __future__ import annotations

import pytest

from services.webhooks.signatures.stripe import verifier
from services.webhooks.tests.conftest import stripe_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_happy_path(stripe_secret: str, now: float) -> None:
    body = b'{"id":"evt_1","type":"invoice.paid","created":1700000000,"data":{"object":{"id":"in_1"}}}'
    ts = int(now)
    header = stripe_sign(stripe_secret, body, ts)
    ctx = await verifier.verify(
        body=body,
        headers={"Stripe-Signature": header},
        secrets=[Secret("stripe", stripe_secret, label="active")],
        now=now,
    )
    assert ctx.provider == "stripe"
    assert ctx.signed_timestamp == ts


@pytest.mark.asyncio
async def test_tampered_body(stripe_secret: str, now: float) -> None:
    body = b'{"id":"evt_1","type":"invoice.paid","data":{}}'
    ts = int(now)
    header = stripe_sign(stripe_secret, body, ts)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"id":"evt_X","type":"invoice.paid","data":{}}',
            headers={"Stripe-Signature": header},
            secrets=[Secret("stripe", stripe_secret)],
            now=now,
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_replay_window(stripe_secret: str, now: float) -> None:
    body = b'{"id":"evt_1","type":"x","data":{}}'
    ts = int(now) - 1000  # way out of window
    header = stripe_sign(stripe_secret, body, ts)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"Stripe-Signature": header},
            secrets=[Secret("stripe", stripe_secret)],
            now=now,
        )
    assert exc.value.reason == "expired_timestamp"


@pytest.mark.asyncio
async def test_malformed_header(stripe_secret: str) -> None:
    body = b"{}"
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"Stripe-Signature": "not-a-stripe-header"},
            secrets=[Secret("stripe", stripe_secret)],
        )
    assert exc.value.reason == "malformed_signature_header"


@pytest.mark.asyncio
async def test_no_v1_value(stripe_secret: str, now: float) -> None:
    body = b"{}"
    ts = int(now)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"Stripe-Signature": f"t={ts},v0=00"},  # v0 not v1
            secrets=[Secret("stripe", stripe_secret)],
            now=now,
        )
    assert exc.value.reason == "malformed_signature_header"


@pytest.mark.asyncio
async def test_missing_header(stripe_secret: str) -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={},
            secrets=[Secret("stripe", stripe_secret)],
        )
    assert exc.value.reason == "missing_signature_header"
