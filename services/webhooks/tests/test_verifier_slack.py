"""Slack verifier — happy + spoof + replay-window + missing-header tests.

Spec coverage: US1, US2, FR-003..FR-008, FR-012, FR-015, SC-002, SC-003.
"""
from __future__ import annotations

import pytest

from services.webhooks.signatures.slack import verifier
from services.webhooks.tests.conftest import slack_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_happy_path(slack_secret: str, now: float) -> None:
    body = b'{"team_id":"T0001","event":{"type":"message","text":"hi","ts":"1.2","channel":"C","user":"U"}}'
    ts = int(now)
    sig = slack_sign(slack_secret, body, ts)
    ctx = await verifier.verify(
        body=body,
        headers={
            "X-Slack-Request-Timestamp": str(ts),
            "X-Slack-Signature": sig,
        },
        secrets=[Secret("slack", slack_secret, label="active")],
        now=now,
    )
    assert ctx.provider == "slack"
    assert ctx.body == body
    assert ctx.secret_label == "active"
    assert ctx.signed_timestamp == ts


@pytest.mark.asyncio
async def test_tampered_body_rejected(slack_secret: str, now: float) -> None:
    body = b'{"team_id":"T0001","event":{"text":"hi","ts":"1","channel":"C","user":"U"}}'
    ts = int(now)
    sig = slack_sign(slack_secret, body, ts)
    tampered = body[:-1] + b" "  # change last byte
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=tampered,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
            secrets=[Secret("slack", slack_secret)],
            now=now,
        )
    assert exc.value.reason == "signature_mismatch"
    assert exc.value.provider == "slack"


@pytest.mark.asyncio
async def test_replay_window(slack_secret: str, now: float) -> None:
    body = b'{"team_id":"T","event":{"text":"hi","ts":"1","channel":"C","user":"U"}}'
    ts = int(now) - 400  # 400s old, > 300s window
    sig = slack_sign(slack_secret, body, ts)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
            secrets=[Secret("slack", slack_secret)],
            now=now,
        )
    assert exc.value.reason == "expired_timestamp"


@pytest.mark.asyncio
async def test_missing_signature_header(slack_secret: str, now: float) -> None:
    body = b'{"team_id":"T","event":{"text":"hi"}}'
    ts = int(now)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"X-Slack-Request-Timestamp": str(ts)},
            secrets=[Secret("slack", slack_secret)],
            now=now,
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_missing_timestamp(slack_secret: str, now: float) -> None:
    body = b'{}'
    sig = slack_sign(slack_secret, body, int(now))
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"X-Slack-Signature": sig},
            secrets=[Secret("slack", slack_secret)],
            now=now,
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_no_secret_configured(now: float) -> None:
    body = b'{}'
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={
                "X-Slack-Request-Timestamp": str(int(now)),
                "X-Slack-Signature": "v0=00",
            },
            secrets=[],
            now=now,
        )
    assert exc.value.reason == "secret_not_configured"


@pytest.mark.asyncio
async def test_byte_literal_verification(slack_secret: str, now: float) -> None:
    """Verification must use the literal request bytes, not a reparsed
    JSON form (FR-012). Whitespace inside the body must matter."""
    body_a = b'{"a": 1}'
    body_b = b'{"a":1}'  # semantically equal, byte-different
    ts = int(now)
    sig_a = slack_sign(slack_secret, body_a, ts)
    # signing body_a but submitting body_b → mismatch
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body_b,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig_a,
            },
            secrets=[Secret("slack", slack_secret)],
            now=now,
        )
    assert exc.value.reason == "signature_mismatch"
