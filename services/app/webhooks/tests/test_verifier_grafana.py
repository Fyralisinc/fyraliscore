"""Grafana Alerting HMAC verifier tests (IN-GRAFANA). Mirrors the Mercury suite,
but the signature header is bare hex (no `sha256=` prefix) under
`X-Grafana-Alerting-Signature`."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.app.webhooks.signatures.grafana import verifier
from services.app.webhooks.verifier import Secret, WebhookVerificationError


_SECRET = "grafana-webhook-secret"
_HEADER = "X-Grafana-Alerting-Signature"


def _sign(secret: str, body: bytes) -> str:
    # Grafana sends the bare lowercase hex digest (no prefix).
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


@pytest.mark.asyncio
async def test_happy_path():
    body = b'{"status":"firing","alerts":[{"fingerprint":"fp1"}]}'
    ctx = await verifier.verify(
        body=body,
        headers={_HEADER: _sign(_SECRET, body)},
        secrets=[Secret("grafana", _SECRET, label="primary")],
    )
    assert ctx.provider == "grafana"
    assert ctx.secret_label == "primary"
    assert ctx.signed_timestamp is None


@pytest.mark.asyncio
async def test_uppercase_hex_accepted():
    body = b'{"status":"resolved"}'
    sig = _sign(_SECRET, body).upper()  # some clients send uppercase hex
    ctx = await verifier.verify(
        body=body,
        headers={_HEADER: sig},
        secrets=[Secret("grafana", _SECRET)],
    )
    assert ctx.provider == "grafana"


@pytest.mark.asyncio
async def test_tampered_body():
    body = b'{"status":"firing"}'
    sig = _sign(_SECRET, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"status":"resolved"}',
            headers={_HEADER: sig},
            secrets=[Secret("grafana", _SECRET)],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_wrong_secret_rejected():
    body = b"{}"
    with pytest.raises(WebhookVerificationError):
        await verifier.verify(
            body=body,
            headers={_HEADER: _sign("other-secret", body)},
            secrets=[Secret("grafana", _SECRET)],
        )


@pytest.mark.asyncio
async def test_missing_header():
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}", headers={}, secrets=[Secret("grafana", _SECRET)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_timestamp_mode(monkeypatch):
    # When a timestamp header is configured, Grafana signs `ts + ":" + body`.
    monkeypatch.setenv("GRAFANA_WEBHOOK_TIMESTAMP_HEADER", "X-Grafana-Alerting-Timestamp")
    body = b'{"status":"firing"}'
    ts = "1717322400"
    signed = hmac.new(_SECRET.encode(), f"{ts}:".encode() + body, hashlib.sha256).hexdigest()
    ctx = await verifier.verify(
        body=body,
        headers={_HEADER: signed, "X-Grafana-Alerting-Timestamp": ts},
        secrets=[Secret("grafana", _SECRET)],
    )
    assert ctx.signed_timestamp == ts
