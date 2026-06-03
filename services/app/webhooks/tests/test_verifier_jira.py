"""Jira HMAC verifier tests (IN-17). Mirrors the GitHub verifier suite."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.app.webhooks.signatures.jira import verifier
from services.app.webhooks.verifier import Secret, WebhookVerificationError


_SECRET = "jira-webhook-secret"


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


@pytest.mark.asyncio
async def test_happy_path():
    body = b'{"webhookEvent":"jira:issue_updated","issue":{"id":"10001"}}'
    ctx = await verifier.verify(
        body=body,
        headers={"X-Hub-Signature": _sign(_SECRET, body)},
        secrets=[Secret("jira", _SECRET, label="primary")],
    )
    assert ctx.provider == "jira"
    assert ctx.secret_label == "primary"
    assert ctx.signed_timestamp is None


@pytest.mark.asyncio
async def test_tampered_body():
    body = b'{"webhookEvent":"jira:issue_updated"}'
    sig = _sign(_SECRET, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"webhookEvent":"jira:issue_deleted"}',
            headers={"X-Hub-Signature": sig},
            secrets=[Secret("jira", _SECRET)],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_missing_header():
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}", headers={}, secrets=[Secret("jira", _SECRET)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_malformed_prefix():
    body = b"{}"
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"X-Hub-Signature": "md5=deadbeef"},
            secrets=[Secret("jira", _SECRET)],
        )
    assert exc.value.reason == "malformed_signature_header"


@pytest.mark.asyncio
async def test_no_secret_configured():
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}", headers={"X-Hub-Signature": "sha256=x"}, secrets=[],
        )
    assert exc.value.reason == "secret_not_configured"


@pytest.mark.asyncio
async def test_rotation_overlap_second_secret_matches():
    body = b'{"a":1}'
    sig = _sign(_SECRET, body)
    ctx = await verifier.verify(
        body=body,
        headers={"X-Hub-Signature": sig},
        secrets=[
            Secret("jira", "old-secret", label="prev"),
            Secret("jira", _SECRET, label="current"),
        ],
    )
    assert ctx.secret_label == "current"
