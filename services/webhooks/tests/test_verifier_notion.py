"""Notion verifier tests (IN-14 webhooks).

Notion signs the raw body with HMAC-SHA256 keyed by the App-level
`verification_token`, header `X-Notion-Signature: sha256=<hex>`. The
token is App-level (one subscription per integration), the same shape as
the GitHub App webhook secret — so rotation carries current + previous.
"""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.webhooks.signatures.notion import verifier
from services.webhooks.verifier import Secret, WebhookVerificationError


_TOKEN = "secret_notion_verification_token_abc123"


def _sign(token: str, body: bytes) -> str:
    digest = hmac.new(token.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest.mark.asyncio
async def test_happy_path() -> None:
    body = b'{"type":"page.content_updated","entity":{"id":"p1","type":"page"}}'
    ctx = await verifier.verify(
        body=body,
        headers={"X-Notion-Signature": _sign(_TOKEN, body)},
        secrets=[Secret("notion", _TOKEN, label="app:current")],
    )
    assert ctx.provider == "notion"
    assert ctx.secret_label == "app:current"
    assert ctx.signed_timestamp is None  # Notion HMACs the body alone.


@pytest.mark.asyncio
async def test_bare_hex_accepted() -> None:
    """Forward-compat: accept the bare hex without the `sha256=` prefix."""
    body = b'{"x":1}'
    bare = hmac.new(_TOKEN.encode(), body, hashlib.sha256).hexdigest()
    ctx = await verifier.verify(
        body=body,
        headers={"X-Notion-Signature": bare},
        secrets=[Secret("notion", _TOKEN)],
    )
    assert ctx.provider == "notion"


@pytest.mark.asyncio
async def test_tampered_body() -> None:
    body = b'{"entity":{"id":"p1"}}'
    sig = _sign(_TOKEN, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"entity":{"id":"EVIL"}}',
            headers={"X-Notion-Signature": sig},
            secrets=[Secret("notion", _TOKEN)],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_rotation_previous_token_matches() -> None:
    """A body signed with the PREVIOUS token verifies during the overlap
    window when both tokens are active."""
    body = b'{"type":"page.created"}'
    prev = "secret_notion_previous_token"
    sig = _sign(prev, body)
    ctx = await verifier.verify(
        body=body,
        headers={"X-Notion-Signature": sig},
        secrets=[
            Secret("notion", _TOKEN, label="app:current"),
            Secret("notion", prev, label="app:previous"),
        ],
    )
    assert ctx.secret_label == "app:previous"


@pytest.mark.asyncio
async def test_missing_header() -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={},
            secrets=[Secret("notion", _TOKEN)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_no_secret_configured() -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={"X-Notion-Signature": "sha256=00"},
            secrets=[],
        )
    assert exc.value.reason == "secret_not_configured"
