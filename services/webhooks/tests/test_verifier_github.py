"""GitHub verifier tests. Spec: US3 / FR-003..FR-008 / SC-002."""
from __future__ import annotations

import pytest

from services.webhooks.signatures.github import verifier
from services.webhooks.tests.conftest import github_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_happy_path(github_secret: str) -> None:
    body = b'{"action":"opened","pull_request":{"id":42,"number":7,"title":"x"}}'
    sig = github_sign(github_secret, body)
    ctx = await verifier.verify(
        body=body,
        headers={"X-Hub-Signature-256": sig, "X-GitHub-Event": "pull_request"},
        secrets=[Secret("github", github_secret, label="primary")],
    )
    assert ctx.provider == "github"
    assert ctx.secret_label == "primary"
    assert ctx.signed_timestamp is None  # GitHub doesn't sign a ts


@pytest.mark.asyncio
async def test_tampered_body(github_secret: str) -> None:
    body = b'{"action":"opened"}'
    sig = github_sign(github_secret, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b'{"action":"closed"}',
            headers={"X-Hub-Signature-256": sig},
            secrets=[Secret("github", github_secret)],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_missing_header(github_secret: str) -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={},
            secrets=[Secret("github", github_secret)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_malformed_prefix(github_secret: str) -> None:
    body = b"{}"
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"X-Hub-Signature-256": "md5=00"},  # wrong prefix
            secrets=[Secret("github", github_secret)],
        )
    assert exc.value.reason == "malformed_signature_header"


@pytest.mark.asyncio
async def test_sha1_legacy_header_rejected(github_secret: str) -> None:
    """The legacy SHA-1 X-Hub-Signature header must not be accepted —
    only X-Hub-Signature-256."""
    body = b"{}"
    sig256 = github_sign(github_secret, body)
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={"X-Hub-Signature": sig256},  # legacy header name
            secrets=[Secret("github", github_secret)],
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_no_secret() -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={"X-Hub-Signature-256": "sha256=00"},
            secrets=[],
        )
    assert exc.value.reason == "secret_not_configured"
