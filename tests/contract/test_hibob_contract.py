"""Contract test: HiBob webhook path parses a REAL Bob Webhooks V2 payload.

Confirms Phase-2 finding: #20 ("HiBob uses synthetic body fields absent from
real payloads") is a FALSE POSITIVE for HiBob. Official docs show every Bob V2
delivery carries a top-level `companyId` (a JSON NUMBER) as the tenant key, and
is signed HMAC-SHA512/base64 in `Bob-Signature` — exactly what the code already
does. This test locks the real contract in so it can't silently drift.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from services.app.webhooks.signatures.hibob import verifier as hibob_verifier
from services.app.webhooks.tenant_resolver import _extract_hibob
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_TOKEN = "test-hibob-webhook-secret"


def _fixture():
    return load_fixture("hibob", "webhook", "event")


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign(raw: bytes) -> str:
    return base64.b64encode(
        hmac.new(_TOKEN.encode("utf-8"), raw, hashlib.sha512).digest()
    ).decode("ascii")


def test_hibob_tenant_resolution_reads_numeric_companyid():
    """companyId is a JSON number in real deliveries; the resolver must
    stringify it to the install key (finding #20 was a false positive)."""
    body = _fixture().body
    assert isinstance(body["companyId"], int)  # real payloads send a number
    assert _extract_hibob(body, {}) == str(body["companyId"])


async def test_hibob_signature_verifies_sha512_base64():
    body = _fixture().body
    raw = _raw(body)
    ctx = await hibob_verifier.verify(
        body=raw,
        headers={"Bob-Signature": _sign(raw)},
        secrets=[Secret("hibob", _TOKEN)],
    )
    assert ctx.provider == "hibob"


async def test_hibob_tampered_signature_rejected():
    body = _fixture().body
    raw = _raw(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await hibob_verifier.verify(
            body=raw,
            headers={"Bob-Signature": _sign(raw)},
            secrets=[Secret("hibob", "wrong-secret")],
        )
    assert exc.value.reason == "signature_mismatch"
