"""Contract test: the Ramp webhook path parses a REAL Ramp flat event.

Guards the Phase-2 drift fix (finding #35): real Ramp (api.ramp.com) deliveries
are flat events with a ROOT-level `business_id` (the tenant) — NOT the
QuickBooks-clone `eventNotifications[0].business_id` the code assumed (that path
always missed, so live Ramp tenant resolution failed). Verified against
docs.ramp.com.

NOTE: X-Ramp-Signature is HMAC-SHA256 over the raw body, but the hex-vs-base64
encoding is undocumented upstream; this test signs base64 to match the current
verifier — flagged as the one remaining unknown needing a real delivery.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from services.app.webhooks.signatures.ramp import verifier as ramp_verifier
from services.app.webhooks.tenant_resolver import _extract_ramp
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.ingest.ingestion.handlers.ramp import handle_ramp_transaction
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_TOKEN = "test-ramp-webhook-secret"


def _fixture():
    return load_fixture("ramp", "webhook", "transaction_event")


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign_b64(raw: bytes) -> str:
    return base64.b64encode(
        hmac.new(_TOKEN.encode("utf-8"), raw, hashlib.sha256).digest()
    ).decode("ascii")


def _sign_hex(raw: bytes) -> str:
    return hmac.new(_TOKEN.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def test_ramp_tenant_resolution_reads_root_business_id():
    body = _fixture().body
    # Real Ramp: business_id is a root field, not under eventNotifications.
    assert "eventNotifications" not in body
    assert _extract_ramp(body, {}) == body["business_id"]


@pytest.mark.parametrize("encode", [
    _sign_b64,                       # base64 digest
    _sign_hex,                       # hex digest
    lambda raw: f"sha256={_sign_hex(raw)}",   # sha256=hex prefixed
    lambda raw: f"sha256={_sign_b64(raw)}",   # sha256=base64 prefixed
])
async def test_ramp_signature_verifies_dual_encoding(encode):
    """Phase 3B: X-Ramp-Signature encoding (hex vs base64) is undocumented, so
    the verifier accepts EITHER — and an optional sha256= prefix — against the
    HMAC-SHA256 spec. Every shape below validates."""
    body = _fixture().body
    raw = _raw(body)
    ctx = await ramp_verifier.verify(
        body=raw,
        headers={"x-ramp-signature": encode(raw)},
        secrets=[Secret("ramp", _TOKEN)],
    )
    assert ctx.provider == "ramp"


async def test_ramp_tampered_signature_rejected():
    body = _fixture().body
    raw = _raw(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await ramp_verifier.verify(
            body=raw,
            headers={"x-ramp-signature": _sign_b64(raw)},
            secrets=[Secret("ramp", "wrong-secret")],
        )
    assert exc.value.reason == "signature_mismatch"


async def test_ramp_handler_parses_flat_event():
    body = _fixture().body
    draft = await handle_ramp_transaction(body, {})
    assert draft.source_channel == "ramp:transaction"
    assert draft.content["business_id"] == body["business_id"]
    assert draft.content["entity_id"] == body["object"]["id"]
    assert draft.content.get("thin_change") is True
    # versioned by the stable event id (constant across retries)
    assert draft.external_id == (
        f"ramp:{body['business_id']}:txn:{body['object']['id']}:chg:{body['id']}"
    )
