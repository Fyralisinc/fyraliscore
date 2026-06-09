"""Contract test: the production Gusto webhook path parses a REAL Gusto payload.

Drives the real verifier + tenant-resolver + handler against the doc-sourced
fixture (tests/contract/fixtures/gusto/webhook/employee_event.json). Guards the
Phase-2 drift fix: real Gusto deliveries are flat snake_case with the company in
`resource_uuid` (resource_type=="Company"), signed as lowercase-hex HMAC-SHA256
in `X-Gusto-Signature` keyed by the verification_token — NOT the QuickBooks-clone
shape (eventNotifications / companyId / base64 / Gusto-Signature) the code
assumed before. Verified against docs.gusto.com + Gusto/gusto.github.io.
"""
from __future__ import annotations

import hashlib
import hmac
import json

import pytest

from services.app.webhooks.signatures.gusto import verifier as gusto_verifier
from services.app.webhooks.tenant_resolver import _extract_gusto
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.ingest.ingestion.handlers.gusto import handle_gusto_object
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_TOKEN = "test-verification-token"


def _fixture():
    return load_fixture("gusto", "webhook", "employee_event")


def _body_bytes(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign(body_bytes: bytes) -> str:
    return hmac.new(_TOKEN.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()


async def test_real_gusto_signature_verifies_hex_x_header():
    """The verifier accepts a real Gusto lowercase-hex HMAC-SHA256 in the
    X-Gusto-Signature header (not base64, not the legacy Gusto-Signature name)."""
    body = _fixture().body
    raw = _body_bytes(body)
    ctx = await gusto_verifier.verify(
        body=raw,
        headers={"X-Gusto-Signature": _sign(raw)},
        secrets=[Secret("gusto", _TOKEN)],
    )
    assert ctx.provider == "gusto"


async def test_real_gusto_signature_accepts_uppercase_hex():
    body = _fixture().body
    raw = _body_bytes(body)
    ctx = await gusto_verifier.verify(
        body=raw,
        headers={"X-Gusto-Signature": _sign(raw).upper()},  # hex is case-insensitive
        secrets=[Secret("gusto", _TOKEN)],
    )
    assert ctx.provider == "gusto"


async def test_gusto_tampered_signature_rejected():
    body = _fixture().body
    raw = _body_bytes(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await gusto_verifier.verify(
            body=raw,
            headers={"X-Gusto-Signature": _sign(raw)},
            secrets=[Secret("gusto", "wrong-token")],
        )
    assert exc.value.reason == "signature_mismatch"


async def test_gusto_legacy_header_name_now_rejected():
    """The old QBO-clone header name must no longer satisfy the verifier."""
    body = _fixture().body
    raw = _body_bytes(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await gusto_verifier.verify(
            body=raw,
            headers={"Gusto-Signature": _sign(raw)},  # legacy name, no X- prefix
            secrets=[Secret("gusto", _TOKEN)],
        )
    assert exc.value.reason == "missing_signature_header"


def test_gusto_tenant_resolution_reads_resource_uuid():
    """The company is resolved from `resource_uuid` (always the company), even
    for a non-Company event where entity_uuid is the employee."""
    body = _fixture().body
    assert _extract_gusto(body, {}) == body["resource_uuid"]
    assert body["resource_uuid"] != body["entity_uuid"]  # non-Company event


async def test_gusto_handler_parses_flat_thin_notification():
    body = _fixture().body
    draft = await handle_gusto_object(body, {})
    assert draft.source_channel == "gusto:object"
    # company = resource_uuid; entity = entity_type/entity_uuid; versioned by uuid
    assert draft.content["company_uuid"] == body["resource_uuid"]
    assert draft.content["entity_id"] == body["entity_uuid"]
    assert draft.content.get("thin_change") is True
    assert draft.external_id == (
        f"gusto:{body['resource_uuid']}:employee:"
        f"{body['entity_uuid']}:chg:{body['uuid']}"
    )
