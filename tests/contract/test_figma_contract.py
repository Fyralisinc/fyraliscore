"""Contract test: the Figma webhook path parses a REAL Figma Webhooks V2 delivery.

Guards the Phase-2 architectural fix (finding #C, R2): a real Figma V2 delivery
carries the Figma-assigned `webhook_id` (the install scope) and NO `team_id` in
the body, and has NO stable per-event id. The pre-R2 code resolved the tenant by
`team_id` (absent in production → live resolution always failed) and built the
external_id from an event `id` (absent → the handler raised). The fix:
  - resolver reads `webhook_id` (install keyed by it; see figma/onboarding.py),
  - handler namespaces external_id by the webhook_id with (file_key, timestamp)
    as the event discriminator.

Verified against developers.figma.com Webhooks V2. Verification is
passcode-in-body (the receiver constant-time-compares `body.passcode`), NOT an
HMAC header.
"""
from __future__ import annotations

import json

import pytest

from services.app.webhooks.signatures.figma import verifier as figma_verifier
from services.app.webhooks.tenant_resolver import _extract_figma
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.ingest.ingestion.handlers.figma import handle_figma_event
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_PASSCODE = "test-figma-shared-passcode"
_WEBHOOK_ID = "2843917465"


def _fixture():
    return load_fixture("figma", "webhook", "file_update")


def _live_body() -> dict:
    """The fixture body with the placeholder passcode replaced by the active
    secret (so the verifier accepts it)."""
    body = dict(_fixture().body)
    body["passcode"] = _PASSCODE
    return body


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def test_figma_tenant_resolution_reads_webhook_id_not_team_id():
    body = _fixture().body
    # Real Figma V2: webhook_id present, team_id absent.
    assert "team_id" not in body
    assert _extract_figma(body, {}) == _WEBHOOK_ID


async def test_figma_passcode_in_body_verifies():
    body = _live_body()
    ctx = await figma_verifier.verify(
        body=_raw(body),
        headers={},  # no HMAC header — passcode is in the body
        secrets=[Secret("figma", _PASSCODE)],
    )
    assert ctx.provider == "figma"


async def test_figma_wrong_passcode_rejected():
    body = _live_body()
    with pytest.raises(WebhookVerificationError) as exc:
        await figma_verifier.verify(
            body=_raw(body),
            headers={},
            secrets=[Secret("figma", "wrong-passcode")],
        )
    assert exc.value.reason == "passcode_mismatch"


async def test_figma_handler_namespaces_by_webhook_id_and_file_key_timestamp():
    body = _live_body()
    draft = await handle_figma_event(body, {})
    assert draft.source_channel == "figma:event"
    # No event id in a real delivery → discriminate by (file_key, timestamp),
    # namespaced by the webhook_id (the install scope, not team_id).
    assert draft.content["webhook_id"] == _WEBHOOK_ID
    assert draft.content.get("team_id") is None
    assert draft.external_id == (
        f"figma:{_WEBHOOK_ID}:event:{body['file_key']}:{body['timestamp']}"
    )


async def test_figma_distinct_timestamp_is_a_new_observation():
    """A re-version of the same file (new timestamp) must NOT collapse onto the
    earlier observation — the mutable-source dedup lesson, preserved under the
    webhook_id namespace."""
    a = await handle_figma_event(_live_body(), {})
    later = _live_body()
    later["timestamp"] = "2026-01-15T22:59:59Z"
    b = await handle_figma_event(later, {})
    assert a.external_id != b.external_id
    assert a.external_id.startswith(f"figma:{_WEBHOOK_ID}:event:")
