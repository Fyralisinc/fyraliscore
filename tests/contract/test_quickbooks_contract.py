"""Contract test: the QuickBooks webhook path fans a REAL multi-realm,
multi-entity Intuit delivery out into one ingest per (realmId, entity).

Guards the Phase-2 architectural fix (finding #7, R1): a single Intuit POST
batches multiple `eventNotifications[]` — EACH with its own `realmId` (a
connected company = a distinct Fyralis tenant) — and each notification's
`dataChangeEvent.entities[]` lists multiple changed entities. The pre-R1
ingress did one resolve→one ingest, dropping every realm past the first and
every entity past the first. The router now fans out: `_qbo_fanout_units`
splits the delivery, and each unit is re-resolved to ITS realm's tenant.

Verified against developer.intuit.com webhooks docs. `intuit-signature` is
base64(HMAC-SHA256(raw_body)) keyed by the APP-level verifier token (shared
across realms), so the single up-front verification covers the whole batch.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from services.app.webhooks.signatures.quickbooks import verifier as qbo_verifier
from services.app.webhooks.tenant_resolver import _extract_quickbooks
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.app.webhooks.router import _qbo_fanout_units
from services.ingest.ingestion.handlers.quickbooks import handle_quickbooks_object
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_TOKEN = "test-intuit-verifier-token"

_REALM_A = "1111111111111111111"
_REALM_B = "2222222222222222222"


def _fixture():
    return load_fixture("quickbooks", "webhook", "entity_change_multi")


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign_b64(raw: bytes) -> str:
    return base64.b64encode(
        hmac.new(_TOKEN.encode("utf-8"), raw, hashlib.sha256).digest()
    ).decode("ascii")


def test_qbo_fanout_splits_into_per_realm_per_entity_units():
    body = _fixture().body
    units = _qbo_fanout_units(body)
    # realm A has 2 entities, realm B has 1 → 3 units total.
    assert len(units) == 3
    realms = [realm for realm, _ in units]
    assert realms.count(_REALM_A) == 2
    assert realms.count(_REALM_B) == 1

    # Each unit is a FLAT single-entity payload the handler's flat branch
    # accepts, with its realm threaded through.
    by_id = {u["id"]: (realm, u) for realm, u in units}
    assert by_id["146"][0] == _REALM_A
    assert by_id["146"][1]["name"] == "Invoice"
    assert by_id["207"][0] == _REALM_A
    assert by_id["55"][0] == _REALM_B
    for _realm, unit in units:
        assert unit["realmId"] == _realm
        assert "name" in unit and "id" in unit
        # No record-type tag → routes to the handler's webhook (flat) branch,
        # not the backfill branch.
        assert "_fyralis_record_type" not in unit


def test_qbo_each_realm_resolves_independently():
    body = _fixture().body
    units = _qbo_fanout_units(body)
    # The fan-out re-resolves each realm via a minimal {realmId}; the resolver
    # extractor must read a top-level realmId so per-realm resolution works.
    for realm, _unit in units:
        assert _extract_quickbooks({"realmId": realm}, {}) == realm
    # And distinct realms map to distinct tenant keys (A != B).
    assert _extract_quickbooks({"realmId": _REALM_A}, {}) != _extract_quickbooks(
        {"realmId": _REALM_B}, {}
    )


async def test_qbo_signature_verifies_whole_batch():
    body = _fixture().body
    raw = _raw(body)
    ctx = await qbo_verifier.verify(
        body=raw,
        headers={"intuit-signature": _sign_b64(raw)},
        secrets=[Secret("quickbooks", _TOKEN)],
    )
    assert ctx.provider == "quickbooks"


async def test_qbo_tampered_signature_rejected():
    body = _fixture().body
    raw = _raw(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await qbo_verifier.verify(
            body=raw,
            headers={"intuit-signature": _sign_b64(raw)},
            secrets=[Secret("quickbooks", "wrong-token")],
        )
    assert exc.value.reason == "signature_mismatch"


async def test_qbo_each_unit_handler_parses_to_distinct_observation():
    body = _fixture().body
    units = _qbo_fanout_units(body)
    drafts = [await handle_quickbooks_object(unit, {}) for _realm, unit in units]

    # Every unit produces a thin-change observation on the one QBO channel.
    for draft in drafts:
        assert draft.source_channel == "quickbooks:object"
        assert draft.content.get("thin_change") is True

    # external_ids are realm-namespaced + entity-distinct → zero collision
    # across realms/entities (the dedup invariant the fan-out must preserve).
    external_ids = {d.external_id for d in drafts}
    assert len(external_ids) == 3
    # Spot-check the realm-A invoice unit maps to the documented thin-change key.
    inv = next(d for d in drafts if d.content["entity_id"] == "146")
    assert inv.content["realm_id"] == _REALM_A
    assert inv.external_id == (
        f"qbo:{_REALM_A}:invoice:146:chg:2026-01-15T14:30:00.000-0700"
    )
