"""Tests for services/ingest/ingestion/handlers/carta.py (cap-table)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.carta import (
    carta_entity,
    carta_version,
    handle_carta_object,
)


pytestmark = pytest.mark.asyncio


_FIRM = "f6e1d4a0-0000-4000-8000-000000000001"  # the Carta issuer id


def _grant_entity(**over):
    """A v1alpha1 option grant: camelCase fields + protobuf wrappers."""
    base = {
        "id": "5001", "issuerId": _FIRM, "securityLabel": "OG-5001",
        "stakeholderId": "1", "shareClassId": "1", "stockOptionType": "ISO",
        "quantity": {"value": "1000"},
        "outstandingQuantity": {"value": "1000"},
        "exercisedQuantity": {"value": "0"},
        "exercisePrice": {"currencyCode": {"value": "USD"},
                          "amount": {"value": "0.25"}},
        "issueDate": {"value": "2026-01-05"},
        "lastModifiedDatetime": {"value": "2026-05-20T12:30:00Z"},
    }
    base.update(over)
    return base


def _grant(**over):
    return {"_fyralis_record_type": "optiongrant", "_fyralis_firm_id": _FIRM,
            "entity": _grant_entity(**over)}


async def test_handler_registered():
    assert get_handler("carta:object") is handle_carta_object
    assert CHANNEL_TRUST_MAP["carta:object"] == "authoritative"


async def test_open_grant_is_signal_with_digest_external_id():
    entity = _grant_entity()
    draft = await handle_carta_object(_grant(), {})
    assert draft.source_channel == "carta:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["status"] == "outstanding"
    # external_id versioned by the content digest, discriminated by entity_kind.
    version = carta_version(entity)
    assert draft.external_id == carta_entity(_FIRM, "option_grant", "5001", version)
    assert draft.external_id == f"carta:{_FIRM}:option_grant:5001:{version}"
    assert draft.content["object_type"] == "option_grant"
    # Wrapper decoding: decimals -> plain strings, money -> {amount, currency}.
    assert draft.content["quantity"] == "1000"
    assert draft.content["exercise_price"] == {"amount": "0.25", "currency": "USD"}
    # occurred_at comes from the lastModifiedDatetime wrapper.
    assert draft.occurred_at.isoformat() == "2026-05-20T12:30:00+00:00"
    assert "OG-5001" in draft.content_text


async def test_exercised_grant_is_state_change():
    draft = await handle_carta_object(
        _grant(exercisedQuantity={"value": "1000"}), {},
    )
    assert draft.kind == "state_change"
    assert draft.content["status"] == "exercised"


async def test_canceled_grant_is_state_change():
    draft = await handle_carta_object(
        _grant(canceledDate={"value": "2026-05-01"}), {},
    )
    assert draft.kind == "state_change"
    assert draft.content["status"] == "canceled"


async def test_mutation_produces_distinct_external_id():
    """Mutable-source dedup lesson: ANY field change (new content digest) must
    land distinct — Carta v1alpha1 entities have no SyncToken-style counter."""
    v0 = await handle_carta_object(_grant(), {})
    v1 = await handle_carta_object(
        _grant(exercisedQuantity={"value": "1000"},
               lastModifiedDatetime={"value": "2026-05-21T09:00:00Z"}), {},
    )
    assert v0.external_id != v1.external_id
    # Identical wire payloads dedup to the SAME external_id.
    twin = await handle_carta_object(_grant(), {})
    assert twin.external_id == v0.external_id


async def test_entity_kind_discriminates_same_id():
    """Two DIFFERENT entity kinds sharing the SAME id must NOT collide — the
    entity_kind discriminator is the cap-table-shaped guard."""
    grant = await handle_carta_object(_grant(id="42"), {})
    stakeholder = await handle_carta_object({
        "_fyralis_record_type": "stakeholder", "_fyralis_firm_id": _FIRM,
        "entity": {"id": "42", "issuerId": _FIRM, "fullName": "Founder One",
                   "relationship": "FOUNDER"},
    }, {})
    assert grant.external_id != stakeholder.external_id
    assert grant.external_id.startswith(f"carta:{_FIRM}:option_grant:42:")
    assert stakeholder.external_id.startswith(f"carta:{_FIRM}:stakeholder:42:")


async def test_stakeholder_record_and_former_state_change():
    active = await handle_carta_object({
        "_fyralis_record_type": "stakeholder", "_fyralis_firm_id": _FIRM,
        "entity": {"id": "2001", "issuerId": _FIRM, "fullName": "Jane Doe",
                   "email": "jane@example.com", "employeeId": "EMP-2001",
                   "relationship": "EMPLOYEE", "entityType": "INDIVIDUAL"},
    }, {})
    assert active.kind == "signal"
    assert active.content["object_type"] == "stakeholder"
    assert active.content["status"] == "employee"
    assert active.content["full_name"] == "Jane Doe"
    assert active.source_actor_ref == "carta:stakeholder:2001"

    former = await handle_carta_object({
        "_fyralis_record_type": "stakeholder", "_fyralis_firm_id": _FIRM,
        "entity": {"id": "2002", "issuerId": _FIRM, "fullName": "John Doe",
                   "relationship": "EX_EMPLOYEE"},
    }, {})
    assert former.kind == "state_change"
    assert former.content["status"] == "former"


async def test_shareclass_record():
    draft = await handle_carta_object({
        "_fyralis_record_type": "shareclass", "_fyralis_firm_id": _FIRM,
        "entity": {"id": "3001", "issuerId": _FIRM, "name": "Common",
                   "prefix": "CS", "type": "COMMON",
                   "authorizedShareCount": {"value": "10000000"},
                   "parValue": {"currencyCode": {"value": "USD"},
                                "amount": {"value": "0.0001"}},
                   "seniority": 1, "pariPassu": False},
    }, {})
    assert draft.content["object_type"] == "share_class"
    assert draft.content["authorized_share_count"] == "10000000"
    assert draft.content["par_value"] == {"amount": "0.0001", "currency": "USD"}
    assert draft.kind == "signal"


async def test_convertible_note_conversion_is_state_change():
    note = {"id": "4001", "issuerId": _FIRM, "securityLabel": "CN-4001",
            "stakeholderId": "3",
            "cashPaid": {"currencyCode": {"value": "USD"},
                         "amount": {"value": "250000.00"}},
            "priceCap": {"currencyCode": {"value": "USD"},
                         "amount": {"value": "8000000.00"}},
            "discountPercentage": {"value": "20"},
            "issueDatetime": {"value": "2025-11-01T00:00:00Z"}}
    open_note = await handle_carta_object({
        "_fyralis_record_type": "convertiblenote", "_fyralis_firm_id": _FIRM,
        "entity": dict(note),
    }, {})
    assert open_note.kind == "signal"
    assert open_note.content["status"] == "outstanding"
    assert open_note.content["cash_paid"] == {"amount": "250000.00",
                                              "currency": "USD"}

    converted = await handle_carta_object({
        "_fyralis_record_type": "convertiblenote", "_fyralis_firm_id": _FIRM,
        "entity": {**note, "conversionDatetime": {"value": "2026-06-01T00:00:00Z"}},
    }, {})
    assert converted.kind == "state_change"
    assert converted.content["status"] == "converted"
    # occurred_at prefers the conversion datetime.
    assert converted.occurred_at.isoformat() == "2026-06-01T00:00:00+00:00"


async def test_extras_absent_keys_not_emitted():
    """A bare grant must not bloat content with empty extras."""
    draft = await handle_carta_object(_grant(), {})
    for k in ("cash_paid", "par_value", "full_name", "interest_rate"):
        assert k not in draft.content


async def test_missing_firm_raises():
    from lib.shared.errors import ValidationError
    payload = _grant()
    payload["_fyralis_firm_id"] = ""
    with pytest.raises(ValidationError):
        await handle_carta_object(payload, {})


async def test_unknown_record_type_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_carta_object({
            "_fyralis_record_type": "bogus", "_fyralis_firm_id": _FIRM,
            "entity": {"id": "1"},
        }, {})


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_carta_object({"foo": "bar"}, {})
