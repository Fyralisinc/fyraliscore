"""Tests for services/ingest/ingestion/handlers/carta.py (cap-table)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.carta import (
    carta_entity,
    handle_carta_object,
)


pytestmark = pytest.mark.asyncio


_FIRM = "firm_9341452000000001"


def _grant(**over):
    base = {
        "Id": "5001", "SyncToken": "0", "DocNumber": "OG-5001",
        "Status": "active", "Quantity": 1000, "StrikePrice": 0.25,
        "StakeholderRef": {"value": "1", "name": "Employee-1"},
        "MetaData": {"LastUpdatedTime": "2026-05-20T12:30:00-08:00"},
    }
    base.update(over)
    return {"_fyralis_record_type": "optiongrant", "_fyralis_firm_id": _FIRM,
            "entity": base}


async def test_handler_registered():
    assert get_handler("carta:object") is handle_carta_object
    assert CHANNEL_TRUST_MAP["carta:object"] == "authoritative"


async def test_open_grant_is_signal_with_synctoken_external_id():
    draft = await handle_carta_object(_grant(), {})
    assert draft.source_channel == "carta:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["status"] == "active"
    # external_id versioned by SyncToken, discriminated by entity_kind.
    assert draft.external_id == carta_entity(_FIRM, "option_grant", "5001", "0")
    assert draft.external_id == f"carta:{_FIRM}:option_grant:5001:0"
    assert draft.content["object_type"] == "option_grant"
    assert "Employee-1" in draft.content_text


async def test_exercised_grant_is_state_change():
    draft = await handle_carta_object(_grant(Status="exercised", SyncToken="1"), {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "exercised"
    assert draft.external_id == f"carta:{_FIRM}:option_grant:5001:1"


async def test_synctoken_bump_produces_distinct_external_id():
    """Mutable-source dedup lesson: an edit (new SyncToken) must land distinct."""
    v0 = await handle_carta_object(_grant(), {})
    v1 = await handle_carta_object(_grant(SyncToken="1"), {})
    assert v0.external_id != v1.external_id


async def test_entity_kind_discriminates_same_id():
    """Two DIFFERENT entity kinds sharing the SAME id + sync_token must NOT
    collide — the entity_kind discriminator is the cap-table-shaped guard."""
    grant = await handle_carta_object(_grant(Id="42", SyncToken="0"), {})
    shareholder = await handle_carta_object({
        "_fyralis_record_type": "shareholder", "_fyralis_firm_id": _FIRM,
        "entity": {"Id": "42", "SyncToken": "0", "Status": "active",
                   "ShareCount": 1000,
                   "StakeholderRef": {"value": "2", "name": "Founder"},
                   "MetaData": {"LastUpdatedTime": "2026-05-20T12:30:00-08:00"}},
    }, {})
    assert grant.external_id != shareholder.external_id
    assert grant.external_id == f"carta:{_FIRM}:option_grant:42:0"
    assert shareholder.external_id == f"carta:{_FIRM}:shareholder:42:0"


async def test_safe_note_record():
    draft = await handle_carta_object({
        "_fyralis_record_type": "safenote", "_fyralis_firm_id": _FIRM,
        "entity": {"Id": "4001", "SyncToken": "0", "Status": "outstanding",
                   "InvestmentAmount": 250000.0, "ValuationCap": 8000000.0,
                   "DiscountRate": 0.2,
                   "StakeholderRef": {"value": "3", "name": "Seed Fund"},
                   "MetaData": {"LastUpdatedTime": "2026-05-10T00:00:00-08:00"}},
    }, {})
    assert draft.content["object_type"] == "safe_note"
    assert draft.external_id == f"carta:{_FIRM}:safe_note:4001:0"
    assert draft.content["investment_amount"] == 250000.0
    assert draft.content["valuation_cap"] == 8000000.0


async def test_shareclass_record():
    draft = await handle_carta_object({
        "_fyralis_record_type": "shareclass", "_fyralis_firm_id": _FIRM,
        "entity": {"Id": "3001", "SyncToken": "0", "Status": "active",
                   "ShareCount": 10_000_000, "PricePerShare": 1.50,
                   "MetaData": {"LastUpdatedTime": "2026-05-10T00:00:00-08:00"}},
    }, {})
    assert draft.content["object_type"] == "share_class"
    assert draft.content["price_per_share"] == 1.50


async def test_extras_absent_keys_not_emitted():
    """A bare grant must not bloat content with empty extras."""
    draft = await handle_carta_object(_grant(), {})
    for k in ("investment_amount", "valuation_cap", "price_per_share"):
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
            "entity": {"Id": "1"},
        }, {})


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_carta_object({"foo": "bar"}, {})
