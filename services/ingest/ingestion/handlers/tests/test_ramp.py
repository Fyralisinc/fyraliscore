"""Tests for services/ingest/ingestion/handlers/ramp.py (finance).

Pins the REAL Ramp Developer API record shapes (docs.ramp.com): the four
fetcher-tagged streams (transaction / reimbursement / card / user), the dual
money representation (top-level major-unit number vs nested minor-unit
CurrencyAmount object), the state-versioned external_id scheme
(`ramp:{business}:{seg}:{id}:{state}`), and the flat live-webhook event
(root `business_id` + `object.id`, no entity body -> thin change).
"""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.ramp import handle_ramp_transaction


pytestmark = pytest.mark.asyncio


_BUSINESS = "5fdf3df7-8a8e-4db5-9f55-c33a86bd2c07"
_TXN_ID = "7afc9351-d932-4d2c-a02e-2d4677ba5ee7"
_REIMB_ID = "55b3b1ab-602b-4b13-aafd-be17d075b9be"
_CARD_ID = "f9bd6422-3da9-4509-b04a-58dbecabc6f5"
_USER_ID = "0e1a8b07-9d33-43ff-bd49-f4ed99b71fae"


def _tagged(record_type: str, entity: dict) -> dict:
    return {
        "_fyralis_record_type": record_type,
        "_fyralis_business_id": _BUSINESS,
        "entity": entity,
    }


def _transaction(**over) -> dict:
    base = {
        "id": _TXN_ID,
        "state": "CLEARED",
        "amount": 1234.56,                       # major units (dollars)
        "currency_code": "USD",
        "original_transaction_amount": {         # minor units (cents)
            "amount": 123456,
            "currency_code": "USD",
            "minor_unit_conversion_rate": 100,
        },
        "user_transaction_time": "2026-05-20T12:30:00Z",
        "settlement_date": "2026-05-21T00:00:00Z",
        "merchant_name": "Amazon Web Services",
        "merchant_id": "a2c8dc48-1d33-4d8a-a0d7-d6f12c0f08af",
        "sk_category_name": "Cloud Computing",
        "card_holder": {
            "first_name": "Ada",
            "last_name": "Lovelace",
            "department_name": "Engineering",
            "user_id": _USER_ID,
        },
        "card_id": _CARD_ID,
        "disputes": [],
        "sync_status": "SYNC_READY",
    }
    base.update(over)
    return base


def _reimbursement(**over) -> dict:
    base = {
        "id": _REIMB_ID,
        "state": "PENDING",
        "amount": 88.5,                          # major units (payor pays)
        "currency": "USD",
        "payee_amount": {                        # minor units (cents)
            "amount": 8850,
            "currency_code": "USD",
            "minor_unit_conversion_rate": 100,
        },
        "created_at": "2026-05-18T09:00:00Z",
        "updated_at": "2026-05-20T10:00:00Z",
        "user_full_name": "Ada Lovelace",
        "user_id": _USER_ID,
        "merchant": "Conference Catering Co",
        "type": "OUT_OF_POCKET",
        "direction": "BUSINESS_TO_USER",
    }
    base.update(over)
    return base


async def test_handler_registered():
    assert get_handler("ramp:transaction") is handle_ramp_transaction
    assert CHANNEL_TRUST_MAP["ramp:transaction"] == "authoritative"


# --- transactions ------------------------------------------------------------

async def test_cleared_transaction_is_signal_with_state_versioned_external_id():
    draft = await handle_ramp_transaction(_tagged("transaction", _transaction()), {})
    assert draft.source_channel == "ramp:transaction"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "transaction"
    assert draft.content["status"] == "cleared"
    # external_id versioned by state (§4): ramp:{business}:txn:{id}:{state}.
    assert draft.external_id == f"ramp:{_BUSINESS}:txn:{_TXN_ID}:cleared"
    # top-level `amount` is already major units (dollars).
    assert draft.content["amount"] == 1234.56
    assert draft.content["currency"] == "USD"
    assert "Amazon Web Services" in draft.content_text
    assert draft.source_actor_ref == f"ramp:user:{_USER_ID}"
    assert draft.occurred_at.isoformat().startswith("2026-05-20T12:30")


async def test_declined_transaction_is_state_change():
    draft = await handle_ramp_transaction(
        _tagged("transaction", _transaction(state="DECLINED")), {},
    )
    assert draft.kind == "state_change"
    assert draft.content["status"] == "declined"
    assert draft.external_id == f"ramp:{_BUSINESS}:txn:{_TXN_ID}:declined"


async def test_disputed_transaction_is_state_change():
    draft = await handle_ramp_transaction(
        _tagged("transaction", _transaction(
            disputes=[{"id": "d-1", "type": "MERCHANT_ERROR"}],
        )), {},
    )
    assert draft.kind == "state_change"
    assert draft.content["status"] == "disputed"
    assert draft.content["disputed"] is True


async def test_state_flip_produces_distinct_external_id():
    """Mutable-source dedup lesson: a state change must land as a NEW
    observation (the repo dedups on (channel, external_id))."""
    cleared = await handle_ramp_transaction(
        _tagged("transaction", _transaction()), {},
    )
    declined = await handle_ramp_transaction(
        _tagged("transaction", _transaction(state="DECLINED")), {},
    )
    assert cleared.external_id != declined.external_id


async def test_minor_unit_money_fallback():
    """When the major-unit `amount` is absent, the nested CurrencyAmount object
    (integer cents + minor_unit_conversion_rate) must decode to dollars."""
    entity = _transaction()
    del entity["amount"]
    del entity["currency_code"]
    draft = await handle_ramp_transaction(_tagged("transaction", entity), {})
    assert draft.content["amount"] == 1234.56
    assert draft.content["currency"] == "USD"


async def test_original_amount_emitted_only_for_foreign_currency():
    same = await handle_ramp_transaction(_tagged("transaction", _transaction()), {})
    assert "original_amount" not in same.content

    fx = _transaction(original_transaction_amount={
        "amount": 105000,
        "currency_code": "EUR",
        "minor_unit_conversion_rate": 100,
    })
    foreign = await handle_ramp_transaction(_tagged("transaction", fx), {})
    assert foreign.content["original_amount"] == {
        "amount": 1050.0, "currency": "EUR",
    }


async def test_decline_details_captured():
    draft = await handle_ramp_transaction(
        _tagged("transaction", _transaction(
            state="DECLINED",
            decline_details={"reason": "POLICY_VIOLATION"},
        )), {},
    )
    assert draft.content["decline_reason"] == "POLICY_VIOLATION"


# --- reimbursements ----------------------------------------------------------

async def test_pending_reimbursement_is_signal():
    draft = await handle_ramp_transaction(
        _tagged("reimbursement", _reimbursement()), {},
    )
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "reimbursement"
    assert draft.external_id == f"ramp:{_BUSINESS}:reimb:{_REIMB_ID}:pending"
    assert draft.content["amount"] == 88.5
    assert "Ada Lovelace" in draft.content_text
    # occurred_at prefers updated_at (the incremental high-water field).
    assert draft.occurred_at.isoformat().startswith("2026-05-20T10:00")


async def test_reimbursed_terminal_state_is_state_change():
    draft = await handle_ramp_transaction(
        _tagged("reimbursement", _reimbursement(state="REIMBURSED")), {},
    )
    assert draft.kind == "state_change"
    assert draft.external_id == f"ramp:{_BUSINESS}:reimb:{_REIMB_ID}:reimbursed"


# --- cards -------------------------------------------------------------------

async def test_active_card_is_signal():
    draft = await handle_ramp_transaction(_tagged("card", {
        "id": _CARD_ID,
        "state": "ACTIVE",
        "display_name": "AWS Infra",
        "last_four": "4242",
        "cardholder_id": _USER_ID,
        "cardholder_name": "Ada Lovelace",
        "is_physical": False,
        "created_at": "2026-04-01T00:00:00Z",
    }), {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "card"
    assert draft.external_id == f"ramp:{_BUSINESS}:card:{_CARD_ID}:active"
    assert "4242" in draft.content_text


async def test_suspended_card_is_state_change():
    draft = await handle_ramp_transaction(_tagged("card", {
        "id": _CARD_ID, "state": "SUSPENDED",
        "display_name": "AWS Infra", "created_at": "2026-04-01T00:00:00Z",
    }), {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"ramp:{_BUSINESS}:card:{_CARD_ID}:suspended"


# --- users -------------------------------------------------------------------

async def test_active_user_is_signal():
    draft = await handle_ramp_transaction(_tagged("user", {
        "id": _USER_ID,
        "status": "USER_ACTIVE",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "role": "BUSINESS_ADMIN",
    }), {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "user"
    assert draft.external_id == f"ramp:{_BUSINESS}:user:{_USER_ID}:user_active"
    assert draft.source_actor_ref == f"ramp:user:{_USER_ID}"


async def test_inactive_user_is_state_change():
    draft = await handle_ramp_transaction(_tagged("user", {
        "id": _USER_ID, "status": "USER_INACTIVE",
        "first_name": "Ada", "last_name": "Lovelace",
    }), {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"ramp:{_BUSINESS}:user:{_USER_ID}:user_inactive"


# --- live webhook path (real flat Ramp event) --------------------------------

async def test_webhook_flat_event_is_thin_change():
    payload = {
        "id": "evt-2c61a3e7-0b6e-4f43-9e26-0a9a3f6b3a11",
        "type": "transactions.cleared",
        "created_at": "2026-05-31T08:00:00Z",
        "business_id": _BUSINESS,
        "object": {"id": _TXN_ID},
    }
    draft = await handle_ramp_transaction(payload, {})
    assert draft.content["object_type"] == "transaction"
    assert draft.content["thin_change"] is True
    assert draft.content["operation"] == "cleared"
    # Versioned by the STABLE event id (constant across retries).
    assert draft.external_id == (
        f"ramp:{_BUSINESS}:txn:{_TXN_ID}:chg:"
        "evt-2c61a3e7-0b6e-4f43-9e26-0a9a3f6b3a11"
    )
    assert draft.occurred_at.isoformat().startswith("2026-05-31T08:00")


async def test_webhook_retry_dedups_on_event_id():
    payload = {
        "id": "evt-stable", "type": "transactions.declined",
        "created_at": "2026-05-31T08:00:00Z",
        "business_id": _BUSINESS, "object": {"id": _TXN_ID},
    }
    first = await handle_ramp_transaction(dict(payload), {})
    retry = await handle_ramp_transaction(dict(payload), {})
    assert first.external_id == retry.external_id


# --- guards ------------------------------------------------------------------

async def test_extras_absent_keys_not_emitted():
    """A minimal transaction must not bloat content with absent keys."""
    draft = await handle_ramp_transaction(_tagged("transaction", {
        "id": _TXN_ID, "state": "CLEARED", "amount": 10.0,
        "currency_code": "USD",
        "user_transaction_time": "2026-05-20T12:30:00Z",
    }), {})
    for k in ("merchant", "memo", "disputed", "decline_reason",
              "original_amount", "line_item_count"):
        assert k not in draft.content


async def test_unsupported_record_type_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_ramp_transaction(
            _tagged("invoice", {"id": "x"}), {},
        )


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_ramp_transaction({"foo": "bar"}, {})
