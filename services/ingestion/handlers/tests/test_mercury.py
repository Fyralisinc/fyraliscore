"""Tests for services/ingestion/handlers/mercury.py (finance)."""
from __future__ import annotations

import pytest

from services.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingestion.handlers.mercury import handle_mercury_transaction


pytestmark = pytest.mark.asyncio


_ACCT = "acc-checking"


def _txn(**over):
    base = {
        "id": "t-1001",
        "amount": -5000.00,
        "counterpartyName": "Acme Cloud",
        "status": "sent",
        "kind": "externalTransfer",
        "createdAt": "2026-05-20T12:30:00.000Z",
        "postedAt": "2026-05-20T12:30:00.000Z",
        "bankDescription": "Acme Cloud externalTransfer",
    }
    base.update(over)
    return {"_fyralis_record_type": "transaction", "_fyralis_account_id": _ACCT,
            "transaction": base}


async def test_handler_registered():
    assert get_handler("mercury:transaction") is handle_mercury_transaction
    assert CHANNEL_TRUST_MAP["mercury:transaction"] == "authoritative"


async def test_transaction_is_signal_with_versioned_external_id():
    draft = await handle_mercury_transaction(_txn(), {})
    assert draft.source_channel == "mercury:transaction"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id versioned by status so a status change lands as a new obs.
    assert draft.external_id == f"mercury:{_ACCT}:txn:t-1001:sent"
    assert draft.content["object_type"] == "transaction"
    assert draft.content["direction"] == "outflow"
    assert "Acme Cloud" in draft.content_text


async def test_failed_transaction_is_state_change():
    draft = await handle_mercury_transaction({
        "_fyralis_record_type": "transaction", "_fyralis_account_id": _ACCT,
        "transaction": {"id": "t-1001", "amount": -5000.0,
                        "counterpartyName": "Acme Cloud", "status": "failed",
                        "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"mercury:{_ACCT}:txn:t-1001:failed"


async def test_status_change_produces_distinct_external_id():
    """Mutable-source dedup lesson: a status change must NOT collapse onto the
    earlier observation."""
    sent = await handle_mercury_transaction(_txn(), {})
    failed = await handle_mercury_transaction({
        "_fyralis_record_type": "transaction", "_fyralis_account_id": _ACCT,
        "transaction": {"id": "t-1001", "amount": -5000.0,
                        "counterpartyName": "Acme Cloud", "status": "failed",
                        "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert sent.external_id != failed.external_id


async def test_inflow_direction():
    draft = await handle_mercury_transaction({
        "_fyralis_record_type": "transaction", "_fyralis_account_id": _ACCT,
        "transaction": {"id": "t-2", "amount": 120000.0,
                        "counterpartyName": "Stripe", "status": "sent",
                        "createdAt": "2026-05-20T00:00:00.000Z"},
    }, {})
    assert draft.content["direction"] == "inflow"
    assert "inflow" in draft.content_text


async def test_account_snapshot_is_signal_with_balance():
    draft = await handle_mercury_transaction({
        "_fyralis_record_type": "account_snapshot",
        "_fyralis_account_id": _ACCT,
        "as_of": "2026-05-31T00:00:00.000Z",
        "account": {"id": _ACCT, "name": "Operating Checking", "type": "checking",
                    "availableBalance": 482350.12, "currentBalance": 491200.00},
    }, {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "account_snapshot"
    assert draft.external_id == f"mercury:{_ACCT}:balance:2026-05-31"
    assert draft.content["available_balance"] == 482350.12
    assert "482,350.12" in draft.content_text


# --- live webhook path -----------------------------------------------------

async def test_webhook_transaction_created():
    payload = {
        "type": "transaction.created",
        "organizationId": "org-1",
        "_fyralis_account_id": _ACCT,
        "transaction": {"id": "t-1001", "amount": -5000.0,
                        "counterpartyName": "Acme Cloud", "status": "sent",
                        "createdAt": "2026-05-20T12:30:00.000Z"},
    }
    draft = await handle_mercury_transaction(payload, {})
    assert draft.content["object_type"] == "transaction"
    # external_id parity with the backfilled transaction record.
    assert draft.external_id == f"mercury:{_ACCT}:txn:t-1001:sent"


async def test_backfill_and_webhook_dedup_to_same_external_id():
    backfill = await handle_mercury_transaction(_txn(), {})
    webhook = await handle_mercury_transaction({
        "type": "transaction.created", "organizationId": "org-1",
        "_fyralis_account_id": _ACCT,
        "transaction": {"id": "t-1001", "amount": -5000.0,
                        "counterpartyName": "Acme Cloud", "status": "sent",
                        "createdAt": "2026-05-20T12:30:00.000Z"},
    }, {})
    assert backfill.external_id == webhook.external_id


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_mercury_transaction({"foo": "bar"}, {})
