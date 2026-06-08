"""Tests for services/ingest/ingestion/handlers/deel.py (finance)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.deel import handle_deel_payment


pytestmark = pytest.mark.asyncio


_CONTRACT = "con-eng-monthly"


def _payment(**over):
    base = {
        "id": "p-1001",
        "amount": -5000.00,
        "counterpartyName": "Acme Cloud",
        "status": "sent",
        "kind": "externalTransfer",
        "createdAt": "2026-05-20T12:30:00.000Z",
        "postedAt": "2026-05-20T12:30:00.000Z",
        "bankDescription": "Acme Cloud externalTransfer",
    }
    base.update(over)
    return {"_fyralis_record_type": "payment", "_fyralis_contract_id": _CONTRACT,
            "payment": base}


async def test_handler_registered():
    assert get_handler("deel:payment") is handle_deel_payment
    assert CHANNEL_TRUST_MAP["deel:payment"] == "authoritative"


async def test_payment_is_signal_with_versioned_external_id():
    draft = await handle_deel_payment(_payment(), {})
    assert draft.source_channel == "deel:payment"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id versioned by status so a status change lands as a new obs.
    assert draft.external_id == f"deel:{_CONTRACT}:payment:p-1001:sent"
    assert draft.content["object_type"] == "payment"
    assert draft.content["direction"] == "outflow"
    assert "Acme Cloud" in draft.content_text


async def test_failed_payment_is_state_change():
    draft = await handle_deel_payment({
        "_fyralis_record_type": "payment", "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-1001", "amount": -5000.0,
                    "counterpartyName": "Acme Cloud", "status": "failed",
                    "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"deel:{_CONTRACT}:payment:p-1001:failed"


async def test_rejected_payment_is_state_change():
    draft = await handle_deel_payment({
        "_fyralis_record_type": "payment", "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-1001", "amount": -5000.0,
                    "counterpartyName": "Acme Cloud", "status": "rejected",
                    "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"deel:{_CONTRACT}:payment:p-1001:rejected"


async def test_status_change_produces_distinct_external_id():
    """Mutable-source dedup lesson: a status change must NOT collapse onto the
    earlier observation."""
    sent = await handle_deel_payment(_payment(), {})
    failed = await handle_deel_payment({
        "_fyralis_record_type": "payment", "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-1001", "amount": -5000.0,
                    "counterpartyName": "Acme Cloud", "status": "failed",
                    "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert sent.external_id != failed.external_id


async def test_inflow_direction():
    draft = await handle_deel_payment({
        "_fyralis_record_type": "payment", "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-2", "amount": 120000.0,
                    "counterpartyName": "Stripe", "status": "sent",
                    "createdAt": "2026-05-20T00:00:00.000Z"},
    }, {})
    assert draft.content["direction"] == "inflow"
    assert "inflow" in draft.content_text


async def test_contract_snapshot_is_signal():
    draft = await handle_deel_payment({
        "_fyralis_record_type": "contract_snapshot",
        "_fyralis_contract_id": _CONTRACT,
        "updated": "2026-05-31T00:00:00.000Z",
        "contract": {"id": _CONTRACT, "name": "Eng Monthly", "type": "ongoing_time_based",
                     "status": "in_progress", "rate": 8500.00},
    }, {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "contract_snapshot"
    assert draft.external_id == f"deel:{_CONTRACT}:contract:2026-05-31T00:00:00.000Z"
    assert draft.content["rate"] == 8500.00
    assert "Eng Monthly" in draft.content_text


# --- live webhook path -----------------------------------------------------

async def test_webhook_payment_created():
    payload = {
        "type": "payment.created",
        "organizationId": "org-1",
        "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-1001", "amount": -5000.0,
                    "counterpartyName": "Acme Cloud", "status": "sent",
                    "createdAt": "2026-05-20T12:30:00.000Z"},
    }
    draft = await handle_deel_payment(payload, {})
    assert draft.content["object_type"] == "payment"
    # external_id parity with the backfilled payment record.
    assert draft.external_id == f"deel:{_CONTRACT}:payment:p-1001:sent"


async def test_backfill_and_webhook_dedup_to_same_external_id():
    backfill = await handle_deel_payment(_payment(), {})
    webhook = await handle_deel_payment({
        "type": "payment.created", "organizationId": "org-1",
        "_fyralis_contract_id": _CONTRACT,
        "payment": {"id": "p-1001", "amount": -5000.0,
                    "counterpartyName": "Acme Cloud", "status": "sent",
                    "createdAt": "2026-05-20T12:30:00.000Z"},
    }, {})
    assert backfill.external_id == webhook.external_id


# --- rich-field ingestion ---------------------------------------------------

async def test_payment_rich_fields_captured():
    draft = await handle_deel_payment(_payment(
        deelCategory="SaaS",
        generalLedgerCodeName="GL-6010",
        externalMemo="Invoice #42",
        counterpartyId="cp-9",
        estimatedDeliveryDate="2026-05-22T00:00:00.000Z",
        currencyExchangeInfo={"convertedFromCurrency": "EUR", "rate": 1.08},
    ), {})
    c = draft.content
    assert c["deel_category"] == "SaaS"
    assert c["general_ledger_code_name"] == "GL-6010"
    assert c["external_memo"] == "Invoice #42"
    assert c["counterparty_id"] == "cp-9"
    assert c["estimated_delivery_date"] == "2026-05-22T00:00:00.000Z"
    assert c["currency_exchange_info"]["rate"] == 1.08


async def test_failure_reason_in_content_and_text():
    draft = await handle_deel_payment(_payment(
        status="failed", reasonForFailure="insufficient funds",
    ), {})
    assert draft.kind == "state_change"
    assert draft.content["reason_for_failure"] == "insufficient funds"
    assert "insufficient funds" in draft.content_text


async def test_details_routing_is_pii_redacted():
    draft = await handle_deel_payment(_payment(details={
        "electronicRoutingInfo": {
            "accountNumber": "000123456789",
            "routingNumber": "021000021",
            "bankName": "Acme Partner Bank",
        },
    }), {})
    routing = draft.content["details"]["electronicRoutingInfo"]
    # last-4 only — full account/routing numbers must NOT land verbatim.
    assert routing["accountNumber"] == "••6789"
    assert routing["routingNumber"] == "••0021"
    assert routing["bankName"] == "Acme Partner Bank"  # non-sensitive kept


async def test_extras_absent_keys_not_emitted():
    """A bare payment must not bloat content with None-valued extras."""
    draft = await handle_deel_payment(_payment(), {})
    assert "reason_for_failure" not in draft.content
    assert "details" not in draft.content
    assert "deel_category" not in draft.content


async def test_snapshot_attribution_fields():
    draft = await handle_deel_payment({
        "_fyralis_record_type": "contract_snapshot",
        "_fyralis_contract_id": _CONTRACT,
        "updated": "2026-05-31T00:00:00.000Z",
        "contract": {"id": _CONTRACT, "name": "Eng Monthly", "type": "ongoing_time_based",
                     "status": "in_progress", "rate": 1.0,
                     "workerName": "Jane Doe", "legalBusinessName": "Fyralis Inc"},
    }, {})
    assert draft.content["worker_name"] == "Jane Doe"
    assert draft.content["legal_business_name"] == "Fyralis Inc"


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_deel_payment({"foo": "bar"}, {})
