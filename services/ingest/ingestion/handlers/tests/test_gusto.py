"""Tests for services/ingest/ingestion/handlers/gusto.py (finance)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.gusto import handle_gusto_object


pytestmark = pytest.mark.asyncio


_COMPANY = "9341452000000001"


def _invoice(**over):
    base = {
        "Id": "1037", "SyncToken": "0", "DocNumber": "1037",
        "TotalAmt": 5000.00, "Balance": 5000.00,
        "CustomerRef": {"value": "1", "name": "Globex"},
        "TxnDate": "2026-05-01",
        "DueDate": "2026-06-30",
        "MetaData": {"LastUpdatedTime": "2026-05-20T12:30:00-08:00"},
    }
    base.update(over)
    return {"_fyralis_record_type": "invoice", "_fyralis_company_uuid": _COMPANY,
            "entity": base}


async def test_handler_registered():
    assert get_handler("gusto:object") is handle_gusto_object
    assert CHANNEL_TRUST_MAP["gusto:object"] == "authoritative"


async def test_open_invoice_is_signal_with_synctoken_external_id():
    draft = await handle_gusto_object(_invoice(), {})
    assert draft.source_channel == "gusto:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["status"] == "open"
    # external_id versioned by SyncToken.
    assert draft.external_id == f"gusto:{_COMPANY}:invoice:1037:0"
    assert draft.content["object_type"] == "invoice"
    assert "Globex" in draft.content_text


async def test_paid_invoice_is_state_change():
    base = _invoice()
    base["entity"]["Balance"] = 0.0
    base["entity"]["SyncToken"] = "1"
    draft = await handle_gusto_object(base, {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "paid"
    assert draft.external_id == f"gusto:{_COMPANY}:invoice:1037:1"


async def test_overdue_invoice_is_state_change():
    past = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    base = _invoice()
    base["entity"]["DueDate"] = past
    base["entity"]["Balance"] = 5000.0
    draft = await handle_gusto_object(base, {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "overdue"


async def test_synctoken_bump_produces_distinct_external_id():
    """Mutable-source dedup lesson: an edit (new SyncToken) must land distinct."""
    v0 = await handle_gusto_object(_invoice(), {})
    bumped = _invoice()
    bumped["entity"]["SyncToken"] = "1"
    v1 = await handle_gusto_object(bumped, {})
    assert v0.external_id != v1.external_id


async def test_bill_record():
    draft = await handle_gusto_object({
        "_fyralis_record_type": "bill", "_fyralis_company_uuid": _COMPANY,
        "entity": {"Id": "204", "SyncToken": "0", "TotalAmt": 3200.0,
                   "Balance": 3200.0, "VendorRef": {"value": "7", "name": "AWS"},
                   "MetaData": {"LastUpdatedTime": "2026-05-10T00:00:00-08:00"}},
    }, {})
    assert draft.content["object_type"] == "bill"
    assert draft.external_id == f"gusto:{_COMPANY}:bill:204:0"
    assert "AWS" in draft.content_text


async def test_payment_record_is_signal():
    draft = await handle_gusto_object({
        "_fyralis_record_type": "payment", "_fyralis_company_uuid": _COMPANY,
        "entity": {"Id": "88", "SyncToken": "0", "TotalAmt": 8000.0,
                   "CustomerRef": {"value": "1", "name": "Initech"},
                   "MetaData": {"LastUpdatedTime": "2026-05-11T00:00:00-08:00"}},
    }, {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "payment"


# --- live webhook path -----------------------------------------------------

async def test_webhook_event_notification_is_thin_change():
    payload = {
        "eventNotifications": [{
            "companyId": _COMPANY,
            "dataChangeEvent": {"entities": [{
                "name": "Invoice", "id": "1038", "operation": "Update",
                "lastUpdated": "2026-05-31T00:00:00-08:00",
            }]},
        }],
    }
    draft = await handle_gusto_object(payload, {})
    assert draft.content["object_type"] == "invoice"
    assert draft.content["thin_change"] is True
    assert "1038" in draft.external_id


# --- rich-field ingestion ---------------------------------------------------

async def test_invoice_line_items_and_dimensions_captured():
    inv = _invoice()
    inv["entity"]["Line"] = [{
        "Id": "1", "Amount": 5000.0, "Description": "Platform License",
        "DetailType": "SalesItemLineDetail",
        "SalesItemLineDetail": {
            "ItemRef": {"value": "5", "name": "Platform License"},
            "Qty": 1, "UnitPrice": 5000.0,
            "ClassRef": {"value": "3", "name": "Sales"},
        },
    }]
    inv["entity"]["TxnTaxDetail"] = {"TotalTax": 400.0, "TaxLine": [{"Amount": 400.0}]}
    inv["entity"]["ClassRef"] = {"value": "3", "name": "Sales"}
    inv["entity"]["DepartmentRef"] = {"value": "2", "name": "East"}
    draft = await handle_gusto_object(inv, {})
    c = draft.content
    assert len(c["line_items"]) == 1
    li = c["line_items"][0]
    assert li["item"] == "Platform License"
    assert li["quantity"] == 1
    assert li["unit_price"] == 5000.0
    assert c["tax"]["total_tax"] == 400.0
    assert c["class"] == "Sales"
    assert c["department"] == "East"
    assert "1 line" in draft.content_text


async def test_payment_linked_txns_and_cash_fields():
    draft = await handle_gusto_object({
        "_fyralis_record_type": "payment", "_fyralis_company_uuid": _COMPANY,
        "entity": {
            "Id": "88", "SyncToken": "0", "TotalAmt": 8000.0, "UnappliedAmt": 500.0,
            "CustomerRef": {"value": "1", "name": "Initech"},
            "DepositToAccountRef": {"value": "35", "name": "Operating Checking"},
            "PaymentMethodRef": {"value": "2", "name": "Wire"},
            "Line": [{"Amount": 7500.0,
                      "LinkedTxn": [{"TxnId": "1037", "TxnType": "Invoice"}]}],
            "MetaData": {"LastUpdatedTime": "2026-05-11T00:00:00-08:00"},
        },
    }, {})
    c = draft.content
    assert c["unapplied_amount"] == 500.0
    assert c["deposit_to_account"] == "Operating Checking"
    assert c["payment_method"] == "Wire"


async def test_bill_expense_account_line_captured():
    draft = await handle_gusto_object({
        "_fyralis_record_type": "bill", "_fyralis_company_uuid": _COMPANY,
        "entity": {
            "Id": "204", "SyncToken": "0", "TotalAmt": 3200.0, "Balance": 3200.0,
            "VendorRef": {"value": "7", "name": "AWS"},
            "Line": [{
                "Id": "1", "Amount": 3200.0, "Description": "Cloud Infra",
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": "33", "name": "Cloud Infra"},
                },
            }],
            "MetaData": {"LastUpdatedTime": "2026-05-10T00:00:00-08:00"},
        },
    }, {})
    assert draft.content["line_items"][0]["account"] == "Cloud Infra"


async def test_multicurrency_fields_captured():
    inv = _invoice()
    inv["entity"]["CurrencyRef"] = {"value": "EUR", "name": "Euro"}
    inv["entity"]["ExchangeRate"] = 1.08
    inv["entity"]["HomeTotalAmt"] = 5400.0
    inv["entity"]["HomeBalance"] = 5400.0
    draft = await handle_gusto_object(inv, {})
    assert draft.content["exchange_rate"] == 1.08
    assert draft.content["home_total_amount"] == 5400.0


async def test_extras_absent_keys_not_emitted():
    """A bare invoice must not bloat content with empty extras."""
    draft = await handle_gusto_object(_invoice(), {})
    for k in ("line_items", "linked_txns", "tax", "class", "exchange_rate"):
        assert k not in draft.content


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_gusto_object({"foo": "bar"}, {})
