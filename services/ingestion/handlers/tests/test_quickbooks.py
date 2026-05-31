"""Tests for services/ingestion/handlers/quickbooks.py (finance)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingestion.handlers.quickbooks import handle_quickbooks_object


pytestmark = pytest.mark.asyncio


_REALM = "9341452000000001"


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
    return {"_fyralis_record_type": "invoice", "_fyralis_realm_id": _REALM,
            "entity": base}


async def test_handler_registered():
    assert get_handler("quickbooks:object") is handle_quickbooks_object
    assert CHANNEL_TRUST_MAP["quickbooks:object"] == "authoritative"


async def test_open_invoice_is_signal_with_synctoken_external_id():
    draft = await handle_quickbooks_object(_invoice(), {})
    assert draft.source_channel == "quickbooks:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["status"] == "open"
    # external_id versioned by SyncToken.
    assert draft.external_id == f"qbo:{_REALM}:invoice:1037:0"
    assert draft.content["object_type"] == "invoice"
    assert "Globex" in draft.content_text


async def test_paid_invoice_is_state_change():
    base = _invoice()
    base["entity"]["Balance"] = 0.0
    base["entity"]["SyncToken"] = "1"
    draft = await handle_quickbooks_object(base, {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "paid"
    assert draft.external_id == f"qbo:{_REALM}:invoice:1037:1"


async def test_overdue_invoice_is_state_change():
    past = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    base = _invoice()
    base["entity"]["DueDate"] = past
    base["entity"]["Balance"] = 5000.0
    draft = await handle_quickbooks_object(base, {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "overdue"


async def test_synctoken_bump_produces_distinct_external_id():
    """Mutable-source dedup lesson: an edit (new SyncToken) must land distinct."""
    v0 = await handle_quickbooks_object(_invoice(), {})
    bumped = _invoice()
    bumped["entity"]["SyncToken"] = "1"
    v1 = await handle_quickbooks_object(bumped, {})
    assert v0.external_id != v1.external_id


async def test_bill_record():
    draft = await handle_quickbooks_object({
        "_fyralis_record_type": "bill", "_fyralis_realm_id": _REALM,
        "entity": {"Id": "204", "SyncToken": "0", "TotalAmt": 3200.0,
                   "Balance": 3200.0, "VendorRef": {"value": "7", "name": "AWS"},
                   "MetaData": {"LastUpdatedTime": "2026-05-10T00:00:00-08:00"}},
    }, {})
    assert draft.content["object_type"] == "bill"
    assert draft.external_id == f"qbo:{_REALM}:bill:204:0"
    assert "AWS" in draft.content_text


async def test_payment_record_is_signal():
    draft = await handle_quickbooks_object({
        "_fyralis_record_type": "payment", "_fyralis_realm_id": _REALM,
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
            "realmId": _REALM,
            "dataChangeEvent": {"entities": [{
                "name": "Invoice", "id": "1038", "operation": "Update",
                "lastUpdated": "2026-05-31T00:00:00-08:00",
            }]},
        }],
    }
    draft = await handle_quickbooks_object(payload, {})
    assert draft.content["object_type"] == "invoice"
    assert draft.content["thin_change"] is True
    assert "1038" in draft.external_id


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_quickbooks_object({"foo": "bar"}, {})
