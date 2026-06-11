"""Tests for services/ingest/ingestion/handlers/gusto.py (finance/payroll)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.gusto import handle_gusto_object


pytestmark = pytest.mark.asyncio


_COMPANY = "8b342a55-907e-4ba8-a95d-d29fbf95d6e1"


def _employee(**over):
    base = {
        "uuid": "emp-1001", "version": "v-aaa1",
        "first_name": "Ava", "last_name": "Reyes",
        "work_email": "ava.reyes@acme.example",
        "department": "Engineering",
        "terminated": False, "onboarded": True,
        "current_employment_status": "full_time",
        "payment_method": "Direct Deposit",
        "jobs": [{
            "uuid": "job-1", "primary": True, "title": "Software Engineer",
            "hire_date": "2025-08-15",
            # Compensation rate is a decimal STRING in dollars (real wire).
            "rate": "98000.00", "payment_unit": "Year",
        }],
        "terminations": [],
    }
    base.update(over)
    return {"_fyralis_record_type": "employee",
            "_fyralis_company_uuid": _COMPANY, "entity": base}


def _payroll(**over):
    base = {
        "payroll_uuid": "pay-2001", "uuid": "pay-2001",
        "check_date": "2026-05-15", "processed": True,
        "processed_date": "2026-05-15", "off_cycle": False, "external": False,
        "pay_period": {"start_date": "2026-05-01", "end_date": "2026-05-14",
                       "pay_schedule_uuid": "sched-1"},
        # All dollar amounts are decimal STRINGS (real wire shape).
        "totals": {"gross_pay": "42000.00", "net_pay": "33600.00",
                   "employee_taxes": "8400.00", "employer_taxes": "3360.00",
                   "company_debit": "45360.00", "benefits": "0.00",
                   "reimbursements": "0.00"},
        "employee_compensations": [],
    }
    base.update(over)
    return {"_fyralis_record_type": "payroll",
            "_fyralis_company_uuid": _COMPANY, "entity": base}


async def test_handler_registered():
    assert get_handler("gusto:object") is handle_gusto_object
    assert CHANNEL_TRUST_MAP["gusto:object"] == "authoritative"


# --- employee records --------------------------------------------------------

async def test_active_employee_is_signal_with_versioned_external_id():
    draft = await handle_gusto_object(_employee(), {})
    assert draft.source_channel == "gusto:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.content["status"] == "active"
    # external_id versioned by the employee's own concurrency token.
    assert draft.external_id == f"gusto:{_COMPANY}:employee:emp-1001:v-aaa1"
    assert draft.content["object_type"] == "employee"
    assert "Ava Reyes" in draft.content_text
    assert "Software Engineer" in draft.content_text
    # Decimal-string dollars kept verbatim.
    assert draft.content["rate"] == "98000.00"
    assert draft.content["payment_unit"] == "Year"


async def test_terminated_employee_is_state_change():
    draft = await handle_gusto_object(_employee(
        terminated=True,
        terminations=[{"effective_date": "2026-05-10", "active": False}],
    ), {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "terminated"
    # occurred_at anchors on the termination's effective_date.
    assert draft.occurred_at == datetime(2026, 5, 10, tzinfo=timezone.utc)


async def test_onboarding_employee_is_signal():
    draft = await handle_gusto_object(_employee(onboarded=False), {})
    assert draft.kind == "signal"
    assert draft.content["status"] == "onboarding"


async def test_version_bump_produces_distinct_external_id():
    """Mutable-source dedup lesson: a changed employee (new `version`) must
    land distinct; an unchanged re-walk twin must collide."""
    v0 = await handle_gusto_object(_employee(), {})
    v1 = await handle_gusto_object(_employee(version="v-aaa2"), {})
    twin = await handle_gusto_object(_employee(), {})
    assert v0.external_id != v1.external_id
    assert v0.external_id == twin.external_id


async def test_employee_occurred_at_falls_back_to_hire_date():
    draft = await handle_gusto_object(_employee(), {})
    assert draft.occurred_at == datetime(2025, 8, 15, tzinfo=timezone.utc)


async def test_employee_person_entity_hint_uses_work_email():
    draft = await handle_gusto_object(_employee(), {})
    hints = {(h["type"], h.get("id")) for h in draft.entities_hint}
    assert ("gusto_object", "employee:emp-1001") in hints
    assert ("person", "ava.reyes@acme.example") in hints


# --- payroll records ----------------------------------------------------------

async def test_processed_payroll_is_state_change_with_processed_external_id():
    draft = await handle_gusto_object(_payroll(), {})
    assert draft.kind == "state_change"
    assert draft.content["status"] == "processed"
    # No top-level version on Payroll — the processed flip discriminates.
    assert draft.external_id == f"gusto:{_COMPANY}:payroll:pay-2001:processed"
    assert draft.content["object_type"] == "payroll"
    # occurred_at anchors on check_date (the cash event).
    assert draft.occurred_at == datetime(2026, 5, 15, tzinfo=timezone.utc)
    # Decimal-string dollars kept verbatim; formatted in the text.
    assert draft.content["gross_pay"] == "42000.00"
    assert draft.content["net_pay"] == "33600.00"
    assert "$42,000.00" in draft.content_text


async def test_unprocessed_payroll_is_signal_pending():
    draft = await handle_gusto_object(
        _payroll(processed=False, processed_date=None), {},
    )
    assert draft.kind == "signal"
    assert draft.content["status"] == "pending"
    assert draft.external_id == f"gusto:{_COMPANY}:payroll:pay-2001:unprocessed"


async def test_processed_flip_produces_distinct_external_id():
    pending = await handle_gusto_object(
        _payroll(processed=False, processed_date=None), {},
    )
    processed = await handle_gusto_object(_payroll(), {})
    assert pending.external_id != processed.external_id


async def test_off_cycle_payroll_noted_in_text():
    draft = await handle_gusto_object(_payroll(off_cycle=True), {})
    assert "off-cycle" in draft.content_text


# --- live webhook path (real flat thin notification) -------------------------

async def test_webhook_thin_notification_parses_flat_shape():
    payload = {
        "uuid": "03ffbf0c-48a9-4f1c-932a-546413a26ad1",
        "event_type": "employee.updated",
        "resource_type": "Company",
        "resource_uuid": _COMPANY,
        "entity_type": "Employee",
        "entity_uuid": "emp-1001",
        "timestamp": 1771058841,
    }
    draft = await handle_gusto_object(payload, {})
    assert draft.content["object_type"] == "employee"
    assert draft.content["thin_change"] is True
    assert draft.content["company_uuid"] == _COMPANY
    assert draft.content["entity_id"] == "emp-1001"
    assert draft.content["operation"] == "updated"
    # Versioned by the delivery's own uuid -> re-deliveries dedup.
    assert draft.external_id == (
        f"gusto:{_COMPANY}:employee:emp-1001:chg:"
        "03ffbf0c-48a9-4f1c-932a-546413a26ad1"
    )
    # epoch-seconds timestamp -> occurred_at.
    assert draft.occurred_at == datetime.fromtimestamp(
        1771058841, tz=timezone.utc,
    )


async def test_webhook_redelivery_dedups_distinct_events_stay_distinct():
    base = {
        "uuid": "evt-1", "event_type": "payroll.processed",
        "resource_type": "Company", "resource_uuid": _COMPANY,
        "entity_type": "Payroll", "entity_uuid": "pay-2001",
        "timestamp": 1771058841,
    }
    first = await handle_gusto_object(dict(base), {})
    redelivery = await handle_gusto_object(dict(base), {})
    distinct = await handle_gusto_object({**base, "uuid": "evt-2"}, {})
    assert first.external_id == redelivery.external_id
    assert first.external_id != distinct.external_id


# --- validation ----------------------------------------------------------------

async def test_clean_absent_keys_not_emitted():
    """A minimal employee must not bloat content with None-valued keys."""
    minimal = _employee(department=None, manager_uuid=None, jobs=[])
    draft = await handle_gusto_object(minimal, {})
    for k in ("department", "manager_uuid", "job_title", "rate"):
        assert k not in draft.content


async def test_employee_missing_uuid_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_gusto_object(_employee(uuid=None), {})


async def test_unsupported_record_type_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_gusto_object({
            "_fyralis_record_type": "invoice",  # the dead QBO-clone taxonomy
            "_fyralis_company_uuid": _COMPANY, "entity": {"Id": "1037"},
        }, {})


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_gusto_object({"foo": "bar"}, {})
