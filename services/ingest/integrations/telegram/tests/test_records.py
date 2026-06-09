"""Tests for services/ingest/integrations/telegram/records.py (IN-TELEGRAM)."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.integrations.telegram.records import (
    CHANNEL,
    build_message_record,
    parse_message_record,
)


def _raw(mid=7, *, date=1781000000, edit_date=None, sender=99):
    return {
        "id": mid, "date": date, "edit_date": edit_date,
        "message": "hi", "out": False,
        "from_id": {"user_id": sender} if sender is not None else None,
    }


def test_build_injects_dialog_context():
    rec = build_message_record(
        _raw(), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    assert rec["_fyralis_record_type"] == "message"
    assert rec["_fyralis_installation_id"] == "inst"
    assert rec["_fyralis_dialog_id"] == 42
    assert rec["_fyralis_dialog_kind"] == "channel"
    assert rec["_fyralis_dialog_title"] == "Eng"
    # original message fields preserved
    assert rec["id"] == 7 and rec["message"] == "hi"


def test_parse_round_trip():
    rec = build_message_record(
        _raw(mid=7, sender=99), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    p = parse_message_record(rec)
    assert p.message_id == 7
    assert p.dialog_id == 42
    assert p.sender_id == 99
    assert p.occurred_at.year == 2026
    assert p.external_id == "telegram:inst:42:7:none"
    assert CHANNEL == "telegram:message"


def test_parse_edit_versioned():
    rec = build_message_record(
        _raw(mid=7, edit_date=1781000500), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    p = parse_message_record(rec)
    assert p.edit_date == 1781000500
    assert p.external_id == "telegram:inst:42:7:1781000500"


def test_parse_no_sender_ok():
    rec = build_message_record(
        _raw(sender=None), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title=None,
    )
    p = parse_message_record(rec)
    assert p.sender_id is None  # channel broadcast / service message


def test_parse_missing_id_raises():
    rec = build_message_record(
        _raw(), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    rec.pop("id")
    with pytest.raises(ValidationError):
        parse_message_record(rec)


def test_parse_missing_dialog_raises():
    rec = build_message_record(
        _raw(), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    rec.pop("_fyralis_dialog_id")
    with pytest.raises(ValidationError):
        parse_message_record(rec)


def test_parse_unparseable_date_raises():
    rec = build_message_record(
        _raw(date=None), installation_id="inst", dialog_id=42,
        dialog_kind="channel", dialog_title="Eng",
    )
    with pytest.raises(ValidationError):
        parse_message_record(rec)
