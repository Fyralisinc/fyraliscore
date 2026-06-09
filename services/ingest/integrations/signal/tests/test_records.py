"""Tests for services/ingest/integrations/signal/records.py (IN-SIGNAL)."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.integrations.signal.records import (
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


def test_build_injects_thread_context():
    rec = build_message_record(
        _raw(), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    assert rec["_fyralis_record_type"] == "message"
    assert rec["_fyralis_installation_id"] == "inst"
    assert rec["_fyralis_thread_id"] == 42
    assert rec["_fyralis_thread_kind"] == "group"
    assert rec["_fyralis_thread_title"] == "Eng"
    # original message fields preserved
    assert rec["id"] == 7 and rec["message"] == "hi"


def test_parse_round_trip():
    rec = build_message_record(
        _raw(mid=7, sender=99), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    p = parse_message_record(rec)
    assert p.message_id == 7
    assert p.thread_id == 42
    assert p.sender_id == 99
    assert p.occurred_at.year == 2026
    assert p.external_id == "signal:inst:42:7:none"
    assert CHANNEL == "signal:message"


def test_parse_edit_unsupported_is_none():
    # v1 does not support edits; even if a record carries an edit_date the
    # external_id renders 'none' only when edit_date is falsy. A present
    # edit_date still versions defensively (parity with telegram's constructor).
    rec = build_message_record(
        _raw(mid=7, edit_date=None), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    p = parse_message_record(rec)
    assert p.edit_date is None
    assert p.external_id == "signal:inst:42:7:none"


def test_parse_no_sender_ok():
    rec = build_message_record(
        _raw(sender=None), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title=None,
    )
    p = parse_message_record(rec)
    assert p.sender_id is None  # group system / self-sent message


def test_parse_missing_id_raises():
    rec = build_message_record(
        _raw(), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    rec.pop("id")
    with pytest.raises(ValidationError):
        parse_message_record(rec)


def test_parse_missing_thread_raises():
    rec = build_message_record(
        _raw(), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    rec.pop("_fyralis_thread_id")
    with pytest.raises(ValidationError):
        parse_message_record(rec)


def test_parse_unparseable_date_raises():
    rec = build_message_record(
        _raw(date=None), installation_id="inst", thread_id=42,
        thread_kind="group", thread_title="Eng",
    )
    with pytest.raises(ValidationError):
        parse_message_record(rec)
