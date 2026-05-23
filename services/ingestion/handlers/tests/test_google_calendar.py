"""Tests for services/ingestion/handlers/google_calendar.py (IN-15)."""
from __future__ import annotations

import pytest

from services.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingestion.handlers.google_calendar import handle_google_calendar_event


pytestmark = pytest.mark.asyncio


def _event(**over):
    base = {
        "kind": "calendar#event",
        "id": "evt-1",
        "status": "confirmed",
        "summary": "Q3 roadmap review",
        "description": "Agenda: Atlas, Helios",
        "eventType": "default",
        "start": {"dateTime": "2026-04-22T14:00:00-07:00"},
        "end": {"dateTime": "2026-04-22T15:00:00-07:00"},
        "organizer": {"email": "alice@acme.com", "displayName": "Alice"},
        "creator": {"email": "alice@acme.com"},
        "attendees": [
            {"email": "alice@acme.com", "responseStatus": "accepted", "organizer": True},
            {"email": "bob@acme.com", "responseStatus": "accepted"},
            {"email": "investor@vc.com", "responseStatus": "tentative"},
        ],
        "htmlLink": "https://calendar.google.com/event?eid=evt-1",
        "updated": "2026-04-20T10:00:00.000Z",
        "created": "2026-04-01T09:00:00.000Z",
        "_fyralis_calendar_id": "alice@acme.com",
        "_fyralis_owner_email": "alice@acme.com",
    }
    base.update(over)
    return base


async def test_confirmed_event_is_signal_with_rich_content():
    draft = await handle_google_calendar_event(_event(), {})
    assert draft.source_channel == "google_calendar:event"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.external_id == "gcal:alice@acme.com:evt-1"
    assert draft.content["object_type"] == "event"
    assert draft.content["summary"] == "Q3 roadmap review"
    assert draft.content["attendee_count"] == 3
    assert draft.content["duration_minutes"] == 60
    assert draft.source_actor_ref == "email:alice@acme.com"
    # occurred_at is the start time (normalised to UTC: 14:00 PDT == 21:00 UTC).
    assert draft.occurred_at.isoformat().startswith("2026-04-22T21:00:00")


async def test_cancelled_event_is_state_change():
    draft = await handle_google_calendar_event(
        _event(status="cancelled"), {},
    )
    assert draft.kind == "state_change"
    assert "cancelled" in draft.content_text.lower()


async def test_entity_hints_mark_external_attendee():
    draft = await handle_google_calendar_event(_event(), {})
    by_id = {e["id"]: e for e in draft.entities_hint if e["type"] == "email_address"}
    assert by_id["bob@acme.com"]["external"] is False
    assert by_id["investor@vc.com"]["external"] is True
    # the organizer hint carries its role.
    assert by_id["alice@acme.com"]["role"] == "organizer"
    # the meeting topic is surfaced as an entity hint.
    assert {"type": "meeting_topic", "id": "Q3 roadmap review"} in draft.entities_hint


async def test_all_day_event_occurred_at_is_midnight_utc():
    draft = await handle_google_calendar_event(
        _event(start={"date": "2026-05-01"}, end={"date": "2026-05-02"}), {},
    )
    assert draft.occurred_at.isoformat().startswith("2026-05-01T00:00:00")


async def test_cancelled_event_without_start_falls_back_to_updated():
    """A sync-delta cancellation often drops `start`; occurred_at must still
    resolve (Risk #5)."""
    ev = {
        "kind": "calendar#event",
        "id": "evt-9",
        "status": "cancelled",
        "updated": "2026-04-25T12:00:00.000Z",
        "_fyralis_calendar_id": "alice@acme.com",
        "_fyralis_owner_email": "alice@acme.com",
    }
    draft = await handle_google_calendar_event(ev, {})
    assert draft.kind == "state_change"
    assert draft.occurred_at.isoformat().startswith("2026-04-25T12:00:00")


async def test_external_id_stable_across_calls():
    """Backfill + poll twins must derive the same external_id (dedup)."""
    a = await handle_google_calendar_event(_event(), {})
    b = await handle_google_calendar_event(_event(), {})
    assert a.external_id == b.external_id == "gcal:alice@acme.com:evt-1"


async def test_missing_id_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_google_calendar_event({"kind": "calendar#event"}, {})


async def test_handler_registered():
    assert get_handler("google_calendar:event") is handle_google_calendar_event
    assert CHANNEL_TRUST_MAP["google_calendar:event"] == "authoritative"
