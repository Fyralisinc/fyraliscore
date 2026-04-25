"""Tests for services/ingestion/handlers/calendar.py."""
from __future__ import annotations

import pytest

from services.ingestion.handlers.calendar import (
    CalendarSignatureError,
    handle_calendar_webhook,
    verify_calendar_token,
)


# =====================================================================
# Token check
# =====================================================================

def test_calendar_token_happy_path():
    verify_calendar_token("my-secret-token", "my-secret-token")


def test_calendar_token_mismatch_raises():
    with pytest.raises(CalendarSignatureError) as exc:
        verify_calendar_token("wrong", "my-secret-token")
    assert exc.value.context.get("reason") == "mismatch"


def test_calendar_token_missing_header_raises():
    with pytest.raises(CalendarSignatureError) as exc:
        verify_calendar_token(None, "my-secret-token")
    assert exc.value.context.get("reason") == "missing_signature"


def test_calendar_token_missing_secret_raises():
    with pytest.raises(CalendarSignatureError) as exc:
        verify_calendar_token("abc", "")
    assert exc.value.context.get("reason") == "missing_secret"


# =====================================================================
# Attendee resolution
# =====================================================================

class _FakeActorResolver:
    def __init__(self, mapping):
        self._mapping = mapping

    async def resolve_by_source_actor_ref(self, ref):
        return self._mapping.get(ref)


async def test_calendar_attendees_resolved_to_actor_ids_where_known():
    alice_id = "11111111-1111-1111-1111-111111111111"
    bob_id = "22222222-2222-2222-2222-222222222222"
    resolver = _FakeActorResolver({
        "email:alice@company.com": alice_id,
        "email:bob@company.com": bob_id,
    })
    payload = {
        "action": "created",
        "event": {
            "id": "evt1",
            "summary": "1:1 with Bob",
            "description": "quick sync",
            "start": {"dateTime": "2026-04-22T14:00:00Z"},
            "end": {"dateTime": "2026-04-22T15:00:00Z"},
            "organizer": {"email": "alice@company.com"},
            "attendees": [
                {"email": "alice@company.com"},
                {"email": "bob@company.com"},
                {"email": "external@other.com"},
            ],
            "status": "confirmed",
        },
    }
    draft = await handle_calendar_webhook(
        payload, {}, tenant_id="t1", actor_resolver=resolver
    )
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    ids_by_type: dict[str, list[str]] = {}
    for e in draft.entities_hint:
        ids_by_type.setdefault(e["type"], []).append(e["id"])
    # Alice and Bob should become actor entities.
    assert alice_id in ids_by_type.get("actor", [])
    assert bob_id in ids_by_type.get("actor", [])
    # The unknown external attendee stays as an email_address.
    assert "external@other.com" in ids_by_type.get("email_address", [])


async def test_calendar_created_content_text():
    payload = {
        "action": "created",
        "event": {
            "id": "evt2",
            "summary": "Team sync",
            "start": {"dateTime": "2026-04-22T14:00:00Z"},
            "end": {"dateTime": "2026-04-22T15:00:00Z"},
            "organizer": {"email": "alice@company.com"},
        },
    }
    draft = await handle_calendar_webhook(payload, {}, tenant_id="t1")
    assert "alice scheduled 'Team sync'" in draft.content_text


async def test_calendar_cancelled_is_state_change():
    payload = {
        "action": "cancelled",
        "event": {
            "id": "evt3",
            "summary": "Team sync",
            "start": {"dateTime": "2026-04-22T14:00:00Z"},
            "end": {"dateTime": "2026-04-22T15:00:00Z"},
            "organizer": {"email": "alice@company.com"},
            "status": "cancelled",
        },
    }
    draft = await handle_calendar_webhook(payload, {})
    assert draft.kind == "state_change"
    assert "cancelled" in draft.content_text.lower()


async def test_calendar_missing_event_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_calendar_webhook({"action": "updated"}, {})


async def test_calendar_alias_hits_picked_up_in_description():
    class _AliasResolver:
        async def fast_path_resolve(self, phrase, tenant_id):
            if phrase.strip().lower() == "payments service":
                return {"type": "commitment", "id": "commitment-uuid"}
            return None

    payload = {
        "action": "updated",
        "event": {
            "id": "evt4",
            "summary": "Roadmap review",
            "description": "agenda includes payments service rollout plan",
            "start": {"dateTime": "2026-04-22T14:00:00Z"},
            "end": {"dateTime": "2026-04-22T15:00:00Z"},
            "organizer": {"email": "alice@company.com"},
        },
    }
    draft = await handle_calendar_webhook(
        payload, {},
        tenant_id="t1",
        alias_resolver=_AliasResolver(),
    )
    refs = [e for e in draft.entities_hint if e["type"] == "commitment"]
    assert refs and refs[0]["id"] == "commitment-uuid"
