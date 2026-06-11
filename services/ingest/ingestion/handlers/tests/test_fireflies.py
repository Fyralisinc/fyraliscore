"""Tests for services/ingest/ingestion/handlers/fireflies.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.fireflies import handle_fireflies_transcript


pytestmark = pytest.mark.asyncio


_WS = "ws-acme"


def _transcript(**over):
    base = {
        "id": "ts-1001",
        "title": "Weekly Engineering Sync",
        "dateTime": "2026-05-20T12:30:00.000Z",
        "version": "2026-05-20T12:30:00.000Z",
        "participants": [
            {"name": "Alice", "email": "alice@acme.example"},
            {"name": "Bob", "email": "bob@acme.example"},
        ],
        "summary": {"overview": "Talked about the release.",
                    "action_items": ["ship it"]},
        "duration": 45,
    }
    base.update(over)
    return {"_fyralis_record_type": "transcript", "_fyralis_workspace_id": _WS,
            "transcript": base}


async def test_handler_registered():
    assert get_handler("fireflies:transcript") is handle_fireflies_transcript
    assert CHANNEL_TRUST_MAP["fireflies:transcript"] == "attested_agent"


async def test_transcript_is_signal_with_versioned_external_id():
    draft = await handle_fireflies_transcript(_transcript(), {})
    assert draft.source_channel == "fireflies:transcript"
    assert draft.trust_tier == "attested_agent"
    assert draft.kind == "signal"
    # external_id namespaced by workspace + versioned by content version.
    assert draft.external_id == (
        f"fireflies:{_WS}:transcript:ts-1001:2026-05-20T12:30:00.000Z"
    )
    assert draft.content["object_type"] == "transcript"
    assert draft.content["title"] == "Weekly Engineering Sync"
    assert "Alice" in draft.content_text


async def test_reprocessed_transcript_produces_distinct_external_id():
    """Mutable-source dedup lesson: a re-processed (re-versioned) transcript must
    NOT collapse onto the earlier observation."""
    v1 = await handle_fireflies_transcript(_transcript(version="v1"), {})
    v2 = await handle_fireflies_transcript(_transcript(version="v2"), {})
    assert v1.external_id != v2.external_id


async def test_summary_and_action_items_captured():
    draft = await handle_fireflies_transcript(_transcript(), {})
    assert draft.content["summary"] == "Talked about the release."
    assert draft.content["action_items"] == ["ship it"]
    assert draft.content["duration_minutes"] == 45


async def test_epoch_millis_date_and_snake_organizer_are_supported():
    draft = await handle_fireflies_transcript(_transcript(
        dateTime=None,
        date=1_777_593_600_000,
        version=1_777_593_600_000,
        organizer_email="owner@example.com",
    ), {})
    assert draft.occurred_at.isoformat() == "2026-05-01T00:00:00+00:00"
    assert draft.content["organizer_email"] == "owner@example.com"


async def test_participants_become_entities():
    draft = await handle_fireflies_transcript(_transcript(), {})
    person_entities = [e for e in draft.entities_hint if e["type"] == "person"]
    assert {e["id"] for e in person_entities} == {"Alice", "Bob"}
    workspace_entities = [e for e in draft.entities_hint if e["type"] == "fireflies_workspace"]
    assert workspace_entities[0]["id"] == _WS


# --- live webhook path -----------------------------------------------------

async def test_webhook_transcript_completed():
    payload = {
        "type": "transcript.completed",
        "workspaceId": _WS,
        "transcript": {"id": "ts-1001", "title": "Weekly Engineering Sync",
                       "dateTime": "2026-05-20T12:30:00.000Z",
                       "version": "2026-05-20T12:30:00.000Z"},
    }
    draft = await handle_fireflies_transcript(payload, {})
    assert draft.content["object_type"] == "transcript"
    # external_id parity with the backfilled transcript record.
    assert draft.external_id == (
        f"fireflies:{_WS}:transcript:ts-1001:2026-05-20T12:30:00.000Z"
    )


async def test_backfill_and_webhook_dedup_to_same_external_id():
    backfill = await handle_fireflies_transcript(_transcript(), {})
    webhook = await handle_fireflies_transcript({
        "type": "transcript.completed", "workspaceId": _WS,
        "transcript": {"id": "ts-1001", "title": "Weekly Engineering Sync",
                       "dateTime": "2026-05-20T12:30:00.000Z",
                       "version": "2026-05-20T12:30:00.000Z"},
    }, {})
    assert backfill.external_id == webhook.external_id


async def test_missing_id_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_fireflies_transcript({
            "_fyralis_record_type": "transcript", "_fyralis_workspace_id": _WS,
            "transcript": {"title": "no id"},
        }, {})


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_fireflies_transcript({"foo": "bar"}, {})
