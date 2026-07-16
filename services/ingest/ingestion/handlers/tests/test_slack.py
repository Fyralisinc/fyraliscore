"""Tests for services/ingest/ingestion/handlers/slack.py DM (im/mpim) + mutation
subtype handling.

DMs and channel messages share the one `slack:message` handler and the
`external_id="{channel}:{ts}"` dedup key. These tests pin the DM-specific
behaviour added for per-user-OAuth DM ingestion: channel_type passthrough,
message_changed captured as a distinct edit signal, and message_deleted
rejected.
"""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.slack import handle_slack_message


pytestmark = pytest.mark.asyncio


def _cb(event: dict) -> dict:
    return {"type": "event_callback", "team_id": "T0001", "event": event}


async def test_handler_registered():
    assert get_handler("slack:message") is handle_slack_message
    assert "slack:message" in CHANNEL_TRUST_MAP


async def test_message_im_is_signal_with_dm_channel_type():
    draft = await handle_slack_message(_cb({
        "type": "message", "channel": "D9A1E26A14F2", "channel_type": "im",
        "user": "U_FRIEND", "text": "are we still on for friday?",
        "ts": "1780184162.000102",
    }), {})
    assert draft.source_channel == "slack:message"
    assert draft.kind == "signal"
    assert draft.external_id == "D9A1E26A14F2:1780184162.000102"
    assert draft.content["channel_type"] == "im"
    assert draft.content["subtype"] is None
    assert draft.source_actor_ref == "slack:U_FRIEND"


async def test_message_mpim_group_dm():
    draft = await handle_slack_message(_cb({
        "type": "message", "channel": "G510A6885592", "channel_type": "mpim",
        "user": "U_BOB", "text": "who's on the incident?",
        "ts": "1780184162.000200",
    }), {})
    assert draft.external_id == "G510A6885592:1780184162.000200"
    assert draft.content["channel_type"] == "mpim"


async def test_message_changed_is_distinct_edit_signal_on_edit_ts():
    """An edit is captured as its OWN observation keyed on the edit ts (dedup is
    insert-only, so reusing the original ts would drop the edited text). The
    original message ts is preserved in content.original_ts."""
    draft = await handle_slack_message(_cb({
        "type": "message", "subtype": "message_changed",
        "channel": "D9A1E26A14F2", "channel_type": "im",
        "message": {
            "type": "message", "user": "U_FRIEND",
            "text": "(edited) actually let's make it 4pm",
            "ts": "1780184162.000102", "edited_ts": "1780245425.000004",
        },
        "previous_message": {"type": "message", "user": "U_FRIEND",
                             "text": "let's make it 3pm", "ts": "1780184162.000102"},
        "ts": "1780245425.000004", "event_ts": "1780245425.000004",
    }), {})
    assert draft.content_text == "(edited) actually let's make it 4pm"
    # Keyed on the EDIT ts → distinct from the original observation.
    assert draft.external_id == "D9A1E26A14F2:1780245425.000004"
    assert draft.content["subtype"] == "message_changed"
    assert draft.content["original_ts"] == "1780184162.000102"


async def test_message_deleted_is_rejected():
    with pytest.raises(ValidationError):
        await handle_slack_message(_cb({
            "type": "message", "subtype": "message_deleted",
            "channel": "D9A1E26A14F2", "channel_type": "im",
            "deleted_ts": "1780184162.000102", "ts": "1780245425.000010",
        }), {})


async def test_channel_message_still_works():
    draft = await handle_slack_message(_cb({
        "type": "message", "channel": "C0GENERAL01", "channel_type": "channel",
        "user": "U_BOB", "text": "deploy is green", "ts": "1780184162.000300",
        "thread_ts": "1780184000.000001", "parent_user_id": "U_ALICE",
        "reply_count": 3, "reply_users": ["U_ALICE", "U_BOB"],
    }), {})
    assert draft.external_id == "C0GENERAL01:1780184162.000300"
    assert draft.content["channel_type"] == "channel"
    assert draft.content["thread_ts"] == "1780184000.000001"
    assert draft.content["parent_user_id"] == "U_ALICE"
    assert draft.content["reply_count"] == 3


async def test_system_event_without_text_still_rejected():
    with pytest.raises(ValidationError):
        await handle_slack_message(_cb({
            "type": "message", "subtype": "channel_join",
            "channel": "C0GENERAL01", "user": "U_BOB", "ts": "1780184162.000400",
        }), {})
