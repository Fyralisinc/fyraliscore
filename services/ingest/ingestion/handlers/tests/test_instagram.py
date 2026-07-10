"""Tests for the Instagram Messaging handler."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.instagram import handle_instagram
from services.ingest.integrations.instagram.records import (
    build_history_record,
    build_webhook_record,
)


pytestmark = pytest.mark.asyncio


def _webhook_event(*, is_echo: bool = False):
    return {
        "sender": {"id": "ig-business" if is_echo else "cust-1"},
        "recipient": {"id": "cust-1" if is_echo else "ig-business"},
        "timestamp": 1781000000000,
        "message": {
            "mid": "mid-1",
            "text": "hello from instagram",
            "is_echo": is_echo,
        },
    }


async def test_dispatch_and_trust_wired():
    assert get_handler("instagram:message") is handle_instagram
    assert CHANNEL_TRUST_MAP["instagram:message"] == "attested_agent"


async def test_inbound_customer_message_draft():
    record = build_webhook_record(
        _webhook_event(),
        ig_business_account_id="ig-business",
        page_id="page-1",
    )
    assert record is not None

    draft = await handle_instagram(record, {})

    assert draft.source_channel == "instagram:message"
    assert draft.kind == "signal"
    assert draft.trust_tier == "attested_agent"
    assert draft.source_actor_ref == "instagram:ig-business:user:cust-1"
    assert draft.external_id == "instagram:ig-business:message:mid-1"
    assert "hello from instagram" in draft.content_text
    assert draft.content["direction"] == "inbound"
    assert "provider_event" not in draft.content
    assert record["provider_event"] == _webhook_event()
    kinds = {e["type"] for e in draft.entities_hint}
    assert {
        "instagram_business_account",
        "instagram_conversation",
        "instagram_customer",
    } <= kinds


async def test_outbound_business_message_uses_business_actor():
    record = build_webhook_record(
        _webhook_event(is_echo=True),
        ig_business_account_id="ig-business",
    )
    assert record is not None

    draft = await handle_instagram(record, {})

    assert draft.source_actor_ref == "instagram:business:ig-business"
    assert draft.content["direction"] == "outbound"


async def test_outbound_delivery_alias_uses_the_customer_thread():
    record = build_webhook_record(
        {
            "sender": {"id": "meta-delivery-id"},
            "recipient": {"id": "cust-1"},
            "timestamp": 1781000000000,
            "message": {
                "mid": "mid-delivery-alias",
                "text": "business reply",
                "is_echo": True,
            },
        },
        ig_business_account_id="ig-business",
        entry_id="meta-delivery-id",
    )
    assert record is not None
    assert record["customer_id"] == "cust-1"
    assert record["thread_key"] == "ig-business:cust-1"
    assert record["conversation_id"] == "ig-business:cust-1"


async def test_history_outbound_delivery_alias_uses_the_customer_thread():
    record = build_history_record(
        {
            "id": "mid-history-delivery-alias",
            "created_time": "2026-07-10T12:00:00+00:00",
            "message": "business reply",
            "from": {"id": "meta-delivery-id"},
            "to": {"data": [{"id": "cust-1"}]},
        },
        ig_business_account_id="ig-business",
        page_id=None,
        conversation_id="conv-1",
        webhook_delivery_account_id="meta-delivery-id",
        participant_id="cust-1",
    )

    assert record["direction"] == "outbound"
    assert record["customer_id"] == "cust-1"
    assert record["thread_key"] == "ig-business:cust-1"
    assert (await handle_instagram(record, {})).source_actor_ref == (
        "instagram:business:ig-business"
    )


async def test_status_record_is_authoritative_state_change():
    record = build_webhook_record(
        {
            "sender": {"id": "cust-1"},
            "recipient": {"id": "ig-business"},
            "timestamp": 1781000001000,
            "read": {"watermark": "1781000001000", "mids": ["mid-1"]},
        },
        ig_business_account_id="ig-business",
    )
    assert record is not None

    draft = await handle_instagram(record, {})

    assert draft.kind == "state_change"
    assert draft.trust_tier == "authoritative"
    assert draft.external_id == (
        "instagram:ig-business:status:mid-1:read:1781000001000"
    )


async def test_reaction_and_edit_have_distinct_versioned_external_ids():
    reaction = build_webhook_record(
        {
            "sender": {"id": "cust-1"},
            "recipient": {"id": "ig-business"},
            "timestamp": 1781000002000,
            "reaction": {"mid": "mid-1", "action": "react", "reaction": "love"},
        },
        ig_business_account_id="ig-business",
    )
    edit = build_webhook_record(
        {
            "sender": {"id": "cust-1"},
            "recipient": {"id": "ig-business"},
            "timestamp": 1781000003000,
            "message_edit": {"mid": "mid-1", "num_edit": 2, "text": "updated"},
        },
        ig_business_account_id="ig-business",
    )
    assert reaction is not None and edit is not None

    reaction_draft = await handle_instagram(reaction, {})
    edit_draft = await handle_instagram(edit, {})

    assert reaction_draft.kind == edit_draft.kind == "state_change"
    assert reaction_draft.external_id != edit_draft.external_id
    assert reaction_draft.external_id == (
        "instagram:ig-business:status:mid-1:reaction:cust-1:react:love:1781000002000"
    )
    assert edit_draft.external_id == "instagram:ig-business:status:mid-1:message_edit:2"


async def test_history_and_webhook_external_id_parity():
    history = build_history_record(
        {
            "id": "mid-1",
            "created_time": "2026-06-09T12:00:00+00:00",
            "message": "hello from instagram",
            "from": {"id": "cust-1"},
            "to": {"data": [{"id": "ig-business"}]},
        },
        ig_business_account_id="ig-business",
        page_id=None,
        conversation_id="conv-1",
        participant_id="cust-1",
    )
    webhook = build_webhook_record(
        _webhook_event(),
        ig_business_account_id="ig-business",
    )
    assert webhook is not None

    assert (await handle_instagram(history, {})).external_id == (
        await handle_instagram(webhook, {})
    ).external_id


async def test_malformed_record_raises():
    with pytest.raises(ValidationError):
        await handle_instagram({"_fyralis_record_type": "message"}, {})
