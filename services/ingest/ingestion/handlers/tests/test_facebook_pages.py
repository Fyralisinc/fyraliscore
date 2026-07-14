from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers.facebook_pages import handle_facebook_pages


pytestmark = pytest.mark.asyncio


async def test_webhook_and_backfill_message_dedup_match():
    webhook = await handle_facebook_pages(
        {
            "source": "webhook",
            "page_id": "PAGE1",
            "sender": {"id": "PSID1"},
            "recipient": {"id": "PAGE1"},
            "timestamp": 1_704_067_200_000,
            "message": {"mid": "m_1", "text": "hello"},
        },
        {},
    )
    backfill = await handle_facebook_pages(
        {
            "source": "backfill",
            "page_id": "PAGE1",
            "conversation_id": "t_1",
            "id": "m_1",
            "created_time": "2024-01-01T00:00:00+0000",
            "message": {
                "id": "m_1",
                "message": "hello",
                "from": {"id": "PSID1"},
                "to": {"data": [{"id": "PAGE1"}]},
            },
        },
        {},
    )

    assert webhook.external_id == "facebook_pages:PAGE1:m_1"
    assert backfill.external_id == webhook.external_id
    assert webhook.source_actor_ref == "PSID1"
    assert backfill.source_actor_ref == "PSID1"


async def test_page_authored_echo_uses_page_actor_ref():
    draft = await handle_facebook_pages(
        {
            "source": "webhook",
            "page_id": "PAGE1",
            "sender": {"id": "PAGE1"},
            "recipient": {"id": "PSID1"},
            "timestamp": 1_704_067_200_000,
            "message": {"mid": "m_echo", "text": "reply", "is_echo": True},
        },
        {},
    )

    assert draft.source_actor_ref == "PAGE1"
    assert draft.content["is_echo"] is True
