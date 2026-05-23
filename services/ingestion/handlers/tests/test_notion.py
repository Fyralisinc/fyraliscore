"""Tests for services/ingestion/handlers/notion.py (IN-14)."""
from __future__ import annotations

import pytest

from services.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingestion.handlers.notion import handle_notion_object


pytestmark = pytest.mark.asyncio


def _page(**over):
    base = {
        "object": "page",
        "id": "page-1",
        "last_edited_time": "2025-03-01T12:00:00.000Z",
        "created_time": "2025-02-01T00:00:00.000Z",
        "last_edited_by": {"object": "user", "id": "user-9"},
        "url": "https://notion.so/page-1",
        "parent": {"type": "database_id", "database_id": "db-1"},
        "properties": {
            "Name": {
                "type": "title",
                "title": [{"plain_text": "Ship rate limiter"}],
            },
            "Status": {"type": "status", "status": {"name": "In progress"}},
            "Blocks": {
                "type": "relation",
                "relation": [{"id": "page-2"}],
            },
        },
        "_fyralis_workspace_id": "ws-1",
    }
    base.update(over)
    return base


async def test_page_database_row_is_state_change():
    draft = await handle_notion_object(_page(), {})
    assert draft.source_channel == "notion:object"
    assert draft.trust_tier == "attested_agent"
    assert draft.kind == "state_change"  # in a DB + has a status property
    assert draft.external_id == "notion:page:page-1"
    assert draft.content["object_type"] == "page"
    assert draft.content["title"] == "Ship rate limiter"
    assert draft.content["in_database"] is True
    assert draft.source_actor_ref == "notion:user-9"
    # occurred_at comes from last_edited_time, not created_time.
    assert draft.occurred_at.isoformat().startswith("2025-03-01T12:00:00")
    # entity hints include the page, the database, and the relation edge.
    ids = {(e["type"], e["id"]) for e in draft.entities_hint}
    assert ("notion_page", "page-1") in ids
    assert ("notion_database", "db-1") in ids
    assert ("notion_page", "page-2") in ids


async def test_loose_page_without_status_is_signal():
    page = _page(
        parent={"type": "workspace", "workspace": True},
        properties={"title": {"type": "title", "title": [{"plain_text": "Notes"}]}},
    )
    draft = await handle_notion_object(page, {})
    assert draft.kind == "signal"
    assert draft.content["in_database"] is False


async def test_block_carries_text_and_truncation_marker():
    block = {
        "object": "block",
        "id": "block-7",
        "type": "paragraph",
        "last_edited_time": "2025-03-02T00:00:00.000Z",
        "paragraph": {"rich_text": [{"plain_text": "Hello "}, {"plain_text": "world"}]},
        "_fyralis_truncated": {"reason": "depth_cap", "depth": 3},
        "_fyralis_workspace_id": "ws-1",
    }
    draft = await handle_notion_object(block, {})
    assert draft.external_id == "notion:block:block-7"
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "block"
    assert draft.content["text"] == "Hello world"
    assert draft.content["_truncated"] == {"reason": "depth_cap", "depth": 3}


async def test_comment_uses_created_time_and_actor():
    comment = {
        "object": "comment",
        "id": "cmt-3",
        "created_time": "2025-03-03T09:00:00.000Z",
        "created_by": {"object": "user", "id": "user-2"},
        "parent": {"type": "page_id", "page_id": "page-1"},
        "rich_text": [{"plain_text": "LGTM"}],
    }
    draft = await handle_notion_object(comment, {})
    assert draft.external_id == "notion:comment:cmt-3"
    assert draft.content["object_type"] == "comment"
    assert draft.content["text"] == "LGTM"
    assert draft.source_actor_ref == "notion:user-2"
    assert draft.occurred_at.isoformat().startswith("2025-03-03T09:00:00")
    ids = {(e["type"], e["id"]) for e in draft.entities_hint}
    assert ("notion_page", "page-1") in ids


async def test_unsupported_object_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_notion_object({"object": "user", "id": "u1"}, {})


async def test_external_id_stable_across_calls():
    """Backfill + poll twins must derive the same external_id (dedup)."""
    a = await handle_notion_object(_page(), {})
    b = await handle_notion_object(_page(), {})
    assert a.external_id == b.external_id == "notion:page:page-1"


async def test_handler_registered_under_notion_object():
    assert get_handler("notion:object") is handle_notion_object
    assert CHANNEL_TRUST_MAP["notion:object"] == "attested_agent"
