"""Tests for services/ingest/ingestion/handlers/miro.py (whiteboard / design)."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.miro import handle_miro_item


pytestmark = pytest.mark.asyncio


_ORG = "org-1"
_BOARD = "board-design"


def _item(**over):
    base = {
        "id": "i-1001",
        "boardId": _BOARD,
        "type": "sticky_note",
        "data": {"content": "Ship onboarding"},
        "createdBy": {"id": "user-1", "type": "user"},
        "createdAt": "2026-05-20T12:30:00.000Z",
        "modifiedAt": "2026-05-20T12:30:00.000Z",
        "version": "1",
    }
    base.update(over)
    return {"_fyralis_record_type": "item", "_fyralis_org_id": _ORG,
            "_fyralis_board_id": _BOARD, "item": base}


async def test_handler_registered():
    assert get_handler("miro:item") is handle_miro_item
    assert CHANNEL_TRUST_MAP["miro:item"] == "authoritative"


async def test_item_is_signal_with_versioned_external_id():
    draft = await handle_miro_item(_item(), {})
    assert draft.source_channel == "miro:item"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id versioned by version so an edit lands as a new obs, and
    # namespaced by org so two tenants stay distinct.
    assert draft.external_id == f"miro:{_ORG}:item:i-1001:1"
    assert draft.content["object_type"] == "item"
    assert draft.content["board_id"] == _BOARD
    assert "Ship onboarding" in draft.content_text


async def test_item_edit_produces_distinct_external_id():
    """Mutable-source dedup lesson: an edit (version bump) must NOT collapse onto
    the earlier observation."""
    v1 = await handle_miro_item(_item(version="1"), {})
    v2 = await handle_miro_item(_item(version="2",
                                      modifiedAt="2026-05-21T08:00:00.000Z"), {})
    assert v1.external_id != v2.external_id
    assert v2.external_id == f"miro:{_ORG}:item:i-1001:2"


async def test_org_namespacing_keeps_tenants_distinct():
    a = await handle_miro_item(_item(), {})
    other = await handle_miro_item({
        "_fyralis_record_type": "item", "_fyralis_org_id": "org-2",
        "_fyralis_board_id": _BOARD,
        "item": {"id": "i-1001", "boardId": _BOARD, "type": "sticky_note",
                 "version": "1", "modifiedAt": "2026-05-20T12:30:00.000Z"},
    }, {})
    # Same board/item ids, different org -> different external_id.
    assert a.external_id != other.external_id


async def test_four_items_distinct_external_ids():
    """The fixture invariant: 4 board items -> 4 DISTINCT external_ids."""
    ids = set()
    for n in range(4):
        draft = await handle_miro_item(_item(id=f"i-{n}"), {})
        ids.add(draft.external_id)
    assert len(ids) == 4


async def test_retired_webhook_shape_is_rejected():
    with pytest.raises(ValidationError, match="tagged backfill/poll"):
        await handle_miro_item({
            "event": "board_item.created",
            "_fyralis_org_id": _ORG,
            "_fyralis_board_id": _BOARD,
            "item": {
                "id": "i-1001",
                "boardId": _BOARD,
                "type": "sticky_note",
            },
        }, {})


async def test_untagged_item_shape_is_rejected():
    with pytest.raises(ValidationError, match="tagged backfill/poll"):
        await handle_miro_item({"item": _item()["item"]}, {})


# --- rich-field ingestion ---------------------------------------------------

async def test_item_rich_fields_captured():
    draft = await handle_miro_item(_item(
        position={"x": 100.0, "y": 50.0},
        geometry={"width": 200.0, "height": 120.0},
        parent={"id": "frame-1"},
    ), {})
    c = draft.content
    assert c["created_by"] == "user-1"
    assert c["position"] == {"x": 100.0, "y": 50.0}
    assert c["parent_id"] == "frame-1"


async def test_extras_absent_keys_not_emitted():
    """A bare item must not bloat content with None-valued extras."""
    draft = await handle_miro_item({
        "_fyralis_record_type": "item", "_fyralis_org_id": _ORG,
        "_fyralis_board_id": _BOARD,
        "item": {"id": "i-9", "boardId": _BOARD, "type": "shape", "version": "1",
                 "modifiedAt": "2026-05-20T00:00:00.000Z"},
    }, {})
    assert "parent_id" not in draft.content
    assert "position" not in draft.content


async def test_unknown_payload_raises():
    with pytest.raises(ValidationError):
        await handle_miro_item({"foo": "bar"}, {})
