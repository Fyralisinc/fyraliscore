"""Tests for services/ingest/ingestion/fetchers/miro.py (whiteboard / design)."""
from __future__ import annotations

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.fetchers import miro as miro_fetcher
from services.ingest.ingestion.fetchers.miro import (
    MiroCursor,
    SHARD_KIND_BOARD_ITEMS,
    fetch_page_miro,
)


pytestmark = pytest.mark.asyncio


_BOARD = "board-design"
_ORG = "org-1"
_CURSOR_PREFIX = "miro-cursor:"


class _FakeClient:
    """Implements the MiroClient read surface the fetcher uses, with opaque
    cursor pagination."""

    def __init__(self, board, full, delta, page_size=50):
        self._board = board
        self._full = full
        self._delta = delta
        self._page_size = page_size
        self.calls: list[dict] = []

    async def get_board(self, board_id):
        return self._board

    async def list_items(self, board_id, *, limit=50, cursor=None):
        self.calls.append({"cursor": cursor, "limit": limit})
        pool = self._delta if cursor == "delta" else self._full
        offset = 0
        if cursor and cursor.startswith(_CURSOR_PREFIX):
            offset = int(cursor[len(_CURSOR_PREFIX):])
        cap = min(limit, self._page_size)
        page = pool[offset:offset + cap]
        next_offset = offset + len(page)
        is_last = next_offset >= len(pool) or not page
        next_cursor = None if is_last else f"{_CURSOR_PREFIX}{next_offset}"
        return page, next_cursor, len(pool)


class _FakeInst:
    _d = {"base_url": "https://api.miro.com/v2", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(miro_fetcher, "_open_miro_client", _open)


def _item(iid, modified, version="1"):
    return {"id": iid, "boardId": _BOARD, "type": "sticky_note",
            "data": {"content": f"item {iid}"}, "version": version,
            "createdAt": modified, "modifiedAt": modified}


async def test_full_backfill_emits_one_record_per_item(monkeypatch):
    board = {"id": _BOARD, "name": "Roadmap", "type": "board"}
    items = [
        _item("i1", "2026-05-01T00:00:00Z"),
        _item("i2", "2026-05-02T00:00:00Z"),
    ]
    client = _FakeClient(board, items, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_BOARD_ITEMS, "board_id": _BOARD,
             "org_id": _ORG}
    res = await fetch_page_miro(_FakeInst(), shard, None)

    # No board snapshot — exactly one record per item.
    kinds = [r["_fyralis_record_type"] for r in res.records]
    assert kinds == ["item", "item"]
    assert all(r["_fyralis_org_id"] == _ORG for r in res.records)
    assert res.end_of_data is True
    cur = MiroCursor.model_validate(res.next_cursor)
    assert cur.seeded is True
    assert cur.high_water_modified == "2026-05-02T00:00:00Z"


async def test_four_items_yield_four_records(monkeypatch):
    """Fixture invariant: a 4-item board produces exactly 4 backfill records."""
    board = {"id": _BOARD, "type": "board"}
    items = [_item(f"i{n}", f"2026-05-0{n + 1}T00:00:00Z") for n in range(4)]
    client = _FakeClient(board, items, [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_BOARD_ITEMS, "board_id": _BOARD, "org_id": _ORG}
    res = await fetch_page_miro(_FakeInst(), shard, None)
    item_records = [r for r in res.records if r["_fyralis_record_type"] == "item"]
    assert len(item_records) == 4


async def test_opaque_cursor_pagination(monkeypatch):
    """Two-page walk: the fetcher round-trips the opaque cursor verbatim."""
    board = {"id": _BOARD, "type": "board"}
    items = [_item(f"i{n}", f"2026-05-0{n + 1}T00:00:00Z") for n in range(4)]
    client = _FakeClient(board, items, [], page_size=2)
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_BOARD_ITEMS, "board_id": _BOARD, "org_id": _ORG}

    res1 = await fetch_page_miro(_FakeInst(), shard, None)
    assert res1.end_of_data is False
    assert len([r for r in res1.records if r["_fyralis_record_type"] == "item"]) == 2

    res2 = await fetch_page_miro(_FakeInst(), shard, res1.next_cursor)
    assert res2.end_of_data is True
    assert len([r for r in res2.records if r["_fyralis_record_type"] == "item"]) == 2
    # The second list_items call carried the opaque cursor from page 1.
    assert client.calls[1]["cursor"] == f"{_CURSOR_PREFIX}2"


async def test_warm_start_sets_incremental_floor(monkeypatch):
    board = {"id": _BOARD, "type": "board"}
    items = [_item("i1", "2026-05-10T00:00:00Z", version="2")]
    client = _FakeClient(board, items, [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_BOARD_ITEMS, "board_id": _BOARD,
             "org_id": _ORG, "item_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_miro(_FakeInst(), shard, None)
    cur = MiroCursor.model_validate(res.next_cursor)
    assert cur.incremental_floor == "2026-05-09T00:00:00Z"
    item_records = [r for r in res.records if r["_fyralis_record_type"] == "item"]
    assert len(item_records) == 1


async def test_empty_board_terminates(monkeypatch):
    client = _FakeClient({"id": _BOARD, "type": "board"}, [], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_BOARD_ITEMS, "board_id": _BOARD, "org_id": _ORG}
    res = await fetch_page_miro(_FakeInst(), shard, None)
    # No items, no board snapshot -> zero records, terminal.
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_board_id_is_noop(monkeypatch):
    client = _FakeClient({}, [], [])
    _wire(monkeypatch, client)
    res = await fetch_page_miro(_FakeInst(), {"shard_kind": SHARD_KIND_BOARD_ITEMS}, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_retry_later_propagates_without_cursor_advance(monkeypatch):
    class _RateLimitedClient:
        async def list_items(self, board_id, *, limit=50, cursor=None):
            raise RetryLater.after(
                request_context=RequestContext(
                    source="miro",
                    operation="board_items.list",
                ),
                delay_seconds=60,
                reason=RetryReason.RATE_LIMIT,
            )

    cursor = MiroCursor(
        page_cursor="cursor-1",
        high_water_modified="2026-05-01T00:00:00Z",
        incremental_floor="2026-05-01T00:00:00Z",
        items_seen=5,
        seeded=True,
    ).model_dump(mode="json")
    original_cursor = dict(cursor)
    _wire(monkeypatch, _RateLimitedClient())

    with pytest.raises(RetryLater):
        await fetch_page_miro(
            _FakeInst(),
            {
                "shard_kind": SHARD_KIND_BOARD_ITEMS,
                "board_id": _BOARD,
                "org_id": _ORG,
            },
            cursor,
        )

    assert cursor == original_cursor
