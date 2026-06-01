"""Tests for services/ingest/ingestion/fetchers/notion.py (IN-14)."""
from __future__ import annotations

import pytest

from lib.shared.errors import NotionApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import notion as nt
from services.ingest.ingestion.fetchers.notion import (
    SHARD_KIND_DATABASE,
    SHARD_KIND_PAGE_TREE,
    NotionCursor,
    fetch_page_notion,
)
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel


pytestmark = pytest.mark.asyncio


class _FakeInst:
    def __getitem__(self, k):
        return "row"


class _FakeNotionClient:
    """Deterministic fake. `rate_limit_once` raises a 429 on the first
    query_database call then serves normally."""

    def __init__(self, *, rate_limit_once=False, block_has_children=False):
        self.rate_limited = rate_limit_once
        self.block_has_children = block_has_children
        self.calls: list[str] = []

    async def query_database(self, database_id, *, start_cursor=None, page_size=100):
        self.calls.append("query_database")
        if self.rate_limited:
            self.rate_limited = False
            raise NotionApiError("429", code="notion_api_rate_limited",
                                 context={"http_status": 429})
        return ([{
            "object": "page", "id": "p1",
            "last_edited_time": "2025-03-01T00:00:00.000Z",
            "parent": {"type": "database_id", "database_id": database_id},
            "properties": {},
        }], None, False)

    async def search(self, *, object_filter=None, start_cursor=None, page_size=100):
        self.calls.append("search")
        return ([
            {"object": "page", "id": "loose1",
             "last_edited_time": "2025-03-01T00:00:00.000Z",
             "parent": {"type": "workspace", "workspace": True}},
            {"object": "page", "id": "dbrow",
             "parent": {"type": "database_id", "database_id": "db1"}},  # skipped
        ], None, False)

    async def list_block_children(self, block_id, *, start_cursor=None, page_size=100):
        self.calls.append("list_block_children")
        return ([{
            "object": "block", "id": "b1", "type": "paragraph",
            "has_children": self.block_has_children,
            "last_edited_time": "2025-03-01T00:00:00.000Z",
            "paragraph": {"rich_text": [{"plain_text": "hi"}]},
        }], None, False)

    async def list_comments(self, block_id, *, start_cursor=None, page_size=100):
        self.calls.append("list_comments")
        return ([{
            "object": "comment", "id": "c1",
            "created_time": "2025-03-01T00:00:00.000Z",
            "rich_text": [{"plain_text": "nice"}],
        }], None, False)


def _patch(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(nt, "_open_notion_client", fake_open)


async def _drain(monkeypatch, fake, shard_id):
    """Run the fetcher to completion, returning all records emitted."""
    _patch(monkeypatch, fake)
    records, cursor, guard = [], None, 0
    while True:
        guard += 1
        assert guard < 50, "fetch loop did not terminate"
        r = await fetch_page_notion(_FakeInst(), shard_id, cursor)
        records.extend(r.records)
        cursor = r.next_cursor
        if r.end_of_data:
            break
    return records


async def test_database_shard_walks_rows_blocks_comments(monkeypatch):
    fake = _FakeNotionClient()
    shard = {"shard_kind": SHARD_KIND_DATABASE, "database_id": "db1", "workspace_id": "w1"}
    records = await _drain(monkeypatch, fake, shard)
    objs = [r["object"] for r in records]
    assert objs.count("page") == 1
    assert objs.count("block") == 1
    assert objs.count("comment") == 1
    # workspace id injected on every record for entity grounding.
    assert all(r.get("_fyralis_workspace_id") == "w1" for r in records)
    assert fake.calls == ["query_database", "list_block_children", "list_comments"]


async def test_page_tree_shard_skips_database_rows(monkeypatch):
    fake = _FakeNotionClient()
    shard = {"shard_kind": SHARD_KIND_PAGE_TREE, "workspace_id": "w1"}
    records = await _drain(monkeypatch, fake, shard)
    page_ids = [r["id"] for r in records if r["object"] == "page"]
    assert page_ids == ["loose1"]  # "dbrow" (database row) was skipped


async def test_rate_limit_repushes_item_and_preserves_cursor(monkeypatch):
    fake = _FakeNotionClient(rate_limit_once=True)
    _patch(monkeypatch, fake)
    shard = {"shard_kind": SHARD_KIND_DATABASE, "database_id": "db1", "workspace_id": "w1"}
    r1 = await fetch_page_notion(_FakeInst(), shard, None)
    assert r1.records == []
    assert r1.end_of_data is False
    # the db_rows work item is still on the stack for the next tick.
    assert any(it["kind"] == "db_rows" for it in r1.next_cursor["stack"])
    # next tick succeeds and the walk proceeds.
    r2 = await fetch_page_notion(_FakeInst(), shard, r1.next_cursor)
    assert any(rec["object"] == "page" for rec in r2.records)


async def test_depth_cap_stamps_truncation_marker(monkeypatch):
    monkeypatch.setenv("NOTION_BLOCK_DEPTH_CAP", "1")
    fake = _FakeNotionClient(block_has_children=True)
    shard = {"shard_kind": SHARD_KIND_DATABASE, "database_id": "db1", "workspace_id": "w1"}
    records = await _drain(monkeypatch, fake, shard)
    blocks = [r for r in records if r["object"] == "block"]
    assert blocks and blocks[0]["_fyralis_truncated"] == {"reason": "depth_cap", "depth": 1}


async def test_empty_stack_is_end_of_data(monkeypatch):
    fake = _FakeNotionClient()
    _patch(monkeypatch, fake)
    # a cursor with an empty seeded stack ⇒ terminal.
    cur = NotionCursor(seeded=True, stack=[]).model_dump(mode="json")
    r = await fetch_page_notion(
        _FakeInst(), {"shard_kind": SHARD_KIND_DATABASE, "database_id": "d"}, cur,
    )
    assert r.records == [] and r.end_of_data is True


async def test_cursor_strict():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        NotionCursor.model_validate({"stack": [], "bogus": 1})


async def test_dispatch_and_routing_wired():
    assert FETCHER_DISPATCH["notion"] is fetch_page_notion
    assert resolve_channel("notion", "backfill") == "notion:object"
    assert resolve_channel("notion", "poll") == "notion:object"


async def test_real_client_opener_is_importable():
    """Real-path guard: the worker resolves the client via
    _clients.open_notion_client (the unit tests monkeypatch the seam, so
    this asserts the production opener actually exists)."""
    from services.ingest.ingestion.fetchers._clients import (  # noqa: F401
        build_notion_client,
        open_notion_client,
    )
