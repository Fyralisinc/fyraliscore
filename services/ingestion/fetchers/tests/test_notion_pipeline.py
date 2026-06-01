"""End-to-end Notion pipeline (IN-14): planner → fetcher → channel route →
handler → ObservationDraft. Exercises the full normalization contract for
Notion (minus Kafka/S3), proving a workspace install yields well-formed
observations for pages, blocks, and comments with stable external_ids.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingestion.fetchers import notion as nt
from services.ingestion.fetchers.notion import fetch_page_notion
from services.ingestion.handlers import get_handler
from services.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingestion.planners.context import PlannerContext
from services.ingestion.planners.notion import plan_shards_notion


pytestmark = pytest.mark.asyncio


class _Inst:
    def __getitem__(self, k):
        return {"installation_id": "ws-1"}.get(k, "row")


class _FakeNotionClient:
    """One database (db-1) with one row (p1); the row has one block and one
    comment. No loose pages."""

    async def search(self, *, object_filter=None, start_cursor=None, page_size=100):
        if object_filter == "database":
            return ([{"id": "db-1", "last_edited_time": "2025-03-01T00:00:00.000Z"}], None, False)
        return ([], None, False)  # no loose pages

    async def query_database(self, database_id, *, start_cursor=None, page_size=100):
        return ([{
            "object": "page", "id": "p1",
            "last_edited_time": "2025-03-01T00:00:00.000Z",
            "last_edited_by": {"id": "user-1"},
            "parent": {"type": "database_id", "database_id": database_id},
            "properties": {
                "Name": {"type": "title", "title": [{"plain_text": "Roadmap item"}]},
                "Status": {"type": "status", "status": {"name": "Done"}},
            },
        }], None, False)

    async def list_block_children(self, block_id, *, start_cursor=None, page_size=100):
        return ([{
            "object": "block", "id": "b1", "type": "paragraph", "has_children": False,
            "last_edited_time": "2025-03-01T00:00:00.000Z",
            "paragraph": {"rich_text": [{"plain_text": "ship it"}]},
        }], None, False)

    async def list_comments(self, block_id, *, start_cursor=None, page_size=100):
        return ([{
            "object": "comment", "id": "c1",
            "created_time": "2025-03-02T00:00:00.000Z",
            "created_by": {"id": "user-2"},
            "parent": {"type": "page_id", "page_id": "p1"},
            "rich_text": [{"plain_text": "done!"}],
        }], None, False)


async def test_workspace_install_yields_observations(monkeypatch):
    client = _FakeNotionClient()

    async def fake_open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(nt, "_open_notion_client", fake_open)

    # 1. Plan shards from the workspace.
    ctx = PlannerContext(
        tenant_id=uuid4(), install=_Inst(), conn=None, source_client=client,
    )
    shards = await plan_shards_notion(ctx)

    # 2. Drain each shard's fetcher; 3. route + handle each record.
    handler = get_handler(resolve_channel("notion", "backfill"))
    drafts = []
    for shard in shards:
        sid = shard.shard_identifier
        cursor, guard = None, 0
        while True:
            guard += 1
            assert guard < 50
            r = await fetch_page_notion(_Inst(), sid, cursor)
            for record in r.records:
                drafts.append(await handler(record, {}))
            cursor = r.next_cursor
            if r.end_of_data:
                break

    by_kind = {}
    for d in drafts:
        by_kind.setdefault(d.content["object_type"], []).append(d)

    # One page (Done status → state_change), one block, one comment.
    assert {d.external_id for d in by_kind["page"]} == {"notion:page:p1"}
    assert by_kind["page"][0].kind == "state_change"
    assert by_kind["page"][0].trust_tier == "attested_agent"
    assert {d.external_id for d in by_kind["block"]} == {"notion:block:b1"}
    assert {d.external_id for d in by_kind["comment"]} == {"notion:comment:c1"}
    # All route through the single notion:object channel.
    assert {d.source_channel for d in drafts} == {"notion:object"}


async def test_backfill_and_poll_twins_dedup_identically(monkeypatch):
    """A27.3 parity: the same object via backfill and via the poll re-run
    derives the SAME (channel, external_id) so the dedup index collapses
    them. Both ingress kinds resolve to the same channel + handler."""
    assert resolve_channel("notion", "backfill") == resolve_channel("notion", "poll")
    client = _FakeNotionClient()

    async def fake_open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(nt, "_open_notion_client", fake_open)

    handler = get_handler("notion:object")
    sid = {"shard_kind": "notion_database", "database_id": "db-1", "workspace_id": "ws-1"}
    # Fetch the same page twice (backfill, then a poll re-run).
    r = await fetch_page_notion(_Inst(), sid, None)
    page = next(rec for rec in r.records if rec["object"] == "page")
    d1 = await handler(page, {})
    d2 = await handler(page, {})
    assert (d1.source_channel, d1.external_id) == (d2.source_channel, d2.external_id)
