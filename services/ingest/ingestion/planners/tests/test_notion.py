"""Tests for services/ingest/ingestion/planners/notion.py (IN-14)."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.notion import (
    SHARD_KIND_DATABASE,
    SHARD_KIND_PAGE_TREE,
    plan_shards_notion,
)


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, **f):
        self._f = f

    def __getitem__(self, k):
        return self._f[k]


class _FakeNotionClient:
    def __init__(self, databases):
        self._databases = databases
        self.search_calls = 0

    async def search(self, *, object_filter=None, start_cursor=None, page_size=100):
        self.search_calls += 1
        assert object_filter == "database"
        return (self._databases, None, False)


def _ctx(client):
    return PlannerContext(
        tenant_id=uuid4(),
        install=_FakeRecord(installation_id="ws-1"),
        conn=None,  # planner doesn't touch the DB
        source_client=client,
    )


async def test_one_shard_per_database_plus_page_tree():
    client = _FakeNotionClient([
        {"id": "db-a", "last_edited_time": "2025-03-01T00:00:00.000Z"},
        {"id": "db-b", "last_edited_time": "2025-01-01T00:00:00.000Z"},
    ])
    shards = await plan_shards_notion(_ctx(client))
    db_shards = [s for s in shards if s.shard_kind == SHARD_KIND_DATABASE]
    tree_shards = [s for s in shards if s.shard_kind == SHARD_KIND_PAGE_TREE]
    assert len(db_shards) == 2
    assert len(tree_shards) == 1
    assert {s.shard_identifier["database_id"] for s in db_shards} == {"db-a", "db-b"}
    assert all(s.shard_identifier["workspace_id"] == "ws-1" for s in db_shards)


async def test_recent_database_scores_higher():
    client = _FakeNotionClient([
        {"id": "recent", "last_edited_time": "2025-12-01T00:00:00.000Z"},
        {"id": "old", "last_edited_time": "2020-01-01T00:00:00.000Z"},
    ])
    shards = await plan_shards_notion(_ctx(client))
    by_id = {
        s.shard_identifier["database_id"]: s.recency_score
        for s in shards if s.shard_kind == SHARD_KIND_DATABASE
    }
    assert by_id["recent"] > by_id["old"]


async def test_missing_source_client_raises():
    ctx = PlannerContext(
        tenant_id=uuid4(),
        install=_FakeRecord(installation_id="ws-1"),
        conn=None,
        source_client=None,
    )
    with pytest.raises(RuntimeError):
        await plan_shards_notion(ctx)


async def test_dispatch_wired():
    assert resolve_planner("notion") is plan_shards_notion
