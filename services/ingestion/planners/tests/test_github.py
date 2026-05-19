"""Tests for services/ingestion/planners/github.py (M6.4)."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext
from services.ingestion.planners.github import (
    EVENT_TYPES,
    SHARD_KIND_REPO_EVENTS,
    plan_shards_github,
)


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, **fields):
        self._fields = fields

    def __getitem__(self, key):
        return self._fields[key]


class _FakeGithubClient:
    def __init__(self, repos):
        self.repos = repos
        self.calls = []

    async def list_installation_repositories(self, installation_id):
        self.calls.append(installation_id)
        return self.repos


def _ctx(repos=None, *, all_mode=False):
    install = _FakeRecord(
        id=uuid4(), tenant_id=uuid4(),
        provider="github", installation_id="42", enabled=True,
    )
    client = _FakeGithubClient(repos=None if all_mode else (repos or []))
    return PlannerContext(
        tenant_id=uuid4(), install=install, conn=None,
        source_client=client,
    )


async def test_one_shard_per_repo_event_type():
    ctx = _ctx(repos=["acme/api", "acme/web"])
    shards = await plan_shards_github(ctx)
    # 2 repos × 2 event_types = 4 shards.
    assert len(shards) == 2 * len(EVENT_TYPES)
    assert {s.shard_identifier["event_type"] for s in shards} == set(EVENT_TYPES)
    assert {s.shard_identifier["repo_full_name"] for s in shards} == {
        "acme/api", "acme/web",
    }


async def test_shard_kind_mirrored_into_identifier():
    ctx = _ctx(repos=["acme/api"])
    shards = await plan_shards_github(ctx)
    for s in shards:
        assert s.shard_kind == SHARD_KIND_REPO_EVENTS
        assert s.shard_identifier["shard_kind"] == SHARD_KIND_REPO_EVENTS


async def test_empty_repo_list_returns_empty():
    ctx = _ctx(repos=[])
    shards = await plan_shards_github(ctx)
    assert shards == []


async def test_all_repos_mode_raises_not_implemented():
    ctx = _ctx(all_mode=True)
    with pytest.raises(NotImplementedError, match="all-repositories mode"):
        await plan_shards_github(ctx)


async def test_missing_source_client_raises_runtime_error():
    install = _FakeRecord(
        id=uuid4(), tenant_id=uuid4(),
        provider="github", installation_id="42", enabled=True,
    )
    ctx = PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )
    with pytest.raises(RuntimeError, match="source_client=None"):
        await plan_shards_github(ctx)


async def test_invalid_repo_name_skipped():
    ctx = _ctx(repos=["valid/repo", "no-slash"])
    shards = await plan_shards_github(ctx)
    # Only the valid one produces shards.
    repos = {s.shard_identifier["repo_full_name"] for s in shards}
    assert repos == {"valid/repo"}


async def test_dispatch_table_wired():
    assert PLANNER_DISPATCH["github"] is plan_shards_github
