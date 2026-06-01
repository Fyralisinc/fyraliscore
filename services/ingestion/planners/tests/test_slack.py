"""Tests for services/ingestion/planners/slack.py (M6.5)."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext
from services.ingestion.planners.slack import (
    SHARD_KIND_CHANNEL_WINDOW,
    plan_shards_slack,
)


pytestmark = pytest.mark.asyncio


class _FakeRec:
    def __init__(self, **f):
        self._f = f
    def __getitem__(self, k):
        return self._f[k]


class _FakeSlackClient:
    def __init__(self, channels):
        self.channels = channels
    async def conversations_list(self):
        return self.channels


def _ctx(channels):
    install = _FakeRec(
        id=uuid4(), tenant_id=uuid4(), provider="slack",
        installation_id="T-team", enabled=True,
    )
    return PlannerContext(
        tenant_id=uuid4(), install=install, conn=None,
        source_client=_FakeSlackClient(channels),
    )


async def test_one_shard_per_channel():
    ctx = _ctx([
        {"id": "C001", "name": "general", "team_id": "T-team"},
        {"id": "C002", "name": "random", "team_id": "T-team"},
    ])
    shards = await plan_shards_slack(ctx)
    assert len(shards) == 2
    assert {s.shard_identifier["channel_id"] for s in shards} == {"C001", "C002"}
    for s in shards:
        assert s.shard_kind == SHARD_KIND_CHANNEL_WINDOW
        assert s.shard_identifier["shard_kind"] == SHARD_KIND_CHANNEL_WINDOW


async def test_empty_channels_returns_empty():
    ctx = _ctx([])
    assert await plan_shards_slack(ctx) == []


async def test_missing_id_skipped():
    ctx = _ctx([{"name": "no-id"}, {"id": "C1"}])
    shards = await plan_shards_slack(ctx)
    assert {s.shard_identifier["channel_id"] for s in shards} == {"C1"}


async def test_missing_source_client_raises():
    install = _FakeRec(id=uuid4(), tenant_id=uuid4(),
                       provider="slack", installation_id="T", enabled=True)
    ctx = PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )
    with pytest.raises(RuntimeError, match="source_client=None"):
        await plan_shards_slack(ctx)


async def test_dispatch_wired():
    assert PLANNER_DISPATCH["slack"] is plan_shards_slack
