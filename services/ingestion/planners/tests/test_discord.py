"""Tests for services/ingestion/planners/discord.py (M6.6)."""
from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext
from services.ingestion.planners.discord import (
    SAMPLING_RATE, SAMPLING_VERSION,
    SHARD_KIND_CHANNEL_WINDOW,
    plan_shards_discord,
)


pytestmark = pytest.mark.asyncio


class _FakeRec:
    def __init__(self, **f):
        self._f = f
    def __getitem__(self, k):
        return self._f[k]


class _FakeDiscordClient:
    def __init__(self, guilds, channels_by_guild):
        self.guilds = guilds
        self.channels_by_guild = channels_by_guild

    async def list_guilds(self):
        return self.guilds

    async def list_guild_channels(self, guild_id):
        return self.channels_by_guild.get(guild_id, [])


def _ctx(tenant_id, guilds, channels):
    install = _FakeRec(id=uuid4(), tenant_id=tenant_id, provider="discord",
                       installation_id="bot-app", enabled=True)
    return PlannerContext(
        tenant_id=tenant_id, install=install, conn=None,
        source_client=_FakeDiscordClient(guilds, channels),
    )


async def test_samples_approx_5_percent():
    tid = uuid4()
    # 100 channels in one guild; expected sample = 5.
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(100)]
    ctx = _ctx(tid, [{"id": "g1"}], {"g1": channels})
    shards = await plan_shards_discord(ctx)
    # 5% of 100 = 5 (max(1, int(100*0.05)) = 5).
    assert len(shards) == 5
    assert all(s.shard_kind == SHARD_KIND_CHANNEL_WINDOW for s in shards)
    assert all(s.shard_identifier["is_sampled"] is True for s in shards)


async def test_sampling_deterministic_per_tenant():
    tid = uuid4()
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(50)]
    ctx_a = _ctx(tid, [{"id": "g1"}], {"g1": channels})
    ctx_b = _ctx(tid, [{"id": "g1"}], {"g1": list(channels)})
    sa = await plan_shards_discord(ctx_a)
    sb = await plan_shards_discord(ctx_b)
    ids_a = {s.shard_identifier["channel_id"] for s in sa}
    ids_b = {s.shard_identifier["channel_id"] for s in sb}
    assert ids_a == ids_b


async def test_sampling_differs_across_tenants():
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(100)]
    ctx_a = _ctx(uuid4(), [{"id": "g1"}], {"g1": channels})
    ctx_b = _ctx(uuid4(), [{"id": "g1"}], {"g1": list(channels)})
    sa = await plan_shards_discord(ctx_a)
    sb = await plan_shards_discord(ctx_b)
    # With 100 channels × 5 samples each, near-certain to differ.
    ids_a = {s.shard_identifier["channel_id"] for s in sa}
    ids_b = {s.shard_identifier["channel_id"] for s in sb}
    assert ids_a != ids_b


async def test_non_text_channels_filtered():
    """Discord channel type != 0 should be excluded from sampling pool."""
    channels = [
        {"id": "text", "type": 0, "name": "text"},
        {"id": "voice", "type": 2, "name": "voice"},
        {"id": "thread", "type": 11, "name": "thread"},
    ]
    ctx = _ctx(uuid4(), [{"id": "g1"}], {"g1": channels})
    shards = await plan_shards_discord(ctx)
    # Only the text channel can be sampled. max(1, int(1*0.05)) = 1.
    assert len(shards) == 1
    assert shards[0].shard_identifier["channel_id"] == "text"


async def test_sampling_version_in_identifier():
    channels = [{"id": "c1", "type": 0}]
    ctx = _ctx(uuid4(), [{"id": "g1"}], {"g1": channels})
    shards = await plan_shards_discord(ctx)
    assert shards[0].shard_identifier["sampling_version"] == SAMPLING_VERSION


async def test_empty_guilds_returns_empty():
    ctx = _ctx(uuid4(), [], {})
    assert await plan_shards_discord(ctx) == []


async def test_missing_source_client_raises():
    install = _FakeRec(id=uuid4(), tenant_id=uuid4(),
                       provider="discord", installation_id="bot", enabled=True)
    ctx = PlannerContext(tenant_id=uuid4(), install=install, conn=None,
                         source_client=None)
    with pytest.raises(RuntimeError, match="source_client=None"):
        await plan_shards_discord(ctx)


async def test_dispatch_wired():
    assert PLANNER_DISPATCH["discord"] is plan_shards_discord
