"""Tests for services/ingest/ingestion/planners/discord.py (M6.6)."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.discord import (
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
    def __init__(
        self,
        guilds,
        channels_by_guild,
        *,
        active_threads_by_guild=None,
        archived_threads_by_channel=None,
    ):
        self.guilds = guilds
        self.channels_by_guild = channels_by_guild
        self.active_threads_by_guild = active_threads_by_guild or {}
        self.archived_threads_by_channel = archived_threads_by_channel or {}

    async def list_guilds(self):
        return self.guilds

    async def list_guild_channels(self, guild_id):
        return self.channels_by_guild.get(guild_id, [])

    async def list_active_guild_threads(self, guild_id):
        return self.active_threads_by_guild.get(guild_id, [])

    async def list_channel_archived_threads(self, channel_id, *, archive_kind):
        return self.archived_threads_by_channel.get((channel_id, archive_kind), [])


def _ctx(
    tenant_id,
    guilds,
    channels,
    *,
    active_threads=None,
    archived_threads=None,
):
    install_id = str(guilds[0]["id"]) if guilds else "bot-app"
    install = _FakeRec(id=uuid4(), tenant_id=tenant_id, provider="discord",
                       installation_id=install_id, enabled=True)
    return PlannerContext(
        tenant_id=tenant_id, install=install, conn=None,
        source_client=_FakeDiscordClient(
            guilds,
            channels,
            active_threads_by_guild=active_threads,
            archived_threads_by_channel=archived_threads,
        ),
    )


async def test_reads_every_text_channel_regardless_server_size():
    tid = uuid4()
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(100)]
    ctx = _ctx(tid, [{"id": "g1"}], {"g1": channels})
    shards = await plan_shards_discord(ctx)
    assert len(shards) == 100
    assert all(s.shard_kind == SHARD_KIND_CHANNEL_WINDOW for s in shards)
    assert all("is_sampled" not in s.shard_identifier for s in shards)
    assert all("sampling_version" not in s.shard_identifier for s in shards)
    assert {s.shard_identifier["channel_id"] for s in shards} == {
        c["id"] for c in channels
    }


async def test_installed_guild_scope_only():
    tid = uuid4()
    ctx = _ctx(
        tid,
        [{"id": "installed"}, {"id": "other"}],
        {
            "installed": [{"id": "c-installed", "name": "general", "type": 0}],
            "other": [{"id": "c-other", "name": "other", "type": 0}],
        },
    )

    shards = await plan_shards_discord(ctx)

    assert [s.shard_identifier["guild_id"] for s in shards] == ["installed"]
    assert [s.shard_identifier["channel_id"] for s in shards] == ["c-installed"]


async def test_full_coverage_deterministic_per_tenant():
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


async def test_full_coverage_order_independent():
    """Same channel universe in a different order -> same selected set."""
    tid = uuid4()
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(40)]
    ctx_a = _ctx(tid, [{"id": "g1"}], {"g1": channels})
    ctx_b = _ctx(tid, [{"id": "g1"}], {"g1": list(reversed(channels))})
    sa = await plan_shards_discord(ctx_a)
    sb = await plan_shards_discord(ctx_b)
    assert {s.shard_identifier["channel_id"] for s in sa} == {
        s.shard_identifier["channel_id"] for s in sb
    }


async def test_full_coverage_same_across_tenants():
    channels = [{"id": f"c{i}", "name": f"chan{i}", "type": 0}
                for i in range(100)]
    ctx_a = _ctx(uuid4(), [{"id": "g1"}], {"g1": channels})
    ctx_b = _ctx(uuid4(), [{"id": "g1"}], {"g1": list(channels)})
    sa = await plan_shards_discord(ctx_a)
    sb = await plan_shards_discord(ctx_b)
    ids_a = {s.shard_identifier["channel_id"] for s in sa}
    ids_b = {s.shard_identifier["channel_id"] for s in sb}
    assert ids_a == ids_b == {c["id"] for c in channels}


async def test_non_message_channels_filtered():
    """Voice/category parents are excluded; announcement/thread streams remain."""
    channels = [
        {"id": "text", "type": 0, "name": "text"},
        {"id": "announcements", "type": 5, "name": "announcements"},
        {"id": "voice", "type": 2, "name": "voice"},
        {"id": "thread", "type": 11, "name": "thread", "parent_id": "text"},
    ]
    ctx = _ctx(uuid4(), [{"id": "g1"}], {"g1": channels})
    shards = await plan_shards_discord(ctx)
    assert {shard.shard_identifier["channel_id"] for shard in shards} == {
        "text",
        "announcements",
        "thread",
    }


async def test_forum_media_and_archived_threads_become_shards():
    """Forum/media parents are containers; their post threads are fetched."""
    channels = [
        {"id": "general", "type": 0, "name": "general"},
        {"id": "news", "type": 5, "name": "news"},
        {"id": "forum", "type": 15, "name": "forum"},
        {"id": "media", "type": 16, "name": "media"},
    ]
    active_threads = {
        "g1": [
            {"id": "active-public", "type": 11, "name": "active", "parent_id": "forum"},
            {"id": "active-private", "type": 12, "name": "private", "parent_id": "general"},
        ]
    }
    archived_threads = {
        ("news", "public"): [
            {"id": "archived-announcement", "type": 10, "name": "older-news", "parent_id": "news"}
        ],
        ("forum", "public"): [
            {"id": "forum-post", "type": 11, "name": "forum-post", "parent_id": "forum"}
        ],
        ("media", "public"): [
            {"id": "media-post", "type": 11, "name": "media-post", "parent_id": "media"}
        ],
    }
    ctx = _ctx(
        uuid4(),
        [{"id": "g1"}],
        {"g1": channels},
        active_threads=active_threads,
        archived_threads=archived_threads,
    )

    shards = await plan_shards_discord(ctx)

    assert {shard.shard_identifier["channel_id"] for shard in shards} == {
        "general",
        "news",
        "active-public",
        "active-private",
        "archived-announcement",
        "forum-post",
        "media-post",
    }
    assert "forum" not in {
        shard.shard_identifier["channel_id"] for shard in shards
    }
    thread = next(
        shard for shard in shards
        if shard.shard_identifier["channel_id"] == "forum-post"
    )
    assert thread.shard_identifier["parent_id"] == "forum"


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
    assert resolve_planner("discord") is plan_shards_discord
