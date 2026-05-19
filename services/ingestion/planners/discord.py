"""services/ingestion/planners/discord.py — Discord backfill (M6.6).

Per A18 + A18.6 + LLD §3.4 (5% sparse sampling). One Shard per
sampled channel within each guild. Sampling is deterministic
per (tenant, sampling_version) so re-runs produce the same set.
"""
from __future__ import annotations

import logging
import random

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "discord_channel_window"
SAMPLING_VERSION = "v1"  # bump if changing the algorithm
SAMPLING_RATE = 0.05


def _sampled_channels(
    tenant_id: str, channels: list[dict],
) -> list[dict]:
    """Deterministic per-guild 5% sample.

    Seed = (tenant_id, SAMPLING_VERSION). Same tenant + same channel
    universe → same sampled set across runs.
    """
    if not channels:
        return []
    seed = hash((tenant_id, SAMPLING_VERSION))
    rng = random.Random(seed)
    k = max(1, int(len(channels) * SAMPLING_RATE))
    return rng.sample(channels, k=min(k, len(channels)))


async def plan_shards_discord(ctx: PlannerContext) -> list[Shard]:
    """One Shard per sampled channel across all guilds."""
    if ctx.source_client is None:
        raise RuntimeError(
            "Discord planner: source_client=None. The PlannerContext "
            "factory must supply a DiscordClient."
        )
    guilds = await ctx.source_client.list_guilds()
    install_id = str(ctx.install["installation_id"])
    tenant_str = str(ctx.tenant_id)
    shards: list[Shard] = []
    for guild in guilds:
        guild_id = guild.get("id")
        if not guild_id:
            continue
        channels = await ctx.source_client.list_guild_channels(guild_id)
        # Filter to text channels only (Discord type 0 = GUILD_TEXT).
        text_channels = [c for c in channels if c.get("type") == 0]
        sampled = _sampled_channels(tenant_str, text_channels)
        for ch in sampled:
            shards.append(Shard(
                shard_kind=SHARD_KIND_CHANNEL_WINDOW,
                shard_identifier={
                    "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
                    "guild_id": guild_id,
                    "channel_id": ch.get("id"),
                    "channel_name": ch.get("name"),
                    "is_sampled": True,
                    "sampling_version": SAMPLING_VERSION,
                    "installation_id": install_id,
                },
                recency_score=1.0,
            ))
    return shards


PLANNER_DISPATCH["discord"] = plan_shards_discord


__all__ = [
    "SAMPLING_RATE", "SAMPLING_VERSION",
    "SHARD_KIND_CHANNEL_WINDOW",
    "plan_shards_discord",
]
