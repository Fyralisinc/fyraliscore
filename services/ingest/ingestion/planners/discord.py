"""services/ingest/ingestion/planners/discord.py — Discord backfill (M6.6).

One Shard per text channel within the installed guild. Discord itself
enforces private-channel access; channels the bot cannot read are
bounded skips in the fetcher rather than fatal source failures.
"""
from __future__ import annotations

import logging

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "discord_channel_window"
SAMPLING_VERSION = "v3-full-coverage"
COVERAGE_MODE = "full_text_channels"


def _selected_channels(channels: list[dict]) -> list[dict]:
    """Return every text channel in stable id order."""
    return sorted(channels, key=lambda c: str(c.get("id") or ""))


async def plan_shards_discord(ctx: PlannerContext) -> list[Shard]:
    """One Shard per text channel in the installed guild."""
    if ctx.source_client is None:
        raise RuntimeError(
            "Discord planner: source_client=None. The PlannerContext "
            "factory must supply a DiscordClient."
        )
    install_id = str(ctx.install["installation_id"])
    shards: list[Shard] = []
    if not install_id:
        return []
    channels = await ctx.source_client.list_guild_channels(install_id)
    # Filter to text channels only (Discord type 0 = GUILD_TEXT).
    text_channels = [c for c in channels if c.get("type") == 0]
    for ch in _selected_channels(text_channels):
        shards.append(Shard(
            shard_kind=SHARD_KIND_CHANNEL_WINDOW,
            shard_identifier={
                "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
                "guild_id": install_id,
                "channel_id": ch.get("id"),
                "channel_name": ch.get("name"),
                "is_sampled": False,
                "coverage": COVERAGE_MODE,
                "sampling_version": SAMPLING_VERSION,
                "installation_id": install_id,
            },
            recency_score=1.0,
        ))
    return shards


PLANNER_DISPATCH["discord"] = plan_shards_discord


__all__ = [
    "COVERAGE_MODE", "SAMPLING_VERSION", "SHARD_KIND_CHANNEL_WINDOW",
    "plan_shards_discord",
]
