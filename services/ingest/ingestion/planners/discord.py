"""services/ingest/ingestion/planners/discord.py — Discord backfill (M6.6).

One Shard per message-readable stream within the installed guild: text and
announcement channels directly, plus active/archived threads and forum/media
post threads. Discord itself enforces private-channel access; streams the bot
cannot read are bounded skips in the fetcher rather than fatal source failures.
"""
from __future__ import annotations

import logging
from typing import Any

from lib.shared.errors import DiscordApiError
from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "discord_channel_window"

_GUILD_TEXT = 0
_GUILD_ANNOUNCEMENT = 5
_ANNOUNCEMENT_THREAD = 10
_PUBLIC_THREAD = 11
_PRIVATE_THREAD = 12
_GUILD_FORUM = 15
_GUILD_MEDIA = 16

_MESSAGE_CHANNEL_TYPES = {
    _GUILD_TEXT,
    _GUILD_ANNOUNCEMENT,
    _ANNOUNCEMENT_THREAD,
    _PUBLIC_THREAD,
    _PRIVATE_THREAD,
}
_THREAD_PARENT_TYPES = {
    _GUILD_TEXT,
    _GUILD_ANNOUNCEMENT,
    _GUILD_FORUM,
    _GUILD_MEDIA,
}


def _selected_channels(channels: list[dict]) -> list[dict]:
    """Return every message-readable channel/thread in stable id order."""
    return sorted(
        channels,
        key=lambda c: (
            str(c.get("parent_id") or ""),
            int(c.get("position") or 0),
            str(c.get("id") or ""),
        ),
    )


def _channel_type(channel: dict[str, Any]) -> int | None:
    try:
        return int(channel.get("type"))
    except (TypeError, ValueError):
        return None


def _is_message_channel(channel: dict[str, Any]) -> bool:
    return _channel_type(channel) in _MESSAGE_CHANNEL_TYPES


def _is_thread_parent(channel: dict[str, Any]) -> bool:
    return _channel_type(channel) in _THREAD_PARENT_TYPES


async def _list_discord_message_streams(
    source_client: Any,
    guild_id: str,
    channels: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Guild message backfill surface.

    Discord forum/media parents are thread-only, so the backfill unit is each
    post thread, not the parent container. Text/announcement channels are
    fetchable directly, and their active/archived threads are fetched as their
    own shards.
    """
    streams: dict[str, dict[str, Any]] = {
        str(channel["id"]): channel
        for channel in channels
        if channel.get("id") is not None and _is_message_channel(channel)
    }

    if hasattr(source_client, "list_active_guild_threads"):
        for thread in await _safe_list_active_guild_threads(source_client, guild_id):
            thread_id = str(thread.get("id") or "").strip()
            if thread_id and _is_message_channel(thread):
                streams[thread_id] = thread

    if hasattr(source_client, "list_channel_archived_threads"):
        for parent in channels:
            parent_id = str(parent.get("id") or "").strip()
            if not parent_id or not _is_thread_parent(parent):
                continue
            for archive_kind in ("public", "private"):
                threads = await _safe_list_channel_archived_threads(
                    source_client,
                    parent_id,
                    archive_kind=archive_kind,
                )
                for thread in threads:
                    thread_id = str(thread.get("id") or "").strip()
                    if thread_id and _is_message_channel(thread):
                        streams[thread_id] = thread

    return _selected_channels(list(streams.values()))


async def _safe_list_active_guild_threads(
    source_client: Any,
    guild_id: str,
) -> list[dict[str, Any]]:
    try:
        return await source_client.list_active_guild_threads(guild_id)
    except (DiscordApiError, NotImplementedError, AttributeError) as exc:
        log.info(
            "discord_planner_active_threads_skipped guild_scope=%s error=%s",
            bool(guild_id),
            type(exc).__name__,
        )
        return []


async def _safe_list_channel_archived_threads(
    source_client: Any,
    channel_id: str,
    *,
    archive_kind: str,
) -> list[dict[str, Any]]:
    try:
        return await source_client.list_channel_archived_threads(
            channel_id,
            archive_kind=archive_kind,
        )
    except (DiscordApiError, NotImplementedError, AttributeError) as exc:
        log.info(
            "discord_planner_archived_threads_skipped kind=%s error=%s",
            archive_kind,
            type(exc).__name__,
        )
        return []


async def plan_shards_discord(ctx: PlannerContext) -> list[Shard]:
    """One Shard per message-readable stream in the installed guild."""
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
    message_streams = await _list_discord_message_streams(
        ctx.source_client,
        install_id,
        channels,
    )
    for ch in message_streams:
        shards.append(Shard(
            shard_kind=SHARD_KIND_CHANNEL_WINDOW,
            shard_identifier={
                "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
                "guild_id": install_id,
                "channel_id": ch.get("id"),
                "channel_name": ch.get("name"),
                "channel_type": ch.get("type"),
                "parent_id": ch.get("parent_id"),
                "installation_id": install_id,
            },
            recency_score=1.0,
        ))
    return shards


PLANNER_DISPATCH["discord"] = plan_shards_discord


__all__ = [
    "SHARD_KIND_CHANNEL_WINDOW", "plan_shards_discord",
]
