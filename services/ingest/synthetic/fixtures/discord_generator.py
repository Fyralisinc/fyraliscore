"""Discord guild fixture generator.

`make_discord_guild(guild_id=..., channels=N, messages_per_channel=M)`
produces a deterministic guild shaped to feed `MockDiscordClient`.

Per M6.6 reality: the planner filters channels to type==0 (GUILD_TEXT)
and applies 5% sparse sampling. The generator returns all channels as
type==0 so the planner samples ALL of them — tests can shape the
shard count via the `channels` parameter directly.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Snowflake epoch: 2015-01-01 in ms. Discord IDs encode timestamp.
_DISCORD_EPOCH_MS = 1_420_070_400_000


def make_discord_guild(
    *,
    guild_id: str,
    channels: int = 4,
    messages_per_channel: int = 30,
    channel_type: int = 0,
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a Discord guild fixture.

    Args:
      guild_id: Discord guild snowflake (as string).
      channels: Number of channels.
      messages_per_channel: Messages per channel.
      channel_type: Discord channel type (default 0 = GUILD_TEXT;
        M6.6 planner filters to this).
      page_size: Mock client's default page size for `get_messages`.

    Returns:
      Fixture dict consumable by `MockDiscordClient(fixture=...)`.
    """
    channel_list: list[dict[str, Any]] = []
    for c in range(channels):
        cid = str(_snowflake(guild_id, c, "channel"))
        msgs = [
            _discord_message(guild_id, cid, m)
            for m in range(messages_per_channel)
        ]
        channel_list.append({
            "id": cid,
            "name": f"channel-{c}",
            "type": channel_type,
            "messages": msgs,
        })

    return {
        "guild_id": guild_id,
        "channels": channel_list,
        "page_size": page_size,
    }


def _discord_message(
    guild_id: str, channel_id: str, idx: int,
) -> dict[str, Any]:
    snowflake = str(_snowflake(guild_id, idx, channel_id))
    return {
        "id": snowflake,
        "channel_id": channel_id,
        "type": 0,
        "content": f"synthetic discord message #{idx}",
        "author": {
            "id": str(_snowflake(guild_id, idx, "author")),
            "username": f"user-{idx}",
        },
        "timestamp": "2026-01-01T00:00:00+00:00",
    }


def _snowflake(*parts: Any) -> int:
    """Generate a deterministic Discord-shaped snowflake.

    Real Discord snowflakes are 64-bit IDs with the high 41 bits being
    a timestamp since Discord's epoch. We synthesize ordered snowflakes
    so larger-index messages have larger ids (matching production
    behavior — newer messages have larger ids).
    """
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    digest_int = int.from_bytes(h.digest()[:8], "big")
    # Embed timestamp in high bits so ordering is roughly monotonic.
    # The deterministic-but-not-strictly-monotonic property is fine for
    # mock testing; reconcilers compare snowflake values numerically.
    idx_component = int(parts[1]) if len(parts) > 1 else 0
    timestamp_ms = _DISCORD_EPOCH_MS + idx_component * 1000
    snowflake = (timestamp_ms - _DISCORD_EPOCH_MS) << 22 | (digest_int & 0x3FFFFF)
    return snowflake
