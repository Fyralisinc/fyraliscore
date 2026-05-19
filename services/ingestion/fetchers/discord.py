"""services/ingestion/fetchers/discord.py — Discord backfill (M6.6).

Per A18 + A16 (N1) + A18.4. Paginates via /channels/{cid}/messages
with `before=<snowflake>`. Discord snowflakes are sortable integers,
so cursor.before_snowflake is the oldest seen.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "discord_channel_window"
_DEFAULT_PAGE = 100


class DiscordCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    before_snowflake: str | None = None
    oldest_seen_snowflake: str | None = None
    newest_seen_snowflake: str | None = None
    messages_seen: int = 0


async def _open_discord_client(install: asyncpg.Record):  # noqa: ANN202
    raise RuntimeError(
        "fetchers.discord._open_discord_client not configured; tests rebind."
    )


def _decode(c: dict[str, Any] | None) -> DiscordCursor:
    return DiscordCursor() if c is None else DiscordCursor.model_validate(c)


def _encode(c: DiscordCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


async def fetch_page_discord(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    channel_id = shard_identifier["channel_id"]
    guild_id = shard_identifier.get("guild_id")
    install_id = str(shard_identifier.get("installation_id") or "")
    cur = _decode(cursor)
    client, close = await _open_discord_client(install)
    try:
        messages = await client.get_messages(
            channel_id=channel_id,
            before=cur.before_snowflake,
            limit=_DEFAULT_PAGE,
        )
        is_end = len(messages) < _DEFAULT_PAGE
        records = [{
            "guild_id": guild_id,
            "channel_id": channel_id,
            "installation_id": install_id,
            "message": m,
            "read_path": "backfill",
        } for m in messages]
        oldest = cur.oldest_seen_snowflake
        newest = cur.newest_seen_snowflake
        for m in messages:
            mid = m.get("id")
            if mid:
                if oldest is None or int(mid) < int(oldest):
                    oldest = mid
                if newest is None or int(mid) > int(newest):
                    newest = mid
        new_cursor = DiscordCursor(
            before_snowflake=oldest,  # next page goes older
            oldest_seen_snowflake=oldest,
            newest_seen_snowflake=newest,
            messages_seen=cur.messages_seen + len(records),
        )
        return FetchResult(
            records=records, next_cursor=_encode(new_cursor),
            end_of_data=is_end,
        )
    finally:
        await close()


FETCHER_DISPATCH["discord"] = fetch_page_discord


__all__ = ["DiscordCursor", "SHARD_KIND_CHANNEL_WINDOW",
           "fetch_page_discord"]
