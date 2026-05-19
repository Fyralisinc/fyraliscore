"""services/ingestion/fetchers/slack.py — Slack backfill fetcher (M6.5).

Per A18 + A16 (N1) + A18.4 (shard_kind mirror). Uses
`conversations.history` with `cursor` pagination. Last page stamps
`oldest_seen_ts` for the reconciler's gap check.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"


class SlackCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    next_cursor: str | None = None
    oldest_seen_ts: str | None = None
    newest_seen_ts: str | None = None
    messages_seen: int = 0


async def _open_slack_client(install: asyncpg.Record):  # noqa: ANN202
    raise RuntimeError(
        "fetchers.slack._open_slack_client not configured; tests rebind."
    )


def _decode_cursor(c: dict[str, Any] | None) -> SlackCursor:
    if c is None:
        return SlackCursor()
    return SlackCursor.model_validate(c)


def _encode_cursor(c: SlackCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


async def fetch_page_slack(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    channel_id = shard_identifier["channel_id"]
    install_id = str(shard_identifier.get("installation_id") or "")
    cur = _decode_cursor(cursor)

    client, close = await _open_slack_client(install)
    try:
        messages, next_cursor = await client.conversations_history(
            channel=channel_id, cursor=cur.next_cursor,
        )
        is_end = not next_cursor

        records = [{
            "channel_id": channel_id,
            "team_id": shard_identifier.get("team_id"),
            "installation_id": install_id,
            "message": m,
            "read_path": "backfill",
        } for m in messages]

        # Track oldest/newest seen ts across the entire shard.
        oldest = cur.oldest_seen_ts
        newest = cur.newest_seen_ts
        for m in messages:
            ts = m.get("ts")
            if ts:
                if oldest is None or ts < oldest:
                    oldest = ts
                if newest is None or ts > newest:
                    newest = ts

        new_cursor = SlackCursor(
            next_cursor=next_cursor,
            oldest_seen_ts=oldest,
            newest_seen_ts=newest,
            messages_seen=cur.messages_seen + len(records),
        )
        return FetchResult(
            records=records, next_cursor=_encode_cursor(new_cursor),
            end_of_data=is_end,
        )
    finally:
        await close()


FETCHER_DISPATCH["slack"] = fetch_page_slack


__all__ = ["SHARD_KIND_CHANNEL_WINDOW", "SlackCursor", "fetch_page_slack"]
