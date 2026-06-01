"""services/ingestion/fetchers/slack.py — Slack backfill fetcher (M6.5).

Per A18 + A16 (N1) + A18.4 (shard_kind mirror) + A27.3 (handler
conformance). Uses `conversations.history` with `cursor` pagination.
Last page stamps `oldest_seen_ts` for the reconciler's gap check.

============================================================
HANDLER CONFORMANCE (A27.3) + EXTERNAL_ID PARITY (HLD §02 L278)
============================================================
Each record is emitted in the Slack Events-API `event_callback` shape
the `slack:message` webhook handler consumes — `{event: {...}, ...}`
— NOT a backfill-specific wrapper. The webhook handler derives
`external_id = "{channel}:{ts}"`; `conversations.history` messages
carry `ts` but NOT `channel` (it's the request parameter), so the
fetcher INJECTS `channel` into the event. A backfilled message and
its live webhook twin therefore derive the identical external_id and
dedup to one observation. Slack carries no load-bearing webhook
headers, so no `webhook_metadata` is attached.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"
SHARD_KIND_DM_WINDOW = "slack_dm_window"


class SlackCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    next_cursor: str | None = None
    oldest_seen_ts: str | None = None
    newest_seen_ts: str | None = None
    messages_seen: int = 0


async def _open_slack_client(install: asyncpg.Record):  # noqa: ANN202
    # Real SlackClient pointed at the resolver's slack_api base (prod or
    # local spammer). X3 mock harness monkeypatches this symbol.
    from services.ingestion.fetchers._clients import open_slack_client
    return await open_slack_client(install)


async def _open_slack_user_client(  # noqa: ANN202
    install: asyncpg.Record, shard_identifier: dict[str, Any],
):
    # Per-USER DM client (xoxp token). Identity comes from the
    # slack_dm_window shard_identifier (team + consenting user); the tenant is
    # the shard's install tenant. X3 mock harness monkeypatches this symbol.
    from services.ingestion.fetchers._clients import open_slack_user_client
    return await open_slack_user_client(
        tenant_id=install["tenant_id"],
        team_id=shard_identifier["team_id"],
        user_id=shard_identifier["consenting_user_id"],
        base_url=shard_identifier.get("base_url"),
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
    cur = _decode_cursor(cursor)

    # DM shards (`slack_dm_window`) read under the consenting user's xoxp token
    # and carry a `channel_type` (im/mpim) the bot-channel path lacks —
    # conversations.history messages don't self-describe their surface, so the
    # fetcher INJECTS it onto each event (parity with the live webhook + the
    # inline backfill, which both stamp content.channel_type). Channel shards
    # keep their existing behaviour (no channel_type; the handler stamps None).
    is_dm = shard_identifier.get("shard_kind") == SHARD_KIND_DM_WINDOW
    if is_dm:
        client, close = await _open_slack_user_client(install, shard_identifier)
        channel_type = shard_identifier.get("channel_type")
    else:
        client, close = await _open_slack_client(install)
        channel_type = None
    try:
        from services.integrations.slack.client import SlackApiError
        try:
            messages, next_cursor = await client.conversations_history(
                channel=channel_id, cursor=cur.next_cursor,
            )
        except SlackApiError as e:
            slack_error = (getattr(e, "context", None) or {}).get("slack_error")
            # The bot is not a member of this (public) channel, so Slack
            # refuses to serve its history. A bot is rarely in every
            # channel of a real workspace — skip the channel as a
            # terminal empty page rather than failing the whole backfill
            # run. Live coverage for such a channel only begins once the
            # bot is invited (its message.* events then flow).
            if slack_error in ("not_in_channel", "channel_not_found"):
                log.info(
                    "slack_backfill_skip_inaccessible_channel",
                    extra={"channel_id": channel_id, "slack_error": slack_error},
                )
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=True,
                )
            raise
        is_end = not next_cursor

        # A27.3: emit the event_callback shape the slack:message handler
        # consumes. Inject `channel` into the event so external_id
        # ("{channel}:{ts}") matches the live webhook for the same
        # message. `install_id` is dropped — the webhook body has no
        # such field and the handler doesn't read it; tenant is known
        # from the shard.
        def _event(m: dict[str, Any]) -> dict[str, Any]:
            ev = {**m, "channel": channel_id}
            if channel_type is not None:
                ev["channel_type"] = channel_type
            return ev

        records = [{
            "type": "event_callback",
            "team_id": shard_identifier.get("team_id"),
            "event": _event(m),
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


__all__ = [
    "SHARD_KIND_CHANNEL_WINDOW",
    "SHARD_KIND_DM_WINDOW",
    "SlackCursor",
    "fetch_page_slack",
]
