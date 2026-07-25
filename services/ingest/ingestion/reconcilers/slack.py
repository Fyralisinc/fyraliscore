"""services/ingest/ingestion/reconcilers/slack.py — Slack gap detection (M6.5).

Per A17 + A18 + A18.3. For each done shard: call
`conversations.history(channel, oldest=newest_seen_ts)`. If any
messages return, gap exists.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from services.ingest.ingestion.installations import load_source_installation
import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"
SHARD_KIND_DM_WINDOW = "slack_dm_window"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.slack: pool provider not registered."
        )
    return _pool_provider


async def _open_slack_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_slack_client
    return await open_slack_client(install)


async def _open_slack_user_client(  # noqa: ANN202
    install: asyncpg.Record, ident: dict[str, Any],
):
    # Per-USER DM gap-probe client (xoxp token). A bot token can't read DMs,
    # so DM shards reconcile under the consenting user's token. X3 mock
    # harness monkeypatches this symbol.
    from services.ingest.ingestion.fetchers._clients import open_slack_user_client
    return await open_slack_user_client(
        tenant_id=install["tenant_id"],
        team_id=ident["team_id"],
        user_id=ident["consenting_user_id"],
        base_url=ident.get("base_url"),
    )


def _decode_id(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_cursor(pool: Any, shard_id: Any) -> dict[str, Any] | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None:
        return None
    cur = state.state_data.get("cursor") if state.state_data else None
    return cur if isinstance(cur, dict) else None


async def _check_one_shard(
    *, pool: Any, bot_client: Any, install: asyncpg.Record,
    shard: asyncpg.Record,
) -> ResharedShard | None:
    ident = _decode_id(shard["shard_identifier"])
    channel_id = ident.get("channel_id")
    if not channel_id:
        return None
    shard_kind = ident.get("shard_kind") or SHARD_KIND_CHANNEL_WINDOW
    is_dm = shard_kind == SHARD_KIND_DM_WINDOW

    cursor = await _load_cursor(pool, shard["id"])
    if cursor is None:
        return None
    newest_seen = cursor.get("newest_seen_ts")
    if newest_seen is None:
        return None

    # DM shards gap-probe under the consenting user's xoxp token (a bot token
    # can't read DMs); channel shards reuse the shared bot client.
    close = None
    try:
        if is_dm:
            client, close = await _open_slack_user_client(install, ident)
        else:
            client = bot_client
        try:
            # Slack's conversations.history with `oldest=newest_seen_ts`
            # returns only messages newer than that timestamp.
            messages, _ = await client.conversations_history(
                channel=channel_id, oldest=newest_seen, limit=1,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "reconcilers.slack.history_failed",
                extra={
                    "channel_id": channel_id, "shard_kind": shard_kind,
                    "error": str(exc)[:200],
                },
            )
            return None
    finally:
        if close is not None:
            await close()
    if not messages:
        return None

    if is_dm:
        gap_id: dict[str, Any] = {
            "shard_kind": SHARD_KIND_DM_WINDOW,
            "channel_id": channel_id,
            "channel_type": ident.get("channel_type"),
            "consenting_user_id": ident.get("consenting_user_id"),
            "counterpart_user_id": ident.get("counterpart_user_id"),
            "team_id": ident.get("team_id"),
            "installation_id": ident.get("installation_id"),
            "parent_shard_id": str(shard["id"]),
            "gap_baseline_ts": newest_seen,
        }
    else:
        gap_id = {
            "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
            "channel_id": channel_id,
            "channel_name": ident.get("channel_name"),
            "team_id": ident.get("team_id"),
            "installation_id": ident.get("installation_id"),
            "parent_shard_id": str(shard["id"]),
            "gap_baseline_ts": newest_seen,
        }
    return ResharedShard(
        shard=Shard(
            shard_kind=shard_kind,
            shard_identifier=gap_id,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_slack(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="slack",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    bot_client, close = await _open_slack_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for s in active:
            r = await _check_one_shard(
                pool=pool, bot_client=bot_client, install=install, shard=s,
            )
            if r is not None:
                new_shards.append(r)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=f"slack reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_CHANNEL_WINDOW",
    "SHARD_KIND_DM_WINDOW",
    "reconcile_slack",
    "set_pool_provider",
]
