"""services/ingestion/reconcilers/slack.py — Slack gap detection (M6.5).

Per A17 + A18 + A18.3. For each done shard: call
`conversations.history(channel, oldest=newest_seen_ts)`. If any
messages return, gap exists.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
import orjson

from services.ingestion.planners import Shard
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"
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
    from services.ingestion.fetchers._clients import open_slack_client
    return await open_slack_client(install)


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
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    ident = _decode_id(shard["shard_identifier"])
    channel_id = ident.get("channel_id")
    if not channel_id:
        return None

    cursor = await _load_cursor(pool, shard["id"])
    if cursor is None:
        return None
    newest_seen = cursor.get("newest_seen_ts")
    if newest_seen is None:
        return None

    try:
        # Slack's conversations.history with `oldest=newest_seen_ts`
        # returns only messages newer than that timestamp.
        messages, _ = await client.conversations_history(
            channel=channel_id, oldest=newest_seen, limit=1,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "reconcilers.slack.history_failed",
            extra={"channel_id": channel_id, "error": str(exc)[:200]},
        )
        return None
    if not messages:
        return None

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
            shard_kind=SHARD_KIND_CHANNEL_WINDOW,
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
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, provider, installation_id, enabled
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'slack' AND enabled = TRUE
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_slack_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for s in active:
            r = await _check_one_shard(
                pool=pool, client=client, shard=s,
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


RECONCILER_DISPATCH["slack"] = reconcile_slack


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_CHANNEL_WINDOW",
    "reconcile_slack",
    "set_pool_provider",
]
