"""services/ingest/ingestion/reconcilers/ashby.py — gap detection (recruiting).

After an entity shard completes, its cursor carries `sync_token` (the persisted
Ashby syncToken for incremental polls) and `high_water_updated` (the max entity
updated timestamp the fetcher walked). The reconciler probes the LIVE org for any
entity of that type changed since the shard finished; if one exists, a reshare is
emitted for that entity type, warm-started at the persisted syncToken
(incremental mode).

`external_id` parity (`ashby:{org}:{entity}:{id}`, discriminated by entity_kind)
means re-walked entities dedup against what backfill already wrote — only
genuinely new/changed entities produce new observations. Pragmatic v1: one cheap
1-row incremental probe per entity type; it can over-reshare but never
under-reshares, and dedup makes re-walks idempotent.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "ashby_entity"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.ashby: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_ashby_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.ashby import _open_ashby_client
    return await _open_ashby_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_cursor(pool: Any, shard_id: Any) -> dict[str, Any] | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    return cursor if isinstance(cursor, dict) else None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_ENTITY:
        return None
    entity_type = identifier.get("entity_type")
    if not entity_type:
        return None

    cursor = await _load_shard_cursor(pool, shard["id"])
    if cursor is None:
        return None
    sync_token = cursor.get("sync_token")
    high_water = cursor.get("high_water_updated")
    # Without a persisted syncToken there's no cheap incremental probe — skip
    # (a full re-walk would over-reshare every cycle).
    if not isinstance(sync_token, str) or not sync_token:
        return None

    try:
        rows, _next_cursor, _next_sync = await client.list_entities(
            entity_type, sync_token=sync_token, limit=1,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.ashby.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not rows:
        return None

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_updated"] = high_water
    # Warm-start the reshare at the persisted syncToken (incremental mode).
    gap_identifier["sync_cursor"] = sync_token
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_ENTITY,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_ashby(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, org_id, base_url, secret_ref, disabled_at
          FROM ashby_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_ashby_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active:
            reshared = await _check_one_shard_for_gap(
                pool=pool, client=client, shard=shard,
            )
            if reshared is not None:
                new_shards.append(reshared)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=f"ashby reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["ashby"] = reconcile_ashby


__all__ = ["reconcile_ashby", "set_pool_provider", "SHARD_KIND_ENTITY"]
