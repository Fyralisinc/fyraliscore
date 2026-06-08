"""services/ingest/ingestion/reconcilers/hibob.py — gap detection (People/HR).

After an entity shard completes, its cursor carries `high_water_updated` — the
max row `modified`/version the fetcher walked. The reconciler probes the LIVE
company for any entity of that type modified after the high-water; if one exists,
a reshare is emitted for that entity type, warm-started at the high-water
(incremental mode).

`external_id` parity (versioned by the row's modified field) means re-walked
entities dedup against what backfill already wrote — only genuinely new/changed
entities produce new observations. Pragmatic v1: one cheap 1-row query per entity
type; it can over-reshare but never under-reshares, and dedup makes re-walks
idempotent.
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


SHARD_KIND_ENTITY = "hibob_entity"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.hibob: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_hibob_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.hibob import _open_hibob_client
    return await _open_hibob_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_high_water(pool: Any, shard_id: Any) -> str | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        hw = cursor.get("high_water_updated")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_ENTITY:
        return None
    entity_type = identifier.get("entity_type")
    if not entity_type:
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None

    try:
        rows, _ = await client.list_entities(
            entity_type,
            limit=1,
            offset=0,
            modified_since=high_water,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.hibob.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not rows:
        return None

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_updated"] = high_water
    gap_identifier["updated_cursor"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_ENTITY,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_hibob(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, company_id, service_user_id, base_url,
               secret_ref, disabled_at
          FROM hibob_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_hibob_client(install)
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
            message=f"hibob reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["hibob"] = reconcile_hibob


__all__ = ["reconcile_hibob", "set_pool_provider", "SHARD_KIND_ENTITY"]
