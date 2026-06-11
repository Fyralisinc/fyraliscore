"""services/ingest/ingestion/reconcilers/linkedin.py — gap detection.

After a stream shard completes, its cursor carries `high_water_ms` — the max
epoch-millis watermark the fetcher walked (posts: `lastModifiedAt`;
statistics: `timeRange.start`). The reconciler probes the LIVE organization
for anything newer than the high-water; if found, a reshare is emitted for
that stream, warm-started at the high-water (incremental mode).

Per-stream probe (one cheap call each):
  - post: `list_posts(start=0, count=1)` — the finder sorts DESC by
    `lastModifiedAt`, so the FIRST element is the newest; newer than the
    high-water ⇒ gap.
  - share_statistics / follower_statistics: the time-bound finder from
    `high_water + 1` — any element ⇒ a new snapshot bucket exists ⇒ gap.

`external_id` parity (discriminated by entity_kind) means re-walked elements
dedup against what backfill already wrote — only genuinely new elements produce
new observations. Pragmatic v1: it can over-reshare but never under-reshares,
and dedup makes re-walks idempotent.
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


SHARD_KIND_ENTITY = "linkedin_entity"
RESHARE_RECENCY_SCORE = 1.5

_STATISTICS_TYPES = ("share_statistics", "follower_statistics")


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.linkedin: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_linkedin_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.linkedin import _open_linkedin_client
    return await _open_linkedin_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_high_water(pool: Any, shard_id: Any) -> int | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        hw = cursor.get("high_water_ms")
        return hw if isinstance(hw, int) and not isinstance(hw, bool) else None
    return None


def _newest_modified_ms(rows: list[dict[str, Any]]) -> int | None:
    for row in rows:
        value = row.get("lastModifiedAt") or row.get("createdAt")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


async def _probe_has_gap(
    client: Any, entity_type: str, high_water: int,
) -> bool:
    if entity_type == "post":
        rows, _ = await client.list_posts(start=0, count=1)
        newest = _newest_modified_ms(rows)
        return newest is not None and newest > high_water
    if entity_type in _STATISTICS_TYPES:
        method = (
            client.share_statistics
            if entity_type == "share_statistics" else client.follower_statistics
        )
        elements = await method(start_ms=high_water + 1)
        return bool(elements)
    return False


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
        has_gap = await _probe_has_gap(client, str(entity_type), high_water)
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.linkedin.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not has_gap:
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


async def reconcile_linkedin(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, organization_urn, base_url, secret_ref,
               refresh_secret_ref, disabled_at
          FROM linkedin_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_linkedin_client(install)
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
            message=f"linkedin reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["linkedin"] = reconcile_linkedin


__all__ = ["reconcile_linkedin", "set_pool_provider", "SHARD_KIND_ENTITY"]
