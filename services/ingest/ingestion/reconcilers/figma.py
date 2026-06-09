"""services/ingest/ingestion/reconcilers/figma.py — gap detection (design).

After a file shard completes, its cursor carries `high_water_created` — the max
event `createdAt` the fetcher walked. The reconciler probes the LIVE file for any
event newer than the high-water; if one exists, a reshare is emitted for that
file, warm-started at the high-water (incremental mode).

`external_id` parity (versioned by version/updated) means re-walked events dedup
against what backfill already wrote — only genuinely new/changed events produce
new observations. Pragmatic v1: the probe is one cheap query per file; it can
over-reshare but never under-reshares, and dedup makes re-walks idempotent.
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


SHARD_KIND_FILE_EVENTS = "figma_file_events"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.figma: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_figma_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.figma import _open_figma_client
    return await _open_figma_client(install)


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
        hw = cursor.get("high_water_created")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_FILE_EVENTS:
        return None
    file_key = identifier.get("file_key")
    if not file_key:
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None  # No reference point (empty file / cursor).

    try:
        events, _, _ = await client.list_events(
            file_key, limit=1, offset=0, start=high_water[:10],
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.figma.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    # An event strictly newer than the high-water means there's a gap.
    newest = None
    for e in events:
        created = e.get("createdAt") or e.get("created_at")
        if isinstance(created, str):
            newest = created
            break
    if newest is None or newest <= high_water:
        return None

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_created"] = high_water
    # Warm-start the reshared walk at the high-water so it only re-fetches the
    # changed tail (incremental mode in the fetcher).
    gap_identifier["event_cursor"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_FILE_EVENTS,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_figma(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, base_url, secret_ref, team_id, disabled_at
          FROM figma_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_figma_client(install)
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
            message=f"figma reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["figma"] = reconcile_figma


__all__ = ["reconcile_figma", "set_pool_provider", "SHARD_KIND_FILE_EVENTS"]
