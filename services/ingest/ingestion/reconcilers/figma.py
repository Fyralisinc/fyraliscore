"""services/ingest/ingestion/reconcilers/figma.py — gap detection (design).

After a file shard completes, its cursor carries `high_water_created` — the max
event `createdAt` the fetcher walked. The reconciler probes the LIVE file for any
event newer than the high-water; if one exists, it emits both an incremental
event reshare and a version-aware full-document snapshot probe. The snapshot
probe only downloads the complete design when Figma's file version changed.

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
SHARD_KIND_FILE_SNAPSHOT = "figma_file_snapshot"
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


async def _load_snapshot_state(
    pool: Any,
    *,
    tenant_id: Any,
    installation_id: Any,
) -> dict[str, dict[str, Any]]:
    """Load the durable snapshot high-water for active Figma files.

    Event changes are the cheap signal that makes a source worth revisiting;
    the stored version lets the snapshot fetcher avoid another full document
    download for a comment-only change.
    """
    rows = await pool.fetch(
        """
        SELECT file_key, file_name, project_name, snapshot_version
          FROM figma_files
         WHERE tenant_id = $1
           AND figma_installation_id = $2
           AND state = 'active'
        """,
        tenant_id,
        installation_id,
    )
    return {
        str(row["file_key"]): {
            "file_name": row["file_name"],
            "project_name": row["project_name"],
            "snapshot_version": row["snapshot_version"],
        }
        for row in rows
        if row["file_key"] is not None
    }


async def _check_one_shard_for_gap(
    *,
    pool: Any,
    client: Any,
    shard: asyncpg.Record,
    snapshot_state: dict[str, dict[str, Any]],
) -> list[ResharedShard]:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_FILE_EVENTS:
        return []
    file_key = identifier.get("file_key")
    if not file_key:
        return []

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return []  # No reference point (empty file / cursor).

    try:
        events, _, _ = await client.list_events(
            file_key, limit=1, offset=0, start=high_water[:10],
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.figma.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return []

    # An event strictly newer than the high-water means there's a gap.
    newest = None
    for e in events:
        created = e.get("createdAt") or e.get("created_at")
        if isinstance(created, str):
            newest = created
            break
    if newest is None or newest <= high_water:
        return []

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_created"] = high_water
    # Warm-start the reshared walk at the high-water so it only re-fetches the
    # changed tail (incremental mode in the fetcher).
    gap_identifier["event_cursor"] = high_water
    event_reshare = ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_FILE_EVENTS,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )

    persisted_snapshot = snapshot_state.get(str(file_key), {})
    snapshot_identifier = {
        "shard_kind": SHARD_KIND_FILE_SNAPSHOT,
        "file_key": file_key,
        "file_name": persisted_snapshot.get("file_name")
        or identifier.get("file_name"),
        "project_name": persisted_snapshot.get("project_name"),
        "team_id": identifier.get("team_id"),
        "installation_id": identifier.get("installation_id"),
        "snapshot_version": persisted_snapshot.get("snapshot_version"),
        "parent_shard_id": str(shard["id"]),
        "gap_baseline_created": high_water,
    }
    snapshot_reshare = ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_FILE_SNAPSHOT,
            shard_identifier=snapshot_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )
    return [event_reshare, snapshot_reshare]


async def reconcile_figma(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, base_url, secret_ref, team_id, auth_kind,
               refresh_secret_ref, token_expires_at, disabled_at
          FROM figma_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    snapshot_state = await _load_snapshot_state(
        pool,
        tenant_id=run["tenant_id"],
        installation_id=install["id"],
    )

    client, close = await _open_figma_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active:
            new_shards.extend(await _check_one_shard_for_gap(
                pool=pool,
                client=client,
                shard=shard,
                snapshot_state=snapshot_state,
            )
            )
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=(
                "figma reconciler: "
                f"{len(new_shards)} event/snapshot reshare(s)."
            ),
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["figma"] = reconcile_figma


__all__ = [
    "reconcile_figma",
    "set_pool_provider",
    "SHARD_KIND_FILE_EVENTS",
    "SHARD_KIND_FILE_SNAPSHOT",
]
