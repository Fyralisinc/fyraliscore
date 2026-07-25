"""services/ingest/ingestion/reconcilers/jira.py — gap detection (IN-17).

Per A17 + A18 + A18.3 (reconciler pool-provider seam).

============================================================
GAP DETECTION ALGORITHM
============================================================
After a project shard completes, its cursor carries `high_water_updated` — the
max issue `updated` timestamp the fetcher walked. The reconciler probes the
LIVE project with a 1-row JQL search `project = KEY AND updated >= <high_water>`:
if anything changed since the high-water, a reshare is emitted for that
project. The reshared shard re-runs the walk warm-started at the high-water.

`external_id` parity (versioned by `updated`) means re-walked issues dedup
against what backfill already wrote — only genuinely new/changed issues +
transitions + comments produce new observations. Pragmatic v1: the probe is one
cheap query per project; it can over-reshare but never under-reshares, and dedup
makes re-walks idempotent.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from services.ingest.ingestion.installations import load_source_installation
import orjson

from services.ingest.ingestion.fetchers.jira import _to_jql_minute_after
from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_PROJECT_ISSUES = "jira_project_issues"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.jira: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_jira_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.jira import _open_jira_client
    return await _open_jira_client(install)


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
    if identifier.get("shard_kind") != SHARD_KIND_PROJECT_ISSUES:
        return None
    project_key = identifier.get("project_key")
    if not project_key:
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None  # No reference point (empty project / cursor).
    # EXCLUSIVE floor = the minute AFTER the high-water. With `>=` in the probe
    # this excludes the high-water's own minute, so a project that hasn't
    # changed since the walk returns no gap and the reconciler converges (a
    # plain minute-floor would re-match the boundary issue's seconds forever).
    floor = _to_jql_minute_after(high_water)
    if floor is None:
        return None

    try:
        has_updates = await client.has_updates_since(
            project_key=project_key, updated_min_jql=floor,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.jira.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not has_updates:
        return None

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_updated"] = high_water
    # Warm-start the reshared walk at the high-water so it only re-fetches the
    # changed tail (incremental mode in the fetcher).
    gap_identifier["updated_cursor"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_PROJECT_ISSUES,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_jira(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="jira",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_jira_client(install)
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
            message=f"jira reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_PROJECT_ISSUES",
    "reconcile_jira",
    "set_pool_provider",
]
