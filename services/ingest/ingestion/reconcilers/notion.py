"""services/ingest/ingestion/reconcilers/notion.py — Notion gap detection (IN-14).

Per A17 + A18 + A18.3 (reconciler pool-provider seam).

============================================================
GAP DETECTION ALGORITHM
============================================================
After a Notion shard completes, its cursor carries `last_edited_at` — the
high-water timestamp of every object the fetcher walked. The reconciler
probes the LIVE latest edit for that shard's scope and compares:

  - notion_database  : `latest_database_edit(database_id)` — newest row
                       edit time. If newer than the cursor high-water, the
                       database changed during/after backfill → reshare.
  - notion_page_tree : `latest_page_edit()` — newest page edit time in the
                       workspace. If newer than the cursor high-water →
                       reshare the loose-page sweep.

A reshared shard re-runs the same walk with a boosted recency so it lands
ahead of any remaining low-recency backfill. `external_id` parity means
re-walked objects dedup against what backfill already wrote — only genuine
new/changed objects produce new observations.

This is a pragmatic v1: the probe is one cheap 1-row query per shard. It
can over-reshare (an edit to an already-seen object re-walks the scope)
but never under-reshares, and dedup makes re-walks idempotent.

============================================================
SOURCE CONTRACT
============================================================
`SourceDefinition.reconciler_binding` points to `reconcile_notion`. The pool
provider seam (A18.3) remains for reading shard cursors.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from services.ingest.ingestion.installations import load_source_installation
import orjson

from lib.shared.provider_transport import RetryLater
from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state
from services.ingest.integrations.notion import metrics


log = logging.getLogger(__name__)


SHARD_KIND_DATABASE = "notion_database"
SHARD_KIND_PAGE_TREE = "notion_page_tree"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.notion: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_notion_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_notion_client
    return await open_notion_client(install)


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
        hw = cursor.get("last_edited_at")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    shard_kind = identifier.get("shard_kind")
    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        # No reference point: the shard walked zero objects (e.g. a page_tree
        # shard in a workspace whose pages are all database rows). With no
        # high-water there is nothing to compare a probe against, so a re-share
        # would re-walk the same empty scope forever. Mirror the calendar
        # reconciler's `high_water is None -> return None` guard. (Without this,
        # the `latest <= high_water` check below is skipped on None and the
        # shard re-shares unconditionally — the IN-14 runaway loop.)
        return None

    try:
        if shard_kind == SHARD_KIND_DATABASE:
            db_id = identifier.get("database_id")
            if not db_id:
                return None
            latest = await client.latest_database_edit(db_id)
        elif shard_kind == SHARD_KIND_PAGE_TREE:
            latest = await client.latest_page_edit()
        else:
            return None
    except RetryLater:
        # The workflow owns durable not-before scheduling. Treating a quota
        # pause as "no gap" would incorrectly complete reconciliation.
        raise
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.notion.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if latest is None:
        return None
    if latest <= high_water:
        return None  # no edits newer than what we walked (high_water is non-None here)

    metrics.record_fetch_event("reconcile_gap")
    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_edited_at"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=shard_kind,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_notion(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="notion",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_notion_client(install)
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
            message=f"notion reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_DATABASE",
    "SHARD_KIND_PAGE_TREE",
    "reconcile_notion",
    "set_pool_provider",
]
