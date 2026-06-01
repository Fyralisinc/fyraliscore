"""services/ingestion/reconcilers/google_drive.py — gap detection (IN-16).

Per A17 + A18 + A18.3 (reconciler pool-provider seam).

============================================================
GAP DETECTION ALGORITHM
============================================================
After a drive shard completes, its cursor carries `next_start_page_token` — the
Changes-API token marking the point the fetcher walked up to. The reconciler
probes the LIVE drive with `changes.list?pageToken=<token>&includeRemoved=true
&pageSize=1`: if any change exists past that token, a reshare is emitted for
that drive. A trash counts (includeRemoved=true).

A reshared shard re-runs the walk with a boosted recency. `external_id` parity
(`gdrive:{file_id}:{version}`) means re-walked files dedup against what backfill
already wrote — only genuinely new/changed files produce new observations.
Pragmatic v1: the probe is one cheap 1-row query per drive; it can over-reshare
but never under-reshares, and dedup makes re-walks idempotent.
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
from services.integrations.google_drive import metrics


log = logging.getLogger(__name__)


SHARD_KIND_FILES = "google_drive_files"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.google_drive: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_drive_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingestion.fetchers.google_drive import _open_drive_client
    return await _open_drive_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_start_token(pool: Any, shard_id: Any) -> str | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        tok = cursor.get("next_start_page_token")
        return tok if isinstance(tok, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_FILES:
        return None
    owner_email = identifier.get("owner_email")
    drive_id = identifier.get("drive_id") or "my-drive"
    if not owner_email:
        return None

    start_token = await _load_shard_start_token(pool, shard["id"])
    if start_token is None:
        # No reference point (no token captured); nothing to compare.
        return None

    try:
        has_changes = await client.has_changes_since(
            user_email=owner_email,
            page_token=start_token,
            drive_id=drive_id,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.google_drive.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not has_changes:
        return None

    metrics.record_fetch_event("reconcile_gap")
    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    # Warm-start the reshared shard from the captured token so it runs an
    # incremental (delta) walk rather than a full backfill.
    gap_identifier["start_page_token"] = start_token
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_FILES,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_google_drive(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, workspace_domain, service_account_email,
               scope, disabled_at
          FROM google_drive_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_drive_client(install)
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
            message=f"google_drive reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["google_drive"] = reconcile_google_drive


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_FILES",
    "reconcile_google_drive",
    "set_pool_provider",
]
