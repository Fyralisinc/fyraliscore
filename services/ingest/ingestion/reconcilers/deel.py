"""services/ingest/ingestion/reconcilers/deel.py — gap detection (finance).

After a contract shard completes, its cursor carries `high_water_created` — the
max payment `createdAt` the fetcher walked. The reconciler probes the LIVE
contract for any payment newer than the high-water; if one exists, a reshare
is emitted for that contract, warm-started at the high-water (incremental mode).

`external_id` parity (versioned by status) means re-walked payments dedup
against what backfill already wrote — only genuinely new/changed payments
produce new observations. Pragmatic v1: the probe is one cheap query per
contract; it can over-reshare but never under-reshares, and dedup makes re-walks
idempotent. The probe also serves as the token keepalive.
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


SHARD_KIND_CONTRACT_PAYMENTS = "deel_contract_payments"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.deel: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_deel_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.deel import _open_deel_client
    return await _open_deel_client(install)


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
    if identifier.get("shard_kind") != SHARD_KIND_CONTRACT_PAYMENTS:
        return None
    contract_id = identifier.get("contract_id")
    if not contract_id:
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None  # No reference point (empty contract / cursor).

    try:
        payments, _, _ = await client.list_payments(
            contract_id, limit=1, offset=0, start=high_water[:10],
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.deel.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    # A payment strictly newer than the high-water means there's a gap.
    newest = None
    for p in payments:
        created = p.get("createdAt") or p.get("postedAt")
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
    gap_identifier["payment_cursor"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_CONTRACT_PAYMENTS,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_deel(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="deel",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_deel_client(install)
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
            message=f"deel reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = ["reconcile_deel", "set_pool_provider", "SHARD_KIND_CONTRACT_PAYMENTS"]
