"""services/ingest/ingestion/reconcilers/gusto.py — gap detection (finance/payroll).

After a `payroll` shard completes, its cursor carries `high_water` — the max
`check_date` the fetcher walked. The reconciler probes the LIVE company for any
payroll with a check_date strictly after the high-water (one cheap
`start_date=<hw>&date_filter_by=check_date` page, filtered strictly-greater
client-side because the server-side date filter is day-granular and inclusive);
if one exists, a reshare is emitted warm-started at the high-water
(incremental mode).

`employee` shards have no updated-since semantics (the endpoint is a full
re-walk each poll cycle, deduped by the version-discriminated external_id), so
they are skipped here — the periodic re-walk IS their reconciliation.

`external_id` parity (versioned by employee `version` / payroll processed
state) means re-walked rows dedup against what backfill already wrote — only
genuinely new/changed rows produce new observations. Pragmatic v1: it can
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


SHARD_KIND_ENTITY = "gusto_entity"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.gusto: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_gusto_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.gusto import _open_gusto_client
    return await _open_gusto_client(install)


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
        hw = cursor.get("high_water")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_ENTITY:
        return None
    # Only payroll shards carry a date high-water (employee shards full
    # re-walk every poll cycle — nothing to gap-check against).
    if identifier.get("entity_type") != "payroll":
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None

    try:
        rows, _ = await client.list_payrolls(
            page=1,
            per=100,
            start_date=high_water,
            date_filter_by="check_date",
            payroll_types=("regular", "off_cycle"),
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.gusto.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    # The server date filter is inclusive at day granularity; only a payroll
    # with a check_date strictly past the high-water is a genuine gap.
    fresh = [
        r for r in rows
        if isinstance(r.get("check_date"), str)
        and r["check_date"] > high_water
    ]
    if not fresh:
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


async def reconcile_gusto(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, company_uuid, base_url, secret_ref,
               refresh_secret_ref, disabled_at
          FROM gusto_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        run["tenant_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_gusto_client(install)
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
            message=f"gusto reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["gusto"] = reconcile_gusto


__all__ = ["reconcile_gusto", "set_pool_provider", "SHARD_KIND_ENTITY"]
