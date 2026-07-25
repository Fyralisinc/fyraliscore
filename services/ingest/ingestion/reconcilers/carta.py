"""services/ingest/ingestion/reconcilers/carta.py — gap detection (cap-table).

After an `optionGrant` entity shard completes, its cursor carries
`high_water_modified` — the max `lastModifiedDatetime` the fetcher walked. The
reconciler probes the LIVE issuer for any grant modified after the high-water
(`GET /v1alpha1/issuers/{issuer}/optionGrants?pageSize=1&
lastModifiedDatetimeAfter=<hw>`); if one exists, a reshare is emitted for that
shard, warm-started at the high-water (incremental mode).

ONLY optionGrants is probeable: it is the one `/v1alpha1` collection with a
server-side modified-since filter (CONFIRMED — see
integrations/carta/client.py). The other entity types (stakeholder /
shareClass / convertibleNote) carry no modification timestamp and no delta
filter, so there is no cheap gap probe for them — their shards are skipped here
and rely on the periodic full re-walk, which is idempotent via the
content-digest-versioned external_id
(`carta:{issuer}:{kind}:{id}:{version}`).

`external_id` parity means re-walked entities dedup against what backfill
already wrote — only genuinely new/changed entities produce new observations.
Pragmatic v1: one cheap 1-row query per grant shard; it can over-reshare but
never under-reshares, and dedup makes re-walks idempotent.

> **TODO(human):** the rendered docs don't state whether
> `lastModifiedDatetimeAfter` is inclusive ("on or after") or strictly after.
> The gap-probe's clean-termination assumes STRICTLY-after (an inclusive bound
> would re-match the high-water row every pass and reshare forever); the
> synthetic mock implements strictly-after. Verify on first real
> (partner-gated) traffic and, if inclusive, nudge the probe bound.
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


SHARD_KIND_ENTITY = "carta_entity"
RESHARE_RECENCY_SCORE = 1.5
# The only /v1alpha1 collection with a modified-since filter to probe.
_PROBEABLE_ENTITY_TYPE = "optionGrant"


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.carta: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_carta_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.carta import _open_carta_client
    return await _open_carta_client(install)


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
        hw = cursor.get("high_water_modified")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_ENTITY:
        return None
    entity_type = identifier.get("entity_type")
    if entity_type != _PROBEABLE_ENTITY_TYPE:
        # No server-side delta filter for the other collections — nothing
        # cheap to probe; full re-walks stay idempotent (module docstring).
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None

    try:
        rows, _ = await client.list_option_grants(
            page_size=1, modified_after=high_water,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.carta.probe_failed",
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


async def reconcile_carta(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="carta",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_carta_client(install)
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
            message=f"carta reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = ["reconcile_carta", "set_pool_provider", "SHARD_KIND_ENTITY"]
