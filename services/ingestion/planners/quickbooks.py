"""services/ingestion/planners/quickbooks.py — QuickBooks planner (finance).

Per the Jira loader precedent (A18.2): QuickBooks' install record is realm-scoped,
and the planner needs the 1-to-N active-entity list to emit one shard per entity
type. The enrichment lives in `quickbooks_entities`; the SourceOnboarding loader
JSON-aggregates it into `ctx.install["entities"]` so the planner stays stateless.

Each shard is one entity type's stream (Invoice / Bill / BillPayment / Payment)
for the realm. The fetcher walks the query endpoint, then incrementally via the
per-entity LastUpdatedTime high-water cursor.

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "quickbooks_entity"


def _decode_entities(install: Any) -> list[dict[str, Any]]:
    raw = install["entities"] if "entities" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [e for e in decoded if isinstance(e, dict)]


async def plan_shards_quickbooks(ctx: PlannerContext) -> list[Shard]:
    """One `quickbooks_entity` shard per active entity type."""
    install_id = str(ctx.install["id"])
    realm_id = str(ctx.install["realm_id"]) if "realm_id" in ctx.install else None
    entities = _decode_entities(ctx.install)

    shards: list[Shard] = []
    for ent in entities:
        entity_type = ent.get("entity_type")
        if not isinstance(entity_type, str) or not entity_type:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_ENTITY,
            shard_identifier={
                "shard_kind": SHARD_KIND_ENTITY,
                "entity_type": entity_type,
                "realm_id": realm_id,
                "installation_id": install_id,
                # The high-water LastUpdatedTime cursor — None on first sync.
                "updated_cursor": ent.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.quickbooks.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["quickbooks"] = plan_shards_quickbooks


__all__ = ["SHARD_KIND_ENTITY", "plan_shards_quickbooks"]
