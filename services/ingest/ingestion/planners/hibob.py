"""services/ingest/ingestion/planners/hibob.py — HiBob planner (People/HR).

Per the Gusto/Carta loader precedent (A18.2): HiBob's install record is
company-scoped, and the planner needs the 1-to-N active-entity list to emit one
shard per entity type. The enrichment lives in `hibob_entities`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["entities"]` so the
planner stays stateless.

Each shard is one People/HR entity type's stream for the company. The fetcher
walks the read endpoint, then incrementally via the per-entity `modified`
high-water cursor (`updated_cursor`).

TODO(human): confirm the HiBob resource taxonomy to shard. This planner is
    entity-type-agnostic (it reads the active entity list from the
    `hibob_entities` child table); the seeded entities are `employee`,
    `lifecycle`, `timeoff`, and `payroll`, each scoped by the HiBob company id.
    The employee directory (People Search) is the highest-signal entity; add the
    others as their read surface is confirmed.

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "hibob_entity"


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


async def plan_shards_hibob(ctx: PlannerContext) -> list[Shard]:
    """One `hibob_entity` shard per active entity type."""
    install_id = str(ctx.install["id"])
    company_id = (
        str(ctx.install["company_id"]) if "company_id" in ctx.install else None
    )
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
                "company_id": company_id,
                "installation_id": install_id,
                # The high-water `modified` cursor — None on first sync.
                "updated_cursor": ent.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.hibob.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["hibob"] = plan_shards_hibob


__all__ = ["SHARD_KIND_ENTITY", "plan_shards_hibob"]
