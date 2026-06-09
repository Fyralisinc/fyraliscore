"""services/ingest/ingestion/planners/linkedin.py — LinkedIn planner.

Per the Carta/Gusto loader precedent (A18.2): LinkedIn's install record is
organization-scoped, and the planner needs the 1-to-N active-entity list to emit
one shard per entity type. The enrichment lives in `linkedin_entities`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["entities"]` so the
planner stays stateless.

Each shard is one people/recruiting entity type's stream for the organization.
The fetcher walks the read endpoint, then incrementally via the per-entity
updated-at high-water cursor.

TODO(human): confirm the LinkedIn resource taxonomy to shard. This planner is
    entity-type-agnostic (it reads the active entity list from the
    `linkedin_entities` child table), but the seeded LinkedIn entities are
    `share`, `social_action`, and `follower_stat`, each scoped by the
    organization URN. ACCESS IS PARTNER-GATED — confirm the high-signal entity
    set against the approved entitlement and add the others as their read surface
    is confirmed.

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "linkedin_entity"


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


async def plan_shards_linkedin(ctx: PlannerContext) -> list[Shard]:
    """One `linkedin_entity` shard per active entity type."""
    install_id = str(ctx.install["id"])
    organization_urn = (
        str(ctx.install["organization_urn"])
        if "organization_urn" in ctx.install else None
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
                "organization_urn": organization_urn,
                "installation_id": install_id,
                # The high-water LastUpdatedTime cursor — None on first sync.
                "updated_cursor": ent.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.linkedin.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["linkedin"] = plan_shards_linkedin


__all__ = ["SHARD_KIND_ENTITY", "plan_shards_linkedin"]
