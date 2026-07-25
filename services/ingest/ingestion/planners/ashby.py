"""services/ingest/ingestion/planners/ashby.py — Ashby planner (Recruiting ATS).

Per the Gusto/Carta loader precedent (A18.2): Ashby's install record is
organization-scoped, and the planner needs the 1-to-N active-entity list to emit
one shard per entity type. The enrichment lives in `ashby_entities`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["entities"]` so the
planner stays stateless.

Each shard is one recruiting entity type's stream for the organization. The
fetcher walks the RPC `.list` endpoint (cursor pagination), then incrementally
via the persisted Ashby `syncToken` (`sync_cursor`) — NOT a timestamp cursor,
unlike the Gusto/Carta archetype.

TODO(human): confirm the Ashby resource taxonomy to shard. This planner is
    entity-type-agnostic (it reads the active entity list from the
    `ashby_entities` child table); the seeded entities are `candidate`,
    `application`, `job`, `interview`, and `offer`. Application + interview
    feedback are the highest-signal funnel entities; add the others as their
    read surface is confirmed.

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "ashby_entity"


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


async def plan_shards_ashby(ctx: PlannerContext) -> list[Shard]:
    """One `ashby_entity` shard per active entity type."""
    install_id = str(ctx.install["id"])
    org_id = str(ctx.install["org_id"]) if "org_id" in ctx.install else None
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
                "org_id": org_id,
                "installation_id": install_id,
                # The persisted Ashby syncToken — None on first sync. The
                # fetcher reads this exact key for incremental warm-start.
                "sync_cursor": ent.get("sync_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.ashby.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = ["SHARD_KIND_ENTITY", "plan_shards_ashby"]
