"""services/ingest/ingestion/planners/carta.py — Carta planner (cap-table).

Per the Gusto loader precedent (A18.2): Carta's install record is issuer-scoped
(`carta_installations.firm_id` holds the Carta issuer id), and the planner needs
the 1-to-N active-entity list to emit one shard per entity type. The enrichment
lives in `carta_entities`; the SourceOnboarding loader JSON-aggregates it into
`ctx.install["entities"]` so the planner stays stateless.

Each shard is one cap-table entity type's stream for the issuer. The seeded
entity types match the real `/v1alpha1` issuer collections (CONFIRMED — see
integrations/carta/client.py): `stakeholder` / `shareClass` / `optionGrant` /
`convertibleNote`. The fetcher walks `GET /v1alpha1/issuers/{issuer}/
{collection}` with AIP-158 `pageToken` pagination; incrementally, only
`optionGrant` has a server-side delta filter (`lastModifiedDatetimeAfter`) — the
per-entity `updated_cursor` warm-starts it. The other entity types full-re-walk
idempotently (the planner stays agnostic; the fetcher decides).

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "carta_entity"


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


async def plan_shards_carta(ctx: PlannerContext) -> list[Shard]:
    """One `carta_entity` shard per active entity type."""
    install_id = str(ctx.install["id"])
    firm_id = str(ctx.install["firm_id"]) if "firm_id" in ctx.install else None
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
                "firm_id": firm_id,
                "installation_id": install_id,
                # The lastModifiedDatetime high-water cursor — None on first
                # sync; only honoured by the optionGrant fetcher path.
                "updated_cursor": ent.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.carta.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["carta"] = plan_shards_carta


__all__ = ["SHARD_KIND_ENTITY", "plan_shards_carta"]
