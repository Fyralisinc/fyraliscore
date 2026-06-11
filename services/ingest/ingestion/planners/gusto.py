"""services/ingest/ingestion/planners/gusto.py — Gusto planner (finance/payroll).

Per the Jira loader precedent (A18.2): Gusto's install record is company-scoped,
and the planner needs the 1-to-N active-entity list to emit one shard per entity
kind. The enrichment lives in `gusto_entities`; the SourceOnboarding loader
JSON-aggregates it into `ctx.install["entities"]` so the planner stays stateless.

Each shard is one entity kind's stream for the company. The taxonomy (VERIFIED
against docs.gusto.com) is the `/v1/companies/{company_uuid}/...` read surface:

  - `employee` — `GET .../employees` (no updated-since filter → full re-walk,
    version-deduped downstream).
  - `payroll`  — `GET .../payrolls` (date-window filter → `check_date`
    high-water incremental via the per-entity `updated_cursor`).

The planner itself is entity-kind-agnostic (it reads the active entity list
from the `gusto_entities` child table); the seeded kinds come from
`client.DEFAULT_ENTITIES`.

`ctx.source_client` is None — entities are read from DB state.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "gusto_entity"


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


async def plan_shards_gusto(ctx: PlannerContext) -> list[Shard]:
    """One `gusto_entity` shard per active entity kind."""
    install_id = str(ctx.install["id"])
    company_uuid = str(ctx.install["company_uuid"]) if "company_uuid" in ctx.install else None
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
                "company_uuid": company_uuid,
                "installation_id": install_id,
                # The high-water cursor (payroll check_date) — None on first
                # sync and on employee shards (full re-walk).
                "updated_cursor": ent.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.gusto.planned",
        extra={"entity_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["gusto"] = plan_shards_gusto


__all__ = ["SHARD_KIND_ENTITY", "plan_shards_gusto"]
