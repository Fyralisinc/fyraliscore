"""services/ingest/ingestion/planners/grafana.py — Grafana planner (IN-GRAFANA).

Per ingestion LLD §3 + the Jira/Mercury loader precedent (A18.2). Unlike Jira
(one shard per project) or Mercury (one shard per account), Grafana annotations
and alert state are ORG-WIDE — there is no per-resource sub-table, so the planner
emits exactly ONE `grafana_org_annotations` shard per install. The shard streams
the org's annotations via `GET /api/annotations`.

`ctx.source_client` is None — the planner reads only the install row (loaded by
SourceOnboarding's `_LOAD_GRAFANA_INSTALL_SQL`), same as Calendar/Drive.
"""
from __future__ import annotations

import logging

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ORG_ANNOTATIONS = "grafana_org_annotations"


async def plan_shards_grafana(ctx: PlannerContext) -> list[Shard]:
    """One `grafana_org_annotations` shard for the install.

    Reads DB state only (`ctx.source_client` is None), so it stays stateless —
    same as Calendar/Drive. The warm-start cursor (`annotations_cursor_ms`, the
    high-water annotation `time` in epoch ms) is carried into the shard so a
    re-onboarding runs incrementally; None on first sync -> full walk.
    """
    install = ctx.install
    install_id = str(install["id"])
    base_url = install["base_url"] if "base_url" in install else None
    if not base_url:
        log.warning("planners.grafana.no_base_url", extra={"installation_id": install_id})
        return []

    org_id = install["org_id"] if "org_id" in install else "1"
    updated_cursor = (
        install["annotations_cursor_ms"] if "annotations_cursor_ms" in install else None
    )

    shard = Shard(
        shard_kind=SHARD_KIND_ORG_ANNOTATIONS,
        shard_identifier={
            "shard_kind": SHARD_KIND_ORG_ANNOTATIONS,
            "installation_id": install_id,
            "base_url": base_url,
            "org_id": org_id,
            # High-water annotation `time` (epoch ms) — None on first full sync.
            "updated_cursor": updated_cursor,
        },
        recency_score=1.0,
        window_start=None,
        window_end=None,
    )

    log.info(
        "planners.grafana.planned",
        extra={"installation_id": install_id, "shards": 1},
    )
    return [shard]




__all__ = ["SHARD_KIND_ORG_ANNOTATIONS", "plan_shards_grafana"]
