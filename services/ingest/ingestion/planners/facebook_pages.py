"""Facebook Pages backfill planner."""
from __future__ import annotations

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


SHARD_KIND_PAGE_HISTORY = "facebook_page_history"


async def plan_shards_facebook_pages(ctx: PlannerContext) -> list[Shard]:
    install_id = str(ctx.install["id"])
    page_id = str(ctx.install["page_id"])
    return [
        Shard(
            shard_kind=SHARD_KIND_PAGE_HISTORY,
            shard_identifier={
                "shard_kind": SHARD_KIND_PAGE_HISTORY,
                "installation_id": install_id,
                "page_id": page_id,
                "page_name": ctx.install["page_name"] if "page_name" in ctx.install else None,
                "backfill_mode": "All available history",
            },
            recency_score=1.0,
            window_start=None,
            window_end=None,
        )
    ]


PLANNER_DISPATCH["facebook_pages"] = plan_shards_facebook_pages


__all__ = ["SHARD_KIND_PAGE_HISTORY", "plan_shards_facebook_pages"]
