"""services/ingestion/planners/notion.py — Notion backfill planner (IN-14).

Per ingestion LLD §3 + A18.6 (PlannerContext for API-at-plan-time
enumeration).

============================================================
ROLE
============================================================
Decomposes one Notion workspace install into shards:

  - one `notion_database` shard per database the integration can see
    (`POST /v1/search?filter=database`, fully paginated). Each database
    shard's fetcher walks the DB's rows → each row's body blocks → each
    row's comments.
  - one `notion_page_tree` shard for LOOSE pages — pages that are not
    rows of a database (`POST /v1/search?filter=page`). The fetcher walks
    each loose page's blocks + comments.

This split mirrors GitHub's per-(repo, event_type) sharding: it bounds
each fetch unit and lets recency ordering run recently-edited databases
first (high-value intent signal lands early under the rate limit).

============================================================
WIRE-IN
============================================================
Assigns into `PLANNER_DISPATCH['notion']` at module-import time;
`services/ingestion/planners/__init__.py` imports this module.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_DATABASE = "notion_database"
SHARD_KIND_PAGE_TREE = "notion_page_tree"
_RECENCY_TAU_DAYS = 7.0


def _recency_score(last_edited_time: Any) -> float:
    """exp(-age_days/τ); higher = more recent = run earlier. Defaults to
    1.0 when the timestamp is missing/unparseable (neutral priority)."""
    if not isinstance(last_edited_time, str):
        return 1.0
    s = last_edited_time
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(s)
    except ValueError:
        return 1.0
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (datetime.now(timezone.utc) - ts).total_seconds() / 86400.0)
    return math.exp(-age_days / _RECENCY_TAU_DAYS)


async def _paginate_search(
    client: Any, object_filter: str,
) -> list[dict[str, Any]]:
    """Fully paginate `POST /v1/search` for one object type."""
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        results, next_cursor, has_more = await client.search(
            object_filter=object_filter, start_cursor=cursor,
        )
        out.extend(results)
        if not has_more or not next_cursor:
            break
        cursor = next_cursor
    return out


async def plan_shards_notion(ctx: PlannerContext) -> list[Shard]:
    """One `notion_database` shard per visible database + one
    `notion_page_tree` shard for loose pages."""
    if ctx.source_client is None:
        raise RuntimeError(
            "Notion planner called with source_client=None; the "
            "PlannerContext factory must supply a NotionClient. "
            "See _build_source_client in source_onboarding.py."
        )
    workspace_id = str(ctx.install["installation_id"])
    shards: list[Shard] = []

    databases = await _paginate_search(ctx.source_client, "database")
    for db in databases:
        db_id = db.get("id")
        if not isinstance(db_id, str) or not db_id:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_DATABASE,
            shard_identifier={
                "shard_kind": SHARD_KIND_DATABASE,
                "database_id": db_id,
                "workspace_id": workspace_id,
            },
            recency_score=_recency_score(db.get("last_edited_time")),
            window_start=None, window_end=None,
        ))

    # A single page-tree shard sweeps loose (non-database-row) pages. The
    # fetcher filters out pages whose parent is a database (those are
    # covered by their database shard) to avoid double-walking.
    shards.append(Shard(
        shard_kind=SHARD_KIND_PAGE_TREE,
        shard_identifier={
            "shard_kind": SHARD_KIND_PAGE_TREE,
            "workspace_id": workspace_id,
        },
        recency_score=1.0,
        window_start=None, window_end=None,
    ))

    log.info(
        "planners.notion.planned",
        extra={"database_shards": len(databases), "workspace_id": workspace_id},
    )
    return shards


PLANNER_DISPATCH["notion"] = plan_shards_notion


__all__ = [
    "SHARD_KIND_DATABASE",
    "SHARD_KIND_PAGE_TREE",
    "plan_shards_notion",
]
