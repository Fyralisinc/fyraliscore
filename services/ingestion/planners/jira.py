"""services/ingestion/planners/jira.py — Jira planner (IN-17).

Per ingestion LLD §3 + the Gmail/Calendar loader precedent (A18.2): Jira's
install record is site-scoped, but the planner needs the 1-to-N active-project
list to emit one shard per project. The enrichment lives in `jira_projects`;
the SourceOnboarding loader JSON-aggregates it into `ctx.install["projects"]`
so the planner stays stateless (no DB I/O), exactly like Calendar's calendar
aggregation.

Each shard is one project's issue stream (`project = KEY ORDER BY updated
ASC`). The fetcher walks `/rest/api/3/search` with `expand=changelog` on first
run, then incrementally via the per-project `updated` high-water cursor.

`ctx.source_client` is None — projects are read from DB state (populated at
seed/install time by `JiraClient.list_projects`), same as Calendar.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_PROJECT_ISSUES = "jira_project_issues"


def _decode_projects(install: Any) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated `projects` column on the install record."""
    raw = install["projects"] if "projects" in install else None
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
    return [p for p in decoded if isinstance(p, dict)]


async def plan_shards_jira(ctx: PlannerContext) -> list[Shard]:
    """One `jira_project_issues` shard per active project.

    Reads DB state only (projects pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Calendar/Gmail.
    """
    install_id = str(ctx.install["id"])
    projects = _decode_projects(ctx.install)

    shards: list[Shard] = []
    for proj in projects:
        project_key = proj.get("project_key")
        if not isinstance(project_key, str) or not project_key:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_PROJECT_ISSUES,
            shard_identifier={
                "shard_kind": SHARD_KIND_PROJECT_ISSUES,
                "project_key": project_key,
                "project_id": proj.get("project_id"),
                "installation_id": install_id,
                # The high-water `updated` cursor — None on first full sync.
                "updated_cursor": proj.get("updated_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.jira.planned",
        extra={"project_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["jira"] = plan_shards_jira


__all__ = ["SHARD_KIND_PROJECT_ISSUES", "plan_shards_jira"]
