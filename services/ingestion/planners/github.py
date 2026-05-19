"""services/ingestion/planners/github.py — GitHub backfill planner (M6.4).

Per ingestion LLD §3 + [05-lld-amendments.md A18] (per-source backfill =
net-new code) + A18.6 (PlannerContext for API-at-plan-time
enumeration; M6.4 substrate addition).

============================================================
ROLE
============================================================
Decomposes one GitHub install into a list of `Shard`, one per
(repo, event_type) pair. Uses `ctx.source_client` to enumerate
repos via Octokit's `/installation/repositories` endpoint (per
[GithubClient.list_installation_repositories](../../../services/integrations/github/client.py)).

============================================================
EVENT TYPES (M6.4 — 2 types initially, extensible later)
============================================================
Backfill scope: one shard per (repo, event_type). M6.4 ships TWO
event types — issues + pull_requests — which together cover the
high-signal observation density per the LLD. Other event types
(issue_comments, pr_review_comments, commits) are deferred; their
shards can be added later by extending `EVENT_TYPES`.

With ~20 repos/tenant typical and 2 event_types = ~40 shards/tenant.
The settled-decision target of ~250 leaves headroom for additional
event_types in future work.

============================================================
ALL-REPOS vs SELECTED-REPOS MODE
============================================================
GitHub's `list_installation_repositories` returns:
  - `list[str]` (selected mode) — explicit selection in the App's
    installation grant.
  - `None` (all-repos mode) — App was granted org-wide access.

In all-repos mode the planner cannot enumerate from this endpoint
alone; for M6.4 we mark the install with a `failure_reason` calling
out the unsupported mode. Per-source policy improvement (e.g., use
`/search/repositories?q=org:<name>`) is future work, NOT M6.4 scope.

============================================================
WIRE-IN
============================================================
This module assigns into `PLANNER_DISPATCH['github']` at module-
import time. `services/ingestion/planners/__init__.py` imports the
module to trigger the assignment.
"""
from __future__ import annotations

import logging

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_REPO_EVENTS = "github_repo_events"
EVENT_TYPES = ("issues", "pull_requests")


async def plan_shards_github(ctx: PlannerContext) -> list[Shard]:
    """One Shard per (repo, event_type) for this install.

    Uses `ctx.source_client.list_installation_repositories(installation_id)`
    to enumerate repos. Each repo gets `len(EVENT_TYPES)` shards.

    All-repos mode (returns None) is not yet supported; raises
    `NotImplementedError` (caught by SourceOnboarding → run marked
    failed with a clear reason).
    """
    install = ctx.install
    installation_id = str(install["installation_id"])
    if ctx.source_client is None:
        raise RuntimeError(
            "GitHub planner called with source_client=None; the "
            "PlannerContext factory must supply a GithubClient. "
            "See _build_source_client in source_onboarding.py."
        )
    repos = await ctx.source_client.list_installation_repositories(
        installation_id,
    )
    if repos is None:
        raise NotImplementedError(
            "GitHub planner: all-repositories mode is not yet supported "
            "in M6.4. Install has org-wide grant; per-repo enumeration "
            "via search endpoint is deferred. Mark install as scoped "
            "via the GitHub App settings to unblock backfill."
        )
    shards: list[Shard] = []
    for repo_full_name in repos:
        if "/" not in repo_full_name:
            log.warning(
                "planners.github.invalid_repo_name",
                extra={"repo": repo_full_name},
            )
            continue
        owner, repo = repo_full_name.split("/", 1)
        for event_type in EVENT_TYPES:
            shards.append(Shard(
                shard_kind=SHARD_KIND_REPO_EVENTS,
                shard_identifier={
                    "shard_kind": SHARD_KIND_REPO_EVENTS,
                    "repo_full_name": repo_full_name,
                    "owner": owner,
                    "repo": repo,
                    "event_type": event_type,
                    "installation_id": installation_id,
                },
                recency_score=1.0,
                window_start=None, window_end=None,
            ))
    return shards


PLANNER_DISPATCH["github"] = plan_shards_github


__all__ = ["EVENT_TYPES", "SHARD_KIND_REPO_EVENTS", "plan_shards_github"]
