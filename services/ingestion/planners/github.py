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
EVENT TYPES (gap-closure — full mandatory signal set)
============================================================
Backfill scope: one shard per (repo, event_type), covering the
mandatory CompanyOS GitHub signal set at parity with the live handler:

  - Class A (repo-level list): issues, pull_requests, issue_comments,
    commits.
  - Class B (PR-parent fan-out): pr_reviews (always on; bounded by PR
    count), check_runs (OPTIONAL — gated by GITHUB_BACKFILL_CHECK_RUNS=1,
    default off, because per-PR-head check-run fan-out is the
    highest-cost / lowest-ROI signal).

With ~20 repos/tenant typical and 5 always-on event_types =
~100 shards/tenant (~120 with check_runs). The settled-decision target
of ~250 leaves headroom. See docs/ingestion/github-backfill-gap-closure.md.

============================================================
ALL-REPOS vs SELECTED-REPOS MODE
============================================================
`GET /installation/repositories` enumerates the repos accessible to the
installation in BOTH selected and all-repos (org-wide) mode. The planner
uses `list_repositories_for_backfill`, which fully paginates that
endpoint and returns the concrete repo list regardless of mode — so an
org-wide grant is supported and a large selection is not silently
truncated. (`list_installation_repositories`, which returns None as the
all-repos signal, remains for the OAuth callback's
`selected_repositories` column.)

============================================================
WIRE-IN
============================================================
This module assigns into `PLANNER_DISPATCH['github']` at module-
import time. `services/ingestion/planners/__init__.py` imports the
module to trigger the assignment.
"""
from __future__ import annotations

import logging
import os

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_REPO_EVENTS = "github_repo_events"
# Always-on event types (one shard per (repo, event_type)).
EVENT_TYPES = (
    "issues", "pull_requests", "issue_comments", "commits", "pr_reviews",
)
# Opt-in via GITHUB_BACKFILL_CHECK_RUNS=1 — expensive per-PR-head fan-out.
OPTIONAL_EVENT_TYPES = ("check_runs",)


def _effective_event_types() -> tuple[str, ...]:
    """`EVENT_TYPES` plus any opt-in types whose env flag is set."""
    if os.environ.get("GITHUB_BACKFILL_CHECK_RUNS", "") == "1":
        return EVENT_TYPES + OPTIONAL_EVENT_TYPES
    return EVENT_TYPES


async def plan_shards_github(ctx: PlannerContext) -> list[Shard]:
    """One Shard per (repo, event_type) for this install.

    Uses `ctx.source_client.list_repositories_for_backfill(installation_id)`
    to enumerate every accessible repo (selected OR all-repos mode, fully
    paginated). Each repo gets `len(EVENT_TYPES)` shards.
    """
    install = ctx.install
    installation_id = str(install["installation_id"])
    if ctx.source_client is None:
        raise RuntimeError(
            "GitHub planner called with source_client=None; the "
            "PlannerContext factory must supply a GithubClient. "
            "See _build_source_client in source_onboarding.py."
        )
    # `list_repositories_for_backfill` fully enumerates the installation's
    # accessible repos regardless of selected/all-repos mode (org-wide
    # grants included) and is not capped at 90 — so a customer who
    # installs the App org-wide, or on >90 repos, backfills completely.
    repos = await ctx.source_client.list_repositories_for_backfill(
        installation_id,
    )
    event_types = _effective_event_types()
    shards: list[Shard] = []
    for repo_full_name in repos:
        if "/" not in repo_full_name:
            log.warning(
                "planners.github.invalid_repo_name",
                extra={"repo": repo_full_name},
            )
            continue
        owner, repo = repo_full_name.split("/", 1)
        for event_type in event_types:
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


__all__ = [
    "EVENT_TYPES",
    "OPTIONAL_EVENT_TYPES",
    "SHARD_KIND_REPO_EVENTS",
    "plan_shards_github",
]
