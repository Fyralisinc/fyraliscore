"""M6.4 GitHub clean-path helper. Patches the planner's
source-client factory (in source_onboarding) AND the fetcher /
reconciler seams. Clean path: reconciler etag-fast-path returns no
changes → no gap.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import github as gh_fetcher
from services.ingestion.reconcilers import github as gh_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FakeClient:
    """Both the planner's client AND the fetcher/reconciler client."""

    async def list_installation_repositories(self, installation_id):
        return ["acme/api"]

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        # 1 page, 2 records, end-of-data.
        return ([
            {"id": 1, "title": "issue-1",
             "updated_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "title": "issue-2",
             "updated_at": "2025-01-02T00:00:00Z"},
        ], "W/clean-etag", None)

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        # Etag fast-path: no changes since fetcher's stored etag.
        return (False, etag)


# Source-onboarding planner-side build:
async def _fake_build_source_client(source, pool, install):
    if source == "github":
        return _FakeClient()
    return None


# Fetcher seam:
async def _fake_fetcher_open(install):
    async def close(): return None
    return _FakeClient(), close


# Reconciler seam:
async def _fake_reconciler_open(install):
    async def close(): return None
    return _FakeClient(), close


so_mod._build_source_client = _fake_build_source_client
gh_fetcher._open_github_client = _fake_fetcher_open
gh_reconciler._open_github_client = _fake_reconciler_open
