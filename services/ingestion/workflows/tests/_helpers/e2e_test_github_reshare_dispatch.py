"""M6.4 GitHub reshare-path helper. First reconciler pass
detects gap; gap-fill shard backfills; second pass clean.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import github as gh_fetcher
from services.ingestion.reconcilers import github as gh_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _PlannerFetcherClient:
    """Used by planner + fetcher subprocesses. Backfill returns records
    with updated_at <= 2025-01-01; gap-fill fetcher returns 1 record.
    """

    def __init__(self):
        self.fetch_calls = 0

    async def list_installation_repositories(self, installation_id):
        return ["acme/api"]

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        self.fetch_calls += 1
        return ([
            {"id": 1, "title": "issue-1",
             "updated_at": "2025-01-01T00:00:00Z"},
        ], "W/etag-page-1", None)


class _ReconcilerClient:
    """Reconciler-side: stateful so we converge.

    Pass-0: head says changes; list returns newer record → gap.
    Pass-1 onwards: head says no changes → clean.
    """

    def __init__(self):
        self.head_calls = 0

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        self.head_calls += 1
        # First call returns changes; subsequent calls (pass-1+) return
        # clean so the cycle converges.
        return (self.head_calls == 1, f"W/etag-call-{self.head_calls}")

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        return ([
            {"id": 99, "updated_at": "2025-02-01T00:00:00Z"},
        ], "W/post-etag", None)


_PFC = _PlannerFetcherClient()
_REC_CLIENT = _ReconcilerClient()


async def _fake_build_source_client(source, pool, install):
    if source == "github":
        return _PFC
    return None


async def _fake_fetcher_open(install):
    async def close(): return None
    return _PFC, close


async def _fake_reconciler_open(install):
    async def close(): return None
    return _REC_CLIENT, close


so_mod._build_source_client = _fake_build_source_client
gh_fetcher._open_github_client = _fake_fetcher_open
gh_reconciler._open_github_client = _fake_reconciler_open
