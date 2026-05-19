"""MockGithubClient — GitHub API surface used by M6.4 backfill.

Implements the three methods M6.4 planner/fetcher/reconciler call:
  - list_installation_repositories(installation_id) -> list[str] | None
  - list_repo_events(owner, repo, event_type, page, per_page, etag)
    -> tuple[list[dict], etag, next_page]
  - head_repo_events(owner, repo, event_type, etag)
    -> tuple[has_changes, etag]

Stateful: tracks etag state per (repo, event_type); head_repo_events
returns 304-equivalent (`has_changes=False`) when the etag matches.
list_repo_events paginates by `page` (1-indexed); next_page is None on
the last page.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import GithubApiError
from services.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.synthetic.mock_clients._base import _MockBase


class MockGithubClient(_MockBase):
    """Stateful in-process replacement for `GithubClient`.

    `fixture` shape (per `make_github_repos`):
        {
          "installation_id": "12345",
          "repos": [
            {
              "full_name": "octo/repo-a",
              "events_by_type": {
                "issues": [{"id": "...", "updated_at": "...", ...}, ...],
                "pull_requests": [{...}, ...],
              },
            },
            ...
          ],
          "per_page": 30,
        }
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._etags: dict[tuple[str, str, str], str] = {}

    # ---- M6.4 surface ----
    async def list_installation_repositories(
        self, installation_id: str,
    ) -> list[str] | None:
        self._check_fault()
        return [r["full_name"] for r in self._fixture["repos"]]

    async def list_repo_events(
        self,
        *,
        owner: str,
        repo: str,
        event_type: str,
        page: int = 1,
        per_page: int = 30,
        etag: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, int | None]:
        self._check_fault()
        full_name = f"{owner}/{repo}"
        events = self._events_for(full_name, event_type)
        per_page = min(per_page, int(self._fixture.get("per_page", per_page)))
        start = (page - 1) * per_page
        end = start + per_page
        page_records = events[start:end]
        next_page = page + 1 if end < len(events) else None
        new_etag = f'W/"{full_name}:{event_type}:v{len(events)}"'
        self._etags[(owner, repo, event_type)] = new_etag
        return page_records, new_etag, next_page

    async def head_repo_events(
        self,
        *,
        owner: str,
        repo: str,
        event_type: str,
        etag: str | None = None,
    ) -> tuple[bool, str]:
        self._check_fault()
        full_name = f"{owner}/{repo}"
        events = self._events_for(full_name, event_type)
        current_etag = f'W/"{full_name}:{event_type}:v{len(events)}"'
        has_changes = etag != current_etag
        return has_changes, current_etag

    # ---- Helpers ----
    def _events_for(
        self, full_name: str, event_type: str,
    ) -> list[dict[str, Any]]:
        for r in self._fixture["repos"]:
            if r["full_name"] == full_name:
                return list(r.get("events_by_type", {}).get(event_type, []))
        return []

    # ---- Fault raisers ----
    def _raise_rate_limit(self) -> NoReturn:
        # GitHub's secondary-rate-limit surface is GithubApiError;
        # production has no separate GithubRateLimitError class.
        raise GithubApiError(
            "MockGithubClient: secondary rate limit (X2 fault)",
        )

    def _raise_5xx(self) -> NoReturn:
        raise GithubApiError("MockGithubClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise GithubApiError("MockGithubClient: 401 bad credentials (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise GithubApiError(
            "MockGithubClient: transient transport error (X2 fault)",
        )
