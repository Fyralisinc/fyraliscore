"""MockJiraClient — Jira Cloud REST v3 surface used by IN-17 backfill/poll.

Stateless in-process replacement for `JiraClient`
(`services/ingest/integrations/jira/client.py`). Implements the methods the
production fetcher (`fetchers/jira.py`) and reconciler (`reconcilers/jira.py`)
call against the `_open_jira_client` seam:

  - search_issues(jql=..., next_page_token=..., max_results=...)
      -> (issues: list[dict], next_page_token: str|None, is_last: bool)
  - has_updates_since(project_key=..., updated_min_jql=...) -> bool
  - list_projects(start_at=..., max_results=...)
      -> (projects: list[dict], next_start_at: int|None, total: int)  (seed probe)
  - myself() -> dict   (connectivity/credential probe)

`search_issues` mirrors the REAL `/rest/api/3/search/jql` contract: it is
TOKEN-paginated (no startAt/total). The mock derives the target project from
the JQL (`project = "KEY"`), honours the `updated >= "<floor>"` incremental
clause when present, and pages by an opaque `next_page_token` capped at the
fixture's `page_size`. The returned `next_page_token is None` iff `is_last`.

Faults: every public method calls `self._check_fault()` first (A21). The four
raisers surface `JiraApiError` with the production `code` values so the
fetcher/reconciler branch exactly as they would against the real client (the
fetcher keys its rate-limit fallback on `code == "jira_api_rate_limited"`).
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, NoReturn

from lib.shared.errors import JiraApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


# `project = "ENG"` (single or double quoted) — the production fetcher always
# emits a double-quoted key (`_build_jql`).
_PROJECT_RE = re.compile(r'project\s*=\s*["\']([^"\']+)["\']')
# `updated >= "2026/01/05 00:30"` — the incremental floor (yyyy/MM/dd HH:mm).
_FLOOR_RE = re.compile(r'updated\s*>=\s*["\']([^"\']+)["\']')


class MockJiraClient(_MockBase):
    """In-process replacement for `JiraClient`, driven by a `make_jira` fixture.

    `fixture` shape (per `make_jira`):
        {
          "site_host": "acme.atlassian.net",
          "page_size": 50,
          "projects": [
            {"project_key": "ENG", "project_id": "10000",
             "issues": [ <raw /search/jql issue dict>, ... ]},  # updated ASC
            ...
          ],
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
        self._page_size = int(fixture.get("page_size", 50)) or 50

    # ---------------------------------------------------------------
    # Public read surface (mirrors JiraClient)
    # ---------------------------------------------------------------
    async def search_issues(
        self,
        *,
        jql: str,
        next_page_token: str | None = None,
        max_results: int = 100,
        fields: str | None = None,
        expand: str | None = "changelog",
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """Token-paginated `/search/jql`. Returns `(issues, next_token, is_last)`.

        The token is the opaque string `"off:<n>"` encoding the next start
        offset into the (floor-filtered) issue list, so the fetcher can thread
        it back verbatim — exactly like the real `nextPageToken`.
        """
        self._check_fault()

        issues = self._issues_for_jql(jql)

        start = self._decode_token(next_page_token)
        per_page = min(int(max_results or self._page_size), self._page_size)
        end = start + per_page
        page = issues[start:end]

        is_last = end >= len(issues)
        token = None if is_last else self._encode_token(end)
        return page, token, is_last

    async def has_updates_since(
        self, *, project_key: str, updated_min_jql: str,
    ) -> bool:
        """Reconciler gap probe: any issue in `project_key` updated at/after
        `updated_min_jql` (an inclusive `yyyy/MM/dd HH:mm` JQL literal)?"""
        self._check_fault()
        floor = self._parse_jql_minute(updated_min_jql)
        for proj in self._fixture.get("projects", []):
            if proj.get("project_key") != project_key:
                continue
            for issue in proj.get("issues", []):
                updated = (issue.get("fields") or {}).get("updated")
                ts = self._parse_jira_ts(updated)
                if floor is None or (ts is not None and ts >= floor):
                    return True
        return False

    async def list_projects(
        self,
        *,
        start_at: int = 0,
        max_results: int = 50,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`/rest/api/3/project/search` — projects visible to the token.
        Returns `(projects, next_start_at, total)` (seed/install probe)."""
        self._check_fault()
        projects = [
            {
                "id": p.get("project_id"),
                "key": p.get("project_key"),
                "name": f"Project {p.get('project_key')}",
            }
            for p in self._fixture.get("projects", [])
        ]
        total = len(projects)
        end = start_at + max_results
        page = projects[start_at:end]
        next_start = end if end < total else None
        return page, next_start, total

    async def myself(self) -> dict[str, Any]:
        """`/rest/api/3/myself` — connectivity/credential probe."""
        self._check_fault()
        return {
            "accountId": "mock-account-id",
            "emailAddress": f"bot@{self._fixture.get('site_host', 'mock')}",
            "displayName": "Mock Jira Bot",
        }

    async def aclose(self) -> None:
        """No-op (mock holds no httpx client); present for surface parity."""
        return None

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------
    def _issues_for_jql(self, jql: str) -> list[dict[str, Any]]:
        """Resolve the project from the JQL, then apply the optional
        `updated >=` incremental floor. Issues stay in `updated ASC` order
        (the fixture emits them oldest-first), matching the real ORDER BY."""
        m = _PROJECT_RE.search(jql or "")
        if not m:
            return []
        project_key = m.group(1)
        issues: list[dict[str, Any]] = []
        for proj in self._fixture.get("projects", []):
            if proj.get("project_key") == project_key:
                issues = list(proj.get("issues", []))
                break

        floor_m = _FLOOR_RE.search(jql or "")
        if floor_m:
            floor = self._parse_jql_minute(floor_m.group(1))
            if floor is not None:
                issues = [
                    i for i in issues
                    if (ts := self._parse_jira_ts(
                        (i.get("fields") or {}).get("updated"))) is not None
                    and ts >= floor
                ]
        return issues

    @staticmethod
    def _encode_token(offset: int) -> str:
        return f"off:{offset}"

    @staticmethod
    def _decode_token(token: str | None) -> int:
        if not token:
            return 0
        try:
            return int(token.split(":", 1)[1])
        except (IndexError, ValueError):
            return 0

    @staticmethod
    def _parse_jira_ts(value: Any) -> datetime | None:
        """Parse a Jira wire timestamp (`...+0000`, no colon in offset)."""
        if not isinstance(value, str) or not value:
            return None
        s = value
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        elif len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    @staticmethod
    def _parse_jql_minute(value: str | None) -> datetime | None:
        """Parse a `yyyy/MM/dd HH:mm` JQL datetime literal (minute precision).
        Treated as UTC for the mock's monotonic fixture clock."""
        if not isinstance(value, str) or not value:
            return None
        try:
            return datetime.strptime(value, "%Y/%m/%d %H:%M").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None

    # ---------------------------------------------------------------
    # Fault raisers (production JiraApiError codes — A21)
    # ---------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        raise JiraApiError(
            "MockJiraClient: rate limit (429), retry budget exhausted (X2 fault)",
            code="jira_api_rate_limited",
            context={"http_status": 429},
        )

    def _raise_5xx(self) -> NoReturn:
        raise JiraApiError(
            "MockJiraClient: 503 (X2 fault)",
            code="jira_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise JiraApiError(
            "MockJiraClient: 401 API token rejected (X2 fault)",
            code="jira_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise JiraApiError(
            "MockJiraClient: transient transport error (X2 fault)",
            code="jira_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockJiraClient"]
