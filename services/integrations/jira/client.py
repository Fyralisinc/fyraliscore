"""services/integrations/jira/client.py — outbound Jira Cloud REST v3 client.

Single outbound surface for backfill + poll-incremental + the planner's
project enumeration. Jira Cloud is authenticated with HTTP **Basic auth** =
`base64(account_email:api_token)` against a per-tenant site base URL
(`https://<site>.atlassian.net`). The API token is LONG-LIVED (issued from
id.atlassian.com), so — like the Notion client — there is no per-request
token mint; it is resolved once from the secret store (or preset in spammer
mode) and reused for the life of the client.

Rate limits: Jira Cloud returns HTTP 429 with a `Retry-After` header. All
read methods route through `_request`, which honours `Retry-After` with a
bounded retry budget before surfacing `JiraApiError(jira_api_rate_limited)`
— the same posture as the Notion/GitHub clients.

Pagination: classic Jira list endpoints return
`{startAt, maxResults, total, values|issues: [...]}`. The list helpers return
`(items, next_start_at, is_last)`; `next_start_at is None` is terminal.

Logging redaction: the API token and the Basic-auth header are NEVER logged.
The base URL host is hashed before it touches a log line.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import JiraApiError


log = structlog.get_logger("integrations.jira.client")


_DEFAULT_TIMEOUT_S = 30.0
# Jira Cloud's /rest/api/3/search maxResults is capped at 100 for issue
# search; project search allows up to 50.
_DEFAULT_PAGE_SIZE = 100
_PROJECT_PAGE_SIZE = 50

# Issue fields the fetcher needs to build observations. Kept explicit (not
# `*all`) to bound payload size; `comment` and the changelog (via expand) are
# what carry the comment + transition signals.
DEFAULT_ISSUE_FIELDS = (
    "summary,description,issuetype,status,priority,assignee,reporter,creator,"
    "created,updated,resolution,resolutiondate,labels,components,parent,"
    "project,comment,customfield_10016,customfield_10020"
)
# customfield_10016 = Story Points (common default); customfield_10020 = Sprint
# (common default). Absent fields are simply ignored by the handler.


def short_host_hash(base_url: str) -> str:
    """Non-reversible 16-hex digest of the site host for logs."""
    return hashlib.blake2b(base_url.encode("utf-8"), digest_size=8).hexdigest()


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class JiraClient:
    """Outbound Jira Cloud REST v3 client, one per backfill/poll shard open.

    Built by `services/ingestion/fetchers/_clients.py::build_jira_client`
    (production / spammer) and by the seed/onboarding project probe. Shares
    the process-wide httpx client when one is injected.
    """

    def __init__(
        self,
        *,
        base_url: str,
        account_email: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        api_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._account_email = account_email
        # Preset token (spammer mode presets a recognized token); otherwise
        # resolved lazily from the secret store on first request.
        self._api_token: str | None = api_token
        self._token_lock = asyncio.Lock()
        # In production the base is the per-install site URL; a spammer/test
        # override (api_base_url) wins so backfill can point at the mock.
        self._api_base_url = (api_base_url or base_url).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client

    def _httpx(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_S)
            self._owns_client = True
        return self._http

    async def aclose(self) -> None:
        if self._owns_client and self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _token(self) -> str:
        if self._api_token is not None:
            return self._api_token
        async with self._token_lock:
            if self._api_token is not None:
                return self._api_token
            if (
                self._secret_store is None
                or self._secret_ref is None
                or self._tenant_id is None
            ):
                raise JiraApiError(
                    "jira client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="jira_api_unauthorized",
                )
            raw = await self._secret_store.get(
                self._secret_ref, tenant_id=self._tenant_id,
            )
            self._api_token = (
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
            return self._api_token

    async def _auth_header(self) -> str:
        token = await self._token()
        creds = f"{self._account_email}:{token}".encode("utf-8")
        return "Basic " + base64.b64encode(creds).decode("ascii")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One Jira API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after
        the budget is spent) is mapped to `JiraApiError`.
        """
        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        max_attempts = int(os.environ.get("JIRA_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("JIRA_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.request(
                    method, url, headers=headers,
                    json=json_body, params=params,
                )
            except httpx.TransportError as exc:
                raise JiraApiError(
                    "transport error calling jira",
                    code="jira_api_error",
                    context={"error_type": type(exc).__name__, "path": path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise JiraApiError(
                        "jira response was not a JSON object",
                        code="jira_api_error",
                        context={"path": path},
                    )
                return body

            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def search_issues(
        self,
        *,
        jql: str,
        next_page_token: str | None = None,
        max_results: int = _DEFAULT_PAGE_SIZE,
        fields: str | None = None,
        expand: str | None = "changelog",
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`POST /rest/api/3/search/jql` — issues matching `jql`.

        NOTE (real-API, 2025): Atlassian REMOVED the classic
        `/rest/api/3/search` (returns 410) in favour of `/search/jql`, which
        is **token-paginated** — no `startAt`/`total`. Pass the `nextPageToken`
        from the previous page; the response carries the next one (absent on
        the last page) + `isLast`. `expand=changelog` inlines each issue's
        changelog histories (the transition signal).

        Returns `(issues, next_page_token, is_last)`. `next_page_token is None`
        (== `is_last`) signals no more pages.
        """
        body: dict[str, Any] = {
            "jql": jql,
            "maxResults": max_results,
            "fields": (fields or DEFAULT_ISSUE_FIELDS).split(","),
        }
        if expand:
            body["expand"] = expand  # /search/jql takes expand as a string
        if next_page_token:
            body["nextPageToken"] = next_page_token
        resp = await self._request("POST", "/rest/api/3/search/jql", json_body=body)
        issues = resp.get("issues")
        issues = [i for i in issues if isinstance(i, dict)] if isinstance(issues, list) else []
        token = resp.get("nextPageToken")
        token = token if isinstance(token, str) and token else None
        # Terminal when the API says so OR there's no continuation token.
        is_last = bool(resp.get("isLast")) or token is None
        return issues, (None if is_last else token), is_last

    async def list_projects(
        self,
        *,
        start_at: int = 0,
        max_results: int = _PROJECT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /rest/api/3/project/search` — projects visible to the token.

        Returns `(projects, next_start_at, total)`. Used at seed/install time
        to populate `jira_projects`.
        """
        params = {"startAt": start_at, "maxResults": max_results}
        resp = await self._request(
            "GET", "/rest/api/3/project/search", params=params,
        )
        values = resp.get("values")
        values = [v for v in values if isinstance(v, dict)] if isinstance(values, list) else []
        total = int(resp.get("total", 0) or 0)
        is_last = bool(resp.get("isLast", True)) or not values
        next_start = start_at + len(values)
        return values, (None if is_last else next_start), total

    async def has_updates_since(
        self, *, project_key: str, updated_min_jql: str,
    ) -> bool:
        """Cheap reconciler gap probe: are there issues in `project_key`
        updated at/after `updated_min_jql` (a `yyyy/MM/dd HH:mm` JQL literal)?
        Uses `/rest/api/3/search/approximate-count` (the new endpoint's count
        surface, since `/search/jql` no longer returns `total`).

        STRICT `>` (not `>=`) is load-bearing: the high-water mark IS an issue
        the backfill already walked, so an inclusive `>=` would always match it
        and the reconciler would re-share forever (observed: 100+ reshare passes
        hammering the API). `>` converges — it only fires on issues in a strictly
        later minute. Sub-minute updates after the walk (JQL `updated` is
        minute-precision) are caught by the live webhook + the periodic
        reconciler, not this one-shot onboarding probe."""
        safe_key = project_key.replace('"', "")
        jql = f'project = "{safe_key}" AND updated > "{updated_min_jql}"'
        body = await self._request(
            "POST", "/rest/api/3/search/approximate-count",
            json_body={"jql": jql},
        )
        return int(body.get("count", 0) or 0) > 0

    async def myself(self) -> dict[str, Any]:
        """`GET /rest/api/3/myself` — the authenticated account; a cheap
        connectivity + credential probe used by the seed script."""
        return await self._request("GET", "/rest/api/3/myself")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> JiraApiError:
    """Map a non-2xx Jira response to a typed `JiraApiError`."""
    status = response.status_code
    if status in (401, 403):
        return JiraApiError(
            f"jira {status}: API token rejected or insufficient permission",
            code="jira_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return JiraApiError(
            "jira 404: project/issue not found or not visible to the token",
            code="jira_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return JiraApiError(
            "jira rate limit (429), retry budget exhausted",
            code="jira_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return JiraApiError(
        f"jira returned {status}",
        code="jira_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["JiraClient", "DEFAULT_ISSUE_FIELDS", "short_host_hash"]
