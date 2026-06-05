"""services/ingest/integrations/github/client.py — outbound GitHub REST client.

Single outbound surface for v1. Mints installation access tokens via
the App-JWT flow (`POST /app/installations/<id>/access_tokens`), caches
them in-process per installation_id, and is the chokepoint that
triggers `_disable_installation_github` on the documented revocation
response shapes (R2):
  - HTTP 401 with body `{"message": "Bad credentials", ...}`
  - HTTP 404 with `documentation_url` ending in `/apps/apps` or
    `/apps/installations`

Other 4xx/5xx is a regular `GithubApiError` and does NOT fire the
chokepoint (preserves retry budget; matches IN-09 posture).

Logging redaction (FR-016 / SC-008): `installation_id_hash` only; the
minted JWT and installation access token are NEVER logged at any level.
"""
from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog

from lib.shared.errors import GithubApiError, GithubJWTError

from services.ingest.integrations.github import metrics
from services.ingest.integrations.github.jwt import mint_app_jwt
from services.ingest.integrations.github.uninstall import (
    _disable_installation_github,
    _short_installation_hash,
)


log = structlog.get_logger("integrations.github.client")


_GITHUB_API_BASE = "https://api.github.com"
_DEFAULT_TIMEOUT_S = 10.0
_TOKEN_NEAR_EXPIRY_S = 60.0  # re-mint if cached token expires within 60s
_APPS_DOC_URL_PATTERN = re.compile(
    r"/rest/apps/(apps|installations)",
    re.IGNORECASE,
)
# M6.4 backfill: maps the shard's REST event_type to the collection path.
_GH_EVENT_PATH = {
    "issues": "issues",
    "pull_requests": "pulls",
    # M6.4 gap-closure (Class A — repo-level list endpoints):
    "issue_comments": "issues/comments",
    "commits": "commits",
}


def _gh_event_query(event_type: str, *, per_page: int, page: int) -> str:
    """Per-event REST query string. issues/pulls + comments support
    `state`/`sort`/`direction`; the commits collection takes neither
    (it pages by sha/since/page only). Always carries per_page+page.
    """
    paging = f"per_page={per_page}&page={page}"
    if event_type == "commits":
        # /commits has no `state`; default ordering is reverse-chronological.
        return paging
    if event_type == "issue_comments":
        # /issues/comments has no `state`; ascending by update for a stable
        # forward scan.
        return f"sort=updated&direction=asc&{paging}"
    # issues / pull_requests
    return f"state=all&sort=updated&direction=asc&{paging}"
_LINK_NEXT_PATTERN = re.compile(r'[?&]page=(\d+)[^>]*>;\s*rel="next"')

# Backfill read-path rate-limit retry. Installation requests hit a
# primary 5000/h limit plus secondary (abuse) limits; honoring
# Retry-After with a bounded budget lets a transient 429 / secondary
# limit be absorbed instead of failing the whole shard. Matches the
# internal retry the Slack and Discord clients already do. Read at call
# time so they are env-configurable (and test-overridable).


def _parse_retry_after(value: str | None) -> float:
    """Seconds to wait from a Retry-After header (integer-seconds form;
    GitHub also uses this for secondary limits). Falls back to 1s."""
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


@dataclass(frozen=True, slots=True)
class CachedInstallationToken:
    """In-process cache entry for a per-installation access token."""

    token: str
    expires_at: datetime  # UTC


@dataclass
class _InstallationContext:
    """Bookkeeping the client needs to fire the chokepoint."""

    tenant_id: UUID
    installation_row_id: UUID


class GithubClient:
    """Outbound GitHub REST client.

    Instantiated once per gateway process (in the app lifespan) and
    shared across requests. Per-installation locks serialize concurrent
    token mints for the same installation (Risk #3).
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        tenant_resolver: Any | None = None,
        backfill_installation_id: str | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._pool = pool
        self._api_base_url = (api_base_url or endpoint("github_api")).rstrip("/")
        self._tenant_resolver = tenant_resolver
        # Installation the backfill read methods (list_repo_events /
        # head_repo_events) mint a token against. The M6.4 fetcher calls
        # those methods WITHOUT an installation_id (mock-client parity),
        # so `_open_github_client` binds it here at open time.
        self._backfill_installation_id = backfill_installation_id
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client
        self._installation_tokens: dict[str, CachedInstallationToken] = {}
        self._token_locks: dict[str, asyncio.Lock] = {}
        self._installation_contexts: dict[str, _InstallationContext] = {}
        self._last_repos_truncated: bool = False
        self._last_repos_total_available: int | None = None

    def _httpx(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_S)
            self._owns_client = True
        return self._http

    async def aclose(self) -> None:
        if self._owns_client and self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _get_with_rl_retry(
        self, url: str, headers: dict[str, str],
    ) -> httpx.Response:
        """GET with bounded Retry-After-aware retry on 429 / secondary
        rate limits (403 + Retry-After). Returns the final response; the
        caller maps a non-2xx (including a still-429 after the budget is
        exhausted) to GithubApiError. Transport errors propagate to the
        caller's existing handler.
        """
        max_attempts = int(os.environ.get("GITHUB_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("GITHUB_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()
        attempt = 0
        while True:
            attempt += 1
            response = await client.get(url, headers=headers)
            rate_limited = response.status_code == 429 or (
                response.status_code == 403
                and response.headers.get("Retry-After") is not None
            )
            if not rate_limited or attempt >= max_attempts:
                return response
            delay = _parse_retry_after(response.headers.get("Retry-After"))
            await asyncio.sleep(min(max_sleep, delay))

    # -----------------------------------------------------------------
    # Public surface
    # -----------------------------------------------------------------

    async def register_installation_context(
        self,
        installation_id: str,
        *,
        tenant_id: UUID,
        installation_row_id: UUID,
    ) -> None:
        """Hand the client the tenant + row mapping for an installation
        so the chokepoint can disable the right row on revocation. Called
        by the OAuth callback after the UPSERT lands and (lazily) by the
        webhook router when it first sees a delivery for an installation.
        """
        self._installation_contexts[installation_id] = _InstallationContext(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
        )

    async def mint_installation_token(self, installation_id: str) -> str:
        """Return a valid installation access token, minting on cache
        miss / near-expiry. Process-local cache; per-installation lock
        prevents stampede.
        """
        cached = self._installation_tokens.get(installation_id)
        if cached is not None and _is_fresh(cached):
            return cached.token

        # Serialize concurrent mints for the same installation_id.
        lock = self._token_locks.setdefault(installation_id, asyncio.Lock())
        async with lock:
            cached = self._installation_tokens.get(installation_id)
            if cached is not None and _is_fresh(cached):
                return cached.token

            try:
                jwt_token = mint_app_jwt()
            except GithubJWTError as exc:
                metrics.record_installation_token_mint(result="error")
                raise GithubApiError(
                    "App JWT mint failed",
                    code="github_jwt_unavailable",
                    context={
                        "jwt_reason": exc.reason,
                    },
                ) from exc

            url = (
                f"{self._api_base_url}/app/installations/"
                f"{installation_id}/access_tokens"
            )
            client = self._httpx()
            try:
                response = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {jwt_token}",
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                    },
                )
            except httpx.TransportError as exc:
                metrics.record_installation_token_mint(result="error")
                raise GithubApiError(
                    "transport error during installation-token mint",
                    code="github_api_error",
                    context={"error_type": type(exc).__name__},
                ) from exc

            metrics.record_outbound_request(
                path="/app/installations/{id}/access_tokens",
                status=response.status_code,
            )

            if response.status_code == 201:
                body = _safe_json(response)
                token = body.get("token") if isinstance(body, dict) else None
                expires_at_str = (
                    body.get("expires_at") if isinstance(body, dict) else None
                )
                if not isinstance(token, str) or not isinstance(
                    expires_at_str, str
                ):
                    metrics.record_installation_token_mint(result="error")
                    raise GithubApiError(
                        "installation access token response missing fields",
                        code="github_api_error",
                    )
                try:
                    expires_at = _parse_iso(expires_at_str)
                except ValueError as exc:
                    metrics.record_installation_token_mint(result="error")
                    raise GithubApiError(
                        "installation access token expires_at unparseable",
                        code="github_api_error",
                    ) from exc

                self._installation_tokens[installation_id] = (
                    CachedInstallationToken(token=token, expires_at=expires_at)
                )
                metrics.record_installation_token_mint(result="ok")
                log.info(
                    "github_installation_token_minted",
                    installation_id_hash=_short_installation_hash(
                        installation_id
                    ),
                )
                return token

            # Failure paths.
            metrics.record_installation_token_mint(result="error")
            await self._maybe_disable_on_revocation(
                installation_id=installation_id, response=response,
            )
            raise _api_error_from_response(response)

    async def _paginate_installation_repositories(
        self, installation_id: str, *, per_page: int = 100, max_repos: int = 0,
    ) -> tuple[list[str], bool, bool]:
        """Fully paginate `GET /installation/repositories`.

        Returns `(repos, all_mode_marker, any_selection_marker)`.
        Paginates until a short page (end of data) or, when
        `max_repos > 0`, until that many repos have been collected (in
        which case `self._last_repos_truncated` is set and a warning is
        logged — never silent). `per_page=100` is GitHub's maximum.
        """
        token = await self.mint_installation_token(installation_id)
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        self._last_repos_truncated = False
        self._last_repos_total_available = None
        repos: list[str] = []
        any_selection_marker = False
        all_mode_marker = False

        page = 1
        while True:
            url = (
                f"{self._api_base_url}/installation/repositories"
                f"?per_page={per_page}&page={page}"
            )
            try:
                response = await self._get_with_rl_retry(url, headers)
            except httpx.TransportError as exc:
                raise GithubApiError(
                    "transport error fetching installation repositories",
                    code="github_api_error",
                    context={"error_type": type(exc).__name__},
                ) from exc

            metrics.record_outbound_request(
                path="/installation/repositories",
                status=response.status_code,
            )

            if response.status_code != 200:
                await self._maybe_disable_on_revocation(
                    installation_id=installation_id, response=response,
                )
                raise _api_error_from_response(response)

            body = _safe_json(response)
            if not isinstance(body, dict):
                raise GithubApiError(
                    "installation repositories response is not an object",
                    code="github_api_error",
                )

            selection = body.get("repository_selection")
            if selection == "all":
                all_mode_marker = True
            elif selection == "selected":
                any_selection_marker = True

            total_count = body.get("total_count")
            if isinstance(total_count, int):
                self._last_repos_total_available = total_count

            page_repos = body.get("repositories", [])
            if not isinstance(page_repos, list):
                page_repos = []
            for r in page_repos:
                if isinstance(r, dict):
                    full = r.get("full_name")
                    if isinstance(full, str) and full:
                        repos.append(full)

            if len(page_repos) < per_page:
                break  # end of data
            if max_repos and len(repos) >= max_repos:
                self._last_repos_truncated = True
                log.warning(
                    "github_repos_pagination_capped",
                    installation_id_hash=_short_installation_hash(
                        installation_id
                    ),
                    retrieved=len(repos),
                    cap=max_repos,
                    total_available=self._last_repos_total_available,
                )
                break
            page += 1

        return repos, all_mode_marker, any_selection_marker

    async def list_installation_repositories(
        self, installation_id: str
    ) -> list[str] | None:
        """Return the `<owner>/<repo>` list for the installation, OR
        None if the installation is in "all-repositories" mode (NULL
        semantics per data-model.md). Used by the OAuth callback to seed
        `selected_repositories` (NULL = all-repos grant).

        Fully paginated (per_page=100); no longer caps at 90 repos.
        For backfill enumeration that needs the concrete repo list even
        in all-repos mode, use `list_repositories_for_backfill`.
        """
        repos, all_mode_marker, any_selection_marker = (
            await self._paginate_installation_repositories(installation_id)
        )
        # If the API said `repository_selection='all'` AND no `selected`
        # marker was seen, return None (the NULL/all-repos semantic).
        if all_mode_marker and not any_selection_marker:
            return None
        return repos

    async def list_repositories_for_backfill(
        self, installation_id: str
    ) -> list[str]:
        """Every repository accessible to the installation, fully
        paginated, regardless of selected/all-repos mode.

        The backfill planner uses this so org-wide (all-repos) installs
        are supported — `GET /installation/repositories` enumerates the
        accessible repos in both modes — and large selections are not
        silently truncated. Bound with `GITHUB_MAX_BACKFILL_REPOS`
        (env, default 0 = no cap); on cap a warning is logged and
        `self._last_repos_truncated` is set.
        """
        import os

        max_repos = int(os.environ.get("GITHUB_MAX_BACKFILL_REPOS", "0"))
        repos, _all_mode, _selected = (
            await self._paginate_installation_repositories(
                installation_id, max_repos=max_repos,
            )
        )
        return repos

    # -----------------------------------------------------------------
    # Backfill read surface (M6.4) — mirrors MockGithubClient so the
    # fetcher / reconciler exercise the SAME code against the real API
    # (or a local spammer) as against the in-process mock.
    # -----------------------------------------------------------------

    def _backfill_inst(self, installation_id: str | None) -> str:
        inst = installation_id or self._backfill_installation_id
        if not inst:
            raise GithubApiError(
                "github backfill read called without an installation_id; "
                "build the client with backfill_installation_id=…",
                code="github_api_error",
            )
        return inst

    async def list_repo_events(
        self,
        *,
        owner: str,
        repo: str,
        event_type: str,
        page: int = 1,
        per_page: int = 30,
        etag: str | None = None,
        installation_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, int | None]:
        """One page of a repo's events for `event_type`.

        Returns `(page_records, etag, next_page)` — the exact shape the
        M6.4 fetcher consumes. `event_type` maps to the REST collection
        (`issues` → /issues, `pull_requests` → /pulls, `issue_comments`
        → /issues/comments, `commits` → /commits). The response `ETag`
        is returned for the reconciler's conditional fast-path;
        `next_page` is parsed from the `Link` header (rel="next"),
        falling back to `page+1` when a full page came back.
        """
        token = await self.mint_installation_token(
            self._backfill_inst(installation_id),
        )
        path = _GH_EVENT_PATH[event_type]
        query = _gh_event_query(event_type, per_page=per_page, page=page)
        url = f"{self._api_base_url}/repos/{owner}/{repo}/{path}?{query}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if etag:
            headers["If-None-Match"] = etag
        try:
            response = await self._get_with_rl_retry(url, headers)
        except httpx.TransportError as exc:
            raise GithubApiError(
                "transport error fetching repo events",
                code="github_api_error",
                context={"error_type": type(exc).__name__},
            ) from exc

        metrics.record_outbound_request(
            path=f"/repos/{{owner}}/{{repo}}/{path}",
            status=response.status_code,
        )
        new_etag = response.headers.get("ETag", etag or "")
        if response.status_code == 304:
            # Not modified — nothing new on this page.
            return [], new_etag, None
        if response.status_code != 200:
            raise _api_error_from_response(response)

        body = _safe_json(response)
        records = body if isinstance(body, list) else []
        next_page = _parse_next_page(response.headers.get("Link"))
        if next_page is None and len(records) >= per_page:
            next_page = page + 1
        return records, new_etag, next_page

    async def head_repo_events(
        self,
        *,
        owner: str,
        repo: str,
        event_type: str,
        etag: str | None = None,
        installation_id: str | None = None,
    ) -> tuple[bool, str]:
        """Conditional fast-path probe for the reconciler. Issues a
        1-record conditional GET; a `304 Not Modified` means nothing
        changed. Returns `(has_changes, current_etag)`."""
        token = await self.mint_installation_token(
            self._backfill_inst(installation_id),
        )
        path = _GH_EVENT_PATH[event_type]
        url = (
            f"{self._api_base_url}/repos/{owner}/{repo}/{path}"
            f"?state=all&sort=updated&direction=desc&per_page=1&page=1"
        )
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if etag:
            headers["If-None-Match"] = etag
        try:
            response = await self._get_with_rl_retry(url, headers)
        except httpx.TransportError as exc:
            raise GithubApiError(
                "transport error probing repo events",
                code="github_api_error",
                context={"error_type": type(exc).__name__},
            ) from exc

        metrics.record_outbound_request(
            path=f"/repos/{{owner}}/{{repo}}/{path}",
            status=response.status_code,
        )
        current_etag = response.headers.get("ETag", etag or "")
        if response.status_code == 304:
            return False, current_etag
        if response.status_code != 200:
            raise _api_error_from_response(response)
        return etag != current_etag, current_etag

    # -----------------------------------------------------------------
    # Fan-out read surface (gap-closure Class B) — nested child
    # collections with no repo-level list endpoint. Each returns the
    # `(records, etag, next_page)` triple the fan-out fetcher consumes.
    # -----------------------------------------------------------------

    async def list_pr_reviews(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        page: int = 1,
        per_page: int = 100,
        etag: str | None = None,
        installation_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, int | None]:
        """One page of reviews for a single PR
        (`GET /repos/{o}/{r}/pulls/{n}/reviews`). Bare list response."""
        token = await self.mint_installation_token(
            self._backfill_inst(installation_id),
        )
        url = (
            f"{self._api_base_url}/repos/{owner}/{repo}/pulls/{pull_number}"
            f"/reviews?per_page={per_page}&page={page}"
        )
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if etag:
            headers["If-None-Match"] = etag
        try:
            response = await self._get_with_rl_retry(url, headers)
        except httpx.TransportError as exc:
            raise GithubApiError(
                "transport error fetching pr reviews",
                code="github_api_error",
                context={"error_type": type(exc).__name__},
            ) from exc
        metrics.record_outbound_request(
            path="/repos/{owner}/{repo}/pulls/{n}/reviews",
            status=response.status_code,
        )
        new_etag = response.headers.get("ETag", etag or "")
        if response.status_code == 304:
            return [], new_etag, None
        if response.status_code != 200:
            raise _api_error_from_response(response)
        body = _safe_json(response)
        records = body if isinstance(body, list) else []
        next_page = _parse_next_page(response.headers.get("Link"))
        if next_page is None and len(records) >= per_page:
            next_page = page + 1
        return records, new_etag, next_page

    async def list_check_runs(
        self,
        *,
        owner: str,
        repo: str,
        ref: str,
        page: int = 1,
        per_page: int = 100,
        etag: str | None = None,
        installation_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, int | None]:
        """One page of check-runs for a commit ref
        (`GET /repos/{o}/{r}/commits/{ref}/check-runs`). The response is a
        WRAPPED object `{total_count, check_runs:[...]}`; we unwrap the
        list. Requires the App's `checks: read` permission."""
        token = await self.mint_installation_token(
            self._backfill_inst(installation_id),
        )
        url = (
            f"{self._api_base_url}/repos/{owner}/{repo}/commits/{ref}"
            f"/check-runs?per_page={per_page}&page={page}"
        )
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if etag:
            headers["If-None-Match"] = etag
        try:
            response = await self._get_with_rl_retry(url, headers)
        except httpx.TransportError as exc:
            raise GithubApiError(
                "transport error fetching check runs",
                code="github_api_error",
                context={"error_type": type(exc).__name__},
            ) from exc
        metrics.record_outbound_request(
            path="/repos/{owner}/{repo}/commits/{ref}/check-runs",
            status=response.status_code,
        )
        new_etag = response.headers.get("ETag", etag or "")
        if response.status_code == 304:
            return [], new_etag, None
        if response.status_code != 200:
            raise _api_error_from_response(response)
        body = _safe_json(response)
        records = (
            body.get("check_runs", []) if isinstance(body, dict) else []
        )
        next_page = _parse_next_page(response.headers.get("Link"))
        if next_page is None and len(records) >= per_page:
            next_page = page + 1
        return records, new_etag, next_page

    # -----------------------------------------------------------------
    # Chokepoint
    # -----------------------------------------------------------------

    async def _maybe_disable_on_revocation(
        self, *, installation_id: str, response: httpx.Response,
    ) -> None:
        """Check the response shape; if it matches one of the documented
        revocation signals (R2), invoke `_disable_installation_github`
        exactly once for this Python coroutine (idempotent on the DB row).
        """
        chokepoint_reason: str | None = None

        if response.status_code == 401:
            body = _safe_json(response)
            if (
                isinstance(body, dict)
                and isinstance(body.get("message"), str)
                and body["message"].strip().lower() == "bad credentials"
            ):
                chokepoint_reason = "outbound_401_bad_credentials"
        elif response.status_code == 404:
            body = _safe_json(response)
            if isinstance(body, dict):
                doc_url = body.get("documentation_url")
                if isinstance(doc_url, str) and _APPS_DOC_URL_PATTERN.search(
                    doc_url
                ):
                    chokepoint_reason = "outbound_404_apps_not_found"

        if chokepoint_reason is None:
            return

        ctx = self._installation_contexts.get(installation_id)
        if ctx is None:
            # We can't disable the row without the (tenant_id,
            # installation_row_id) mapping. Log and continue; the next
            # webhook will register the context and a subsequent failure
            # will fire the chokepoint correctly.
            log.warning(
                "github_chokepoint_skipped_no_context",
                installation_id_hash=_short_installation_hash(installation_id),
                reason=chokepoint_reason,
            )
            return

        await _disable_installation_github(
            pool=self._pool,
            installation_row_id=ctx.installation_row_id,
            tenant_id=ctx.tenant_id,
            installation_id=installation_id,
            reason=chokepoint_reason,
            installation_token_cache=self._installation_tokens,
            tenant_resolver=self._tenant_resolver,
        )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _is_fresh(cached: CachedInstallationToken) -> bool:
    """True if cached token is still valid for at least
    `_TOKEN_NEAR_EXPIRY_S` seconds.
    """
    remaining = (
        cached.expires_at - datetime.now(timezone.utc)
    ).total_seconds()
    return remaining > _TOKEN_NEAR_EXPIRY_S


def _parse_iso(value: str) -> datetime:
    """Parse GitHub's ISO-8601 'Z'-suffixed datetime."""
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    parsed = datetime.fromisoformat(s)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_next_page(link_header: str | None) -> int | None:
    """Extract the `page=N` of the `rel="next"` link from a GitHub
    `Link` header, or None when there is no next page."""
    if not link_header:
        return None
    m = _LINK_NEXT_PATTERN.search(link_header)
    return int(m.group(1)) if m else None


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(response: httpx.Response) -> GithubApiError:
    """Build a `GithubApiError` from a non-2xx response. The chokepoint
    check is the caller's responsibility; this just shapes the exception.
    """
    body = _safe_json(response)
    github_msg = (
        body.get("message") if isinstance(body, dict) else None
    )

    if response.status_code == 401:
        return GithubApiError(
            f"github 401: {github_msg or 'unauthorized'}",
            code="github_api_unauthorized",
            context={
                "http_status": 401,
                "github_message": github_msg,
            },
        )
    if response.status_code == 404:
        return GithubApiError(
            f"github 404: {github_msg or 'not found'}",
            code="github_api_not_found",
            context={
                "http_status": 404,
                "github_message": github_msg,
            },
        )
    if response.status_code == 429:
        return GithubApiError(
            "github rate limit (429)",
            code="github_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
            },
        )
    return GithubApiError(
        f"github returned {response.status_code}",
        code="github_api_error",
        context={
            "http_status": response.status_code,
            "github_message": github_msg,
        },
    )


__all__ = ["GithubClient", "CachedInstallationToken"]
