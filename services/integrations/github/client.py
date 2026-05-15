"""services/integrations/github/client.py — outbound GitHub REST client.

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
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog

from lib.shared.errors import GithubApiError, GithubJWTError

from services.integrations.github import metrics
from services.integrations.github.jwt import mint_app_jwt
from services.integrations.github.uninstall import (
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
        api_base_url: str = _GITHUB_API_BASE,
        tenant_resolver: Any | None = None,
    ) -> None:
        self._pool = pool
        self._api_base_url = api_base_url.rstrip("/")
        self._tenant_resolver = tenant_resolver
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

    async def list_installation_repositories(
        self, installation_id: str
    ) -> list[str] | None:
        """Return the `<owner>/<repo>` list for the installation, OR
        None if the installation is in "all-repositories" mode (NULL
        semantics per data-model.md).

        Reads up to 3 pages (90 repos at `per_page=30`); on truncation,
        sets `self._last_repos_truncated=True` and records the
        `total_count` GitHub reported via `self._last_repos_total_available`.
        """
        token = await self.mint_installation_token(installation_id)
        client = self._httpx()
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

        for page in range(1, 4):  # 3 pages × 30 = 90 repo cap (R8)
            url = (
                f"{self._api_base_url}/installation/repositories"
                f"?per_page=30&page={page}"
            )
            try:
                response = await client.get(url, headers=headers)
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

            if len(page_repos) < 30:
                break
        else:  # for-else: ran all 3 iterations without breaking
            if (
                isinstance(self._last_repos_total_available, int)
                and self._last_repos_total_available > len(repos)
            ):
                self._last_repos_truncated = True
                log.warning(
                    "github_repos_pagination_truncated",
                    installation_id_hash=_short_installation_hash(
                        installation_id
                    ),
                    retrieved=len(repos),
                    total_available=self._last_repos_total_available,
                )

        # If the API said `repository_selection='all'` AND no `selected`
        # marker was seen, return None (the NULL/all-repos semantic).
        if all_mode_marker and not any_selection_marker:
            return None

        return repos

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
