"""services/ingest/integrations/linkedin/client.py — outbound LinkedIn client.

Single outbound surface for backfill + poll-incremental against the REAL
LinkedIn Community Management API (Rest.li finders under
``https://api.linkedin.com/rest``). LinkedIn is authenticated with an OAuth 2.0
Bearer **access token** (3-legged; the org read surface is partner-gated) and
every call is scoped to an ``organization_urn`` (the scope-id, analogous to
Carta's ``firm_id`` / QuickBooks' ``realmId``). The access token is resolved
once from the secret store (or preset in spammer mode) and reused for the life
of the client.

Wire contract (pinned against Microsoft Learn, 2026-06):
  - Posts finder:  ``GET /rest/posts?q=author&author={encoded org URN}``
    — OFFSET-paginated via ``start``/``count`` (count max 100, default 10),
    response envelope ``{"elements": [...], "paging": {start, count, links}}``,
    sorted by ``lastModifiedAt`` DESC by default (``sortBy=LAST_MODIFIED``).
    https://learn.microsoft.com/en-us/linkedin/marketing/community-management/shares/posts-api
  - Share statistics:
    ``GET /rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={URN}``
    — NOT paginated; optional Rest.li-2.0 ``timeIntervals=(timeRange:(start:ms,
    end:ms),timeGranularityType:DAY|MONTH)`` for time-bound buckets (rolling
    12-month window).
    https://learn.microsoft.com/en-us/linkedin/marketing/community-management/organizations/share-statistics
  - Follower statistics:
    ``GET /rest/organizationalEntityFollowerStatistics?q=organizationalEntity&…``
    — time-bound buckets carry ``followerGains`` + ``timeRange``
    (DAY/WEEK/MONTH; data from 12 months back until ~2 days before now).
    https://learn.microsoft.com/en-us/linkedin/marketing/community-management/organizations/follower-statistics
  - Org probe: ``GET /rest/organizations/{numeric id}``.
    https://learn.microsoft.com/en-us/linkedin/marketing/community-management/organizations/organization-lookup-api

EVERY call carries the two REQUIRED headers (missing → 400/426):
  - ``LinkedIn-Version: YYYYMM`` (pinned default below, env-overridable via
    ``LINKEDIN_VERSION``), and
  - ``X-Restli-Protocol-Version: 2.0.0``.
URN query params are Rest.li-2.0 / URL encoded (``urn%3Ali%3Aorganization%3A123``).
All timestamps on the wire are **epoch-millis integers** (``createdAt``,
``lastModifiedAt``, ``timeRange.start/end``).

Reactive refresh: on a 401/403, the client asks the shared OAuth refresh core
to exchange the install's `refresh_secret_ref` at
``https://www.linkedin.com/oauth/v2/accessToken``, persists the returned access
and refresh token refs, then retries once. Programmatic refresh tokens are only
issued to approved partner programs; if refresh fails, the original auth error
surfaces and shard_fetch records the shard as degraded.

Rate limits: 429 + Retry-After (env knobs LINKEDIN_RL_MAX_ATTEMPTS /
LINKEDIN_RL_MAX_SLEEP_SEC). Non-2xx maps to ``LinkedinApiError``.

Logging redaction: the access token / auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID
from urllib.parse import quote

import httpx
import structlog

from lib.shared.errors import LinkedinApiError


log = structlog.get_logger("integrations.linkedin.client")


_DEFAULT_TIMEOUT_S = 30.0
# Posts finder caps `count` at 100 (default 10); we default to the cap.
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 100
# Pinned versioned-API month (the latest GA moniker at pin time). Override per
# env via LINKEDIN_VERSION when LinkedIn sunsets this version.
_DEFAULT_LINKEDIN_VERSION = "202605"
_RESTLI_PROTOCOL_VERSION = "2.0.0"


def linkedin_version() -> str:
    """The dated `LinkedIn-Version: YYYYMM` header value (env-overridable)."""
    return os.environ.get("LINKEDIN_VERSION", _DEFAULT_LINKEDIN_VERSION)


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


def organization_id_of(organization_urn: str) -> str:
    """The numeric organization id from `urn:li:organization:{id}` (a bare id
    passes through unchanged — synthetic installs use opaque scope ids)."""
    return organization_urn.rpartition(":")[2] or organization_urn


def _author_urn(organization_urn: str) -> str:
    """The full author/organizationalEntity URN for the wire. Installs SHOULD
    configure the full `urn:li:organization:{id}`; a bare id is up-converted."""
    if organization_urn.startswith("urn:"):
        return organization_urn
    return f"urn:li:organization:{organization_urn}"


def _time_intervals_param(
    start_ms: int, end_ms: int | None, granularity: str,
) -> str:
    """Rest.li-2.0 `timeIntervals` value, encoded the way the documented sample
    requests are: parens stay raw, inner colons/commas are percent-encoded —
    `(timeRange%3A(start%3A...%2Cend%3A...)%2CtimeGranularityType%3ADAY)`."""
    inner = f"start:{int(start_ms)}"
    if end_ms is not None:
        inner += f",end:{int(end_ms)}"
    raw = f"(timeRange:({inner}),timeGranularityType:{granularity})"
    return quote(raw, safe="()")


class LinkedinClient:
    """Outbound LinkedIn REST (Rest.li) client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_linkedin_client`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        organization_urn: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        access_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        install_row_id: Any | None = None,
        refresh_secret_ref: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._organization_urn = organization_urn
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is https://api.linkedin.com/rest; a spammer/
        # test override (api_base_url) wins so backfill points at the mock.
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
        if self._access_token is not None:
            return self._access_token
        async with self._token_lock:
            if self._access_token is not None:
                return self._access_token
            if (
                self._secret_store is None
                or self._secret_ref is None
                or self._tenant_id is None
            ):
                raise LinkedinApiError(
                    "linkedin client has no access token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="linkedin_api_unauthorized",
                )
            raw = await self._secret_store.get(
                self._secret_ref, tenant_id=self._tenant_id,
            )
            self._access_token = (
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
            return self._access_token

    async def _request(
        self, method: str, path: str, *, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from services.ingest.integrations.linkedin import metrics
        from services.ingest.integrations.oauth_refresh import refresh_on_unauthorized

        url = f"{self._api_base_url}{path}"
        max_attempts = int(os.environ.get("LINKEDIN_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("LINKEDIN_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        reminted = False
        while True:
            attempt += 1
            token = await self._token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                # BOTH headers are REQUIRED on every Community-Management call;
                # missing/expired version → 400/426.
                "LinkedIn-Version": linkedin_version(),
                "X-Restli-Protocol-Version": _RESTLI_PROTOCOL_VERSION,
            }
            try:
                response = await client.request(
                    method, url, headers=headers, params=params,
                )
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise LinkedinApiError(
                    "transport error calling linkedin",
                    code="linkedin_api_error",
                    context={"error_type": type(exc).__name__, "path": path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                metrics.record_request("rate_limited")
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                metrics.record_request("ok")
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise LinkedinApiError(
                        "linkedin response was not a JSON object",
                        code="linkedin_api_error",
                        context={"path": path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
                if not reminted:
                    reminted = True
                    new_token = await refresh_on_unauthorized(
                        provider="linkedin",
                        pool=self._pool,
                        secret_store=self._secret_store,
                        http=client,
                        tenant_id=self._tenant_id,
                        install_row_id=self._install_row_id,
                        current_access_ref=self._secret_ref,
                        refresh_secret_ref=self._refresh_secret_ref,
                    )
                    if new_token is not None:
                        self._access_token = new_token
                        continue
            else:
                metrics.record_request("error")
            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface (Rest.li finders)
    # -----------------------------------------------------------------

    async def list_posts(
        self,
        *,
        start: int = 0,
        count: int = _DEFAULT_PAGE_SIZE,
        sort_by: str = "LAST_MODIFIED",
    ) -> tuple[list[dict[str, Any]], int | None]:
        """`GET /posts?q=author&author={org URN}` — one offset page of the
        organization's posts. Returns `(elements, next_start)`;
        `next_start is None` is terminal.

        Results are sorted DESC by `lastModifiedAt` (`sortBy=LAST_MODIFIED`,
        the API default) — callers doing incremental sync early-stop once an
        element's `lastModifiedAt` drops at/under their high-water. Scope:
        `r_organization_social`. NOTE (per the Posts-API doc): a short page is
        NOT always terminal — when more posts exist, `paging.links` carries a
        `next` link, so termination is `elements empty, or short page with no
        next link`.
        """
        count = max(1, min(_MAX_PAGE_SIZE, int(count)))
        params: dict[str, Any] = {
            "q": "author",
            "author": _author_urn(self._organization_urn),
            "start": int(start),
            "count": count,
            "sortBy": sort_by,
        }
        resp = await self._request("GET", "/posts", params=params)
        elements = resp.get("elements")
        rows = (
            [e for e in elements if isinstance(e, dict)]
            if isinstance(elements, list) else []
        )
        paging = resp.get("paging") if isinstance(resp.get("paging"), dict) else {}
        links = paging.get("links") if isinstance(paging.get("links"), list) else []
        has_next = any(
            isinstance(link, dict) and link.get("rel") == "next" for link in links
        )
        is_last = not rows or (len(rows) < count and not has_next)
        return rows, (None if is_last else int(start) + len(rows))

    async def share_statistics(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        granularity: str = "DAY",
    ) -> list[dict[str, Any]]:
        """`GET /organizationalEntityShareStatistics?q=organizationalEntity&…`
        — the org's share statistics. With `start_ms`, time-bound buckets
        (each element carries `timeRange` + `totalShareStatistics`); without,
        the single lifetime aggregate. NOT paginated (per the doc). The API
        only serves a rolling 12-month window. Scope: `rw_organization_admin`.
        Granularity ∈ {DAY, MONTH}.
        """
        return await self._organizational_entity_statistics(
            "/organizationalEntityShareStatistics",
            start_ms=start_ms, end_ms=end_ms, granularity=granularity,
        )

    async def follower_statistics(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        granularity: str = "DAY",
    ) -> list[dict[str, Any]]:
        """`GET /organizationalEntityFollowerStatistics?q=organizationalEntity&…`
        — with `start_ms`, time-bound buckets (each element carries `timeRange`
        + `followerGains{organicFollowerGain, paidFollowerGain}`); without, the
        lifetime facet breakdown. Time-bound data spans 12 months back until
        ~2 days before now. Scope: `rw_organization_admin`. Granularity ∈
        {DAY, WEEK, MONTH}.
        """
        return await self._organizational_entity_statistics(
            "/organizationalEntityFollowerStatistics",
            start_ms=start_ms, end_ms=end_ms, granularity=granularity,
        )

    async def _organizational_entity_statistics(
        self,
        resource_path: str,
        *,
        start_ms: int | None,
        end_ms: int | None,
        granularity: str,
    ) -> list[dict[str, Any]]:
        # The query string is built by hand: the Rest.li-2.0 `timeIntervals`
        # value must keep its parens raw, which httpx params= would escape.
        query = (
            "q=organizationalEntity&organizationalEntity="
            + quote(_author_urn(self._organization_urn), safe="")
        )
        if start_ms is not None:
            query += "&timeIntervals=" + _time_intervals_param(
                start_ms, end_ms, granularity,
            )
        resp = await self._request("GET", f"{resource_path}?{query}")
        elements = resp.get("elements")
        if not isinstance(elements, list):
            return []
        return [e for e in elements if isinstance(e, dict)]

    async def get_organization(self) -> dict[str, Any]:
        """`GET /organizations/{numeric id}` — connectivity/entitlement probe
        for the organization the token administers (403 without the
        ADMINISTRATOR page role). Returns the organization object
        (`id`, `localizedName`, `vanityName`, …)."""
        org_id = organization_id_of(self._organization_urn)
        return await self._request(
            "GET", f"/organizations/{quote(org_id, safe='')}",
        )


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
) -> LinkedinApiError:
    status = response.status_code
    if status in (401, 403):
        return LinkedinApiError(
            f"linkedin {status}: access token rejected or insufficient scope "
            "(may need refresh, or the partner entitlement / page role is missing)",
            code="linkedin_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return LinkedinApiError(
            "linkedin 404: entity/organization not found or not visible",
            code="linkedin_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return LinkedinApiError(
            "linkedin rate limit (429), retry budget exhausted",
            code="linkedin_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return LinkedinApiError(
        f"linkedin returned {status}",
        code="linkedin_api_error",
        context={"http_status": status, "path": path},
    )


# The organization streams we shard on, keyed to the real Community-Management
# read surface. The entity_kind discriminates the external_id:
#   post                — /rest/posts?q=author (org posts, epoch-millis stamps)
#   share_statistics    — /rest/organizationalEntityShareStatistics (time-bound)
#   follower_statistics — /rest/organizationalEntityFollowerStatistics (time-bound)
DEFAULT_ENTITIES = ("post", "share_statistics", "follower_statistics")


__all__ = [
    "LinkedinClient",
    "LinkedinApiError",
    "DEFAULT_ENTITIES",
    "linkedin_version",
    "organization_id_of",
]
