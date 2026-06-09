"""services/ingest/integrations/linkedin/client.py — outbound LinkedIn client.

Single outbound surface for backfill + poll-incremental. LinkedIn is
authenticated with an OAuth 2.0 Bearer **access token** (short-lived) and every
call is scoped to an ``organization_urn`` (the scope-id, analogous to Carta's
``firm_id`` / Gusto's ``company_uuid`` / QuickBooks' ``realmId``). The access
token is resolved once from the secret store (or preset in spammer mode) and
reused for the life of the client.

TODO(human): implement LinkedIn OAuth token refresh — NONE exists yet (this is
    the documented-but-unbuilt seam, exactly as the Carta / Gusto / QuickBooks
    archetype ships). The install row persists `refresh_secret_ref` +
    `token_expires_at`; wire either a refresh-on-401 exchange here (exchange
    refresh token -> persist rotated token -> retry once) OR an oauth_poller. Do
    NOT assume tokens never expire. (LinkedIn access tokens are ~60 days,
    refresh tokens ~1 year — confirm against your partner entitlement.)

TODO(human): confirm LinkedIn API host + read endpoints + OAuth scopes. The host
    is intended to be set in `lib/integrations/endpoints.py` (`linkedin_api`) and
    is overridable per env (`LINKEDIN_API_BASE_URL`) and per install (`base_url`).
    The read surface below clones the Gusto/QuickBooks query endpoint as a
    placeholder; LinkedIn's real read surface is REST collections under
    `/rest/...` or `/v2/...` scoped by an `organization` URN query param
    (shares/posts, organizationalEntityShareStatistics / socialActions,
    organizationFollowerStatistics). Implement only the verified read surface and
    tag speculative endpoints. ACCESS IS PARTNER-GATED (Marketing Developer
    Platform / Talent Solutions, invite-only).

Rate limits: default to 429 + Retry-After (env knobs LINKEDIN_RL_MAX_ATTEMPTS /
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

# Import the canonical `LinkedinApiError(CompanyOSError)` from the shared error
# module (another agent defines it during the wiring phase, mirroring
# `BrexApiError` / `CartaApiError`). `lib/shared/errors.py` is a SHARED file this
# phase must NOT edit, so until the canonical class lands a local subclass with
# the same stable `code` contract keeps the pipeline self-contained. The local
# fallback is NEVER preferred once the canonical class is importable.
try:  # pragma: no cover - prefer the canonical error once wired
    from lib.shared.errors import LinkedinApiError  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    from lib.shared.errors import CompanyOSError

    class LinkedinApiError(CompanyOSError):  # type: ignore[no-redef]
        """Outbound LinkedIn REST call failure (people/recruiting source —
        OAuth/Carta archetype). Stable `code` values mirror Carta:
          - linkedin_api_unauthorized / linkedin_api_not_found /
            linkedin_api_rate_limited / linkedin_api_error.
        The access/refresh tokens are NEVER placed on context.
        """

        default_code = "linkedin_api_error"

        def __init__(
            self,
            message: str,
            *,
            code: str | None = None,
            context: dict[str, Any] | None = None,
            **extra: Any,
        ) -> None:
            merged = dict(context or {})
            merged.update(extra)
            super().__init__(message, **merged)
            if code is not None:
                self._code = code


log = structlog.get_logger("integrations.linkedin.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MINOR_VERSION = "1"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class LinkedinClient:
    """Outbound LinkedIn client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_linkedin_client`
    (added during the wiring phase).
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
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._organization_urn = organization_urn
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical LINKEDIN host; a spammer/test
        # override (api_base_url) wins so backfill points at the mock.
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

        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            # TODO(human): confirm whether LinkedIn requires the versioned
            # protocol headers (X-Restli-Protocol-Version: 2.0.0 and the dated
            # LinkedIn-Version) on the entitled REST surface.
        }
        max_attempts = int(os.environ.get("LINKEDIN_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("LINKEDIN_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
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
            else:
                metrics.record_request("error")
            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def query(
        self,
        entity: str,
        *,
        where: str | None = None,
        order_by: str = "Metadata.LastUpdatedTime",
        start_position: int = 1,
        max_results: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Run a SELECT against one entity. Returns `(rows, next_start_position)`;
        `next_start_position is None` is terminal.

        TODO(human): confirm LinkedIn's real list/pagination shape. This clones
        the Carta/Gusto/QuickBooks query-language placeholder (`SELECT * FROM
        <Entity> [WHERE ...] ORDERBY <f> STARTPOSITION n MAXRESULTS m`).
        LinkedIn's real REST surface is page/cursor-based collections scoped by an
        `organization` URN query param — shares/posts (`/rest/posts?q=author`),
        organizationalEntityShareStatistics / socialActions, and
        organizationFollowerStatistics — paginated via start/count or an opaque
        page token. Replace `client.query(...)` + the WHERE filter with the
        verified shape. `Metadata.LastUpdatedTime` is the incremental cursor field
        placeholder.
        """
        sql = f"SELECT * FROM {entity}"
        if where:
            sql += f" WHERE {where}"
        sql += f" ORDERBY {order_by} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        path = f"/v1/organizations/{quote(self._organization_urn, safe='')}/query"
        params = {"query": sql, "minorversion": _MINOR_VERSION}
        resp = await self._request("GET", path, params=params)
        qr = resp.get("QueryResponse")
        if not isinstance(qr, dict):
            return [], None
        rows = qr.get(entity)
        rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        # LINKEDIN returns maxResults == page length; a short page is terminal.
        returned = int(qr.get("maxResults", len(rows)) or 0)
        next_start = start_position + len(rows)
        is_last = returned < max_results or not rows
        return rows, (None if is_last else next_start)

    async def org_info(self) -> dict[str, Any]:
        """`GET /v1/organizations/{org}/orginfo/{org}` — connectivity probe.

        TODO(human): confirm the real connectivity probe. LinkedIn's equivalent
        is `GET /rest/organizations/{id}` (or `/v2/organizations/{id}`) for the
        organization the token is entitled to; this clones the Carta firminfo
        probe shape as a placeholder.
        """
        org = quote(self._organization_urn, safe="")
        path = f"/v1/organizations/{org}/orginfo/{org}"
        return await self._request(
            "GET", path, params={"minorversion": _MINOR_VERSION},
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
            "(may need refresh, or the partner entitlement is missing)",
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


# The people/recruiting entity kinds we shard on. LinkedIn organization data is
# entity-shaped (NOT transactional), so the entity_kind discriminates the
# external_id. Per the cross-agent CONTRACT the entity types are:
#   share | social_action | follower_stat
DEFAULT_ENTITIES = ("share", "social_action", "follower_stat")


__all__ = ["LinkedinClient", "LinkedinApiError", "DEFAULT_ENTITIES"]
