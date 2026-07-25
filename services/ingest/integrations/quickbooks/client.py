"""services/ingest/integrations/quickbooks/client.py — outbound QuickBooks Online client.

Single outbound surface for backfill + poll-incremental. QuickBooks Online is
authenticated with an OAuth 2.0 Bearer **access token** (~60 min lifetime) and
every call is scoped to a company ``realmId``. The access token is resolved
through a short-lived secret-ref cache (or preset in Provider Lab mode), so rotation
is picked up without process restart. Production token refresh (the rotating
refresh token) is owned by the oauth_poller; this read client consumes the
current access token and reactively refreshes once on 401.

Reads go through the **query endpoint**:
    GET /v3/company/{realmId}/query?query=<SQL>&minorversion=75
returning ``{"QueryResponse": {"<Entity>": [...], "startPosition", "maxResults"}}``.
Pagination is offset-based via ``STARTPOSITION n MAXRESULTS m`` in the SQL.

Every outbound attempt executes through the shared ``ProviderTransport`` under
an exact tenant + installation binding.  The transport owns quota acquisition,
bounded full-jitter retries, shared cooldowns, concurrency and timeouts.
Non-retryable provider responses still map to ``QuickBooksApiError``.

Logging redaction: the access token / auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import QuickBooksApiError
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.quickbooks.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MINOR_VERSION = "75"


class QuickBooksClient:
    """Outbound QuickBooks Online client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_quickbooks_client`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        realm_id: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        access_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        install_row_id: Any | None = None,
        refresh_secret_ref: str | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        require_tenant_installation: bool = True,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._realm_id = realm_id
        # Reactive OAuth re-mint (Phase 3): on a 401 the client refreshes the
        # rotating access token via the install's refresh token, then retries.
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._access_token_cache = SecretValueCache(preset=access_token)
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical QBO host; a lab/test
        # override (api_base_url) wins so backfill points at the mock.
        self._api_base_url = (api_base_url or base_url).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                http_client is not None or api_base_url is not None
            ),
        )
        self._provider = ProviderRequestBinding(
            source="quickbooks",
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            installation_id=(
                str(install_row_id) if install_row_id is not None else None
            ),
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
            require_tenant=True,
            require_installation=require_tenant_installation,
        )

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
        return await self._access_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: QuickBooksApiError(
                "quickbooks client has no access token and cannot resolve "
                "one (missing secret_store / secret_ref / tenant_id)",
                code="quickbooks_api_unauthorized",
            )
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> dict[str, Any]:
        from services.ingest.integrations.quickbooks import metrics
        from services.ingest.integrations.oauth_refresh import (
            refresh_on_unauthorized,
        )

        url = f"{self._api_base_url}{path}"
        client = self._httpx()

        reminted = False
        refreshed_token: str | None = None
        while True:
            # Recompute the token each attempt so a reactive re-mint (below)
            # takes effect on the retry.
            token = refreshed_token or await self._token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }

            async def _once() -> httpx.Response:
                try:
                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                    )
                except httpx.TimeoutException as exc:
                    metrics.record_request("error")
                    raise ProviderTimeoutError(
                        "QuickBooks request timed out",
                        source="quickbooks",
                        operation=operation,
                        error_type=type(exc).__name__,
                    ) from exc
                except httpx.TransportError as exc:
                    metrics.record_request("error")
                    raise ProviderTransientError(
                        "QuickBooks transport error",
                        source="quickbooks",
                        operation=operation,
                        error_type=type(exc).__name__,
                    ) from exc

                if response.status_code == 429:
                    metrics.record_request("rate_limited")
                    raise ProviderRateLimited(
                        "QuickBooks rate limit",
                        retry_after_seconds=parse_retry_after(
                            response.headers.get("Retry-After"),
                        ),
                        status_code=429,
                        header_parser_id="http.retry_after",
                        source="quickbooks",
                        operation=operation,
                    )
                if response.status_code >= 500:
                    metrics.record_request("error")
                    raise ProviderTransientError(
                        f"QuickBooks returned HTTP {response.status_code}",
                        source="quickbooks",
                        operation=operation,
                        http_status=response.status_code,
                    )
                return response

            response = await self._provider.execute(operation, _once)
            if response.status_code // 100 == 2:
                metrics.record_request("ok")
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise QuickBooksApiError(
                        "quickbooks response was not a JSON object",
                        code="quickbooks_api_error",
                        context={"path": path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
                # Reactive re-mint: the ~1h access token likely expired
                # mid-fetch. Refresh once via the install's rotating refresh
                # token, then retry. A failed refresh returns None → fall
                # through to raise (shard_fetch records a degraded shard).
                if not reminted:
                    reminted = True
                    new_token = await refresh_on_unauthorized(
                        provider="quickbooks",
                        pool=self._pool,
                        secret_store=self._secret_store,
                        http=client,
                        tenant_id=self._tenant_id,
                        install_row_id=self._install_row_id,
                        current_access_ref=self._secret_ref,
                        refresh_secret_ref=self._refresh_secret_ref,
                        request_binding=self._provider,
                    )
                    if new_token is not None:
                        self._access_token_cache.set(new_token)
                        refreshed_token = new_token
                        continue
                raise _api_error_from_response(response, path)
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

        QBO query language: `SELECT * FROM Invoice [WHERE ...] ORDERBY <f>
        STARTPOSITION n MAXRESULTS m`. `Metadata.LastUpdatedTime` is the
        incremental cursor field.
        """
        sql = f"SELECT * FROM {entity}"
        if where:
            sql += f" WHERE {where}"
        sql += f" ORDERBY {order_by} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        path = f"/v3/company/{self._realm_id}/query"
        params = {"query": sql, "minorversion": _MINOR_VERSION}
        resp = await self._request(
            "GET",
            path,
            params=params,
            operation="entities.query",
        )
        qr = resp.get("QueryResponse")
        if not isinstance(qr, dict):
            return [], None
        rows = qr.get(entity)
        rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        # QBO returns maxResults == page length; a short page is terminal.
        returned = int(qr.get("maxResults", len(rows)) or 0)
        next_start = start_position + len(rows)
        is_last = returned < max_results or not rows
        return rows, (None if is_last else next_start)

    async def company_info(self) -> dict[str, Any]:
        """`GET /v3/company/{realm}/companyinfo/{realm}` — connectivity probe."""
        path = f"/v3/company/{self._realm_id}/companyinfo/{quote(self._realm_id)}"
        return await self._request(
            "GET",
            path,
            params={"minorversion": _MINOR_VERSION},
            operation="company_info.get",
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
) -> QuickBooksApiError:
    status = response.status_code
    if status in (401, 403):
        return QuickBooksApiError(
            f"quickbooks {status}: access token rejected or insufficient scope "
            "(may need refresh)",
            code="quickbooks_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return QuickBooksApiError(
            "quickbooks 404: entity/realm not found or not visible",
            code="quickbooks_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return QuickBooksApiError(
            "quickbooks rate limit (429), retry budget exhausted",
            code="quickbooks_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return QuickBooksApiError(
        f"quickbooks returned {status}",
        code="quickbooks_api_error",
        context={"http_status": status, "path": path},
    )


# The entity types we shard on, in dependency order (customers/vendors first
# would be ideal but v1 ingests the four transactional entities directly).
DEFAULT_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")


__all__ = ["QuickBooksClient", "DEFAULT_ENTITIES"]
