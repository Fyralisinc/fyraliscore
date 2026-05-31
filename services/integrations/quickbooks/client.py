"""services/integrations/quickbooks/client.py — outbound QuickBooks Online client.

Single outbound surface for backfill + poll-incremental. QuickBooks Online is
authenticated with an OAuth 2.0 Bearer **access token** (~60 min lifetime) and
every call is scoped to a company ``realmId``. The access token is resolved once
from the secret store (or preset in spammer mode) and reused for the life of the
client. Production token refresh (the rotating refresh token) is owned by the
oauth_poller; this read client consumes the current access token.

Reads go through the **query endpoint**:
    GET /v3/company/{realmId}/query?query=<SQL>&minorversion=75
returning ``{"QueryResponse": {"<Entity>": [...], "startPosition", "maxResults"}}``.
Pagination is offset-based via ``STARTPOSITION n MAXRESULTS m`` in the SQL.

Rate limits: 10 req/s, 120/min batch per realm; 429 on throttle (honoured with a
bounded Retry-After retry). Non-2xx maps to ``QuickBooksApiError``.

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

from lib.shared.errors import QuickBooksApiError


log = structlog.get_logger("integrations.quickbooks.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MINOR_VERSION = "75"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class QuickBooksClient:
    """Outbound QuickBooks Online client, one per backfill/poll shard open.

    Built by `services/ingestion/fetchers/_clients.py::build_quickbooks_client`.
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
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._realm_id = realm_id
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical QBO host; a spammer/test
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
                raise QuickBooksApiError(
                    "quickbooks client has no access token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="quickbooks_api_unauthorized",
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
        from services.integrations.quickbooks import metrics

        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("QUICKBOOKS_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("QUICKBOOKS_RL_MAX_SLEEP_SEC", "30"))
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
                raise QuickBooksApiError(
                    "transport error calling quickbooks",
                    code="quickbooks_api_error",
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
                    raise QuickBooksApiError(
                        "quickbooks response was not a JSON object",
                        code="quickbooks_api_error",
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
        resp = await self._request("GET", path, params=params)
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
