"""services/ingest/integrations/deel/client.py - outbound Deel REST client.

Verified Deel surface:

  * base: https://api.letsdeel.com/rest/v2
  * contracts: GET /contracts and GET /contracts/{id}
  * payments/invoices: GET /invoices
  * envelope: {data: ..., page: {cursor, total_rows}}
  * required version header: X-Version: YYYY-MM-DD
  * money: decimal strings in major units, decoded by the handler

The public fetcher seam remains `list_contracts`, `get_contract`, and
`list_payments`; internally `list_payments` reads the org invoice stream and
filters to the requested contract when the invoice payload carries a contract id.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import DeelApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.deel.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 100
_DEFAULT_API_VERSION = "2026-01-01"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


def _normalise_base_url(url: str) -> str:
    base = url.rstrip("/")
    parsed = urlsplit(base)
    if parsed.netloc == "api.letsdeel.com" and not parsed.path.endswith("/rest/v2"):
        return f"{base}/rest/v2"
    return base


class DeelClient:
    """Outbound Deel REST API client, one per backfill/poll shard open."""

    def __init__(
        self,
        *,
        base_url: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        api_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        api_version: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._api_token_cache = SecretValueCache(preset=api_token)
        self._token_lock = asyncio.Lock()
        self._api_base_url = _normalise_base_url(api_base_url or base_url)
        self._api_version = (
            api_version
            or os.environ.get("DEEL_API_VERSION")
            or _DEFAULT_API_VERSION
        )
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
        return await self._api_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: DeelApiError(
                "deel client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="deel_api_unauthorized",
            )
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from services.ingest.integrations.deel import metrics

        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "X-Version": self._api_version,
        }
        max_attempts = int(os.environ.get("DEEL_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("DEEL_RL_MAX_SLEEP_SEC", "30"))
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
                raise DeelApiError(
                    "transport error calling deel",
                    code="deel_api_error",
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
                    raise DeelApiError(
                        "deel response was not a JSON object",
                        code="deel_api_error",
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

    async def list_contracts(self) -> list[dict[str, Any]]:
        """`GET /contracts` - all contracts visible to the token."""
        return await self._list_data_pages("/contracts")

    async def get_contract(self, contract_id: str) -> dict[str, Any]:
        """`GET /contracts/{id}` - one contract from the `{data}` envelope."""
        body = await self._request(
            "GET", f"/contracts/{quote(contract_id, safe='')}",
        )
        data = body.get("data")
        if isinstance(data, dict):
            return data
        return body

    async def list_payments(
        self,
        contract_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """Read Deel invoices and expose them as the existing payment stream.

        Deel's real stream is org-level `/invoices`, not
        `/contract/{id}/payments`. We gather the available invoices, keep those
        for `contract_id` when the payload carries a contract reference, and
        then apply the fetcher's offset window.
        """
        invoices = await self._list_invoices(contract_id=contract_id, start=start)
        if contract_id:
            filtered = [i for i in invoices if _invoice_contract_id(i) in {"", contract_id}]
        else:
            filtered = invoices

        eff_limit = max(1, min(_MAX_PAGE_SIZE, int(limit or _DEFAULT_PAGE_SIZE)))
        page = filtered[offset:offset + eff_limit]
        total = len(filtered)
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def _list_invoices(
        self, *, contract_id: str | None, start: str | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": _MAX_PAGE_SIZE, "offset": 0}
        if contract_id:
            params["contract_id"] = contract_id
        if start:
            params["created_after"] = start

        out: list[dict[str, Any]] = []
        cursor: str | None = None
        offset = 0
        while True:
            page_params = dict(params)
            page_params["offset"] = offset
            if cursor:
                page_params["cursor"] = cursor
            body = await self._request("GET", "/invoices", params=page_params)
            items = _data_list(body, "invoices")
            out.extend(items)
            cursor = _page_cursor(body)
            if cursor:
                offset += len(items)
                continue
            total = _page_total(body)
            offset += len(items)
            if total is not None and offset < total and items:
                continue
            return out

    async def _list_data_pages(self, path: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": _MAX_PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor
            body = await self._request("GET", path, params=params)
            items = _data_list(body)
            out.extend(items)
            cursor = _page_cursor(body)
            if not cursor:
                return out


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _data_list(body: dict[str, Any], fallback_key: str | None = None) -> list[dict[str, Any]]:
    data = body.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(fallback_key, str):
        value = body.get(fallback_key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _page_cursor(body: dict[str, Any]) -> str | None:
    page = body.get("page")
    if isinstance(page, dict):
        cursor = page.get("cursor") or page.get("next_cursor")
        if isinstance(cursor, str) and cursor:
            return cursor
    cursor = body.get("cursor") or body.get("next_cursor")
    return cursor if isinstance(cursor, str) and cursor else None


def _page_total(body: dict[str, Any]) -> int | None:
    page = body.get("page")
    if isinstance(page, dict):
        total = page.get("total_rows") or page.get("total")
    else:
        total = body.get("total")
    if isinstance(total, (int, float)):
        return int(total)
    return None


def _invoice_contract_id(invoice: dict[str, Any]) -> str:
    for key in ("contract_id", "contractId"):
        value = invoice.get(key)
        if value not in (None, ""):
            return str(value)
    contract = invoice.get("contract")
    if isinstance(contract, dict):
        value = contract.get("id") or contract.get("contract_id")
        if value not in (None, ""):
            return str(value)
    return ""


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> DeelApiError:
    status = response.status_code
    if status in (401, 403):
        return DeelApiError(
            f"deel {status}: API token rejected or insufficient scope",
            code="deel_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return DeelApiError(
            "deel 404: contract/resource not found or not visible to the token",
            code="deel_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return DeelApiError(
            "deel rate limit (429), retry budget exhausted",
            code="deel_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return DeelApiError(
        f"deel returned {status}",
        code="deel_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["DeelClient"]
