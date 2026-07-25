"""services/ingest/integrations/brex/client.py - outbound Brex REST client.

Verified Brex surface:

  * base host: https://platform.brexapis.com
  * accounts: GET /v2/accounts/cash (cursor envelope) and
    GET /v2/accounts/card (bare array or items envelope)
  * transactions: GET /v2/transactions/cash/{account_id} and
    GET /v2/transactions/card/primary
  * pagination: opaque next_cursor, limit max 1000
  * money: signed integer cents objects, decoded by the handler

The public method names intentionally match the old fetcher seam
(`list_accounts`, `get_account`, `list_transactions`), but the HTTP paths are
the real Brex /v2 paths.
"""
from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import BrexApiError
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


log = structlog.get_logger("integrations.brex.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 1000


class BrexClient:
    """Outbound Brex API client, one per backfill/poll shard open."""

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
        installation_row_id: UUID | str | None = None,
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
        self._api_token_cache = SecretValueCache(preset=api_token)
        self._token_lock = asyncio.Lock()
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
            source="brex",
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            installation_id=(
                str(installation_row_id)
                if installation_row_id is not None
                else None
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
        return await self._api_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: BrexApiError(
                "brex client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="brex_api_unauthorized",
            )
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> Any:
        from services.ingest.integrations.brex import metrics

        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        client = self._httpx()

        async def _once() -> httpx.Response:
            try:
                response = await client.request(
                    method, url, headers=headers, params=params,
                )
            except httpx.TimeoutException as exc:
                metrics.record_request("error")
                raise ProviderTimeoutError(
                    "Brex request timed out",
                    source="brex",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise ProviderTransientError(
                    "Brex transport error",
                    source="brex",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc

            if response.status_code == 429:
                metrics.record_request("rate_limited")
                raise ProviderRateLimited(
                    "Brex rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="brex",
                    operation=operation,
                )
            if response.status_code >= 500:
                metrics.record_request("error")
                raise ProviderTransientError(
                    f"Brex returned HTTP {response.status_code}",
                    source="brex",
                    operation=operation,
                    http_status=response.status_code,
                )
            return response

        response = await self._provider.execute(operation, _once)
        if response.status_code // 100 == 2:
            metrics.record_request("ok")
            return _safe_json(response)

        if response.status_code in (401, 403):
            metrics.record_request("unauthorized")
        else:
            metrics.record_request("error")
        raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_accounts(self) -> list[dict[str, Any]]:
        """List all cash and card accounts visible to the token."""
        cash = await self._list_cursor_items(
            "/v2/accounts/cash",
            item_keys=("items", "accounts"),
            operation="accounts.cash.list",
        )
        for account in cash:
            account.setdefault("_fyralis_account_kind", "cash")
            account.setdefault("type", account.get("type") or "cash")

        card_body = await self._request(
            "GET",
            "/v2/accounts/card",
            operation="accounts.card.list",
        )
        cards = _extract_list(card_body, "items", "accounts", "cards")
        for account in cards:
            account.setdefault("_fyralis_account_kind", "card")
            account.setdefault("type", account.get("type") or "card")

        return cash + cards

    async def get_account(self, account_id: str) -> dict[str, Any]:
        """Return one account by enumerating the real cash/card account lists."""
        for account in await self.list_accounts():
            if _account_id(account) == account_id:
                return account
        raise BrexApiError(
            "brex account not found or not visible to the token",
            code="brex_api_not_found",
            context={"account_id": account_id},
        )

    async def list_transactions(
        self,
        account_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
        account_kind: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """List transactions for a cash account or the primary card account.

        The fetcher still receives the historical `(items, next_offset, total)`
        shape. Real cursor pagination is walked internally and returns
        `next_offset=None` when exhausted. A legacy `{transactions,total}` body
        is still parsed for local compatibility.
        """
        kind = (account_kind or "").lower()
        if kind in {"card", "credit_card", "primary_card"}:
            return await self._list_card_transactions(limit=limit, start=start)
        if kind in {"cash", "checking", "savings"}:
            return await self._list_cash_transactions(
                account_id, limit=limit, start=start,
            )

        try:
            return await self._list_cash_transactions(
                account_id, limit=limit, start=start,
            )
        except BrexApiError as exc:
            if getattr(exc, "code", "") != "brex_api_not_found":
                raise
        return await self._list_card_transactions(limit=limit, start=start)

    async def _list_cash_transactions(
        self, account_id: str, *, limit: int, start: str | None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        path = f"/v2/transactions/cash/{quote(account_id, safe='')}"
        return await self._list_transaction_cursor(
            path,
            limit=limit,
            start=start,
            operation="transactions.cash.list",
        )

    async def _list_card_transactions(
        self, *, limit: int, start: str | None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        return await self._list_transaction_cursor(
            "/v2/transactions/card/primary",
            limit=limit,
            start=start,
            operation="transactions.card.list",
        )

    async def _list_transaction_cursor(
        self,
        path: str,
        *,
        limit: int,
        start: str | None,
        operation: str,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        effective_limit = max(1, min(_MAX_PAGE_SIZE, int(limit or _DEFAULT_PAGE_SIZE)))
        params = _txn_params(effective_limit, start, cursor=None)
        first = await self._request(
            "GET",
            path,
            params=params,
            operation=operation,
        )
        if not isinstance(first, dict):
            raise BrexApiError(
                "brex transactions response was not a JSON object",
                code="brex_api_error",
                context={"path": path},
            )
        items, next_cursor, total = _parse_transactions_page(first)

        if next_cursor is None and "next_cursor" not in first:
            total_int = int(total if total is not None else len(items))
            # Legacy compatibility: one offset-shaped page. The real path does
            # not use offset, so terminality is based on the returned total.
            next_offset = len(items)
            is_last = next_offset >= total_int or not items
            return items, (None if is_last else next_offset), total_int

        all_items = list(items)
        while next_cursor:
            page = await self._request(
                "GET",
                path,
                params=_txn_params(effective_limit, start, cursor=next_cursor),
                operation=operation,
            )
            if not isinstance(page, dict):
                break
            page_items, next_cursor, _ = _parse_transactions_page(page)
            all_items.extend(page_items)
        return all_items, None, len(all_items)

    async def _list_cursor_items(
        self,
        path: str,
        *,
        item_keys: tuple[str, ...],
        operation: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": _MAX_PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor
            body = await self._request(
                "GET",
                path,
                params=params,
                operation=operation,
            )
            if not isinstance(body, dict):
                return out
            out.extend(_extract_list(body, *item_keys))
            raw_next = body.get("next_cursor")
            cursor = raw_next if isinstance(raw_next, str) and raw_next else None
            if not cursor:
                return out


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _txn_params(
    limit: int, start: str | None, *, cursor: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    if start:
        params["posted_at_start"] = start
    return params


def _extract_list(body: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in keys:
            value = body.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def _account_id(account: dict[str, Any]) -> str:
    return str(account.get("id") or account.get("account_id") or "")


def _parse_transactions_page(
    resp: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, int | None]:
    raw_items = resp.get("items")
    if not isinstance(raw_items, list):
        raw_items = resp.get("transactions")
    items = [t for t in raw_items if isinstance(t, dict)] if isinstance(raw_items, list) else []

    if "next_cursor" in resp:
        nc = resp.get("next_cursor")
        next_cursor = nc if isinstance(nc, str) and nc else None
        return items, next_cursor, None

    raw_total = resp.get("total")
    total = int(raw_total) if isinstance(raw_total, (int, float)) else None
    return items, None, total


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> BrexApiError:
    status = response.status_code
    if status in (401, 403):
        return BrexApiError(
            f"brex {status}: API token rejected or insufficient scope",
            code="brex_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return BrexApiError(
            "brex 404: account/resource not found or not visible to the token",
            code="brex_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return BrexApiError(
            "brex rate limit (429), retry budget exhausted",
            code="brex_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return BrexApiError(
        f"brex returned {status}",
        code="brex_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["BrexClient", "_parse_transactions_page"]
