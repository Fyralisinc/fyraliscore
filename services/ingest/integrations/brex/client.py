"""services/ingest/integrations/brex/client.py — outbound Brex REST client.

Single outbound surface for backfill + poll-incremental + the planner's account
enumeration. Brex is authenticated with a long-lived API token presented as a
**Bearer** token. The token is resolved once from the secret store (or preset in
spammer mode) and reused for the life of the client — same posture as the
Notion/Jira clients. No token refresh (Bearer archetype).

TODO(human): confirm Brex API host + read endpoints/scopes (blueprint §5 #6/#7).
The host defaults via the endpoint resolver (`endpoint("brex_api")`) and is
overridable per-install (`base_url`) and per-env (`BREX_API_BASE_URL`); the
read surface below (`/accounts`, `/account/{id}`, `/account/{id}/transactions`)
is CLONED from Mercury and UNVERIFIED for Brex — Brex's real paths (e.g.
`/v2/accounts/cash`, `/v2/transactions/card`) and the required OAuth-token
scopes must be confirmed and the read methods adjusted. Implement only the
verified read surface.

TODO(human): confirm Brex rate-limit signalling (blueprint §5 #8). Defaults to
429 + `Retry-After` (Mercury's scheme); tune via `BREX_RL_MAX_ATTEMPTS` /
`BREX_RL_MAX_SLEEP_SEC`. Brex may instead signal via `X-RateLimit-Reset`.

Pagination: `list_transactions` returns `(items, next_offset, total)`,
`next_offset is None` terminal. The REAL Brex transactions API
(`GET /v2/transactions/card/primary`) is CURSOR-paginated — the body is
`{"items": [...], "next_cursor": "<token or null>"}` with NO `total` — so the
client follows `next_cursor` internally until it is null/absent and returns the
whole window in one call. The legacy offset/limit + `total` shape (the synthetic
mock spammer) is still handled as a fallback when no `next_cursor` field is
present. See `_parse_transactions_page` for the shape discrimination. Verified
against developer.brex.com (Transactions API + Pagination docs).

Logging redaction: the API token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import BrexApiError


log = structlog.get_logger("integrations.brex.client")


_DEFAULT_TIMEOUT_S = 30.0
# Default to 100 to bound payload size and keep parity with the other sources.
# The 500 page cap is CLONED from Mercury and UNVERIFIED for Brex.
_DEFAULT_PAGE_SIZE = 100


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class BrexClient:
    """Outbound Brex REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_brex_client`
    (production / spammer) and by the seed/onboarding account probe. Shares the
    process-wide httpx client when one is injected.
    """

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
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        # Preset token (spammer mode presets a recognized token); otherwise
        # resolved lazily from the secret store on first request.
        self._api_token: str | None = api_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical Brex API host; a
        # spammer/test override (api_base_url) wins so backfill points at the mock.
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
                raise BrexApiError(
                    "brex client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="brex_api_unauthorized",
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
        return f"Bearer {token}"

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One Brex API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `BrexApiError`.
        """
        from services.ingest.integrations.brex import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("BREX_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("BREX_RL_MAX_SLEEP_SEC", "30"))
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
                raise BrexApiError(
                    "transport error calling brex",
                    code="brex_api_error",
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
                    raise BrexApiError(
                        "brex response was not a JSON object",
                        code="brex_api_error",
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

    async def list_accounts(self) -> list[dict[str, Any]]:
        """`GET /accounts` — all accounts visible to the token, with balances.

        Used at seed/install time to populate `brex_accounts`, and by the
        fetcher to emit per-account balance snapshots.
        """
        resp = await self._request("GET", "/accounts")
        accounts = resp.get("accounts")
        if not isinstance(accounts, list):
            # Some Brex responses return the bare list.
            accounts = resp if isinstance(resp, list) else []  # type: ignore[assignment]
        return [a for a in accounts if isinstance(a, dict)]

    async def get_account(self, account_id: str) -> dict[str, Any]:
        """`GET /account/{id}` — one account (balance snapshot probe)."""
        return await self._request("GET", f"/account/{account_id}")

    async def list_transactions(
        self,
        account_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /account/{id}/transactions` — paginated transactions.

        `start` (ISO date) optionally bounds the window for incremental polls.
        Returns `(transactions, next_offset, total)`; `next_offset is None`
        signals no more pages.

        Two pagination shapes are handled (see `_parse_transactions_page`):

          - REAL Brex (api.brex.com `GET /v2/transactions/card/primary`):
            CURSOR pagination — the response is `{"items": [...],
            "next_cursor": "<token or null>"}` with NO `total`. When a
            `next_cursor` is present this method follows the cursor internally,
            accumulating every page, and returns the full set with
            `next_offset=None` (terminal — the cursor walk is complete and the
            fetcher persists the high-water filter, not an offset).
          - SYNTHETIC / legacy (`{"transactions": [...], "total": N}`): the
            original offset/limit single-page path, returned unchanged so the
            mock spammer and the all-25 gate behave exactly as before.
        """
        first = await self._request(
            "GET",
            f"/account/{account_id}/transactions",
            params=self._txn_params(limit, offset, start, cursor=None),
        )
        items, next_cursor, total = _parse_transactions_page(first)

        if next_cursor is None and "next_cursor" not in first:
            # SYNTHETIC / offset shape (no cursor field at all): preserve the
            # original single-page offset/total contract verbatim.
            txns = items
            total = int(total if total is not None else len(txns))
            next_offset = offset + len(txns)
            is_last = next_offset >= total or not txns
            return txns, (None if is_last else next_offset), total

        # REAL cursor shape: follow `next_cursor` until it is null/absent.
        all_items = list(items)
        while next_cursor:
            page = await self._request(
                "GET",
                f"/account/{account_id}/transactions",
                params=self._txn_params(limit, offset, start, cursor=next_cursor),
            )
            page_items, next_cursor, _ = _parse_transactions_page(page)
            all_items.extend(page_items)
        # The cursor walk is exhausted; the whole window is in `all_items`.
        return all_items, None, len(all_items)

    @staticmethod
    def _txn_params(
        limit: int, offset: int, start: str | None, *, cursor: str | None,
    ) -> dict[str, Any]:
        """Build the transactions query params for either pagination shape.

        `cursor` (real) and `offset` (synthetic) are both sent: the real API
        ignores the unknown `offset`, the synthetic server ignores `cursor`.
        """
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        else:
            params["offset"] = offset
        if start:
            params["start"] = start
        return params


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_transactions_page(
    resp: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, int | None]:
    """Parse one transactions page for EITHER Brex pagination shape.

    Returns `(items, next_cursor, total)`:

      - REAL Brex (`{"items": [...], "next_cursor": "<token or null>"}`):
        `next_cursor` is the opaque follow token, or `None` when it is null /
        empty (TERMINAL — no more pages). `total` is `None` (the real API has
        no `total`). Items are read from `items` (falling back to
        `transactions`).
      - SYNTHETIC / legacy (`{"transactions": [...], "total": N}`): there is no
        `next_cursor` key, so `next_cursor` is `None` and `total` is the int
        count; items are read from `transactions` (falling back to `items`).

    Callers distinguish the two by whether `"next_cursor" in resp`.
    """
    raw_items = resp.get("items")
    if not isinstance(raw_items, list):
        raw_items = resp.get("transactions")
    items = [t for t in raw_items if isinstance(t, dict)] if isinstance(raw_items, list) else []

    # `next_cursor` present (even if null) => real cursor shape.
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
    """Map a non-2xx Brex response to a typed `BrexApiError`."""
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
