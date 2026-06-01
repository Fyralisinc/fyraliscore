"""services/ingest/integrations/mercury/client.py — outbound Mercury banking REST client.

Single outbound surface for backfill + poll-incremental + the planner's account
enumeration. Mercury is authenticated with a long-lived API token presented as a
**Bearer** token (the token is also accepted as the Basic-auth username with an
empty password). The token is resolved once from the secret store (or preset in
spammer mode) and reused for the life of the client — same posture as the
Notion/Jira clients.

Rate limits: Mercury returns HTTP 429 with a `Retry-After` header. All read
methods route through `_request`, which honours `Retry-After` with a bounded
retry budget before surfacing `MercuryApiError(mercury_api_rate_limited)`.

Pagination: list endpoints return `{total, accounts|transactions: [...]}` and
accept `limit` + `offset`. The list helpers return `(items, next_offset,
is_last)`; `next_offset is None` is terminal.

Logging redaction: the API token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import MercuryApiError


log = structlog.get_logger("integrations.mercury.client")


_DEFAULT_TIMEOUT_S = 30.0
# Mercury transaction listing caps the page at 500; default to 100 to bound
# payload size and keep parity with the other sources.
_DEFAULT_PAGE_SIZE = 100


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class MercuryClient:
    """Outbound Mercury REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_mercury_client`
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
        # In production the base is the canonical Mercury API host; a
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
                raise MercuryApiError(
                    "mercury client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="mercury_api_unauthorized",
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
        """One Mercury API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `MercuryApiError`.
        """
        from services.ingest.integrations.mercury import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("MERCURY_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("MERCURY_RL_MAX_SLEEP_SEC", "30"))
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
                raise MercuryApiError(
                    "transport error calling mercury",
                    code="mercury_api_error",
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
                    raise MercuryApiError(
                        "mercury response was not a JSON object",
                        code="mercury_api_error",
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

        Used at seed/install time to populate `mercury_accounts`, and by the
        fetcher to emit per-account balance snapshots.
        """
        resp = await self._request("GET", "/accounts")
        accounts = resp.get("accounts")
        if not isinstance(accounts, list):
            # Some Mercury responses return the bare list.
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
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if start:
            params["start"] = start
        resp = await self._request(
            "GET", f"/account/{account_id}/transactions", params=params,
        )
        txns = resp.get("transactions")
        txns = [t for t in txns if isinstance(t, dict)] if isinstance(txns, list) else []
        total = int(resp.get("total", len(txns)) or 0)
        next_offset = offset + len(txns)
        is_last = next_offset >= total or not txns
        return txns, (None if is_last else next_offset), total


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
) -> MercuryApiError:
    """Map a non-2xx Mercury response to a typed `MercuryApiError`."""
    status = response.status_code
    if status in (401, 403):
        return MercuryApiError(
            f"mercury {status}: API token rejected or insufficient scope",
            code="mercury_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return MercuryApiError(
            "mercury 404: account/resource not found or not visible to the token",
            code="mercury_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return MercuryApiError(
            "mercury rate limit (429), retry budget exhausted",
            code="mercury_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return MercuryApiError(
        f"mercury returned {status}",
        code="mercury_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["MercuryClient"]
