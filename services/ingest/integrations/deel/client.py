"""services/ingest/integrations/deel/client.py — outbound Deel REST client.

Single outbound surface for backfill + poll-incremental + the planner's contract
enumeration. Deel is authenticated with a long-lived API token presented as a
**Bearer** token. The token is resolved once from the secret store (or preset in
spammer mode) and reused for the life of the client — same posture as the
Notion/Jira clients.

TODO(human): confirm Deel read endpoints + OAuth scopes. The paths below
(`/contracts`, `/contract/{id}`, `/contract/{id}/payments`) follow the Mercury
archetype's shape; verify them (and any required token scopes) against the Deel
API docs before prod — only the verified read surface should ship.

Rate limits: TODO(human): confirm Deel rate-limit signalling (429 + Retry-After
vs X-RateLimit-Reset). The archetype defaults to 429 + `Retry-After`; tune via
`DEEL_RL_MAX_ATTEMPTS` / `DEEL_RL_MAX_SLEEP_SEC`. All read methods route through
`_request`, which honours `Retry-After` with a bounded retry budget before
surfacing `DeelApiError(deel_api_rate_limited)`.

Pagination: list endpoints return `{total, contracts|payments: [...]}` and
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

from lib.shared.errors import DeelApiError


log = structlog.get_logger("integrations.deel.client")


_DEFAULT_TIMEOUT_S = 30.0
# Deel payment listing caps the page at 500; default to 100 to bound
# payload size and keep parity with the other sources.
_DEFAULT_PAGE_SIZE = 100


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class DeelClient:
    """Outbound Deel REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_deel_client`
    (production / spammer) and by the seed/onboarding contract probe. Shares the
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
        # In production the base is the canonical Deel API host; a
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
                raise DeelApiError(
                    "deel client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="deel_api_unauthorized",
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
        """One Deel API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `DeelApiError`.
        """
        from services.ingest.integrations.deel import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
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
        """`GET /contracts` — all contracts visible to the token.

        Used at seed/install time to populate `deel_contracts`, and by the
        fetcher to emit per-contract state snapshots.
        """
        resp = await self._request("GET", "/contracts")
        contracts = resp.get("contracts")
        if not isinstance(contracts, list):
            # Some Deel responses return the bare list.
            contracts = resp if isinstance(resp, list) else []  # type: ignore[assignment]
        return [c for c in contracts if isinstance(c, dict)]

    async def get_contract(self, contract_id: str) -> dict[str, Any]:
        """`GET /contract/{id}` — one contract (state snapshot probe)."""
        return await self._request("GET", f"/contract/{contract_id}")

    async def list_payments(
        self,
        contract_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /contract/{id}/payments` — paginated payments.

        `start` (ISO date) optionally bounds the window for incremental polls.
        Returns `(payments, next_offset, total)`; `next_offset is None`
        signals no more pages.
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if start:
            params["start"] = start
        resp = await self._request(
            "GET", f"/contract/{contract_id}/payments", params=params,
        )
        payments = resp.get("payments")
        payments = [p for p in payments if isinstance(p, dict)] if isinstance(payments, list) else []
        total = int(resp.get("total", len(payments)) or 0)
        next_offset = offset + len(payments)
        is_last = next_offset >= total or not payments
        return payments, (None if is_last else next_offset), total


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
) -> DeelApiError:
    """Map a non-2xx Deel response to a typed `DeelApiError`."""
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
