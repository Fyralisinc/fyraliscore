"""services/ingest/integrations/miro/client.py — outbound Miro REST client.

Single outbound surface for backfill + poll-incremental + the planner's board
enumeration. Miro is authenticated with a long-lived Bearer token issued to an
org-level app. The token is resolved once from the secret store (or preset in
spammer mode) and reused for the life of the client — same posture as the
Brex/Notion/Jira clients. No token refresh (Bearer archetype).

TODO(human): confirm Miro API host + read endpoints/scopes (board enumeration +
board-items list). The host defaults via the endpoint resolver
(`endpoint("miro_api")`) and is overridable per-install (`base_url`) and per-env
(`MIRO_API_BASE_URL`); the read surface below (`/boards`, `/boards/{id}`,
`/boards/{id}/items`) is CLONED from Brex and UNVERIFIED for Miro — Miro's real
paths (e.g. `/v2/boards`, `/v2/boards/{board_id}/items`) and the required OAuth
scopes must be confirmed and the read methods adjusted. Implement only the
verified read surface.

TODO(human): confirm Miro rate-limit signalling. Defaults to 429 +
`Retry-After` (Brex's scheme); tune via `MIRO_RL_MAX_ATTEMPTS` /
`MIRO_RL_MAX_SLEEP_SEC`. Miro may instead signal credit-based limits via
`X-RateLimit-*` headers.

Pagination: list helpers return `(items, next_cursor, total)`, `next_cursor is
None` terminal — OPAQUE CURSOR token (Miro's `cursor` query param + the
response `cursor`), NOT offset/limit. The cursor is whatever opaque string the
API returns; the fetcher round-trips it verbatim. This is CLONED-and-adapted
from Brex's offset scheme to a cursor scheme but UNVERIFIED for Miro (see the
fetcher's pagination TODO).

Logging redaction: the Bearer token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import MiroApiError


log = structlog.get_logger("integrations.miro.client")


_DEFAULT_TIMEOUT_S = 30.0
# Default to 50 (Miro's typical board-items page cap) to bound payload size.
# The page cap is CLONED from Brex (100) but lowered to a Miro-plausible value;
# UNVERIFIED — confirm against the board-items list endpoint.
_DEFAULT_PAGE_SIZE = 50


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class MiroClient:
    """Outbound Miro REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_miro_client`
    (production / spammer) and by the seed/onboarding board probe. Shares the
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
        # In production the base is the canonical Miro API host; a
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
                raise MiroApiError(
                    "miro client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="miro_api_unauthorized",
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
        """One Miro API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `MiroApiError`.
        """
        from services.ingest.integrations.miro import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("MIRO_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("MIRO_RL_MAX_SLEEP_SEC", "30"))
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
                raise MiroApiError(
                    "transport error calling miro",
                    code="miro_api_error",
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
                    raise MiroApiError(
                        "miro response was not a JSON object",
                        code="miro_api_error",
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

    async def list_boards(self) -> list[dict[str, Any]]:
        """`GET /boards` — all boards visible to the org token.

        Used at seed/install time to populate `miro_boards`, and by the planner
        to emit one shard per board.
        """
        resp = await self._request("GET", "/boards")
        boards = resp.get("data")
        if not isinstance(boards, list):
            boards = resp.get("boards")
        if not isinstance(boards, list):
            # Some responses return the bare list.
            boards = resp if isinstance(resp, list) else []  # type: ignore[assignment]
        return [b for b in boards if isinstance(b, dict)]

    async def get_board(self, board_id: str) -> dict[str, Any]:
        """`GET /boards/{id}` — one board (metadata probe)."""
        return await self._request("GET", f"/boards/{board_id}")

    async def list_items(
        self,
        board_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int]:
        """`GET /boards/{id}/items` — cursor-paginated board items.

        `cursor` (opaque) continues a previous page. Returns
        `(items, next_cursor, total)`; `next_cursor is None` signals no more
        pages. The cursor is round-tripped verbatim from the response.
        """
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        resp = await self._request(
            "GET", f"/boards/{board_id}/items", params=params,
        )
        items = resp.get("data")
        if not isinstance(items, list):
            items = resp.get("items")
        items = [i for i in items if isinstance(i, dict)] if isinstance(items, list) else []
        total = int(resp.get("total", len(items)) or 0)
        # Opaque cursor: the response carries the NEXT cursor (Miro returns it
        # under `cursor`); absent/empty => terminal page.
        raw_cursor = resp.get("cursor")
        next_cursor = raw_cursor if isinstance(raw_cursor, str) and raw_cursor else None
        return items, next_cursor, total


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
) -> MiroApiError:
    """Map a non-2xx Miro response to a typed `MiroApiError`."""
    status = response.status_code
    if status in (401, 403):
        return MiroApiError(
            f"miro {status}: token rejected or insufficient scope",
            code="miro_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return MiroApiError(
            "miro 404: board/resource not found or not visible to the token",
            code="miro_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return MiroApiError(
            "miro rate limit (429), retry budget exhausted",
            code="miro_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return MiroApiError(
        f"miro returned {status}",
        code="miro_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["MiroClient"]
