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
from services.ingest.integrations.secret_cache import SecretValueCache


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
        self._api_token_cache = SecretValueCache(preset=api_token)
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
        return await self._api_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: MiroApiError(
                "miro client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="miro_api_unauthorized",
            )
        )

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

        ADDITIVE pagination (verified against developers.miro.com): the real
        `GET /v2/boards` returns an OFFSET-paginated envelope
        ``{"data":[...],"total":N,"size":N,"offset":N,"limit":N,
        "links":{"self":...,"next":"<url>"}}``. We walk every page so a tenant
        with >limit boards is fully enumerated (the old single-page fetch
        silently dropped boards past the first page). Page advance prefers the
        server-supplied ``links.next`` cursor (round-tripped verbatim); when no
        ``links.next`` is present we fall back to advancing ``offset`` by the
        page ``size`` while ``offset + size < total``. The synthetic mock and
        any bare-list / unpaginated response still terminate after one page
        (no ``links.next``, no ``total`` larger than what we've seen), so the
        all-25 synthetic gate is unaffected.
        """
        out: list[dict[str, Any]] = []
        # `next_path` is whatever the server hands us in links.next (a URL or a
        # path); on the first call we hit the base /boards path. We bound the
        # walk to defend against a server that returns a self-referential next.
        next_path: str | None = "/boards"
        offset = 0
        seen_offsets: set[str] = set()
        max_pages = int(os.environ.get("MIRO_BOARDS_MAX_PAGES", "1000"))
        pages = 0
        while next_path is not None and pages < max_pages:
            pages += 1
            resp = await self._request("GET", next_path)
            out.extend(_boards_from_response(resp))

            # 1) Prefer the explicit server cursor (links.next). Absent => done.
            link_next = _links_next(resp)
            if link_next is not None:
                rel = _relativize(link_next, self._api_base_url)
                if rel in seen_offsets:
                    break  # self-referential guard
                seen_offsets.add(rel)
                next_path = rel
                continue

            # 2) No links.next: fall back to offset/limit math. Advance only
            #    while the envelope says more rows remain (offset+size < total).
            total = resp.get("total")
            size = resp.get("size")
            limit = resp.get("limit")
            cur_offset = resp.get("offset")
            if (
                isinstance(total, int)
                and isinstance(size, int)
                and size > 0
            ):
                base = cur_offset if isinstance(cur_offset, int) else offset
                step = limit if isinstance(limit, int) and limit > 0 else size
                offset = base + size
                if offset >= total:
                    next_path = None
                else:
                    key = f"offset={offset}"
                    if key in seen_offsets:
                        break
                    seen_offsets.add(key)
                    next_path = f"/boards?offset={offset}&limit={step}"
                continue

            # 3) No cursor and no offset/total envelope (bare list / synthetic /
            #    legacy single-page response): terminal after one page.
            next_path = None

        return out

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

def _boards_from_response(resp: Any) -> list[dict[str, Any]]:
    """Extract the board dicts from one `GET /boards` page.

    Handles the real envelope (`data`), the legacy `boards` key, and a bare
    list response — the same fallbacks the original single-page code used."""
    if isinstance(resp, list):
        return [b for b in resp if isinstance(b, dict)]
    boards = resp.get("data") if isinstance(resp, dict) else None
    if not isinstance(boards, list):
        boards = resp.get("boards") if isinstance(resp, dict) else None
    if not isinstance(boards, list):
        return []
    return [b for b in boards if isinstance(b, dict)]


def _links_next(resp: Any) -> str | None:
    """Return the `links.next` cursor URL/path if the page advertises one.

    Miro's v2 envelope carries pagination links under `links`; `next` is
    present only while more pages remain (absent/empty => terminal page)."""
    if not isinstance(resp, dict):
        return None
    links = resp.get("links")
    if not isinstance(links, dict):
        return None
    nxt = links.get("next")
    return nxt if isinstance(nxt, str) and nxt else None


def _relativize(url: str, api_base_url: str) -> str:
    """Turn a (possibly absolute) `links.next` into a path `_request` can use.

    `_request` prepends `self._api_base_url`, so a full URL must be stripped
    back to its path(+query). A bare path is returned unchanged (leading slash
    ensured)."""
    base = api_base_url.rstrip("/")
    if base and url.startswith(base):
        rel = url[len(base):]
        return rel if rel.startswith("/") else f"/{rel}"
    if url.startswith(("http://", "https://")):
        # Different host than our base: strip scheme+host, keep path+query.
        from urllib.parse import urlsplit

        parts = urlsplit(url)
        rel = parts.path or "/"
        if parts.query:
            rel = f"{rel}?{parts.query}"
        return rel
    return url if url.startswith("/") else f"/{url}"


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
