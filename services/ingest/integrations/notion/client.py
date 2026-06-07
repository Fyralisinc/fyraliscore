"""services/ingest/integrations/notion/client.py — outbound Notion REST client.

Single outbound surface for backfill + poll-incremental. Notion bot
tokens are LONG-LIVED (issued once at OAuth install), so — unlike the
GitHub client — there is no per-request token mint / JWT / token cache.
The token is resolved once from the secret store (or preset in spammer
mode) and reused for the life of the client.

Rate limits (R1 / Risk #1): Notion enforces ~3 req/s per integration and
returns HTTP 429 with a `Retry-After` (integer seconds) header. All read
methods route through `_request`, which honours `Retry-After` with a
bounded retry budget before surfacing `NotionApiError(rate_limited)` —
the same posture as the Slack/GitHub clients.

Pagination: every Notion list endpoint returns
`{results: [...], next_cursor: str|null, has_more: bool}`. The list
helpers return the `(results, next_cursor, has_more)` triple the fetcher
and reconciler consume; `has_more=false` is the terminal signal.

Logging redaction: the bot token is NEVER logged. The workspace id is
hashed (`workspace_id_hash`) before it touches a log line.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import NotionApiError


log = structlog.get_logger("integrations.notion.client")


_NOTION_API_BASE = "https://api.notion.com"
# Pin the Notion API version (R5 / Risk #5). Notion requires the
# `Notion-Version` header on every request; behaviour changes across
# versions, so the pin is a reviewed constant. Env override for tests
# and controlled bumps.
_NOTION_VERSION = os.environ.get("NOTION_API_VERSION", "2022-06-28")
_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100  # Notion's maximum.


def short_workspace_hash(workspace_id: str) -> str:
    """Non-reversible 16-hex digest of a workspace id for logs."""
    return hashlib.blake2b(
        workspace_id.encode("utf-8"), digest_size=8,
    ).hexdigest()


def _parse_retry_after(value: str | None) -> float:
    """Seconds to wait from a Retry-After header. Falls back to 1s."""
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class NotionClient:
    """Outbound Notion REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_notion_client`
    (production / spammer) and by the OAuth callback's workspace probe.
    Shares the process-wide httpx client when one is injected.
    """

    def __init__(
        self,
        *,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        workspace_id: str | None = None,
        bot_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._workspace_id = workspace_id
        # Preset token (OAuth callback hands the freshly-minted token, and
        # spammer mode presets a recognized token); otherwise resolved
        # lazily from the secret store on first request.
        self._bot_token: str | None = bot_token
        self._token_lock = asyncio.Lock()
        self._api_base_url = (api_base_url or _NOTION_API_BASE).rstrip("/")
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
        if self._bot_token is not None:
            return self._bot_token
        async with self._token_lock:
            if self._bot_token is not None:
                return self._bot_token
            if (
                self._secret_store is None
                or self._secret_ref is None
                or self._tenant_id is None
            ):
                raise NotionApiError(
                    "notion client has no bot token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="notion_api_unauthorized",
                )
            raw = await self._secret_store.get(
                self._secret_ref, tenant_id=self._tenant_id,
            )
            self._bot_token = (
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
            return self._bot_token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One Notion API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429
        after the budget is spent) is mapped to `NotionApiError`.
        """
        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": _NOTION_VERSION,
            "Content-Type": "application/json",
        }
        max_attempts = int(os.environ.get("NOTION_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("NOTION_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.request(
                    method, url, headers=headers,
                    json=json_body, params=params,
                )
            except httpx.TransportError as exc:
                # Network blip — transient, so recoverable (backfill parks
                # + retries rather than terminal-failing the shard).
                raise NotionApiError(
                    "transport error calling notion",
                    code="notion_api_error",
                    recoverable=True,
                    context={"error_type": type(exc).__name__, "path": path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise NotionApiError(
                        "notion response was not a JSON object",
                        code="notion_api_error",
                        context={"path": path},
                    )
                return body

            # Revocation chokepoint (R2): a 401 means the integration token
            # was revoked / the integration removed. Disable the install so
            # the backfill orphan-scan parks (instead of hammering a dead
            # token) and inbound webhooks for this workspace stop resolving.
            # Fired BEFORE raising; the raised 401 is `recoverable` so the
            # in-flight shard parks rather than terminal-failing the run.
            if response.status_code == 401:
                await self._maybe_disable_on_revocation()

            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def search(
        self,
        *,
        object_filter: str | None = None,
        start_cursor: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`POST /v1/search`. `object_filter` ∈ {"database","page"} narrows
        results to that object type (None = both). Returns
        `(results, next_cursor, has_more)`."""
        body: dict[str, Any] = {"page_size": page_size}
        if object_filter is not None:
            body["filter"] = {"value": object_filter, "property": "object"}
        if start_cursor is not None:
            body["start_cursor"] = start_cursor
        return _unwrap_list(await self._request("POST", "/v1/search", json_body=body))

    async def query_database(
        self,
        database_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`POST /v1/databases/{id}/query` — rows (page objects) of a DB."""
        body: dict[str, Any] = {"page_size": page_size}
        if start_cursor is not None:
            body["start_cursor"] = start_cursor
        return _unwrap_list(
            await self._request(
                "POST", f"/v1/databases/{database_id}/query", json_body=body,
            )
        )

    async def list_block_children(
        self,
        block_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`GET /v1/blocks/{id}/children` — child blocks of a page/block."""
        params: dict[str, Any] = {"page_size": page_size}
        if start_cursor is not None:
            params["start_cursor"] = start_cursor
        return _unwrap_list(
            await self._request(
                "GET", f"/v1/blocks/{block_id}/children", params=params,
            )
        )

    async def list_comments(
        self,
        block_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`GET /v1/comments?block_id=…` — comments on a page/block."""
        params: dict[str, Any] = {"block_id": block_id, "page_size": page_size}
        if start_cursor is not None:
            params["start_cursor"] = start_cursor
        return _unwrap_list(
            await self._request("GET", "/v1/comments", params=params)
        )

    async def latest_database_edit(self, database_id: str) -> str | None:
        """`last_edited_time` of the most-recently-edited row in a database,
        or None if empty/unknown. Used by the reconciler's gap probe — a
        single 1-row query sorted descending by edit time."""
        body = await self._request(
            "POST", f"/v1/databases/{database_id}/query",
            json_body={
                "page_size": 1,
                "sorts": [
                    {"timestamp": "last_edited_time", "direction": "descending"},
                ],
            },
        )
        results, _cursor, _more = _unwrap_list(body)
        if results:
            edited = results[0].get("last_edited_time")
            return edited if isinstance(edited, str) else None
        return None

    async def latest_page_edit(self) -> str | None:
        """`last_edited_time` of the most-recently-edited LOOSE page (one NOT
        owned by a database) in the workspace — the reconciler probe for the
        notion_page_tree shard.

        Database rows are EXCLUDED to mirror the page_tree fetcher, which skips
        `_is_database_row` pages because they are covered by notion_database
        shards (services/ingest/ingestion/fetchers/notion.py loose_pages walk). Without
        this exclusion the probe returns the newest database row — a timestamp
        the page-tree walk never records as its high-water — so the reconciler
        sees `latest > high_water` on every pass and re-shares forever
        (IN-14 convergence; the page_tree probe must match the page_tree
        coverage). `/v1/search` sorts descending, so we scan one bounded page
        and return the newest non-database page; if the newest pages are all
        database rows we return None (no loose-page change to chase)."""
        body = await self._request(
            "POST", "/v1/search",
            json_body={
                "page_size": 50,
                "filter": {"value": "page", "property": "object"},
                "sort": {"timestamp": "last_edited_time", "direction": "descending"},
            },
        )
        results, _cursor, _more = _unwrap_list(body)
        for page in results:
            parent = page.get("parent")
            if isinstance(parent, dict) and parent.get("type") == "database_id":
                continue  # database row — owned by a notion_database shard
            edited = page.get("last_edited_time")
            return edited if isinstance(edited, str) else None
        return None

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        """`GET /v1/pages/{id}` — a single page object (properties only)."""
        return await self._request("GET", f"/v1/pages/{page_id}")

    async def retrieve_bot_user(self) -> dict[str, Any]:
        """`GET /v1/users/me` — the bot user; its `bot.workspace_name` and
        the response `id` identify the install. Used by the OAuth callback
        as a connectivity probe."""
        return await self._request("GET", "/v1/users/me")

    # -----------------------------------------------------------------
    # Revocation chokepoint
    # -----------------------------------------------------------------

    async def _maybe_disable_on_revocation(self) -> None:
        """On a 401, disable this workspace's install (R2 chokepoint).

        Requires the DB-backed context `(pool, tenant_id, workspace_id)` —
        present for the backfill/reconcile/webhook clients but NOT in
        spammer mode (preset token, no pool) or the OAuth probe. When any
        is missing we log and skip; a later DB-backed failure fires it.
        Never raises (the caller is about to surface the original 401).
        """
        if (
            self._pool is None
            or self._tenant_id is None
            or self._workspace_id is None
        ):
            log.info(
                "notion_chokepoint_skipped_no_context",
                has_pool=self._pool is not None,
                has_tenant=self._tenant_id is not None,
                has_workspace=self._workspace_id is not None,
            )
            return
        # Imported lazily to avoid a client→uninstall→client import cycle.
        from services.ingest.integrations.notion.uninstall import (
            _disable_installation_notion,
        )

        await _disable_installation_notion(
            pool=self._pool,
            tenant_id=self._tenant_id,
            workspace_id=str(self._workspace_id),
        )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _unwrap_list(
    body: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, bool]:
    """Extract `(results, next_cursor, has_more)` from a Notion list
    response. Defensive against missing fields (treats absence as a
    terminal empty page)."""
    results = body.get("results")
    if not isinstance(results, list):
        results = []
    next_cursor = body.get("next_cursor")
    if not isinstance(next_cursor, str):
        next_cursor = None
    has_more = bool(body.get("has_more"))
    return [r for r in results if isinstance(r, dict)], next_cursor, has_more


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> NotionApiError:
    """Map a non-2xx Notion response to a typed `NotionApiError`. Notion
    error bodies carry `{code, message, status}`."""
    body = _safe_json(response)
    notion_code = body.get("code") if isinstance(body, dict) else None
    status = response.status_code

    if status == 401:
        # Token revoked / integration removed. RECOVERABLE because the
        # outbound chokepoint (NotionClient._maybe_disable_on_revocation)
        # disables the install on this same 401 — so the backfill parks the
        # shard (does not terminal-fail the run) and the orphan-scan then
        # re-claims it cheaply against the now-disabled install (no further
        # API calls) until a re-OAuth / re-enable resumes it. (GitHub keeps
        # 401 non-recoverable because it has an unsuspend webhook + re-plan;
        # Notion has neither, so parking-until-re-enable is the resilient
        # path — IN-14 worker-crash hardening.)
        return NotionApiError(
            "notion 401: integration token rejected",
            code="notion_api_unauthorized",
            recoverable=True,
            context={"http_status": 401, "notion_code": notion_code, "path": path},
        )
    if status == 404:
        # Genuine not-found (object deleted / un-shared) — not transient.
        # The fetcher already skips a single-object 404 mid-walk; if a 404
        # reaches the shard boundary it is a real config fault, fail fast.
        return NotionApiError(
            "notion 404: object not found or not shared with integration",
            code="notion_api_not_found",
            context={"http_status": 404, "notion_code": notion_code, "path": path},
        )
    if status == 429:
        return NotionApiError(
            "notion rate limit (429), retry budget exhausted",
            code="notion_api_rate_limited",
            recoverable=True,
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    # Upstream 5xx is transient — recoverable (park + retry). Other 4xx is
    # a terminal client fault — fail fast.
    return NotionApiError(
        f"notion returned {status}",
        code="notion_api_error",
        recoverable=status >= 500,
        context={"http_status": status, "notion_code": notion_code, "path": path},
    )


__all__ = ["NotionClient", "short_workspace_hash"]
