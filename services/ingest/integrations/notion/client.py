"""services/ingest/integrations/notion/client.py — outbound Notion REST client.

Single outbound surface for backfill, reconciliation, and webhook hydration.
Notion bot tokens are LONG-LIVED (issued once at OAuth install), so there is no
per-request token mint or refresh. The token is resolved once from the secret
store (or preset in Provider-Lab mode) and reused for the life of the client.

Every actual provider attempt runs through ``ProviderTransport``. HTTP 429,
timeouts, and 5xx responses become its typed retry contract; long cooldowns
therefore escape as ``RetryLater`` for durable workflow scheduling rather than
sleeping in this client or returning an unchanged cursor.

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


class NotionClient:
    """Outbound Notion REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_notion_client`
    (production / Provider Lab) and by the OAuth callback's workspace probe.
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
        self._workspace_id = workspace_id
        # Preset token (OAuth callback hands the freshly-minted token, and
        # Provider Lab mode supplies a recognized token); otherwise resolved
        # lazily from the secret store on first request.
        self._bot_token_cache = SecretValueCache(preset=bot_token)
        self._token_lock = asyncio.Lock()
        self._api_base_url = (api_base_url or _NOTION_API_BASE).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                http_client is not None or api_base_url is not None
            ),
        )
        self._provider = ProviderRequestBinding(
            source="notion",
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
        return await self._bot_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: NotionApiError(
                "notion client has no bot token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="notion_api_unauthorized",
            )
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> dict[str, Any]:
        """Execute one semantic Notion operation through ProviderTransport."""
        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": _NOTION_VERSION,
            "Content-Type": "application/json",
        }
        client = self._httpx()

        async def _once() -> httpx.Response:
            try:
                response = await client.request(
                    method, url, headers=headers,
                    json=json_body, params=params,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Notion request timed out",
                    source="notion",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Notion transport error",
                    source="notion",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            if response.status_code == 429:
                raise ProviderRateLimited(
                    "Notion rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="notion",
                    operation=operation,
                )
            if response.status_code >= 500:
                raise ProviderTransientError(
                    f"Notion returned HTTP {response.status_code}",
                    source="notion",
                    operation=operation,
                    http_status=response.status_code,
                )
            return response

        response = await self._provider.execute(operation, _once)
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
        return _unwrap_list(
            await self._request(
                "POST",
                "/v1/search",
                json_body=body,
                operation="search",
            )
        )

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
                "POST",
                f"/v1/databases/{database_id}/query",
                json_body=body,
                operation="databases.query",
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
                "GET",
                f"/v1/blocks/{block_id}/children",
                params=params,
                operation="blocks.children.list",
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
            await self._request(
                "GET",
                "/v1/comments",
                params=params,
                operation="comments.list",
            )
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
            operation="databases.query",
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
        coverage). `/v1/search` sorts descending, so we scan the result set
        and return the newest non-database page; if the entire result set is
        database rows we return None (no loose-page change to chase).

        Pagination (Phase-3 drift fix, finding #36): real Notion paginates
        `/v1/search` with `has_more`/`next_cursor`, so a workspace whose newest
        objects are ALL database rows can push every loose page past the first
        page. We loop, threading `next_cursor` back as `start_cursor`, until we
        find a loose page or `has_more` is false. The single-call path is the
        fallback (a response with `has_more` absent/false stops after one call),
        so Provider Lab's bounded fixtures behave exactly as before."""
        start_cursor: str | None = None
        while True:
            json_body: dict[str, Any] = {
                "page_size": _DEFAULT_PAGE_SIZE,
                "filter": {"value": "page", "property": "object"},
                "sort": {"timestamp": "last_edited_time", "direction": "descending"},
            }
            if start_cursor is not None:
                json_body["start_cursor"] = start_cursor
            body = await self._request(
                "POST",
                "/v1/search",
                json_body=json_body,
                operation="search",
            )
            results, next_cursor, has_more = _unwrap_list(body)
            for page in results:
                parent = page.get("parent")
                if isinstance(parent, dict) and parent.get("type") == "database_id":
                    continue  # database row — owned by a notion_database shard
                edited = page.get("last_edited_time")
                return edited if isinstance(edited, str) else None
            # No loose page on this page of results. Continue ONLY when the real
            # `has_more=true` signal is set and a cursor advances us; absent/false
            # `has_more` (the single-call fallback) terminates here.
            if not (has_more and next_cursor):
                return None
            start_cursor = next_cursor

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        """`GET /v1/pages/{id}` — a single page object (properties only)."""
        return await self._request(
            "GET",
            f"/v1/pages/{page_id}",
            operation="pages.retrieve",
        )

    async def retrieve_bot_user(self) -> dict[str, Any]:
        """`GET /v1/users/me` — the bot user; its `bot.workspace_name` and
        the response `id` identify the install. Used by the OAuth callback
        as a connectivity probe."""
        return await self._request(
            "GET",
            "/v1/users/me",
            operation="users.me",
        )

    # -----------------------------------------------------------------
    # Revocation chokepoint
    # -----------------------------------------------------------------

    async def _maybe_disable_on_revocation(self) -> None:
        """On a 401, disable this workspace's install (R2 chokepoint).

        Requires the DB-backed context `(pool, tenant_id, workspace_id)` —
        present for the backfill/reconcile/webhook clients but NOT in
        Provider Lab mode (preset token, no pool) or the OAuth probe. When any
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
