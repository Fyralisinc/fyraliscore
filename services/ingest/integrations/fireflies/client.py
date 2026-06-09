"""services/ingest/integrations/fireflies/client.py — outbound Fireflies client.

Single outbound surface for backfill + poll-incremental + the planner's
workspace probe. Fireflies is authenticated with a long-lived API token
presented as a **Bearer** token. The token is resolved once from the secret
store (or preset in spammer mode) and reused for the life of the client — same
posture as the Brex/Notion/Jira clients. No token refresh (Bearer archetype).

TODO(human): confirm Fireflies API host + read endpoints/scopes. The host
defaults via the endpoint resolver (`endpoint("fireflies_api")`) and is
overridable per-install (`base_url`) and per-env (`FIREFLIES_API_BASE_URL`); the
read surface below (`/transcripts`, `/transcript/{id}`) is CLONED from Brex and
UNVERIFIED for Fireflies — Fireflies' real API is a GraphQL endpoint
(`https://api.fireflies.ai/graphql`) exposing a `transcripts` query and a
`transcript(id:)` query, NOT REST paths. If the GraphQL surface is confirmed,
swap `_request` for a single POST to `/graphql` with a query+variables body and
adapt `list_transcripts` / `get_transcript` to read `data.transcripts` /
`data.transcript`. Implement only the verified read surface.

TODO(human): confirm Fireflies rate-limit signalling. Defaults to 429 +
`Retry-After` (Brex's scheme); tune via `FIREFLIES_RL_MAX_ATTEMPTS` /
`FIREFLIES_RL_MAX_SLEEP_SEC`. Fireflies may instead signal via a GraphQL error
extension (`code: "too_many_requests"`).

Pagination: `list_transcripts` returns `(items, next_offset, total)`,
`next_offset is None` terminal — offset/limit, CLONED from Brex and UNVERIFIED
for Fireflies (see the fetcher's pagination TODO; the GraphQL `transcripts`
query is `skip`/`limit` based, which maps cleanly onto offset/limit).

Logging redaction: the API token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import FirefliesApiError


log = structlog.get_logger("integrations.fireflies.client")


_DEFAULT_TIMEOUT_S = 30.0
# Default to 50 to bound payload size (transcripts are large) and keep parity
# with the other sources. The 500 page cap is CLONED from Brex and UNVERIFIED.
_DEFAULT_PAGE_SIZE = 50

# The REAL Fireflies read query (POST /graphql) — finding #5. `transcripts` is
# skip/limit paginated and `fromDate` bounds incremental polls. Field set mirrors
# what the handler/fetcher consume from the REST shape so the parse is uniform.
_TRANSCRIPTS_QUERY = """
query Transcripts($limit: Int, $skip: Int, $fromDate: DateTime) {
  transcripts(limit: $limit, skip: $skip, fromDate: $fromDate) {
    id
    title
    date
    duration
    organizer_email
    participants
    transcript_url
  }
}
""".strip()


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class FirefliesClient:
    """Outbound Fireflies client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_fireflies_client`
    (production / spammer) and by the seed/onboarding workspace probe. Shares the
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
        # In production the base is the canonical Fireflies API host; a
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
                raise FirefliesApiError(
                    "fireflies client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="fireflies_api_unauthorized",
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
        """One Fireflies API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `FirefliesApiError`.
        """
        from services.ingest.integrations.fireflies import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("FIREFLIES_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("FIREFLIES_RL_MAX_SLEEP_SEC", "30"))
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
                raise FirefliesApiError(
                    "transport error calling fireflies",
                    code="fireflies_api_error",
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
                    raise FirefliesApiError(
                        "fireflies response was not a JSON object",
                        code="fireflies_api_error",
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

    async def get_workspace(self) -> dict[str, Any]:
        """`GET /workspace` — the workspace the token is scoped to.

        Used at seed/install time to resolve the `workspace_id` the planner
        shards on (a Fireflies token is workspace-scoped). UNVERIFIED — the
        GraphQL surface exposes this via the `user`/`team` query.
        """
        return await self._request("GET", "/workspace")

    async def get_transcript(self, transcript_id: str) -> dict[str, Any]:
        """`GET /transcript/{id}` — one full transcript body (probe / hydrate)."""
        return await self._request("GET", f"/transcript/{transcript_id}")

    async def list_transcripts(
        self,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /transcripts` — paginated meeting transcripts, newest-first.

        `start` (ISO date) optionally bounds the window for incremental polls.
        Returns `(transcripts, next_offset, total)`; `next_offset is None`
        signals no more pages.
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if start:
            params["start"] = start
        resp = await self._request("GET", "/transcripts", params=params)
        items = resp.get("transcripts")
        items = [t for t in items if isinstance(t, dict)] if isinstance(items, list) else []
        total = int(resp.get("total", len(items)) or 0)
        next_offset = offset + len(items)
        is_last = next_offset >= total or not items
        return items, (None if is_last else next_offset), total

    # -----------------------------------------------------------------
    # GraphQL read surface (the REAL Fireflies API — finding #5)
    # -----------------------------------------------------------------
    # api.fireflies.ai is a single GraphQL endpoint (POST /graphql) exposing a
    # `transcripts(limit, skip, fromDate)` query, NOT REST paths. The REST
    # surface above is the synthetic/mock shape; the methods below speak the real
    # GraphQL protocol and are what production backfill uses when the install's
    # endpoint is GraphQL. Additive: the REST path is preserved for the mock.

    async def _graphql(
        self, query: str, variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """POST a GraphQL `{query, variables}` to `/graphql` and return `data`.

        Raises FirefliesApiError on a transport error, a non-2xx, or a GraphQL
        `errors` array (the real API returns 200 with an `errors` extension for
        rate limits — `code: too_many_requests`)."""
        from services.ingest.integrations.fireflies import metrics

        auth = await self._auth_header()
        # The GraphQL endpoint is the base when it already ends in /graphql,
        # else base + /graphql.
        base = self._api_base_url
        url = base if base.endswith("/graphql") else f"{base}/graphql"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables

        max_attempts = int(os.environ.get("FIREFLIES_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("FIREFLIES_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.post(url, headers=headers, json=payload)
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise FirefliesApiError(
                    "transport error calling fireflies graphql",
                    code="fireflies_api_error",
                    context={"error_type": type(exc).__name__},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                metrics.record_request("rate_limited")
                await asyncio.sleep(
                    min(max_sleep, _parse_retry_after(response.headers.get("Retry-After")))
                )
                continue
            if response.status_code // 100 != 2:
                if response.status_code in (401, 403):
                    metrics.record_request("unauthorized")
                else:
                    metrics.record_request("error")
                raise _api_error_from_response(response, "/graphql")

            body = _safe_json(response)
            if not isinstance(body, dict):
                raise FirefliesApiError(
                    "fireflies graphql response was not a JSON object",
                    code="fireflies_api_error", context={"path": "/graphql"},
                )
            errors = body.get("errors")
            if errors:
                # Rate-limit shows up as a 200 with errors[].extensions.code.
                code = "fireflies_api_error"
                if any(
                    isinstance(e, dict)
                    and isinstance(e.get("extensions"), dict)
                    and e["extensions"].get("code") == "too_many_requests"
                    for e in errors if isinstance(e, dict)
                ):
                    code = "fireflies_api_rate_limited"
                    metrics.record_request("rate_limited")
                else:
                    metrics.record_request("error")
                raise FirefliesApiError(
                    "fireflies graphql returned errors",
                    code=code, context={"errors": str(errors)[:300]},
                )
            metrics.record_request("ok")
            data = body.get("data")
            return data if isinstance(data, dict) else {}

    async def list_transcripts_graphql(
        self,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        skip: int = 0,
        from_date: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """The REAL Fireflies read: `transcripts(limit, skip, fromDate)` GraphQL
        query → `data.transcripts`. GraphQL has no `total`, so a full page means
        "maybe more" (next_skip = skip+limit) and a short page is terminal
        (next_skip None)."""
        variables = {"limit": limit, "skip": skip}
        if from_date:
            variables["fromDate"] = from_date
        data = await self._graphql(_TRANSCRIPTS_QUERY, variables)
        items = data.get("transcripts")
        items = [t for t in items if isinstance(t, dict)] if isinstance(items, list) else []
        next_skip = skip + limit if len(items) >= limit and items else None
        return items, next_skip


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
) -> FirefliesApiError:
    """Map a non-2xx Fireflies response to a typed `FirefliesApiError`."""
    status = response.status_code
    if status in (401, 403):
        return FirefliesApiError(
            f"fireflies {status}: API token rejected or insufficient scope",
            code="fireflies_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return FirefliesApiError(
            "fireflies 404: transcript/resource not found or not visible to the token",
            code="fireflies_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return FirefliesApiError(
            "fireflies rate limit (429), retry budget exhausted",
            code="fireflies_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return FirefliesApiError(
        f"fireflies returned {status}",
        code="fireflies_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["FirefliesClient"]
