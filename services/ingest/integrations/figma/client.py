"""services/ingest/integrations/figma/client.py — outbound Figma REST client.

Single outbound surface for backfill + poll-incremental + the planner's file
enumeration. Figma is authenticated with a long-lived org/team access token
presented as a **Bearer** token. The token is resolved once from the secret store
(or preset in spammer mode) and reused for the life of the client — same posture
as the Brex/Jira clients. No token refresh (Bearer archetype; OAuth refresh is
out of v1 scope).

TODO(human): confirm Figma API host + read endpoints/scopes. The host defaults
via the endpoint resolver (`endpoint("figma_api")` -> https://api.figma.com) and
is overridable per-install (`base_url`) and per-env (`FIGMA_API_BASE_URL`); the
read surface below (`/v1/teams/{id}/files`, `/v1/files/{key}`,
`/v1/files/{key}/events`) is CLONED from Brex's account/transaction shape and is
UNVERIFIED for Figma — Figma's real reads are `GET /v1/files/:key` (whole tree),
`GET /v1/files/:key/versions`, `GET /v1/files/:key/comments`,
`GET /v1/teams/:id/projects` + `GET /v1/projects/:id/files`. There is NO single
`/events` list endpoint in the real API — backfill must derive "events" from
versions + comments. The required OAuth scopes (`file_content:read`,
`file_versions:read`, `file_comments:read`, `projects:read`) must be confirmed
and the read methods adjusted. Implement only the verified read surface.

TODO(human): confirm Figma rate-limit signalling. Defaults to 429 +
`Retry-After` (Brex's scheme); tune via `FIGMA_RL_MAX_ATTEMPTS` /
`FIGMA_RL_MAX_SLEEP_SEC`. Figma uses a leaky-bucket scheme with three endpoint
tiers; Tier-1 file reads are ~10-20/min (Dev/Full seat) and as low as ~6/MONTH
on View/Collab seats — the token identity MUST be Dev/Full-seat.

Pagination: `list_events` returns `(items, next_offset, total)`, `next_offset is
None` terminal — offset/limit, CLONED from Brex and UNVERIFIED for Figma (the
real file-list endpoints' pagination shape — cursor vs full list — is unverified;
see the fetcher's pagination TODO).

Logging redaction: the access token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import CompanyOSError


log = structlog.get_logger("integrations.figma.client")


_DEFAULT_TIMEOUT_S = 30.0
# Default to 100 to bound payload size and keep parity with the other sources.
# The 500 page cap is CLONED from Brex and UNVERIFIED for Figma.
_DEFAULT_PAGE_SIZE = 100


class FigmaApiError(CompanyOSError):
    """Outbound Figma REST call failure (design source — Bearer/Brex archetype).

    Figma uses a long-lived org/team access token (`Authorization: Bearer
    {token}`, no refresh in v1). Mirrors BrexApiError.

    TODO(human): promote this to `lib/shared/errors.py` alongside BrexApiError /
    RampApiError (the shared-file / wiring agent owns that edit — see the source
    summary `notes`). It is defined locally here so this vertical compiles and
    runs standalone without touching the shared errors module.

    Stable `code` values:
      - figma_api_unauthorized: 401/403 — token rejected / insufficient scope
      - figma_api_not_found: 404 — file/resource not visible to the token
      - figma_api_rate_limited: 429 with retry budget exhausted
      - figma_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access token is
    NEVER placed on context.
    """
    default_code = "figma_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class FigmaClient:
    """Outbound Figma REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_figma_client`
    (production / spammer) and by the seed/onboarding file probe. Shares the
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
        # In production the base is the canonical Figma API host; a
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
                raise FigmaApiError(
                    "figma client has no api token and cannot resolve one "
                    "(missing secret_store / secret_ref / tenant_id)",
                    code="figma_api_unauthorized",
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
        # TODO(human): confirm Figma's bearer header. The REST API historically
        # also accepted `X-Figma-Token: {token}` for personal access tokens;
        # OAuth uses `Authorization: Bearer {token}`. We send Bearer (Brex parity).
        return f"Bearer {token}"

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One Figma API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `FigmaApiError`.
        """
        from services.ingest.integrations.figma import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("FIGMA_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("FIGMA_RL_MAX_SLEEP_SEC", "30"))
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
                raise FigmaApiError(
                    "transport error calling figma",
                    code="figma_api_error",
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
                    raise FigmaApiError(
                        "figma response was not a JSON object",
                        code="figma_api_error",
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

    async def list_files(self) -> list[dict[str, Any]]:
        """`GET /v1/teams/{id}/files` — all files visible to the token.

        Used at seed/install time to populate `figma_files`, and as the planner's
        shard list. (Real Figma enumerates teams→projects→files; this single-call
        shape is CLONED from Brex `list_accounts` and UNVERIFIED.)
        """
        resp = await self._request("GET", "/v1/files")
        files = resp.get("files")
        if not isinstance(files, list):
            # Some responses may return the bare list.
            files = resp if isinstance(resp, list) else []  # type: ignore[assignment]
        return [f for f in files if isinstance(f, dict)]

    async def get_file(self, file_key: str) -> dict[str, Any]:
        """`GET /v1/files/{key}/meta` — one file's lightweight metadata."""
        return await self._request("GET", f"/v1/files/{file_key}/meta")

    async def list_events(
        self,
        file_key: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /v1/files/{key}/events` — paginated file events/versions.

        `start` (ISO date) optionally bounds the window for incremental polls.
        Returns `(events, next_offset, total)`; `next_offset is None` signals no
        more pages. (Real Figma derives "events" from /versions + /comments;
        this single-stream shape is CLONED from Brex `list_transactions`.)
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if start:
            params["start"] = start
        resp = await self._request(
            "GET", f"/v1/files/{file_key}/events", params=params,
        )
        events = resp.get("events")
        events = [e for e in events if isinstance(e, dict)] if isinstance(events, list) else []
        total = int(resp.get("total", len(events)) or 0)
        next_offset = offset + len(events)
        is_last = next_offset >= total or not events
        return events, (None if is_last else next_offset), total


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
) -> FigmaApiError:
    """Map a non-2xx Figma response to a typed `FigmaApiError`."""
    status = response.status_code
    if status in (401, 403):
        return FigmaApiError(
            f"figma {status}: access token rejected or insufficient scope",
            code="figma_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return FigmaApiError(
            "figma 404: file/resource not found or not visible to the token",
            code="figma_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return FigmaApiError(
            "figma rate limit (429), retry budget exhausted",
            code="figma_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return FigmaApiError(
        f"figma returned {status}",
        code="figma_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["FigmaClient", "FigmaApiError"]
