"""services/ingest/integrations/grafana/client.py — outbound Grafana HTTP API client.

Single outbound surface for the annotations backfill + poll-incremental + the
reconciler's gap probe. Grafana is authenticated with a SERVICE-ACCOUNT TOKEN
presented as a **Bearer** token (`Authorization: Bearer glsa_...`); API keys were
deprecated in 2025. The token is long-lived: resolved once from the secret store
(or preset in spammer mode) and reused for the life of the client — same posture
as the Mercury/Jira/Notion clients.

Rate limits: Grafana returns HTTP 429 with a `Retry-After` header under load. All
read methods route through `_request`, which honours `Retry-After` with a bounded
retry budget before surfacing `GrafanaApiError(grafana_api_rate_limited)`.

Annotations API shape (load-bearing): `GET /api/annotations` returns a bare JSON
**array** (not an object), filterable by `from` / `to` (epoch MILLISECONDS) and
`limit` (default 100). Each element carries `id, alertId, dashboardUID, panelId,
userId, userName, newState, prevState, time, timeEnd, text, tags, data`. The
backfill walks the window newest-first in pages of `limit`, advancing the upper
bound backward until a short page signals the floor.

Logging redaction: the service-account token and the Authorization header are
NEVER logged. The base URL host is hashed before it touches a log line.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import GrafanaApiError


log = structlog.get_logger("integrations.grafana.client")


_DEFAULT_TIMEOUT_S = 30.0
# Grafana's annotations endpoint defaults `limit` to 100; keep parity with the
# other sources and bound payload size.
_DEFAULT_PAGE_SIZE = 100


def short_host_hash(base_url: str) -> str:
    """Non-reversible 16-hex digest of the instance host for logs."""
    return hashlib.blake2b(base_url.encode("utf-8"), digest_size=8).hexdigest()


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class GrafanaClient:
    """Outbound Grafana HTTP API client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_grafana_client`
    (production / spammer) and by the seed/onboarding org probe. Shares the
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
        # In production the base is the per-install instance URL; a spammer/test
        # override (api_base_url) wins so backfill points at the mock.
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
                raise GrafanaApiError(
                    "grafana client has no service-account token and cannot "
                    "resolve one (missing secret_store / secret_ref / tenant_id)",
                    code="grafana_api_unauthorized",
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
    ) -> Any:
        """One Grafana API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON (object OR array — the annotations endpoint
        returns a bare array). Non-2xx (including a still-429 after the budget
        is spent) is mapped to `GrafanaApiError`.
        """
        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("GRAFANA_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("GRAFANA_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.request(
                    method, url, headers=headers, params=params,
                )
            except httpx.TransportError as exc:
                raise GrafanaApiError(
                    "transport error calling grafana",
                    code="grafana_api_error",
                    context={"error_type": type(exc).__name__, "path": path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                body = _safe_json(response)
                if body is None:
                    raise GrafanaApiError(
                        "grafana response was not valid JSON",
                        code="grafana_api_error",
                        context={"path": path},
                    )
                return body

            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_annotations(
        self,
        *,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = _DEFAULT_PAGE_SIZE,
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """`GET /api/annotations` — annotations in the [from_ms, to_ms] window.

        `from_ms` / `to_ms` are epoch MILLISECONDS (Grafana's unit); both
        optional. `type_filter` ∈ {None, "alert", "annotation"} — None returns
        BOTH user annotations and Grafana's auto-created alert-state-change
        annotations. Returns the (possibly empty) list of annotation objects,
        newest-first.
        """
        params: dict[str, Any] = {"limit": limit}
        if from_ms is not None:
            params["from"] = int(from_ms)
        if to_ms is not None:
            params["to"] = int(to_ms)
        if type_filter:
            params["type"] = type_filter
        resp = await self._request("GET", "/api/annotations", params=params)
        if not isinstance(resp, list):
            # Defensive: some proxies wrap the array under a key.
            resp = resp.get("annotations") if isinstance(resp, dict) else []
        return [a for a in resp if isinstance(a, dict)] if isinstance(resp, list) else []

    async def has_annotations_since(self, *, from_ms: int) -> bool:
        """Cheap reconciler gap probe: is there ≥1 annotation with `time` at/after
        `from_ms` (epoch ms)? The caller passes an EXCLUSIVE floor (high-water + 1
        ms) so the high-water annotation itself does not re-match."""
        rows = await self.list_annotations(from_ms=from_ms, limit=1)
        return len(rows) > 0

    async def get_org(self) -> dict[str, Any]:
        """`GET /api/org` — the org the service-account token is scoped to; a
        cheap connectivity + credential probe used by the seed script."""
        resp = await self._request("GET", "/api/org")
        return resp if isinstance(resp, dict) else {}


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
) -> GrafanaApiError:
    """Map a non-2xx Grafana response to a typed `GrafanaApiError`."""
    status = response.status_code
    if status in (401, 403):
        return GrafanaApiError(
            f"grafana {status}: service-account token rejected or insufficient "
            f"role (needs annotations:read)",
            code="grafana_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return GrafanaApiError(
            "grafana 404: endpoint/org not found or not visible to the token",
            code="grafana_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return GrafanaApiError(
            "grafana rate limit (429), retry budget exhausted",
            code="grafana_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return GrafanaApiError(
        f"grafana returned {status}",
        code="grafana_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["GrafanaClient", "short_host_hash"]
