"""services/ingest/integrations/grafana/client.py — outbound Grafana HTTP API client.

Single outbound surface for the annotations backfill + poll-incremental + the
reconciler's gap probe. Grafana is authenticated with a SERVICE-ACCOUNT TOKEN
presented as a **Bearer** token (`Authorization: Bearer glsa_...`); API keys were
deprecated in 2025. The token is long-lived: resolved once from the secret store
(or preset in Provider Lab mode) and reused for the life of the client — same posture
as the Mercury/Jira/Notion clients.

Every outbound attempt runs through ``ProviderTransport``. Grafana 429s,
timeouts, transport failures, and 5xx responses become typed transport
outcomes; durable cooldowns escape as ``RetryLater`` rather than sleeping in
this client.

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
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import GrafanaApiError
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


log = structlog.get_logger("integrations.grafana.client")


_DEFAULT_TIMEOUT_S = 30.0
# Grafana's annotations endpoint defaults `limit` to 100; keep parity with the
# other sources and bound payload size.
_DEFAULT_PAGE_SIZE = 100


def short_host_hash(base_url: str) -> str:
    """Non-reversible 16-hex digest of the instance host for logs."""
    return hashlib.blake2b(base_url.encode("utf-8"), digest_size=8).hexdigest()


class GrafanaClient:
    """Outbound Grafana HTTP API client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_grafana_client`
    (production / Provider Lab) and by the seed/onboarding org probe. Shares the
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
        # Preset token (Provider Lab mode supplies a recognized token); otherwise
        # resolved lazily from the secret store on first request.
        self._api_token_cache = SecretValueCache(preset=api_token)
        self._token_lock = asyncio.Lock()
        # In production the base is the per-install instance URL; a lab/test
        # override (api_base_url) wins so backfill points at the mock.
        self._api_base_url = (api_base_url or base_url).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                http_client is not None or api_base_url is not None
            ),
        )
        self._provider = ProviderRequestBinding(
            source="grafana",
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
        return await self._api_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: GrafanaApiError(
                "grafana client has no service-account token and cannot "
                "resolve one (missing secret_store / secret_ref / tenant_id)",
                code="grafana_api_unauthorized",
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
        operation: str,
    ) -> Any:
        """Execute one semantic Grafana operation through ProviderTransport."""
        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        client = self._httpx()

        async def _once() -> httpx.Response:
            try:
                response = await client.request(
                    method, url, headers=headers, params=params,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Grafana request timed out",
                    source="grafana",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Grafana transport error",
                    source="grafana",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc

            if response.status_code == 429:
                raise ProviderRateLimited(
                    "Grafana rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="grafana",
                    operation=operation,
                )
            if response.status_code >= 500:
                raise ProviderTransientError(
                    f"Grafana returned HTTP {response.status_code}",
                    source="grafana",
                    operation=operation,
                    http_status=response.status_code,
                )
            return response

        response = await self._provider.execute(operation, _once)
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
        resp = await self._request(
            "GET",
            "/api/annotations",
            params=params,
            operation="annotations.list",
        )
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
        resp = await self._request(
            "GET",
            "/api/org",
            operation="org.get",
        )
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
