"""services/ingest/integrations/carta/client.py — outbound Carta client.

Single outbound surface for backfill + poll-incremental. Carta is authenticated
with an OAuth 2.0 Bearer **access token** (short-lived) and every call is scoped
to a firm ``firm_id`` (the scope-id, analogous to Gusto's ``company_uuid`` /
QuickBooks' ``realmId``). The access token is resolved once from the secret store
(or preset in spammer mode) and reused for the life of the client.

TODO(human): implement Carta OAuth token refresh — NONE exists yet (this is the
    documented-but-unbuilt seam, exactly as the Gusto / QuickBooks archetype
    ships). The install row persists `refresh_secret_ref` + `token_expires_at`;
    wire either a refresh-on-401 exchange here (exchange refresh token -> persist
    rotated token -> retry once) OR an oauth_poller. Do NOT assume tokens never
    expire.

TODO(human): confirm Carta API host + read endpoints + OAuth scopes. The host is
    intended to be set in `lib/integrations/endpoints.py` (`carta_api`) and is
    overridable per env (`CARTA_API_BASE_URL`) and per install (`base_url`). The
    read surface below clones the Gusto/QuickBooks query endpoint as a
    placeholder; Carta's real read surface is REST collections under
    `/v1/firms/{firm_id}/...` (shareholders, share_classes, safes, option_grants).
    Implement only the verified read surface and tag speculative endpoints.

Rate limits: default to 429 + Retry-After (env knobs CARTA_RL_MAX_ATTEMPTS /
CARTA_RL_MAX_SLEEP_SEC). Non-2xx maps to ``CartaApiError``.

Logging redaction: the access token / auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID
from urllib.parse import quote

import httpx
import structlog

# TODO(human): add a canonical `CartaApiError(CompanyOSError)` to
#   `lib/shared/errors.py` (mirroring `GustoApiError`) during the wiring phase
#   and import it here. `lib/shared/errors.py` is a SHARED file this phase must
#   not edit, so until then a local subclass keeps the pipeline self-contained
#   with the same stable `code` contract.
try:  # pragma: no cover - prefer the canonical error once wired
    from lib.shared.errors import CartaApiError  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    from lib.shared.errors import CompanyOSError

    class CartaApiError(CompanyOSError):  # type: ignore[no-redef]
        """Outbound Carta REST call failure (cap-table source — OAuth/Gusto
        archetype). Stable `code` values mirror Gusto:
          - carta_api_unauthorized / carta_api_not_found /
            carta_api_rate_limited / carta_api_error.
        The access/refresh tokens are NEVER placed on context.
        """

        default_code = "carta_api_error"

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


log = structlog.get_logger("integrations.carta.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MINOR_VERSION = "1"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class CartaClient:
    """Outbound Carta client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_carta_client`
    (added during the wiring phase).
    """

    def __init__(
        self,
        *,
        base_url: str,
        firm_id: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        access_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._firm_id = firm_id
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical CARTA host; a spammer/test
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
        if self._access_token is not None:
            return self._access_token
        async with self._token_lock:
            if self._access_token is not None:
                return self._access_token
            if (
                self._secret_store is None
                or self._secret_ref is None
                or self._tenant_id is None
            ):
                raise CartaApiError(
                    "carta client has no access token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="carta_api_unauthorized",
                )
            raw = await self._secret_store.get(
                self._secret_ref, tenant_id=self._tenant_id,
            )
            self._access_token = (
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
            return self._access_token

    async def _request(
        self, method: str, path: str, *, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from services.ingest.integrations.carta import metrics

        token = await self._token()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("CARTA_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("CARTA_RL_MAX_SLEEP_SEC", "30"))
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
                raise CartaApiError(
                    "transport error calling carta",
                    code="carta_api_error",
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
                    raise CartaApiError(
                        "carta response was not a JSON object",
                        code="carta_api_error",
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

    async def query(
        self,
        entity: str,
        *,
        where: str | None = None,
        order_by: str = "Metadata.LastUpdatedTime",
        start_position: int = 1,
        max_results: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Run a SELECT against one entity. Returns `(rows, next_start_position)`;
        `next_start_position is None` is terminal.

        TODO(human): confirm Carta's real list/pagination shape. This clones the
        Gusto/QuickBooks query-language placeholder (`SELECT * FROM <Entity>
        [WHERE ...] ORDERBY <f> STARTPOSITION n MAXRESULTS m`). Carta's real REST
        surface is page/cursor-based collections under `/v1/firms/{firm_id}/...`;
        replace `client.query(...)` + the WHERE filter with the verified shape.
        `Metadata.LastUpdatedTime` is the incremental cursor field placeholder.
        """
        sql = f"SELECT * FROM {entity}"
        if where:
            sql += f" WHERE {where}"
        sql += f" ORDERBY {order_by} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        path = f"/v1/firms/{self._firm_id}/query"
        params = {"query": sql, "minorversion": _MINOR_VERSION}
        resp = await self._request("GET", path, params=params)
        qr = resp.get("QueryResponse")
        if not isinstance(qr, dict):
            return [], None
        rows = qr.get(entity)
        rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        # CARTA returns maxResults == page length; a short page is terminal.
        returned = int(qr.get("maxResults", len(rows)) or 0)
        next_start = start_position + len(rows)
        is_last = returned < max_results or not rows
        return rows, (None if is_last else next_start)

    async def firm_info(self) -> dict[str, Any]:
        """`GET /v1/firms/{firm}/firminfo/{firm}` — connectivity probe."""
        path = f"/v1/firms/{self._firm_id}/firminfo/{quote(self._firm_id)}"
        return await self._request(
            "GET", path, params={"minorversion": _MINOR_VERSION},
        )


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
) -> CartaApiError:
    status = response.status_code
    if status in (401, 403):
        return CartaApiError(
            f"carta {status}: access token rejected or insufficient scope "
            "(may need refresh)",
            code="carta_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return CartaApiError(
            "carta 404: entity/firm not found or not visible",
            code="carta_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return CartaApiError(
            "carta rate limit (429), retry budget exhausted",
            code="carta_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return CartaApiError(
        f"carta returned {status}",
        code="carta_api_error",
        context={"http_status": status, "path": path},
    )


# The cap-table entity kinds we shard on. Carta is cap-table-shaped (NOT
# transactional like Ramp), so the entity_kind discriminates the external_id.
DEFAULT_ENTITIES = ("Shareholder", "ShareClass", "SafeNote", "OptionGrant")


__all__ = ["CartaClient", "CartaApiError", "DEFAULT_ENTITIES"]
