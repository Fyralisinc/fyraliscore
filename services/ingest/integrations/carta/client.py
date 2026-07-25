"""services/ingest/integrations/carta/client.py — outbound Carta client.

Single outbound surface for backfill + poll-incremental. Carta's API Platform is
an **issuer** cap-table REST suite under ``/v1alpha1`` (alpha — expect breaking
changes), authenticated with an OAuth 2.0 Bearer **access token** (~1 h; no
refresh grant — re-minted via client_credentials, see
`integrations/oauth_refresh.py`). The access token is resolved through a
short-lived secret-ref cache (or preset in Provider Lab mode), so rotation is picked
up without process restart. A reactive 401 re-mint retries the request once
with a fresh token.

CONFIRMED contract (OpenAPI "Issuer v1alpha1" embedded in
docs.carta.com/api-platform/reference, e.g.
https://docs.carta.com/api-platform/reference/v1alpha1issuersliststakeholders):

  - Read surface: ``GET /v1alpha1/issuers`` (issuers visible to the token;
    pageSize default 25, max 50), ``GET /v1alpha1/issuers/{id}``, and per
    issuer the entity collections ``stakeholders`` (pageSize max 100),
    ``shareClasses``, ``optionGrants``, ``convertibleNotes`` (max 50 each).
  - Pagination: Google AIP-158 — request ``pageSize`` + opaque ``pageToken``;
    the response carries ``nextPageToken`` keyed beside the collection
    (``{"optionGrants": [...], "nextPageToken": "..."}``). An absent/empty
    ``nextPageToken`` means there are no subsequent pages.
  - Server-side delta: ONLY ``optionGrants`` supports
    ``lastModifiedDatetimeAfter`` / ``lastModifiedDatetimeBefore`` (ISO 8601
    UTC, "on or after" / "on or before" semantics). The other collections have
    no modified-since filter — incremental sync is a full idempotent re-walk.
  - Money/decimals arrive as protobuf wrapper objects:
    ``{"value": "<decimal string>"}`` (v1alpha1Decimal) and
    ``{"currencyCode": {"value": "USD"}, "amount": {"value": "1.25"}}``
    (v1alpha1Money); dates/datetimes are ``{"value": "2021-01-01[T..Z]"}``.
    Decoding happens in the handler.
  - Hosts (OAS ``servers``): production ``https://api.carta.com``, mock
    ``https://mock-api.carta.com``, playground
    ``https://api.playground.carta.team``. ACCESS IS PARTNER-GATED — see
    `lib/integrations/endpoints.py` (``carta_api``).

Rate limits, retry budgets, timeouts, and distributed cooldowns are owned by
``ProviderTransport``. Non-retryable HTTP outcomes map to ``CartaApiError``.

Logging redaction: the access token / auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID
from urllib.parse import quote

import httpx
import structlog

from lib.shared.errors import CartaApiError
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


log = structlog.get_logger("integrations.carta.client")


_DEFAULT_TIMEOUT_S = 30.0
# Server default is 25; the documented maximum is 50 (100 for stakeholders) —
# values above the cap are coerced server-side, so 50 is the safe common page.
_DEFAULT_PAGE_SIZE = 50


# entity_type (shard taxonomy) -> the /v1alpha1 collection segment, which is
# ALSO the response-envelope key ({"<collection>": [...], "nextPageToken"}).
ENTITY_COLLECTIONS: dict[str, str] = {
    "stakeholder": "stakeholders",
    "shareClass": "shareClasses",
    "optionGrant": "optionGrants",
    "convertibleNote": "convertibleNotes",
}

# The cap-table entity kinds we shard on (one shard per (issuer, entity_type)).
# Carta is cap-table-shaped (NOT transactional like Ramp), so the entity_kind
# discriminates the external_id.
DEFAULT_ENTITIES = tuple(ENTITY_COLLECTIONS)


class CartaClient:
    """Outbound Carta client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_carta_client`.
    ``issuer_id`` is the per-install scope id (stored in
    ``carta_installations.firm_id``); ``list_issuers``/``probe`` work without it.
    """

    def __init__(
        self,
        *,
        base_url: str,
        issuer_id: str | None = None,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        access_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        install_row_id: Any | None = None,
        refresh_secret_ref: str | None = None,
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
        self._issuer_id = issuer_id
        # Reactive token RE-MINT on 401. Carta has no refresh grant —
        # refresh_secret_ref holds the client_credentials secret used to re-mint.
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._access_token_cache = SecretValueCache(preset=access_token)
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical Carta host; a lab/test
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
            source="carta",
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            installation_id=(
                str(install_row_id) if install_row_id is not None else None
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
        return await self._access_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: CartaApiError(
                "carta client has no access token and cannot resolve "
                "one (missing secret_store / secret_ref / tenant_id)",
                code="carta_api_unauthorized",
            )
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> dict[str, Any]:
        from services.ingest.integrations.carta import metrics
        from services.ingest.integrations.oauth_refresh import (
            refresh_on_unauthorized,
        )

        url = f"{self._api_base_url}{path}"
        token = await self._token()
        response = await self._execute_http(
            method=method,
            url=url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            params=params,
            operation=operation,
        )

        if response.status_code in (401, 403):
            new_token = await refresh_on_unauthorized(
                provider="carta",
                pool=self._pool,
                secret_store=self._secret_store,
                # The shared refresh core owns the exact
                # ``oauth.token.mint`` ProviderTransport execution. Passing
                # Carta's raw client here avoids charging the same upstream
                # request twice.
                http=self._httpx(),
                tenant_id=self._tenant_id,
                install_row_id=self._install_row_id,
                current_access_ref=self._secret_ref,
                refresh_secret_ref=self._refresh_secret_ref,
                request_binding=self._provider,
            )
            if new_token is not None:
                self._access_token_cache.set(new_token)
                response = await self._execute_http(
                    method=method,
                    url=url,
                    headers={
                        "Authorization": f"Bearer {new_token}",
                        "Accept": "application/json",
                    },
                    params=params,
                    operation=operation,
                )

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

    async def _execute_http(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        operation: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> httpx.Response:
        from services.ingest.integrations.carta import metrics

        client = self._httpx()

        async def _once() -> httpx.Response:
            try:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    data=data,
                )
            except httpx.TimeoutException as exc:
                metrics.record_request("error")
                raise ProviderTimeoutError(
                    "Carta request timed out",
                    source="carta",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise ProviderTransientError(
                    "Carta transport error",
                    source="carta",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc

            if response.status_code == 429:
                metrics.record_request("rate_limited")
                raise ProviderRateLimited(
                    "Carta rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="carta",
                    operation=operation,
                )
            if response.status_code >= 500:
                metrics.record_request("error")
                raise ProviderTransientError(
                    f"Carta returned HTTP {response.status_code}",
                    source="carta",
                    operation=operation,
                    http_status=response.status_code,
                )
            return response

        return await self._provider.execute(operation, _once)

    def _require_issuer(self) -> str:
        if not self._issuer_id:
            raise CartaApiError(
                "carta client has no issuer_id (required for per-issuer "
                "entity listing)",
                code="carta_api_error",
            )
        return self._issuer_id

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_issuers(
        self,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /v1alpha1/issuers` — issuers visible to the token.

        Returns `(issuers, next_page_token)`; `next_page_token is None` is
        terminal (the response omits `nextPageToken` on the last page).
        """
        params: dict[str, Any] = {}
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token:
            params["pageToken"] = page_token
        body = await self._request(
            "GET",
            "/v1alpha1/issuers",
            params=params,
            operation="issuers.list",
        )
        return _decode_page(body, "issuers")

    async def get_issuer(self, issuer_id: str | None = None) -> dict[str, Any]:
        """`GET /v1alpha1/issuers/{id}` — one issuer (visibility check).

        Returns the issuer object from the `{"issuer": {...}}` envelope.
        """
        target = issuer_id or self._require_issuer()
        body = await self._request(
            "GET",
            f"/v1alpha1/issuers/{quote(target, safe='')}",
            operation="issuers.get",
        )
        issuer = body.get("issuer")
        return issuer if isinstance(issuer, dict) else {}

    async def probe(self) -> dict[str, Any]:
        """Cheap connectivity/auth probe: `GET /v1alpha1/issuers?pageSize=1`."""
        return await self._request(
            "GET",
            "/v1alpha1/issuers",
            params={"pageSize": 1},
            operation="issuers.list",
        )

    async def list_entity(
        self,
        entity_type: str,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
        modified_after: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One page of `GET /v1alpha1/issuers/{issuer}/{collection}` rows.

        `entity_type` ∈ DEFAULT_ENTITIES ("stakeholder" / "shareClass" /
        "optionGrant" / "convertibleNote"). Returns `(rows, next_page_token)`;
        `next_page_token is None` is terminal.

        `modified_after` maps to `lastModifiedDatetimeAfter` (ISO 8601 UTC,
        inclusive "on or after") and is ONLY supported by optionGrants — passing
        it for any other entity_type raises ValueError (the other collections
        have no server-side delta filter).
        """
        collection = ENTITY_COLLECTIONS.get(entity_type)
        if collection is None:
            raise ValueError(f"unknown carta entity_type {entity_type!r}")
        if modified_after is not None and entity_type != "optionGrant":
            raise ValueError(
                "lastModifiedDatetimeAfter is only supported for optionGrant",
            )
        issuer = self._require_issuer()
        params: dict[str, Any] = {"pageSize": page_size}
        if page_token:
            params["pageToken"] = page_token
        if modified_after is not None:
            params["lastModifiedDatetimeAfter"] = modified_after
        path = f"/v1alpha1/issuers/{quote(issuer, safe='')}/{collection}"
        body = await self._request(
            "GET",
            path,
            params=params,
            operation={
                "stakeholder": "stakeholders.list",
                "shareClass": "share_classes.list",
                "optionGrant": "option_grants.list",
                "convertibleNote": "convertible_notes.list",
            }[entity_type],
        )
        return _decode_page(body, collection)

    async def list_stakeholders(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "stakeholder", page_size=page_size, page_token=page_token,
        )

    async def list_share_classes(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "shareClass", page_size=page_size, page_token=page_token,
        )

    async def list_option_grants(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None, modified_after: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "optionGrant", page_size=page_size, page_token=page_token,
            modified_after=modified_after,
        )

    async def list_convertible_notes(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "convertibleNote", page_size=page_size, page_token=page_token,
        )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _decode_page(
    body: dict[str, Any], collection: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Decode an AIP-158 list envelope: `{"<collection>": [...],
    "nextPageToken": "..."}`. An absent/empty nextPageToken is terminal."""
    rows = body.get(collection)
    rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
    token = body.get("nextPageToken")
    next_token = token if isinstance(token, str) and token else None
    return rows, next_token


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
            "(may need re-mint)",
            code="carta_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return CartaApiError(
            "carta 404: issuer/collection not found or not visible",
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


__all__ = [
    "CartaClient",
    "CartaApiError",
    "DEFAULT_ENTITIES",
    "ENTITY_COLLECTIONS",
]
