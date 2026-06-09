"""services/ingest/integrations/gusto/client.py — outbound Gusto client.

Single outbound surface for backfill + poll-incremental. Gusto is authenticated
with an OAuth 2.0 Bearer **access token** (short-lived) and every call is scoped
to a company ``company_uuid``. The access token is resolved once from the secret
store (or preset in spammer mode) and reused for the life of the client.

TODO(human): implement Gusto OAuth token refresh — NONE exists yet (this is the
    documented-but-unbuilt seam, exactly as the QuickBooks archetype ships). The
    install row persists `refresh_secret_ref` + `token_expires_at`; wire either a
    refresh-on-401 exchange here (exchange refresh token -> persist rotated token
    -> retry once) OR an oauth_poller. Do NOT assume tokens never expire.

TODO(human): confirm Gusto API host + read endpoints + OAuth scopes. The host is
    set in `lib/integrations/endpoints.py` (`gusto_api`) and is overridable per
    env (`GUSTO_API_BASE_URL`) and per install (`base_url`). The read surface
    below clones the QuickBooks query endpoint as a placeholder; Gusto's real
    read surface is REST collections under `/v1/companies/{company_uuid}/...`
    (payrolls, employees, contractor_payments). Implement only the verified read
    surface and tag speculative endpoints.

Rate limits: default to 429 + Retry-After (env knobs GUSTO_RL_MAX_ATTEMPTS /
GUSTO_RL_MAX_SLEEP_SEC). Non-2xx maps to ``GustoApiError``.

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

from lib.shared.errors import GustoApiError


log = structlog.get_logger("integrations.gusto.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MINOR_VERSION = "75"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class GustoClient:
    """Outbound Gusto client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_gusto_client`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        company_uuid: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        access_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        install_row_id: Any | None = None,
        refresh_secret_ref: str | None = None,
        token_expires_at: Any | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._company_uuid = company_uuid
        # Phase 3: proactive (expiry skew) + reactive (401) OAuth re-mint
        # (inert in spammer mode).
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._token_expires_at = token_expires_at
        self._proactive_checked = False
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical GUSTO host; a spammer/test
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
                raise GustoApiError(
                    "gusto client has no access token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="gusto_api_unauthorized",
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
        from services.ingest.integrations.gusto import metrics
        from services.ingest.integrations.oauth_refresh import (
            maybe_proactive_refresh,
            refresh_on_unauthorized,
        )

        url = f"{self._api_base_url}{path}"
        max_attempts = int(os.environ.get("GUSTO_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("GUSTO_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        if not self._proactive_checked:
            self._proactive_checked = True
            proactive = await maybe_proactive_refresh(
                provider="gusto", pool=self._pool,
                secret_store=self._secret_store, http=client,
                tenant_id=self._tenant_id, install_row_id=self._install_row_id,
                refresh_secret_ref=self._refresh_secret_ref,
                token_expires_at=self._token_expires_at,
            )
            if proactive is not None:
                self._access_token = proactive

        attempt = 0
        reminted = False
        while True:
            attempt += 1
            token = await self._token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            try:
                response = await client.request(
                    method, url, headers=headers, params=params,
                )
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise GustoApiError(
                    "transport error calling gusto",
                    code="gusto_api_error",
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
                    raise GustoApiError(
                        "gusto response was not a JSON object",
                        code="gusto_api_error",
                        context={"path": path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
                if not reminted:
                    reminted = True
                    new_token = await refresh_on_unauthorized(
                        provider="gusto", pool=self._pool,
                        secret_store=self._secret_store, http=client,
                        tenant_id=self._tenant_id,
                        install_row_id=self._install_row_id,
                        current_access_ref=self._secret_ref,
                        refresh_secret_ref=self._refresh_secret_ref,
                    )
                    if new_token is not None:
                        self._access_token = new_token
                        continue
                raise _api_error_from_response(response, path)
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

        GUSTO query language: `SELECT * FROM Invoice [WHERE ...] ORDERBY <f>
        STARTPOSITION n MAXRESULTS m`. `Metadata.LastUpdatedTime` is the
        incremental cursor field.
        """
        sql = f"SELECT * FROM {entity}"
        if where:
            sql += f" WHERE {where}"
        sql += f" ORDERBY {order_by} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        path = f"/v3/company/{self._company_uuid}/query"
        params = {"query": sql, "minorversion": _MINOR_VERSION}
        resp = await self._request("GET", path, params=params)
        qr = resp.get("QueryResponse")
        if not isinstance(qr, dict):
            return [], None
        rows = qr.get(entity)
        rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        # GUSTO returns maxResults == page length; a short page is terminal.
        returned = int(qr.get("maxResults", len(rows)) or 0)
        next_start = start_position + len(rows)
        is_last = returned < max_results or not rows
        return rows, (None if is_last else next_start)

    async def company_info(self) -> dict[str, Any]:
        """`GET /v3/company/{company}/companyinfo/{company}` — connectivity probe."""
        path = f"/v3/company/{self._company_uuid}/companyinfo/{quote(self._company_uuid)}"
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
) -> GustoApiError:
    status = response.status_code
    if status in (401, 403):
        return GustoApiError(
            f"gusto {status}: access token rejected or insufficient scope "
            "(may need refresh)",
            code="gusto_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return GustoApiError(
            "gusto 404: entity/company not found or not visible",
            code="gusto_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return GustoApiError(
            "gusto rate limit (429), retry budget exhausted",
            code="gusto_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return GustoApiError(
        f"gusto returned {status}",
        code="gusto_api_error",
        context={"http_status": status, "path": path},
    )


# The entity types we shard on, in dependency order (customers/vendors first
# would be ideal but v1 ingests the four transactional entities directly).
DEFAULT_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")


__all__ = ["GustoClient", "DEFAULT_ENTITIES"]
