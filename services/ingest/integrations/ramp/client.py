"""services/ingest/integrations/ramp/client.py — outbound Ramp client.

Single outbound surface for backfill + poll-incremental. Ramp is authenticated
with an OAuth 2.0 Bearer **access token** (~hours lifetime) and every call is
scoped to a company ``businessId``. The access token is resolved once from the
secret store (or preset in spammer mode) and reused for the life of the client.
Production token refresh (the rotating refresh token) is owned by the
oauth_poller; this read client consumes the current access token.

This module is cloned from the QuickBooks archetype. The Ramp-specific
read surface (host, endpoints, query vs REST list, OAuth scopes) is UNVERIFIED
and kept configurable behind the archetype defaults — see the TODO markers below.

Reads go through the cloned **query endpoint** shape:
    GET /v3/company/{businessId}/query?query=<SQL>&minorversion=75
returning ``{"QueryResponse": {"<Entity>": [...], "startPosition", "maxResults"}}``.
Pagination is offset-based via ``STARTPOSITION n MAXRESULTS m`` in the SQL.
TODO(human): confirm Ramp read endpoints + OAuth scopes (Ramp is likely a
REST list API under https://api.ramp.com/developer/v1, NOT a SQL-query API —
if so, switch ``query()`` to a list+from_date REST call; see fetchers/ramp.py).

TODO(human): implement Ramp OAuth token refresh (refresh-on-401 or poller;
none exists, this is the QBO seam — persist refresh_secret_ref + token_expires_at
and exchange the rotating refresh token, then retry once).

Rate limits: default to 429 + ``Retry-After`` (Mercury/QBO scheme), honoured with
a bounded retry. Non-2xx maps to ``RampApiError``.
TODO(human): confirm Ramp rate-limit signalling (429+Retry-After vs
X-RateLimit-Reset); env knobs RAMP_RL_MAX_ATTEMPTS / RAMP_RL_MAX_SLEEP_SEC.

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

from lib.shared.errors import RampApiError


log = structlog.get_logger("integrations.ramp.client")


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


class RampClient:
    """Outbound Ramp Online client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_ramp_client`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        business_id: str,
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
        self._business_id = business_id
        # Phase 3: proactive (expiry skew) + reactive (401) OAuth re-mint
        # (inert in spammer mode).
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._token_expires_at = token_expires_at
        self._proactive_checked = False
        self._access_token: str | None = access_token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical RAMP host; a spammer/test
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
                raise RampApiError(
                    "ramp client has no access token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="ramp_api_unauthorized",
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
        from services.ingest.integrations.ramp import metrics
        from services.ingest.integrations.oauth_refresh import (
            maybe_proactive_refresh,
            refresh_on_unauthorized,
        )

        url = f"{self._api_base_url}{path}"
        max_attempts = int(os.environ.get("RAMP_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("RAMP_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        if not self._proactive_checked:
            self._proactive_checked = True
            proactive = await maybe_proactive_refresh(
                provider="ramp", pool=self._pool,
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
                raise RampApiError(
                    "transport error calling ramp",
                    code="ramp_api_error",
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
                    raise RampApiError(
                        "ramp response was not a JSON object",
                        code="ramp_api_error",
                        context={"path": path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
                if not reminted:
                    reminted = True
                    new_token = await refresh_on_unauthorized(
                        provider="ramp", pool=self._pool,
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

        RAMP query language: `SELECT * FROM Invoice [WHERE ...] ORDERBY <f>
        STARTPOSITION n MAXRESULTS m`. `Metadata.LastUpdatedTime` is the
        incremental cursor field.
        """
        sql = f"SELECT * FROM {entity}"
        if where:
            sql += f" WHERE {where}"
        sql += f" ORDERBY {order_by} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        path = f"/v3/company/{self._business_id}/query"
        params = {"query": sql, "minorversion": _MINOR_VERSION}
        resp = await self._request("GET", path, params=params)
        qr = resp.get("QueryResponse")
        if not isinstance(qr, dict):
            return [], None
        rows = qr.get(entity)
        rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        # RAMP returns maxResults == page length; a short page is terminal.
        returned = int(qr.get("maxResults", len(rows)) or 0)
        next_start = start_position + len(rows)
        is_last = returned < max_results or not rows
        return rows, (None if is_last else next_start)

    async def company_info(self) -> dict[str, Any]:
        """`GET /v3/company/{business}/companyinfo/{business}` — connectivity probe."""
        path = f"/v3/company/{self._business_id}/companyinfo/{quote(self._business_id)}"
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
) -> RampApiError:
    status = response.status_code
    if status in (401, 403):
        return RampApiError(
            f"ramp {status}: access token rejected or insufficient scope "
            "(may need refresh)",
            code="ramp_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return RampApiError(
            "ramp 404: entity/business not found or not visible",
            code="ramp_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return RampApiError(
            "ramp rate limit (429), retry budget exhausted",
            code="ramp_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return RampApiError(
        f"ramp returned {status}",
        code="ramp_api_error",
        context={"http_status": status, "path": path},
    )


# The entity types we shard on. Kept as the QBO archetype taxonomy
# (Invoice/Bill/BillPayment/Payment) so the cloned synthetic loop stays
# self-consistent end-to-end; the verified Ramp taxonomy is per blueprint §4
# (transaction / card / reimbursement — the cash/card flow entities carry the
# highest signal value).
# TODO(human): confirm Ramp resource taxonomy + exact entity names/casing
# (transaction vs card vs reimbursement) and re-key the generator + planner +
# handler decode together; start with the transaction flow (highest signal).
DEFAULT_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")


__all__ = ["RampClient", "DEFAULT_ENTITIES"]
