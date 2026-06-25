"""services/ingest/integrations/gusto/client.py — outbound Gusto client.

Single outbound surface for backfill + poll-incremental. Gusto is a payroll
REST API (base path ``/v1`` on ``https://api.gusto.com``; the demo environment
is ``https://api.gusto-demo.com``) authenticated with an OAuth 2.0 Bearer
**access token** (expires after 2 h; the rotating refresh token re-mints it via
``POST /oauth/token`` — see `integrations/oauth_refresh.py`). Every read is
scoped to a company ``company_uuid``:

    GET /v1/companies/{company_uuid}            — connectivity probe (single object)
    GET /v1/companies/{company_uuid}/employees  — bare JSON array
    GET /v1/companies/{company_uuid}/payrolls   — bare JSON array

VERIFIED against docs.gusto.com (embedded-payroll/app-integrations reference,
2026-06): list responses are **bare arrays**; pagination is **offset-style via
query params** `page` / `per` (per defaults to 25, max 100) with the totals in
**response headers** `X-Total-Count` / `X-Page` / `X-Per-Page` (no Link
header). Payrolls accept a date window (`start_date` / `end_date`, optionally
`date_filter_by=check_date`); employees have NO updated-since filter (callers
full re-walk + dedup). Dollar amounts are decimal STRINGS (e.g. "1234.56").

The optional `X-Gusto-API-Version` header pins the API version (date string,
e.g. "2026-02-01" — the reference default); omitted requests fall back to the
app's Developer Portal minimum. Pin via GUSTO_API_VERSION.

Rate limits: 429 + Retry-After (env knobs GUSTO_RL_MAX_ATTEMPTS /
GUSTO_RL_MAX_SLEEP_SEC). Non-2xx maps to ``GustoApiError``. A 401/403 triggers
one reactive token re-mint via `refresh_on_unauthorized` (inert in spammer
mode), then retries once.

Logging redaction: the access token / auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Sequence
from uuid import UUID
from urllib.parse import quote

import httpx
import structlog

from lib.shared.errors import GustoApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.gusto.client")


_DEFAULT_TIMEOUT_S = 30.0
# docs.gusto.com pagination: `per` defaults to 25, max 100.
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 100
# Pinned API version (docs.gusto.com reference default). Overridable per env.
_DEFAULT_API_VERSION = "2026-02-01"


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


def _api_version() -> str:
    return os.environ.get("GUSTO_API_VERSION", _DEFAULT_API_VERSION)


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
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._company_uuid = company_uuid
        # Phase 3: reactive OAuth re-mint on 401 (inert in spammer mode).
        self._install_row_id = install_row_id
        self._refresh_secret_ref = refresh_secret_ref
        self._access_token_cache = SecretValueCache(preset=access_token)
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical Gusto host; a spammer/test
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
        return await self._access_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: GustoApiError(
                "gusto client has no access token and cannot resolve "
                "one (missing secret_store / secret_ref / tenant_id)",
                code="gusto_api_unauthorized",
            )
        )

    async def _request(
        self, method: str, path: str, *, params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        from services.ingest.integrations.gusto import metrics
        from services.ingest.integrations.oauth_refresh import (
            refresh_on_unauthorized,
        )

        url = f"{self._api_base_url}{path}"
        max_attempts = int(os.environ.get("GUSTO_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("GUSTO_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        reminted = False
        refreshed_token: str | None = None
        while True:
            attempt += 1
            token = refreshed_token or await self._token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "X-Gusto-API-Version": _api_version(),
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
                return response

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
                        self._access_token_cache.set(new_token)
                        refreshed_token = new_token
                        continue
                raise _api_error_from_response(response, path)
            metrics.record_request("error")
            raise _api_error_from_response(response, path)

    def _list_rows(self, response: httpx.Response, path: str) -> list[dict[str, Any]]:
        body = _safe_json(response)
        if not isinstance(body, list):
            raise GustoApiError(
                "gusto list response was not a JSON array",
                code="gusto_api_error",
                context={"path": path},
            )
        return [r for r in body if isinstance(r, dict)]

    @staticmethod
    def _next_page(
        page: int, rows: list[dict[str, Any]], per: int, headers: httpx.Headers,
    ) -> int | None:
        """The next `page`, or None when terminal.

        Prefer the documented count headers (`X-Total-Count` / `X-Page` /
        `X-Per-Page`); fall back to the short-/empty-page heuristic when a
        server omits them. A short or empty page is always terminal.
        """
        if not rows or len(rows) < per:
            return None
        total = headers.get("X-Total-Count")
        if total is not None:
            try:
                cur_page = int(headers.get("X-Page", page))
                per_page = int(headers.get("X-Per-Page", per))
                if cur_page * per_page >= int(total):
                    return None
                return cur_page + 1
            except (TypeError, ValueError):
                pass
        return page + 1

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_employees(
        self,
        *,
        page: int = 1,
        per: int = _DEFAULT_PAGE_SIZE,
        terminated: bool | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """`GET /v1/companies/{company_uuid}/employees` — one page of employee
        objects (bare array). Returns `(rows, next_page)`; `next_page is None`
        is terminal.

        There is NO updated-since filter on this endpoint — incremental sync is
        a full re-walk, deduped downstream via the `version`-discriminated
        external_id. `terminated` is a FILTER (true → only terminated /
        scheduled-to-terminate employees); leave it None to walk the default
        collection.
        """
        per = min(per, _MAX_PAGE_SIZE)
        params: dict[str, Any] = {"page": page, "per": per}
        if terminated is not None:
            params["terminated"] = "true" if terminated else "false"
        path = f"/v1/companies/{quote(self._company_uuid)}/employees"
        response = await self._request("GET", path, params=params)
        rows = self._list_rows(response, path)
        return rows, self._next_page(page, rows, per, response.headers)

    async def list_payrolls(
        self,
        *,
        page: int = 1,
        per: int = _DEFAULT_PAGE_SIZE,
        start_date: str | None = None,
        end_date: str | None = None,
        date_filter_by: str | None = None,
        processing_statuses: Sequence[str] | None = None,
        payroll_types: Sequence[str] | None = None,
        sort_order: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """`GET /v1/companies/{company_uuid}/payrolls` — one page of payroll
        objects (bare array). Returns `(rows, next_page)`; `next_page is None`
        is terminal.

        Date window: `start_date` / `end_date` (YYYY-MM-DD) filter by pay
        period by default; `date_filter_by="check_date"` switches the filter
        field (the incremental high-water this repo tracks). The window may not
        exceed 1 year. `processing_statuses` defaults server-side to
        `processed`; `payroll_types` defaults to `regular`.
        """
        per = min(per, _MAX_PAGE_SIZE)
        params: dict[str, Any] = {"page": page, "per": per}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if date_filter_by:
            params["date_filter_by"] = date_filter_by
        if processing_statuses:
            params["processing_statuses"] = ",".join(processing_statuses)
        if payroll_types:
            params["payroll_types"] = ",".join(payroll_types)
        if sort_order:
            params["sort_order"] = sort_order
        path = f"/v1/companies/{quote(self._company_uuid)}/payrolls"
        response = await self._request("GET", path, params=params)
        rows = self._list_rows(response, path)
        return rows, self._next_page(page, rows, per, response.headers)

    async def company(self) -> dict[str, Any]:
        """`GET /v1/companies/{company_uuid}` — cheap connectivity/credential
        probe. Returns the single company object (uuid, name, trade_name,
        company_status, ...)."""
        path = f"/v1/companies/{quote(self._company_uuid)}"
        response = await self._request("GET", path)
        body = _safe_json(response)
        if not isinstance(body, dict):
            raise GustoApiError(
                "gusto company response was not a JSON object",
                code="gusto_api_error",
                context={"path": path},
            )
        return body


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
            "gusto 404: resource/company not found or not visible",
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


# The entity kinds we shard on — one `gusto_entity` shard per kind. `employee`
# walks /employees (full re-walk + version dedup); `payroll` walks /payrolls
# (check_date high-water incremental).
DEFAULT_ENTITIES = ("employee", "payroll")


__all__ = ["GustoClient", "DEFAULT_ENTITIES"]
