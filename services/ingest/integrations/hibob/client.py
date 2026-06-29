"""services/ingest/integrations/hibob/client.py - outbound HiBob REST client.

Verified HiBob surface:

  * auth: HTTP Basic base64(service_user_id:token)
  * employees: POST /v1/people/search, no pagination
  * time off changes: GET /v1/timeoff/requests/changes
  * salary/payroll history: GET /v1/bulk/people/salaries, cursor pagination
  * lifecycle/work history: GET /v1/bulk/people/work, cursor pagination
"""
from __future__ import annotations

import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import HibobApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.hibob.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 200

DEFAULT_ENTITIES = ("employee", "lifecycle", "timeoff", "payroll")

_PEOPLE_FIELDS = [
    "root.id",
    "root.displayName",
    "root.email",
    "work.department",
    "work.title",
    "work.startDate",
    "work.site",
    "about.avatar",
]


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class HibobClient:
    """Outbound HiBob REST client, one per backfill/poll shard open."""

    def __init__(
        self,
        *,
        base_url: str,
        company_id: str,
        service_user_id: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._company_id = company_id
        self._service_user_id = service_user_id
        self._token_cache = SecretValueCache(preset=token)
        self._token_lock = asyncio.Lock()
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

    async def _token_value(self) -> str:
        return await self._token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: HibobApiError(
                "hibob client has no service-user token and cannot resolve "
                "one (missing secret_store / secret_ref / tenant_id)",
                code="hibob_api_unauthorized",
            )
        )

    async def _auth_header(self) -> str:
        token = await self._token_value()
        creds = f"{self._service_user_id}:{token}".encode("utf-8")
        return "Basic " + base64.b64encode(creds).decode("ascii")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        from services.ingest.integrations.hibob import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        max_attempts = int(os.environ.get("HIBOB_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("HIBOB_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                )
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise HibobApiError(
                    "transport error calling hibob",
                    code="hibob_api_error",
                    context={"error_type": type(exc).__name__, "path": path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                metrics.record_request("rate_limited")
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                metrics.record_request("ok")
                return _safe_json(response)

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
            else:
                metrics.record_request("error")
            raise _api_error_from_response(response, path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_entities(
        self,
        entity_type: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        page_cursor: str | None = None,
        modified_since: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        kind = entity_type.lower()
        eff_limit = max(1, min(_MAX_PAGE_SIZE, int(limit or _DEFAULT_PAGE_SIZE)))
        if kind == "employee":
            rows = await self._people_search(modified_since=modified_since)
            page = rows[offset:offset + eff_limit]
            next_offset = offset + len(page)
            return page, (str(next_offset) if next_offset < len(rows) and page else None)
        if kind == "timeoff":
            return await self._timeoff_changes(modified_since=modified_since)
        if kind == "payroll":
            return await self._cursor_table(
                "/v1/bulk/people/salaries",
                result_key="results",
                limit=eff_limit,
                cursor=page_cursor,
                modified_since=modified_since,
            )
        if kind == "lifecycle":
            return await self._cursor_table(
                "/v1/bulk/people/work",
                result_key="results",
                limit=eff_limit,
                cursor=page_cursor,
                modified_since=modified_since,
            )
        return [], None

    async def company_info(self) -> dict[str, Any]:
        """Cheap connectivity probe via a one-field people search."""
        rows = await self._people_search(limit=1, modified_since=None)
        return {
            "id": self._company_id,
            "company_id": self._company_id,
            "sample_employee_count": len(rows),
        }

    async def _people_search(
        self,
        *,
        limit: int | None = None,
        modified_since: str | None,
    ) -> list[dict[str, Any]]:
        body = {
            "fields": _PEOPLE_FIELDS,
            "showInactive": True,
        }
        response = await self._request("POST", "/v1/people/search", json_body=body)
        rows = _extract_list(response, "employees")
        rows = [_normalise_employee(r) for r in rows]
        if modified_since:
            rows = [r for r in rows if (_row_modified(r) or "") > modified_since]
        return rows[:limit] if limit is not None else rows

    async def _timeoff_changes(
        self, *, modified_since: str | None,
    ) -> tuple[list[dict[str, Any]], None]:
        since = modified_since or _six_month_floor()
        params = {"since": since, "to": _now_iso()}
        response = await self._request(
            "GET", "/v1/timeoff/requests/changes", params=params,
        )
        rows = _extract_list(response, "requests", "changes", "values")
        rows = [_stamp_entity_kind(r, "timeoff") for r in rows]
        return rows, None

    async def _cursor_table(
        self,
        path: str,
        *,
        result_key: str,
        limit: int,
        cursor: str | None,
        modified_since: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        params: dict[str, Any] = {
            "limit": limit,
            "includeArchived": "true",
        }
        if cursor:
            params["cursor"] = cursor
        response = await self._request("GET", path, params=params)
        rows = _extract_list(response, result_key, "results", "values")
        if modified_since:
            rows = [r for r in rows if (_row_modified(r) or "") > modified_since]
        next_cursor = _next_cursor(response)
        return rows, next_cursor


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _extract_list(body: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in keys:
            value = body.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def _next_cursor(body: Any) -> str | None:
    if not isinstance(body, dict):
        return None
    metadata = body.get("response_metadata")
    if isinstance(metadata, dict):
        value = metadata.get("next_cursor") or metadata.get("nextCursor")
        if isinstance(value, str) and value:
            return value
    value = body.get("next_cursor") or body.get("cursor")
    return value if isinstance(value, str) and value else None


def _normalise_employee(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.setdefault("id", row.get("id") or row.get("/root/id"))
    out.setdefault("displayName", row.get("displayName") or row.get("/root/displayName"))
    out.setdefault("email", row.get("email") or row.get("/root/email"))
    work = row.get("work")
    if isinstance(work, dict):
        out.setdefault("department", work.get("department"))
        out.setdefault("title", work.get("title"))
        out.setdefault("startDate", work.get("startDate"))
    out.setdefault("status", row.get("status") or row.get("employmentStatus") or "active")
    return _stamp_entity_kind(out, "employee")


def _stamp_entity_kind(row: dict[str, Any], kind: str) -> dict[str, Any]:
    out = dict(row)
    out.setdefault("_hibob_entity_type", kind)
    return out


def _row_modified(row: dict[str, Any]) -> str | None:
    for key in ("modified", "modifiedAt", "lastModified", "updatedAt", "updated"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _six_month_floor() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> HibobApiError:
    status = response.status_code
    if status in (401, 403):
        return HibobApiError(
            f"hibob {status}: service-user token rejected or insufficient scope",
            code="hibob_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return HibobApiError(
            "hibob 404: entity/resource not found or not visible",
            code="hibob_api_not_found",
            context={"http_status": status, "path": path},
        )
    if status == 429:
        return HibobApiError(
            "hibob rate limit (429), retry budget exhausted",
            code="hibob_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return HibobApiError(
        f"hibob returned {status}",
        code="hibob_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["HibobClient", "DEFAULT_ENTITIES"]
