"""services/ingest/integrations/hibob/client.py — outbound HiBob REST client.

Single outbound surface for backfill + poll-incremental. HiBob is authenticated
with a **service user**: a ``service_user_id`` + a long-lived ``token`` presented
as HTTP **Basic** auth (``Authorization: Basic base64(id:token)``). This clones
Brex's secret-resolved-token client posture (long-lived credential, resolved once
from the secret store or preset in spammer mode and reused for the life of the
client; NO token refresh), but the scheme is Basic, not Bearer — so the token
half is resolved from the secret store and the id half rides on the install row.

TODO(human): confirm HiBob API host + read endpoints/paths + the per-entity
    "modified since" filter. The host defaults via the endpoint resolver
    (``endpoint("hibob_api")``) and is overridable per-install (``base_url``) and
    per-env (``HIBOB_API_BASE_URL``). The read surface below
    (``/v1/people``, ``/v1/people/{id}``, lifecycle/time-off/payroll list paths)
    is modelled on HiBob's documented People API but the exact collection paths
    + query params per entity type are UNVERIFIED; implement only the verified
    read surface and tag speculative endpoints.

TODO(human): confirm HiBob concurrent rate-limit numbers + signalling. This
    defaults to 429 + ``Retry-After`` (the Brex scheme); tune the retry budget
    via ``HIBOB_RL_MAX_ATTEMPTS`` / ``HIBOB_RL_MAX_SLEEP_SEC``. HiBob's real
    per-account concurrency limit is UNVERIFIED.

Logging redaction: the service-user token and the Basic auth header are NEVER
logged.
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import HibobApiError


log = structlog.get_logger("integrations.hibob.client")


_DEFAULT_TIMEOUT_S = 30.0
# Default to 100 to bound payload size and keep parity with the other sources.
# HiBob's real per-entity page cap is UNVERIFIED (see the fetcher TODO).
_DEFAULT_PAGE_SIZE = 100


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class HibobClient:
    """Outbound HiBob REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_hibob_client`
    (production / spammer) and by the seed/onboarding probe. Shares the
    process-wide httpx client when one is injected.

    Auth is HTTP Basic ``base64(service_user_id:token)``: the ``service_user_id``
    is the public half (rides on the install row), the ``token`` is the secret
    half (resolved from the secret store; preset in spammer mode).
    """

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
        # Preset token (spammer mode presets a recognized token); otherwise
        # resolved lazily from the secret store on first request.
        self._token: str | None = token
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical HiBob API host; a
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

    async def _token_value(self) -> str:
        if self._token is not None:
            return self._token
        async with self._token_lock:
            if self._token is not None:
                return self._token
            if (
                self._secret_store is None
                or self._secret_ref is None
                or self._tenant_id is None
            ):
                raise HibobApiError(
                    "hibob client has no service-user token and cannot resolve "
                    "one (missing secret_store / secret_ref / tenant_id)",
                    code="hibob_api_unauthorized",
                )
            raw = await self._secret_store.get(
                self._secret_ref, tenant_id=self._tenant_id,
            )
            self._token = (
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
            return self._token

    async def _auth_header(self) -> str:
        """HTTP Basic ``base64(service_user_id:token)``."""
        token = await self._token_value()
        creds = f"{self._service_user_id}:{token}".encode("utf-8")
        return "Basic " + base64.b64encode(creds).decode("ascii")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One HiBob API call with bounded Retry-After-aware 429 retry.

        Returns the parsed JSON object. Non-2xx (including a still-429 after the
        budget is spent) is mapped to `HibobApiError`.
        """
        from services.ingest.integrations.hibob import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}{path}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
        }
        max_attempts = int(os.environ.get("HIBOB_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("HIBOB_RL_MAX_SLEEP_SEC", "30"))
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
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise HibobApiError(
                        "hibob response was not a JSON object",
                        code="hibob_api_error",
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
    #
    # ONE generic entity lister keeps the fetcher entity-agnostic (one shard
    # per entity_type). Returns `(rows, next_offset)`; `next_offset is None`
    # is terminal — offset/limit pagination, mirroring Brex/Mercury.
    # -----------------------------------------------------------------

    async def list_entities(
        self,
        entity_type: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        modified_since: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """List one People/HR entity type for the company.

        TODO(human): confirm the per-entity collection path + the response
            envelope key + the "modified since" param name. HiBob's People API
            returns ``{"employees": [...]}`` for people; lifecycle/time-off/
            payroll live under different paths/keys (UNVERIFIED). The mapping
            below is modelled on the documented People API and tagged
            speculative for the other three entity types.

        Returns `(rows, next_offset)`; `next_offset is None` is terminal.
        """
        path, envelope_key = _entity_endpoint(entity_type)
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if modified_since:
            # TODO(human): confirm HiBob's incremental filter param name (e.g.
            # `modifiedSince` vs `since`); placeholder per the People API.
            params["modifiedSince"] = modified_since
        resp = await self._request("GET", path, params=params)
        rows = resp.get(envelope_key)
        if not isinstance(rows, list):
            # Some HiBob list responses return a bare list / a generic "values".
            rows = resp.get("values") if isinstance(resp.get("values"), list) else []
        rows = [r for r in rows if isinstance(r, dict)]
        next_offset = offset + len(rows)
        # A short page (< limit) is terminal.
        is_last = len(rows) < limit or not rows
        return rows, (None if is_last else next_offset)

    async def company_info(self) -> dict[str, Any]:
        """Connectivity / scope probe.

        TODO(human): confirm the HiBob company/account metadata endpoint. The
            ``/v1/company/named-lists`` placeholder below is a cheap authenticated
            GET; swap for the verified company-info path.
        """
        return await self._request("GET", "/v1/company/named-lists")


# ---------------------------------------------------------------------
# Entity → (path, response-envelope-key) mapping
# ---------------------------------------------------------------------

# TODO(human): confirm each entity's real collection path + response key.
# `employee` is modelled on the documented People API (`GET /v1/people` →
# `{"employees": [...]}`); the other three are UNVERIFIED placeholders.
_ENTITY_ENDPOINTS: dict[str, tuple[str, str]] = {
    "employee": ("/v1/people", "employees"),
    "lifecycle": ("/v1/people/lifecycle", "values"),
    "timeoff": ("/v1/timeoff/requests", "requests"),
    "payroll": ("/v1/payroll/history", "values"),
}


def _entity_endpoint(entity_type: str) -> tuple[str, str]:
    return _ENTITY_ENDPOINTS.get(entity_type, (f"/v1/{entity_type}", "values"))


# The entity types we shard on (one shard per type). Per the CONTRACT.
DEFAULT_ENTITIES = ("employee", "lifecycle", "timeoff", "payroll")


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
) -> HibobApiError:
    """Map a non-2xx HiBob response to a typed `HibobApiError`."""
    status = response.status_code
    if status in (401, 403):
        return HibobApiError(
            f"hibob {status}: service-user token rejected or insufficient scope",
            code="hibob_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return HibobApiError(
            "hibob 404: entity/resource not found or not visible to the service user",
            code="hibob_api_not_found",
            context={"http_status": 404, "path": path},
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
