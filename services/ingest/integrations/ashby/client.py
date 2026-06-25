"""services/ingest/integrations/ashby/client.py — outbound Ashby RPC client.

Single outbound surface for backfill + poll-incremental. Ashby is authenticated
with a long-lived **API key** presented as HTTP Basic: the key is the username
and the password is EMPTY, i.e. the ``Authorization: Basic <base64("KEY:")>``
header (CONFIRMED from Ashby's first-party API docs — same posture as a Jira
api-token, but Ashby uses an empty password rather than email:token). The key is
resolved once from the secret store (or preset in spammer mode) and reused for
the life of the client. No token refresh (API-key archetype, like Brex/Jira).

Read surface (CONFIRMED): Ashby exposes an RPC-style API — every call is an
HTTP ``POST`` to ``/<Category>.list`` or ``/<Category>.info`` with a JSON body.
``.list`` paginates with a CURSOR: the request body carries ``cursor`` (and
``syncToken`` for incremental delta polls); the response carries ``results``,
``moreDataAvailable`` (bool), ``nextCursor`` (string|null), and — when a
``syncToken`` was supplied or the listing supports sync — a refreshed
``syncToken`` to persist for the next incremental poll.

TODO(human): confirm Ashby concurrent rate-limit numbers + the exact rate-limit
    signal. UNVERIFIED. The default below is 429 + ``Retry-After`` (env knobs
    ASHBY_RL_MAX_ATTEMPTS / ASHBY_RL_MAX_SLEEP_SEC); Ashby's docs describe a
    burst/sustained quota whose concurrent number is not pinned here — tune the
    retry budget once the real limits are confirmed.

Logging redaction: the API key and the Authorization header are NEVER logged.
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import AshbyApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.ashby.client")


_DEFAULT_TIMEOUT_S = 30.0
# Ashby's .list endpoints cap the page size; 100 keeps parity with the other
# entity-model sources and bounds payload size.
_DEFAULT_PAGE_SIZE = 100


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


def _basic_auth_value(api_key: str) -> str:
    """`Basic <base64("KEY:")>` — the API key as the Basic *username* with an
    EMPTY password (CONFIRMED Ashby auth scheme). The key is never logged."""
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


class AshbyClient:
    """Outbound Ashby RPC client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_ashby_client`
    (production / spammer; added during the wiring phase). Shares the
    process-wide httpx client when one is injected.
    """

    def __init__(
        self,
        *,
        base_url: str,
        org_id: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        api_key: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._org_id = org_id
        # Preset key (spammer mode presets a recognized key); otherwise resolved
        # lazily from the secret store on first request.
        self._api_key_cache = SecretValueCache(preset=api_key)
        self._token_lock = asyncio.Lock()
        # In production the base is the canonical Ashby host; a spammer/test
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

    async def _key(self) -> str:
        return await self._api_key_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: AshbyApiError(
                "ashby client has no api key and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="ashby_api_unauthorized",
            )
        )

    async def _auth_header(self) -> str:
        return _basic_auth_value(await self._key())

    async def _rpc(
        self, method_path: str, body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One Ashby RPC `POST /<method_path>` with bounded Retry-After-aware
        429 retry. Returns the parsed JSON object. Non-2xx (including a still-429
        after the budget is spent) is mapped to `AshbyApiError`.

        Ashby returns 200 even for application-level failures, wrapping the
        result in ``{"success": bool, "errors": [...], "results": ...}``; a
        ``success == False`` body is mapped to a typed error so the fetcher's
        rate-limit / terminal handling stays uniform.
        """
        from services.ingest.integrations.ashby import metrics

        auth = await self._auth_header()
        url = f"{self._api_base_url}/{method_path.lstrip('/')}"
        headers = {
            "Authorization": auth,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        max_attempts = int(os.environ.get("ASHBY_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("ASHBY_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.request(
                    "POST", url, headers=headers, json=(body or {}),
                )
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise AshbyApiError(
                    "transport error calling ashby",
                    code="ashby_api_error",
                    context={"error_type": type(exc).__name__,
                             "path": method_path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                metrics.record_request("rate_limited")
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                body_json = _safe_json(response)
                if not isinstance(body_json, dict):
                    metrics.record_request("error")
                    raise AshbyApiError(
                        "ashby response was not a JSON object",
                        code="ashby_api_error",
                        context={"path": method_path},
                    )
                # Ashby's envelope: success flag + results. A False success at
                # HTTP 200 is an application error (bad cursor, scope, …).
                if body_json.get("success") is False:
                    metrics.record_request("error")
                    raise AshbyApiError(
                        "ashby rpc returned success=false",
                        code="ashby_api_error",
                        context={"path": method_path,
                                 "errors": body_json.get("errors")},
                    )
                metrics.record_request("ok")
                return body_json

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
            else:
                metrics.record_request("error")
            raise _api_error_from_response(response, method_path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_entities(
        self,
        category: str,
        *,
        cursor: str | None = None,
        sync_token: str | None = None,
        limit: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], str | None, str | None]:
        """`POST /<Category>.list` — one cursor page of one entity category.

        ``cursor`` resumes a backfill page walk; ``sync_token`` (mutually used on
        a fresh incremental poll) requests only entities changed since the token
        was minted. Returns ``(results, next_cursor, next_sync_token)``:

          - ``next_cursor is None`` (driven off ``moreDataAvailable``) is terminal
            for the current walk.
          - ``next_sync_token`` is the refreshed ``syncToken`` to PERSIST for the
            next incremental poll (None when the listing did not return one).

        ``category`` is the Ashby RPC category (e.g. ``candidate`` -> POSTs to
        ``candidate.list``). The caller passes the lowercase entity_type.
        """
        payload: dict[str, Any] = {"limit": limit}
        if cursor:
            payload["cursor"] = cursor
        if sync_token:
            payload["syncToken"] = sync_token
        resp = await self._rpc(f"{category}.list", payload)

        results = resp.get("results")
        results = [r for r in results if isinstance(r, dict)] if isinstance(results, list) else []

        more = bool(resp.get("moreDataAvailable"))
        next_cursor = resp.get("nextCursor")
        next_cursor = next_cursor if (more and isinstance(next_cursor, str) and next_cursor) else None

        next_sync_token = resp.get("syncToken")
        next_sync_token = next_sync_token if isinstance(next_sync_token, str) and next_sync_token else None

        return results, next_cursor, next_sync_token

    async def get_entity(self, category: str, entity_id: str) -> dict[str, Any]:
        """`POST /<Category>.info` — one entity by id (detail / probe).

        Returns the bare entity object (unwrapped from the ``results`` envelope
        when present).
        """
        resp = await self._rpc(f"{category}.info", {"id": entity_id})
        results = resp.get("results")
        return results if isinstance(results, dict) else resp


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
) -> AshbyApiError:
    """Map a non-2xx Ashby response to a typed `AshbyApiError`."""
    status = response.status_code
    if status in (401, 403):
        return AshbyApiError(
            f"ashby {status}: API key rejected or insufficient scope",
            code="ashby_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return AshbyApiError(
            "ashby 404: entity/org not found or not visible to the key",
            code="ashby_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return AshbyApiError(
            "ashby rate limit (429), retry budget exhausted",
            code="ashby_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return AshbyApiError(
        f"ashby returned {status}",
        code="ashby_api_error",
        context={"http_status": status, "path": path},
    )


# The entity types we shard on (per the cross-agent CONTRACT). Ashby is
# recruiting-ATS-shaped (NOT transactional), so the entity_kind discriminates
# the external_id.
DEFAULT_ENTITIES = ("candidate", "application", "job", "interview", "offer")


__all__ = ["AshbyClient", "DEFAULT_ENTITIES"]
