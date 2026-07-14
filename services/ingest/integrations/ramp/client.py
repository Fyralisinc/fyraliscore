"""services/ingest/integrations/ramp/client.py — outbound Ramp client.

Single outbound surface for backfill + poll-incremental. VERIFIED against the
official Ramp Developer API (docs.ramp.com, OpenAPI `/openapi/developer-api.json`):

  - Base: ``https://api.ramp.com/developer/v1`` — plain REST collections.
  - Auth: OAuth 2.0 **client-credentials** — mint a Bearer access token at
    ``POST {base}/token`` (HTTP Basic ``client_id:client_secret``, form body
    ``grant_type=client_credentials&scope=…``). Tokens live ``expires_in``
    seconds (~1 h) and there is NO refresh token for this grant — expiry/401 is
    handled by RE-MINTING (reactively via `refresh_on_unauthorized`, which now
    re-mints for ramp; proactively by the oauth poller).
  - Read surface: ``GET /transactions``, ``/reimbursements``, ``/cards``,
    ``/users`` + the ``GET /business`` connectivity probe.
  - Pagination: **KEYSET** — every list response is the envelope
    ``{"data": [...], "page": {"next": "<full URL embedding start=<last id>>"
    or null}}``. ``page_size`` default 20, allowed 2..100. The client follows
    ``page.next`` URLs; when an ``api_base_url`` override is set (spammer /
    mock), foreign-host next-URLs are re-rooted onto the override so mocks work.
  - Incremental: transactions support ``from_date``/``to_date`` (filter on
    ``user_transaction_time``, ISO8601); reimbursements support
    ``updated_after`` (filter on ``updated_at``). Cards/users have NO
    server-side incremental filter — callers fall back to a full idempotent
    re-walk (dedup via the deterministic external_id).

Spammer/test mode: a preset ``access_token`` (or ``api_base_url`` override)
skips real OAuth entirely — no client credentials needed.

Rate limits: 429 + ``Retry-After`` honoured with a bounded retry budget
(env knobs RAMP_RL_MAX_ATTEMPTS / RAMP_RL_MAX_SLEEP_SEC). Non-2xx maps to
``RampApiError`` with the stable ``ramp_api_*`` codes.

Logging redaction: the access token / client secret / auth header are NEVER
logged.
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import Any
from uuid import UUID
from urllib.parse import urlsplit

import httpx
import structlog

from lib.shared.errors import RampApiError
from services.ingest.integrations.secret_cache import (
    SecretValueCache,
    coerce_secret_text,
)


log = structlog.get_logger("integrations.ramp.client")


_DEFAULT_TIMEOUT_S = 30.0
# page_size is documented 2..100 (default 20); use the max for backfill.
_DEFAULT_PAGE_SIZE = 100
_MIN_PAGE_SIZE = 2

# Read-only scopes for the streams we ingest (docs.ramp.com OAuth scopes;
# overridable per-deploy via RAMP_OAUTH_SCOPES).
DEFAULT_SCOPES = (
    "transactions:read reimbursements:read cards:read users:read business:read"
)


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class RampClient:
    """Outbound Ramp Developer API client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_ramp_client`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        business_id: str = "",
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
        client_id: str | None = None,
        client_secret: str | None = None,
        scopes: str | None = None,
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
        self._access_token_cache = SecretValueCache(preset=access_token)
        self._token_lock = asyncio.Lock()
        # Client-credentials mint material. New installs can store a JSON
        # client_id/client_secret payload behind refresh_secret_ref; direct
        # constructor args and env remain compatibility/test fallbacks.
        self._client_id = client_id
        self._client_secret_cache = SecretValueCache(preset=client_secret)
        self._client_secret_lock = asyncio.Lock()
        self._scopes = scopes
        # In production the base is the canonical Ramp host
        # (https://api.ramp.com/developer/v1); a spammer/test override
        # (api_base_url) wins so backfill points at the mock.
        self._api_base_url = (api_base_url or base_url).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client

    @property
    def business_id(self) -> str:
        return self._business_id

    def _httpx(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_S)
            self._owns_client = True
        return self._http

    async def aclose(self) -> None:
        if self._owns_client and self._http is not None:
            await self._http.aclose()
            self._http = None

    # -----------------------------------------------------------------
    # Token: preset (spammer) → secret store → client-credentials mint
    # -----------------------------------------------------------------

    async def _mint_credentials(self) -> tuple[str | None, str | None]:
        from services.ingest.integrations.oauth_refresh import (
            decode_client_credentials_secret,
        )

        cid = self._client_id or os.environ.get("RAMP_CLIENT_ID")
        stored = self._client_secret_cache.get_if_fresh()
        if stored is None and (
            self._secret_store is not None
            and self._refresh_secret_ref is not None
            and self._tenant_id is not None
        ):
            stored = await self._client_secret_cache.resolve(
                lock=self._client_secret_lock,
                secret_store=self._secret_store,
                secret_ref=self._refresh_secret_ref,
                tenant_id=self._tenant_id,
                missing_error=lambda: RampApiError(
                    "ramp client cannot resolve client-credentials secret",
                    code="ramp_api_unauthorized",
                ),
            )
        if stored:
            stored_cid, stored_secret = decode_client_credentials_secret(stored)
            return (
                stored_cid or cid,
                stored_secret or os.environ.get("RAMP_CLIENT_SECRET"),
            )
        return cid, os.environ.get("RAMP_CLIENT_SECRET")

    async def mint_token(self) -> dict[str, Any]:
        """`POST {base}/token` — client-credentials mint (docs.ramp.com
        authorization). HTTP Basic `client_id:client_secret`, form body
        `grant_type=client_credentials&scope=…`. Returns the token response
        (`access_token`, `expires_in`, `token_type`, `scope`; NO refresh token
        for this grant) and caches the access token on the client."""
        from services.ingest.integrations.ramp import metrics

        cid, csec = await self._mint_credentials()
        if not (cid and csec):
            raise RampApiError(
                "ramp client cannot mint a token (missing client_id/"
                "client_secret; set refresh_secret_ref or "
                "RAMP_CLIENT_ID/RAMP_CLIENT_SECRET)",
                code="ramp_api_unauthorized",
            )
        basic = base64.b64encode(f"{cid}:{csec}".encode("utf-8")).decode("ascii")
        scopes = self._scopes or os.environ.get("RAMP_OAUTH_SCOPES", DEFAULT_SCOPES)
        try:
            response = await self._httpx().post(
                f"{self._api_base_url}/token",
                headers={
                    "Authorization": f"Basic {basic}",
                    "Accept": "application/json",
                },
                data={"grant_type": "client_credentials", "scope": scopes},
            )
        except httpx.TransportError as exc:
            metrics.record_request("error")
            raise RampApiError(
                "transport error minting ramp token",
                code="ramp_api_error",
                context={"error_type": type(exc).__name__, "path": "/token"},
            ) from exc
        if response.status_code // 100 != 2:
            metrics.record_request("unauthorized")
            raise RampApiError(
                f"ramp token mint returned {response.status_code}",
                code="ramp_api_unauthorized",
                context={"http_status": response.status_code, "path": "/token"},
            )
        body = _safe_json(response)
        token = body.get("access_token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            raise RampApiError(
                "ramp token mint response missing access_token",
                code="ramp_api_error",
                context={"path": "/token"},
            )
        self._access_token_cache.set(token)
        return body

    async def _token(self) -> str:
        value = self._access_token_cache.get_if_fresh()
        if value is not None:
            return value
        async with self._token_lock:
            value = self._access_token_cache.get_if_fresh()
            if value is not None:
                return value
            if (
                self._secret_store is not None
                and self._secret_ref is not None
                and self._tenant_id is not None
            ):
                raw = await self._secret_store.get(
                    self._secret_ref, tenant_id=self._tenant_id,
                )
                return self._access_token_cache.set(coerce_secret_text(raw))
            cid, csec = await self._mint_credentials()
            if cid and csec:
                body = await self.mint_token()
                token = body.get("access_token") if isinstance(body, dict) else None
                if isinstance(token, str) and token:
                    return token
            raise RampApiError(
                "ramp client has no access token and cannot resolve or mint "
                "one (missing secret_store/secret_ref/tenant_id and no "
                "client credentials)",
                code="ramp_api_unauthorized",
            )

    # -----------------------------------------------------------------
    # Request core
    # -----------------------------------------------------------------

    def _resolve_page_url(self, next_url: str) -> str:
        """Resolve a `page.next` URL against the configured base.

        Production next-URLs already start with the canonical base and pass
        through untouched. When an `api_base_url` override is in effect
        (spammer/mock) a foreign-host next-URL is re-rooted: the collection
        segment + query are grafted onto the override base so the keyset walk
        stays on the mock."""
        if next_url.startswith(self._api_base_url + "/"):
            return next_url
        parsed = urlsplit(next_url)
        resource = parsed.path.rstrip("/").rsplit("/", 1)[-1]
        rebuilt = f"{self._api_base_url}/{resource}"
        return f"{rebuilt}?{parsed.query}" if parsed.query else rebuilt

    async def _request(
        self,
        method: str,
        path: str | None = None,
        *,
        params: dict[str, Any] | None = None,
        url: str | None = None,
    ) -> dict[str, Any]:
        from services.ingest.integrations.ramp import metrics
        from services.ingest.integrations.oauth_refresh import (
            maybe_proactive_refresh,
            refresh_on_unauthorized,
        )

        if url is None:
            url = f"{self._api_base_url}{path}"
        log_path = path or urlsplit(url).path
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
                self._access_token_cache.set(proactive)

        attempt = 0
        reminted = False
        refreshed_token: str | None = None
        while True:
            attempt += 1
            token = refreshed_token or await self._token()
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
                    context={"error_type": type(exc).__name__, "path": log_path},
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
                        context={"path": log_path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
                if not reminted:
                    reminted = True
                    # Reactive re-mint: refresh_on_unauthorized's ramp config
                    # performs the client-credentials exchange + persists the
                    # new token ref (inert in spammer mode — preset token, no
                    # secret store).
                    new_token = await refresh_on_unauthorized(
                        provider="ramp", pool=self._pool,
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
                    # Self-mint fallback when the client holds creds directly.
                    cid, csec = await self._mint_credentials()
                    if cid and csec:
                        try:
                            body = await self.mint_token()
                        except RampApiError:
                            raise _api_error_from_response(response, log_path)
                        token = body.get("access_token") if isinstance(body, dict) else None
                        if isinstance(token, str) and token:
                            refreshed_token = token
                        continue
                raise _api_error_from_response(response, log_path)
            metrics.record_request("error")
            raise _api_error_from_response(response, log_path)

    # -----------------------------------------------------------------
    # Public read surface (keyset-paginated REST collections)
    # -----------------------------------------------------------------

    async def _list(
        self,
        path: str,
        params: dict[str, Any],
        page_url: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One keyset page. Returns `(rows, next_page_url)`;
        `next_page_url is None` is terminal (`page.next` null at EOF)."""
        if page_url:
            resp = await self._request("GET", url=self._resolve_page_url(page_url))
        else:
            resp = await self._request(
                "GET", path,
                params={k: v for k, v in params.items() if v is not None},
            )
        data = resp.get("data")
        rows = [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []
        page = resp.get("page")
        next_url = page.get("next") if isinstance(page, dict) else None
        next_url = next_url if isinstance(next_url, str) and next_url else None
        return rows, next_url

    async def list_transactions(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        state: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /transactions`. `from_date`/`to_date` filter on
        `user_transaction_time` (ISO8601); `start` is the id of the last entity
        of the previous page; pass `page_url` to follow a prior `page.next`."""
        return await self._list(
            "/transactions",
            {
                "from_date": from_date,
                "to_date": to_date,
                "state": state,
                "page_size": _clamp_page_size(page_size),
                "start": start,
            },
            page_url,
        )

    async def list_reimbursements(
        self,
        *,
        updated_after: str | None = None,
        from_date: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /reimbursements`. `updated_after` filters on `updated_at`
        (the true incremental filter); `from_date` filters on `created_at`."""
        return await self._list(
            "/reimbursements",
            {
                "updated_after": updated_after,
                "from_date": from_date,
                "page_size": _clamp_page_size(page_size),
                "start": start,
            },
            page_url,
        )

    async def list_cards(
        self,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /cards`. NO server-side incremental filter — callers re-walk
        the full collection idempotently (dedup via external_id)."""
        return await self._list(
            "/cards",
            {"page_size": _clamp_page_size(page_size), "start": start},
            page_url,
        )

    async def list_users(
        self,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /users`. NO server-side incremental filter — callers re-walk
        the full collection idempotently (dedup via external_id)."""
        return await self._list(
            "/users",
            {"page_size": _clamp_page_size(page_size), "start": start},
            page_url,
        )

    async def business(self) -> dict[str, Any]:
        """`GET /business` — cheap connectivity/credential probe. Returns the
        company info (`id` is the business_id every webhook carries at root)."""
        return await self._request("GET", "/business")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _clamp_page_size(value: int) -> int:
    return max(_MIN_PAGE_SIZE, min(_DEFAULT_PAGE_SIZE, int(value)))


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
            "(re-mint via client_credentials)",
            code="ramp_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return RampApiError(
            "ramp 404: resource not found or not visible",
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


# The entity streams we shard on — the VERIFIED Ramp resource taxonomy
# (docs.ramp.com): card transactions, out-of-pocket reimbursements, issued
# cards, and the employee directory. Lowercase singular; the planner seeds one
# `ramp_entity` shard per stream and the fetcher/handler key on these literals.
DEFAULT_ENTITIES = ("transaction", "reimbursement", "card", "user")


__all__ = ["RampClient", "DEFAULT_ENTITIES", "DEFAULT_SCOPES"]
