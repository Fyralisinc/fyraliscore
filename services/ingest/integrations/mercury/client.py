"""services/ingest/integrations/mercury/client.py — outbound Mercury banking REST client.

Single outbound surface for backfill, reconciliation, and the planner's account
enumeration. Mercury is authenticated with a long-lived Bearer API token.
Every actual provider attempt runs through ``ProviderTransport``; HTTP 429,
timeouts, and 5xx responses become typed retry outcomes instead of sleeping or
retrying inside this client.

Pagination: list endpoints return `{total, accounts|transactions: [...]}` and
accept `limit` + `offset`. The list helpers return `(items, next_offset,
is_last)`; `next_offset is None` is terminal.

Logging redaction: the API token and the auth header are NEVER logged.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import MercuryApiError
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


log = structlog.get_logger("integrations.mercury.client")


_DEFAULT_TIMEOUT_S = 30.0
# Mercury transaction listing caps the page at 500; default to 100 to bound
# payload size and keep parity with the other sources.
_DEFAULT_PAGE_SIZE = 100


class MercuryClient:
    """Outbound Mercury REST client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_mercury_client`
    (production / Provider Lab) and by the seed/onboarding account probe. Shares the
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
        # In production the base is the canonical Mercury API host; a
        # Lab/test override (api_base_url) points backfill at Provider Lab.
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
            source="mercury",
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
            missing_error=lambda: MercuryApiError(
                "mercury client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="mercury_api_unauthorized",
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
    ) -> dict[str, Any]:
        """Execute one semantic Mercury operation through ProviderTransport."""
        from services.ingest.integrations.mercury import metrics

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
                metrics.record_request("error")
                raise ProviderTimeoutError(
                    "Mercury request timed out",
                    source="mercury",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise ProviderTransientError(
                    "Mercury transport error",
                    source="mercury",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc

            if response.status_code == 429:
                metrics.record_request("rate_limited")
                raise ProviderRateLimited(
                    "Mercury rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="mercury",
                    operation=operation,
                )
            if response.status_code >= 500:
                metrics.record_request("error")
                raise ProviderTransientError(
                    f"Mercury returned HTTP {response.status_code}",
                    source="mercury",
                    operation=operation,
                    http_status=response.status_code,
                )
            return response

        response = await self._provider.execute(operation, _once)
        if response.status_code // 100 == 2:
            metrics.record_request("ok")
            body = _safe_json(response)
            if not isinstance(body, dict):
                raise MercuryApiError(
                    "mercury response was not a JSON object",
                    code="mercury_api_error",
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

    async def list_accounts(self) -> list[dict[str, Any]]:
        """`GET /accounts` — all accounts visible to the token, with balances.

        Used at seed/install time to populate `mercury_accounts`, and by the
        fetcher to emit per-account balance snapshots.
        """
        resp = await self._request(
            "GET",
            "/accounts",
            operation="accounts.list",
        )
        accounts = resp.get("accounts")
        if not isinstance(accounts, list):
            # Some Mercury responses return the bare list.
            accounts = resp if isinstance(resp, list) else []  # type: ignore[assignment]
        return [a for a in accounts if isinstance(a, dict)]

    async def get_account(self, account_id: str) -> dict[str, Any]:
        """`GET /account/{id}` — one account (balance snapshot probe)."""
        return await self._request(
            "GET",
            f"/account/{account_id}",
            operation="accounts.get",
        )

    async def list_transactions(
        self,
        account_id: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /account/{id}/transactions` — paginated transactions.

        `start` (ISO date) optionally bounds the window for incremental polls.
        Returns `(transactions, next_offset, total)`; `next_offset is None`
        signals no more pages.
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if start:
            params["start"] = start
        resp = await self._request(
            "GET",
            f"/account/{account_id}/transactions",
            params=params,
            operation="transactions.list",
        )
        txns = resp.get("transactions")
        txns = [t for t in txns if isinstance(t, dict)] if isinstance(txns, list) else []
        total = int(resp.get("total", len(txns)) or 0)
        next_offset = offset + len(txns)
        is_last = next_offset >= total or not txns
        return txns, (None if is_last else next_offset), total


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
) -> MercuryApiError:
    """Map a non-2xx Mercury response to a typed `MercuryApiError`."""
    status = response.status_code
    if status in (401, 403):
        return MercuryApiError(
            f"mercury {status}: API token rejected or insufficient scope",
            code="mercury_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return MercuryApiError(
            "mercury 404: account/resource not found or not visible to the token",
            code="mercury_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return MercuryApiError(
            "mercury rate limit (429), retry budget exhausted",
            code="mercury_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return MercuryApiError(
        f"mercury returned {status}",
        code="mercury_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["MercuryClient"]
