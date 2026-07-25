"""services/ingest/integrations/fireflies/client.py - outbound Fireflies client.

Fireflies is GraphQL-only: all reads are POSTs to /graphql. The methods keep
the existing fetcher seam while speaking the real protocol:

  * user() -> data.user (used as the token-owner identity)
  * transcript(id) -> data.transcript
  * transcripts(skip, limit<=50, fromDate, toDate) -> data.transcripts

GraphQL errors are terminal even when returned with HTTP 200.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import FirefliesApiError
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


log = structlog.get_logger("integrations.fireflies.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 50

_USER_QUERY = """
query User {
  user {
    id
    email
    name
  }
}
""".strip()

_TRANSCRIPTS_QUERY = """
query Transcripts($limit: Int, $skip: Int, $fromDate: DateTime, $toDate: DateTime) {
  transcripts(limit: $limit, skip: $skip, fromDate: $fromDate, toDate: $toDate) {
    id
    title
    date
    duration
    organizer_email
    organizerEmail
    participants
    transcript_url
    summary {
      overview
      shorthand_bullet
      gist
      action_items
      actionItems
    }
  }
}
""".strip()

_TRANSCRIPT_QUERY = """
query Transcript($id: String!) {
  transcript(id: $id) {
    id
    title
    date
    duration
    organizer_email
    organizerEmail
    participants
    transcript_url
    summary {
      overview
      shorthand_bullet
      gist
      action_items
      actionItems
    }
  }
}
""".strip()


class FirefliesClient:
    """Outbound Fireflies GraphQL client, one per backfill/poll shard open."""

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
        self._api_token_cache = SecretValueCache(preset=api_token)
        self._token_lock = asyncio.Lock()
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
            source="fireflies",
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
            missing_error=lambda: FirefliesApiError(
                "fireflies client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="fireflies_api_unauthorized",
            )
        )

    def _graphql_url(self) -> str:
        return (
            self._api_base_url
            if self._api_base_url.endswith("/graphql")
            else f"{self._api_base_url}/graphql"
        )

    async def _graphql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        *,
        operation: str,
    ) -> dict[str, Any]:
        from services.ingest.integrations.fireflies import metrics

        token = await self._token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables

        client = self._httpx()

        async def _once() -> dict[str, Any]:
            try:
                response = await client.post(
                    self._graphql_url(), headers=headers, json=payload,
                )
            except httpx.TimeoutException as exc:
                metrics.record_request("error")
                raise ProviderTimeoutError(
                    "Fireflies GraphQL request timed out",
                    source="fireflies",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise ProviderTransientError(
                    "Fireflies GraphQL transport error",
                    source="fireflies",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc

            if response.status_code == 429:
                metrics.record_request("rate_limited")
                raise ProviderRateLimited(
                    "Fireflies rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="fireflies",
                    operation=operation,
                )
            if response.status_code >= 500:
                metrics.record_request("error")
                raise ProviderTransientError(
                    f"Fireflies returned HTTP {response.status_code}",
                    source="fireflies",
                    operation=operation,
                    http_status=response.status_code,
                )

            if response.status_code // 100 != 2:
                if response.status_code in (401, 403):
                    metrics.record_request("unauthorized")
                else:
                    metrics.record_request("error")
                raise _api_error_from_response(response, "/graphql")

            body = _safe_json(response)
            if not isinstance(body, dict):
                raise FirefliesApiError(
                    "fireflies graphql response was not a JSON object",
                    code="fireflies_api_error",
                    context={"path": "/graphql"},
                )
            errors = body.get("errors")
            if errors:
                code = _graphql_error_code(errors)
                if code == "fireflies_api_rate_limited":
                    metrics.record_request("rate_limited")
                    raise ProviderRateLimited(
                        "Fireflies GraphQL rate limit",
                        retry_after_seconds=parse_retry_after(
                            response.headers.get("Retry-After"),
                        ),
                        header_parser_id="http.retry_after",
                        source="fireflies",
                        operation=operation,
                    )
                else:
                    metrics.record_request("error")
                raise FirefliesApiError(
                    "fireflies graphql returned errors",
                    code=code,
                    context={"errors": str(errors)[:300], "path": "/graphql"},
                )
            metrics.record_request("ok")
            data = body.get("data")
            return data if isinstance(data, dict) else {}

        return await self._provider.execute(operation, _once)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def get_workspace(self) -> dict[str, Any]:
        """Return the API-key owner as the install identity.

        Fireflies has no workspace object in the public GraphQL API. The user
        object is the durable owner identity we store in the existing
        workspace_id column.
        """
        data = await self._graphql(
            _USER_QUERY,
            operation="user.get",
        )
        user = data.get("user")
        if not isinstance(user, dict):
            return {}
        uid = str(user.get("id") or user.get("email") or "")
        return {
            **user,
            "id": uid,
            "workspace_id": uid,
            "workspace_name": user.get("name") or user.get("email"),
        }

    async def get_transcript(self, transcript_id: str) -> dict[str, Any]:
        data = await self._graphql(
            _TRANSCRIPT_QUERY,
            {"id": transcript_id},
            operation="transcript.get",
        )
        transcript = data.get("transcript")
        return _normalise_transcript(transcript) if isinstance(transcript, dict) else {}

    async def list_transcripts(
        self,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        items, next_skip = await self.list_transcripts_graphql(
            limit=limit, skip=offset, from_date=start,
        )
        total_hint = (next_skip + 1) if next_skip is not None else offset + len(items)
        return items, next_skip, total_hint

    async def list_transcripts_graphql(
        self,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        skip: int = 0,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        eff_limit = max(1, min(_MAX_PAGE_SIZE, int(limit or _DEFAULT_PAGE_SIZE)))
        variables: dict[str, Any] = {"limit": eff_limit, "skip": max(0, skip)}
        if from_date:
            variables["fromDate"] = from_date
        if to_date:
            variables["toDate"] = to_date
        data = await self._graphql(
            _TRANSCRIPTS_QUERY,
            variables,
            operation="transcripts.list",
        )
        raw_items = data.get("transcripts")
        items = [
            _normalise_transcript(t)
            for t in raw_items
            if isinstance(t, dict)
        ] if isinstance(raw_items, list) else []
        next_skip = skip + eff_limit if len(items) >= eff_limit and items else None
        return items, next_skip


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _normalise_transcript(t: dict[str, Any]) -> dict[str, Any]:
    out = dict(t)
    date_value = out.get("date")
    if "dateTime" not in out and date_value is not None:
        iso = _epoch_millis_to_iso(date_value)
        if iso is not None:
            out["dateTime"] = iso
    if "organizerEmail" not in out and out.get("organizer_email") is not None:
        out["organizerEmail"] = out.get("organizer_email")
    if "transcript_url" in out and "meetingLink" not in out:
        out["meetingLink"] = out.get("transcript_url")
    return out


def _epoch_millis_to_iso(value: Any) -> str | None:
    try:
        millis = float(value)
    except (TypeError, ValueError):
        return value if isinstance(value, str) else None
    if millis <= 0:
        return None
    # Fireflies `date` is epoch milliseconds.
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc).isoformat()


def _graphql_error_code(errors: Any) -> str:
    if isinstance(errors, list):
        for err in errors:
            if not isinstance(err, dict):
                continue
            ext = err.get("extensions")
            if isinstance(ext, dict):
                raw = str(ext.get("code") or "").lower()
                if raw in {"too_many_requests", "rate_limited", "rate_limit"}:
                    return "fireflies_api_rate_limited"
                if raw in {"unauthorized", "forbidden"}:
                    return "fireflies_api_unauthorized"
    return "fireflies_api_error"


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> FirefliesApiError:
    status = response.status_code
    if status in (401, 403):
        return FirefliesApiError(
            f"fireflies {status}: API token rejected or insufficient scope",
            code="fireflies_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return FirefliesApiError(
            "fireflies 404: transcript/resource not found or not visible",
            code="fireflies_api_not_found",
            context={"http_status": 404, "path": path},
        )
    if status == 429:
        return FirefliesApiError(
            "fireflies rate limit (429), retry budget exhausted",
            code="fireflies_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return FirefliesApiError(
        f"fireflies returned {status}",
        code="fireflies_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["FirefliesClient"]
