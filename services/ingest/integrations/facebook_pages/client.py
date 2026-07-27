"""Meta Graph API client for Facebook Page / Messenger conversations."""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from lib.integrations.endpoints import endpoint
from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.provider_transport import (
    ProviderPermanentError,
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


_DEFAULT_GRAPH_VERSION = "v23.0"
FACEBOOK_PAGES_WEBHOOK_FIELDS = ("messages", "message_echoes")


class FacebookGraphAuthError(ProviderPermanentError):
    """Meta Graph OAuth code 190 without retaining the provider payload."""

    default_code = "facebook_graph_access_token_invalid"

    def __init__(
        self,
        *,
        operation: str,
        http_status: int,
        graph_error_subcode: int | None,
    ) -> None:
        super().__init__(
            "Facebook Pages Graph access token is invalid",
            source="facebook_pages",
            operation=operation,
            http_status=http_status,
            graph_error_code=190,
            graph_error_subcode=graph_error_subcode,
        )
        self.graph_error_code = 190
        self.graph_error_subcode = graph_error_subcode


def graph_api_version() -> str:
    raw = os.environ.get("FACEBOOK_GRAPH_API_VERSION", _DEFAULT_GRAPH_VERSION).strip()
    return raw if raw.startswith("v") else f"v{raw}"


def graph_api_base_url() -> str:
    base = endpoint("facebook_graph_api").rstrip("/")
    version = graph_api_version()
    if base.rsplit("/", 1)[-1].startswith("v"):
        return base
    return f"{base}/{version}"


class FacebookPagesClient:
    """Small async wrapper around the Graph endpoints used by v1."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        access_token: str | None = None,
        page_access_token_ref: str | None = None,
        pool: asyncpg.Pool | None = None,
        secret_store: Any = None,
        tenant_id: UUID | None = None,
        http_client: httpx.AsyncClient | None = None,
        installation_row_id: UUID | str | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        require_tenant_installation: bool = True,
    ) -> None:
        self._base_url = (base_url or graph_api_base_url()).rstrip("/")
        self._access_token = access_token
        self._page_access_token_ref = page_access_token_ref
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._installation_row_id = (
            UUID(str(installation_row_id))
            if installation_row_id is not None
            else None
        )
        self._http = http_client or httpx.AsyncClient(timeout=30.0)
        self._owns_http = http_client is None
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                http_client is not None or base_url is not None
            ),
        )
        self._provider = ProviderRequestBinding(
            source="facebook_pages",
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

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _token(self, override: str | None = None) -> str:
        token, _ = await self._token_with_ref(
            override,
            operation="access_token.resolve",
        )
        return token

    async def _token_with_ref(
        self,
        override: str | None,
        *,
        operation: str,
    ) -> tuple[str, str | None]:
        if override:
            return override, None
        if self._access_token:
            return self._access_token, None
        if not self._page_access_token_ref or self._secret_store is None:
            raise SecretStoreError(
                "facebook_pages access token unavailable",
                reason="missing_access_token_ref",
            )
        if (
            self._pool is not None
            and self._tenant_id is not None
            and self._installation_row_id is not None
        ):
            from services.ingest.integrations.facebook_pages.token_lifecycle import (
                page_access_token_for_request,
            )

            return await page_access_token_for_request(
                self._pool,
                self._secret_store,
                tenant_id=self._tenant_id,
                installation_row_id=self._installation_row_id,
                operation=operation,
            )
        try:
            raw = await self._secret_store.get(
                self._page_access_token_ref,
                tenant_id=self._tenant_id,
            )
        except (SecretNotFoundError, SecretStoreError, ValueError):
            raise
        token = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return token, self._page_access_token_ref

    async def _request(
        self,
        method: str,
        path: str,
        *,
        access_token: str | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        operation: str,
    ) -> dict[str, Any]:
        qs = dict(params or {})
        token, token_ref = await self._token_with_ref(
            access_token,
            operation=operation,
        )
        qs["access_token"] = token
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            return await self._execute_json(
                method,
                url,
                params=qs,
                data=data,
                operation=operation,
            )
        except FacebookGraphAuthError as exc:
            # User-token operations (for example /me/accounts during recovery)
            # must not recursively attempt Page-token recovery.
            if (
                access_token is not None
                or token_ref is None
                or self._pool is None
                or self._secret_store is None
                or self._tenant_id is None
                or self._installation_row_id is None
            ):
                raise
            from services.ingest.integrations.facebook_pages.token_lifecycle import (
                page_access_token_for_request,
                recover_page_access_token,
                schedule_page_token_recovery,
            )

            schedule = await schedule_page_token_recovery(
                self._pool,
                tenant_id=self._tenant_id,
                installation_row_id=self._installation_row_id,
                expected_page_token_ref=token_ref,
                graph_error_subcode=exc.graph_error_subcode,
            )
            if schedule.stale_page_token_ref:
                replacement, _ = await page_access_token_for_request(
                    self._pool,
                    self._secret_store,
                    tenant_id=self._tenant_id,
                    installation_row_id=self._installation_row_id,
                    operation=operation,
                )
            else:
                replacement = await recover_page_access_token(
                    self._pool,
                    self._secret_store,
                    tenant_id=self._tenant_id,
                    installation_row_id=self._installation_row_id,
                    operation=operation,
                )
            qs["access_token"] = replacement
            # This is a separate, fully metered ProviderTransport execution.
            # A second auth failure escapes; no recursive recovery loop exists.
            return await self._execute_json(
                method,
                url,
                params=qs,
                data=data,
                operation=operation,
            )

    async def _execute_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None = None,
        operation: str,
    ) -> dict[str, Any]:
        async def _once() -> dict[str, Any]:
            try:
                response = await self._http.request(
                    method,
                    url,
                    params=params,
                    data=data,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Facebook Pages request timed out",
                    source="facebook_pages",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Facebook Pages transport error",
                    source="facebook_pages",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            if response.status_code == 429:
                raise ProviderRateLimited(
                    "Facebook Pages Graph rate limit",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    source="facebook_pages",
                    operation=operation,
                )
            if response.status_code >= 500:
                raise ProviderTransientError(
                    f"Facebook Pages returned HTTP {response.status_code}",
                    source="facebook_pages",
                    operation=operation,
                    http_status=response.status_code,
                )
            payload: Any = None
            try:
                payload = response.json()
            except ValueError:
                payload = None
            if response.status_code // 100 != 2:
                graph_error = (
                    payload.get("error")
                    if isinstance(payload, dict)
                    and isinstance(payload.get("error"), dict)
                    else {}
                )
                if graph_error.get("code") == 190:
                    raw_subcode = graph_error.get("error_subcode")
                    subcode = (
                        raw_subcode
                        if isinstance(raw_subcode, int)
                        and not isinstance(raw_subcode, bool)
                        else None
                    )
                    raise FacebookGraphAuthError(
                        operation=operation,
                        http_status=response.status_code,
                        graph_error_subcode=subcode,
                    )
                raise ProviderPermanentError(
                    f"Facebook Pages returned HTTP {response.status_code}",
                    source="facebook_pages",
                    operation=operation,
                    http_status=response.status_code,
                )
            if payload is None:
                raise ProviderTransientError(
                    "Facebook Pages returned malformed JSON",
                    source="facebook_pages",
                    operation=operation,
                )
            if not isinstance(payload, dict):
                raise ProviderTransientError(
                    "Facebook Pages response was not a JSON object",
                    source="facebook_pages",
                    operation=operation,
                )
            return payload

        return await self._provider.execute(operation, _once)

    async def exchange_code(
        self,
        *,
        code: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
    ) -> dict[str, Any]:
        return await self._execute_json(
            "GET",
            f"{self._base_url}/oauth/access_token",
            params={
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
            operation="oauth.token.exchange",
        )

    async def exchange_long_lived_user_token(
        self,
        *,
        short_lived_user_access_token: str,
        client_id: str,
        client_secret: str,
    ) -> dict[str, Any]:
        """Exchange a valid short-lived User token for Meta's long-lived form."""

        return await self._execute_json(
            "GET",
            f"{self._base_url}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "fb_exchange_token": short_lived_user_access_token,
            },
            operation="oauth.user_token.extend",
        )

    async def list_pages(self, user_access_token: str) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            payload = await self._request(
                "GET",
                "/me/accounts",
                access_token=user_access_token,
                params={
                    "fields": "id,name,access_token,tasks",
                    "limit": 100,
                    **({"after": after} if after else {}),
                },
                operation="pages.list",
            )
            pages.extend(p for p in payload.get("data", []) if isinstance(p, dict))
            after = _next_after(payload)
            if not after:
                return pages

    async def subscribe_page(
        self,
        *,
        page_id: str,
        page_access_token: str,
        fields: tuple[str, ...] = FACEBOOK_PAGES_WEBHOOK_FIELDS,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/{page_id}/subscribed_apps",
            access_token=page_access_token,
            data={"subscribed_fields": ",".join(fields)},
            operation="pages.subscribe",
        )

    async def list_conversations(
        self,
        *,
        page_id: str,
        after: str | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None]:
        payload = await self._request(
            "GET",
            f"/{page_id}/conversations",
            params={
                "fields": "id,updated_time,participants,message_count",
                "limit": limit,
                **({"after": after} if after else {}),
            },
            operation="conversations.list",
        )
        data = [c for c in payload.get("data", []) if isinstance(c, dict)]
        return data, _next_after(payload)

    async def list_messages(
        self,
        *,
        conversation_id: str,
        after: str | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None]:
        payload = await self._request(
            "GET",
            f"/{conversation_id}/messages",
            params={
                "fields": (
                    "id,created_time,from,to,message,attachments,shares,sticker"
                ),
                "limit": limit,
                **({"after": after} if after else {}),
            },
            operation="messages.list",
        )
        data = [m for m in payload.get("data", []) if isinstance(m, dict)]
        return data, _next_after(payload)

    async def upsert_conversation_state(
        self,
        *,
        installation_id: UUID,
        tenant_id: UUID,
        page_id: str,
        conversation: dict[str, Any],
    ) -> None:
        if self._pool is None:
            return
        conversation_id = conversation.get("id")
        if not isinstance(conversation_id, str) or not conversation_id:
            return
        participants = _participant_ids(conversation)
        updated_time = _parse_graph_time(conversation.get("updated_time"))
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO facebook_page_conversations (
                    facebook_page_installation_id, tenant_id, page_id,
                    conversation_id, participant_ids, updated_time,
                    message_count, state, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,'active', now())
                ON CONFLICT (facebook_page_installation_id, conversation_id)
                DO UPDATE SET
                    participant_ids = EXCLUDED.participant_ids,
                    updated_time = COALESCE(EXCLUDED.updated_time, facebook_page_conversations.updated_time),
                    message_count = GREATEST(
                        facebook_page_conversations.message_count,
                        EXCLUDED.message_count
                    ),
                    state = 'active',
                    updated_at = now()
                """,
                installation_id,
                tenant_id,
                page_id,
                conversation_id,
                participants,
                updated_time,
                _safe_int(conversation.get("message_count")),
            )

    async def mark_conversation_exhausted(
        self,
        *,
        installation_id: UUID,
        conversation_id: str,
        oldest_message_at: datetime | None,
        newest_message_at: datetime | None,
        message_count: int,
        reason: str,
    ) -> None:
        if self._pool is None:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE facebook_page_conversations
                   SET state = 'exhausted',
                       oldest_message_at = COALESCE($3, oldest_message_at),
                       newest_message_at = COALESCE($4, newest_message_at),
                       message_count = GREATEST(message_count, $5),
                       exhausted_at = now(),
                       exhausted_reason = $6,
                       backfill_cursor = NULL,
                       updated_at = now()
                 WHERE facebook_page_installation_id = $1
                   AND conversation_id = $2
                """,
                installation_id,
                conversation_id,
                oldest_message_at,
                newest_message_at,
                message_count,
                reason,
            )


def _next_after(payload: dict[str, Any]) -> str | None:
    paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
    cursors = paging.get("cursors") if isinstance(paging.get("cursors"), dict) else {}
    after = cursors.get("after")
    return after if isinstance(after, str) and after else None


def _participant_ids(conversation: dict[str, Any]) -> list[str]:
    participants = conversation.get("participants")
    data = participants.get("data") if isinstance(participants, dict) else participants
    ids: list[str] = []
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        pid = item.get("id")
        if isinstance(pid, str) and pid:
            ids.append(pid)
    return ids


def _parse_graph_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            return None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "FacebookGraphAuthError",
    "FacebookPagesClient",
    "graph_api_base_url",
    "graph_api_version",
]
