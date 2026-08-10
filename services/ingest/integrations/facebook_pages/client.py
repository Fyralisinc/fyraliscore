"""Meta Graph API client for Facebook Page / Messenger conversations."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from lib.shared.errors import SecretNotFoundError, SecretStoreError

_DEFAULT_GRAPH_VERSION = "v23.0"
FACEBOOK_PAGES_WEBHOOK_FIELDS = ("messages", "message_echoes")


def graph_api_version() -> str:
    raw = os.environ.get("FACEBOOK_GRAPH_API_VERSION", _DEFAULT_GRAPH_VERSION).strip()
    return raw if raw.startswith("v") else f"v{raw}"


def graph_api_base_url() -> str:
    base = os.environ.get("FACEBOOK_GRAPH_API_BASE_URL", "").strip()
    if not base:
        synthetic_base = os.environ.get("SYNTHETIC_SOURCE_API_BASE", "").strip()
        base = (
            f"{synthetic_base.rstrip('/')}/facebook"
            if synthetic_base
            else "https://graph.facebook.com"
        )
    base = base.rstrip("/")
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
    ) -> None:
        self._base_url = (base_url or graph_api_base_url()).rstrip("/")
        self._access_token = access_token
        self._page_access_token_ref = page_access_token_ref
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._http = http_client or httpx.AsyncClient(timeout=30.0)
        self._owns_http = http_client is None

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _token(self, override: str | None = None) -> str:
        if override:
            return override
        if self._access_token:
            return self._access_token
        if not self._page_access_token_ref or self._secret_store is None:
            raise SecretStoreError(
                "facebook_pages access token unavailable",
                reason="missing_access_token_ref",
            )
        try:
            raw = await self._secret_store.get(
                self._page_access_token_ref,
                tenant_id=self._tenant_id,
            )
        except (SecretNotFoundError, SecretStoreError, ValueError):
            raise
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        access_token: str | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        qs = dict(params or {})
        qs["access_token"] = await self._token(access_token)
        response = await self._http.request(
            method,
            f"{self._base_url}/{path.lstrip('/')}",
            params=qs,
            data=data,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    async def exchange_code(
        self,
        *,
        code: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
    ) -> dict[str, Any]:
        response = await self._http.get(
            f"{self._base_url}/oauth/access_token",
            params={
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

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
                    updated_time = COALESCE(
                        EXCLUDED.updated_time,
                        facebook_page_conversations.updated_time
                    ),
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


__all__ = ["FacebookPagesClient", "graph_api_base_url", "graph_api_version"]
