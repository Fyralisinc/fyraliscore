"""Narrow client for the Instagram API with Instagram Login.

Only account data and message metadata flow through this client. Tokens stay in
the secret store and never appear in errors or structured logs.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import httpx

from lib.shared.errors import InstagramApiError
from services.ingest.integrations.secret_cache import SecretValueCache


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 100
_DEFAULT_GRAPH_VERSION = "v24.0"
_RATE_LIMIT_CODES = {4, 17, 32, 613}


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _meta_error(response: httpx.Response) -> dict[str, Any]:
    value = _safe_json(response).get("error")
    return value if isinstance(value, dict) else {}


def _is_rate_limited(response: httpx.Response) -> bool:
    if response.status_code == 429:
        return True
    error = _meta_error(response)
    try:
        return int(error.get("code")) in _RATE_LIMIT_CODES
    except (TypeError, ValueError):
        return False


def _retry_after(response: httpx.Response, attempt: int) -> float:
    raw = response.headers.get("Retry-After")
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return min(30.0, 0.5 * (2 ** max(0, attempt - 1)))


def _error_code(response: httpx.Response) -> str:
    if _is_rate_limited(response):
        return "instagram_api_rate_limited"
    if response.status_code in (401, 403):
        return "instagram_api_unauthorized"
    if response.status_code == 404:
        return "instagram_api_not_found"
    return "instagram_api_error"


def _api_error(response: httpx.Response, path: str) -> InstagramApiError:
    error = _meta_error(response)
    message = str(error.get("message") or "Meta Graph API request failed")[:300]
    return InstagramApiError(
        message,
        code=_error_code(response),
        context={
            "http_status": response.status_code,
            "path": path,
            "retry_after": response.headers.get("Retry-After"),
            "meta_error_code": error.get("code"),
            "meta_error_subcode": error.get("error_subcode") or error.get("subcode"),
        },
    )


def _paging_after(value: Any) -> str | None:
    paging = value.get("paging") if isinstance(value, dict) else {}
    if not isinstance(paging, dict) or not paging.get("next"):
        return None
    cursors = paging.get("cursors")
    after = cursors.get("after") if isinstance(cursors, dict) else None
    return str(after) if after else None


class InstagramClient:
    """Instagram Login API client used by install, history, and recovery."""

    def __init__(
        self,
        *,
        base_url: str = "https://graph.instagram.com",
        access_token: str | None = None,
        secret_store: Any | None = None,
        secret_ref: str | None = None,
        tenant_id: UUID | None = None,
        graph_version: str | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._graph_version = (
            graph_version
            or os.environ.get("INSTAGRAM_GRAPH_VERSION")
            or os.environ.get("META_GRAPH_VERSION")
            or _DEFAULT_GRAPH_VERSION
        ).strip("/")
        self._secret_store = secret_store
        self._secret_ref = secret_ref
        self._tenant_id = tenant_id
        self._token_cache = SecretValueCache(preset=access_token)
        self._token_lock = asyncio.Lock()
        self._http = http_client
        self._owns_client = http_client is None

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
        return await self._token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: InstagramApiError(
                "Instagram access token is unavailable",
                code="instagram_api_unauthorized",
            ),
        )

    def _url(self, path: str) -> str:
        clean = path if path.startswith("/") else f"/{path}"
        return f"{self._base_url}/{self._graph_version}{clean}"

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._token()
        attempts = max(1, int(os.environ.get("INSTAGRAM_RL_MAX_ATTEMPTS", "4")))
        client = self._httpx()
        for attempt in range(1, attempts + 1):
            try:
                response = await client.request(
                    method,
                    self._url(path),
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    params=params,
                    json=json_body,
                )
            except httpx.TransportError as exc:
                if attempt < attempts:
                    await asyncio.sleep(min(30.0, 0.5 * (2 ** (attempt - 1))))
                    continue
                raise InstagramApiError(
                    "transport error calling Meta Graph API",
                    code="instagram_api_error",
                    context={"path": path, "error_type": type(exc).__name__},
                ) from exc

            retryable = _is_rate_limited(response) or response.status_code in {500, 502, 503, 504}
            if retryable and attempt < attempts:
                await asyncio.sleep(_retry_after(response, attempt))
                continue
            if response.status_code // 100 != 2:
                raise _api_error(response, path)
            body = _safe_json(response)
            if not body:
                raise InstagramApiError(
                    "Meta Graph API response was not a JSON object",
                    code="instagram_api_error",
                    context={"path": path},
                )
            return body
        raise AssertionError("Instagram request retry loop fell through")

    async def validate_account(self, ig_business_account_id: str | None = None) -> dict[str, Any]:
        account = (ig_business_account_id or "me").strip()
        return await self._request("GET", f"/{account}", params={"fields": "id,username,name"})

    async def list_conversations(
        self,
        *,
        ig_business_account_id: str,
        limit: int = _DEFAULT_PAGE_SIZE,
        after: str | None = None,
        user_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        params: dict[str, Any] = {
            "platform": "instagram",
            "limit": max(1, min(_MAX_PAGE_SIZE, int(limit))),
            "fields": "id,updated_time,participants{id,username,name}",
        }
        if after:
            params["after"] = after
        if user_id:
            params["user_id"] = user_id
        body = await self._request("GET", f"/{ig_business_account_id}/conversations", params=params)
        data = body.get("data")
        records = [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return records, _paging_after(body)

    async def find_conversation(
        self,
        *,
        ig_business_account_id: str,
        instagram_scoped_user_id: str,
    ) -> dict[str, Any] | None:
        records, _ = await self.list_conversations(
            ig_business_account_id=ig_business_account_id,
            user_id=instagram_scoped_user_id,
            limit=10,
        )
        return records[0] if records else None

    async def list_conversation_messages(
        self,
        *,
        conversation_id: str,
        limit: int = _DEFAULT_PAGE_SIZE,
        after: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        size = max(1, min(_MAX_PAGE_SIZE, int(limit)))
        expansion = (
            f"messages.limit({size})"
            + (f".after({after})" if after else "")
            + "{id,created_time,from,to,message,attachments,is_unsupported}"
        )
        body = await self._request("GET", f"/{conversation_id}", params={"fields": expansion})
        messages = body.get("messages") if isinstance(body.get("messages"), dict) else {}
        data = messages.get("data") if isinstance(messages, dict) else []
        records = [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return records, _paging_after(messages)

    async def get_message(self, message_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/{message_id}",
            params={"fields": "id,created_time,from,to,message,attachments,is_unsupported"},
        )

    async def subscribe_webhooks(
        self,
        *,
        ig_business_account_id: str,
        fields: list[str],
    ) -> None:
        body = await self._request(
            "POST",
            f"/{ig_business_account_id}/subscribed_apps",
            params={"subscribed_fields": ",".join(sorted(set(fields)))},
        )
        if body.get("success") is not True:
            raise InstagramApiError(
                "Meta did not confirm Instagram webhook subscription",
                code="instagram_api_error",
                context={"path": f"/{ig_business_account_id}/subscribed_apps"},
            )

    async def list_webhook_subscriptions(self, *, ig_business_account_id: str) -> list[dict[str, Any]]:
        body = await self._request("GET", f"/{ig_business_account_id}/subscribed_apps")
        data = body.get("data")
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    async def get_user_profile(self, instagram_scoped_user_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/{instagram_scoped_user_id}",
            params={"fields": "name,username,is_verified_user,is_user_follow_business,is_business_follow_user"},
        )


__all__ = ["InstagramClient"]
