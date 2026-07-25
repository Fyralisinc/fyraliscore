"""services/ingest/integrations/slack/client.py — outbound Slack Web API client.

Thin async wrapper around Slack Web API operations, resolving the exact
installation's token through the IN-08 secret store. Every HTTP attempt runs
through the universal ProviderTransport; the client only classifies Slack
responses and never sleeps or retries locally.

Becomes the substrate for Slack-outbound Acts in a follow-up (IN-10).
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog

from lib.shared.errors import CompanyOSError
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
from services.ingest.integrations.secret_cache import (
    SecretValueCache,
    coerce_secret_text,
)


log = structlog.get_logger("integrations.slack.client")


_SLACK_API_BASE = "https://slack.com/api"
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_MAX_INLINE_RETRY_AFTER_S = 30.0


def _default_wall_budget_s() -> float:
    """Wall-clock retry budget for one Slack call.

    Explicit override: `SLACK_RETRY_WALL_BUDGET_S` (seconds). Otherwise it is
    derived from `SLACK_API_TIER`. This budget bounds the complete operation;
    it does not authorize sleeping through a long provider cooldown. The
    independent inline Retry-After limit below controls that decision.
    """
    raw = os.environ.get("SLACK_RETRY_WALL_BUDGET_S", "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return 75.0 if os.environ.get("SLACK_API_TIER", "3").strip() == "1" else 30.0


def _default_max_inline_retry_after_s() -> float:
    """Longest Slack cooldown that may retain a worker.

    Longer provider-directed waits surface as ``RetryLater`` so the caller can
    persist ``next_attempt_at`` and release the worker. Operators may lower
    this independently of the total retry wall budget.
    """
    raw = os.environ.get("SLACK_MAX_INLINE_RETRY_AFTER_S", "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return _DEFAULT_MAX_INLINE_RETRY_AFTER_S


class SlackApiError(CompanyOSError):
    """A Slack Web API call returned `ok=false` or exhausted its
    retry budget. The structured context carries `endpoint`,
    `slack_error` (when present), and `attempts`."""
    default_code = "slack_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        recoverable: bool | None = None,
        **context: Any,
    ) -> None:
        super().__init__(message, **context)
        if code is not None:
            self._code = code
        if recoverable is not None:
            self._recoverable = recoverable


class SlackClient:
    """Per-installation Slack Web API client.

    Each instance is bound to a single tenant + installation. Multiple
    callers can share an instance; the embedded `httpx.AsyncClient`
    is lazy-initialised and closed by the GC (or via explicit
    `await self.aclose()`).
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        secret_store: Any,
        tenant_id: UUID,
        installation_row_id: UUID,
        team_id: str,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        wall_budget_s: float | None = None,
        max_inline_retry_after_s: float | None = None,
        base_url: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._api_base = (base_url or endpoint("slack_api")).rstrip("/")
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._installation_row_id = installation_row_id
        self._team_id = team_id
        self._max_attempts = max_attempts
        # Tier-aware default (env) unless an explicit budget is passed.
        self._wall_budget_s = (
            wall_budget_s if wall_budget_s is not None else _default_wall_budget_s()
        )
        self._max_inline_retry_after_s = (
            max_inline_retry_after_s
            if max_inline_retry_after_s is not None
            else _default_max_inline_retry_after_s()
        )
        self._bot_token_cache = SecretValueCache()
        self._bot_token_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = http_client
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=http_client is not None or base_url is not None,
        )
        self._provider = ProviderRequestBinding(
            source="slack",
            tenant_id=str(tenant_id),
            installation_id=str(installation_row_id),
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
        )

    async def _resolve_token(self) -> str:
        """Return the bearer token `_call` authenticates with.

        Overridable seam: the base client authenticates as the workspace
        BOT (`slack_bot_token:{team}`); `SlackUserClient` overrides this to
        resolve a per-USER token (`slack_user_token:{team}:{user}`) so DM
        reads run under the consenting user's grant. Keeping `_call`
        token-agnostic means the rate-limit / retry / pagination machinery
        is shared verbatim between the bot and user paths.
        """
        return await self._resolve_bot_token()

    async def _resolve_bot_token(self) -> str:
        return await self._resolve_labeled_secret(
            label=f"slack_bot_token:{self._team_id}",
            cache=self._bot_token_cache,
            lock=self._bot_token_lock,
            missing_error=lambda: SlackApiError(
                "bot token not found for installation",
                endpoint=None,
                tenant=str(self._tenant_id),
            ),
        )

    async def _resolve_labeled_secret(
        self,
        *,
        label: str,
        cache: SecretValueCache,
        lock: asyncio.Lock,
        missing_error: Any,
    ) -> str:
        value = cache.get_if_fresh()
        if value is not None:
            return value
        async with lock:
            value = cache.get_if_fresh()
            if value is not None:
                return value
            row = await self._pool.fetchrow(
                """
                SELECT id::text AS id
                  FROM encrypted_secrets
                 WHERE tenant_id = $1
                   AND label = $2
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                self._tenant_id,
                label,
            )
            if row is None:
                raise missing_error()
            plaintext = await self._secret_store.get(
                row["id"], tenant_id=self._tenant_id,
            )
            return cache.set(coerce_secret_text(plaintext))

    def _httpx(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _call(
        self,
        endpoint: str,
        *,
        method: str = "POST",
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue one transport-owned Slack operation."""
        token = await self._resolve_token()
        url = f"{self._api_base}/{endpoint}"
        client = self._httpx()
        headers = {"Authorization": f"Bearer {token}"}

        async def _once() -> dict[str, Any]:
            try:
                if method == "POST":
                    r = await client.post(url, headers=headers, json=json_body)
                else:
                    r = await client.get(url, headers=headers, params=params)
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Slack request timed out",
                    source="slack",
                    operation=endpoint,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Slack transport error",
                    source="slack",
                    operation=endpoint,
                    error_type=type(exc).__name__,
                ) from exc

            if r.status_code == 429:
                raise ProviderRateLimited(
                    "Slack rate limit",
                    retry_after_seconds=parse_retry_after(
                        r.headers.get("Retry-After"),
                    ),
                    status_code=429,
                    header_parser_id="http.retry_after",
                    endpoint=endpoint,
                )

            if r.status_code >= 500:
                raise ProviderTransientError(
                    f"Slack returned HTTP {r.status_code}",
                    source="slack",
                    operation=endpoint,
                    endpoint=endpoint,
                    http_status=r.status_code,
                )
            if r.status_code >= 400:
                code = (
                    "slack_api_unauthorized"
                    if r.status_code in (401, 403)
                    else "slack_api_error"
                )
                raise SlackApiError(
                    f"Slack returned HTTP {r.status_code}",
                    code=code,
                    endpoint=endpoint,
                    http_status=r.status_code,
                )
            try:
                data = r.json()
            except ValueError as exc:
                raise SlackApiError(
                    "Slack returned invalid JSON",
                    endpoint=endpoint,
                    http_status=r.status_code,
                ) from exc
            if data.get("ok") is True:
                return data
            # Non-ok responses are not retried (Slack error codes are
            # generally permanent for a given input).
            raise SlackApiError(
                "Slack API returned ok=false",
                endpoint=endpoint,
                slack_error=data.get("error"),
            )

        return await self._provider.execute(
            endpoint,
            _once,
            quota_dimensions={"workspace": self._team_id},
        )

    # -----------------------------------------------------------------
    # Endpoint wrappers
    # -----------------------------------------------------------------

    async def chat_post_message(
        self, *, channel: str, text: str, **extra: Any,
    ) -> dict[str, Any]:
        return await self._call(
            "chat.postMessage",
            json_body={"channel": channel, "text": text, **extra},
        )

    async def users_info(self, user_id: str) -> dict[str, Any]:
        return await self._call(
            "users.info", method="GET", params={"user": user_id},
        )

    async def conversations_info(self, channel_id: str) -> dict[str, Any]:
        return await self._call(
            "conversations.info",
            method="GET",
            params={"channel": channel_id},
        )

    # -----------------------------------------------------------------
    # Backfill read surface (M6.5) — mirrors MockSlackClient so the
    # planner / fetcher / reconciler exercise the real Web API the same
    # way they exercise the in-process mock.
    # -----------------------------------------------------------------

    async def conversations_list(self) -> list[dict[str, Any]]:
        """List the workspace's channels (planner shard source).

        Cursor-paginated to completion, so a workspace with >1000
        channels is not silently truncated. Public channels by default;
        set SLACK_BACKFILL_INCLUDE_PRIVATE=1 (requires the app's
        groups:read scope) to also enumerate private channels, or
        SLACK_BACKFILL_CHANNEL_TYPES to set the comma-separated types
        explicitly. Each entry carries at least `id` and `name`;
        `team_id` is injected for mock-client parity.
        """
        import os

        types = os.environ.get(
            "SLACK_BACKFILL_CHANNEL_TYPES", "public_channel"
        )
        if (
            os.environ.get("SLACK_BACKFILL_INCLUDE_PRIVATE", "") == "1"
            and "private_channel" not in types
        ):
            types = f"{types},private_channel"

        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"types": types, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = await self._call(
                "conversations.list", method="GET", params=params,
            )
            for c in data.get("channels") or []:
                if isinstance(c, dict):
                    out.append({
                        "id": c.get("id"),
                        "name": c.get("name"),
                        "team_id": c.get("context_team_id") or self._team_id,
                    })
            cursor = (
                (data.get("response_metadata") or {}).get("next_cursor")
                or None
            )
            if not cursor:
                break
        return out

    async def conversations_history(
        self,
        *,
        channel: str,
        cursor: str | None = None,
        oldest: str | None = None,
        limit: int | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One page of a channel's messages. Returns
        `(messages, next_cursor)` — `next_cursor` is None when Slack
        reports no further page (`response_metadata.next_cursor` empty).
        """
        params: dict[str, Any] = {"channel": channel}
        if cursor:
            params["cursor"] = cursor
        if oldest is not None:
            params["oldest"] = oldest
        if limit is not None:
            params["limit"] = limit
        data = await self._call(
            "conversations.history", method="GET", params=params,
        )
        messages = data.get("messages") or []
        next_cursor = (
            (data.get("response_metadata") or {}).get("next_cursor") or None
        )
        return messages, next_cursor


class SlackUserClient(SlackClient):
    """Per-USER Slack Web API client (xoxp user token) for DM ingestion.

    Human↔human direct messages and group DMs can NEVER be read by a bot
    token — only by a USER token granted by a consenting participant. This
    client authenticates as that user (resolving `slack_user_token:{team}:
    {user}` from the secret store) and enumerates the user's own im/mpim
    conversations.

    Reuses `SlackClient._call` (rate-limit + 429/transport retry) and
    `conversations_history` verbatim; overrides only (a) token resolution
    (`_resolve_token`) and (b) `conversations_list`, which here requests
    `types=im,mpim` and maps Slack's conversation objects to the planner's
    `{id, channel_type, user, name, team_id}` shape (the BOT client's
    `conversations_list` requests public channels — a different surface, so
    it is overridden rather than parameterised to keep each path's intent
    explicit).

    Per-user grain: one instance per (tenant, team_id, user_id). The
    `user_id` is the CONSENTING user whose token we hold; for an `im` the
    counterpart is carried on each conversation's `user` field.
    """

    def __init__(self, *, user_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._user_id = user_id
        self._user_token_cache = SecretValueCache()
        self._user_token_lock = asyncio.Lock()

    async def _resolve_token(self) -> str:
        return await self._resolve_labeled_secret(
            label=f"slack_user_token:{self._team_id}:{self._user_id}",
            cache=self._user_token_cache,
            lock=self._user_token_lock,
            missing_error=lambda: SlackApiError(
                "user token not found for DM install",
                endpoint=None,
                tenant=str(self._tenant_id),
                team_id=self._team_id,
                user_id=self._user_id,
            ),
        )

    async def conversations_list(  # type: ignore[override]
        self, *, types: str = "im,mpim",
    ) -> list[dict[str, Any]]:
        """Enumerate the consenting user's DM + group-DM conversations
        (planner shard source for `slack_dm_window`).

        Cursor-paginated to completion. Each entry carries `id`,
        `channel_type` ("im"/"mpim", derived from Slack's `is_im`/`is_mpim`
        flags), the `user` counterpart (im only), `name`, and `team_id`.
        """
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"types": types, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = await self._call(
                "conversations.list", method="GET", params=params,
            )
            for c in data.get("channels") or []:
                if not isinstance(c, dict):
                    continue
                ctype = (
                    "im" if c.get("is_im")
                    else "mpim" if c.get("is_mpim")
                    # Mock/Provider Lab convenience: an explicit channel_type.
                    else c.get("channel_type")
                )
                out.append({
                    "id": c.get("id"),
                    "channel_type": ctype,
                    "user": c.get("user"),  # im counterpart; None for mpim
                    "name": c.get("name"),
                    "team_id": c.get("context_team_id") or self._team_id,
                })
            cursor = (
                (data.get("response_metadata") or {}).get("next_cursor")
                or None
            )
            if not cursor:
                break
        return out


__all__ = ["SlackClient", "SlackUserClient", "SlackApiError"]
