"""services/ingest/integrations/slack/client.py — outbound Slack Web API client.

Thin async wrapper around three Slack Web API endpoints (`chat.postMessage`,
`users.info`, `conversations.info`), resolving the per-installation bot
token through the IN-08 secret store. Honors Slack's 429 `Retry-After`
header with a bounded retry budget; transport errors retry with
exponential backoff within the same budget.

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


log = structlog.get_logger("integrations.slack.client")


_SLACK_API_BASE = "https://slack.com/api"
_DEFAULT_MAX_ATTEMPTS = 3


def _default_wall_budget_s() -> float:
    """Wall-clock retry budget for one Slack call.

    Explicit override: `SLACK_RETRY_WALL_BUDGET_S` (seconds). Otherwise it is
    derived from `SLACK_API_TIER` — a Tier-1 (non-Marketplace, post-2025-05-29)
    429 carries a ~60s `Retry-After`, so the budget MUST exceed one such wait
    or the 429 handler gives up before it can retry. Tier 2–4 Retry-Afters are
    small, so 30s is ample.
    """
    raw = os.environ.get("SLACK_RETRY_WALL_BUDGET_S", "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return 75.0 if os.environ.get("SLACK_API_TIER", "3").strip() == "1" else 30.0


class SlackApiError(CompanyOSError):
    """A Slack Web API call returned `ok=false` or exhausted its
    retry budget. The structured context carries `endpoint`,
    `slack_error` (when present), and `attempts`."""
    default_code = "slack_api_error"


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
        base_url: str | None = None,
        http_client: httpx.AsyncClient | None = None,
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
        self._bot_token: str | None = None  # lazy
        self._client: httpx.AsyncClient | None = http_client

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
        if self._bot_token is not None:
            return self._bot_token
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
            f"slack_bot_token:{self._team_id}",
        )
        if row is None:
            raise SlackApiError(
                "bot token not found for installation",
                endpoint=None,
                tenant=str(self._tenant_id),
            )
        plaintext = await self._secret_store.get(
            row["id"], tenant_id=self._tenant_id,
        )
        self._bot_token = (
            plaintext.decode("utf-8") if isinstance(plaintext, (bytes, bytearray))
            else str(plaintext)
        )
        return self._bot_token

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
        """Issue a Slack Web API call with Tier 1–4 rate-limit and
        transient-error backoff. Returns the parsed JSON on `ok=true`;
        raises `SlackApiError` on `ok=false` or budget exhaustion.
        """
        token = await self._resolve_token()
        url = f"{self._api_base}/{endpoint}"
        client = self._httpx()
        headers = {"Authorization": f"Bearer {token}"}

        loop = asyncio.get_event_loop()
        deadline = loop.time() + self._wall_budget_s
        attempt = 0
        last_status: int | None = None
        last_slack_error: str | None = None

        while attempt < self._max_attempts:
            attempt += 1
            try:
                if method == "POST":
                    r = await client.post(url, headers=headers, json=json_body)
                else:
                    r = await client.get(url, headers=headers, params=params)
            except httpx.TransportError as exc:
                # Transport-level error → exponential backoff within
                # the wall budget. Last attempt raises.
                if attempt >= self._max_attempts:
                    raise SlackApiError(
                        "transport error after retries",
                        endpoint=endpoint,
                        attempts=attempt,
                        error_type=type(exc).__name__,
                    ) from exc
                sleep_s = min(2 ** (attempt - 1), deadline - loop.time())
                if sleep_s <= 0:
                    raise SlackApiError(
                        "transport error and wall budget exhausted",
                        endpoint=endpoint,
                        attempts=attempt,
                    ) from exc
                await asyncio.sleep(sleep_s)
                continue

            last_status = r.status_code
            if r.status_code == 429:
                retry_after = _parse_retry_after(r.headers.get("Retry-After"))
                if retry_after is None:
                    retry_after = 1.0
                remaining = deadline - loop.time()
                if attempt >= self._max_attempts or retry_after >= remaining:
                    raise SlackApiError(
                        "Slack rate limit (429) exhausted retry budget",
                        endpoint=endpoint,
                        retry_after=retry_after,
                        attempts=attempt,
                    )
                await asyncio.sleep(retry_after)
                continue

            r.raise_for_status()
            data = r.json()
            if data.get("ok") is True:
                return data
            last_slack_error = data.get("error")
            # Non-ok responses are not retried (Slack error codes are
            # generally permanent for a given input).
            raise SlackApiError(
                "Slack API returned ok=false",
                endpoint=endpoint,
                slack_error=last_slack_error,
                attempts=attempt,
            )

        # Loop fell through — should be unreachable.
        raise SlackApiError(  # pragma: no cover
            "Slack API call exhausted retry budget",
            endpoint=endpoint,
            attempts=attempt,
            status=last_status,
            slack_error=last_slack_error,
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
        self._user_token: str | None = None  # lazy; preset in spammer mode

    async def _resolve_token(self) -> str:
        if self._user_token is not None:
            return self._user_token
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
            f"slack_user_token:{self._team_id}:{self._user_id}",
        )
        if row is None:
            raise SlackApiError(
                "user token not found for DM install",
                endpoint=None,
                tenant=str(self._tenant_id),
                team_id=self._team_id,
                user_id=self._user_id,
            )
        plaintext = await self._secret_store.get(
            row["id"], tenant_id=self._tenant_id,
        )
        self._user_token = (
            plaintext.decode("utf-8") if isinstance(plaintext, (bytes, bytearray))
            else str(plaintext)
        )
        return self._user_token

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
                    # Mock/spammer convenience: an explicit channel_type.
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


def _parse_retry_after(value: str | None) -> float | None:
    """Slack uses integer-seconds `Retry-After`. Be liberal: tolerate
    a stray decimal. Returns None for unparseable values."""
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


__all__ = ["SlackClient", "SlackUserClient", "SlackApiError"]
