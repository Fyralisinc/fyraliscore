"""services/integrations/slack/client.py — outbound Slack Web API client.

Thin async wrapper around three Slack Web API endpoints (`chat.postMessage`,
`users.info`, `conversations.info`), resolving the per-installation bot
token through the IN-08 secret store. Honors Slack's 429 `Retry-After`
header with a bounded retry budget; transport errors retry with
exponential backoff within the same budget.

Becomes the substrate for Slack-outbound Acts in a follow-up (IN-10).
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog

from lib.shared.errors import CompanyOSError


log = structlog.get_logger("integrations.slack.client")


_SLACK_API_BASE = "https://slack.com/api"
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_WALL_BUDGET_S = 30.0


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
        wall_budget_s: float = _DEFAULT_WALL_BUDGET_S,
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
        self._wall_budget_s = wall_budget_s
        self._bot_token: str | None = None  # lazy
        self._client: httpx.AsyncClient | None = http_client

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
        token = await self._resolve_bot_token()
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
        """List the workspace's public channels (planner shard source).
        Returns the `channels` array; each entry carries at least `id`
        and `name`. `team_id` is injected for mock-client parity."""
        data = await self._call(
            "conversations.list",
            method="GET",
            params={"types": "public_channel", "limit": 1000},
        )
        channels = data.get("channels") or []
        return [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "team_id": c.get("context_team_id") or self._team_id,
            }
            for c in channels
            if isinstance(c, dict)
        ]

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


def _parse_retry_after(value: str | None) -> float | None:
    """Slack uses integer-seconds `Retry-After`. Be liberal: tolerate
    a stray decimal. Returns None for unparseable values."""
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


__all__ = ["SlackClient", "SlackApiError"]
