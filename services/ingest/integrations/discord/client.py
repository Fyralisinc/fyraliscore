"""Bounded Discord REST reader for BYOC resource-access review."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from lib.shared.errors import DiscordApiError
from lib.shared.secrets import load_app_secret_text_from_env


class DiscordClient:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        secret_store: Any,
        tenant_id: UUID,
        installation_row_id: UUID,
        guild_id: str,
        tenant_resolver: Any | None = None,
        max_attempts: int = 3,
        wall_budget_s: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
        bot_token: str | None = None,
    ) -> None:
        del pool, secret_store, installation_row_id, tenant_resolver
        configured = os.environ.get(
            "DISCORD_API_BASE_URL", "https://discord.com/api/v10"
        )
        synthetic = os.environ.get("SYNTHETIC_SOURCE_API_BASE", "").strip()
        self._api_base = (
            base_url
            or (f"{synthetic.rstrip('/')}/discord" if synthetic else configured)
        ).rstrip("/")
        self._tenant_id = tenant_id
        self._guild_id = guild_id
        self._max_attempts = max_attempts
        self._wall_budget_s = wall_budget_s
        self._bot_token = bot_token
        self._client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _token(self) -> str:
        token = self._bot_token or load_app_secret_text_from_env("DISCORD_BOT_TOKEN")
        if not token:
            raise DiscordApiError(
                "DISCORD_BOT_TOKEN env var not configured",
                code="discord_secret_unavailable",
                context={"tenant_id": str(self._tenant_id)},
            )
        return token

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        deadline = time.monotonic() + self._wall_budget_s
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.get(
                    f"{self._api_base}{path}",
                    headers={"Authorization": f"Bot {self._token()}"},
                    params=params,
                )
            except httpx.TransportError as exc:
                if attempt == self._max_attempts or time.monotonic() >= deadline:
                    raise DiscordApiError(
                        "Discord transport error",
                        code="discord_api_error",
                    ) from exc
                await asyncio.sleep(min(2 ** (attempt - 1), 2.0))
                continue

            if response.status_code == 429:
                try:
                    retry_after = float(response.headers.get("Retry-After", "1"))
                except ValueError:
                    retry_after = 1.0
                if (
                    attempt == self._max_attempts
                    or time.monotonic() + retry_after >= deadline
                ):
                    raise DiscordApiError(
                        "Discord rate limit exhausted retry budget",
                        code="discord_api_rate_limited",
                    )
                await asyncio.sleep(retry_after)
                continue
            if response.status_code in {401, 403}:
                code = (
                    "discord_channel_forbidden"
                    if response.status_code == 403
                    else "discord_api_unauthorized"
                )
                raise DiscordApiError(
                    "Discord authorization failed",
                    code=code,
                    context={"http_status": response.status_code},
                )
            if response.status_code >= 400:
                raise DiscordApiError(
                    "Discord request failed",
                    code="discord_api_error",
                    context={"http_status": response.status_code},
                )
            if not response.content:
                return {}
            return response.json()
        raise DiscordApiError(
            "Discord retry budget exhausted", code="discord_api_error"
        )

    async def list_guilds(self) -> list[dict[str, Any]]:
        result = await self._get("/users/@me/guilds", params={"limit": 200})
        return (
            [item for item in result if isinstance(item, dict)]
            if isinstance(result, list)
            else []
        )

    async def list_guild_channels(self, guild_id: str) -> list[dict[str, Any]]:
        result = await self._get(f"/guilds/{guild_id}/channels")
        return (
            [item for item in result if isinstance(item, dict)]
            if isinstance(result, list)
            else []
        )

    async def list_active_guild_threads(self, guild_id: str) -> list[dict[str, Any]]:
        result = await self._get(f"/guilds/{guild_id}/threads/active")
        threads = result.get("threads") if isinstance(result, dict) else None
        return (
            [item for item in threads if isinstance(item, dict)]
            if isinstance(threads, list)
            else []
        )

    async def list_channel_archived_threads(
        self,
        channel_id: str,
        *,
        archive_kind: str,
    ) -> list[dict[str, Any]]:
        if archive_kind not in {"public", "private"}:
            raise ValueError("archive_kind must be 'public' or 'private'")
        result = await self._get(
            f"/channels/{channel_id}/threads/archived/{archive_kind}",
            params={"limit": 100},
        )
        threads = result.get("threads") if isinstance(result, dict) else None
        return (
            [item for item in threads if isinstance(item, dict)]
            if isinstance(threads, list)
            else []
        )

    async def get_messages(
        self,
        *,
        channel_id: str,
        before: str | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            key: value
            for key, value in {"before": before, "after": after, "limit": limit}.items()
            if value is not None
        }
        result = await self._get(f"/channels/{channel_id}/messages", params=params)
        return (
            [item for item in result if isinstance(item, dict)]
            if isinstance(result, list)
            else []
        )


__all__ = ["DiscordClient"]
