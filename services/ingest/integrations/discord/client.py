"""services/ingest/integrations/discord/client.py — outbound Discord REST client.

Single chokepoint for every Discord API call. Resolves the **app-level
Bot Token** from `DISCORD_BOT_TOKEN` env var (NOT a per-installation
OAuth Bearer — see commands.py docstring for the rationale; the OAuth
`access_token` returned by `oauth.v2.access` is a user Bearer that
cannot authorize bot-scope API calls). Every HTTP attempt executes through
ProviderTransport, which owns Discord route/global cooldowns and bounded
retry. Triggers the bot-kick chokepoint
(`_disable_and_zeroize_discord`) on 401 / 403-with-code-50001.

Phase 1 surface:
  - post_followup_message(interaction_token, content) — async reply
    to a previously-acked slash command.
  - get_guild_member(user_id) — enrichment for source_actor_ref.
  - get_channel(channel_id) — enrichment for channel name.
  - post_register_global_command(application_id, command_spec) —
    invoked by oauth.callback_handler via commands.py.

Logging redaction (FR-005 / SC-006): the structured logger NEVER
emits the raw `guild_id`. Operators correlate via `tenant_id` and
`installation_row_id`.
"""
from __future__ import annotations

import time
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog

from lib.shared.errors import DiscordApiError
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)
from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)
from services.ingest.integrations.discord.uninstall import _disable_and_zeroize_discord


log = structlog.get_logger("integrations.discord.client")


_DISCORD_API_BASE = "https://discord.com/api/v10"
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_WALL_BUDGET_S = 30.0


class DiscordClient:
    """Per-installation outbound Discord client.

    Bound to one tenant + installation + guild. The embedded
    `httpx.AsyncClient` is lazy-initialised; call `aclose()` to release
    sockets, or let GC handle it.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        secret_store: Any,
        tenant_id: UUID,
        installation_row_id: UUID,
        guild_id: str,
        tenant_resolver: Any | None = None,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        wall_budget_s: float = _DEFAULT_WALL_BUDGET_S,
        http_client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
        bot_token: str | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._api_base = (base_url or endpoint("discord_api")).rstrip("/")
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._installation_row_id = installation_row_id
        self._guild_id = guild_id
        self._tenant_resolver = tenant_resolver
        self._max_attempts = max_attempts
        self._wall_budget_s = wall_budget_s
        self._bot_token_override = bot_token
        self._owns_client = http_client is None
        self._client: httpx.AsyncClient | None = http_client
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=http_client is not None or base_url is not None,
        )
        self._provider = ProviderRequestBinding(
            source="discord",
            tenant_id=str(tenant_id),
            installation_id=str(installation_row_id),
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
        )

    async def _resolve_bot_token(self) -> str:
        """Read the app-level Bot Token from `DISCORD_BOT_TOKEN` env.

        Bot tokens are application-scoped (one per Discord app in the
        Developer Portal), not per-installation — so env-var resolution
        is the correct model. The per-guild `discord_bot_token:<gid>`
        rows in encrypted_secrets hold the OAuth `access_token` (a user
        Bearer) for future refresh-token flows; they are NOT what
        authorizes bot-scope API calls.

        Raises `DiscordApiError(code='discord_secret_unavailable')`
        if the env var is unset or empty.
        """
        if self._bot_token_override is not None:
            return self._bot_token_override
        token = load_app_secret_text_from_env("DISCORD_BOT_TOKEN")
        if not token:
            raise DiscordApiError(
                "DISCORD_BOT_TOKEN env var not configured",
                code="discord_secret_unavailable",
                context={"tenant_id": str(self._tenant_id)},
            )
        return token

    def _httpx(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _trigger_chokepoint(self, http_status: int) -> None:
        await _disable_and_zeroize_discord(
            pool=self._pool,
            secret_store=self._secret_store,
            installation_row_id=self._installation_row_id,
            tenant_id=self._tenant_id,
            guild_id=self._guild_id,
            reason=f"outbound_{http_status}",
            tenant_resolver=self._tenant_resolver,
        )

    async def _request(
        self,
        method: str,
        endpoint_template: str,
        *,
        endpoint_substituted: str,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        require_bot_token: bool = True,
        disable_on_missing_access: bool = True,
    ) -> dict[str, Any]:
        """The hot loop. `endpoint_template` is the unsubstituted form
        (e.g. `/guilds/{guild_id}/members/{user_id}`) — used in the
        structured log so raw IDs are never logged. `endpoint_substituted`
        is what we actually hit."""
        url = f"{self._api_base}{endpoint_substituted}"
        headers: dict[str, str] = {}
        if require_bot_token:
            token = await self._resolve_bot_token()
            headers["Authorization"] = f"Bot {token}"
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        client = self._httpx()

        async def _once() -> dict[str, Any]:
            request_start = time.monotonic()
            try:
                if method == "GET":
                    r = await client.get(url, headers=headers, params=params)
                elif method == "POST":
                    r = await client.post(
                        url, headers=headers, json=json_body, params=params,
                    )
                else:
                    r = await client.request(
                        method, url, headers=headers,
                        json=json_body, params=params,
                    )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Discord request timed out",
                    source="discord",
                    operation=endpoint_template,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Discord transport error",
                    source="discord",
                    operation=endpoint_template,
                    error_type=type(exc).__name__,
                ) from exc

            duration_ms = int((time.monotonic() - request_start) * 1000)
            log.info(
                "discord_api_request",
                method=method,
                endpoint=endpoint_template,  # NOT the substituted URL
                tenant_id=str(self._tenant_id),
                http_status=r.status_code,
                duration_ms=duration_ms,
            )

            # ProviderTransport publishes the route/global cooldown and owns
            # all bounded retries.
            if r.status_code == 429:
                retry_after = _parse_retry_after(r.headers.get("Retry-After"))
                if retry_after is None:
                    retry_after = _parse_retry_after(
                        r.headers.get("X-RateLimit-Reset-After"),
                    )
                if retry_after is None:
                    try:
                        body = r.json()
                    except ValueError:
                        body = {}
                    if isinstance(body, dict):
                        retry_after = parse_retry_after(body.get("retry_after"))
                raise ProviderRateLimited(
                    "Discord rate limit",
                    retry_after_seconds=retry_after,
                    status_code=429,
                    header_parser_id="discord.rate_limit_headers",
                    route=endpoint_template,
                    global_limit=(
                        r.headers.get("X-RateLimit-Global", "").lower() == "true"
                    ),
                )

            # ---- 401 (or 403 code=50001) → chokepoint, then raise ----
            chokepoint_status: int | None = None
            if r.status_code == 401:
                chokepoint_status = 401
            elif r.status_code == 403:
                try:
                    body = r.json()
                except Exception:  # noqa: BLE001
                    body = {}
                if isinstance(body, dict) and body.get("code") == 50001:
                    if not disable_on_missing_access:
                        raise DiscordApiError(
                            "discord channel is not readable by this bot",
                            code="discord_channel_forbidden",
                            context={
                                "tenant_id": str(self._tenant_id),
                                "http_status": 403,
                                "discord_error_code": 50001,
                            },
                        )
                    chokepoint_status = 403

            if chokepoint_status is not None:
                await self._trigger_chokepoint(chokepoint_status)
                raise DiscordApiError(
                    "installation was disabled following an authorization failure",
                    code="discord_api_unauthorized",
                    context={
                        "tenant_id": str(self._tenant_id),
                        "http_status": chokepoint_status,
                    },
                )

            # ---- 2xx → return JSON (or {} for 204) ----
            if 200 <= r.status_code < 300:
                if r.status_code == 204 or not r.content:
                    return {}
                try:
                    return r.json()
                except Exception:  # noqa: BLE001
                    return {}

            if r.status_code >= 500:
                raise ProviderTransientError(
                    f"Discord returned {r.status_code}",
                    source="discord",
                    operation=endpoint_template,
                    http_status=r.status_code,
                )

            # Other 4xx are permanent for the submitted operation.
            raise DiscordApiError(
                f"discord returned {r.status_code}",
                code="discord_api_error",
                context={
                    "tenant_id": str(self._tenant_id),
                    "http_status": r.status_code,
                },
            )

        return await self._provider.execute(
            endpoint_template,
            _once,
            concurrency_key=f"discord:{endpoint_template}",
            quota_dimensions={
                "guild": self._guild_id,
                "route": endpoint_template,
            },
        )

    # -----------------------------------------------------------------
    # Public surface
    # -----------------------------------------------------------------

    async def post_followup_message(
        self,
        application_id: str,
        interaction_token: str,
        *,
        content: str,
    ) -> dict[str, Any]:
        """POST /webhooks/{app_id}/{interaction_token}. Does NOT use a
        bot token — the interaction_token is the credential."""
        return await self._request(
            "POST",
            "/webhooks/{application_id}/{interaction_token}",
            endpoint_substituted=f"/webhooks/{application_id}/{interaction_token}",
            json_body={"content": content},
            require_bot_token=False,
        )

    async def get_guild_member(self, user_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/guilds/{guild_id}/members/{user_id}",
            endpoint_substituted=f"/guilds/{self._guild_id}/members/{user_id}",
        )

    async def get_channel(self, channel_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/channels/{channel_id}",
            endpoint_substituted=f"/channels/{channel_id}",
        )

    async def post_register_global_command(
        self,
        application_id: str,
        command_spec: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/applications/{application_id}/commands",
            endpoint_substituted=f"/applications/{application_id}/commands",
            json_body=command_spec,
        )

    # -----------------------------------------------------------------
    # Backfill read surface (M6.6) — mirrors MockDiscordClient so the
    # planner / fetcher / reconciler exercise the real REST API the same
    # way they exercise the in-process mock. These endpoints return JSON
    # arrays; `_request` returns the parsed body (a list) on 2xx.
    # -----------------------------------------------------------------

    async def list_guilds(self) -> list[dict[str, Any]]:
        """The bot's guilds (planner shard source). `GET /users/@me/guilds`.

        Cursor-paginated via `after` (Discord caps the page at 200), so a
        bot in >200 guilds is not silently truncated to the first page.
        """
        out: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 200}
            if after:
                params["after"] = after
            result = await self._request(
                "GET", "/users/@me/guilds",
                endpoint_substituted="/users/@me/guilds",
                params=params,
            )
            page = result if isinstance(result, list) else []
            out.extend(g for g in page if isinstance(g, dict))
            if len(page) < 200:
                break
            last_id = page[-1].get("id") if isinstance(page[-1], dict) else None
            if not last_id:
                break
            after = str(last_id)
        return out

    async def list_guild_channels(
        self, guild_id: str,
    ) -> list[dict[str, Any]]:
        """A guild's channels. `GET /guilds/{guild_id}/channels`."""
        result = await self._request(
            "GET", "/guilds/{guild_id}/channels",
            endpoint_substituted=f"/guilds/{guild_id}/channels",
        )
        return result if isinstance(result, list) else []

    async def list_active_guild_threads(
        self, guild_id: str,
    ) -> list[dict[str, Any]]:
        """Active threads visible to the bot. `GET /guilds/{guild_id}/threads/active`."""
        result = await self._request(
            "GET",
            "/guilds/{guild_id}/threads/active",
            endpoint_substituted=f"/guilds/{guild_id}/threads/active",
            disable_on_missing_access=False,
        )
        if not isinstance(result, dict):
            return []
        threads = result.get("threads")
        return threads if isinstance(threads, list) else []

    async def list_channel_archived_threads(
        self,
        channel_id: str,
        *,
        archive_kind: str,
    ) -> list[dict[str, Any]]:
        """Archived public/private threads for one parent channel.

        Public archived threads exist under text, announcement, forum, and media
        parents. Private archived threads require elevated thread permissions;
        Full Server Sync grants Administrator, so the sandbox can enumerate them.
        """
        if archive_kind not in {"public", "private"}:
            raise ValueError("archive_kind must be 'public' or 'private'")

        out: list[dict[str, Any]] = []
        before: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 100}
            if before:
                params["before"] = before
            result = await self._request(
                "GET",
                f"/channels/{{channel_id}}/threads/archived/{archive_kind}",
                endpoint_substituted=(
                    f"/channels/{channel_id}/threads/archived/{archive_kind}"
                ),
                params=params,
                disable_on_missing_access=False,
            )
            if not isinstance(result, dict):
                break
            page = result.get("threads")
            threads = [item for item in page if isinstance(item, dict)] if isinstance(page, list) else []
            out.extend(threads)
            if not result.get("has_more") or not threads:
                break
            archive_timestamp = (
                (threads[-1].get("thread_metadata") or {}).get("archive_timestamp")
                if isinstance(threads[-1].get("thread_metadata"), dict)
                else None
            )
            if not archive_timestamp:
                break
            before = str(archive_timestamp)
        return out

    async def get_messages(
        self,
        *,
        channel_id: str,
        before: str | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """One page of a channel's messages (newest-first), paginated by
        snowflake. `GET /channels/{channel_id}/messages`."""
        params: dict[str, Any] = {}
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after
        if limit is not None:
            params["limit"] = limit
        result = await self._request(
            "GET", "/channels/{channel_id}/messages",
            endpoint_substituted=f"/channels/{channel_id}/messages",
            params=params or None,
            disable_on_missing_access=False,
        )
        return result if isinstance(result, list) else []


def _parse_retry_after(value: str | None) -> float | None:
    """Discord returns float seconds; HTTP-date is accepted defensively."""
    return parse_retry_after(value)


__all__ = ["DiscordClient"]
