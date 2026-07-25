"""services/ingest/integrations/discord/commands.py — slash-command registration.

Phase 1 surface: a single global slash command `/fyralis ask <query>`.

Registration verb: `POST /applications/{app_id}/commands` per
Clarifications Q2 (the per-name upsert path; Discord auto-upserts on
`name` collision since API v9). PUT bulk-overwrite was explicitly
rejected; a one-time bootstrap was rejected because it breaks the
self-serve contract in SC-001.

Auth: uses the **app-level Bot Token** from `DISCORD_BOT_TOKEN` env
var (NOT the per-installation OAuth access_token). The OAuth flow's
`access_token` is a user Bearer that confirms the install but does
not carry permission to register global commands — that requires the
app's bot token from the Developer Portal's Bot tab. This was
discovered live during IN-09 dev-testing; the per-installation
`discord_bot_token:<gid>` rows in encrypted_secrets continue to hold
the OAuth access_token for future refresh-token use, but they are
NOT what we authenticate global-command writes with.

Called from `oauth.callback_handler` after a successful token
exchange. Failure here does NOT block the install (FR-012); the
audit row carries `status='error'` and the Discord error code.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import DiscordOAuthError
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
from services.ingest.integrations.provider_transport_runtime import (
    get_provider_transport_runtime,
)


log = structlog.get_logger("integrations.discord.commands")


_DISCORD_API_BASE = "https://discord.com/api/v10"

_FYRALIS_COMMAND_SPEC: dict[str, Any] = {
    "name": "fyralis",
    "type": 1,
    "description": "Ask Fyralis a question about your organization.",
    "options": [
        {
            "name": "ask",
            "type": 3,
            "description": "What you want to ask.",
            "required": True,
        }
    ],
}


async def register_fyralis_command(
    application_id: str,
    bot_token: str | None = None,
    *,
    tenant_id: UUID | str | None = None,
    installation_id: UUID | str | None = None,
    guild_id: str | None = None,
    http_client: httpx.AsyncClient | None = None,
    provider_transport: ProviderExecutor | None = None,
    request_policy: RequestPolicy | PolicyResolver | None = None,
    quota_resolver: QuotaResolver | None = None,
    allow_unlimited_local: bool | None = None,
) -> dict[str, Any]:
    """POST the `/fyralis` command spec.

    Returns Discord's response JSON (carries the persistent command id).
    Raises `DiscordOAuthError(code='discord_command_registration_failed')`
    on a 4xx response. ProviderTransport owns bounded retry for 429, 5xx,
    timeout, and connection failures and may return ``RetryLater``. The OAuth
    caller audits either outcome without rolling back the completed install.

    `bot_token` is accepted for back-compat with existing tests but is
    ignored in favour of the env-level `DISCORD_BOT_TOKEN`. See the
    module docstring for why.
    """
    auth_token = load_app_secret_text_from_env("DISCORD_BOT_TOKEN")
    if not auth_token:
        raise DiscordOAuthError(
            "DISCORD_BOT_TOKEN env var not configured — cannot register global commands",
            code="discord_command_registration_failed",
            context={"http_status": 0, "discord_error_code": "missing_bot_token"},
        )
    url = f"{_DISCORD_API_BASE}/applications/{application_id}/commands"
    headers = {
        "Authorization": f"Bot {auth_token}",
        "Content-Type": "application/json",
    }
    operation = "/applications/{application_id}/commands"
    runtime = get_provider_transport_runtime()
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=10.0)
    local_unlimited = explicit_local_transport(
        requested=(
            runtime is None
            if allow_unlimited_local is None
            else allow_unlimited_local
        ),
        has_local_injection=http_client is not None,
    )
    binding = ProviderRequestBinding(
        source="discord",
        tenant_id=str(tenant_id) if tenant_id is not None else None,
        installation_id=(
            str(installation_id) if installation_id is not None else None
        ),
        transport=(
            provider_transport
            or (runtime.transport if runtime is not None else None)
        ),
        request_policy=request_policy,
        quota_resolver=(
            quota_resolver
            or (runtime.quota_resolver if runtime is not None else None)
        ),
        allow_unlimited_local=(
            local_unlimited if runtime is None else False
        ),
    )

    async def _once() -> httpx.Response:
        try:
            response = await client.post(
                url,
                json=_FYRALIS_COMMAND_SPEC,
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(
                "Discord command registration timed out",
                source="discord",
                operation=operation,
            ) from exc
        except httpx.TransportError as exc:
            raise ProviderTransientError(
                "Discord command registration transport error",
                source="discord",
                operation=operation,
                error_type=type(exc).__name__,
            ) from exc
        if response.status_code == 429:
            retry_after = parse_retry_after(
                response.headers.get("Retry-After"),
            )
            if retry_after is None:
                retry_after = parse_retry_after(
                    response.headers.get("X-RateLimit-Reset-After"),
                )
            if retry_after is None:
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                if isinstance(payload, dict):
                    retry_after = parse_retry_after(
                        payload.get("retry_after"),
                    )
            raise ProviderRateLimited(
                "Discord command registration rate limit",
                retry_after_seconds=retry_after,
                status_code=429,
                header_parser_id="discord.rate_limit_headers",
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                f"Discord command registration returned {response.status_code}",
                source="discord",
                operation=operation,
                http_status=response.status_code,
            )
        return response

    try:
        resp = await binding.execute(
            operation,
            _once,
            concurrency_key=f"discord:{operation}",
            quota_dimensions={
                "app": application_id,
                "guild": guild_id or "",
                "route": operation,
            },
        )
    finally:
        if owns_client:
            await client.aclose()

    if 200 <= resp.status_code < 300:
        try:
            return resp.json()
        except Exception:  # noqa: BLE001
            return {}

    if 400 <= resp.status_code < 500:
        try:
            err_body = resp.json()
        except Exception:  # noqa: BLE001
            err_body = {}
        discord_error_code = err_body.get("code") if isinstance(err_body, dict) else None
        log.info(
            "discord_command_registration_failed",
            http_status=resp.status_code,
            discord_error_code=discord_error_code,
        )
        raise DiscordOAuthError(
            "registration failed",
            code="discord_command_registration_failed",
            context={
                "http_status": resp.status_code,
                "discord_error_code": discord_error_code,
            },
        )

    # 5xx — let httpx raise (caller can choose to retry or fail)
    resp.raise_for_status()
    return {}  # unreachable


__all__ = ["register_fyralis_command"]
