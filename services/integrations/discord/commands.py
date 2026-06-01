"""services/integrations/discord/commands.py — slash-command registration.

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

import os
from typing import Any

import httpx
import structlog

from lib.shared.errors import DiscordOAuthError


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
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """POST the `/fyralis` command spec.

    Returns Discord's response JSON (carries the persistent command id).
    Raises `DiscordOAuthError(code='discord_command_registration_failed')`
    on a 4xx response — caller writes audit row with `status='error'`
    and 5xx propagates as `httpx.HTTPStatusError` (caller chooses).

    `bot_token` is accepted for back-compat with existing tests but is
    ignored in favour of the env-level `DISCORD_BOT_TOKEN`. See the
    module docstring for why.
    """
    auth_token = os.environ.get("DISCORD_BOT_TOKEN", "")
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
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=10.0)
    try:
        resp = await client.post(url, json=_FYRALIS_COMMAND_SPEC, headers=headers)
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
