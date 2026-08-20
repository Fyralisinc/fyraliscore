"""Discord OAuth metadata used by the BYOC onboarding control plane.

Discord ingestion is owned by the SourceConnector runtime.  This module keeps
the provider authorization handoff small and independent from the retired
source-specific ingestion implementation.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode
from uuid import UUID

import asyncpg

from services.ingest.integrations.oauth_state_tokens import (
    issue_state_token as _issue_state_token,
)

_DISCORD_AUTHORIZE_URL = "https://discord.com/oauth2/authorize"
_DISCORD_SCOPES = "applications.commands bot"
_DISCORD_STANDARD_PERMISSIONS = "68608"
_DISCORD_ADMINISTRATOR_PERMISSIONS = "8"
_DISCORD_ACCESS_MODE_STANDARD = "standard"
_DISCORD_ACCESS_MODE_FULL_SERVER_SYNC = "full_server_sync"


def discord_access_mode(value: Any) -> str:
    mode = str(value or "").strip().lower().replace("-", "_")
    if mode in {
        _DISCORD_ACCESS_MODE_FULL_SERVER_SYNC,
        "administrator",
        "admin",
        "full",
    }:
        return _DISCORD_ACCESS_MODE_FULL_SERVER_SYNC
    return _DISCORD_ACCESS_MODE_STANDARD


def discord_permissions_for_access_mode(access_mode: Any) -> str:
    if discord_access_mode(access_mode) == _DISCORD_ACCESS_MODE_FULL_SERVER_SYNC:
        return _DISCORD_ADMINISTRATOR_PERMISSIONS
    return _DISCORD_STANDARD_PERMISSIONS


def discord_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    state_token: str,
    access_mode: Any = _DISCORD_ACCESS_MODE_STANDARD,
) -> str:
    return f"{_DISCORD_AUTHORIZE_URL}?" + urlencode(
        {
            "client_id": client_id,
            "scope": _DISCORD_SCOPES,
            "permissions": discord_permissions_for_access_mode(access_mode),
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state_token,
        }
    )


async def issue_state_token(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    ttl_seconds: int = 600,
) -> str:
    return await _issue_state_token(
        tenant_id,
        pool,
        ttl_seconds=ttl_seconds,
        provider="discord",
    )


__all__ = [
    "discord_access_mode",
    "discord_authorize_url",
    "discord_permissions_for_access_mode",
    "issue_state_token",
]
