"""Customer-owned Slack app provisioning for BYOC onboarding."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from lib.shared.ids import uuid7
from services.platform.runtime.source_browser_agent_setup import (
    SLACK_BOT_EVENTS,
    SLACK_BOT_SCOPES,
    SLACK_USER_SCOPES,
)


SLACK_MANIFEST_CREATE_URL = "https://slack.com/api/apps.manifest.create"

CONFIGURATION_TOKEN_INPUT_NAMES = (
    "slack_app_config_token",
    "slack_app_configuration_token",
    "SLACK_APP_CONFIG_TOKEN",
    "app_configuration_token",
    "configuration_token",
)


class SlackManifestCreateError(RuntimeError):
    def __init__(
        self,
        slack_error: str,
        *,
        message: str | None = None,
        status_code: int = 502,
        response: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message or slack_error)
        self.slack_error = slack_error
        self.status_code = status_code
        self.response = response or {}


@dataclass(frozen=True)
class SlackAppCredentials:
    app_id: str
    client_id: str
    client_secret: str
    signing_secret: str
    verification_token: str | None
    oauth_authorize_url: str | None
    manifest_sha256: str


@dataclass(frozen=True)
class SlackAppCredentialRefs:
    app_id: str
    client_id: str
    client_secret_ref: str
    signing_secret_ref: str
    verification_token_ref: str | None
    oauth_authorize_url: str | None


@dataclass(frozen=True)
class SlackAppRuntimeCredentials:
    app_id: str
    client_id: str
    client_secret: str
    signing_secret: str
    oauth_authorize_url: str | None


def configuration_token_from_inputs(inputs: dict[str, Any] | None) -> str | None:
    if not inputs:
        return None
    for name in CONFIGURATION_TOKEN_INPUT_NAMES:
        value = inputs.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def build_manifest(
    *,
    oauth_redirect_url: str,
    events_request_url: str | None,
) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "org_deploy_enabled": False,
        "socket_mode_enabled": False,
        "token_rotation_enabled": False,
    }
    if events_request_url:
        settings["event_subscriptions"] = {
            "request_url": events_request_url,
            "bot_events": list(SLACK_BOT_EVENTS),
        }

    return {
        "display_information": {
            "name": "Fyralis BYOC",
            "description": "Slack ingestion app for Fyralis BYOC.",
            "background_color": "#0b1020",
        },
        "features": {
            "bot_user": {
                "display_name": "Fyralis",
                "always_online": False,
            },
        },
        "oauth_config": {
            "redirect_urls": [oauth_redirect_url],
            "scopes": {
                "bot": list(SLACK_BOT_SCOPES),
                "user": list(SLACK_USER_SCOPES),
            },
        },
        "settings": settings,
    }


async def create_app_from_manifest(
    *,
    configuration_token: str,
    oauth_redirect_url: str,
    events_request_url: str | None,
) -> SlackAppCredentials:
    token = configuration_token.strip()
    if not token:
        raise SlackManifestCreateError(
            "missing_configuration_token",
            message="Slack app configuration token is required.",
            status_code=400,
        )

    manifest = build_manifest(
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
    )
    manifest_text = json.dumps(manifest, sort_keys=True, separators=(",", ":"))

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            SLACK_MANIFEST_CREATE_URL,
            json={"token": token, "manifest": manifest_text},
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise SlackManifestCreateError(
            "invalid_slack_response",
            message="Slack did not return JSON for app manifest creation.",
            status_code=502,
        ) from exc

    if not response.is_success:
        raise SlackManifestCreateError(
            str(payload.get("error") or f"http_{response.status_code}"),
            status_code=502,
            response=payload,
        )
    if not payload.get("ok"):
        slack_error = str(payload.get("error") or "manifest_create_failed")
        raise SlackManifestCreateError(
            slack_error,
            status_code=400 if slack_error in {"invalid_auth", "not_authed"} else 502,
            response=payload,
        )

    credentials = payload.get("credentials")
    if not isinstance(credentials, dict):
        credentials = {}
    app_id = str(payload.get("app_id") or "").strip()
    client_id = str(credentials.get("client_id") or "").strip()
    client_secret = str(credentials.get("client_secret") or "").strip()
    signing_secret = str(credentials.get("signing_secret") or "").strip()
    verification_token = (
        str(credentials.get("verification_token") or "").strip() or None
    )
    oauth_authorize_url = str(payload.get("oauth_authorize_url") or "").strip() or None
    missing = [
        name
        for name, value in {
            "app_id": app_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "signing_secret": signing_secret,
        }.items()
        if not value
    ]
    if missing:
        raise SlackManifestCreateError(
            "incomplete_slack_credentials",
            message=f"Slack manifest response was missing {', '.join(missing)}.",
            status_code=502,
            response=payload,
        )

    return SlackAppCredentials(
        app_id=app_id,
        client_id=client_id,
        client_secret=client_secret,
        signing_secret=signing_secret,
        verification_token=verification_token,
        oauth_authorize_url=oauth_authorize_url,
        manifest_sha256=hashlib.sha256(manifest_text.encode("utf-8")).hexdigest(),
    )


async def store_app_credentials(
    *,
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_id: UUID,
    credentials: SlackAppCredentials,
) -> SlackAppCredentialRefs:
    client_secret_ref = await secret_store.put(
        credentials.client_secret,
        label=f"slack_app_client_secret:{credentials.app_id}",
        tenant_id=tenant_id,
    )
    signing_secret_ref = await secret_store.put(
        credentials.signing_secret,
        label=f"slack_app_signing_secret:{credentials.app_id}",
        tenant_id=tenant_id,
    )
    verification_token_ref = None
    if credentials.verification_token:
        verification_token_ref = await secret_store.put(
            credentials.verification_token,
            label=f"slack_app_verification_token:{credentials.app_id}",
            tenant_id=tenant_id,
        )

    await pool.execute(
        """
        INSERT INTO slack_app_credentials (
            id, tenant_id, app_id, client_id, client_secret_ref,
            signing_secret_ref, verification_token_ref, oauth_authorize_url,
            manifest_sha256, last_used_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
        ON CONFLICT (tenant_id, app_id) DO UPDATE
            SET client_id = EXCLUDED.client_id,
                client_secret_ref = EXCLUDED.client_secret_ref,
                signing_secret_ref = EXCLUDED.signing_secret_ref,
                verification_token_ref = EXCLUDED.verification_token_ref,
                oauth_authorize_url = EXCLUDED.oauth_authorize_url,
                manifest_sha256 = EXCLUDED.manifest_sha256,
                disabled_at = NULL,
                last_used_at = now()
        """,
        uuid7(),
        tenant_id,
        credentials.app_id,
        credentials.client_id,
        client_secret_ref,
        signing_secret_ref,
        verification_token_ref,
        credentials.oauth_authorize_url,
        credentials.manifest_sha256,
    )
    return SlackAppCredentialRefs(
        app_id=credentials.app_id,
        client_id=credentials.client_id,
        client_secret_ref=client_secret_ref,
        signing_secret_ref=signing_secret_ref,
        verification_token_ref=verification_token_ref,
        oauth_authorize_url=credentials.oauth_authorize_url,
    )


async def fetch_app_credentials(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    app_id: str | None = None,
) -> SlackAppCredentialRefs | None:
    if app_id:
        row = await pool.fetchrow(
            """
            SELECT app_id, client_id, client_secret_ref, signing_secret_ref,
                   verification_token_ref, oauth_authorize_url
              FROM slack_app_credentials
             WHERE tenant_id = $1
               AND app_id = $2
               AND disabled_at IS NULL
            """,
            tenant_id,
            app_id,
        )
    else:
        row = await pool.fetchrow(
            """
            SELECT app_id, client_id, client_secret_ref, signing_secret_ref,
                   verification_token_ref, oauth_authorize_url
              FROM slack_app_credentials
             WHERE tenant_id = $1
               AND disabled_at IS NULL
             ORDER BY last_used_at DESC NULLS LAST, created_at DESC
             LIMIT 1
            """,
            tenant_id,
        )
    if row is None:
        return None
    return SlackAppCredentialRefs(
        app_id=row["app_id"],
        client_id=row["client_id"],
        client_secret_ref=row["client_secret_ref"],
        signing_secret_ref=row["signing_secret_ref"],
        verification_token_ref=row["verification_token_ref"],
        oauth_authorize_url=row["oauth_authorize_url"],
    )


async def resolve_runtime_credentials(
    *,
    secret_store: Any,
    tenant_id: UUID,
    refs: SlackAppCredentialRefs,
) -> SlackAppRuntimeCredentials:
    client_secret = await secret_store.get(
        refs.client_secret_ref,
        tenant_id=tenant_id,
    )
    signing_secret = await secret_store.get(
        refs.signing_secret_ref,
        tenant_id=tenant_id,
    )
    return SlackAppRuntimeCredentials(
        app_id=refs.app_id,
        client_id=refs.client_id,
        client_secret=_secret_bytes_to_text(client_secret),
        signing_secret=_secret_bytes_to_text(signing_secret),
        oauth_authorize_url=refs.oauth_authorize_url,
    )


async def mark_app_credentials_used(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    app_id: str,
) -> None:
    await pool.execute(
        """
        UPDATE slack_app_credentials
           SET last_used_at = now()
         WHERE tenant_id = $1
           AND app_id = $2
        """,
        tenant_id,
        app_id,
    )


def _secret_bytes_to_text(value: bytes | bytearray | str) -> str:
    if isinstance(value, str):
        return value
    return bytes(value).decode("utf-8")
