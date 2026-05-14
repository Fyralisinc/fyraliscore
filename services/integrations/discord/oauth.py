"""services/integrations/discord/oauth.py — Discord OAuth install + callback.

Mirrors `services/integrations/slack/oauth.py` in shape, with the
following Discord-specific differences (see spec.md Clarifications):

- OAuth scopes: `applications.commands+bot` (Discord uses bot install
  + slash-command scope; Slack used chat-history scopes).
- Token exchange auth: HTTP Basic (client_id:client_secret), NOT body
  parameters — Discord OAuth v2 requires Basic.
- Guild id: extracted from `response.guild.id` in the OAuth response.
- Slash command registration: piggy-backs on the OAuth callback per
  Clarifications Q2 (POST upsert verb via `commands.py`).
- The `discord_public_key:<gid>` row mirrors the application's
  Ed25519 public key per-installation — research R8 (lets the IN-08
  load_secrets DB-backed path resolve uniformly without per-provider
  special-casing).

Reuses `lib/shared/secrets`, `oauth_install_states`, `installation_audit_log`,
and `provider_installations` from IN-08 — zero new tables.

The OAuth state token uses the *same* HMAC key (`OAUTH_STATE_HMAC_KEY`)
as the Slack flow; the table row's `provider` column disambiguates so
a Slack-issued nonce will not consume against a Discord callback.

NOTE on imports from the Slack module: `issue_state_token` and
`verify_and_consume_state` in slack/oauth.py are *already* provider-
agnostic (they take a `provider` kwarg and don't filter on it during
consume — the HMAC binding is the auth). Per Plan T014 + research R5,
we IMPORT those functions directly rather than duplicating ~80 lines.
The Slack module's docstring refers to them as Slack-specific but
the implementation is generic. If a future refactor lifts them into
`services.integrations.oauth_state`, callers in this file change to
one import path; no behaviour change.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import (
    DiscordOAuthError,
    InstallationCollisionError,
    SecretStoreError,
    StateTokenInvalidError,
)
from lib.shared.ids import uuid7
from services.integrations.discord import commands as discord_commands
from services.integrations.discord import metrics
from services.integrations.slack.oauth import (
    issue_state_token as _generic_issue_state_token,
    verify_and_consume_state as _generic_verify_and_consume_state,
)


log = structlog.get_logger("integrations.discord.oauth")


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Discord scopes per FR-006. Space-separated in the OAuth URL.
_DISCORD_SCOPES = "applications.commands bot"

# Minimum permissions: send_messages (0x800) + view_channel (0x400) = 0xC00 = 3072.
_DISCORD_PERMISSIONS = "3072"

_DISCORD_AUTHORIZE_URL = "https://discord.com/oauth2/authorize"
_DISCORD_TOKEN_URL = "https://discord.com/api/v10/oauth2/token"

# Redirect target URLs (path-relative; UI shell owns these routes).
_SUCCESS_REDIRECT = "/integrations/discord/installed"
_ERROR_REDIRECT = "/integrations/discord/install-error"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def short_guild_hash(guild_id: str) -> str:
    """Non-reversible 16-hex digest of `guild_id`. Used in the success
    redirect's `?guild=` query param so the URL is not a workspace-
    enumeration vector (FR-005 / SC-006)."""
    return hashlib.blake2b(guild_id.encode("utf-8"), digest_size=8).hexdigest()


# Re-export the generic state-token helpers under the Discord namespace
# so call sites read naturally. We always pass `provider='discord'`.

async def issue_state_token(
    tenant_id: UUID, pool: asyncpg.Pool, *, ttl_seconds: int = 600,
) -> str:
    return await _generic_issue_state_token(
        tenant_id, pool, ttl_seconds=ttl_seconds, provider="discord",
    )


async def verify_and_consume_state(
    state: str, pool: asyncpg.Pool,
) -> tuple[UUID, dict[str, Any]]:
    return await _generic_verify_and_consume_state(state, pool)


# ---------------------------------------------------------------------
# Install handler — GET /integrations/discord/install
# ---------------------------------------------------------------------

async def install_handler(request: Request) -> RedirectResponse:
    """Issue a state token for the authenticated session's tenant and
    redirect to Discord's OAuth consent screen.

    Auth: Bearer middleware. `request.state.auth.tenant_id` is the
    tenant the install will be bound to.
    """
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        return JSONResponse(
            {
                "code": "missing_bearer",
                "message": "install requires an authenticated session",
                "context": {"provider": "discord"},
            },
            status_code=401,
        )

    client_id = os.environ.get("DISCORD_CLIENT_ID")
    redirect_uri = os.environ.get("DISCORD_REDIRECT_URI")
    if not client_id or not redirect_uri:
        log.error(
            "discord_install_unconfigured",
            has_client_id=bool(client_id),
            has_redirect_uri=bool(redirect_uri),
        )
        return JSONResponse(
            {
                "code": "discord_client_unconfigured",
                "message": "DISCORD_CLIENT_ID or DISCORD_REDIRECT_URI not set",
                "context": {"provider": "discord"},
            },
            status_code=500,
        )

    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": "discord"},
            },
            status_code=503,
        )

    state_token = await issue_state_token(auth.tenant_id, pool)
    metrics.record_install_outcome("initiated")

    from urllib.parse import urlencode
    qs = urlencode(
        {
            "client_id": client_id,
            "scope": _DISCORD_SCOPES,
            "permissions": _DISCORD_PERMISSIONS,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state_token,
        }
    )
    return RedirectResponse(
        url=f"{_DISCORD_AUTHORIZE_URL}?{qs}", status_code=302,
    )


# ---------------------------------------------------------------------
# Callback handler — GET /integrations/discord/callback
# ---------------------------------------------------------------------

async def _exchange_code_for_tokens(code: str) -> dict[str, Any]:
    """POST `https://discord.com/api/v10/oauth2/token`. Returns parsed JSON.

    Discord requires HTTP Basic with (client_id, client_secret) — NOT
    body parameters like Slack. Body is form-urlencoded grant_type +
    code + redirect_uri.

    Raises on HTTP-level errors; caller maps to `discord_oauth_token_exchange_failed`.
    """
    client_id = os.environ.get("DISCORD_CLIENT_ID", "")
    client_secret = os.environ.get("DISCORD_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("DISCORD_REDIRECT_URI", "")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(
            _DISCORD_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
    r.raise_for_status()
    return r.json()


def _extract_guild_id(discord_response: dict[str, Any]) -> str | None:
    """Defensive extraction per research R7. Discord returns `guild.id`
    on bot installs in current API; older shapes may not include it.
    """
    guild = discord_response.get("guild")
    if isinstance(guild, dict):
        gid = guild.get("id")
        if isinstance(gid, str) and gid:
            return gid
    return None


def _extract_application_id(discord_response: dict[str, Any]) -> str | None:
    """Discord returns the bot's application id under `application.id`
    in newer OAuth responses; older responses put it at `application_id`.
    """
    app = discord_response.get("application")
    if isinstance(app, dict):
        aid = app.get("id")
        if isinstance(aid, str) and aid:
            return aid
    app_id = discord_response.get("application_id")
    if isinstance(app_id, str) and app_id:
        return app_id
    # Fall back to env var — every Discord deployment has DISCORD_APPLICATION_ID set.
    return os.environ.get("DISCORD_APPLICATION_ID") or None


async def _persist_secrets(
    secret_store: Any,
    tenant_id: UUID,
    guild_id: str,
    bot_token: str,
) -> tuple[str, str]:
    """Store bot token + per-installation mirror of the application
    Ed25519 public key. Returns `(bot_ref, public_key_ref)`.

    The application public key is identical across installations
    (research R8) — mirroring per `<guild_id>` lets `load_secrets`'s
    DB-backed path resolve uniformly via `provider_installations.secret_ref`.
    """
    if not bot_token:
        raise SecretStoreError(
            "Discord OAuth response missing bot token (access_token)",
            reason="missing_bot_token",
        )
    bot_ref = await secret_store.put(
        bot_token.encode("utf-8") if isinstance(bot_token, str) else bot_token,
        label=f"discord_bot_token:{guild_id}",
        tenant_id=tenant_id,
    )

    public_key = os.environ.get("WEBHOOK_SECRET_DISCORD", "")
    if not public_key:
        raise SecretStoreError(
            "WEBHOOK_SECRET_DISCORD not configured — cannot mirror app public key",
            reason="missing_public_key",
        )
    public_key_ref = await secret_store.put(
        public_key.encode("utf-8"),
        label=f"discord_public_key:{guild_id}",
        tenant_id=tenant_id,
    )
    return bot_ref, public_key_ref


async def _upsert_installation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    guild_id: str,
    public_key_ref: str,
) -> tuple[UUID, bool]:
    """UPSERT `provider_installations` keyed by `(provider='discord',
    installation_id=guild_id)`. `secret_ref` points at the *public key*
    row (not the bot token) so the signature verifier's `load_secrets`
    DB path returns the verifier-relevant secret.

    Zero rows ⇒ cross-tenant collision (the WHERE-clause filtered out
    the UPDATE branch). Raises `InstallationCollisionError`.

    Returns `(installation_row_id, was_inserted)`.
    """
    row_id = uuid7()
    row = await pool.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'discord', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled    = TRUE
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id,
        tenant_id,
        guild_id,
        public_key_ref,
    )
    if row is None:
        raise InstallationCollisionError(
            "guild_id is already bound to a different Fyralis tenant",
        )
    return row["id"], bool(row["was_inserted"])


async def _write_audit(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    installation_row_id: UUID | None,
    action: str,
    status: str,
    context: dict[str, Any] | None = None,
) -> None:
    """Best-effort append to `installation_audit_log`. Never raises."""
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider, action, status, context)
            VALUES ($1, $2, $3, 'discord', $4, $5, $6::jsonb)
            """,
            uuid7(),
            tenant_id,
            installation_row_id,
            action,
            status,
            json.dumps(context or {}),
        )
    except Exception as exc:  # noqa: BLE001 — audit is best-effort
        log.error(
            "installation_audit_log_write_failed",
            action=action,
            status=status,
            error_type=type(exc).__name__,
        )


def _invalidate_resolver_cache(request: Request, guild_id: str) -> None:
    """Drop any cached `(discord, guild_id)` entry so the very next
    interaction for this guild consults the DB."""
    resolver = getattr(request.app.state, "tenant_resolver", None)
    if resolver is None:
        return
    cache = getattr(resolver, "_cache", None)
    if cache is None:
        return
    try:
        cache.invalidate(("discord", guild_id))
    except Exception:  # noqa: BLE001
        pass


def _error_redirect(reason: str) -> RedirectResponse:
    """Build a 302 to the install-error UI page."""
    metrics.record_install_outcome(reason)
    return RedirectResponse(
        url=f"{_ERROR_REDIRECT}?reason={reason}",
        status_code=302,
        headers={"X-Install-Error-Reason": reason},
    )


async def _cleanup_prior_secrets(
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_id: UUID,
    guild_id: str,
    keep_bot_ref: str,
    keep_public_key_ref: str,
) -> None:
    """Best-effort delete of any prior `encrypted_secrets` rows for
    this guild that are NOT the freshly-issued refs. Closes the
    SC-010 orphan-cleanup gap (analyze finding E1).

    Tolerant of `secret_store.delete` raising — the main install path
    still succeeds.
    """
    try:
        rows = await pool.fetch(
            """
            SELECT id::text AS id
              FROM encrypted_secrets
             WHERE tenant_id = $1
               AND (label = $2 OR label = $3)
               AND id::text <> $4
               AND id::text <> $5
            """,
            tenant_id,
            f"discord_bot_token:{guild_id}",
            f"discord_public_key:{guild_id}",
            keep_bot_ref,
            keep_public_key_ref,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "discord_reinstall_orphan_query_failed",
            error_type=type(exc).__name__,
        )
        return
    for row in rows:
        try:
            await secret_store.delete(row["id"], tenant_id=tenant_id)
        except Exception:  # noqa: BLE001 — best-effort
            pass


async def callback_handler(request: Request) -> Any:
    """GET /integrations/discord/callback. Public route. State-token authed."""
    started_at = time.monotonic()
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")

    if not code or not state:
        log.info("discord_install_failure", reason="state_invalid")
        return _error_redirect("state_invalid")

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return _error_redirect("secret_store_unavailable")

    # 1. Verify HMAC + atomic consume.
    try:
        tenant_id, _payload = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as e:
        log.info("discord_install_failure", reason=e.reason)
        return _error_redirect(e.reason)

    # 2. Exchange code for tokens.
    try:
        discord_response = await _exchange_code_for_tokens(code)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "discord_install_failure",
            reason="discord_oauth_token_exchange_failed",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "discord_oauth_token_exchange_failed"},
        )
        return _error_redirect("discord_oauth_token_exchange_failed")

    bot_token = discord_response.get("access_token") or ""
    if not isinstance(bot_token, str) or not bot_token:
        log.info(
            "discord_install_failure",
            reason="discord_oauth_token_exchange_failed",
            detail="missing_access_token",
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "discord_oauth_token_exchange_failed",
             "detail": "missing_access_token"},
        )
        return _error_redirect("discord_oauth_token_exchange_failed")

    guild_id = _extract_guild_id(discord_response)
    if guild_id is None:
        log.info("discord_install_failure", reason="discord_oauth_missing_guild")
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "discord_oauth_missing_guild"},
        )
        return _error_redirect("discord_oauth_missing_guild")

    application_id = _extract_application_id(discord_response)
    if application_id is None:
        log.info(
            "discord_install_failure",
            reason="discord_oauth_missing_application_id",
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "discord_oauth_missing_application_id"},
        )
        return _error_redirect("discord_oauth_token_exchange_failed")

    # 3. Persist tokens (bot + mirrored public key).
    try:
        bot_ref, public_key_ref = await _persist_secrets(
            secret_store, tenant_id, guild_id, bot_token,
        )
    except SecretStoreError as exc:
        log.error(
            "discord_install_failure",
            reason="secret_store_unavailable",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "secret_store_unavailable"},
        )
        return _error_redirect("secret_store_unavailable")

    # 4. Upsert installation (cross-tenant collision guard).
    try:
        installation_row_id, was_inserted = await _upsert_installation(
            pool, tenant_id, guild_id, public_key_ref,
        )
    except InstallationCollisionError:
        log.info("discord_install_failure", reason="installation_collision")
        await _write_audit(
            pool, tenant_id, None, "install", "rejected_collision",
            {"failure_code": "installation_collision"},
        )
        return _error_redirect("installation_collision")

    # 5. Re-install cleanup — analyze finding E1 (SC-010 orphan-free).
    if not was_inserted:
        await _cleanup_prior_secrets(
            pool, secret_store, tenant_id, guild_id, bot_ref, public_key_ref,
        )

    # 6. Register the /fyralis slash command (US4). Failure does NOT
    # block the install (FR-012); audit row carries status='error'.
    registration_status = "ok"
    registration_context: dict[str, Any] = {}
    try:
        cmd_resp = await discord_commands.register_fyralis_command(
            application_id, bot_token,
        )
        registration_context["registered_command_id"] = cmd_resp.get("id")
    except DiscordOAuthError as exc:
        registration_status = "error"
        registration_context = {
            "failure_code": exc.code,
            **exc.context,
        }
        log.info(
            "discord_install_command_registration_failed",
            code=exc.code,
            http_status=exc.context.get("http_status"),
        )

    # 7. Audit.
    audit_context: dict[str, Any] = {
        "was_reinstall": not was_inserted,
        "registration_status": registration_status,
        **registration_context,
    }
    await _write_audit(
        pool, tenant_id, installation_row_id,
        "install",
        "error" if registration_status == "error" else "ok",
        audit_context,
    )

    # 8. Invalidate cache + metrics + redirect.
    _invalidate_resolver_cache(request, guild_id)
    metrics.record_install_outcome(
        "success" if registration_status == "ok" else "discord_command_registration_failed",
    )
    metrics.observe_install_duration(time.monotonic() - started_at)

    return RedirectResponse(
        url=f"{_SUCCESS_REDIRECT}?guild={short_guild_hash(guild_id)}",
        status_code=302,
    )


__all__ = [
    "short_guild_hash",
    "issue_state_token",
    "verify_and_consume_state",
    "install_handler",
    "callback_handler",
]
