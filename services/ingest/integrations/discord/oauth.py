"""services/ingest/integrations/discord/oauth.py — Discord OAuth install + callback.

Mirrors `services/ingest/integrations/slack/oauth.py` in shape, with the
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
`services.ingest.integrations.oauth_state`, callers in this file change to
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
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)
from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.integrations.discord import commands as discord_commands
from services.ingest.integrations.discord import metrics
from services.ingest.integrations.slack.oauth import (
    issue_state_token as _generic_issue_state_token,
    verify_and_consume_state as _generic_verify_and_consume_state,
)
from services.ingest.integrations.oauth_native_connect import (
    build_oauth_native_connect_router,
)
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


log = structlog.get_logger("integrations.discord.oauth")


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Discord scopes per FR-006. Space-separated in the OAuth URL.
_DISCORD_SCOPES = "applications.commands bot"

# Minimum permissions for source onboarding:
# view_channel (0x400) + send_messages (0x800) + read_message_history (0x10000)
# = 0x10C00 = 68608. Read Message History keeps first backfill friction low
# for newly installed servers; private channels still need channel/category
# role access if they override the bot role.
_DISCORD_STANDARD_PERMISSIONS = "68608"

# Full Server Sync mode: administrator (0x8). Discord defines this as allowing
# all permissions and bypassing channel permission overwrites.
_DISCORD_ADMINISTRATOR_PERMISSIONS = "8"
_DISCORD_PERMISSIONS = _DISCORD_STANDARD_PERMISSIONS
_DISCORD_ACCESS_MODE_STANDARD = "standard"
_DISCORD_ACCESS_MODE_FULL_SERVER_SYNC = "full_server_sync"

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
    from urllib.parse import urlencode

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

    return RedirectResponse(
        url=discord_authorize_url(
            client_id=client_id,
            redirect_uri=redirect_uri,
            state_token=state_token,
            access_mode=discord_access_mode(request.query_params.get("access_mode")),
        ),
        status_code=302,
    )


async def _connect_handoff(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    request: Request,
    body: dict[str, Any],
) -> dict[str, Any]:
    client_id = str(body.get("client_id") or os.environ.get("DISCORD_CLIENT_ID") or "").strip()
    redirect_uri = os.environ.get("DISCORD_REDIRECT_URI", "").strip()
    access_mode = discord_access_mode(body.get("access_mode"))
    missing = [
        name
        for name, value in {
            "DISCORD_CLIENT_ID": client_id,
            "DISCORD_REDIRECT_URI": redirect_uri,
            "DISCORD_CLIENT_SECRET": load_app_secret_text_from_env("DISCORD_CLIENT_SECRET"),
            "DISCORD_APPLICATION_ID": os.environ.get("DISCORD_APPLICATION_ID", ""),
            "DISCORD_BOT_TOKEN": load_app_secret_text_from_env("DISCORD_BOT_TOKEN"),
            "WEBHOOK_SECRET_DISCORD": load_app_secret_text_from_env("WEBHOOK_SECRET_DISCORD"),
        }.items()
        if not value
    ]
    install_url = None
    if not missing:
        state_token = await issue_state_token(tenant_id, pool)
        install_url = discord_authorize_url(
            client_id=client_id,
            redirect_uri=redirect_uri,
            state_token=state_token,
            access_mode=access_mode,
        )
    return {
        "install_url": install_url,
        "discord_access_mode": access_mode,
        "discord_permissions": discord_permissions_for_access_mode(access_mode),
        "oauth_redirect_url": redirect_uri,
        "events_request_url": str(body.get("events_request_url") or "").strip() or None,
        "provider_console_url": "https://discord.com/developers/applications",
        "missing_configuration": missing,
    }


# ---------------------------------------------------------------------
# Callback handler — GET /integrations/discord/callback
# ---------------------------------------------------------------------

async def _exchange_code_for_tokens(
    code: str,
    *,
    tenant_id: UUID | str,
    http_client: httpx.AsyncClient | None = None,
    provider_transport: ProviderExecutor | None = None,
    request_policy: RequestPolicy | PolicyResolver | None = None,
    quota_resolver: QuotaResolver | None = None,
    allow_unlimited_local: bool | None = None,
) -> dict[str, Any]:
    """POST `https://discord.com/api/v10/oauth2/token`. Returns parsed JSON.

    Discord requires HTTP Basic with (client_id, client_secret) — NOT
    body parameters like Slack. Body is form-urlencoded grant_type +
    code + redirect_uri.

    The signed state establishes the exact Fyralis tenant. The Discord guild
    and durable installation row do not exist until after this exchange, so
    this operation is honestly tenant-scoped rather than assigned a synthetic
    installation id.
    """
    client_id = os.environ.get("DISCORD_CLIENT_ID", "")
    client_secret = load_app_secret_text_from_env("DISCORD_CLIENT_SECRET")
    redirect_uri = os.environ.get("DISCORD_REDIRECT_URI", "")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    runtime = get_provider_transport_runtime()
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=15.0)
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
        tenant_id=str(tenant_id),
        installation_id=None,
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
        require_tenant=True,
        require_installation=False,
    )

    async def _once() -> httpx.Response:
        try:
            response = await client.post(
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
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(
                "Discord OAuth token exchange timed out",
                source="discord",
                operation="/oauth2/token",
            ) from exc
        except httpx.TransportError as exc:
            raise ProviderTransientError(
                "Discord OAuth token exchange transport error",
                source="discord",
                operation="/oauth2/token",
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
                "Discord OAuth token exchange rate limit",
                retry_after_seconds=retry_after,
                status_code=429,
                header_parser_id="discord.rate_limit_headers",
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                f"Discord OAuth token exchange returned {response.status_code}",
                source="discord",
                operation="/oauth2/token",
                http_status=response.status_code,
            )
        if response.status_code >= 400:
            raise ProviderPermanentError(
                f"Discord OAuth token exchange returned {response.status_code}",
                source="discord",
                operation="/oauth2/token",
                http_status=response.status_code,
            )
        return response

    try:
        response = await binding.execute(
            "/oauth2/token",
            _once,
            quota_dimensions={
                "app": client_id,
                "route": "/oauth2/token",
            },
        )
        return response.json()
    finally:
        if owns_client:
            await client.aclose()


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

    public_key = load_app_secret_text_from_env("WEBHOOK_SECRET_DISCORD")
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
    executor: asyncpg.Pool | asyncpg.Connection,
    tenant_id: UUID,
    guild_id: str,
    public_key_ref: str,
) -> tuple[UUID, bool]:
    """UPSERT `provider_installations` keyed by `(provider='discord',
    installation_id=guild_id)`. `secret_ref` points at the *public key*
    row (not the bot token) so the signature verifier's `load_secrets`
    DB path returns the verifier-relevant secret.

    Per A12: accepts pool OR connection so the callback can wrap the
    install + onboarding_triggers insert in one atomic transaction
    (per A20).

    Zero rows ⇒ cross-tenant collision (the WHERE-clause filtered out
    the UPDATE branch). Raises `InstallationCollisionError`.

    Returns `(installation_row_id, was_inserted)`.
    """
    row_id = uuid7()
    row = await executor.fetchrow(
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


async def _emit_onboarding_trigger(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    trigger_kind: str,
    payload: dict[str, Any],
) -> None:
    """Per A20: write an onboarding_triggers row atomically with the
    install. Idempotent via migration 0057's partial unique index on
    (tenant_id, source, installation_row_id) WHERE
    installation_row_id IS NOT NULL — OAuth retries / reinstalls
    produce at most one trigger row per (tenant, install)."""
    await conn.execute(
        """
        INSERT INTO onboarding_triggers (
            id, tenant_id, source, trigger_kind,
            installation_row_id, payload
        ) VALUES ($1, $2, 'discord', $3, $4, $5::jsonb)
        ON CONFLICT (tenant_id, source, installation_row_id)
            WHERE installation_row_id IS NOT NULL
            DO NOTHING
        """,
        uuid7(), tenant_id, trigger_kind,
        installation_row_id, json.dumps(payload),
    )


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
        discord_response = await _exchange_code_for_tokens(
            code,
            tenant_id=tenant_id,
        )
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

    # 4. Upsert installation + emit onboarding_triggers atomically (A20).
    # Cross-tenant collision rolls back both inserts. The post-commit
    # work below (cleanup, command registration, cache invalidation)
    # stays outside the transaction — it's best-effort and not part of
    # the install-trigger atomicity contract.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                installation_row_id, was_inserted = await _upsert_installation(
                    conn, tenant_id, guild_id, public_key_ref,
                )
                await _emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=installation_row_id,
                    trigger_kind=("install" if was_inserted else "reinstall"),
                    payload={"guild_id": guild_id},
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
            application_id,
            bot_token,
            tenant_id=tenant_id,
            installation_id=installation_row_id,
            guild_id=guild_id,
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
    except Exception as exc:  # noqa: BLE001 — registration is best-effort
        # ProviderTransport may return RetryLater after a 429/5xx/timeout.
        # The installation row is already committed and command registration
        # is explicitly non-blocking, so preserve the install and audit the
        # deferred operation instead of turning success into a 500.
        registration_status = "error"
        registration_context = {
            "failure_code": "discord_command_registration_deferred",
            "error_code": getattr(exc, "code", type(exc).__name__),
        }
        log.info(
            "discord_install_command_registration_deferred",
            error_code=registration_context["error_code"],
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


router = build_oauth_native_connect_router(
    source="discord",
    authorization_mode="oauth_plus_gateway",
    provider_console_url="https://discord.com/developers/applications",
    payload_fields=[
        "guild_id",
        "application_id",
        "approved_channel_ids",
        "oauth_redirect_url",
        "events_request_url",
    ],
    build_handoff=_connect_handoff,
)


__all__ = [
    "short_guild_hash",
    "issue_state_token",
    "verify_and_consume_state",
    "discord_access_mode",
    "discord_authorize_url",
    "discord_permissions_for_access_mode",
    "install_handler",
    "callback_handler",
    "router",
]
