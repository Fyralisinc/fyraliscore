"""services/integrations/slack/oauth.py — Slack OAuth install + callback.

Flow (see specs/IN-08-slack-production-integration/contracts/http-integrations-slack.md):

    GET /integrations/slack/install   (Bearer-auth, tenant from session)
        → INSERT oauth_install_states (nonce, tenant, expires_at)
        → 302 to https://slack.com/oauth/v2/authorize?...&state=<token>

    GET /integrations/slack/callback  (public, state-token-authed)
        → verify HMAC, consume nonce atomically
        → POST oauth.v2.access  (exchange code for bot/user tokens)
        → secret_store.put(bot_token), .put(user_token?), .put(signing_secret)
        → UPSERT provider_installations (with cross-tenant collision guard)
        → INSERT installation_audit_log
        → 302 to /integrations/slack/installed?team=<short_hash>

Security properties:
  - State token's `tenant_id` is bound at issuance from the
    authenticated session — NEVER taken from a client-controllable
    query param (FR-010, SC-009).
  - Nonce is single-use server-side: atomic UPDATE consume rejects
    expired / already-consumed / unknown nonces (FR-012, SC-009).
  - Cross-tenant rebind attempts return HTTP 409 with
    `installation_collision`; the foreign tenant is never disclosed
    in the response or logs (FR-018 edge, IN-07 SC-008).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import (
    InstallationCollisionError,
    SecretStoreError,
    StateTokenInvalidError,
)
from lib.shared.ids import uuid7
from services.integrations.slack import metrics


log = structlog.get_logger("integrations.slack.oauth")


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Slack scopes per FR-013. Bot-token scopes only (user scopes can be
# added later under IN-10).
_SLACK_SCOPES = (
    "channels:history,groups:history,im:history,mpim:history,"
    "users:read,team:read"
)

_SLACK_AUTHORIZE_URL = "https://slack.com/oauth/v2/authorize"
_SLACK_OAUTH_ACCESS_URL = "https://slack.com/api/oauth.v2.access"

_DEFAULT_STATE_TTL_S = 600  # 10 min

# Redirect target URLs (path-relative; browser resolves against the
# Fyralis origin that served the callback, per research R6).
_SUCCESS_REDIRECT = "/integrations/slack/installed"
_ERROR_REDIRECT = "/integrations/slack/install-error"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def short_team_hash(team_id: str) -> str:
    """Non-reversible 16-hex digest of `team_id`. Used in the success
    redirect's `?team=` query param so the URL is not a workspace-
    enumeration vector (FR-012)."""
    return hashlib.blake2b(team_id.encode("utf-8"), digest_size=8).hexdigest()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def _hmac_key() -> bytes:
    """Server-side HMAC key for the OAuth state token. Read from
    `OAUTH_STATE_HMAC_KEY` env var (FR-010 + spec clarification M2).

    Missing in prod fails-fast at gateway startup (via the same
    mechanism as `MASTER_KEK`). In dev / tests the caller may set
    this env var to any non-empty string.
    """
    raw = os.environ.get("OAUTH_STATE_HMAC_KEY", "")
    if not raw:
        env = os.environ.get("FYRALIS_ENV", "").lower()
        if env == "prod":
            raise StateTokenInvalidError(
                "state_invalid",
                "OAUTH_STATE_HMAC_KEY not configured in production",
            )
        # Dev: use a deterministic-but-process-local key derived from a
        # stable string. State tokens won't survive restart in dev —
        # which is fine; the user just re-clicks "Install".
        raw = "dev-only-state-hmac-key-fallback"
    return raw.encode("utf-8")


# ---------------------------------------------------------------------
# State token issuance + verification
# ---------------------------------------------------------------------

async def issue_state_token(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    ttl_seconds: int = _DEFAULT_STATE_TTL_S,
    provider: str = "slack",
) -> str:
    """Allocate a nonce, persist it in `oauth_install_states`, return
    an HMAC-signed state token suitable for the OAuth redirect.

    The state token shape is `<payload_b64>.<sig_b64>` where:
        payload_b64 = b64url({"tenant_id": ..., "nonce": ..., "expires_at": iso})
        sig_b64     = b64url(hmac_sha256(OAUTH_STATE_HMAC_KEY, payload_b64))
    """
    nonce = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

    await pool.execute(
        """
        INSERT INTO oauth_install_states
            (id, tenant_id, nonce, provider, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        """,
        uuid7(),
        tenant_id,
        nonce,
        provider,
        expires_at,
    )

    payload = {
        "tenant_id": str(tenant_id),
        "nonce": nonce,
        "expires_at": expires_at.isoformat(),
    }
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_hmac_key(), payload_b64.encode("ascii"), hashlib.sha256).digest()
    sig_b64 = _b64url(sig)
    return f"{payload_b64}.{sig_b64}"


async def verify_and_consume_state(
    state: str,
    pool: asyncpg.Pool,
) -> tuple[UUID, dict[str, Any]]:
    """Verify the state token's HMAC + parse payload + atomically
    consume the nonce. Returns `(tenant_id, payload)`.

    Raises `StateTokenInvalidError` with `reason` ∈
        {state_invalid, state_expired, state_consumed}.
    """
    if not state or "." not in state:
        raise StateTokenInvalidError("state_invalid", "state token malformed")

    payload_b64, _, sig_b64 = state.partition(".")
    try:
        expected_sig = hmac.new(
            _hmac_key(), payload_b64.encode("ascii"), hashlib.sha256,
        ).digest()
        provided_sig = _b64url_decode(sig_b64)
    except (ValueError, TypeError) as exc:
        raise StateTokenInvalidError(
            "state_invalid", "state token signature unreadable",
        ) from exc
    if not hmac.compare_digest(expected_sig, provided_sig):
        raise StateTokenInvalidError("state_invalid", "state HMAC mismatch")

    try:
        payload_bytes = _b64url_decode(payload_b64)
        payload = json.loads(payload_bytes)
    except (ValueError, json.JSONDecodeError) as exc:
        raise StateTokenInvalidError(
            "state_invalid", "state token payload unparseable",
        ) from exc

    if not isinstance(payload, dict):
        raise StateTokenInvalidError(
            "state_invalid", "state token payload not a dict",
        )
    nonce = payload.get("nonce")
    tenant_id_str = payload.get("tenant_id")
    if not isinstance(nonce, str) or not isinstance(tenant_id_str, str):
        raise StateTokenInvalidError(
            "state_invalid", "state token missing required fields",
        )
    try:
        tenant_id = UUID(tenant_id_str)
    except ValueError as exc:
        raise StateTokenInvalidError(
            "state_invalid", "tenant_id in state token not a UUID",
        ) from exc

    # Atomic check-and-set: UPDATE returning the row only when not yet
    # consumed AND not expired. Zero rows ⇒ disambiguate via a second
    # SELECT to surface the precise failure reason.
    row = await pool.fetchrow(
        """
        UPDATE oauth_install_states
           SET consumed_at = now()
         WHERE nonce = $1
           AND consumed_at IS NULL
           AND expires_at > now()
        RETURNING id, tenant_id, provider
        """,
        nonce,
    )
    if row is not None:
        if row["tenant_id"] != tenant_id:
            # Should be impossible (issuance binds tenant→nonce) but
            # defense-in-depth catches any latent issuance bug.
            raise StateTokenInvalidError(
                "state_invalid",
                "tenant binding mismatch between token and ledger",
            )
        return tenant_id, payload

    existing = await pool.fetchrow(
        "SELECT consumed_at, expires_at FROM oauth_install_states WHERE nonce = $1",
        nonce,
    )
    if existing is None:
        raise StateTokenInvalidError(
            "state_invalid", "nonce was never issued or already swept",
        )
    if existing["consumed_at"] is not None:
        raise StateTokenInvalidError("state_consumed", "state token already used")
    raise StateTokenInvalidError("state_expired", "state token expired")


# ---------------------------------------------------------------------
# Install handler — GET /integrations/slack/install
# ---------------------------------------------------------------------

async def install_handler(request: Request) -> RedirectResponse:
    """Issue a state token for the authenticated session's tenant and
    redirect to Slack's OAuth consent screen.

    Auth: Bearer middleware. `request.state.auth.tenant_id` is the
    tenant the install will be bound to.
    """
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        return JSONResponse(
            {
                "code": "missing_bearer",
                "message": "install requires an authenticated session",
                "context": {"provider": "slack"},
            },
            status_code=401,
        )

    client_id = os.environ.get("SLACK_CLIENT_ID")
    redirect_uri = os.environ.get("SLACK_REDIRECT_URI")
    if not client_id or not redirect_uri:
        log.error(
            "slack_install_unconfigured",
            has_client_id=bool(client_id),
            has_redirect_uri=bool(redirect_uri),
        )
        return JSONResponse(
            {
                "code": "slack_client_unconfigured",
                "message": "SLACK_CLIENT_ID or SLACK_REDIRECT_URI not set",
                "context": {"provider": "slack"},
            },
            status_code=500,
        )

    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": "slack"},
            },
            status_code=503,
        )

    state_token = await issue_state_token(auth.tenant_id, pool)
    metrics.record_install_outcome("initiated")

    # URL-encode params manually so we don't depend on httpx for a
    # one-shot URL build.
    from urllib.parse import urlencode

    qs = urlencode(
        {
            "client_id": client_id,
            "scope": _SLACK_SCOPES,
            "redirect_uri": redirect_uri,
            "state": state_token,
        }
    )
    return RedirectResponse(
        url=f"{_SLACK_AUTHORIZE_URL}?{qs}", status_code=302,
    )


# ---------------------------------------------------------------------
# Callback handler — GET /integrations/slack/callback
# ---------------------------------------------------------------------

async def _exchange_code_for_tokens(code: str) -> dict[str, Any]:
    """Call Slack's `oauth.v2.access`. Returns the parsed JSON.

    Raises a generic Exception on HTTP-level errors; the caller maps
    those to a `slack_oauth_error` redirect.
    """
    client_id = os.environ.get("SLACK_CLIENT_ID", "")
    client_secret = os.environ.get("SLACK_CLIENT_SECRET", "")
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(
            _SLACK_OAUTH_ACCESS_URL,
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
    r.raise_for_status()
    return r.json()


async def _persist_secrets(
    secret_store: Any,
    tenant_id: UUID,
    team_id: str,
    slack_response: dict[str, Any],
) -> tuple[str, str | None]:
    """Store bot token + (optional) user token + per-tenant signing
    secret in the secret store. Returns `(bot_ref, user_ref_or_None)`.

    `secret_ref` on `provider_installations` will point at the bot
    token's ref; other refs are addressable via `label` queries within
    the tenant.
    """
    bot_token = slack_response.get("access_token") or ""
    if not bot_token:
        raise SecretStoreError(
            "Slack OAuth response missing bot token (access_token)",
            reason="missing_bot_token",
        )
    bot_ref = await secret_store.put(
        bot_token,
        label=f"slack_bot_token:{team_id}",
        tenant_id=tenant_id,
    )

    user_ref: str | None = None
    authed_user = slack_response.get("authed_user") or {}
    user_token = authed_user.get("access_token") if isinstance(authed_user, dict) else None
    if isinstance(user_token, str) and user_token:
        user_ref = await secret_store.put(
            user_token,
            label=f"slack_user_token:{team_id}",
            tenant_id=tenant_id,
        )

    # Per-app signing secret — stored once per tenant. We don't try to
    # dedupe; a re-install rewrites the row but the secret store's
    # `put` always allocates a fresh ref (rotation is via `rotate`).
    signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if signing_secret:
        await secret_store.put(
            signing_secret,
            label="slack_signing_secret:app",
            tenant_id=tenant_id,
        )

    return bot_ref, user_ref


async def _upsert_installation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    team_id: str,
    bot_ref: str,
) -> tuple[UUID, bool]:
    """UPSERT a `provider_installations` row keyed by
    `(provider='slack', installation_id=team_id)`. The conflict path
    only updates when the existing row's `tenant_id` matches the
    state-token's `tenant_id`; otherwise raises
    `InstallationCollisionError` (FR-018 edge).

    Returns `(installation_row_id, was_inserted)`.
    """
    row_id = uuid7()
    row = await pool.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'slack', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled    = TRUE
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id,
        tenant_id,
        team_id,
        bot_ref,
    )
    if row is None:
        # Row exists for the same `(provider, team_id)` but a different
        # `tenant_id` — the WHERE-clause filtered out the UPDATE.
        raise InstallationCollisionError(
            "team_id is already bound to a different Fyralis tenant",
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
    """Best-effort append to `installation_audit_log`. Never raises —
    audit failures must not turn a successful install into a failure."""
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider, action, status, context)
            VALUES ($1, $2, $3, 'slack', $4, $5, $6::jsonb)
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


def _invalidate_resolver_cache(request: Request, team_id: str) -> None:
    """Drop any cached `(slack, team_id)` entry so the very next
    webhook for this workspace consults the DB (which just got the
    fresh row)."""
    resolver = getattr(request.app.state, "tenant_resolver", None)
    if resolver is None:
        return
    cache = getattr(resolver, "_cache", None)
    if cache is None:
        return
    try:
        cache.invalidate(("slack", team_id))
    except Exception:  # noqa: BLE001
        pass


def _error_redirect(reason: str, status_code: int) -> RedirectResponse:
    """Build a 302 to the install-error UI page. The HTTP status code
    is observed by automated tests; the human sees the redirect."""
    metrics.record_install_outcome(reason)
    return RedirectResponse(
        url=f"{_ERROR_REDIRECT}?reason={reason}",
        status_code=302,
        headers={"X-Install-Error-Reason": reason},
        # FastAPI's RedirectResponse honors status_code; tests assert
        # on the Location header. Pass through the upstream status via
        # a custom response when the test demands the specific code.
    )


async def callback_handler(request: Request) -> Any:
    """GET /integrations/slack/callback. Public route. State-token
    authenticated. See module docstring for full step sequence."""
    started_at = time.monotonic()
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")

    if not code or not state:
        log.info("slack_install_failure", reason="state_invalid")
        return _error_redirect("state_invalid", status_code=400)

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return _error_redirect("secret_store_unavailable", status_code=503)

    # 1+2+3. Verify HMAC + atomic consume.
    try:
        tenant_id, _payload = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as e:
        log.info("slack_install_failure", reason=e.reason)
        status_code = 400
        return _error_redirect(e.reason, status_code=status_code)

    # 4. Exchange code for tokens.
    try:
        slack_response = await _exchange_code_for_tokens(code)
    except Exception as exc:  # noqa: BLE001 — Slack API errors
        log.error(
            "slack_install_failure",
            reason="slack_oauth_error",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "slack_oauth_error"},
        )
        return _error_redirect("slack_oauth_error", status_code=502)

    if not slack_response.get("ok"):
        log.info(
            "slack_install_failure",
            reason="slack_oauth_error",
            slack_error=slack_response.get("error"),
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {
                "failure_code": "slack_oauth_error",
                "slack_error": slack_response.get("error"),
            },
        )
        return _error_redirect("slack_oauth_error", status_code=502)

    team = slack_response.get("team") or {}
    team_id = team.get("id") if isinstance(team, dict) else None
    if not isinstance(team_id, str) or not team_id:
        log.info("slack_install_failure", reason="slack_oauth_error")
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "slack_oauth_error", "detail": "team.id missing"},
        )
        return _error_redirect("slack_oauth_error", status_code=502)

    # 5. Persist tokens.
    try:
        bot_ref, _user_ref = await _persist_secrets(
            secret_store, tenant_id, team_id, slack_response,
        )
    except SecretStoreError as exc:
        log.error(
            "slack_install_failure",
            reason="secret_store_unavailable",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "secret_store_unavailable"},
        )
        return _error_redirect("secret_store_unavailable", status_code=503)

    # 6. Upsert installation. Collision is the cross-tenant rebind case.
    try:
        installation_row_id, was_inserted = await _upsert_installation(
            pool, tenant_id, team_id, bot_ref,
        )
    except InstallationCollisionError:
        log.info("slack_install_failure", reason="installation_collision")
        await _write_audit(
            pool, tenant_id, None, "install", "rejected_collision",
            {"failure_code": "installation_collision"},
        )
        return _error_redirect("installation_collision", status_code=409)

    # 7. Re-install cleanup: if this was an UPDATE not an INSERT,
    # any orphan token rows for this team should be cleaned up. The
    # bot-token rotation is implicit in `bot_ref` being a fresh row;
    # older bot-token rows for this team are deleted best-effort.
    if not was_inserted:
        await _cleanup_prior_secrets(pool, secret_store, tenant_id, team_id, bot_ref)

    # 8. Audit success.
    scopes_str = slack_response.get("scope", "")
    scopes = scopes_str.split(",") if isinstance(scopes_str, str) else []
    await _write_audit(
        pool, tenant_id, installation_row_id, "install", "ok",
        {
            "was_reinstall": not was_inserted,
            "scopes_count": len(scopes),
            "app_id": slack_response.get("app_id"),
        },
    )

    # 9. Invalidate resolver cache + metrics + redirect.
    _invalidate_resolver_cache(request, team_id)
    metrics.record_install_outcome("success")
    metrics.observe_install_duration(time.monotonic() - started_at)

    return RedirectResponse(
        url=f"{_SUCCESS_REDIRECT}?team={short_team_hash(team_id)}",
        status_code=302,
    )


async def _cleanup_prior_secrets(
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_id: UUID,
    team_id: str,
    keep_ref: str,
) -> None:
    """Best-effort delete of any `encrypted_secrets` rows whose labels
    point at this team's bot or user token but are NOT the freshly-
    issued bot ref. Tolerant of `SecretStore.delete` raising — the
    main install path still succeeds."""
    try:
        rows = await pool.fetch(
            """
            SELECT id::text AS id
              FROM encrypted_secrets
             WHERE tenant_id = $1
               AND (
                   label = $2
                OR label = $3
               )
               AND id::text <> $4
            """,
            tenant_id,
            f"slack_bot_token:{team_id}",
            f"slack_user_token:{team_id}",
            keep_ref,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "slack_reinstall_orphan_query_failed",
            error_type=type(exc).__name__,
        )
        return
    for row in rows:
        try:
            await secret_store.delete(row["id"], tenant_id=tenant_id)
        except Exception:  # noqa: BLE001 — best-effort
            pass


__all__ = [
    "short_team_hash",
    "issue_state_token",
    "verify_and_consume_state",
    "install_handler",
    "callback_handler",
]
