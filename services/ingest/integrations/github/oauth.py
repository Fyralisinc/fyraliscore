"""services/ingest/integrations/github/oauth.py — GitHub App install + callback.

Flow (see contracts/http-integrations-github.md):

    GET /integrations/github/install   (Bearer-authed; tenant from session)
        → INSERT oauth_install_states (nonce, tenant, expires_at, provider='github')
        → 302 to https://github.com/apps/<slug>/installations/new?state=<token>

    GET /integrations/github/callback  (public; state-token authed)
        → verify HMAC + atomic nonce consume (provider='github')
        → reject cross-tenant collisions using the existing installation row
        → mint installation access token (via GithubClient)
        → GET /installation/repositories to prove the grant is usable
        → UPSERT provider_installations (cross-tenant collision guard)
        → INSERT installation_audit_log
        → 302 to /integrations/github/installed?installation=<short-hash>

Security properties:
  - State token's `tenant_id` bound at issuance from the authenticated
    session; never read from a client-controlled query param.
  - Nonce is single-use server-side (atomic UPDATE consume).
  - Cross-tenant rebinds return 302 to install-error with
    `installation_collision`; the foreign tenant_id never appears in
    the response body, redirect Location, or any log line.
  - No webhook secret is generated per-installation (FR-007 / Q1):
    GitHub Apps use a single App-level secret loaded via
    `WEBHOOK_SECRET_GITHUB` or the secret-store equivalent.
"""
from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

import asyncpg
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import (
    GithubApiError,
    GithubOAuthError,
    InstallationCollisionError,
    StateTokenInvalidError,
)
from lib.shared.ids import uuid7

# Reuse the slack module's generic state-token helpers (already
# provider-neutral via the `provider` kwarg).
from services.ingest.integrations.slack.oauth import (
    issue_state_token as _generic_issue_state_token,
    verify_and_consume_state as _generic_verify_and_consume_state,
)

from services.ingest.integrations.github import metrics
from services.ingest.integrations.github.uninstall import _short_installation_hash
from services.ingest.integrations.oauth_native_connect import (
    build_oauth_native_connect_router,
)


log = structlog.get_logger("integrations.github.oauth")


_GITHUB_INSTALL_BASE = "https://github.com/apps"
_GITHUB_INSTALLED_REDIRECT = "/integrations/github/installed"
_GITHUB_INSTALL_ERROR_REDIRECT = "/integrations/github/install-error"


# ---------------------------------------------------------------------
# State-token convenience wrappers
# ---------------------------------------------------------------------

async def issue_state_token(
    tenant_id: UUID, pool: asyncpg.Pool,
) -> str:
    """Issue a state token bound to `tenant_id` with provider='github'."""
    return await _generic_issue_state_token(
        tenant_id, pool, provider="github",
    )


async def verify_and_consume_state(
    state: str, pool: asyncpg.Pool,
) -> tuple[UUID, dict[str, Any]]:
    return await _generic_verify_and_consume_state(state, pool)


# ---------------------------------------------------------------------
# Install handler — GET /integrations/github/install
# ---------------------------------------------------------------------

async def install_handler(request: Request) -> Any:
    """Bearer-authed entry point. Issues a state token and redirects
    the admin to GitHub's App-install consent page.

    Required env:
      - GITHUB_APP_SLUG  — URL-safe App slug (e.g., 'fyralis').
    """
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        return JSONResponse(
            {
                "code": "missing_bearer",
                "message": "install requires an authenticated session",
                "context": {"provider": "github"},
            },
            status_code=401,
        )

    app_slug = os.environ.get("GITHUB_APP_SLUG", "").strip()
    if not app_slug:
        log.error("github_install_unconfigured", has_app_slug=False)
        return JSONResponse(
            {
                "code": "github_client_unconfigured",
                "message": "GITHUB_APP_SLUG not set",
                "context": {"provider": "github"},
            },
            status_code=500,
        )

    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": "github"},
            },
            status_code=503,
        )

    state_token = await issue_state_token(auth.tenant_id, pool)
    metrics.record_install_callback("initiated")

    qs = urlencode({"state": state_token})
    return RedirectResponse(
        url=f"{_GITHUB_INSTALL_BASE}/{app_slug}/installations/new?{qs}",
        status_code=302,
    )


async def _connect_handoff(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    request: Request,
    body: dict[str, Any],
) -> dict[str, Any]:
    app_slug = str(body.get("app_slug") or os.environ.get("GITHUB_APP_SLUG") or "").strip()
    private_key_sources = [
        name
        for name in (
            "GITHUB_APP_PRIVATE_KEY_SECRET_REF",
            "GITHUB_APP_PRIVATE_KEY",
            "GITHUB_APP_PRIVATE_KEY_PATH",
        )
        if os.environ.get(name, "").strip()
    ]
    missing = [
        name
        for name, configured in {
            "GITHUB_APP_SLUG": bool(app_slug),
            "GITHUB_APP_ID": bool(os.environ.get("GITHUB_APP_ID", "").strip()),
            "WEBHOOK_SECRET_GITHUB": bool(
                os.environ.get("WEBHOOK_SECRET_GITHUB_SECRET_REF", "").strip()
                or os.environ.get("WEBHOOK_SECRET_GITHUB", "").strip()
            ),
        }.items()
        if not configured
    ]
    if not private_key_sources:
        missing.append("GITHUB_APP_PRIVATE_KEY_SOURCE")
    elif len(private_key_sources) > 1:
        missing.append("GITHUB_APP_PRIVATE_KEY_SOURCE_CONFLICT")
    install_url = None
    if not missing:
        state_token = await issue_state_token(tenant_id, pool)
        install_url = (
            f"{_GITHUB_INSTALL_BASE}/{app_slug}/installations/new?"
            + urlencode({"state": state_token})
        )
    public_url = str(request.base_url).rstrip("/")
    return {
        "install_url": install_url,
        "oauth_redirect_url": f"{public_url}/integrations/github/callback",
        "events_request_url": str(body.get("events_request_url") or "").strip() or None,
        "provider_console_url": "https://github.com/settings/apps",
        "missing_configuration": missing,
    }


# ---------------------------------------------------------------------
# Callback handler — GET /integrations/github/callback
# ---------------------------------------------------------------------

async def callback_handler(request: Request) -> Any:
    """Public route; authenticated by state-token HMAC + nonce consume.

    Query: installation_id, setup_action ∈ {install, update}, state.
    """
    installation_id = request.query_params.get("installation_id", "").strip()
    setup_action = request.query_params.get("setup_action", "").strip()
    state = request.query_params.get("state", "").strip()

    if not installation_id:
        metrics.record_install_callback("missing_installation_id")
        return _redirect_install_error("missing_installation_id")
    if not state:
        metrics.record_install_callback("state_invalid")
        return _redirect_install_error("state_invalid")

    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": "github"},
            },
            status_code=503,
        )

    # Step 1: verify and consume state token.
    try:
        tenant_id, _payload = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as exc:
        metrics.record_install_callback(exc.reason)
        log.info(
            "github_callback_state_invalid",
            reason=exc.reason,
        )
        return _redirect_install_error(exc.reason)

    short_hash = _short_installation_hash(installation_id)

    # Step 2: fail cross-tenant collisions before making outbound probes. The
    # transaction still keeps the authoritative guard below for race safety.
    existing_row = await pool.fetchrow(
        """
        SELECT id, tenant_id
          FROM provider_installations
         WHERE provider = 'github'
           AND installation_id = $1
        """,
        installation_id,
    )
    if existing_row is not None and existing_row["tenant_id"] != tenant_id:
        metrics.record_install_callback("installation_collision")
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=None,
            action="rejected_collision",
            status="rejected_collision",
            context={
                "installation_id_hash": short_hash,
                "setup_action": setup_action,
            },
        )
        log.info(
            "github_callback_installation_collision",
            tenant_id=str(tenant_id),
            installation_id_hash=short_hash,
        )
        return _redirect_install_error("installation_collision")

    # Step 3: prove the installation grant is usable before committing any
    # fresh install row or onboarding trigger. This keeps bad credentials from
    # leaving durable source state behind.
    client = getattr(request.app.state, "github_client", None)
    if client is None:
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=None,
            action="install",
            status="error",
            context={
                "failure_code": "github_client_unavailable",
                "installation_id_hash": short_hash,
            },
        )
        metrics.record_install_callback("github_client_unavailable")
        return _redirect_install_error("github_client_unavailable")

    if existing_row is not None:
        await client.register_installation_context(
            installation_id,
            tenant_id=tenant_id,
            installation_row_id=existing_row["id"],
        )

    try:
        selected_repositories = await client.list_installation_repositories(
            installation_id,
        )
    except (GithubApiError, GithubOAuthError) as exc:
        error_code = getattr(exc, "code", "github_api_error")
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=(
                existing_row["id"] if existing_row is not None else None
            ),
            action="install",
            status="error",
            context={
                "failure_code": "github_credential_validation_failed",
                "installation_id_hash": short_hash,
                "github_error_code": error_code,
            },
        )
        log.warning(
            "github_callback_credential_validation_failed",
            tenant_id=str(tenant_id),
            installation_id_hash=short_hash,
            error_code=error_code,
        )
        metrics.record_install_callback("github_credential_validation_failed")
        return _redirect_install_error("github_credential_validation_failed")

    # Step 4: UPSERT provider_installations + emit onboarding_triggers
    # atomically (A20). Cross-tenant collision rolls back both inserts if a
    # concurrent callback won the row between the read above and this write.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                installation_row_id, was_inserted = await _upsert_installation_in_tx(
                    conn,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                )
                await _emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=installation_row_id,
                    trigger_kind=("install" if was_inserted else "reinstall"),
                    payload={"installation_id_hash": short_hash},
                )
    except InstallationCollisionError:
        metrics.record_install_callback("installation_collision")
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=None,
            action="rejected_collision",
            status="rejected_collision",
            context={
                "installation_id_hash": short_hash,
                "setup_action": setup_action,
            },
        )
        log.info(
            "github_callback_installation_collision",
            tenant_id=str(tenant_id),
            installation_id_hash=short_hash,
        )
        return _redirect_install_error("installation_collision")

    # Step 5: register the installation context on the outbound client
    # so the chokepoint can find the row + tenant if a 401/404 fires
    # after install.
    await client.register_installation_context(
        installation_id,
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
    )

    # Step 6: persist selected_repositories. selected_repositories=None means
    # "all-repositories" mode per R10, persisted as NULL.
    serialized = (
        json.dumps(selected_repositories)
        if selected_repositories is not None
        else None
    )
    await pool.execute(
        """
        UPDATE provider_installations
           SET selected_repositories = $2::jsonb
         WHERE id = $1
        """,
        installation_row_id,
        serialized,
    )

    # Step 7: write install / reinstall / update audit row.
    action_label = _install_action_label(
        setup_action=setup_action,
        was_inserted=was_inserted,
    )
    await _audit(
        pool=pool,
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        action=action_label,
        status="ok",
        context={
            "installation_id_hash": short_hash,
            "setup_action": setup_action,
            "selected_repository_count": (
                len(selected_repositories)
                if isinstance(selected_repositories, list)
                else None
            ),
            "all_repositories_mode": (
                selected_repositories is None
            ),
        },
    )

    metrics.record_install_callback("ok")
    log.info(
        "github_callback_install_complete",
        tenant_id=str(tenant_id),
        installation_row_id=str(installation_row_id),
        installation_id_hash=short_hash,
        action=action_label,
        setup_action=setup_action,
    )

    qs = urlencode({"installation": short_hash})
    return RedirectResponse(
        url=f"{_GITHUB_INSTALLED_REDIRECT}?{qs}", status_code=302,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

async def _upsert_installation(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    installation_id: str,
) -> tuple[UUID, bool]:
    """UPSERT `(provider='github', installation_id)` with cross-tenant
    collision guard. Returns `(installation_row_id, was_inserted)`.

    Raises `InstallationCollisionError` when an existing row's
    `tenant_id` differs from the supplied one.
    """
    row_id = uuid7()
    row = await pool.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'github', $3, NULL, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET enabled = TRUE,
                secret_ref = NULL
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id,
        tenant_id,
        installation_id,
    )
    if row is None:
        # The WHERE clause rejected the UPDATE — existing row has a
        # different tenant_id. Per FR-005 we never leak the foreign id.
        raise InstallationCollisionError(
            "github installation_id is already bound to a different tenant",
        )
    return row["id"], bool(row["was_inserted"])


async def _upsert_installation_in_tx(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    installation_id: str,
) -> tuple[UUID, bool]:
    """Connection-bound variant of _upsert_installation. Per A12: same
    SQL, executes on a caller-supplied connection so the callback can
    wrap the install + onboarding_triggers insert in one atomic
    transaction (per A20)."""
    row_id = uuid7()
    row = await conn.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'github', $3, NULL, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET enabled = TRUE,
                secret_ref = NULL
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id, tenant_id, installation_id,
    )
    if row is None:
        raise InstallationCollisionError(
            "github installation_id is already bound to a different tenant",
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
        ) VALUES ($1, $2, 'github', $3, $4, $5::jsonb)
        ON CONFLICT (tenant_id, source, installation_row_id)
            WHERE installation_row_id IS NOT NULL
            DO NOTHING
        """,
        uuid7(), tenant_id, trigger_kind,
        installation_row_id, json.dumps(payload),
    )


def _install_action_label(*, setup_action: str, was_inserted: bool) -> str:
    """Map (setup_action, was_inserted) → installation_audit_log.action.

    - setup_action='install', was_inserted=True   → 'install'
    - setup_action='install', was_inserted=False  → 'reinstall'
    - setup_action='update'                       → 'update'
    - setup_action=other / missing                → 'install' (defensive default)
    """
    if setup_action == "update":
        return "update"
    if not was_inserted:
        return "reinstall"
    return "install"


async def _audit(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    installation_row_id: UUID | None,
    action: str,
    status: str,
    context: dict[str, Any],
) -> None:
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider,
                 action, status, context)
            VALUES ($1, $2, $3, 'github', $4, $5, $6::jsonb)
            """,
            uuid7(),
            tenant_id,
            installation_row_id,
            action,
            status,
            json.dumps(context),
        )
    except Exception:  # noqa: BLE001 — audit is best-effort
        log.error(
            "github_oauth_audit_failed",
            tenant_id=str(tenant_id),
            action=action,
        )


def _redirect_install_error(reason: str) -> RedirectResponse:
    qs = urlencode({"reason": reason})
    return RedirectResponse(
        url=f"{_GITHUB_INSTALL_ERROR_REDIRECT}?{qs}", status_code=302,
    )


router = build_oauth_native_connect_router(
    source="github",
    authorization_mode="github_app",
    provider_console_url="https://github.com/settings/apps",
    payload_fields=[
        "installation_id",
        "organization",
        "repository_selection",
        "oauth_redirect_url",
        "events_request_url",
    ],
    build_handoff=_connect_handoff,
)


__all__ = [
    "install_handler",
    "callback_handler",
    "issue_state_token",
    "verify_and_consume_state",
    "router",
]
