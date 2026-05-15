"""services/integrations/github/oauth.py — GitHub App install + callback.

Flow (see contracts/http-integrations-github.md):

    GET /integrations/github/install   (Bearer-authed; tenant from session)
        → INSERT oauth_install_states (nonce, tenant, expires_at, provider='github')
        → 302 to https://github.com/apps/<slug>/installations/new?state=<token>

    GET /integrations/github/callback  (public; state-token authed)
        → verify HMAC + atomic nonce consume (provider='github')
        → UPSERT provider_installations (cross-tenant collision guard)
        → mint installation access token (via GithubClient)
        → GET /installation/repositories → write selected_repositories
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

import hashlib
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
from services.integrations.slack.oauth import (
    issue_state_token as _generic_issue_state_token,
    verify_and_consume_state as _generic_verify_and_consume_state,
)

from services.integrations.github import metrics
from services.integrations.github.uninstall import _short_installation_hash


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

    # Step 2: UPSERT provider_installations with cross-tenant collision guard.
    try:
        installation_row_id, was_inserted = await _upsert_installation(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=installation_id,
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

    # Step 3: register the installation context on the outbound client
    # so the chokepoint can find the row + tenant if a 401/404 fires
    # mid-callback.
    client = getattr(request.app.state, "github_client", None)
    if client is not None:
        await client.register_installation_context(
            installation_id,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
        )

    # Step 4: seed selected_repositories via `GET /installation/repositories`.
    # Failure is non-fatal (FR-022 / R9) — row stays with
    # selected_repositories=NULL and an audit row notes the unknown flag.
    selected_repositories: list[str] | None = None
    selected_repositories_unknown = False
    if client is not None:
        try:
            selected_repositories = await client.list_installation_repositories(
                installation_id,
            )
            # selected_repositories=None means "all-repositories" mode
            # per R10. We persist None as NULL in the DB column.
        except (GithubApiError, GithubOAuthError) as exc:
            selected_repositories_unknown = True
            log.warning(
                "github_callback_repos_fetch_failed",
                tenant_id=str(tenant_id),
                installation_row_id=str(installation_row_id),
                installation_id_hash=short_hash,
                error_code=getattr(exc, "code", None),
            )

    # Step 5: persist selected_repositories.
    if not selected_repositories_unknown:
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

    # Step 6: write install / reinstall / update audit row.
    if selected_repositories_unknown:
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            action="repository_fetch_failed",
            status="error",
            context={
                "installation_id_hash": short_hash,
                "selected_repositories_unknown": True,
            },
        )

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
                selected_repositories is None and not selected_repositories_unknown
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


__all__ = [
    "install_handler",
    "callback_handler",
    "issue_state_token",
    "verify_and_consume_state",
]
