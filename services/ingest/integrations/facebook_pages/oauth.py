"""Facebook Pages OAuth install and native-connect handoff."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

import asyncpg
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import (
    InstallationCollisionError,
    SecretStoreError,
    StateTokenInvalidError,
)
from lib.shared.ids import uuid7
from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.integrations.facebook_pages.client import (
    FACEBOOK_PAGES_WEBHOOK_FIELDS,
    FacebookPagesClient,
)
from services.ingest.integrations.oauth_native_connect import (
    build_oauth_native_connect_router,
)
from services.ingest.integrations.oauth_state_tokens import (
    _b64url,
    _hmac_key,
    verify_and_consume_state,
)

log = structlog.get_logger("integrations.facebook_pages.oauth")

SOURCE = "facebook_pages"
DISPLAY_NAME = "Facebook Page Messages"

_AUTHORIZE_URL = "https://www.facebook.com/dialog/oauth"
_SCOPES = (
    "pages_show_list",
    "pages_messaging",
    "pages_manage_metadata",
    "pages_read_engagement",
)
_SUCCESS_REDIRECT = "/integrations/facebook_pages/installed"
_ERROR_REDIRECT = "/integrations/facebook_pages/install-error"
_DEFAULT_STATE_TTL_S = 600


def short_page_hash(page_id: str) -> str:
    return hashlib.blake2b(page_id.encode("utf-8"), digest_size=8).hexdigest()


async def issue_facebook_state_token(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    page_id: str | None = None,
    ttl_seconds: int = _DEFAULT_STATE_TTL_S,
) -> str:
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
        SOURCE,
        expires_at,
    )
    payload: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "nonce": nonce,
        "expires_at": expires_at.isoformat(),
    }
    if page_id:
        payload["page_id"] = page_id
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_hmac_key(), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url(sig)}"


async def install_handler(request: Request) -> Any:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        return JSONResponse(
            {
                "code": "missing_bearer",
                "message": "install requires an authenticated session",
                "context": {"provider": SOURCE},
            },
            status_code=401,
        )

    client_id = os.environ.get("FACEBOOK_APP_ID", "").strip()
    redirect_uri = os.environ.get("FACEBOOK_REDIRECT_URI", "").strip()
    if not client_id or not redirect_uri:
        return JSONResponse(
            {
                "code": "facebook_pages_unconfigured",
                "message": "FACEBOOK_APP_ID or FACEBOOK_REDIRECT_URI not set",
                "context": {"provider": SOURCE},
            },
            status_code=500,
        )
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": SOURCE},
            },
            status_code=503,
        )

    page_id = str(request.query_params.get("page_id") or "").strip() or None
    state_token = await issue_facebook_state_token(
        auth.tenant_id,
        pool,
        page_id=page_id,
    )
    authorize_query = urlencode(_authorize_params(client_id, redirect_uri, state_token))
    return RedirectResponse(url=f"{_AUTHORIZE_URL}?{authorize_query}", status_code=302)


async def _connect_handoff(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    request: Request,
    body: dict[str, Any],
) -> dict[str, Any]:
    client_id = str(
        body.get("client_id") or os.environ.get("FACEBOOK_APP_ID") or ""
    ).strip()
    redirect_uri = os.environ.get("FACEBOOK_REDIRECT_URI", "").strip()
    page_id = str(
        body.get("page_id") or os.environ.get("FACEBOOK_PAGE_ID") or ""
    ).strip()
    missing = [
        name
        for name, value in {
            "FACEBOOK_APP_ID": client_id,
            "FACEBOOK_APP_SECRET": load_app_secret_text_from_env("FACEBOOK_APP_SECRET"),
            "FACEBOOK_REDIRECT_URI": redirect_uri,
            "FACEBOOK_WEBHOOK_VERIFY_TOKEN": load_app_secret_text_from_env(
                "FACEBOOK_WEBHOOK_VERIFY_TOKEN",
            ),
        }.items()
        if not value
    ]
    install_url = None
    if not missing:
        state_token = await issue_facebook_state_token(
            tenant_id,
            pool,
            page_id=page_id or None,
        )
        install_url = f"{_AUTHORIZE_URL}?" + urlencode(
            _authorize_params(client_id, redirect_uri, state_token),
        )
    return {
        "install_url": install_url,
        "oauth_redirect_url": redirect_uri,
        "events_request_url": str(body.get("events_request_url") or "").strip()
        or "/integrations/facebook_pages/webhook",
        "provider_console_url": "https://developers.facebook.com/apps/",
        "missing_configuration": missing,
    }


def _authorize_params(
    client_id: str, redirect_uri: str, state_token: str
) -> dict[str, str]:
    return {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state_token,
        "response_type": "code",
        "scope": ",".join(_oauth_scopes()),
    }


def _oauth_scopes() -> tuple[str, ...]:
    override = os.environ.get("FACEBOOK_OAUTH_SCOPES", "").strip()
    if not override:
        return _SCOPES
    scopes = tuple(
        scope.strip()
        for chunk in override.split(",")
        for scope in chunk.split()
        if scope.strip()
    )
    return scopes or _SCOPES


def _error_redirect(reason: str) -> RedirectResponse:
    return RedirectResponse(
        url=f"{_ERROR_REDIRECT}?reason={reason}",
        status_code=302,
        headers={"X-Install-Error-Reason": reason},
    )


async def _persist_secrets(
    secret_store: Any,
    *,
    tenant_id: UUID,
    page_id: str,
    page_access_token: str,
) -> tuple[str, str, str]:
    app_secret = load_app_secret_text_from_env("FACEBOOK_APP_SECRET")
    verify_token = load_app_secret_text_from_env("FACEBOOK_WEBHOOK_VERIFY_TOKEN")
    if not app_secret or not verify_token:
        raise SecretStoreError(
            "FACEBOOK_APP_SECRET and FACEBOOK_WEBHOOK_VERIFY_TOKEN are required",
            reason="missing_facebook_pages_app_secret",
        )
    token_ref = await secret_store.put(
        page_access_token,
        label=f"facebook_pages_page_token:{page_id}",
        tenant_id=tenant_id,
    )
    app_secret_ref = await secret_store.put(
        app_secret,
        label=f"facebook_pages_app_secret:{page_id}",
        tenant_id=tenant_id,
    )
    verify_token_ref = await secret_store.put(
        verify_token,
        label=f"facebook_pages_verify_token:{page_id}",
        tenant_id=tenant_id,
    )
    return token_ref, app_secret_ref, verify_token_ref


async def _upsert_provider_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    page_id: str,
    app_secret_ref: str,
) -> tuple[UUID, bool]:
    row_id = uuid7()
    row = await conn.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, $3, $4, $5, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled = TRUE
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id,
        tenant_id,
        SOURCE,
        page_id,
        app_secret_ref,
    )
    if row is None:
        raise InstallationCollisionError(
            "page_id is already bound to a different Fyralis tenant",
        )
    return row["id"], bool(row["was_inserted"])


async def _upsert_page_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    page: dict[str, Any],
    token_ref: str,
    app_secret_ref: str,
    verify_token_ref: str,
    granted_scopes: list[str],
    subscribed: bool,
    subscribed_fields: tuple[str, ...],
) -> tuple[UUID, bool]:
    page_id = str(page["id"])
    row = await conn.fetchrow(
        """
        INSERT INTO facebook_page_installations (
            tenant_id, page_id, page_name, page_access_token_ref,
            app_secret_ref, verify_token_ref, granted_scopes,
            subscribed_fields, webhook_subscribed_at, enabled, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,
            CASE WHEN $8 THEN $9::text[] ELSE '{}'::text[] END,
            CASE WHEN $8 THEN now() ELSE NULL END,
            true, now()
        )
        ON CONFLICT (page_id) DO UPDATE SET
            tenant_id = EXCLUDED.tenant_id,
            page_name = COALESCE(
                EXCLUDED.page_name,
                facebook_page_installations.page_name
            ),
            page_access_token_ref = EXCLUDED.page_access_token_ref,
            app_secret_ref = EXCLUDED.app_secret_ref,
            verify_token_ref = EXCLUDED.verify_token_ref,
            granted_scopes = EXCLUDED.granted_scopes,
            subscribed_fields = EXCLUDED.subscribed_fields,
            webhook_subscribed_at = COALESCE(
                EXCLUDED.webhook_subscribed_at,
                facebook_page_installations.webhook_subscribed_at
            ),
            enabled = true,
            updated_at = now()
            WHERE facebook_page_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        tenant_id,
        page_id,
        page.get("name"),
        token_ref,
        app_secret_ref,
        verify_token_ref,
        granted_scopes,
        subscribed,
        list(subscribed_fields),
    )
    if row is None:
        raise InstallationCollisionError(
            "page_id is already bound to a different Fyralis tenant",
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
    await conn.execute(
        """
        INSERT INTO onboarding_triggers (
            id, tenant_id, source, trigger_kind,
            installation_row_id, payload
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        ON CONFLICT (tenant_id, source, installation_row_id)
            WHERE installation_row_id IS NOT NULL
            DO NOTHING
        """,
        uuid7(),
        tenant_id,
        SOURCE,
        trigger_kind,
        installation_row_id,
        json.dumps(payload),
    )


async def callback_handler(request: Request) -> Any:
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")
    if not code or not state:
        return _error_redirect("state_invalid")

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return _error_redirect("secret_store_unavailable")

    try:
        tenant_id, payload = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as exc:
        log.info("facebook_pages_install_failure", reason=exc.reason)
        return _error_redirect(exc.reason)

    client_id = os.environ.get("FACEBOOK_APP_ID", "").strip()
    client_secret = load_app_secret_text_from_env("FACEBOOK_APP_SECRET")
    redirect_uri = os.environ.get("FACEBOOK_REDIRECT_URI", "").strip()
    if not client_id or not client_secret or not redirect_uri:
        return _error_redirect("facebook_pages_unconfigured")

    client = FacebookPagesClient()
    try:
        token_response = await client.exchange_code(
            code=code,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
        )
        user_token = token_response.get("access_token")
        if not isinstance(user_token, str) or not user_token:
            return _error_redirect("facebook_oauth_error")
        pages = await client.list_pages(user_token)
        page = _select_page(
            pages,
            requested_page_id=(
                str(payload.get("page_id") or "").strip()
                or os.environ.get("FACEBOOK_PAGE_ID")
            ),
        )
        if page is None:
            return _error_redirect("facebook_page_not_found")
        page_id = str(page["id"])
        page_token = page.get("access_token")
        if not isinstance(page_token, str) or not page_token:
            return _error_redirect("facebook_page_token_missing")
        subscribe_response = await client.subscribe_page(
            page_id=page_id,
            page_access_token=page_token,
            fields=FACEBOOK_PAGES_WEBHOOK_FIELDS,
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "facebook_pages_install_failure",
            reason="facebook_oauth_error",
            error_type=type(exc).__name__,
        )
        return _error_redirect("facebook_oauth_error")
    finally:
        await client.aclose()

    try:
        token_ref, app_secret_ref, verify_token_ref = await _persist_secrets(
            secret_store,
            tenant_id=tenant_id,
            page_id=page_id,
            page_access_token=page_token,
        )
    except SecretStoreError:
        return _error_redirect("secret_store_unavailable")

    granted_scopes = _granted_scopes(token_response)
    subscribed = subscribe_response.get("success") is True
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _upsert_provider_installation(
                    conn,
                    tenant_id=tenant_id,
                    page_id=page_id,
                    app_secret_ref=app_secret_ref,
                )
                page_install_id, was_inserted = await _upsert_page_installation(
                    conn,
                    tenant_id=tenant_id,
                    page=page,
                    token_ref=token_ref,
                    app_secret_ref=app_secret_ref,
                    verify_token_ref=verify_token_ref,
                    granted_scopes=granted_scopes,
                    subscribed=subscribed,
                    subscribed_fields=FACEBOOK_PAGES_WEBHOOK_FIELDS,
                )
                await _emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=page_install_id,
                    trigger_kind=("install" if was_inserted else "reinstall"),
                    payload={
                        "page_id": page_id,
                        "page_name": page.get("name"),
                        "coverage": "All available history",
                    },
                )
    except InstallationCollisionError:
        return _error_redirect("installation_collision")

    return RedirectResponse(
        url=f"{_SUCCESS_REDIRECT}?page={short_page_hash(page_id)}",
        status_code=302,
    )


def _select_page(
    pages: list[dict[str, Any]],
    *,
    requested_page_id: str | None,
) -> dict[str, Any] | None:
    eligible = [
        p
        for p in pages
        if isinstance(p.get("id"), str) and isinstance(p.get("access_token"), str)
    ]
    if requested_page_id:
        for page in eligible:
            if page.get("id") == requested_page_id:
                return page
        return None
    return eligible[0] if eligible else None


def _granted_scopes(token_response: dict[str, Any]) -> list[str]:
    raw = token_response.get("scope") or token_response.get("granted_scopes")
    if isinstance(raw, str):
        return [s for s in raw.replace(" ", ",").split(",") if s]
    if isinstance(raw, list):
        return [str(s) for s in raw if str(s)]
    return list(_SCOPES)


router = build_oauth_native_connect_router(
    source=SOURCE,
    authorization_mode="oauth",
    provider_console_url="https://developers.facebook.com/apps/",
    payload_fields=[
        "page_id",
        "oauth_redirect_url",
        "events_request_url",
        "installation_id",
    ],
    build_handoff=_connect_handoff,
)


__all__ = [
    "DISPLAY_NAME",
    "SOURCE",
    "callback_handler",
    "install_handler",
    "issue_facebook_state_token",
    "router",
    "short_page_hash",
]
