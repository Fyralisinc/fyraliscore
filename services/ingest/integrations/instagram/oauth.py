"""Instagram Login installation and lifecycle endpoints.

The Fyralis deployment owns the Meta app credentials. A tenant admin authorizes
their professional account through Business Login for Instagram; no browser or
API caller ever submits an app secret or webhook verify token.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

import asyncpg
import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import InstagramApiError, StateTokenInvalidError
from lib.shared.ids import uuid7
from lib.shared.secrets import SecretNotFoundError, load_app_secret_text_from_env
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.instagram.client import InstagramClient
from services.ingest.integrations.instagram.onboarding import finalize_install
from services.ingest.integrations.slack.oauth import (
    issue_state_token,
    verify_and_consume_state,
)


log = structlog.get_logger("integrations.instagram.oauth")
router = APIRouter(prefix="/integrations/instagram", tags=["instagram"])

_AUTHORIZE_URL = "https://www.instagram.com/oauth/authorize"
_TOKEN_URL = "https://api.instagram.com/oauth/access_token"
_REFRESH_URL = "https://graph.instagram.com/refresh_access_token"
_DEFAULT_SCOPES = ("instagram_business_basic", "instagram_business_manage_messages")
_DEFAULT_WEBHOOK_FIELDS = (
    "messages",
    "messaging_postbacks",
    "messaging_seen",
    "messaging_referral",
    "message_reactions",
)


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    return auth.tenant_id if isinstance(auth.tenant_id, UUID) else UUID(str(auth.tenant_id))


def _pool(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="database pool unavailable")
    return pool


def _secret_store(request: Request) -> Any:
    store = getattr(request.app.state, "secret_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="secret store unavailable")
    return store


def _configured_scopes() -> tuple[str, ...]:
    raw = os.environ.get("INSTAGRAM_LOGIN_SCOPES", ",".join(_DEFAULT_SCOPES))
    return tuple(sorted({item.strip() for item in raw.split(",") if item.strip()}))


def _configured_webhook_fields() -> list[str]:
    raw = os.environ.get("INSTAGRAM_WEBHOOK_FIELDS", ",".join(_DEFAULT_WEBHOOK_FIELDS))
    return sorted({item.strip() for item in raw.split(",") if item.strip()})


def _config() -> tuple[str, str, str, str]:
    app_id = os.environ.get("INSTAGRAM_APP_ID", "").strip()
    app_secret = load_app_secret_text_from_env("INSTAGRAM_APP_SECRET")
    redirect_uri = os.environ.get("INSTAGRAM_OAUTH_REDIRECT_URI", "").strip()
    if not app_id or not app_secret or not redirect_uri:
        raise InstagramApiError(
            "Instagram Login is not configured",
            code="instagram_api_error",
            context={"missing_app_id": not bool(app_id), "missing_redirect_uri": not bool(redirect_uri)},
        )
    return app_id, app_secret, redirect_uri, os.environ.get("INSTAGRAM_API_BASE_URL", "https://graph.instagram.com").rstrip("/")


def _token_expiry(body: dict[str, Any]) -> datetime | None:
    try:
        seconds = int(body.get("expires_in"))
    except (TypeError, ValueError):
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=max(0, seconds))


def _webhook_delivery_account_id(body: dict[str, Any]) -> str | None:
    """Return Meta's OAuth delivery id when the code exchange provides one."""
    for field in ("user_id", "instagram_user_id", "ig_user_id"):
        value = str(body.get(field) or "").strip()
        if value:
            return value
    return None


async def _exchange_code(code: str) -> dict[str, Any]:
    app_id, app_secret, redirect_uri, _base_url = _config()
    url = os.environ.get("INSTAGRAM_OAUTH_TOKEN_URL", _TOKEN_URL)
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                url,
                data={
                    "client_id": app_id,
                    "client_secret": app_secret,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                    "code": code,
                },
            )
    except httpx.TransportError as exc:
        raise InstagramApiError("Instagram OAuth token exchange failed", code="instagram_api_error") from exc
    if response.status_code // 100 != 2:
        raise InstagramApiError(
            "Instagram rejected the authorization code",
            code="instagram_api_unauthorized" if response.status_code in {400, 401, 403} else "instagram_api_error",
            context={"http_status": response.status_code},
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise InstagramApiError("Instagram OAuth response was invalid", code="instagram_api_error") from exc
    if not isinstance(body, dict) or not isinstance(body.get("access_token"), str):
        raise InstagramApiError("Instagram OAuth response omitted an access token", code="instagram_api_error")
    return body


async def _discover_and_subscribe(
    *,
    access_token: str,
    base_url: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    client = InstagramClient(base_url=base_url, access_token=access_token)
    fields = _configured_webhook_fields()
    try:
        account = await client.validate_account()
        account_id = str(account.get("id") or "").strip()
        if not account_id:
            raise InstagramApiError("Instagram account discovery returned no account id")
        await client.subscribe_webhooks(ig_business_account_id=account_id, fields=fields)
        try:
            max_pages = max(
                1,
                min(100, int(os.environ.get("INSTAGRAM_CONNECT_DISCOVERY_MAX_PAGES", "20"))),
            )
        except ValueError:
            max_pages = 20
        conversations: list[dict[str, Any]] = []
        after: str | None = None
        for _ in range(max_pages):
            page, after = await client.list_conversations(
                ig_business_account_id=account_id,
                limit=50,
                after=after,
            )
            conversations.extend(page)
            if not after:
                break
    finally:
        await client.aclose()
    return account, conversations, fields


async def install_handler(request: Request) -> RedirectResponse | JSONResponse:
    try:
        tenant_id = _tenant_from_request(request)
        app_id, _secret, redirect_uri, _base_url = _config()
        state = await issue_state_token(tenant_id, _pool(request), provider="instagram")
    except InstagramApiError as exc:
        return JSONResponse({"ok": False, "code": exc.code, "message": exc.message}, status_code=500)
    query = urlencode(
        {
            "client_id": app_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": ",".join(_configured_scopes()),
            "state": state,
        }
    )
    return RedirectResponse(
        url=f"{os.environ.get('INSTAGRAM_OAUTH_AUTHORIZE_URL', _AUTHORIZE_URL)}?{query}",
        status_code=302,
    )


async def callback_handler(request: Request) -> JSONResponse:
    provider_error = request.query_params.get("error")
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")
    if provider_error or not code or not state:
        return JSONResponse({"ok": False, "code": "instagram_oauth_denied"}, status_code=400)
    try:
        pool = _pool(request)
        tenant_id, _payload = await verify_and_consume_state(
            state,
            pool,
            expected_provider="instagram",
        )
        token_body = await _exchange_code(code)
        app_id, _app_secret, _redirect_uri, base_url = _config()
        account, conversations, subscription_fields = await _discover_and_subscribe(
            access_token=token_body["access_token"],
            base_url=base_url,
        )
        account_id = str(account.get("id") or "").strip()
        if not account_id:
            raise InstagramApiError("Instagram account discovery returned no account id")
        store = _secret_store(request)
        access_ref = await store.put(
            token_body["access_token"],
            label=f"instagram_access_token:{account_id}",
            tenant_id=tenant_id,
        )
        try:
            installation_id = await finalize_install(
                pool,
                tenant_id=tenant_id,
                base_url=base_url,
                ig_business_account_id=account_id,
                page_id=None,
                instagram_username=account.get("username") if isinstance(account.get("username"), str) else None,
                display_name=account.get("name") if isinstance(account.get("name"), str) else None,
                app_id=app_id,
                access_token_ref=access_ref,
                webhook_delivery_account_id=_webhook_delivery_account_id(token_body),
                token_expires_at=_token_expiry(token_body),
                auth_model="instagram_login_business",
                granted_scopes=list(_configured_scopes()),
                webhook_subscription_fields=subscription_fields,
                webhook_subscribed_at=datetime.now(timezone.utc),
                conversations=conversations,
            )
        except Exception:
            await store.delete(access_ref, tenant_id=tenant_id)
            raise
    except StateTokenInvalidError as exc:
        return JSONResponse({"ok": False, "code": exc.code}, status_code=400)
    except InstagramApiError as exc:
        log.info("instagram.oauth.callback_failed", code=exc.code)
        return JSONResponse({"ok": False, "code": exc.code, "message": exc.message}, status_code=400)
    except Exception as exc:  # noqa: BLE001
        log.exception("instagram.oauth.callback_failed", error_type=type(exc).__name__)
        return JSONResponse({"ok": False, "code": "instagram_install_failed"}, status_code=500)
    return JSONResponse(
        {
            "ok": True,
            "installation_id": str(installation_id),
            "ig_business_account_id": account_id,
            "conversation_count_sample": len(conversations),
        }
    )


@router.get("/status")
async def status(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    async with tenant_transaction(tenant_id, pool=_pool(request)) as conn:
        row = await conn.fetchrow(
            """
            SELECT ig_business_account_id, instagram_username, display_name,
                   connection_status, token_expires_at, webhook_subscribed_at,
                   webhook_subscription_fields, conversation_discovered_at,
                   last_error_code, last_error_at, disabled_at
              FROM instagram_installations
             WHERE tenant_id = $1
             ORDER BY created_at DESC
             LIMIT 1
            """,
            tenant_id,
        )
    if row is None:
        return JSONResponse({"ok": True, "connected": False})
    return JSONResponse(
        jsonable_encoder(
            {
                "ok": True,
                "connected": row["disabled_at"] is None,
                "installation": dict(row),
            }
        )
    )


@router.post("/disconnect")
async def disconnect(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    store = _secret_store(request)
    async with tenant_transaction(tenant_id, pool=_pool(request)) as conn:
        row = await conn.fetchrow(
            """
            SELECT id, access_token_ref FROM instagram_installations
             WHERE tenant_id = $1 AND disabled_at IS NULL
             ORDER BY created_at DESC
             LIMIT 1 FOR UPDATE
            """,
            tenant_id,
        )
        if row is None:
            return JSONResponse({"ok": True, "disconnected": False})
        await conn.execute(
            """
            UPDATE instagram_installations
               SET disabled_at = now(), connection_status = 'revoked',
                   access_token_ref = NULL, updated_at = now()
             WHERE id = $1
            """,
            row["id"],
        )
        await conn.execute(
            """
            UPDATE instagram_webhook_routes
               SET enabled = FALSE, updated_at = now()
             WHERE resolved_tenant_id = $1 AND instagram_installation_id = $2
            """,
            tenant_id,
            row["id"],
        )
        await conn.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider, action, status, context)
            VALUES ($1, $2, NULL, 'instagram', 'uninstall', 'ok', $3::jsonb)
            """,
            uuid7(),
            tenant_id,
            json.dumps({
                "initiated_by": "tenant_admin",
                "instagram_installation_id": str(row["id"]),
            }),
        )
    if row["access_token_ref"]:
        try:
            await store.delete(str(row["access_token_ref"]), tenant_id=tenant_id)
        except SecretNotFoundError:
            pass
    return JSONResponse({"ok": True, "disconnected": True})


@router.post("/connect/preflight", deprecated=True)
async def connect_preflight() -> JSONResponse:
    return JSONResponse(
        {"ok": False, "code": "instagram_oauth_required", "install_path": "/integrations/instagram/install"},
        status_code=410,
    )


@router.post("/connect/finalize", deprecated=True)
async def connect_finalize() -> JSONResponse:
    return JSONResponse(
        {"ok": False, "code": "instagram_oauth_required", "install_path": "/integrations/instagram/install"},
        status_code=410,
    )


__all__ = ["callback_handler", "install_handler", "router"]
