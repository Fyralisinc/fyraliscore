"""services/ingest/integrations/notion/oauth.py — Notion OAuth install + callback.

Flow (mirrors IN-08 Slack / IN-13 GitHub on the same substrate):

    GET /integrations/notion/install   (Bearer-auth, tenant from session)
        → INSERT oauth_install_states (nonce, tenant, expires_at)
        → 302 to https://api.notion.com/v1/oauth/authorize?...&state=<token>

    GET /integrations/notion/callback  (public, state-token-authed)
        → verify HMAC, consume nonce atomically
        → POST /v1/oauth/token  (Basic client_id:client_secret; exchange code)
        → secret_store.put(access_token, label="notion_token:<workspace_id>")
        → UPSERT provider_installations (provider='notion', cross-tenant guard)
        → emit onboarding_triggers (source='notion') in the same txn
        → INSERT installation_audit_log
        → 302 to /integrations/notion/installed?workspace=<short_hash>

Differences from Slack/GitHub:
  - Notion bot tokens are LONG-LIVED — no per-request mint / refresh.
  - There is no inbound webhook signature secret to persist: Notion has
    no reliable content push, so `secret_ref` points straight at the bot
    token (the backfill/poll fetcher reads it for outbound calls). No
    `services/app/webhooks/secrets.py` involvement.

Security properties carry over unchanged: the state token's tenant_id is
bound at issuance from the authenticated session (never a query param);
the nonce is single-use; cross-tenant rebind returns 409
`installation_collision` without disclosing the foreign tenant.
"""
from __future__ import annotations

import base64
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
    InstallationCollisionError,
    SecretStoreError,
    StateTokenInvalidError,
)
from lib.shared.ids import uuid7
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RetryLater,
    parse_retry_after,
)
from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.integrations.notion import metrics
from services.ingest.integrations.notion.client import short_workspace_hash
from services.ingest.integrations.provider_transport import (
    ProviderRequestBinding,
    tenant_preinstall_transport_kwargs,
)
# Reuse the IN-08 state-token primitives (provider-parameterized).
from services.ingest.integrations.slack.oauth import (
    issue_state_token,
    verify_and_consume_state,
)
from services.ingest.integrations.oauth_native_connect import (
    build_oauth_native_connect_router,
)


log = structlog.get_logger("integrations.notion.oauth")


_NOTION_AUTHORIZE_URL = "https://api.notion.com/v1/oauth/authorize"
_NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token"

_SUCCESS_REDIRECT = "/integrations/notion/installed"
_ERROR_REDIRECT = "/integrations/notion/install-error"


# ---------------------------------------------------------------------
# Install handler — GET /integrations/notion/install
# ---------------------------------------------------------------------

async def install_handler(request: Request) -> Any:
    """Issue a state token for the authenticated session's tenant and
    redirect to Notion's OAuth consent screen."""
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        return JSONResponse(
            {
                "code": "missing_bearer",
                "message": "install requires an authenticated session",
                "context": {"provider": "notion"},
            },
            status_code=401,
        )

    client_id = os.environ.get("NOTION_CLIENT_ID")
    redirect_uri = os.environ.get("NOTION_REDIRECT_URI")
    if not client_id or not redirect_uri:
        log.error(
            "notion_install_unconfigured",
            has_client_id=bool(client_id),
            has_redirect_uri=bool(redirect_uri),
        )
        metrics.record_install_outcome("notion_unconfigured")
        return JSONResponse(
            {
                "code": "notion_unconfigured",
                "message": "NOTION_CLIENT_ID or NOTION_REDIRECT_URI not set",
                "context": {"provider": "notion"},
            },
            status_code=500,
        )

    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        return JSONResponse(
            {
                "code": "service_unavailable",
                "message": "gateway pool not initialised",
                "context": {"provider": "notion"},
            },
            status_code=503,
        )

    state_token = await issue_state_token(
        auth.tenant_id, pool, provider="notion",
    )
    metrics.record_install_outcome("initiated")

    from urllib.parse import urlencode

    qs = urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "owner": "user",
            "redirect_uri": redirect_uri,
            "state": state_token,
        }
    )
    return RedirectResponse(url=f"{_NOTION_AUTHORIZE_URL}?{qs}", status_code=302)


async def _connect_handoff(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    request: Request,
    body: dict[str, Any],
) -> dict[str, Any]:
    client_id = str(body.get("client_id") or os.environ.get("NOTION_CLIENT_ID") or "").strip()
    redirect_uri = os.environ.get("NOTION_REDIRECT_URI", "").strip()
    missing = [
        name
        for name, value in {
            "NOTION_CLIENT_ID": client_id,
            "NOTION_REDIRECT_URI": redirect_uri,
            "NOTION_CLIENT_SECRET": load_app_secret_text_from_env("NOTION_CLIENT_SECRET"),
        }.items()
        if not value
    ]
    install_url = None
    if not missing:
        from urllib.parse import urlencode

        state_token = await issue_state_token(tenant_id, pool, provider="notion")
        install_url = f"{_NOTION_AUTHORIZE_URL}?" + urlencode(
            {
                "client_id": client_id,
                "response_type": "code",
                "owner": "user",
                "redirect_uri": redirect_uri,
                "state": state_token,
            }
        )
    return {
        "install_url": install_url,
        "oauth_redirect_url": redirect_uri,
        "events_request_url": str(body.get("events_request_url") or "").strip() or None,
        "provider_console_url": "https://www.notion.so/my-integrations",
        "missing_configuration": missing,
    }


# ---------------------------------------------------------------------
# Callback handler — GET /integrations/notion/callback
# ---------------------------------------------------------------------

async def _exchange_code_for_token(
    code: str,
    *,
    tenant_id: UUID,
    http_client: httpx.AsyncClient | None = None,
    token_url: str | None = None,
) -> dict[str, Any]:
    """Call Notion's `/v1/oauth/token`. Basic-auths with
    client_id:client_secret and returns the parsed JSON
    ({access_token, workspace_id, workspace_name, bot_id, ...})."""
    client_id = os.environ.get("NOTION_CLIENT_ID", "")
    client_secret = load_app_secret_text_from_env("NOTION_CLIENT_SECRET")
    redirect_uri = os.environ.get("NOTION_REDIRECT_URI", "")
    basic = base64.b64encode(
        f"{client_id}:{client_secret}".encode("utf-8"),
    ).decode("ascii")
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=15.0)
    binding_kwargs = tenant_preinstall_transport_kwargs(tenant_id)
    provider = ProviderRequestBinding(
        source="notion",
        tenant_id=str(binding_kwargs["tenant_id"]),
        installation_id=None,
        transport=binding_kwargs.get("provider_transport"),
        request_policy=None,
        quota_resolver=binding_kwargs.get("quota_resolver"),
        allow_unlimited_local=bool(
            binding_kwargs.get("allow_unlimited_local"),
        ),
        require_tenant=True,
        require_installation=False,
    )

    async def _once() -> httpx.Response:
        try:
            response = await client.post(
                token_url
                or os.environ.get("NOTION_OAUTH_TOKEN_URL")
                or _NOTION_TOKEN_URL,
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type": "application/json",
                },
                json={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(
                "Notion OAuth token exchange timed out",
                source="notion",
                operation="oauth.token.exchange",
                error_type=type(exc).__name__,
            ) from exc
        except httpx.TransportError as exc:
            raise ProviderTransientError(
                "Notion OAuth token exchange transport error",
                source="notion",
                operation="oauth.token.exchange",
                error_type=type(exc).__name__,
            ) from exc
        if response.status_code == 429:
            raise ProviderRateLimited(
                "Notion OAuth token exchange rate limit",
                retry_after_seconds=parse_retry_after(
                    response.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source="notion",
                operation="oauth.token.exchange",
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                f"Notion OAuth token exchange returned HTTP "
                f"{response.status_code}",
                source="notion",
                operation="oauth.token.exchange",
                http_status=response.status_code,
            )
        return response

    try:
        r = await provider.execute("oauth.token.exchange", _once)
    finally:
        if owns_client:
            await client.aclose()
    r.raise_for_status()
    body = r.json()
    if not isinstance(body, dict):
        raise ValueError("Notion OAuth token response must be a JSON object")
    return body


async def _upsert_installation(
    executor: asyncpg.Pool | asyncpg.Connection,
    tenant_id: UUID,
    workspace_id: str,
    secret_ref_value: str,
) -> tuple[UUID, bool]:
    """UPSERT a `provider_installations` row keyed by
    `(provider='notion', installation_id=workspace_id)`. The conflict path
    only updates when the existing row's tenant matches; otherwise raises
    `InstallationCollisionError`. Returns `(installation_row_id,
    was_inserted)`."""
    row_id = uuid7()
    row = await executor.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'notion', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled    = TRUE
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id, (xmax = 0) AS was_inserted
        """,
        row_id,
        tenant_id,
        workspace_id,
        secret_ref_value,
    )
    if row is None:
        raise InstallationCollisionError(
            "workspace_id is already bound to a different Fyralis tenant",
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
    """Write an onboarding_triggers row atomically with the install.
    Idempotent via migration 0057's partial unique index on
    (tenant_id, source, installation_row_id)."""
    await conn.execute(
        """
        INSERT INTO onboarding_triggers (
            id, tenant_id, source, trigger_kind,
            installation_row_id, payload
        ) VALUES ($1, $2, 'notion', $3, $4, $5::jsonb)
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
            VALUES ($1, $2, $3, 'notion', $4, $5, $6::jsonb)
            """,
            uuid7(), tenant_id, installation_row_id, action, status,
            json.dumps(context or {}),
        )
    except Exception as exc:  # noqa: BLE001 — audit is best-effort
        log.error(
            "installation_audit_log_write_failed",
            action=action, status=status, error_type=type(exc).__name__,
        )


def _invalidate_resolver_cache(request: Request, workspace_id: str) -> None:
    resolver = getattr(request.app.state, "tenant_resolver", None)
    if resolver is None:
        return
    cache = getattr(resolver, "_cache", None)
    if cache is None:
        return
    try:
        cache.invalidate(("notion", workspace_id))
    except Exception:  # noqa: BLE001
        pass


def _error_redirect(reason: str) -> RedirectResponse:
    metrics.record_install_outcome(reason)
    return RedirectResponse(
        url=f"{_ERROR_REDIRECT}?reason={reason}",
        status_code=302,
        headers={"X-Install-Error-Reason": reason},
    )


async def callback_handler(request: Request) -> Any:
    """GET /integrations/notion/callback. Public route, state-token authed."""
    started_at = time.monotonic()
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")

    if not code or not state:
        log.info("notion_install_failure", reason="state_invalid")
        return _error_redirect("state_invalid")

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return _error_redirect("secret_store_unavailable")

    # Verify HMAC + atomic consume.
    try:
        tenant_id, _payload = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as e:
        log.info("notion_install_failure", reason=e.reason)
        return _error_redirect(e.reason)

    # Exchange code for a long-lived bot token.
    try:
        token_response = await _exchange_code_for_token(
            code,
            tenant_id=tenant_id,
        )
    except RetryLater:
        raise
    except Exception as exc:  # noqa: BLE001 — Notion API / transport error
        log.error(
            "notion_install_failure",
            reason="notion_oauth_error",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "notion_oauth_error"},
        )
        return _error_redirect("notion_oauth_error")

    access_token = token_response.get("access_token")
    workspace_id = token_response.get("workspace_id")
    if not isinstance(access_token, str) or not access_token:
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "notion_oauth_error", "detail": "access_token missing"},
        )
        return _error_redirect("notion_oauth_error")
    if not isinstance(workspace_id, str) or not workspace_id:
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "notion_oauth_error", "detail": "workspace_id missing"},
        )
        return _error_redirect("notion_oauth_error")

    # Persist the bot token; secret_ref points straight at it.
    try:
        secret_ref = await secret_store.put(
            access_token,
            label=f"notion_token:{workspace_id}",
            tenant_id=tenant_id,
        )
    except SecretStoreError as exc:
        log.error(
            "notion_install_failure",
            reason="secret_store_unavailable",
            error_type=type(exc).__name__,
        )
        await _write_audit(
            pool, tenant_id, None, "install", "error",
            {"failure_code": "secret_store_unavailable"},
        )
        return _error_redirect("secret_store_unavailable")

    # UPSERT install + emit onboarding trigger atomically.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                installation_row_id, was_inserted = await _upsert_installation(
                    conn, tenant_id, workspace_id, secret_ref,
                )
                await _emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=installation_row_id,
                    trigger_kind=("install" if was_inserted else "reinstall"),
                    payload={"workspace_id": workspace_id},
                )
    except InstallationCollisionError:
        log.info("notion_install_failure", reason="installation_collision")
        await _write_audit(
            pool, tenant_id, None, "install", "rejected_collision",
            {"failure_code": "installation_collision"},
        )
        return _error_redirect("installation_collision")

    await _write_audit(
        pool, tenant_id, installation_row_id, "install", "ok",
        {
            "was_reinstall": not was_inserted,
            "workspace_name": token_response.get("workspace_name"),
            "bot_id": token_response.get("bot_id"),
        },
    )

    _invalidate_resolver_cache(request, workspace_id)
    metrics.record_install_outcome("success")
    log.info(
        "notion_install_ok",
        workspace_id_hash=short_workspace_hash(workspace_id),
        was_reinstall=not was_inserted,
        duration_s=round(time.monotonic() - started_at, 3),
    )

    return RedirectResponse(
        url=f"{_SUCCESS_REDIRECT}?workspace={short_workspace_hash(workspace_id)}",
        status_code=302,
    )


router = build_oauth_native_connect_router(
    source="notion",
    authorization_mode="oauth",
    provider_console_url="https://www.notion.so/my-integrations",
    payload_fields=[
        "workspace_id",
        "shared_page_ids",
        "shared_database_ids",
        "oauth_redirect_url",
        "events_request_url",
        "installation_id",
    ],
    build_handoff=_connect_handoff,
)


__all__ = ["install_handler", "callback_handler", "router"]
