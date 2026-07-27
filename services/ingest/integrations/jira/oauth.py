"""services/ingest/integrations/jira/oauth.py — admin connect wizard (IN-17).

Jira authenticates with an account email + a long-lived API token against a
per-tenant site (https://<site>.atlassian.net), not an OAuth bounce or DWD
service account. This is the production install surface the audit flagged as
missing: `finalize_install` / `register_webhook_installation` were reachable
only from `scripts/sandbox_jira*.py`. It mirrors the prod-shaped flow of
`scripts/sandbox_jira_seed.py` over HTTP, behind Bearer auth.

Unlike the dev-only `finance_router` panel (Mercury/QuickBooks — `X-Tenant-Id`
header + synthetic credentials), this is a genuine production surface: the
tenant comes from the Bearer-authed `request.state.auth`, the operator submits
their real API token + (optional) webhook secret, and both are persisted
encrypted-at-rest via the gateway's `secret_store` — only opaque refs touch the
install tables.

Flow:

    POST /integrations/jira/connect/preflight
        body: { base_url, account_email, api_token }
        → JiraClient.myself() to verify the credentials
        → list_projects() to enumerate the projects for the selector UI
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/jira/connect/finalize
        body: { base_url, account_email, api_token,
                project_keys?, webhook_secret? }
        → re-verify creds, resolve project_keys (enumerate if omitted)
        → store the API token (and webhook secret, if given) in the secret store
        → finalize_install(): UPSERT jira_installations + jira_projects +
          an onboarding_triggers row (source='jira') so the M6 backfill chain
          fires; register_webhook_installation() seeds the provider_installations
          row the webhook edge resolves the tenant + HMAC secret from
        → 200 OK with the new jira_installations.id

Backfill (jira_installations) and the live webhook edge (provider_installations)
are seeded together but stay independent — exactly as `finalize_install` /
`register_webhook_installation` document.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import JiraApiError
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.jira.client import JiraClient
from services.ingest.integrations.jira.onboarding import (
    finalize_install,
    register_webhook_installation,
    site_host,
)
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


log = structlog.get_logger("integrations.jira.oauth")


router = APIRouter(prefix="/integrations/jira", tags=["jira"])


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    tid = auth.tenant_id
    return tid if isinstance(tid, UUID) else UUID(str(tid))


def _pool_from_request(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="database pool unavailable")
    return pool


def _secret_store_from_request(request: Request) -> Any:
    store = getattr(request.app.state, "secret_store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="secret store unavailable")
    return store


def _require_credentials(body: dict[str, Any]) -> tuple[str, str, str]:
    """Pull + validate the three required credential fields."""
    account_email = (body.get("account_email") or "").strip()
    api_token = (body.get("api_token") or "").strip()

    if "@" not in account_email:
        raise HTTPException(status_code=400, detail="account_email is required")
    if not api_token:
        raise HTTPException(status_code=400, detail="api_token is required")
    base_url = native_connect_base_url(
        body.get("base_url"),
        endpoint_name="jira_api",
        installation_owned=True,
        allowed_hostname_suffixes=(".atlassian.net",),
    )
    return base_url, account_email, api_token


def _auth_failure_response(exc: JiraApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token and
    Basic-auth header are never echoed back (JiraApiError keeps them off
    context by design)."""
    unauthorized = getattr(exc, "code", "") == "jira_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "jira_auth_failed" if unauthorized else "jira_api_error",
            "message": (
                "Jira rejected the API token / account email. Generate a token "
                "at id.atlassian.com and confirm the account can see the site."
                if unauthorized
                else "Could not reach the Jira site. Check the base_url and "
                "that the site is online."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


async def _enumerate_projects(
    client: JiraClient,
) -> tuple[list[str], dict[str, dict]]:
    """Page through project/search → (sorted keys, per-key meta)."""
    keys: list[str] = []
    meta: dict[str, dict] = {}
    start = 0
    while True:
        page, nxt, _total = await client.list_projects(start_at=start)
        for pr in page:
            key = pr.get("key")
            if not key:
                continue
            keys.append(key)
            meta[key] = {"project_id": pr.get("id"), "project_name": pr.get("name")}
        if nxt is None:
            break
        start = nxt
    return keys, meta


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the API token and enumerate projects for the selector UI."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    base_url, account_email, api_token = _require_credentials(body)

    client = JiraClient(
        base_url=base_url,
        account_email=account_email,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        me = await client.myself()
        keys, meta = await _enumerate_projects(client)
    except JiraApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    return JSONResponse(content={
        "ok": True,
        "site_host": site_host(base_url),
        "account": {
            "account_id": me.get("accountId"),
            "display_name": me.get("displayName"),
            "email": me.get("emailAddress"),
        },
        "projects": [
            {"key": k, "id": meta[k].get("project_id"),
             "name": meta[k].get("project_name")}
            for k in keys
        ],
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Persist credentials + install the source.

    Credentials are verified BEFORE any secret is written, so an invalid token
    leaves no `encrypted_secrets` / install rows behind.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    base_url, account_email, api_token = _require_credentials(body)

    requested_keys = body.get("project_keys")
    if requested_keys is not None and not isinstance(requested_keys, list):
        raise HTTPException(status_code=400, detail="project_keys must be a list")
    webhook_secret = (body.get("webhook_secret") or "").strip() or None

    # 1. Verify creds (and resolve the project set if not pinned) — before any
    #    write, so a bad token can't leave half-state.
    client = JiraClient(
        base_url=base_url,
        account_email=account_email,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        await client.myself()
        if requested_keys:
            project_keys = [str(k).strip() for k in requested_keys if str(k).strip()]
            project_meta: dict[str, dict] = {}
        else:
            project_keys, project_meta = await _enumerate_projects(client)
    except JiraApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        api_token, label=f"jira_api_token:{base_url}", tenant_id=tenant_id,
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"jira_webhook_secret:{base_url}",
            tenant_id=tenant_id,
        )

    # 3. Install: jira_installations + jira_projects + onboarding trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        account_email=account_email,
        project_keys=project_keys,
        secret_ref=secret_ref,
        webhook_secret_ref=webhook_secret_ref,
        project_meta=project_meta,
    )

    # 4. Live webhook edge (only when a signing secret was supplied).
    if webhook_secret_ref:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            base_url=base_url,
            webhook_secret_ref=webhook_secret_ref,
        )

    log.info(
        "jira.connect.finalized",
        installation_id=str(install_id),
        project_count=len(project_keys),
        webhook_registered=webhook_secret_ref is not None,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "site_host": site_host(base_url),
        "project_count": len(project_keys),
        "webhook_registered": webhook_secret_ref is not None,
    })


__all__ = ["router"]
