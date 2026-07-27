"""services/ingest/integrations/mercury/oauth.py — admin connect wizard (finance).

Mercury authenticates with a long-lived Bearer API token (Jira-shaped). This is
the production install surface the audit flagged as missing: `finalize_install`
/ `register_webhook_installation` were reachable only through the dev
`finance_router` panel (synthetic data, `X-Tenant-Id` header). It mirrors the
Jira connect wizard — Bearer-authed, real credentials persisted encrypted via
the gateway `secret_store` (only opaque refs reach the install tables).

Flow:

    POST /integrations/mercury/connect/preflight
        body: { api_token, base_url? }
        → MercuryClient.list_accounts() to verify the token + enumerate the
          accounts for the selector UI
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/mercury/connect/finalize
        body: { api_token, base_url?, account_ids?, organization_id?,
                webhook_secret? }
        → re-verify creds, resolve the account set (all enumerated, or the
          `account_ids` subset)
        → store the API token (+ webhook secret, if given) in the secret store
        → finalize_install(): UPSERT mercury_installations + mercury_accounts +
          an onboarding_triggers row (source='mercury') so the M6 backfill chain
          fires; when an organization_id + webhook secret are supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + HMAC secret from
        → 200 OK with the new mercury_installations.id

Backfill (mercury_installations) and the live webhook edge
(provider_installations) are seeded together but stay independent.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import MercuryApiError
from services.ingest.integrations.mercury.client import MercuryClient
from services.ingest.integrations.mercury.onboarding import (
    finalize_install,
    register_webhook_installation,
)
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


log = structlog.get_logger("integrations.mercury.oauth")


router = APIRouter(prefix="/integrations/mercury", tags=["mercury"])


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


def _require_token(body: dict[str, Any]) -> tuple[str, str]:
    api_token = (body.get("api_token") or "").strip()
    if not api_token:
        raise HTTPException(status_code=400, detail="api_token is required")
    base_url = native_connect_base_url(
        body.get("base_url"),
        endpoint_name="mercury_api",
    )
    return api_token, base_url


def _auth_failure_response(exc: MercuryApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token is
    never echoed back (MercuryApiError keeps it off context by design)."""
    unauthorized = getattr(exc, "code", "") == "mercury_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "mercury_auth_failed" if unauthorized else "mercury_api_error",
            "message": (
                "Mercury rejected the API token. Generate one in Mercury "
                "Settings → API tokens and confirm it has read access."
                if unauthorized
                else "Could not reach the Mercury API. Check the base_url and "
                "that the service is reachable."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


def _normalize_account(a: dict[str, Any]) -> dict[str, Any]:
    """Project a raw Mercury account onto the install shape (id/name/kind)."""
    return {
        "account_id": str(a.get("account_id") or a.get("id") or ""),
        "account_name": a.get("account_name") or a.get("name"),
        "account_kind": a.get("account_kind") or a.get("type"),
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the API token and enumerate accounts for the selector UI."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    api_token, base_url = _require_token(body)

    client = MercuryClient(
        base_url=base_url,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        accounts = await client.list_accounts()
    except MercuryApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    normalized = [_normalize_account(a) for a in accounts]
    return JSONResponse(content={
        "ok": True,
        "base_url": base_url,
        "accounts": [a for a in normalized if a["account_id"]],
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
    api_token, base_url = _require_token(body)

    requested_ids = body.get("account_ids")
    if requested_ids is not None and not isinstance(requested_ids, list):
        raise HTTPException(status_code=400, detail="account_ids must be a list")
    organization_id = (body.get("organization_id") or "").strip() or None
    webhook_secret = (body.get("webhook_secret") or "").strip() or None

    # 1. Verify creds + resolve the account set — before any write.
    client = MercuryClient(
        base_url=base_url,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        raw_accounts = await client.list_accounts()
    except MercuryApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    accounts = [a for a in (_normalize_account(a) for a in raw_accounts) if a["account_id"]]
    if requested_ids:
        wanted = {str(x) for x in requested_ids}
        accounts = [a for a in accounts if a["account_id"] in wanted]

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        api_token, label=f"mercury_api_token:{base_url}", tenant_id=tenant_id,
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"mercury_webhook_secret:{base_url}",
            tenant_id=tenant_id,
        )

    # 3. Install: mercury_installations + mercury_accounts + onboarding trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        accounts=accounts,
        secret_ref=secret_ref,
        organization_id=organization_id,
        webhook_secret_ref=webhook_secret_ref,
    )

    # 4. Live webhook edge — needs both the org id (the webhook installation_id)
    #    and a signing secret.
    webhook_registered = False
    if webhook_secret_ref and organization_id:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            organization_id=organization_id,
            webhook_secret_ref=webhook_secret_ref,
        )
        webhook_registered = True

    log.info(
        "mercury.connect.finalized",
        installation_id=str(install_id),
        account_count=len(accounts),
        webhook_registered=webhook_registered,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "account_count": len(accounts),
        "webhook_registered": webhook_registered,
    })


__all__ = ["router"]
