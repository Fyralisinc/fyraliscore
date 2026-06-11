"""services/ingest/integrations/deel/oauth.py — admin connect wizard (finance).

Deel authenticates with a long-lived Bearer API token (Jira-shaped). This is
the production install surface the audit flagged as missing: `finalize_install`
/ `register_webhook_installation` were reachable only through the dev
`finance_router` panel (synthetic data, `X-Tenant-Id` header). It mirrors the
Jira connect wizard — Bearer-authed, real credentials persisted encrypted via
the gateway `secret_store` (only opaque refs reach the install tables).

TODO(human): confirm Deel does NOT issue refresh tokens (this wizard assumes a
long-lived static token, the Mercury archetype). If Deel ever moves to OAuth
access+refresh, this becomes a QBO-shaped preflight/finalize and needs a refresh
seam — none exists today.

Flow:

    POST /integrations/deel/connect/preflight
        body: { api_token, base_url? }
        → DeelClient.list_contracts() to verify the token + enumerate the
          contracts for the selector UI
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/deel/connect/finalize
        body: { api_token, base_url?, contract_ids?, organization_id?,
                webhook_secret? }
        → re-verify creds, resolve the contract set (all enumerated, or the
          `contract_ids` subset)
        → store the API token (+ webhook secret, if given) in the secret store
        → finalize_install(): UPSERT deel_installations + deel_contracts +
          an onboarding_triggers row (source='deel') so the M6 backfill chain
          fires; when an organization_id + webhook secret are supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + HMAC secret from
        → 200 OK with the new deel_installations.id

Backfill (deel_installations) and the live webhook edge
(provider_installations) are seeded together but stay independent.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import DeelApiError
from services.ingest.integrations.deel.client import DeelClient
from services.ingest.integrations.deel.onboarding import (
    finalize_install,
    register_webhook_installation,
)


log = structlog.get_logger("integrations.deel.oauth")


# Canonical Deel API base (same default the finance panel + fetcher use).
_DEFAULT_BASE_URL = "https://api.letsdeel.com/rest/v2"


router = APIRouter(prefix="/integrations/deel", tags=["deel"])


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
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not api_token:
        raise HTTPException(status_code=400, detail="api_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return api_token, base_url


def _auth_failure_response(exc: DeelApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token is
    never echoed back (DeelApiError keeps it off context by design)."""
    unauthorized = getattr(exc, "code", "") == "deel_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "deel_auth_failed" if unauthorized else "deel_api_error",
            "message": (
                "Deel rejected the API token. Generate one in Deel "
                "Settings → API tokens and confirm it has read access."
                if unauthorized
                else "Could not reach the Deel API. Check the base_url and "
                "that the service is reachable."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


def _normalize_contract(c: dict[str, Any]) -> dict[str, Any]:
    """Project a raw Deel contract onto the install shape (id/name/type)."""
    return {
        "contract_id": str(c.get("contract_id") or c.get("id") or ""),
        "contract_name": c.get("contract_name") or c.get("name"),
        "contract_type": c.get("contract_type") or c.get("type"),
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the API token and enumerate contracts for the selector UI."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    api_token, base_url = _require_token(body)

    client = DeelClient(base_url=base_url, api_token=api_token)
    try:
        contracts = await client.list_contracts()
    except DeelApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    normalized = [_normalize_contract(c) for c in contracts]
    return JSONResponse(content={
        "ok": True,
        "base_url": base_url,
        "contracts": [c for c in normalized if c["contract_id"]],
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

    requested_ids = body.get("contract_ids")
    if requested_ids is not None and not isinstance(requested_ids, list):
        raise HTTPException(status_code=400, detail="contract_ids must be a list")
    organization_id = (body.get("organization_id") or "").strip() or None
    webhook_secret = (body.get("webhook_secret") or "").strip() or None

    # 1. Verify creds + resolve the contract set — before any write.
    client = DeelClient(base_url=base_url, api_token=api_token)
    try:
        raw_contracts = await client.list_contracts()
    except DeelApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    contracts = [c for c in (_normalize_contract(c) for c in raw_contracts) if c["contract_id"]]
    if requested_ids:
        wanted = {str(x) for x in requested_ids}
        contracts = [c for c in contracts if c["contract_id"] in wanted]

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        api_token, label=f"deel_api_token:{base_url}", tenant_id=tenant_id,
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"deel_webhook_secret:{base_url}",
            tenant_id=tenant_id,
        )

    # 3. Install: deel_installations + deel_contracts + onboarding trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        contracts=contracts,
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
        "deel.connect.finalized",
        installation_id=str(install_id),
        contract_count=len(contracts),
        webhook_registered=webhook_registered,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "contract_count": len(contracts),
        "webhook_registered": webhook_registered,
    })


__all__ = ["router"]
