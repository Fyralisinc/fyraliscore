"""services/ingest/integrations/ramp/oauth.py — admin connect wizard (finance).

Cloned from the QuickBooks OAuth archetype. Ramp authenticates with OAuth 2.0 —
a short-lived access token plus a rotating refresh token, every call scoped to a
company ``businessId``. As in the QBO archetype this repo deliberately does NOT
implement the full OAuth bounce (authorize → callback → code exchange): the read
client consumes the current access token and the `oauth_poller` owns refresh. So
the genuine production install surface is operator-mediated credential
submission: the operator pastes the `business_id` + the `access_token` (and
`refresh_token`) they obtained from their Ramp OAuth app, and the router verifies
them against the REAL Ramp API before seeding the install.

TODO(human): implement Ramp OAuth token refresh (refresh-on-401 or poller; none
exists — this is the QBO seam). refresh_secret_ref + token_expires_at are
persisted (see finalize_install / the migration); the exchange endpoint + grant
flow + rotation are UNVERIFIED. Do NOT assume tokens never expire.

This is the production surface the audit flagged as missing: `finalize_install`
/ `register_webhook_installation` were reachable only through the dev
`finance_router` panel (synthetic data, `X-Tenant-Id` header). Here the tenant
comes from Bearer auth and the tokens are real + persisted encrypted via the
gateway `secret_store` (only opaque refs reach the install tables).

Flow:

    POST /integrations/ramp/connect/preflight
        body: { business_id, access_token, base_url? }
        → RampClient.company_info() to verify the token + business
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/ramp/connect/finalize
        body: { business_id, access_token, refresh_token?, base_url?, entities?,
                token_expires_at?, webhook_verifier_token? }
        → re-verify creds
        → store the access token (+ refresh token, + webhook verifier token,
          if given) in the secret store
        → finalize_install(): UPSERT ramp_installations + ramp_entities
          + an onboarding_triggers row (source='ramp') so the M6 backfill
          chain fires; when a webhook verifier token is supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + verifier from
        → 200 OK with the new ramp_installations.id
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import RampApiError
from services.ingest.integrations.ramp.client import (
    DEFAULT_ENTITIES,
    RampClient,
)
from services.ingest.integrations.ramp.onboarding import (
    finalize_install,
    register_webhook_installation,
)


log = structlog.get_logger("integrations.ramp.oauth")


# Ramp production host (UNVERIFIED — plausible default, overridable per-install
# via the base_url field and per-env via RAMP_API_BASE_URL).
# TODO(human): confirm Ramp API host + sandbox host.
_DEFAULT_BASE_URL = "https://api.ramp.com/developer/v1"


router = APIRouter(prefix="/integrations/ramp", tags=["ramp"])


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


def _require_creds(body: dict[str, Any]) -> tuple[str, str, str]:
    business_id = (body.get("business_id") or "").strip()
    access_token = (body.get("access_token") or "").strip()
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not business_id:
        raise HTTPException(status_code=400, detail="business_id is required")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return business_id, access_token, base_url


def _auth_failure_response(exc: RampApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The access
    token is never echoed back (RampApiError keeps it off context)."""
    unauthorized = getattr(exc, "code", "") == "ramp_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": (
                "ramp_auth_failed" if unauthorized else "ramp_api_error"
            ),
            "message": (
                "Ramp rejected the access token / business. The token may be "
                "expired — refresh it via your Ramp OAuth app and retry, and "
                "confirm the business_id matches the connected company."
                if unauthorized
                else "Could not reach the Ramp API. Check the base_url "
                "(production vs sandbox) and the business_id."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token + business via the companyinfo probe."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    business_id, access_token, base_url = _require_creds(body)

    client = RampClient(
        base_url=base_url, business_id=business_id, access_token=access_token,
    )
    try:
        info = await client.company_info()
    except RampApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    company = info.get("CompanyInfo") if isinstance(info, dict) else None
    company_name = company.get("CompanyName") if isinstance(company, dict) else None
    return JSONResponse(content={
        "ok": True,
        "business_id": business_id,
        "base_url": base_url,
        "company_name": company_name,
        "entities": list(DEFAULT_ENTITIES),
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Persist tokens + install the source.

    Credentials are verified BEFORE any secret is written, so an invalid token
    leaves no `encrypted_secrets` / install rows behind.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    business_id, access_token, base_url = _require_creds(body)

    refresh_token = (body.get("refresh_token") or "").strip() or None
    webhook_verifier_token = (body.get("webhook_verifier_token") or "").strip() or None
    requested_entities = body.get("entities")
    if requested_entities is not None and not isinstance(requested_entities, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    entities = (
        [str(e).strip() for e in requested_entities if str(e).strip()]
        if requested_entities else list(DEFAULT_ENTITIES)
    )

    # 1. Verify creds — before any write.
    client = RampClient(
        base_url=base_url, business_id=business_id, access_token=access_token,
    )
    try:
        await client.company_info()
    except RampApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    # 2. Persist tokens encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token, label=f"ramp_access_token:{business_id}", tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if refresh_token:
        refresh_secret_ref = await store.put(
            refresh_token, label=f"ramp_refresh_token:{business_id}",
            tenant_id=tenant_id,
        )
    webhook_secret_ref = None
    if webhook_verifier_token:
        webhook_secret_ref = await store.put(
            webhook_verifier_token, label=f"ramp_webhook_verifier:{business_id}",
            tenant_id=tenant_id,
        )

    # 3. Install: ramp_installations + ramp_entities + trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        business_id=business_id,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        refresh_secret_ref=refresh_secret_ref,
        webhook_secret_ref=webhook_secret_ref,
    )

    # 4. Live webhook edge (only when a verifier token was supplied).
    webhook_registered = False
    if webhook_secret_ref:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            business_id=business_id,
            webhook_secret_ref=webhook_secret_ref,
        )
        webhook_registered = True

    log.info(
        "ramp.connect.finalized",
        installation_id=str(install_id),
        business_id=business_id,
        entity_count=len(entities),
        webhook_registered=webhook_registered,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "entity_count": len(entities),
        "webhook_registered": webhook_registered,
    })


__all__ = ["router"]
