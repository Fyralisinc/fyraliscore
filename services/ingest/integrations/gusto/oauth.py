"""services/ingest/integrations/gusto/oauth.py — admin connect wizard (finance).

Gusto authenticates with OAuth 2.0 — a short-lived access token plus a rotating
refresh token, every call scoped to a company ``company_uuid``. This repo
deliberately does NOT implement the OAuth bounce (authorize → callback → code
exchange): the read client consumes the current access token. So the genuine
production install surface is operator-mediated credential submission: the
operator pastes the `company_uuid` + the `access_token` (and `refresh_token`)
they obtained from their Gusto OAuth app, and the router verifies them against
the REAL Gusto API before seeding the install.

TODO(human): implement Gusto OAuth token refresh — NONE exists yet (the
    documented-but-unbuilt seam, exactly as the QuickBooks archetype ships).
    `finalize` already persists `refresh_secret_ref` + (optionally)
    `token_expires_at`; wire a refresh loop (refresh-on-401 in the client OR an
    oauth_poller) so short-lived access tokens are rotated. Do NOT assume tokens
    never expire.

This is the production surface the audit flagged as missing: `finalize_install`
/ `register_webhook_installation` were reachable only through the dev
`finance_router` panel (synthetic data, `X-Tenant-Id` header). Here the tenant
comes from Bearer auth and the tokens are real + persisted encrypted via the
gateway `secret_store` (only opaque refs reach the install tables).

Flow:

    POST /integrations/gusto/connect/preflight
        body: { company_uuid, access_token, base_url? }
        → GustoClient.company_info() to verify the token + company
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/gusto/connect/finalize
        body: { company_uuid, access_token, refresh_token?, base_url?, entities?,
                token_expires_at?, webhook_verifier_token? }
        → re-verify creds
        → store the access token (+ refresh token, + webhook verifier token,
          if given) in the secret store
        → finalize_install(): UPSERT gusto_installations + gusto_entities
          + an onboarding_triggers row (source='gusto') so the M6 backfill
          chain fires; when a webhook verifier token is supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + verifier from
        → 200 OK with the new gusto_installations.id
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import GustoApiError
from services.ingest.integrations.gusto.client import (
    DEFAULT_ENTITIES,
    GustoClient,
)
from services.ingest.integrations.gusto.onboarding import (
    finalize_install,
    register_webhook_installation,
)


log = structlog.get_logger("integrations.gusto.oauth")


# TODO(human): confirm Gusto production API host. The operator may pass a
# sandbox/demo host via base_url when testing; this default is a placeholder.
_DEFAULT_BASE_URL = "https://api.gusto.com"


router = APIRouter(prefix="/integrations/gusto", tags=["gusto"])


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
    company_uuid = (body.get("company_uuid") or "").strip()
    access_token = (body.get("access_token") or "").strip()
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not company_uuid:
        raise HTTPException(status_code=400, detail="company_uuid is required")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return company_uuid, access_token, base_url


def _auth_failure_response(exc: GustoApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The access
    token is never echoed back (GustoApiError keeps it off context)."""
    unauthorized = getattr(exc, "code", "") == "gusto_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": (
                "gusto_auth_failed" if unauthorized else "gusto_api_error"
            ),
            "message": (
                "Gusto rejected the access token / company. The token may be "
                "expired — refresh it via your Gusto OAuth app and retry, and "
                "confirm the company_uuid matches the connected company."
                if unauthorized
                else "Could not reach the Gusto API. Check the base_url "
                "(production vs sandbox) and the company_uuid."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token + company via the companyinfo probe."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    company_uuid, access_token, base_url = _require_creds(body)

    client = GustoClient(
        base_url=base_url, company_uuid=company_uuid, access_token=access_token,
    )
    try:
        info = await client.company_info()
    except GustoApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    company = info.get("CompanyInfo") if isinstance(info, dict) else None
    company_name = company.get("CompanyName") if isinstance(company, dict) else None
    return JSONResponse(content={
        "ok": True,
        "company_uuid": company_uuid,
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
    company_uuid, access_token, base_url = _require_creds(body)

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
    client = GustoClient(
        base_url=base_url, company_uuid=company_uuid, access_token=access_token,
    )
    try:
        await client.company_info()
    except GustoApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    # 2. Persist tokens encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token, label=f"gusto_access_token:{company_uuid}", tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if refresh_token:
        refresh_secret_ref = await store.put(
            refresh_token, label=f"gusto_refresh_token:{company_uuid}",
            tenant_id=tenant_id,
        )
    webhook_secret_ref = None
    if webhook_verifier_token:
        webhook_secret_ref = await store.put(
            webhook_verifier_token, label=f"gusto_webhook_verifier:{company_uuid}",
            tenant_id=tenant_id,
        )

    # 3. Install: gusto_installations + gusto_entities + trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        company_uuid=company_uuid,
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
            company_uuid=company_uuid,
            webhook_secret_ref=webhook_secret_ref,
        )
        webhook_registered = True

    log.info(
        "gusto.connect.finalized",
        installation_id=str(install_id),
        company_uuid=company_uuid,
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
