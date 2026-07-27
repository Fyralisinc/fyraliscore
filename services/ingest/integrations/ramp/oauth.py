"""services/ingest/integrations/ramp/oauth.py — admin connect wizard (finance).

Ramp authenticates with OAuth 2.0 **client credentials** (verified
docs.ramp.com authorization): a Bearer access token is minted at
``POST {base}/token`` (HTTP Basic ``client_id:client_secret``, form body
``grant_type=client_credentials&scope=…``), lives ~1 h, and is RE-MINTED on
expiry — there is NO refresh token for this grant. The production install
surface is operator-mediated credential submission: the operator pastes either

  - the ``client_id`` + ``client_secret`` of the Ramp app (the router mints a
    token and verifies it), or
  - a pre-minted ``access_token`` directly.

``business_id`` is DISCOVERED via the ``GET /business`` probe (its ``id`` is
the same business_id every Ramp webhook carries at root); a provided
``business_id`` is validated against the probe when both exist.

Re-mint credentials for the poll path are stored under
``ramp_installations.refresh_secret_ref`` when the operator supplies
``client_id`` + ``client_secret``. Legacy/access-token-only installs can still
fall back to app-level runtime config (``RAMP_CLIENT_ID`` /
``RAMP_CLIENT_SECRET``).

Flow:

    POST /integrations/ramp/connect/preflight
        body: { access_token? | (client_id, client_secret), base_url?, scopes? }
        → mint (if needed) + RampClient.business() to verify the credential
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/ramp/connect/finalize
        body: preflight fields + { business_id?, entities?,
                webhook_verifier_token? }
        → re-verify creds (mint + probe)
        → store the access token (+ webhook verifier token, if given) in the
          secret store
        → finalize_install(): UPSERT ramp_installations + ramp_entities
          + an onboarding_triggers row (source='ramp') so the M6 backfill
          chain fires; when a webhook verifier token is supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + verifier from
        → 200 OK with the new ramp_installations.id
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import RampApiError
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)
from services.ingest.integrations.ramp.client import (
    DEFAULT_ENTITIES,
    RampClient,
)
from services.ingest.integrations.ramp.onboarding import (
    finalize_install,
    register_webhook_installation,
)


log = structlog.get_logger("integrations.ramp.oauth")


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


def _require_creds(body: dict[str, Any]) -> dict[str, Any]:
    """Validate the credential material: a preset access_token OR a
    client_id+client_secret pair to mint with."""
    access_token = (body.get("access_token") or "").strip() or None
    client_id = (body.get("client_id") or "").strip() or None
    client_secret = (body.get("client_secret") or "").strip() or None
    scopes = (body.get("scopes") or "").strip() or None
    if not access_token and not (client_id and client_secret):
        raise HTTPException(
            status_code=400,
            detail="provide either access_token or client_id + client_secret",
        )
    base_url = native_connect_base_url(
        body.get("base_url"),
        endpoint_name="ramp_api",
    )
    return {
        "access_token": access_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "scopes": scopes,
        "base_url": base_url,
    }


def _auth_failure_response(exc: RampApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token /
    client secret are never echoed back (RampApiError keeps them off context)."""
    unauthorized = getattr(exc, "code", "") == "ramp_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": (
                "ramp_auth_failed" if unauthorized else "ramp_api_error"
            ),
            "message": (
                "Ramp rejected the credentials. Check the client_id/"
                "client_secret (and that client_credentials is a permitted "
                "grant type with the read scopes on the Ramp app), or that "
                "the pasted access_token has not expired."
                if unauthorized
                else "Could not reach the Ramp API. Check the base_url "
                "(production vs sandbox)."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


async def _verify_and_probe(
    creds: dict[str, Any],
    *,
    tenant_id: UUID,
) -> tuple[dict[str, Any], str | None, datetime | None]:
    """Mint a token if needed and probe `GET /business`.

    Returns `(business_info, access_token, token_expires_at)` — the token is
    the minted one (or the pasted one), expiry only known when minted."""
    client = RampClient(
        base_url=creds["base_url"],
        access_token=creds["access_token"],
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        scopes=creds["scopes"],
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        expires_at: datetime | None = None
        access_token = creds["access_token"]
        if not access_token:
            minted = await client.mint_token()
            access_token = minted.get("access_token")
            expires_in = minted.get("expires_in")
            if isinstance(expires_in, (int, float)):
                expires_at = datetime.now(timezone.utc) + timedelta(
                    seconds=int(expires_in),
                )
        info = await client.business()
        return info, access_token, expires_at
    finally:
        await client.aclose()


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the credentials via mint (if needed) + the `GET /business` probe."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    creds = _require_creds(body)

    try:
        info, _, _ = await _verify_and_probe(creds, tenant_id=tenant_id)
    except RampApiError as exc:
        return _auth_failure_response(exc)

    return JSONResponse(content={
        "ok": True,
        "business_id": str(info.get("id") or ""),
        "business_name": info.get("business_name_legal")
        or info.get("business_name_on_card"),
        "base_url": creds["base_url"],
        "entities": list(DEFAULT_ENTITIES),
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Persist the token + install the source.

    Credentials are verified BEFORE any secret is written, so an invalid
    credential leaves no `encrypted_secrets` / install rows behind.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    creds = _require_creds(body)

    webhook_verifier_token = (body.get("webhook_verifier_token") or "").strip() or None
    requested_entities = body.get("entities")
    if requested_entities is not None and not isinstance(requested_entities, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    entities = (
        [str(e).strip() for e in requested_entities if str(e).strip()]
        if requested_entities else list(DEFAULT_ENTITIES)
    )

    # 1. Verify creds + discover the business — before any write.
    try:
        info, access_token, token_expires_at = await _verify_and_probe(
            creds,
            tenant_id=tenant_id,
        )
    except RampApiError as exc:
        return _auth_failure_response(exc)

    discovered = str(info.get("id") or "")
    provided = (body.get("business_id") or "").strip()
    if provided and discovered and provided != discovered:
        raise HTTPException(
            status_code=400,
            detail="business_id does not match the connected Ramp business",
        )
    business_id = discovered or provided
    if not business_id:
        raise HTTPException(
            status_code=400,
            detail="could not determine business_id from the Ramp API",
        )

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token, label=f"ramp_access_token:{business_id}", tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if creds["client_id"] and creds["client_secret"]:
        refresh_secret_ref = await store.put(
            json.dumps(
                {
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                },
                separators=(",", ":"),
            ),
            label=f"ramp_client_credentials:{business_id}",
            tenant_id=tenant_id,
        )
    webhook_secret_ref = None
    if webhook_verifier_token:
        webhook_secret_ref = await store.put(
            webhook_verifier_token, label=f"ramp_webhook_verifier:{business_id}",
            tenant_id=tenant_id,
        )

    # 3. Install: ramp_installations + ramp_entities + trigger. No rotating
    # refresh token exists for client_credentials; refresh_secret_ref stores
    # encrypted re-mint material when it is available.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        business_id=business_id,
        base_url=creds["base_url"],
        entities=entities,
        secret_ref=secret_ref,
        refresh_secret_ref=refresh_secret_ref,
        token_expires_at=token_expires_at,
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
        "business_id": business_id,
        "entity_count": len(entities),
        "webhook_registered": webhook_registered,
    })


__all__ = ["router"]
