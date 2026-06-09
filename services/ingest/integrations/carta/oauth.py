"""services/ingest/integrations/carta/oauth.py — admin connect wizard (cap-table).

Carta authenticates with OAuth 2.0 — a short-lived access token plus a rotating
refresh token, every call scoped to a firm ``firm_id``. This repo deliberately
does NOT implement the OAuth bounce (authorize → callback → code exchange): the
read client consumes the current access token. So the genuine production install
surface is operator-mediated credential submission: the operator pastes the
`firm_id` + the `access_token` (and `refresh_token`) they obtained from their
Carta OAuth app, and the router verifies them against the REAL Carta API before
seeding the install.

CONFIRMED (docs.carta.com/api-platform): Carta OAuth2 supports only
    AUTHORIZATION_CODE and CLIENT_CREDENTIALS grants — there is NO refresh_token
    grant. Access tokens live ~1 hour; you RE-MINT (re-run client_credentials, or
    re-exchange a fresh 60-second auth code) rather than refresh. The API is
    versioned `v1alpha1` (alpha — expect breaking changes), poll-only (no
    webhook), and freshness is bounded by Carta's batch cadence (most tables
    update ~daily by noon ET; benchmark/cap-table datasets quarterly).
TODO(human): (1) ACCESS IS PARTNER-GATED — invite-only + SOC 2 Type 2 since 2025;
    obtain the partner agreement (or direct-customer own-data access) and the
    approved prod host/scopes before real traffic; dev against
    https://mock-api.carta.com. (2) wire a re-mint-on-401 loop in the client
    (client_credentials) since access tokens expire hourly — `finalize` persists
    refresh_secret_ref/token_expires_at but Carta has no refresh grant, so treat
    refresh_secret_ref as the client-credentials material, not an OAuth refresh token.

Carta is POLL-ONLY: there is NO webhook, so this wizard does NOT accept a webhook
verifier token and never registers a provider_installations row. The live edge is
the poller (`services/ingest/integrations/carta/poll.py`), which resolves the
tenant directly from carta_installations.

Flow:

    POST /integrations/carta/connect/preflight
        body: { firm_id, access_token, base_url? }
        → CartaClient.firm_info() to verify the token + firm
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/carta/connect/finalize
        body: { firm_id, access_token, refresh_token?, base_url?, entities?,
                token_expires_at? }
        → re-verify creds
        → store the access token (+ refresh token, if given) in the secret store
        → finalize_install(): UPSERT carta_installations + carta_entities
          + an onboarding_triggers row (source='carta') so the M6 backfill
          chain fires
        → 200 OK with the new carta_installations.id
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.carta.client import (
    DEFAULT_ENTITIES,
    CartaApiError,
    CartaClient,
)
from services.ingest.integrations.carta.onboarding import finalize_install


log = structlog.get_logger("integrations.carta.oauth")


# TODO(human): confirm Carta production API host. The operator may pass a
# sandbox/demo host via base_url when testing; this default is a placeholder.
_DEFAULT_BASE_URL = "https://api.carta.com"


router = APIRouter(prefix="/integrations/carta", tags=["carta"])


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
    firm_id = (body.get("firm_id") or "").strip()
    access_token = (body.get("access_token") or "").strip()
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not firm_id:
        raise HTTPException(status_code=400, detail="firm_id is required")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return firm_id, access_token, base_url


def _auth_failure_response(exc: CartaApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The access
    token is never echoed back (CartaApiError keeps it off context)."""
    unauthorized = getattr(exc, "code", "") == "carta_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": (
                "carta_auth_failed" if unauthorized else "carta_api_error"
            ),
            "message": (
                "Carta rejected the access token / firm. The token may be "
                "expired — refresh it via your Carta OAuth app and retry, and "
                "confirm the firm_id matches the connected firm."
                if unauthorized
                else "Could not reach the Carta API. Check the base_url "
                "(production vs sandbox) and the firm_id."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token + firm via the firminfo probe."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    firm_id, access_token, base_url = _require_creds(body)

    client = CartaClient(
        base_url=base_url, firm_id=firm_id, access_token=access_token,
    )
    try:
        info = await client.firm_info()
    except CartaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    firm = info.get("FirmInfo") if isinstance(info, dict) else None
    firm_name = firm.get("FirmName") if isinstance(firm, dict) else None
    return JSONResponse(content={
        "ok": True,
        "firm_id": firm_id,
        "base_url": base_url,
        "firm_name": firm_name,
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
    firm_id, access_token, base_url = _require_creds(body)

    refresh_token = (body.get("refresh_token") or "").strip() or None
    requested_entities = body.get("entities")
    if requested_entities is not None and not isinstance(requested_entities, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    entities = (
        [str(e).strip() for e in requested_entities if str(e).strip()]
        if requested_entities else list(DEFAULT_ENTITIES)
    )

    # 1. Verify creds — before any write.
    client = CartaClient(
        base_url=base_url, firm_id=firm_id, access_token=access_token,
    )
    try:
        await client.firm_info()
    except CartaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    # 2. Persist tokens encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token, label=f"carta_access_token:{firm_id}", tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if refresh_token:
        refresh_secret_ref = await store.put(
            refresh_token, label=f"carta_refresh_token:{firm_id}",
            tenant_id=tenant_id,
        )

    # 3. Install: carta_installations + carta_entities + trigger. Carta is
    #    poll-only — there is no webhook edge to register.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        firm_id=firm_id,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        refresh_secret_ref=refresh_secret_ref,
    )

    log.info(
        "carta.connect.finalized",
        installation_id=str(install_id),
        firm_id=firm_id,
        entity_count=len(entities),
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "entity_count": len(entities),
    })


__all__ = ["router"]
