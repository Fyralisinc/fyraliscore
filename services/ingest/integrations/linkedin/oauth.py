"""services/ingest/integrations/linkedin/oauth.py — admin connect wizard.

LinkedIn authenticates with OAuth 2.0 — a short-lived access token plus a
rotating refresh token, every call scoped to an ``organization_urn``. This repo
deliberately does NOT implement the OAuth bounce (authorize → callback → code
exchange): the read client consumes the current access token. So the genuine
production install surface is operator-mediated credential submission: the
operator pastes the `organization_urn` + the `access_token` (and `refresh_token`)
they obtained from their LinkedIn OAuth app, and the router verifies them against
the REAL LinkedIn API before seeding the install.

TODO(human): ACCESS IS PARTNER-GATED — LinkedIn organization/recruiting data
    (Marketing Developer Platform / Talent Solutions) is invite-only. (1) obtain
    the partner agreement (or direct-customer own-data access) and the approved
    prod host before real traffic. (2) confirm the exact OAuth scopes — the
    organization read scopes are NOT verified here (candidates:
    r_organization_social, rw_organization_admin, r_organization_followers,
    r_basicprofile). (3) wire a refresh-on-401 loop in the client — LinkedIn
    access tokens are ~60 days, refresh tokens ~1 year; `finalize` persists
    refresh_secret_ref/token_expires_at but no refresh exchange is implemented yet.

LinkedIn is POLL-ONLY: there is NO webhook, so this wizard does NOT accept a
webhook verifier token and never registers a provider_installations row. The live
edge is the poller (`services/ingest/integrations/linkedin/poll.py`), which
resolves the tenant directly from linkedin_installations.

Flow:

    POST /integrations/linkedin/connect/preflight
        body: { organization_urn, access_token, base_url? }
        → LinkedinClient.org_info() to verify the token + organization
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/linkedin/connect/finalize
        body: { organization_urn, access_token, refresh_token?, base_url?,
                entities?, token_expires_at? }
        → re-verify creds
        → store the access token (+ refresh token, if given) in the secret store
        → finalize_install(): UPSERT linkedin_installations + linkedin_entities
          + an onboarding_triggers row (source='linkedin') so the M6 backfill
          chain fires
        → 200 OK with the new linkedin_installations.id
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.linkedin.client import (
    DEFAULT_ENTITIES,
    LinkedinApiError,
    LinkedinClient,
)
from services.ingest.integrations.linkedin.onboarding import finalize_install


log = structlog.get_logger("integrations.linkedin.oauth")


# TODO(human): confirm LinkedIn production API host. The operator may pass a
# sandbox/demo host via base_url when testing; this default is a placeholder
# (the entitled prod host is api.linkedin.com but the REST base path is unverified).
_DEFAULT_BASE_URL = "https://api.linkedin.com"


router = APIRouter(prefix="/integrations/linkedin", tags=["linkedin"])


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
    organization_urn = (body.get("organization_urn") or "").strip()
    access_token = (body.get("access_token") or "").strip()
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not organization_urn:
        raise HTTPException(status_code=400, detail="organization_urn is required")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return organization_urn, access_token, base_url


def _auth_failure_response(exc: LinkedinApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The access
    token is never echoed back (LinkedinApiError keeps it off context)."""
    unauthorized = getattr(exc, "code", "") == "linkedin_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": (
                "linkedin_auth_failed" if unauthorized else "linkedin_api_error"
            ),
            "message": (
                "LinkedIn rejected the access token / organization. The token "
                "may be expired — refresh it via your LinkedIn OAuth app and "
                "retry, and confirm the organization_urn matches the connected "
                "organization (and that your app has the partner entitlement)."
                if unauthorized
                else "Could not reach the LinkedIn API. Check the base_url "
                "(production vs sandbox) and the organization_urn."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token + organization via the orginfo probe."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    organization_urn, access_token, base_url = _require_creds(body)

    client = LinkedinClient(
        base_url=base_url,
        organization_urn=organization_urn,
        access_token=access_token,
    )
    try:
        info = await client.org_info()
    except LinkedinApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    org = info.get("OrgInfo") if isinstance(info, dict) else None
    org_name = org.get("Name") if isinstance(org, dict) else None
    return JSONResponse(content={
        "ok": True,
        "organization_urn": organization_urn,
        "base_url": base_url,
        "organization_name": org_name,
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
    organization_urn, access_token, base_url = _require_creds(body)

    refresh_token = (body.get("refresh_token") or "").strip() or None
    requested_entities = body.get("entities")
    if requested_entities is not None and not isinstance(requested_entities, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    entities = (
        [str(e).strip() for e in requested_entities if str(e).strip()]
        if requested_entities else list(DEFAULT_ENTITIES)
    )

    # 1. Verify creds — before any write.
    client = LinkedinClient(
        base_url=base_url,
        organization_urn=organization_urn,
        access_token=access_token,
    )
    try:
        await client.org_info()
    except LinkedinApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    # 2. Persist tokens encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token,
        label=f"linkedin_access_token:{organization_urn}",
        tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if refresh_token:
        refresh_secret_ref = await store.put(
            refresh_token,
            label=f"linkedin_refresh_token:{organization_urn}",
            tenant_id=tenant_id,
        )

    # 3. Install: linkedin_installations + linkedin_entities + trigger. LinkedIn
    #    is poll-only — there is no webhook edge to register.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        organization_urn=organization_urn,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        refresh_secret_ref=refresh_secret_ref,
    )

    log.info(
        "linkedin.connect.finalized",
        installation_id=str(install_id),
        organization_urn=organization_urn,
        entity_count=len(entities),
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "entity_count": len(entities),
    })


__all__ = ["router"]
