"""services/ingest/integrations/carta/oauth.py — admin connect wizard (cap-table).

Carta authenticates with OAuth 2.0 — a short-lived (~1 h) access token minted at
`POST https://login.app.carta.com/o/access_token/` (client_credentials for
own-account access; there is NO refresh-token grant — tokens are RE-MINTED).
Every read is scoped to an **issuer** (`/v1alpha1/issuers/{issuer_id}/...`).
This repo deliberately does NOT implement the OAuth bounce (authorize →
callback → code exchange): the read client consumes the current access token.
So the genuine production install surface is operator-mediated credential
submission: the operator pastes the `access_token` (and the client-credentials
`client_secret`, which powers the hourly re-mint) obtained from their Carta
OAuth app, and the router verifies them against the REAL Carta API before
seeding the install.

ISSUER ENUMERATION replaces a blind firm-id config: preflight lists the issuers
visible to the token (`GET /v1alpha1/issuers`) so the operator can pick one;
finalize auto-selects when exactly one issuer is visible. The chosen issuer id
is stored in `carta_installations.firm_id` (the column predates the issuer
naming; it holds the Carta issuer id).

CONFIRMED (docs.carta.com/carta/docs/client-credentials-flow +
docs.carta.com/api-platform/docs/authorization): Carta OAuth2 supports only
    AUTHORIZATION_CODE and CLIENT_CREDENTIALS grants. Under client_credentials
    there is NO refresh token; access tokens live ~1 hour and are re-minted by
    re-running the grant (HTTP Basic client_id:client_secret + form
    `scope`/`grant_type`). The API is versioned `v1alpha1` (alpha — expect
    breaking changes) and poll-only (no webhook).
TODO(human): ACCESS IS PARTNER-GATED — obtain the partner agreement (or
    direct-customer own-data access) and the approved scopes before real
    traffic; dev against https://mock-api.carta.com.

Carta is POLL-ONLY: there is NO webhook, so this wizard does NOT accept a webhook
verifier token and never registers a provider_installations row. The live edge is
the poller (`services/ingest/integrations/carta/poll.py`), which resolves the
tenant directly from carta_installations.

Flow:

    POST /integrations/carta/connect/preflight
        body: { access_token, issuer_id?, base_url? }
        → CartaClient.list_issuers() to verify the token + enumerate issuers
        → if issuer_id given, GET /v1alpha1/issuers/{id} verifies visibility
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/carta/connect/finalize
        body: { access_token, issuer_id? (auto-selected if exactly one is
                visible), client_secret?, base_url?, entities?,
                token_expires_at? }
        → re-verify creds + resolve the issuer
        → store the access token (+ the client-credentials secret, if given,
          as refresh_secret_ref — the re-mint material) in the secret store
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
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


log = structlog.get_logger("integrations.carta.oauth")


# Production host CONFIRMED from the Issuer v1alpha1 OpenAPI `servers` list
# (mock: https://mock-api.carta.com; playground: https://api.playground.carta.team).
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


def _require_creds(body: dict[str, Any]) -> tuple[str, str, str | None]:
    """(access_token, base_url, issuer_id?) — issuer_id is optional; it can be
    enumerated. Accepts legacy `firm_id` as an alias for `issuer_id`."""
    access_token = (body.get("access_token") or "").strip()
    base_url = (body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    issuer_id = (body.get("issuer_id") or body.get("firm_id") or "").strip() or None
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return access_token, base_url, issuer_id


def _auth_failure_response(exc: CartaApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The access
    token is never echoed back (CartaApiError keeps it off context)."""
    code = getattr(exc, "code", "")
    unauthorized = code == "carta_api_unauthorized"
    not_found = code == "carta_api_not_found"
    if unauthorized:
        message = (
            "Carta rejected the access token. Tokens expire after ~1 hour — "
            "re-mint one via your Carta OAuth app (client_credentials) and "
            "retry."
        )
        error_code = "carta_auth_failed"
    elif not_found:
        message = (
            "The issuer is not visible to this access token. Run preflight to "
            "enumerate visible issuers and pick one."
        )
        error_code = "carta_issuer_not_visible"
    else:
        message = (
            "Could not reach the Carta API. Check the base_url (production vs "
            "mock/playground) and the token's scopes."
        )
        error_code = "carta_api_error"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": error_code,
            "message": message,
            "underlying_error": str(exc)[:300],
        },
    )


async def _verify_and_resolve_issuer(
    client: CartaClient, issuer_id: str | None,
) -> tuple[str | None, list[dict[str, Any]]]:
    """Verify the token and resolve the issuer.

    Returns `(resolved_issuer_id_or_None, visible_issuers)`. The first page of
    `GET /v1alpha1/issuers` (pageSize 50) both proves connectivity and feeds the
    picker; an explicit issuer_id is verified via `GET /v1alpha1/issuers/{id}`.
    Raises CartaApiError on auth/connectivity failure.
    """
    issuers, _ = await client.list_issuers(page_size=50)
    visible = [
        {"id": i.get("id"), "legal_name": i.get("legalName")}
        for i in issuers if i.get("id")
    ]
    if issuer_id:
        await client.get_issuer(issuer_id)  # 404 -> carta_api_not_found
        return issuer_id, visible
    if len(visible) == 1:
        return str(visible[0]["id"]), visible
    return None, visible


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token via issuer enumeration; verify the issuer if
    one was specified."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    access_token, base_url, issuer_id = _require_creds(body)

    client = CartaClient(
        base_url=base_url,
        issuer_id=issuer_id,
        access_token=access_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        resolved, visible = await _verify_and_resolve_issuer(client, issuer_id)
    except CartaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    return JSONResponse(content={
        "ok": True,
        "issuer_id": resolved,
        "issuers": visible,
        "base_url": base_url,
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
    access_token, base_url, issuer_id = _require_creds(body)

    # The client_credentials secret powers the hourly re-mint; it is stored
    # under refresh_secret_ref (Carta has no OAuth refresh token). Legacy body
    # key `refresh_token` is accepted as an alias.
    client_secret = (
        (body.get("client_secret") or body.get("refresh_token") or "").strip()
        or None
    )
    requested_entities = body.get("entities")
    if requested_entities is not None and not isinstance(requested_entities, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    entities = (
        [str(e).strip() for e in requested_entities if str(e).strip()]
        if requested_entities else list(DEFAULT_ENTITIES)
    )

    # 1. Verify creds + resolve the issuer — before any write.
    client = CartaClient(
        base_url=base_url,
        issuer_id=issuer_id,
        access_token=access_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        resolved, visible = await _verify_and_resolve_issuer(client, issuer_id)
    except CartaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    if resolved is None:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error_code": "carta_issuer_ambiguous",
                "message": (
                    "The token can see several issuers — pass issuer_id "
                    "explicitly (see the preflight `issuers` list)."
                ),
                "issuers": visible,
            },
        )

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        access_token, label=f"carta_access_token:{resolved}",
        tenant_id=tenant_id,
    )
    refresh_secret_ref = None
    if client_secret:
        refresh_secret_ref = await store.put(
            client_secret, label=f"carta_client_secret:{resolved}",
            tenant_id=tenant_id,
        )

    # 3. Install: carta_installations + carta_entities + trigger. Carta is
    #    poll-only — there is no webhook edge to register.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        firm_id=resolved,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        refresh_secret_ref=refresh_secret_ref,
    )

    log.info(
        "carta.connect.finalized",
        installation_id=str(install_id),
        issuer_id=resolved,
        entity_count=len(entities),
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "issuer_id": resolved,
        "entity_count": len(entities),
    })


__all__ = ["router"]
