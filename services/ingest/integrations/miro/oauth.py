"""Miro's poll-only admin connect wizard.

Miro authenticates with a long-lived org-app Bearer token. This production
surface verifies the token before persisting its opaque secret-store reference,
the exact organization scope, and the selected boards.

Flow:

    POST /integrations/miro/connect/preflight
        body: { api_token, base_url? }
        → MiroClient.list_boards() to verify the token + enumerate the boards
          for the selector UI
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/miro/connect/finalize
        body: { api_token, base_url?, board_ids?, org_id? }
        → re-verify creds, resolve the board set (all enumerated, or the
          `board_ids` subset)
        → store the API token in the secret store
        → finalize_install(): UPSERT miro_installations + miro_boards + an
          onboarding_triggers row (source='miro') so the M6 backfill chain
          fires
        → 200 OK with the new miro_installations.id

Miro's discontinued webhook surface is deliberately not accepted here.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import MiroApiError
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.miro.client import MiroClient
from services.ingest.integrations.miro.onboarding import finalize_install
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


log = structlog.get_logger("integrations.miro.oauth")


# CONFIRMED (developers.miro.com): REST base https://api.miro.com/v2. OAuth2
# Bearer — authorize https://miro.com/oauth/authorize; token (NOTE: /v1)
# https://api.miro.com/v1/oauth/token; grants authorization_code + refresh_token
# (access 60 min, refresh 60 days); read scope `boards:read` (covers board items).
router = APIRouter(prefix="/integrations/miro", tags=["miro"])


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
        endpoint_name="miro_api",
    )
    return api_token, base_url


def _auth_failure_response(exc: MiroApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token is
    never echoed back (MiroApiError keeps it off context by design)."""
    unauthorized = getattr(exc, "code", "") == "miro_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "miro_auth_failed" if unauthorized else "miro_api_error",
            "message": (
                "Miro rejected the API token. Generate one in Miro "
                "Settings → Your apps and confirm it has read access."
                if unauthorized
                else "Could not reach the Miro API. Check the base_url and "
                "that the service is reachable."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


def _normalize_board(b: dict[str, Any]) -> dict[str, Any]:
    """Project a raw Miro board onto the install shape (id/name/kind)."""
    return {
        "board_id": str(b.get("board_id") or b.get("id") or ""),
        "board_name": b.get("board_name") or b.get("name"),
        "board_kind": b.get("board_kind") or b.get("type"),
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the API token and enumerate boards for the selector UI."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    api_token, base_url = _require_token(body)

    client = MiroClient(
        base_url=base_url,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        boards = await client.list_boards()
    except MiroApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    normalized = [_normalize_board(b) for b in boards]
    return JSONResponse(content={
        "ok": True,
        "base_url": base_url,
        "boards": [b for b in normalized if b["board_id"]],
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
    if "webhook_secret" in body:
        raise HTTPException(
            status_code=400,
            detail="webhook_secret is not supported for poll-only Miro",
        )

    requested_ids = body.get("board_ids")
    if requested_ids is not None and not isinstance(requested_ids, list):
        raise HTTPException(status_code=400, detail="board_ids must be a list")
    org_id = (body.get("org_id") or "").strip() or None

    # 1. Verify creds + resolve the board set — before any write.
    client = MiroClient(
        base_url=base_url,
        api_token=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        raw_boards = await client.list_boards()
    except MiroApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    boards = [b for b in (_normalize_board(b) for b in raw_boards) if b["board_id"]]
    if requested_ids:
        wanted = {str(x) for x in requested_ids}
        boards = [b for b in boards if b["board_id"] in wanted]

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        api_token, label=f"miro_api_token:{base_url}", tenant_id=tenant_id,
    )

    # 3. Install: miro_installations + miro_boards + onboarding trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        boards=boards,
        secret_ref=secret_ref,
        org_id=org_id,
    )

    log.info(
        "miro.connect.finalized",
        installation_id=str(install_id),
        board_count=len(boards),
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "board_count": len(boards),
    })


__all__ = ["router"]
