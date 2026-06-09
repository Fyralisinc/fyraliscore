"""services/ingest/integrations/figma/oauth.py — admin connect wizard (design).

Figma authenticates with a long-lived org/team Bearer access token (Brex-shaped;
OAuth2 per-resource refresh is out of v1 scope). This is the production install
surface: `finalize_install` / `register_webhook_installation` reachable from a
connect wizard. It mirrors the Brex connect wizard — Bearer-authed, real
credentials persisted encrypted via the gateway `secret_store` (only opaque refs
reach the install tables).

Flow:

    POST /integrations/figma/connect/preflight
        body: { api_token, base_url? }
        → FigmaClient.list_files() to verify the token + enumerate the files for
          the selector UI
        → on auth failure: a structured 400 (no secret is stored)

    POST /integrations/figma/connect/finalize
        body: { api_token, base_url?, file_keys?, team_id?, webhook_secret? }
        → re-verify creds, resolve the file set (all enumerated, or the
          `file_keys` subset)
        → store the access token (+ webhook secret/passcode, if given) in the
          secret store
        → finalize_install(): UPSERT figma_installations + figma_files +
          an onboarding_triggers row (source='figma') so the M6 backfill chain
          fires; when a team_id + webhook secret are supplied,
          register_webhook_installation() seeds the provider_installations row
          the webhook edge resolves the tenant + signing secret from
        → 200 OK with the new figma_installations.id

Backfill (figma_installations) and the live webhook edge (provider_installations)
are seeded together but stay independent.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.figma.client import FigmaApiError, FigmaClient
from services.ingest.integrations.figma.onboarding import (
    finalize_install,
    register_webhook_installation,
)


log = structlog.get_logger("integrations.figma.oauth")


# Default Figma API host for the connect-wizard UI fallback only (an operator may
# override per-install via the `base_url` field). The canonical default + env
# override live in `lib/integrations/endpoints.py` (`figma_api`).
# CONFIRMED (developers.figma.com): REST host https://api.figma.com (base /v1).
# Auth is a personal access token via the `X-Figma-Token` header, OR OAuth2 Bearer
# (authorize https://www.figma.com/oauth; token https://api.figma.com/v1/oauth/token;
# refresh https://api.figma.com/v1/oauth/refresh; read scopes file_content:read,
# file_metadata:read, file_versions:read).
_DEFAULT_BASE_URL = "https://api.figma.com"


router = APIRouter(prefix="/integrations/figma", tags=["figma"])


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


def _auth_failure_response(exc: FigmaApiError) -> JSONResponse:
    """Map a credential/connectivity failure to a structured 400. The token is
    never echoed back (FigmaApiError keeps it off context by design)."""
    unauthorized = getattr(exc, "code", "") == "figma_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "figma_auth_failed" if unauthorized else "figma_api_error",
            "message": (
                "Figma rejected the access token. Generate one in Figma "
                "Settings → Personal access tokens (or an org/team token) and "
                "confirm it has the required read scopes."
                if unauthorized
                else "Could not reach the Figma API. Check the base_url and "
                "that the service is reachable."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


def _normalize_file(f: dict[str, Any]) -> dict[str, Any]:
    """Project a raw Figma file onto the install shape (key/name/project)."""
    return {
        "file_key": str(f.get("file_key") or f.get("key") or ""),
        "file_name": f.get("file_name") or f.get("name"),
        "project_name": f.get("project_name") or f.get("project"),
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify the access token and enumerate files for the selector UI."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    api_token, base_url = _require_token(body)

    client = FigmaClient(base_url=base_url, api_token=api_token)
    try:
        files = await client.list_files()
    except FigmaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    normalized = [_normalize_file(f) for f in files]
    return JSONResponse(content={
        "ok": True,
        "base_url": base_url,
        "files": [f for f in normalized if f["file_key"]],
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

    requested_keys = body.get("file_keys")
    if requested_keys is not None and not isinstance(requested_keys, list):
        raise HTTPException(status_code=400, detail="file_keys must be a list")
    team_id = (body.get("team_id") or "").strip() or None
    webhook_secret = (body.get("webhook_secret") or "").strip() or None

    # 1. Verify creds + resolve the file set — before any write.
    client = FigmaClient(base_url=base_url, api_token=api_token)
    try:
        raw_files = await client.list_files()
    except FigmaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()

    files = [f for f in (_normalize_file(f) for f in raw_files) if f["file_key"]]
    if requested_keys:
        wanted = {str(x) for x in requested_keys}
        files = [f for f in files if f["file_key"] in wanted]

    # 2. Persist secrets encrypted-at-rest; only opaque refs reach the DB.
    secret_ref = await store.put(
        api_token, label=f"figma_api_token:{base_url}", tenant_id=tenant_id,
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"figma_webhook_secret:{base_url}",
            tenant_id=tenant_id,
        )

    # 3. Install: figma_installations + figma_files + onboarding trigger.
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        files=files,
        secret_ref=secret_ref,
        team_id=team_id,
        webhook_secret_ref=webhook_secret_ref,
    )

    # 4. Live webhook edge — needs both the team id (the webhook installation_id)
    #    and a signing secret/passcode.
    webhook_registered = False
    if webhook_secret_ref and team_id:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            team_id=team_id,
            webhook_secret_ref=webhook_secret_ref,
        )
        webhook_registered = True

    log.info(
        "figma.connect.finalized",
        installation_id=str(install_id),
        file_count=len(files),
        webhook_registered=webhook_registered,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "file_count": len(files),
        "webhook_registered": webhook_registered,
    })


__all__ = ["router"]
