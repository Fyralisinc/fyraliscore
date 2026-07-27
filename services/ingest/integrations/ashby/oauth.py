"""Admin-present Ashby connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import AshbyApiError
from services.ingest.integrations.ashby.client import AshbyClient, DEFAULT_ENTITIES
from services.ingest.integrations.ashby.onboarding import (
    finalize_install,
    register_webhook_installation,
)
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


router = APIRouter(prefix="/integrations/ashby", tags=["ashby"])


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


def _inputs(body: dict[str, Any]) -> tuple[str, str, str]:
    api_token = str(body.get("api_token") or "").strip()
    org_id = str(body.get("org_id") or body.get("organization_id") or "ashby").strip()
    if not api_token:
        raise HTTPException(status_code=400, detail="api_token is required")
    base_url = native_connect_base_url(
        body.get("base_url"),
        endpoint_name="ashby_api",
    )
    if not org_id:
        raise HTTPException(status_code=400, detail="org_id is required")
    return api_token, base_url, org_id


def _entities(body: dict[str, Any]) -> list[str]:
    raw = body.get("entities")
    if raw is None:
        return list(DEFAULT_ENTITIES)
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    allowed = set(DEFAULT_ENTITIES)
    out = [str(item).strip() for item in raw if str(item).strip() in allowed]
    return out or list(DEFAULT_ENTITIES)


def _auth_failure(exc: AshbyApiError) -> JSONResponse:
    unauthorized = getattr(exc, "code", "") == "ashby_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "ashby_auth_failed" if unauthorized else "ashby_api_error",
            "message": (
                "Ashby rejected the API token."
                if unauthorized
                else "Could not reach Ashby. Check the API base URL."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    api_token, base_url, org_id = _inputs(body)
    client = AshbyClient(
        base_url=base_url,
        org_id=org_id,
        api_key=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        jobs, _cursor, _sync = await client.list_entities("job", limit=1)
    except AshbyApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()
    return JSONResponse(
        content={
            "ok": True,
            "base_url": base_url,
            "org_id": org_id,
            "sample_job_count": len(jobs),
            "entities": _entities(body),
        }
    )


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    api_token, base_url, org_id = _inputs(body)
    entities = _entities(body)
    webhook_secret = str(body.get("webhook_secret") or "").strip() or None

    client = AshbyClient(
        base_url=base_url,
        org_id=org_id,
        api_key=api_token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        await client.list_entities("job", limit=1)
    except AshbyApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()

    secret_ref = await store.put(
        api_token, label=f"ashby_api_token:{org_id}", tenant_id=tenant_id
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"ashby_webhook_secret:{org_id}", tenant_id=tenant_id
        )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        org_id=org_id,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        webhook_secret_ref=webhook_secret_ref,
    )
    if webhook_secret_ref:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            org_id=org_id,
            webhook_secret_ref=webhook_secret_ref,
        )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(install_id),
            "entity_count": len(entities),
            "webhook_registered": webhook_secret_ref is not None,
        }
    )


__all__ = ["router"]
