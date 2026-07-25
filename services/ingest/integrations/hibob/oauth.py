"""Admin-present HiBob connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import HibobApiError
from services.ingest.integrations.hibob.client import HibobClient, DEFAULT_ENTITIES
from services.ingest.integrations.hibob.onboarding import (
    finalize_install,
    register_webhook_installation,
)
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)


router = APIRouter(prefix="/integrations/hibob", tags=["hibob"])
_DEFAULT_BASE_URL = "https://api.hibob.com"


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


def _inputs(body: dict[str, Any]) -> tuple[str, str, str, str]:
    company_id = str(body.get("company_id") or "").strip()
    service_user_id = str(body.get("service_user_id") or "").strip()
    token = str(body.get("service_user_token") or body.get("token") or "").strip()
    base_url = str(body.get("base_url") or _DEFAULT_BASE_URL).strip().rstrip("/")
    if not company_id:
        raise HTTPException(status_code=400, detail="company_id is required")
    if not service_user_id:
        raise HTTPException(status_code=400, detail="service_user_id is required")
    if not token:
        raise HTTPException(status_code=400, detail="service_user_token is required")
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    return company_id, service_user_id, token, base_url


def _entities(body: dict[str, Any]) -> list[str]:
    raw = body.get("entities")
    if raw is None:
        return list(DEFAULT_ENTITIES)
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="entities must be a list")
    allowed = set(DEFAULT_ENTITIES)
    out = [str(item).strip() for item in raw if str(item).strip() in allowed]
    return out or list(DEFAULT_ENTITIES)


def _auth_failure(exc: HibobApiError) -> JSONResponse:
    unauthorized = getattr(exc, "code", "") == "hibob_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "hibob_auth_failed" if unauthorized else "hibob_api_error",
            "message": (
                "HiBob rejected the service-user credential."
                if unauthorized
                else "Could not reach HiBob. Check the API base URL."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    company_id, service_user_id, token, base_url = _inputs(body)
    client = HibobClient(
        base_url=base_url,
        company_id=company_id,
        service_user_id=service_user_id,
        token=token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        info = await client.company_info()
    except HibobApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()
    return JSONResponse(content={"ok": True, "company": info, "entities": _entities(body)})


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    company_id, service_user_id, token, base_url = _inputs(body)
    entities = _entities(body)
    webhook_secret = str(body.get("webhook_secret") or "").strip() or None

    client = HibobClient(
        base_url=base_url,
        company_id=company_id,
        service_user_id=service_user_id,
        token=token,
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        await client.company_info()
    except HibobApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()

    secret_ref = await store.put(
        token, label=f"hibob_service_user_token:{company_id}", tenant_id=tenant_id
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"hibob_webhook_secret:{company_id}", tenant_id=tenant_id
        )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        company_id=company_id,
        service_user_id=service_user_id,
        base_url=base_url,
        entities=entities,
        secret_ref=secret_ref,
        webhook_secret_ref=webhook_secret_ref,
    )
    if webhook_secret_ref:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            company_id=company_id,
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
