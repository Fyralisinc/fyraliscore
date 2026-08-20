"""Admin-present Grafana connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import GrafanaApiError
from services.ingest.integrations.grafana.client import GrafanaClient
from services.ingest.integrations.grafana.onboarding import (
    finalize_install,
    register_webhook_installation,
)


router = APIRouter(prefix="/integrations/grafana", tags=["grafana"])


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
    base_url = str(body.get("base_url") or body.get("instance_url") or "").strip().rstrip("/")
    token = str(body.get("service_account_token") or body.get("api_token") or "").strip()
    org_id = str(body.get("org_id") or "1").strip()
    if not base_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="base_url must be a full URL")
    if not token:
        raise HTTPException(status_code=400, detail="service_account_token is required")
    return base_url, token, org_id


def _auth_failure(exc: GrafanaApiError) -> JSONResponse:
    unauthorized = getattr(exc, "code", "") == "grafana_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "grafana_auth_failed" if unauthorized else "grafana_api_error",
            "message": (
                "Grafana rejected the service-account token."
                if unauthorized
                else "Could not reach Grafana. Check the instance URL."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    _tenant_from_request(request)
    body = await request.json()
    base_url, token, org_id = _inputs(body)
    client = GrafanaClient(base_url=base_url, api_token=token)
    try:
        org = await client.get_org()
    except GrafanaApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()
    return JSONResponse(content={"ok": True, "base_url": base_url, "org_id": org.get("id") or org_id})


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    base_url, token, org_id = _inputs(body)
    webhook_secret = str(body.get("webhook_secret") or "").strip() or None

    client = GrafanaClient(base_url=base_url, api_token=token)
    try:
        org = await client.get_org()
    except GrafanaApiError as exc:
        return _auth_failure(exc)
    finally:
        await client.aclose()

    resolved_org_id = str(org.get("id") or org_id)
    secret_ref = await store.put(
        token, label=f"grafana_service_account_token:{base_url}", tenant_id=tenant_id
    )
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"grafana_webhook_secret:{base_url}", tenant_id=tenant_id
        )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        org_id=resolved_org_id,
        secret_ref=secret_ref,
        webhook_secret_ref=webhook_secret_ref,
    )
    if webhook_secret_ref:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            base_url=base_url,
            webhook_secret_ref=webhook_secret_ref,
        )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(install_id),
            "org_id": resolved_org_id,
            "webhook_registered": webhook_secret_ref is not None,
        }
    )


__all__ = ["router"]
