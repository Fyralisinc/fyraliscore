"""Admin-present WhatsApp connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


router = APIRouter(prefix="/integrations/whatsapp", tags=["whatsapp"])


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


def _inputs(body: dict[str, Any]) -> dict[str, str | None]:
    phone_number_id = str(body.get("phone_number_id") or "").strip()
    app_secret = str(body.get("app_secret") or "").strip()
    verify_token = str(body.get("verify_token") or "").strip()
    if not phone_number_id or not app_secret or not verify_token:
        raise HTTPException(
            status_code=400,
            detail="phone_number_id, app_secret, and verify_token are required",
        )
    return {
        "phone_number_id": phone_number_id,
        "waba_id": str(body.get("business_account_id") or body.get("waba_id") or "").strip() or None,
        "display_phone_number": str(body.get("display_phone_number") or "").strip() or None,
        "app_secret": app_secret,
        "verify_token": verify_token,
        "access_token": str(body.get("access_token") or "").strip() or None,
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    _tenant_from_request(request)
    body = await request.json()
    values = _inputs(body)
    return JSONResponse(
        content={
            "ok": True,
            "phone_number_id": values["phone_number_id"],
            "business_account_id": values["waba_id"],
            "verification": "shape_only_before_webhook_challenge",
        }
    )


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    values = _inputs(body)
    phone_number_id = str(values["phone_number_id"])
    app_secret_ref = await store.put(
        values["app_secret"],
        label=f"whatsapp_app_secret:{phone_number_id}",
        tenant_id=tenant_id,
    )
    verify_token_ref = await store.put(
        values["verify_token"],
        label=f"whatsapp_verify_token:{phone_number_id}",
        tenant_id=tenant_id,
    )
    access_token_ref = None
    if values["access_token"]:
        access_token_ref = await store.put(
            values["access_token"],
            label=f"whatsapp_access_token:{phone_number_id}",
            tenant_id=tenant_id,
        )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO whatsapp_installations
                (tenant_id, phone_number_id, waba_id, display_phone_number,
                 app_secret_ref, verify_token_ref, access_token_ref,
                 app_secret, verify_token, access_token, enabled, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,NULL,NULL,true, now())
            ON CONFLICT (phone_number_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                waba_id = COALESCE(EXCLUDED.waba_id, whatsapp_installations.waba_id),
                display_phone_number = COALESCE(
                    EXCLUDED.display_phone_number,
                    whatsapp_installations.display_phone_number
                ),
                app_secret_ref = EXCLUDED.app_secret_ref,
                verify_token_ref = EXCLUDED.verify_token_ref,
                access_token_ref = COALESCE(
                    EXCLUDED.access_token_ref,
                    whatsapp_installations.access_token_ref
                ),
                app_secret = NULL,
                verify_token = NULL,
                access_token = CASE
                    WHEN EXCLUDED.access_token_ref IS NOT NULL THEN NULL
                    ELSE whatsapp_installations.access_token
                END,
                enabled = true,
                updated_at = now()
            RETURNING id, phone_number_id
            """,
            tenant_id,
            phone_number_id,
            values["waba_id"],
            values["display_phone_number"],
            app_secret_ref,
            verify_token_ref,
            access_token_ref,
        )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(row["phone_number_id"]),
            "installation_row_id": str(row["id"]),
        }
    )


__all__ = ["router"]
