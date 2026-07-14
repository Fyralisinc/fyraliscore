"""Admin-present AWS connect router."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.aws.onboarding import finalize_install


router = APIRouter(prefix="/integrations/aws", tags=["aws"])


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


def _inputs(body: dict[str, Any]) -> tuple[str, str, str, dict[str, Any], int]:
    account_id = str(body.get("account_id") or "").strip()
    region = str(body.get("region") or "us-east-1").strip()
    credential_kind = str(body.get("credential_kind") or "assume_role").strip()
    backfill_days = int(body.get("backfill_window_days") or 90)
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")
    if credential_kind not in {"assume_role", "static_keys"}:
        raise HTTPException(status_code=400, detail="credential_kind must be assume_role or static_keys")
    if credential_kind == "assume_role":
        role_arn = str(body.get("role_arn") or "").strip()
        if not role_arn.startswith("arn:aws:iam::"):
            raise HTTPException(status_code=400, detail="role_arn is required")
        material: dict[str, Any] = {"role_arn": role_arn}
        if body.get("external_id"):
            material["external_id"] = str(body["external_id"])
    else:
        access_key_id = str(body.get("access_key_id") or "").strip()
        secret_access_key = str(body.get("secret_access_key") or "").strip()
        if not access_key_id or not secret_access_key:
            raise HTTPException(status_code=400, detail="access_key_id and secret_access_key are required")
        material = {
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
        }
        if body.get("session_token"):
            material["session_token"] = str(body["session_token"])
    return account_id, region, credential_kind, material, backfill_days


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    _tenant_from_request(request)
    body = await request.json()
    account_id, region, credential_kind, _material, backfill_days = _inputs(body)
    return JSONResponse(
        content={
            "ok": True,
            "account_id": account_id,
            "region": region,
            "credential_kind": credential_kind,
            "backfill_window_days": backfill_days,
            "verification": "shape_only_before_secret_storage",
        }
    )


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    account_id, region, credential_kind, material, backfill_days = _inputs(body)
    secret_ref = await store.put(
        json.dumps(material, sort_keys=True),
        label=f"aws_{credential_kind}:{account_id}:{region}",
        tenant_id=tenant_id,
    )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        account_id=account_id,
        region=region,
        credential_kind=credential_kind,
        secret_ref=secret_ref,
        backfill_window_days=backfill_days,
    )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(install_id),
            "account_id": account_id,
            "region": region,
            "credential_kind": credential_kind,
        }
    )


__all__ = ["router"]
