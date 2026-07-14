"""Admin-present Signal connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.signal.onboarding import finalize_install


router = APIRouter(prefix="/integrations/signal", tags=["signal"])


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


def _threads(body: dict[str, Any]) -> list[dict[str, Any]]:
    raw = body.get("threads") or []
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="threads must be a list")
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            thread_id = int(item.get("thread_id"))
        except (TypeError, ValueError):
            continue
        out.append(
            {
                "thread_id": thread_id,
                "thread_kind": str(item.get("thread_kind") or "direct"),
                "title": item.get("title"),
            }
        )
    return out


def _inputs(body: dict[str, Any]) -> tuple[str, str, str | None, list[dict[str, Any]]]:
    account_label = str(body.get("account_label") or "").strip()
    session = str(body.get("linked_device_session") or body.get("session") or "").strip()
    backfill_session = str(body.get("backfill_session") or "").strip() or None
    if not account_label:
        raise HTTPException(status_code=400, detail="account_label is required")
    if not session:
        raise HTTPException(status_code=400, detail="linked_device_session is required")
    return account_label, session, backfill_session, _threads(body)


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    _tenant_from_request(request)
    body = await request.json()
    account_label, _session, _backfill_session, threads = _inputs(body)
    return JSONResponse(
        content={
            "ok": True,
            "account_label": account_label,
            "thread_count": len(threads),
            "verification": "linked_device_material_present",
        }
    )


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    account_label, session, backfill_session, threads = _inputs(body)
    session_ref = await store.put(
        session, label=f"signal_linked_device:{account_label}", tenant_id=tenant_id
    )
    backfill_ref = None
    if backfill_session:
        backfill_ref = await store.put(
            backfill_session,
            label=f"signal_backfill_device:{account_label}",
            tenant_id=tenant_id,
        )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        account_label=account_label,
        threads=threads,
        session_secret_ref=session_ref,
        backfill_session_secret_ref=backfill_ref,
    )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(install_id),
            "thread_count": len(threads),
        }
    )


__all__ = ["router"]
