"""Admin-present Telegram connect router."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import TelegramApiError
from services.ingest.integrations.telegram.client import TelegramClient
from services.ingest.integrations.telegram.onboarding import finalize_install


router = APIRouter(prefix="/integrations/telegram", tags=["telegram"])


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


def _inputs(body: dict[str, Any]) -> tuple[str, str, str, str, str | None]:
    account_label = str(body.get("account_label") or "").strip()
    api_id = str(body.get("api_id") or "").strip()
    api_hash = str(body.get("api_hash") or "").strip()
    live_session = str(body.get("live_session") or body.get("session") or "").strip()
    backfill_session = str(body.get("backfill_session") or "").strip() or None
    if not account_label or not api_id or not api_hash or not live_session:
        raise HTTPException(
            status_code=400,
            detail="account_label, api_id, api_hash, and live_session are required",
        )
    return account_label, api_id, api_hash, live_session, backfill_session


async def _resolve_dialogs(
    body: dict[str, Any],
    *,
    api_id: str,
    api_hash: str,
    session: str,
) -> tuple[dict[str, Any], list[dict[str, Any]] | JSONResponse]:
    requested = body.get("dialogs")
    if requested is not None and not isinstance(requested, list):
        raise HTTPException(status_code=400, detail="dialogs must be a list")
    client = TelegramClient(api_id=api_id, api_hash=api_hash, session=session)
    try:
        account = await client.me()
        dialogs = requested if requested else await client.iter_dialogs(limit=75)
    except TelegramApiError as exc:
        return {}, JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error_code": getattr(exc, "code", "telegram_api_error"),
                "message": "Telegram rejected the session or could not enumerate dialogs.",
            },
        )
    finally:
        await client.aclose()
    return account, dialogs


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    _tenant_from_request(request)
    body = await request.json()
    _account_label, api_id, api_hash, live_session, backfill_session = _inputs(body)
    account, dialogs = await _resolve_dialogs(
        body,
        api_id=api_id,
        api_hash=api_hash,
        session=backfill_session or live_session,
    )
    if isinstance(dialogs, JSONResponse):
        return dialogs
    return JSONResponse(
        content={"ok": True, "account": account, "dialog_count": len(dialogs)}
    )


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await request.json()
    account_label, api_id, api_hash, live_session, backfill_session = _inputs(body)
    effective_backfill = backfill_session or live_session
    account, dialogs = await _resolve_dialogs(
        body,
        api_id=api_id,
        api_hash=api_hash,
        session=effective_backfill,
    )
    if isinstance(dialogs, JSONResponse):
        return dialogs
    if not dialogs:
        raise HTTPException(status_code=400, detail="no Telegram dialogs were available")
    api_hash_ref = await store.put(
        api_hash, label=f"telegram_api_hash:{account_label}", tenant_id=tenant_id
    )
    live_session_ref = await store.put(
        live_session, label=f"telegram_live_session:{account_label}", tenant_id=tenant_id
    )
    backfill_session_ref = await store.put(
        effective_backfill,
        label=f"telegram_backfill_session:{account_label}",
        tenant_id=tenant_id,
    )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        account_label=account_label,
        dialogs=dialogs,
        api_id=api_id,
        api_hash_secret_ref=api_hash_ref,
        session_secret_ref=live_session_ref,
        backfill_session_secret_ref=backfill_session_ref,
    )
    return JSONResponse(
        content={
            "ok": True,
            "installation_id": str(install_id),
            "account": account,
            "dialog_count": len(dialogs),
        }
    )


__all__ = ["router"]
