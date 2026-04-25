"""services/greeting/api.py — Phase 6.

FastAPI router that assembles the cached CEO view into the CONTRACTS
§1.1 payload. Also exposes `POST /view/ceo/force-refresh` for dev use.

Auth: mirrors stream.py — static-token resolver via the shared
`ViewCeoStreamManager` token map, so the same token that opens the WS
opens the HTTP endpoint. Single-tenant dogfood is the only runtime;
this is explicitly not a production auth path.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse

from services.greeting.cache import CACHE_KEYS, CachedContent, ViewCeoCacheRepo
from services.greeting.scheduler import GreetingScheduler
from services.greeting.stream import ViewCeoStreamManager


log = logging.getLogger(__name__)


def build_ceo_api_router(
    *,
    cache: ViewCeoCacheRepo,
    scheduler: GreetingScheduler,
    stream_manager: ViewCeoStreamManager,
    default_tenant_id: UUID | None = None,
) -> APIRouter:
    """Router that exposes:
      GET  /view/ceo/home
      POST /view/ceo/force-refresh

    Caller mounts on their FastAPI app. When `default_tenant_id` is set,
    unauthenticated requests fall back to that tenant — used in
    single-tenant dogfood so the UI doesn't need to ship a token while
    real auth is deferred.
    """
    router = APIRouter()

    async def _auth(request: Request) -> UUID:
        token = _extract_token(request)
        if not token:
            if default_tenant_id is not None:
                return default_tenant_id
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"error": "missing_token"},
            )
        tenant_id = stream_manager.resolve_token(token)
        if tenant_id is None:
            if default_tenant_id is not None:
                return default_tenant_id
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"error": "invalid_token"},
            )
        return tenant_id

    @router.get("/view/ceo/home")
    async def get_home(request: Request) -> JSONResponse:
        tenant_id = await _auth(request)
        rows = await cache.get_all(tenant_id)

        # If any expected key is missing, return a partial with sensible
        # defaults — never 500 the UI. The UI renders a staleness
        # indicator when staleness_seconds is large.
        missing = [k for k in CACHE_KEYS if k not in rows]
        if missing:
            log.info(
                "view_ceo.home_partial",
                extra={
                    "tenant_id": str(tenant_id),
                    "missing": missing,
                },
            )

        response = _assemble_home_payload(tenant_id, rows)
        return JSONResponse(response)

    @router.post("/view/ceo/force-refresh")
    async def force_refresh(request: Request) -> JSONResponse:
        tenant_id = await _auth(request)
        await scheduler.refresh_tenant(tenant_id, reason="manual")
        return JSONResponse(
            {"ok": True, "tenant_id": str(tenant_id)},
            status_code=200,
        )

    return router


# =====================================================================
# Payload assembly
# =====================================================================


def _assemble_home_payload(
    tenant_id: UUID,
    rows: dict[str, CachedContent],
) -> dict[str, Any]:
    """Build the CONTRACTS §1.1 shape from cache rows. Missing rows get
    safe defaults so the UI always has something to render.
    """
    greeting_row = rows.get("greeting")
    qg_row = rows.get("query_grid")
    cards_row = rows.get("cards")
    status_row = rows.get("status")
    close_row = rows.get("close_line")

    # --- greeting ----------------------------------------------------
    if greeting_row is not None:
        g = dict(greeting_row.content)
        g["cached_at"] = _iso(greeting_row.cached_at)
        g["staleness_seconds"] = _round_seconds(greeting_row.staleness_seconds)
        # Ensure meta has all required keys.
        meta = dict(g.get("meta") or {})
        meta.setdefault(
            "date_iso", datetime.now(timezone.utc).date().isoformat()
        )
        meta.setdefault("recomputed_at", _iso(greeting_row.cached_at))
        meta.setdefault("signals_watched_count", 0)
        g["meta"] = meta
        g.setdefault("body_html", "")
        greeting = g
    else:
        now_iso = datetime.now(timezone.utc).isoformat()
        greeting = {
            "meta": {
                "date_iso": datetime.now(timezone.utc).date().isoformat(),
                "recomputed_at": now_iso,
                "signals_watched_count": 0,
            },
            "body_html": "",
            "cached_at": now_iso,
            "staleness_seconds": 0,
        }

    # --- query_grid --------------------------------------------------
    if qg_row is not None:
        qg = {
            "queries": list(qg_row.content.get("queries") or []),
            "cached_at": _iso(qg_row.cached_at),
        }
    else:
        qg = {
            "queries": [],
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }

    # --- cards -------------------------------------------------------
    if cards_row is not None:
        cards = list(cards_row.content.get("cards") or [])
        for c in cards:
            c.setdefault("cached_at", _iso(cards_row.cached_at))
    else:
        cards = []

    # --- status ------------------------------------------------------
    if status_row is not None:
        s = dict(status_row.content)
        s.setdefault("substrate_alive", True)
        s.setdefault("calibration_pct", 0)
        s.setdefault("needs_you_count", 0)
        st = s
    else:
        st = {
            "substrate_alive": False,
            "calibration_pct": 0,
            "needs_you_count": 0,
        }

    # --- close_line --------------------------------------------------
    if close_row is not None:
        cl = {
            "body": close_row.content.get("body", ""),
            "metadata": close_row.content.get("metadata") or {
                "signal_count": 0,
                "external_moves": 0,
                "calibration_pct": 0,
            },
        }
    else:
        cl = {
            "body": "",
            "metadata": {
                "signal_count": 0,
                "external_moves": 0,
                "calibration_pct": 0,
            },
        }

    return {
        "greeting": greeting,
        "query_grid": qg,
        "cards": cards,
        "close_line": cl,
        "status": st,
    }


# =====================================================================
# helpers
# =====================================================================


def _extract_token(request: Request) -> str | None:
    auth = request.headers.get("authorization") or request.headers.get(
        "Authorization"
    )
    if auth and auth.lower().startswith("bearer "):
        return auth[len("Bearer "):].strip()
    token = request.query_params.get("token")
    return token


def _iso(ts: datetime | None) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _round_seconds(v: float) -> int:
    try:
        return int(round(float(v)))
    except (ValueError, TypeError):
        return 0


__all__ = ["build_ceo_api_router"]
