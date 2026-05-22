"""services/history/router.py — FastAPI router for the Ledger page.

Currently exposes:

  GET /v1/history/summary?range_days=30
      The Ledger summary strip — six counters with WoW deltas (events,
      model_updates, predictions_made, predictions_accuracy,
      actions_taken, contestations). See spec §6.1.

Note: the existing `GET /v1/history` endpoint is still defined in
services/gateway/main.py. The integrator wires this router in
alongside that endpoint. We keep the new router separate so it can be
swapped / extended without touching main.py.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from services.history.summary import build_summary


router = APIRouter(prefix="/v1/history", tags=["history"])


_MIN_RANGE_DAYS = 1
_MAX_RANGE_DAYS = 365


# ---------------------------------------------------------------------
# Helpers — auth + deps. Local copies so this router has no static
# dependency on services.gateway.main.
# ---------------------------------------------------------------------


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised")
    return deps


def _auth(request: Request) -> Any | None:
    return getattr(request.state, "auth", None)


def _unauth() -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized"},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


def _bad_request(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "bad_request", "reason": reason},
        status_code=status.HTTP_400_BAD_REQUEST,
    )


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------


@router.get("/summary")
async def get_summary(request: Request) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth()

    raw = request.query_params.get("range_days", "30")
    try:
        range_days = int(raw)
    except (ValueError, TypeError):
        return _bad_request("invalid_range_days")
    if range_days < _MIN_RANGE_DAYS or range_days > _MAX_RANGE_DAYS:
        return _bad_request("invalid_range_days")

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        payload = await build_summary(
            tenant_id=auth.tenant_id,
            range_days=range_days,
            conn=conn,
        )
    return JSONResponse(payload, status_code=200)


__all__ = ["router"]
