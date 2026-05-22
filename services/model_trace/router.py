"""services/model_trace/router.py — FastAPI router for the Model
page's trace controls (spec §2.4).

Three GET endpoints, all auth-gated through the gateway's bearer
middleware (tenant resolved from request.state.auth):

  GET /v1/model/{node_id}/trace?direction=back|forward&max_depth=4
      Walk evidence (back) or impact (forward) chain.

  GET /v1/model/{node_id}/supports
      One-hop downstream adjacency: what this node supports.

  GET /v1/model/{node_id}/depends_on
      One-hop upstream adjacency: what this node depends on.

Sparse-data tolerance: if the seed node has no evidence chain, we
return the seed alone (or an empty list for adjacency endpoints). The
UI handles the empty case.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from services.model_trace.repo import (
    depends_on,
    supports,
    trace_back,
    trace_forward,
)


router = APIRouter(prefix="/v1/model", tags=["model"])


_MAX_DEPTH_CEILING = 8


# ---------------------------------------------------------------------
# Helpers — auth + deps. Local copies so the router has no static
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


def _parse_node_id(raw: str) -> UUID | None:
    try:
        return UUID(raw)
    except (ValueError, TypeError):
        return None


def _parse_max_depth(raw: str | None, default: int = 4) -> int | None:
    if raw is None:
        return default
    try:
        depth = int(raw)
    except (ValueError, TypeError):
        return None
    if depth < 0 or depth > _MAX_DEPTH_CEILING:
        return None
    return depth


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------


@router.get("/{node_id}/trace")
async def get_trace(node_id: str, request: Request) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth()
    nid = _parse_node_id(node_id)
    if nid is None:
        return _bad_request("invalid_node_id")

    qp = request.query_params
    direction = qp.get("direction", "back")
    if direction not in ("back", "forward"):
        return _bad_request("invalid_direction")
    max_depth = _parse_max_depth(qp.get("max_depth"))
    if max_depth is None:
        return _bad_request("invalid_max_depth")

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        if direction == "back":
            chain = await trace_back(conn, auth.tenant_id, nid, max_depth)
        else:
            chain = await trace_forward(conn, auth.tenant_id, nid, max_depth)
    return JSONResponse(
        {
            "node_id": str(nid),
            "direction": direction,
            "max_depth": max_depth,
            "chain": [step.to_dict() for step in chain],
        },
        status_code=200,
    )


@router.get("/{node_id}/supports")
async def get_supports(node_id: str, request: Request) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth()
    nid = _parse_node_id(node_id)
    if nid is None:
        return _bad_request("invalid_node_id")

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        items = await supports(conn, auth.tenant_id, nid)
    return JSONResponse(
        {
            "node_id": str(nid),
            "items": [step.to_dict() for step in items],
        },
        status_code=200,
    )


@router.get("/{node_id}/depends_on")
async def get_depends_on(node_id: str, request: Request) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth()
    nid = _parse_node_id(node_id)
    if nid is None:
        return _bad_request("invalid_node_id")

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        items = await depends_on(conn, auth.tenant_id, nid)
    return JSONResponse(
        {
            "node_id": str(nid),
            "items": [step.to_dict() for step in items],
        },
        status_code=200,
    )


__all__ = ["router"]
