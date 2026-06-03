"""Dashboard adapter endpoints."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


def build_dashboard_router() -> APIRouter:
    router = APIRouter(prefix="/dashboard", tags=["dashboard"])

    @router.get("/revenue-at-risk")
    async def get_dashboard_revenue_at_risk(
        request: Request, horizon_days: int = 90
    ) -> dict[str, Any]:
        from services.domain.bridge import render_revenue_at_risk

        auth = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_revenue_at_risk(
                auth.tenant_id, horizon_days=int(horizon_days), conn=conn
            )
        return json.loads(result.model_dump_json())

    @router.get("/goals")
    async def get_dashboard_goals(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_goals

        auth = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_goals(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @router.get("/capacity")
    async def get_dashboard_capacity(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_capacity

        auth = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_capacity(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @router.get("/customer/{customer_id}")
    async def get_dashboard_customer(
        customer_id: str, request: Request, window_days: int = 30
    ) -> Any:
        from services.domain.bridge import render_customer_detail
        from services.platform.access_control.checks import can_read_by_id

        auth = request.state.auth
        deps = _deps(request)
        try:
            cid = UUID(customer_id)
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_customer_id"}, status_code=400)
        async with deps.pool.acquire() as conn:
            decision = await can_read_by_id(
                auth.actor_id,
                "resource",
                cid,
                conn=conn,
                tenant_id=auth.tenant_id,
            )
            if not decision.allowed:
                status_code = 404 if decision.reason == "entity_not_found" else 403
                return JSONResponse(
                    {"error": "access_denied", "reason": decision.reason},
                    status_code=status_code,
                )
            try:
                result = await render_customer_detail(
                    cid,
                    tenant_id=auth.tenant_id,
                    window_days=int(window_days),
                    conn=conn,
                )
            except ValueError as e:
                return JSONResponse({"error": str(e)}, status_code=404)
        return json.loads(result.model_dump_json())

    return router


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps
