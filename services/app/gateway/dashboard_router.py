"""Dashboard adapter endpoints."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.platform.access_control.audit import (
    record_override_if_needed as record_access_override_if_needed,
)
from services.platform.access_control.checks import AccessDecision, can_read_by_id


_ReadableKind = Literal["commitment", "goal", "resource"]


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
            result = await _filter_revenue_at_risk(result, conn=conn, auth=auth)
        return json.loads(result.model_dump_json())

    @router.get("/goals")
    async def get_dashboard_goals(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_goals

        auth = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_goals(auth.tenant_id, conn=conn)
            result = await _filter_goal_tree(result, conn=conn, auth=auth)
        return json.loads(result.model_dump_json())

    @router.get("/capacity")
    async def get_dashboard_capacity(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_capacity

        auth = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_capacity(auth.tenant_id, conn=conn)
            result = await _filter_capacity(result, conn=conn, auth=auth)
        return json.loads(result.model_dump_json())

    @router.get("/customer/{customer_id}")
    async def get_dashboard_customer(
        customer_id: str, request: Request, window_days: int = 30
    ) -> Any:
        from services.domain.bridge import render_customer_detail

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
            await _record_override_if_needed(
                decision,
                conn=conn,
                auth=auth,
                entity_type="resource",
                entity_id=cid,
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
            except ValueError:
                return JSONResponse({"error": "not_found"}, status_code=404)
            result = await _filter_customer_detail(result, conn=conn, auth=auth)
        return json.loads(result.model_dump_json())

    return router


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


async def _filter_revenue_at_risk(
    dashboard: Any,
    *,
    conn: asyncpg.Connection,
    auth: Any,
) -> Any:
    from services.domain.bridge.queries import RevenueAtRiskReport

    visible_customers = []
    for customer in dashboard.report.customers:
        if not await _can_read_entity(
            conn,
            auth,
            "resource",
            customer.customer_resource_id,
        ):
            continue
        commitment_ids = list(customer.at_risk_commitment_ids or [])
        if commitment_ids:
            visible_commitment_count = 0
            for commitment_id in commitment_ids:
                if await _can_read_entity(
                    conn,
                    auth,
                    "commitment",
                    commitment_id,
                ):
                    visible_commitment_count += 1
            if visible_commitment_count != len(commitment_ids):
                continue
        elif customer.total_at_risk_usd > Decimal("0"):
            continue
        visible_customers.append(customer)

    grand_total = sum(
        (c.total_at_risk_usd for c in visible_customers), Decimal("0")
    ).quantize(Decimal("0.01"))
    fallback_count = sum(1 for c in visible_customers if c.fallback_used)
    report = RevenueAtRiskReport(
        tenant_id=dashboard.report.tenant_id,
        horizon_days=dashboard.report.horizon_days,
        generated_at=dashboard.report.generated_at,
        customers=visible_customers,
        grand_total_usd=grand_total,
        fallback_count=fallback_count,
    )
    warning = (
        f"{fallback_count} customer(s) used the ARR fallback "
        "because their customer_commitments rows have "
        "revenue_at_risk_usd=NULL."
        if fallback_count > 0
        else None
    )
    return dashboard.model_copy(
        update={
            "report": report,
            "top_at_risk_customers": [
                c.customer_resource_id for c in visible_customers[:5]
            ],
            "fallback_warning": warning,
        }
    )


async def _filter_goal_tree(
    dashboard: Any,
    *,
    conn: asyncpg.Connection,
    auth: Any,
) -> Any:
    filtered_goals = []
    for goal in dashboard.goals:
        if not await _can_read_entity(conn, auth, "goal", goal.goal_id):
            continue
        critical_path = []
        for entry in goal.critical_path:
            if await _can_read_entity(
                conn,
                auth,
                "commitment",
                entry.commitment.id,
            ):
                critical_path.append(entry)
        filtered_goals.append(
            goal.model_copy(update={"critical_path": critical_path})
        )

    visible_goal_ids = {goal.goal_id for goal in filtered_goals}
    filtered_goals = [
        goal.model_copy(
            update={
                "parent_goal_id": (
                    goal.parent_goal_id
                    if goal.parent_goal_id in visible_goal_ids
                    else None
                )
            }
        )
        for goal in filtered_goals
    ]
    return dashboard.model_copy(update={"goals": filtered_goals})


async def _filter_capacity(
    dashboard: Any,
    *,
    conn: asyncpg.Connection,
    auth: Any,
) -> Any:
    filtered_risks = []
    for risk in dashboard.at_risk:
        if not await _can_read_entity(conn, auth, "resource", risk.resource_id):
            continue
        visible_commitment_ids = []
        for commitment_id in risk.deploying_commitment_ids:
            if await _can_read_entity(
                conn,
                auth,
                "commitment",
                commitment_id,
            ):
                visible_commitment_ids.append(commitment_id)
        filtered_risks.append(
            risk.model_copy(
                update={"deploying_commitment_ids": visible_commitment_ids}
            )
        )
    return dashboard.model_copy(
        update={
            "at_risk": filtered_risks,
            "count_depleted": sum(
                1 for risk in filtered_risks if risk.utilization >= 1.0
            ),
        }
    )


async def _filter_customer_detail(
    dashboard: Any,
    *,
    conn: asyncpg.Connection,
    auth: Any,
) -> Any:
    visible_served = []
    for served in dashboard.served_commitments:
        if await _can_read_entity(
            conn,
            auth,
            "commitment",
            served.commitment_id,
        ):
            visible_served.append(served)

    all_served_visible = len(visible_served) == len(dashboard.served_commitments)
    visible_deployments = []
    for resource_id in dashboard.active_deployments:
        if await _can_read_entity(conn, auth, "resource", resource_id):
            visible_deployments.append(resource_id)

    update: dict[str, Any] = {
        "served_commitments": visible_served,
        "active_deployments": visible_deployments,
    }
    if not all_served_visible:
        update["revenue_at_risk_usd"] = Decimal("0")
        update["health_timeline"] = []
    return dashboard.model_copy(update=update)


async def _can_read_entity(
    conn: asyncpg.Connection,
    auth: Any,
    kind: _ReadableKind,
    entity_id: UUID,
) -> bool:
    decision = await can_read_by_id(
        auth.actor_id,
        kind,
        entity_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await _record_override_if_needed(
        decision,
        conn=conn,
        auth=auth,
        entity_type=kind,
        entity_id=entity_id,
    )
    return decision.allowed


async def _record_override_if_needed(
    decision: AccessDecision,
    *,
    conn: asyncpg.Connection,
    auth: Any,
    entity_type: str,
    entity_id: UUID,
) -> None:
    await record_access_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type=entity_type,
        entity_id=entity_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
