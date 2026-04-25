"""services/bridge/dashboards.py — render functions that package
queries.py output for dashboard endpoints. Each returns a Pydantic
model ready to ship as a JSON body from the Gateway.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.db import get_pool
from lib.shared.types import GoalCachedHealth

from .queries import (
    CapabilityRisk,
    CriticalPathEntry,
    HealthPoint,
    RevenueAtRiskReport,
    capability_at_risk,
    critical_path,
    customer_health_timeline,
    revenue_at_risk,
)


class _BridgeModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class RevenueAtRiskDashboard(_BridgeModel):
    report: RevenueAtRiskReport
    top_at_risk_customers: list[UUID]
    fallback_warning: str | None = None


class CapacityDashboard(_BridgeModel):
    at_risk: list[CapabilityRisk]
    count_depleted: int


class GoalSummary(_BridgeModel):
    goal_id: UUID
    title: str
    cached_health: GoalCachedHealth
    parent_goal_id: UUID | None = None
    critical_path: list[CriticalPathEntry] = Field(default_factory=list)


class GoalTreeDashboard(_BridgeModel):
    goals: list[GoalSummary]
    generated_at: datetime


class CustomerServedCommitment(_BridgeModel):
    commitment_id: UUID
    title: str
    state: str
    revenue_at_risk_usd: Decimal | None = None
    relationship_kind: str
    criticality: str


class CustomerDetailDashboard(_BridgeModel):
    customer_resource_id: UUID
    identity: str
    arr_usd: Decimal
    served_commitments: list[CustomerServedCommitment]
    revenue_at_risk_usd: Decimal
    health_timeline: list[HealthPoint]
    active_deployments: list[UUID]


# =====================================================================
# Dashboard renderers
# =====================================================================


async def render_revenue_at_risk(
    tenant_id: UUID,
    *,
    horizon_days: int = 90,
    conn: asyncpg.Connection | None = None,
) -> RevenueAtRiskDashboard:
    report = await revenue_at_risk(
        tenant_id, horizon_days=horizon_days, conn=conn
    )
    top = [c.customer_resource_id for c in report.customers[:5]]
    fallback_warning = (
        f"{report.fallback_count} customer(s) used the ARR fallback "
        f"because their customer_commitments rows have revenue_at_risk_usd=NULL."
        if report.fallback_count > 0
        else None
    )
    return RevenueAtRiskDashboard(
        report=report,
        top_at_risk_customers=top,
        fallback_warning=fallback_warning,
    )


async def render_capacity(
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> CapacityDashboard:
    at_risk = await capability_at_risk(tenant_id, conn=conn)
    depleted = sum(1 for x in at_risk if x.utilization >= 1.0)
    return CapacityDashboard(at_risk=at_risk, count_depleted=depleted)


async def render_goals(
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> GoalTreeDashboard:
    """List all goals in the tenant with their cached_health and critical-path summary.

    The tree is flat-listed; callers reconstruct the parent/child
    relationships from `parent_goal_id`. This keeps the render simple
    and keeps the 500ms performance target easy to hit.
    """
    sql = """
        SELECT id, title, cached_health, parent_goal_id
        FROM goals
        WHERE tenant_id = $1
          AND archived_at IS NULL
        ORDER BY altitude, created_at
    """

    async def _do(c: asyncpg.Connection) -> GoalTreeDashboard:
        rows = await c.fetch(sql, tenant_id)
        out: list[GoalSummary] = []
        for r in rows:
            cp = await critical_path(r["id"], tenant_id=tenant_id, conn=c)
            out.append(
                GoalSummary(
                    goal_id=r["id"],
                    title=r["title"],
                    cached_health=r["cached_health"],
                    parent_goal_id=r["parent_goal_id"],
                    critical_path=cp,
                )
            )
        return GoalTreeDashboard(
            goals=out, generated_at=datetime.now(timezone.utc)
        )

    if conn is not None:
        return await _do(conn)
    pool = get_pool()
    async with pool.acquire() as c2:
        return await _do(c2)


async def render_customer_detail(
    customer_id: UUID,
    *,
    tenant_id: UUID,
    window_days: int = 30,
    conn: asyncpg.Connection | None = None,
) -> CustomerDetailDashboard:
    """Per-customer dashboard payload."""
    sql_resource = """
        SELECT id, identity, current_value
        FROM resources
        WHERE id = $1 AND tenant_id = $2 AND kind = 'relational'
    """
    sql_served = """
        SELECT cc.commitment_id,
               c.title, c.state,
               cc.revenue_at_risk_usd,
               cc.relationship_kind, cc.criticality
        FROM customer_commitments cc
        JOIN commitments c ON c.id = cc.commitment_id
        WHERE cc.customer_resource_id = $1
          AND cc.tenant_id = $2
        ORDER BY c.created_at DESC
    """
    sql_deploys = """
        SELECT DISTINCT rd.resource_id
        FROM resource_deployments rd
        JOIN customer_commitments cc ON cc.commitment_id = rd.commitment_id
        WHERE cc.customer_resource_id = $1
          AND cc.tenant_id = $2
          AND rd.released_at IS NULL
    """

    async def _do(c: asyncpg.Connection) -> CustomerDetailDashboard:
        cust = await c.fetchrow(sql_resource, customer_id, tenant_id)
        if cust is None:
            raise ValueError(
                f"customer {customer_id} not found in tenant {tenant_id}"
            )
        cv = dict(cust["current_value"] or {})
        arr_cents = int(cv.get("arr_cents", 0) or 0)
        arr_usd = (Decimal(arr_cents) / Decimal(100)).quantize(Decimal("0.01"))

        served_rows = await c.fetch(sql_served, customer_id, tenant_id)
        served: list[CustomerServedCommitment] = []
        total_at_risk = Decimal("0")
        for r in served_rows:
            rar = r["revenue_at_risk_usd"]
            # Compute at-risk contribution using the same fallback semantics
            # as revenue_at_risk (per-served-linkage ARR split).
            served.append(
                CustomerServedCommitment(
                    commitment_id=r["commitment_id"],
                    title=r["title"],
                    state=r["state"],
                    revenue_at_risk_usd=(
                        Decimal(rar) if rar is not None else None
                    ),
                    relationship_kind=r["relationship_kind"],
                    criticality=r["criticality"],
                )
            )

        # revenue_at_risk for this specific customer (reuse top-level).
        report = await revenue_at_risk(tenant_id, conn=c)
        for row in report.customers:
            if row.customer_resource_id == customer_id:
                total_at_risk = row.total_at_risk_usd
                break

        timeline = await customer_health_timeline(
            customer_id, tenant_id=tenant_id, window_days=window_days, conn=c
        )
        deploy_rows = await c.fetch(sql_deploys, customer_id, tenant_id)
        active_deployments = [r["resource_id"] for r in deploy_rows]

        return CustomerDetailDashboard(
            customer_resource_id=customer_id,
            identity=cust["identity"],
            arr_usd=arr_usd,
            served_commitments=served,
            revenue_at_risk_usd=total_at_risk,
            health_timeline=timeline,
            active_deployments=active_deployments,
        )

    if conn is not None:
        return await _do(conn)
    pool = get_pool()
    async with pool.acquire() as c2:
        return await _do(c2)


__all__ = [
    "CapacityDashboard",
    "CustomerDetailDashboard",
    "CustomerServedCommitment",
    "GoalSummary",
    "GoalTreeDashboard",
    "RevenueAtRiskDashboard",
    "render_capacity",
    "render_customer_detail",
    "render_goals",
    "render_revenue_at_risk",
]
