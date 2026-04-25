"""services/bridge/ — Wave 5-B full Bridge queries + dashboards.

Per BUILD-PLAN §6 Prompt 5.B and ARCHITECTURE-FINAL §27. These are
the *dashboard-grade* Bridge queries. The Wave 2-C primitives in
`services/resources/bridge.py` remain for in-cascade quick checks; the
module here supersedes them for anything rendered on a dashboard.

Public surface:

  queries:
    - revenue_at_risk
    - capability_at_risk
    - commitment_feasibility
    - critical_path
    - customer_health_timeline

  dashboards:
    - render_revenue_at_risk
    - render_capacity
    - render_goals
    - render_customer_detail

All queries begin with `WHERE tenant_id = $1` per BUILD-PLAN Hard
Constraint on absolute tenant isolation.
"""
from __future__ import annotations

from .dashboards import (
    CapacityDashboard,
    CustomerDetailDashboard,
    GoalTreeDashboard,
    RevenueAtRiskDashboard,
    render_capacity,
    render_customer_detail,
    render_goals,
    render_revenue_at_risk,
)
from .queries import (
    CapabilityRisk,
    CriticalPathEntry,
    CustomerRevenueRow,
    FeasibilityReport,
    HealthPoint,
    ProposedCommitment,
    RevenueAtRiskReport,
    capability_at_risk,
    commitment_feasibility,
    critical_path,
    customer_health_timeline,
    revenue_at_risk,
)


__all__ = [
    # queries
    "CapabilityRisk",
    "CriticalPathEntry",
    "CustomerRevenueRow",
    "FeasibilityReport",
    "HealthPoint",
    "ProposedCommitment",
    "RevenueAtRiskReport",
    "capability_at_risk",
    "commitment_feasibility",
    "critical_path",
    "customer_health_timeline",
    "revenue_at_risk",
    # dashboards
    "CapacityDashboard",
    "CustomerDetailDashboard",
    "GoalTreeDashboard",
    "RevenueAtRiskDashboard",
    "render_capacity",
    "render_customer_detail",
    "render_goals",
    "render_revenue_at_risk",
]
