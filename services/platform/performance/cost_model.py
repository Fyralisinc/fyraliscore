"""Configurable production cost model for Fyralis launch profiles."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.platform.performance.load_profiles import ProfileName, build_load_plan


@dataclass(frozen=True, slots=True)
class CostAssumptions:
    gateway_compute_usd_per_hour: float = 0.20
    worker_compute_usd_per_hour: float = 0.60
    postgres_compute_usd_per_hour: float = 0.50
    broker_compute_usd_per_hour: float = 0.25
    object_storage_usd_per_gb_month: float = 0.023
    postgres_storage_usd_per_gb_month: float = 0.115
    observability_usd_per_day: float = 5.0


def estimate_cost_profile(
    profile_name: ProfileName,
    *,
    scale: float = 1.0,
    assumptions: CostAssumptions | None = None,
) -> dict[str, Any]:
    if scale <= 0:
        raise ValueError("scale must be positive")
    assumptions = assumptions or CostAssumptions()
    plan = build_load_plan(profile_name, scale=scale)
    budgets = plan["cost_budgets"]
    assert isinstance(budgets, dict)

    fixed_compute_daily = round(
        24
        * (
            assumptions.gateway_compute_usd_per_hour
            + assumptions.worker_compute_usd_per_hour
            + assumptions.postgres_compute_usd_per_hour
            + assumptions.broker_compute_usd_per_hour
        ),
        4,
    )
    object_storage_monthly = round(
        float(budgets["object_storage_growth_gb_per_month"])
        * assumptions.object_storage_usd_per_gb_month,
        4,
    )
    postgres_storage_monthly = round(
        float(budgets["postgres_storage_growth_gb_per_month"])
        * assumptions.postgres_storage_usd_per_gb_month,
        4,
    )
    variable_daily = round(
        float(budgets["think_llm_spend_usd_per_day"])
        + float(budgets["embedding_spend_usd_per_day"])
        + assumptions.observability_usd_per_day
        + (object_storage_monthly + postgres_storage_monthly) / 30,
        4,
    )
    daily_total = round(fixed_compute_daily + variable_daily, 4)

    return {
        "profile": profile_name,
        "scale": scale,
        "assumptions": asdict(assumptions),
        "usage_budgets": budgets,
        "cost_breakdown_usd": {
            "fixed_compute_per_day": fixed_compute_daily,
            "llm_per_day": budgets["think_llm_spend_usd_per_day"],
            "embeddings_per_day": budgets["embedding_spend_usd_per_day"],
            "observability_per_day": assumptions.observability_usd_per_day,
            "object_storage_per_month": object_storage_monthly,
            "postgres_storage_per_month": postgres_storage_monthly,
            "variable_per_day": variable_daily,
            "total_per_day": daily_total,
            "total_per_month": round(daily_total * 30, 4),
        },
    }


__all__ = ["CostAssumptions", "estimate_cost_profile"]
