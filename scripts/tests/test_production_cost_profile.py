from __future__ import annotations

import json
import subprocess
import sys

from services.platform.performance.cost_model import (
    CostAssumptions,
    estimate_cost_profile,
)
from services.platform.performance.load_profiles import build_load_plan


def test_load_profile_exposes_cost_budget_inputs() -> None:
    plan = build_load_plan("beta")

    assert plan["cost_budgets"] == {
        "think_llm_spend_usd_per_day": 25.0,
        "think_llm_tokens_per_day": 5_000_000,
        "think_llm_requests_per_day": 5_000,
        "embedding_spend_usd_per_day": 10.0,
        "object_storage_growth_gb_per_month": 300,
        "postgres_storage_growth_gb_per_month": 50,
    }


def test_cost_profile_uses_configurable_unit_prices() -> None:
    estimate = estimate_cost_profile(
        "beta",
        assumptions=CostAssumptions(
            gateway_compute_usd_per_hour=0.0,
            worker_compute_usd_per_hour=0.0,
            postgres_compute_usd_per_hour=0.0,
            broker_compute_usd_per_hour=0.0,
            object_storage_usd_per_gb_month=1.0,
            postgres_storage_usd_per_gb_month=2.0,
            observability_usd_per_day=0.0,
        ),
    )

    breakdown = estimate["cost_breakdown_usd"]
    assert breakdown["object_storage_per_month"] == 300.0
    assert breakdown["postgres_storage_per_month"] == 100.0
    assert breakdown["llm_per_day"] == 25.0
    assert breakdown["embeddings_per_day"] == 10.0
    assert breakdown["total_per_day"] == 48.3333


def test_cost_profile_scales_usage_budgets_not_fixed_compute() -> None:
    full = estimate_cost_profile("ga")
    smoke = estimate_cost_profile("ga", scale=0.1)

    assert smoke["usage_budgets"]["think_llm_spend_usd_per_day"] == 10.0
    assert full["cost_breakdown_usd"]["fixed_compute_per_day"] == (
        smoke["cost_breakdown_usd"]["fixed_compute_per_day"]
    )
    assert full["cost_breakdown_usd"]["total_per_day"] > (
        smoke["cost_breakdown_usd"]["total_per_day"]
    )


def test_estimate_production_cost_profile_cli_outputs_json() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/estimate_production_cost_profile.py",
            "beta",
            "--scale",
            "0.5",
            "--observability-usd-per-day",
            "1.25",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)

    assert payload["profile"] == "beta"
    assert payload["scale"] == 0.5
    assert payload["assumptions"]["observability_usd_per_day"] == 1.25
