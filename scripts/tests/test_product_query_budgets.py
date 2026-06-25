from __future__ import annotations

import json
from pathlib import Path

from services.app.gateway.product_workflow_metrics import PRODUCT_WORKFLOWS
from scripts.check_product_query_budgets import validate_budget_registry


ROUTE_BY_WORKFLOW = {
    "today": "/v1/today",
    "ask": "/v1/ask",
    "recommendations": "/v1/recommendations",
    "forecasts": "/v1/forecasts",
    "decision_review": "/v1/decision_deltas",
    "model_map": "/map/snapshot",
    "ceo_view": "/view/ceo/home",
    "source_onboarding": "/integrations/gmail/status",
    "dashboard": "/dashboard",
    "substrate": "/models",
    "rendering": "/rendering/card",
    "history": "/v1/history",
}


def _valid_budget(workflow: str) -> dict[str, object]:
    return {
        "owner": "platform",
        "beta_p95_seconds": 2.0,
        "beta_p99_seconds": 5.0,
        "ga_p95_seconds": 1.5,
        "ga_p99_seconds": 3.0,
        "hot_paths": [ROUTE_BY_WORKFLOW[workflow]],
        "index_review": ["tenant_id index reviewed"],
    }


def test_checked_in_product_query_budgets_cover_all_workflows() -> None:
    assert validate_budget_registry() == []


def test_query_budget_check_flags_missing_workflow(tmp_path: Path) -> None:
    path = tmp_path / "budgets.json"
    path.write_text(
        json.dumps(
            {
                "workflows": {
                    workflow: _valid_budget(workflow)
                    for workflow in PRODUCT_WORKFLOWS
                    if workflow != "history"
                }
            }
        ),
        encoding="utf-8",
    )

    violations = validate_budget_registry(path)

    assert "missing workflow budget(s): history" in violations


def test_query_budget_check_flags_hot_path_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "budgets.json"
    budgets = {workflow: _valid_budget(workflow) for workflow in PRODUCT_WORKFLOWS}
    budgets["ask"] = {
        **budgets["ask"],
        "hot_paths": ["/webhooks/slack"],
    }
    path.write_text(json.dumps({"workflows": budgets}), encoding="utf-8")

    violations = validate_budget_registry(path)

    assert any("classifies as None" in violation for violation in violations)


def test_query_budget_check_flags_nonpositive_latency(tmp_path: Path) -> None:
    path = tmp_path / "budgets.json"
    budgets = {workflow: _valid_budget(workflow) for workflow in PRODUCT_WORKFLOWS}
    budgets["today"] = {
        **budgets["today"],
        "beta_p95_seconds": 0,
    }
    path.write_text(json.dumps({"workflows": budgets}), encoding="utf-8")

    violations = validate_budget_registry(path)

    assert "today: beta_p95_seconds must be positive" in violations
