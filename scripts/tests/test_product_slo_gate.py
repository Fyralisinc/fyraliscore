from __future__ import annotations

from scripts.check_product_slo_gate import (
    _extract_scalar_value,
    evaluate_slo_gate,
)


def test_extract_scalar_value_from_prometheus_vector() -> None:
    payload = {
        "status": "success",
        "data": {"result": [{"value": [1780000000.0, "1.25"]}]},
    }

    assert _extract_scalar_value(payload) == 1.25


def test_extract_scalar_value_returns_none_for_no_data() -> None:
    payload = {"status": "success", "data": {"result": []}}

    assert _extract_scalar_value(payload) is None


def test_evaluate_slo_gate_passes_when_under_thresholds() -> None:
    result = evaluate_slo_gate(
        {"error_budget_burn": 1.1, "latency_budget_burn": 1.9},
        error_burn_max=2.0,
        latency_burn_max=2.0,
    )

    assert result.ok is True
    assert result.findings == []


def test_evaluate_slo_gate_allows_no_data_for_new_deploys() -> None:
    result = evaluate_slo_gate(
        {"error_budget_burn": None, "latency_budget_burn": None},
        error_burn_max=2.0,
        latency_burn_max=2.0,
    )

    assert result.ok is True


def test_evaluate_slo_gate_fails_on_error_or_latency_burn() -> None:
    result = evaluate_slo_gate(
        {"error_budget_burn": 2.5, "latency_budget_burn": 3.0},
        error_burn_max=2.0,
        latency_burn_max=2.0,
    )

    assert result.ok is False
    assert result.findings == [
        "product error budget burn 2.5 exceeds 2",
        "product latency budget burn 3 exceeds 2",
    ]
