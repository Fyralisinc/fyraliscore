"""Pure unit tests for apply_delta / compute_utilization.

No pytestmark — these are synchronous functions and must not be marked
asyncio.
"""
from __future__ import annotations

from services.resources.transactions import apply_delta, compute_utilization


def test_apply_delta_financial_acquire():
    out = apply_delta({"amount_cents": 100}, {"amount_cents": 50}, "financial", "acquire")
    assert out["amount_cents"] == 150


def test_apply_delta_financial_spend():
    out = apply_delta({"amount_cents": 100}, {"amount_cents": 30}, "financial", "spend")
    assert out["amount_cents"] == 70


def test_apply_delta_capacity_deploy_and_release():
    cv = {"total_units": 10, "deployed_units": 0, "available_units": 10}
    deployed = apply_delta(cv, {"deployed_units": 3}, "capacity", "deploy")
    assert deployed["deployed_units"] == 3
    assert deployed["available_units"] == 7
    released = apply_delta(deployed, {"deployed_units": 2}, "capacity", "release")
    assert released["deployed_units"] == 1
    assert released["available_units"] == 9


def test_apply_delta_relational_strength_delta():
    cv = {"strength": "moderate", "arr_cents": 10_000}
    weaker = apply_delta(cv, {"strength_delta": -1}, "relational", "weaken")
    assert weaker["strength"] == "weakening"
    floored = apply_delta({"strength": "at_risk"}, {"strength_delta": -5}, "relational", "weaken")
    assert floored["strength"] == "at_risk"
    cap = apply_delta({"strength": "strong"}, {"strength_delta": 5}, "relational", "strengthen")
    assert cap["strength"] == "strong"


def test_apply_delta_relational_arr_delta():
    cv = {"strength": "strong", "arr_cents": 10_000_00}
    out = apply_delta(cv, {"arr_delta_cents": 5_000_00}, "relational", "strengthen")
    assert out["arr_cents"] == 15_000_00


def test_apply_delta_relational_strengthen_no_delta():
    """Implicit +1 on strengthen without explicit strength_delta."""
    out = apply_delta({"strength": "weakening"}, {}, "relational", "strengthen")
    assert out["strength"] == "moderate"


def test_apply_delta_relational_weaken_no_delta():
    out = apply_delta({"strength": "moderate"}, {}, "relational", "weaken")
    assert out["strength"] == "weakening"


def test_apply_delta_ip_expire_sets_flag():
    out = apply_delta({"registration_id": "US1"}, {}, "ip", "expire")
    assert out["expired"] is True


def test_compute_utilization_capacity():
    assert compute_utilization({"total_units": 10, "deployed_units": 10}, "capacity", "available") == "depleted"
    assert compute_utilization({"total_units": 10, "deployed_units": 5}, "capacity", "available") == "deployed"
    assert compute_utilization({"total_units": 10, "deployed_units": 0}, "capacity", "deployed") == "available"


def test_compute_utilization_non_capacity_returns_existing():
    assert compute_utilization({"amount_cents": 100}, "financial", "available") == "available"
    assert compute_utilization({"amount_cents": 100}, "financial", "committed") == "committed"
