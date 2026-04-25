"""Sub-evaluator tests: customer health correctness."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import GroundTruth

from lsob_evaluator_l2.evaluator import (
    classify_customer_health,
    evaluate_customer_health,
)
from lsob_evaluator_l2.mock_sut import MockBelief, MockBeliefSUT


def _gt(*customers):
    return GroundTruth(
        timestamp=datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        actors=[],
        commitments=[],
        customers=list(customers),
        patterns=[],
        predictions_that_will_resolve=[],
    )


def test_classify_customer_health_prefers_worse_rung():
    from lsob_contracts import Belief

    beliefs = [
        Belief(
            claim_id="a",
            proposition="trajectory: healthy -> critical",
            proposition_kind="customer_health",
            asserted_confidence=0.9,
            last_updated=datetime(2026, 1, 31, tzinfo=timezone.utc),
        )
    ]
    # Both "healthy" and "critical" appear; we prefer the later rung.
    assert classify_customer_health(beliefs) == "critical"


async def test_customer_health_perfect():
    gt = _gt(
        {"id": "acme", "true_health": "degraded"},
        {"id": "beta", "true_health": "healthy"},
    )
    sut = MockBeliefSUT(
        canned={
            ("customer", "acme"): [
                MockBelief(
                    proposition="health=degraded", proposition_kind="customer_health"
                )
            ],
            ("customer", "beta"): [
                MockBelief(
                    proposition="health=healthy", proposition_kind="customer_health"
                )
            ],
        }
    )
    results = await evaluate_customer_health([gt], sut)
    by = {r.metric_name: r for r in results}
    assert by["health_accuracy"].value == 1.0
    assert by["mean_ordinal_distance"].value == 0.0


async def test_customer_health_one_wrong_ordinal_distance():
    # Ground truth: acme=critical (index 3). SUT reports warning (index 1).
    # distance = 2.
    gt = _gt({"id": "acme", "true_health": "critical"})
    sut = MockBeliefSUT(
        canned={
            ("customer", "acme"): [
                MockBelief(
                    proposition="health=warning", proposition_kind="customer_health"
                )
            ]
        }
    )
    results = await evaluate_customer_health([gt], sut)
    by = {r.metric_name: r for r in results}
    assert by["health_accuracy"].value == 0.0
    assert by["mean_ordinal_distance"].value == pytest.approx(2.0)


async def test_customer_health_no_customers_marks_na():
    gt = _gt()
    sut = MockBeliefSUT(canned={})
    results = await evaluate_customer_health([gt], sut)
    assert all(r.breakdown_by.get("layer_not_applicable") for r in results)


async def test_customer_health_unknown_defaults_to_healthy():
    # SUT returns no belief for this customer; we default the guess to "healthy".
    gt = _gt({"id": "gamma", "true_health": "critical"})
    sut = MockBeliefSUT(canned={})
    results = await evaluate_customer_health([gt], sut)
    by = {r.metric_name: r for r in results}
    # Guess=healthy, truth=critical -> ordinal distance 3, accuracy 0.
    assert by["health_accuracy"].value == 0.0
    assert by["mean_ordinal_distance"].value == pytest.approx(3.0)
