"""Sub-evaluator tests: prediction accuracy."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import GroundTruth

from lsob_evaluator_l2.evaluator import (
    classify_prediction_outcome,
    evaluate_predictions,
)
from lsob_evaluator_l2.mock_sut import MockBelief, MockBeliefSUT


WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc)


def _gt(*preds):
    return GroundTruth(
        timestamp=datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        actors=[],
        commitments=[],
        customers=[],
        patterns=[],
        predictions_that_will_resolve=list(preds),
    )


def _pred(pid: str, outcome: str, resolves: str = "2026-01-20T00:00:00Z"):
    return {
        "prediction_id": pid,
        "proposition": f"prop-{pid}",
        "asserted_confidence": 0.7,
        "resolves_at": resolves,
        "outcome": outcome,
    }


def test_classify_prediction_outcome_detects_false():
    from lsob_contracts import Belief

    b = [
        Belief(
            claim_id="x",
            proposition="prop -> false",
            proposition_kind="prediction",
            asserted_confidence=0.8,
            last_updated=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    ]
    assert classify_prediction_outcome(b) is False


def test_classify_prediction_outcome_detects_true():
    from lsob_contracts import Belief

    b = [
        Belief(
            claim_id="x",
            proposition="prop -> true",
            proposition_kind="prediction",
            asserted_confidence=0.8,
            last_updated=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    ]
    assert classify_prediction_outcome(b) is True


async def test_evaluate_predictions_perfect():
    gt = _gt(
        _pred("p1", "true"),
        _pred("p2", "false"),
    )
    sut = MockBeliefSUT(
        canned={
            ("model", "p1"): [
                MockBelief(
                    proposition="prop-p1 -> true",
                    proposition_kind="prediction",
                )
            ],
            ("model", "p2"): [
                MockBelief(
                    proposition="prop-p2 -> false",
                    proposition_kind="prediction",
                )
            ],
        }
    )
    results = await evaluate_predictions([gt], sut, WINDOW_START, WINDOW_END)
    by_name = {r.metric_name: r for r in results}
    assert by_name["accuracy"].value == 1.0
    assert by_name["false_positive_rate"].value == 0.0
    assert by_name["false_negative_rate"].value == 0.0


async def test_evaluate_predictions_one_false_positive():
    # Ground truth: p1 is false; SUT thinks true -> FP.
    gt = _gt(_pred("p1", "false"), _pred("p2", "true"))
    sut = MockBeliefSUT(
        canned={
            ("model", "p1"): [
                MockBelief(
                    proposition="prop-p1 -> true", proposition_kind="prediction"
                )
            ],
            ("model", "p2"): [
                MockBelief(
                    proposition="prop-p2 -> true", proposition_kind="prediction"
                )
            ],
        }
    )
    results = await evaluate_predictions([gt], sut, WINDOW_START, WINDOW_END)
    by_name = {r.metric_name: r for r in results}
    # 2 samples, 1 correct -> accuracy 0.5; 1 fp / (1 fp + 0 tn) = 1.0
    assert by_name["accuracy"].value == pytest.approx(0.5)
    assert by_name["false_positive_rate"].value == pytest.approx(1.0)
    assert by_name["false_negative_rate"].value == pytest.approx(0.0)


async def test_predictions_outside_window_skipped():
    gt = _gt(_pred("p1", "true", resolves="2030-01-01T00:00:00Z"))
    sut = MockBeliefSUT(canned={})
    results = await evaluate_predictions([gt], sut, WINDOW_START, WINDOW_END)
    # No in-window predictions -> all three metrics layer_not_applicable
    assert all(r.breakdown_by.get("layer_not_applicable") for r in results)


async def test_evaluate_predictions_unreachable_sut():
    gt = _gt(_pred("p1", "true"))
    sut = MockBeliefSUT(canned={}, fail_predicate=lambda q: True)
    results = await evaluate_predictions([gt], sut, WINDOW_START, WINDOW_END)
    assert all(r.breakdown_by.get("layer_not_applicable") for r in results)
