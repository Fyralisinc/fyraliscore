"""Sub-evaluator tests: commitment state correctness."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import GroundTruth

from lsob_evaluator_l2.evaluator import (
    classify_commitment_state,
    evaluate_commitments,
)
from lsob_evaluator_l2.mock_sut import MockBelief, MockBeliefSUT


def _gt(*commitments):
    return GroundTruth(
        timestamp=datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        actors=[],
        commitments=list(commitments),
        customers=[],
        patterns=[],
        predictions_that_will_resolve=[],
    )


def _belief(prop: str, kind: str = "commitment_state"):
    return MockBelief(proposition=prop, proposition_kind=kind)


def test_classify_commitment_state_picks_most_specific():
    from lsob_contracts import Belief

    b = [
        Belief(
            claim_id="x",
            proposition="state=slipped_but_completed",
            proposition_kind="commitment_state",
            asserted_confidence=0.9,
            last_updated=datetime(2026, 1, 31, tzinfo=timezone.utc),
        )
    ]
    # "slipped_but_completed" comes first in our ordered terms and wins.
    assert classify_commitment_state(b) == "slipped_but_completed"


def test_classify_returns_none_on_empty():
    assert classify_commitment_state([]) is None


async def test_evaluate_commitments_all_correct():
    gt = _gt(
        {"id": "C1", "owner": "a", "true_outcome": "will_slip"},
        {"id": "C2", "owner": "a", "true_outcome": "succeeded"},
    )
    sut = MockBeliefSUT(
        canned={
            ("commitment", "C1"): [_belief("state=will_slip")],
            ("commitment", "C2"): [_belief("state=succeeded")],
        }
    )
    [res] = await evaluate_commitments([gt], sut)
    assert res.metric_name == "state_accuracy"
    assert res.value == 1.0
    assert res.breakdown_by["commitments_queried"] == 2


async def test_evaluate_commitments_half_wrong():
    gt = _gt(
        {"id": "C1", "owner": "a", "true_outcome": "will_slip"},
        {"id": "C2", "owner": "a", "true_outcome": "succeeded"},
    )
    sut = MockBeliefSUT(
        canned={
            ("commitment", "C1"): [_belief("state=will_slip")],
            ("commitment", "C2"): [_belief("state=will_succeed")],
        }
    )
    [res] = await evaluate_commitments([gt], sut)
    assert res.value == pytest.approx(0.5)


async def test_evaluate_commitments_sut_raises_marks_na():
    gt = _gt({"id": "C1", "owner": "a", "true_outcome": "open"})
    sut = MockBeliefSUT(
        canned={}, fail_predicate=lambda q: True
    )
    [res] = await evaluate_commitments([gt], sut)
    assert res.breakdown_by.get("layer_not_applicable") is True


async def test_evaluate_commitments_no_entities_marks_na():
    gt = _gt()  # zero commitments
    sut = MockBeliefSUT(canned={})
    [res] = await evaluate_commitments([gt], sut)
    assert res.breakdown_by.get("layer_not_applicable") is True
