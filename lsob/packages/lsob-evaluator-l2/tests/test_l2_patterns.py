"""Sub-evaluator tests: pattern recall + precision."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import GroundTruth

from lsob_evaluator_l2.evaluator import evaluate_patterns
from lsob_evaluator_l2.mock_sut import MockBelief, MockBeliefSUT


def _gt(*patterns, checkpoint: datetime | None = None):
    return GroundTruth(
        timestamp=checkpoint
        or datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        actors=[],
        commitments=[],
        customers=[],
        patterns=list(patterns),
        predictions_that_will_resolve=[],
    )


async def test_detection_recall_all_matched():
    gt = _gt(
        {
            "id": "P-alice",
            "description": "alice optimism",
            "detection_eligible_after": "2026-01-16T00:00:00Z",
        }
    )
    sut = MockBeliefSUT(
        canned={
            ("pattern", "P-alice"): [
                MockBelief(
                    proposition="alice optimism detected",
                    proposition_kind="pattern",
                    entities=["P-alice"],
                )
            ],
        }
    )
    results = await evaluate_patterns([gt], sut)
    by = {r.metric_name: r for r in results}
    assert by["detection_recall"].value == 1.0
    # Eligible on 2026-01-16, checkpoint 2026-01-31 -> ~15 days -> ~0.5 months.
    assert 0.4 <= by["detection_latency_months"].value <= 0.6
    assert by["false_pattern_rate"].value == 0.0


async def test_detection_recall_zero_when_no_beliefs():
    gt = _gt(
        {
            "id": "P-missing",
            "description": "",
            "detection_eligible_after": "2026-01-01T00:00:00Z",
        }
    )
    sut = MockBeliefSUT(canned={})
    results = await evaluate_patterns([gt], sut)
    by = {r.metric_name: r for r in results}
    assert by["detection_recall"].value == 0.0
    assert by["detection_latency_months"].value == 0.0
    assert by["false_pattern_rate"].value == 0.0


async def test_false_pattern_rate_hand():
    # One genuine pattern, SUT answers with two distinct entities — one the
    # real id, one a phony id. That adds a spurious claim.
    gt = _gt({"id": "P-real", "description": "x"})
    sut = MockBeliefSUT(
        canned={
            ("pattern", "P-real"): [
                MockBelief(
                    proposition="x",
                    proposition_kind="pattern",
                    entities=["P-real", "P-ghost"],
                )
            ]
        }
    )
    results = await evaluate_patterns([gt], sut)
    by = {r.metric_name: r for r in results}
    # matched=1, spurious=1 -> rate=0.5
    assert by["false_pattern_rate"].value == pytest.approx(0.5)


async def test_pattern_match_by_proposition_text():
    gt = _gt({"id": "P-latent", "description": "alice bias"})
    sut = MockBeliefSUT(
        canned={
            ("pattern", "P-latent"): [
                MockBelief(
                    proposition="A latent model about P-latent trends",
                    proposition_kind="pattern",
                    entities=["P-latent"],
                )
            ]
        }
    )
    results = await evaluate_patterns([gt], sut)
    by = {r.metric_name: r for r in results}
    assert by["detection_recall"].value == 1.0
