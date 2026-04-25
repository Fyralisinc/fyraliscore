"""Integration: LayerTwoEvaluator against fixtures/mini_corpus_a.json."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l2.evaluator import LAYER_ID, LayerTwoEvaluator
from lsob_evaluator_l2.mock_sut import MockBeliefSUT, mock_from_ground_truth

FIXTURE_PATH = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "mini_corpus_a.json"
)


def _load_corpus() -> Corpus:
    raw = json.loads(FIXTURE_PATH.read_text())
    return Corpus.model_validate(raw)


async def test_fixture_present():
    assert FIXTURE_PATH.exists(), f"missing fixture {FIXTURE_PATH}"


async def test_perfect_mock_gives_expected_metrics():
    corpus = _load_corpus()
    sut = mock_from_ground_truth(list(corpus.ground_truth))
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="l2-int-perfect",
    )
    results = await LayerTwoEvaluator().evaluate(ctx)
    by = {r.metric_name: r for r in results}

    assert all(r.layer_id == LAYER_ID for r in results)
    assert all(r.run_id == "l2-int-perfect" for r in results)

    # Fixture has 2 commitments; perfect mock → 1.0.
    assert by["state_accuracy"].value == 1.0
    assert by["state_accuracy"].breakdown_by["commitments_queried"] == 2

    # 1 customer, 1 ground-truth health, perfect mock → 1.0 / 0 distance.
    assert by["health_accuracy"].value == 1.0
    assert by["mean_ordinal_distance"].value == 0.0

    # 1 pattern detected → recall 1.0. Pattern eligible 2026-01-16, checkpoint
    # 2026-01-31 → ~15 days → ~0.5 months.
    assert by["detection_recall"].value == 1.0
    assert 0.4 < by["detection_latency_months"].value < 0.6
    assert by["false_pattern_rate"].value == 0.0

    # 1 prediction, outcome=false, resolves 2026-01-09 (inside window).
    assert by["accuracy"].value == 1.0
    assert by["false_positive_rate"].value == 0.0
    assert by["false_negative_rate"].value == 0.0


async def test_wrong_commitments_mock_drops_state_accuracy():
    corpus = _load_corpus()
    # Wrong-commitments mock always answers "will_succeed"; ground truth says
    # "slipped_but_completed" and "open", so 0/2 matches.
    sut = mock_from_ground_truth(
        list(corpus.ground_truth), perfect_commitments=False
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="l2-int-wrong-commit",
    )
    results = await LayerTwoEvaluator().evaluate(ctx)
    by = {r.metric_name: r for r in results}
    assert by["state_accuracy"].value == pytest.approx(0.0)
    # Health and predictions still correct since we only flipped commitments.
    assert by["health_accuracy"].value == 1.0
    assert by["accuracy"].value == 1.0


async def test_wrong_customer_health_drops_health_metrics():
    corpus = _load_corpus()
    sut = mock_from_ground_truth(
        list(corpus.ground_truth), perfect_customers=False
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="l2-int-wrong-health",
    )
    results = await LayerTwoEvaluator().evaluate(ctx)
    by = {r.metric_name: r for r in results}
    # True health in fixture = degraded (index 2); wrong mock says healthy (idx 0).
    assert by["health_accuracy"].value == 0.0
    assert by["mean_ordinal_distance"].value == pytest.approx(2.0)


async def test_empty_sut_degrades_to_not_applicable_commitments():
    # A SUT that raises on every query should produce layer_not_applicable
    # markers rather than crashing.
    corpus = _load_corpus()
    sut = MockBeliefSUT(canned={}, fail_predicate=lambda q: True)
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="l2-int-na",
    )
    results = await LayerTwoEvaluator().evaluate(ctx)
    na_names = {
        r.metric_name
        for r in results
        if r.breakdown_by.get("layer_not_applicable")
    }
    # All headline metrics should have NA-stamped rows.
    assert {
        "state_accuracy",
        "health_accuracy",
        "detection_recall",
    } <= na_names
