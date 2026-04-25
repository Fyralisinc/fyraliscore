"""End-to-end integration test for LayerSixEvaluator on the l6_mini fixture."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l6.evaluator import LayerSixEvaluator
from lsob_evaluator_l6.mock_sut import MockDiffProducingSUT

_FIXTURES = Path(__file__).parent / "fixtures"


def _load_corpus() -> Corpus:
    return Corpus.model_validate_json((_FIXTURES / "l6_mini.json").read_text())


def _load_canned() -> dict:
    return json.loads((_FIXTURES / "l6_canned_sut_diffs.json").read_text())


@pytest.mark.asyncio
async def test_integration_phase_6a_only():
    corpus = _load_corpus()
    sut = MockDiffProducingSUT(canned=_load_canned())
    evaluator = LayerSixEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="l6-integ",
        extras={"enable_llm_judge": False},
    )
    results = await evaluator.evaluate(ctx)
    metric_names = {r.metric_name for r in results}
    assert "state_transition_accuracy" in metric_names
    assert "confidence_alignment_rate" in metric_names
    assert "falsifier_adequacy_rate" in metric_names
    assert "over_splitting_rate" in metric_names
    assert "under_splitting_rate" in metric_names
    assert "layer6b_skipped" in metric_names
    # No pairwise metrics when judge is off.
    assert "pairwise_win_rate" not in metric_names


@pytest.mark.asyncio
async def test_integration_phase_6b_with_mock_judge():
    corpus = _load_corpus()
    sut = MockDiffProducingSUT(canned=_load_canned())
    evaluator = LayerSixEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="l6-integ-judge",
        extras={"enable_llm_judge": True},
    )
    results = await evaluator.evaluate(ctx)
    by_name = {r.metric_name: r for r in results}
    assert "pairwise_win_rate" in by_name
    assert "pairwise_tie_rate" in by_name
    assert "pairwise_loss_rate" in by_name

    # Rates sum to 1.0 (±1e-9 for float noise).
    total = (
        by_name["pairwise_win_rate"].value
        + by_name["pairwise_tie_rate"].value
        + by_name["pairwise_loss_rate"].value
    )
    assert abs(total - 1.0) < 1e-9

    # Prompt hash is surfaced in breakdown_by.
    assert "prompt_hash" in by_name["pairwise_win_rate"].breakdown_by
    ph = by_name["pairwise_win_rate"].breakdown_by["prompt_hash"]
    assert isinstance(ph, str) and len(ph) == 64


@pytest.mark.asyncio
async def test_integration_structural_values_make_sense():
    """Under-split SUT on trig-001 should trip under_splitting_rate = 0.5."""
    corpus = _load_corpus()
    sut = MockDiffProducingSUT(canned=_load_canned())
    evaluator = LayerSixEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="l6-integ-values",
        extras={"enable_llm_judge": False},
    )
    results = await evaluator.evaluate(ctx)
    by_name = {r.metric_name: r for r in results}

    # Both SUT diffs hit the correct to_state -> accuracy is 1.0.
    assert by_name["state_transition_accuracy"].value == 1.0

    # trig-001 SUT claim (0.75) is within 0.15 of ref (0.82); trig-002 SUT
    # claim (0.88) is within 0.15 of ref (0.90). So alignment = 1.0.
    assert by_name["confidence_alignment_rate"].value == 1.0

    # trig-001 had 2 ref claims, SUT produced 1 -> under-split. trig-002 had
    # 1 ref claim -> no under-split. Rate = 0.5.
    assert by_name["under_splitting_rate"].value == 0.5

    # Neither SUT diff exceeds 5 claim_ops, so over-split rate = 0.0.
    assert by_name["over_splitting_rate"].value == 0.0


@pytest.mark.asyncio
async def test_integration_no_reference_diffs():
    """Corpus without reference_diffs gets the `layer6_no_reference` result."""
    corpus = _load_corpus()
    # Strip reference_diffs to simulate an older fixture.
    for gt in corpus.ground_truth:
        gt.reference_diffs = []
    sut = MockDiffProducingSUT()
    evaluator = LayerSixEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="l6-no-ref",
        extras={"enable_llm_judge": True},
    )
    results = await evaluator.evaluate(ctx)
    assert len(results) == 1
    assert results[0].metric_name == "layer6_no_reference"
