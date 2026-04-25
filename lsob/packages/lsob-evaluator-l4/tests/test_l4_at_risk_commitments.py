"""Layer 4 sub-evaluation 1 — at-risk commitment P/R/F1."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import (
    Corpus,
    CorpusMeta,
    EvaluationContext,
    GroundTruth,
)

from lsob_evaluator_l4.evaluator import LayerFourEvaluator
from lsob_evaluator_l4.mock_sut import MockSurfacingSUT, make_commitment_at_risk

UTC = timezone.utc


def _corpus_with_two_slips() -> Corpus:
    checkpoint = datetime(2026, 1, 31, tzinfo=UTC)
    return Corpus(
        meta=CorpusMeta(
            corpus_id="t",
            company_id="t",
            months_simulated=1,
            seed=1,
            config_hash="h",
            start_date=datetime(2026, 1, 1, tzinfo=UTC),
            end_date=datetime(2026, 1, 31, tzinfo=UTC),
        ),
        signals=[],
        ground_truth=[
            GroundTruth(
                timestamp=checkpoint,
                commitments=[
                    {
                        "id": "C1",
                        "true_outcome": "will_slip",
                        "resolution_timestamp": "2026-02-05T00:00:00Z",
                    },
                    {
                        "id": "C2",
                        "true_outcome": "slipped_but_completed",
                        "resolution_timestamp": "2026-02-10T00:00:00Z",
                    },
                    {
                        "id": "C3",
                        "true_outcome": "will_succeed",
                        "resolution_timestamp": "2026-02-01T00:00:00Z",
                    },
                ],
            )
        ],
    )


@pytest.mark.asyncio
async def test_perfect_surfacing_gets_f1_one() -> None:
    corpus = _corpus_with_two_slips()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_commitment_at_risk("C1"),
                make_commitment_at_risk("C2"),
            ]
        },
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["at_risk_commitment_precision"].value == pytest.approx(1.0)
    assert results["at_risk_commitment_recall"].value == pytest.approx(1.0)
    assert results["at_risk_commitment_f1"].value == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_missed_and_spurious_surfacing() -> None:
    corpus = _corpus_with_two_slips()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_commitment_at_risk("C1"),  # TP
                make_commitment_at_risk("C3"),  # FP (will_succeed)
                make_commitment_at_risk("C-ghost"),  # FP
                # C2 missed → FN
            ]
        },
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    # tp=1, fp=2, fn=1 → P=1/3, R=1/2, F1=2*(1/3)(1/2)/(1/3+1/2)=0.4
    assert results["at_risk_commitment_precision"].value == pytest.approx(1 / 3)
    assert results["at_risk_commitment_recall"].value == pytest.approx(0.5)
    assert results["at_risk_commitment_f1"].value == pytest.approx(0.4)
    breakdown = results["at_risk_commitment_precision"].breakdown_by
    assert breakdown["tp"] == 1
    assert breakdown["fp"] == 2
    assert breakdown["fn"] == 1
    # by_month is present and shares month key.
    assert "2026-01" in breakdown["by_month"]


@pytest.mark.asyncio
async def test_empty_positives_and_empty_predictions() -> None:
    """No slipping commitments and no predictions → clean zero, not crash."""
    checkpoint = datetime(2026, 2, 28, tzinfo=UTC)
    corpus = Corpus(
        meta=CorpusMeta(
            corpus_id="t",
            company_id="t",
            months_simulated=1,
            seed=1,
            config_hash="h",
            start_date=datetime(2026, 2, 1, tzinfo=UTC),
            end_date=datetime(2026, 2, 28, tzinfo=UTC),
        ),
        signals=[],
        ground_truth=[
            GroundTruth(
                timestamp=checkpoint,
                commitments=[
                    {"id": "C-green", "true_outcome": "will_succeed"},
                ],
            )
        ],
    )
    sut = MockSurfacingSUT(canned_at_risk={checkpoint: []})
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["at_risk_commitment_precision"].value == 0.0
    assert results["at_risk_commitment_recall"].value == 0.0
    assert results["at_risk_commitment_f1"].value == 0.0
