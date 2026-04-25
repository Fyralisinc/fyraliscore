"""Layer 4 sub-evaluation 2 — customer risk P/R/F1."""

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
from lsob_evaluator_l4.mock_sut import MockSurfacingSUT, make_customer_at_risk

UTC = timezone.utc


def _corpus() -> Corpus:
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
                customers=[
                    {
                        "id": "acme",
                        "true_health": "degraded",
                        "trajectory": ["healthy", "warning", "degraded"],
                    },
                    {
                        "id": "bigco",
                        "true_health": "critical",
                        "trajectory": ["warning", "degraded", "critical"],
                    },
                    {
                        "id": "happy",
                        "true_health": "healthy",
                        "trajectory": ["healthy", "healthy"],
                    },
                ],
            )
        ],
    )


@pytest.mark.asyncio
async def test_perfect_customer_surfacing() -> None:
    corpus = _corpus()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_customer_at_risk("acme"),
                make_customer_at_risk("bigco"),
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
    assert results["customer_risk_precision"].value == pytest.approx(1.0)
    assert results["customer_risk_recall"].value == pytest.approx(1.0)
    assert results["customer_risk_f1"].value == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_partial_customer_surfacing() -> None:
    corpus = _corpus()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_customer_at_risk("acme"),  # TP
                make_customer_at_risk("happy"),  # FP (healthy)
                # bigco missed → FN
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
    # tp=1, fp=1, fn=1 → P=0.5, R=0.5, F1=0.5
    assert results["customer_risk_precision"].value == pytest.approx(0.5)
    assert results["customer_risk_recall"].value == pytest.approx(0.5)
    assert results["customer_risk_f1"].value == pytest.approx(0.5)
