"""Synthetic 3-month corpus where ECE decreases monotonically. Assert negative slope."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from lsob_contracts import Corpus, CorpusMeta, EvaluationContext, GroundTruth

from lsob_evaluator_l3.evaluator import LayerThreeEvaluator
from lsob_evaluator_l3.mock_sut import MockCalibratedSUT


def _build_month_predictions(
    month: int, year: int, confidence: float, n_true: int, n_false: int
) -> list[dict]:
    resolve = datetime(year, month, 15, tzinfo=timezone.utc).isoformat()
    preds = []
    i = 0
    for _ in range(n_true):
        preds.append(
            {
                "prediction_id": f"{year}-{month:02d}-T{i}",
                "proposition": f"prop {i}",
                "asserted_confidence": confidence,
                "resolves_at": resolve,
                "outcome": "true",
                "proposition_kind": "synthetic",
                "actor_id": "alice",
            }
        )
        i += 1
    for _ in range(n_false):
        preds.append(
            {
                "prediction_id": f"{year}-{month:02d}-F{i}",
                "proposition": f"prop {i}",
                "asserted_confidence": confidence,
                "resolves_at": resolve,
                "outcome": "false",
                "proposition_kind": "synthetic",
                "actor_id": "alice",
            }
        )
        i += 1
    return preds


def _build_corpus() -> Corpus:
    # Month 1: all 10 at conf=0.9, acc=0.1 -> ECE = 0.8 (very bad)
    # Month 2: all 10 at conf=0.9, acc=0.5 -> ECE = 0.4 (better)
    # Month 3: all 10 at conf=0.9, acc=0.9 -> ECE = 0.0 (perfect)
    gts: list[GroundTruth] = []
    for month, (n_true, n_false) in enumerate([(1, 9), (5, 5), (9, 1)], start=1):
        preds = _build_month_predictions(month, 2026, 0.9, n_true, n_false)
        gts.append(
            GroundTruth(
                timestamp=datetime(2026, month, 28, tzinfo=timezone.utc),
                predictions_that_will_resolve=preds,
            )
        )
    meta = CorpusMeta(
        corpus_id="synthetic-trend",
        company_id="SyntheticCo",
        months_simulated=3,
        seed=0,
        config_hash="synth-trend-hash",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )
    return Corpus(meta=meta, signals=[], ground_truth=gts)


async def test_ece_slope_is_negative(tmp_path: Path):
    corpus = _build_corpus()
    all_preds = [p for gt in corpus.ground_truth for p in gt.predictions_that_will_resolve]
    sut = MockCalibratedSUT.from_predictions(all_preds, actor_id="alice")
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="trend",
        extras={"output_dir": str(tmp_path)},
    )
    results = await LayerThreeEvaluator().evaluate(ctx)

    monthly = [r for r in results if r.metric_name == "ece_monthly"]
    monthly.sort(key=lambda r: r.breakdown_by["month"])
    assert len(monthly) == 3
    # Monotonic decrease of ECE month over month.
    values = [m.value for m in monthly]
    assert values[0] > values[1] > values[2]
    # With n_bins=10 equal-frequency over 10 predictions at conf=0.9:
    #   Month 1: 9F/1T -> weighted |gap| = (9*0.9 + 1*0.1)/10 = 0.82
    #   Month 2: 5F/5T -> (5*0.9 + 5*0.1)/10 = 0.5
    #   Month 3: 1F/9T -> (1*0.9 + 9*0.1)/10 = 0.18
    assert values[0] == pytest.approx(0.82, abs=1e-9)
    assert values[1] == pytest.approx(0.50, abs=1e-9)
    assert values[2] == pytest.approx(0.18, abs=1e-9)

    slope = next(r for r in results if r.metric_name == "ece_trend_slope")
    r2 = next(r for r in results if r.metric_name == "ece_trend_r2")
    assert slope.value < 0.0
    # Monthly values are (0.82, 0.50, 0.18) - exactly linear slope -0.32, so R^2 == 1.
    assert r2.value == pytest.approx(1.0, abs=1e-9)
