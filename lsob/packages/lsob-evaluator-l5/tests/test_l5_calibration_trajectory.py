"""Calibration trajectory sub-evaluation: ECE should decrease → slope < 0."""

from __future__ import annotations

from datetime import datetime, timezone

from lsob_contracts import EvaluationContext

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT

from conftest import build_corpus


def _predictions(month_idx: int, n: int = 20) -> list[dict]:
    # Build predictions with target confidence = target accuracy (perfect
    # calibration). Then deliberately inflate miscalibration for earlier
    # months so ECE trajectory decreases over time.
    #
    # Strategy: flip `extra_wrong` of the "correct" predictions to incorrect
    # without touching confidence, which pushes observed accuracy below the
    # asserted 1.0 confidence band and inflates ECE.
    preds = []
    # how many high-confidence predictions to deliberately get wrong this month
    extra_wrong = max(0, 5 - month_idx)  # month0→5 wrong, month5→0 wrong
    for i in range(n):
        confidence = 0.95
        # first `n - extra_wrong` are correct; last `extra_wrong` are wrong
        correct = i < (n - extra_wrong)
        preds.append(
            {
                "prediction_id": f"p-{month_idx}-{i}",
                "proposition": "sample",
                "asserted_confidence": confidence,
                "outcome": "true" if correct else "false",
            }
        )
    return preds


async def test_calibration_trajectory_slope_is_negative() -> None:
    per_checkpoint_preds = [_predictions(m) for m in range(6)]
    corpus = build_corpus(
        months=6,
        predictions_per_checkpoint=per_checkpoint_preds,
    )
    sut = MockTemporalSUT()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="test-run",
    )
    ev = LayerFiveEvaluator()
    results = await ev._calibration_trajectory(ctx)
    # Find the slope metric.
    slope_res = next(r for r in results if r.metric_name == "calibration_trajectory_slope")
    assert slope_res.value < 0.0, f"expected negative slope, got {slope_res.value}"
    assert slope_res.breakdown_by.get("claim_met") is True


async def test_calibration_trajectory_handles_missing_predictions() -> None:
    corpus = build_corpus(months=3)
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockTemporalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="run",
    )
    ev = LayerFiveEvaluator()
    results = await ev._calibration_trajectory(ctx)
    assert any(r.metric_name == "calibration_trajectory_slope" for r in results)
    slope_res = next(r for r in results if r.metric_name == "calibration_trajectory_slope")
    # With no predictions per checkpoint, we fall back to layer_not_applicable
    # or insufficient_checkpoints depending on whether L3 is available.
    reason = slope_res.breakdown_by.get("reason")
    assert reason in {"layer_not_applicable", "insufficient_checkpoints"}
