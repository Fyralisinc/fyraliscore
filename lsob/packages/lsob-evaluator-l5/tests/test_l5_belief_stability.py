"""Belief stability over long-stable ground-truth facts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lsob_contracts import EvaluationContext

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT, TemporalBeliefRecord
from lsob_evaluator_l5.stability import find_stable_windows

from conftest import build_corpus


def _six_month_stable_corpus():
    start = datetime(2024, 1, 15, tzinfo=timezone.utc)
    commitments = [
        {"id": "commit-1", "true_outcome": "will_succeed", "owner": "a"}
    ]
    corpus = build_corpus(
        months=6,
        start=start,
        commitments=commitments,
    )
    return corpus, start


def test_find_stable_windows_detects_six_month_run() -> None:
    corpus, _ = _six_month_stable_corpus()
    windows = find_stable_windows(corpus.ground_truth, window=6)
    assert len(windows) == 1
    assert windows[0].entity_id == "commit-1"
    assert windows[0].value == "will_succeed"
    assert len(windows[0].checkpoint_timestamps) == 6


async def test_belief_churn_is_zero_for_perfect_sut() -> None:
    corpus, start = _six_month_stable_corpus()
    # SUT claims "will_succeed" at every checkpoint.
    beliefs = {
        ("commitment", "commit-1"): [
            (
                start + timedelta(days=30 * i),
                TemporalBeliefRecord(
                    proposition="state=will_succeed",
                    proposition_kind="commitment_state",
                ),
            )
            for i in range(6)
        ]
    }
    sut = MockTemporalSUT(beliefs=beliefs)
    ctx = EvaluationContext(
        corpus=corpus, sut=sut, ground_truth_checkpoint=start, run_id="r"
    )
    results = await LayerFiveEvaluator()._belief_stability(ctx)
    churn = results[0]
    assert churn.metric_name == "belief_churn_per_month"
    assert churn.value == 0.0
    assert churn.breakdown_by["excessive"] is False


async def test_belief_churn_flags_flapping_sut() -> None:
    corpus, start = _six_month_stable_corpus()
    beliefs_series = []
    for i in range(6):
        val = "will_succeed" if i % 2 == 0 else "will_slip"
        beliefs_series.append(
            (
                start + timedelta(days=30 * i),
                TemporalBeliefRecord(
                    proposition=f"state={val}",
                    proposition_kind="commitment_state",
                ),
            )
        )
    sut = MockTemporalSUT(beliefs={("commitment", "commit-1"): beliefs_series})
    ctx = EvaluationContext(
        corpus=corpus, sut=sut, ground_truth_checkpoint=start, run_id="r"
    )
    results = await LayerFiveEvaluator()._belief_stability(ctx)
    churn = results[0]
    # 5 transitions across 6 months → churn_per_month = 5/6 > 0.5 threshold.
    assert churn.value > 0.5
    assert churn.breakdown_by["excessive"] is True


async def test_belief_stability_no_stable_windows_degrades() -> None:
    # 3 months < 6 → no stable windows exist.
    corpus = build_corpus(
        months=3,
        commitments=[{"id": "c1", "true_outcome": "open"}],
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockTemporalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="r",
    )
    results = await LayerFiveEvaluator()._belief_stability(ctx)
    assert results[0].value == 0.0
    assert "no stable" in results[0].breakdown_by["reason"]
