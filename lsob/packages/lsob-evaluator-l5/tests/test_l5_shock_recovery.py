"""Shock-recovery sub-evaluation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lsob_contracts import EvaluationContext, TurbulenceEvent, TurbulenceKind

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT, TemporalBeliefRecord

from conftest import build_corpus


async def test_shock_recovery_emits_na_when_no_shocks() -> None:
    corpus = build_corpus(
        months=3,
        commitments=[{"id": "c1", "true_outcome": "will_succeed"}],
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockTemporalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="r",
    )
    results = await LayerFiveEvaluator()._shock_recovery(ctx)
    assert len(results) == 1
    res = results[0]
    assert res.metric_name == "recovery_na"
    assert res.value == 0.0
    assert res.breakdown_by == {"reason": "no shocks in corpus"}


async def test_shock_recovery_measures_months_until_baseline() -> None:
    start = datetime(2024, 1, 15, tzinfo=timezone.utc)
    commitments = [
        {"id": "c1", "true_outcome": "will_succeed"},
    ]
    corpus = build_corpus(months=6, start=start, commitments=commitments)
    # Shock scheduled between month 2 and month 3 (index 2 is pre, 3 is post).
    shock_ts = start + timedelta(days=30 * 3) - timedelta(hours=1)
    event = TurbulenceEvent(
        event_id="shock-1",
        kind=TurbulenceKind.reorg,
        scheduled_at=shock_ts,
    )
    object.__setattr__(corpus, "turbulence_events", [event])

    # Build beliefs:
    #   months 0-2 (pre-shock): correct → baseline accuracy 1.0
    #   month  3   (post shock tick #1): wrong → 0.0
    #   month  4   (post #2): wrong → 0.0
    #   month  5   (post #3): correct again → 1.0 recovers
    beliefs = []
    for i in range(6):
        ts = start + timedelta(days=30 * i)
        if i in (3, 4):
            beliefs.append(
                (ts, TemporalBeliefRecord(proposition="state=will_slip",
                                          proposition_kind="commitment_state"))
            )
        else:
            beliefs.append(
                (ts, TemporalBeliefRecord(proposition="state=will_succeed",
                                          proposition_kind="commitment_state"))
            )
    sut = MockTemporalSUT(beliefs={("commitment", "c1"): beliefs})
    ctx = EvaluationContext(
        corpus=corpus, sut=sut, ground_truth_checkpoint=start, run_id="r"
    )
    results = await LayerFiveEvaluator()._shock_recovery(ctx)
    assert results
    res = results[0]
    assert res.metric_name == "shock_recovery_months"
    # Baseline=1.0. Post-shock: month 3 (wrong, offset 1), month 4 (wrong, 2),
    # month 5 (correct, offset 3) → 3 months to recover.
    assert res.value == 3.0
    per_shock = res.breakdown_by["per_shock"]
    assert per_shock[0]["event_id"] == "shock-1"
    assert per_shock[0]["months_to_recover"] == 3.0
