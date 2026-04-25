"""When the SUT lacks an ``emitted_anomalies`` surface, metrics 3 & 4 must
emit ``layer_not_applicable`` rows while metrics 1 & 2 still run.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import (
    AblationConfig,
    AtRiskReport,
    Belief,
    BeliefQuery,
    Corpus,
    CorpusMeta,
    DiffOp,
    EvaluationContext,
    GroundTruth,
    Signal,
    SUTConfig,
    Trigger,
)

from lsob_evaluator_l4.evaluator import LayerFourEvaluator

UTC = timezone.utc


class _MinimalSUT:
    """Implements SystemUnderTest but NOT AnomalyEmittingSUT."""

    name = "minimal"
    max_concurrent_ingestion = 1

    async def startup(self, config: SUTConfig) -> None:
        return None

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        return None

    async def ingest_signal(self, signal: Signal) -> None:
        return None

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        return []

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return AtRiskReport(timestamp=timestamp, items=[])

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return DiffOp(
            diff_id=f"diff-{trigger.trigger_id}",
            produced_at=trigger.timestamp,
            trigger_id=trigger.trigger_id,
        )

    async def shutdown(self) -> None:
        return None


@pytest.mark.asyncio
async def test_missing_anomaly_surface_emits_layer_not_applicable() -> None:
    checkpoint = datetime(2026, 1, 31, tzinfo=UTC)
    corpus = Corpus(
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
                    {"id": "C-green", "true_outcome": "will_succeed"},
                ],
            )
        ],
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_MinimalSUT(),
        ground_truth_checkpoint=checkpoint,
        run_id="na-run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}

    # Metrics 1 & 2 still run.
    assert "at_risk_commitment_precision" in results
    assert "at_risk_commitment_recall" in results
    assert "at_risk_commitment_f1" in results
    assert "customer_risk_precision" in results
    assert "customer_risk_recall" in results
    assert "customer_risk_f1" in results

    # Metrics 3 & 4 are flagged not-applicable.
    for name in ("anomaly_precision", "alert_fatigue_ratio"):
        assert name in results
        assert results[name].breakdown_by.get("layer_not_applicable") is True
