from types import SimpleNamespace
from uuid import uuid4

from lib.evaluation.epistemic_repair.p4_artifact import build_unrun_p4_artifact
from lib.evaluation.epistemic_repair.p4_population import build_p4_population
from lib.evaluation.epistemic_repair.p4_runner import _batch_decisions


def test_population_is_exactly_six_batches_of_twenty_and_two_episodes() -> None:
    population = build_p4_population()
    assert len(population.batches) == 6
    assert all(len(batch.signals) == 20 for batch in population.batches)
    assert all(len({item.episode_id for item in batch.signals}) == 2 for batch in population.batches)
    assert len({item.signal_id for batch in population.batches for item in batch.signals}) == 120


def test_population_registers_reuse_reopen_correction_and_stale_exclusion() -> None:
    events = [batch.required_event for batch in build_p4_population().batches]
    assert any("relation" in item and "reusing" in item for item in events)
    assert sum("historical" in item for item in events) == 1
    assert any("correct" in item for item in events)
    assert any("exclude_stale" in item for item in events)


def test_unrun_artifact_fails_closed_with_complete_metric_schema() -> None:
    artifact = build_unrun_p4_artifact()
    assert artifact["schema_version"] == "epistemic-repair-p4-online-learning-v1"
    assert artifact["population"]["signal_count"] == 120
    assert not artifact["phase_exit_ready"]
    assert artifact["missing_evidence"]
    assert set(artifact["hard_gates"]) == {"HG-10", "HG-11", "HG-12", "HG-13"}
    assert all(value is None for value in artifact["continuous_metrics"].values())
    assert len(artifact["artifact_content_digest"]) == 64


def test_credit_population_has_required_use_background_and_distractor_counts() -> None:
    tenant = uuid4()
    models = [
        (
            SimpleNamespace(
                model_id=uuid4(), version_id=uuid4(), version=1
            ),
            object(),
        )
        for _ in range(2)
    ]
    all_rows = []
    for ordinal in range(1, 7):
        all_rows.extend(
            _batch_decisions(
                tenant_id=tenant,
                batch_id=f"p4-batch-{ordinal}",
                ordinal=ordinal,
                models=models,
                relation_receipt=SimpleNamespace(relation_version_id=uuid4()),
            )
        )
    useful = [item for item in all_rows if item.referenced]
    background = [item for item in all_rows if item.necessary_background]
    distractors = [
        item
        for item in all_rows
        if not item.referenced and not item.necessary_background
    ]
    historical = [
        item for item in all_rows if item.context_item_kind == "historical_observation"
    ]
    assert len(useful) >= 20
    assert len(background) >= 10
    assert len(distractors) == 10
    assert len(historical) == 1
    assert historical[0].historical_reopen_reason is not None
    assert len({item.decision_id for item in all_rows}) == len(all_rows)
