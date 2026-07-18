from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from lib.evaluation.epistemic_repair.p5_population import (
    P5_EPISODE_IDS,
    P5_SIGNAL_COUNT,
    P5_SIGNALS_PER_BATCH,
    P5Signal,
    build_p5_population,
)


def test_population_is_exactly_three_interleaved_batches_of_twenty_five() -> None:
    population = build_p5_population()

    assert len(population.batches) == 3
    assert len(population.signals) == P5_SIGNAL_COUNT == 75
    for batch in population.batches:
        assert len(batch.signals) == P5_SIGNALS_PER_BATCH == 25
        assert [item.position for item in batch.signals] == list(range(1, 26))
        assert tuple(dict.fromkeys(item.episode_id for item in batch.signals)) == (
            P5_EPISODE_IDS
        )
        assert set(item.episode_id for item in batch.signals) == set(P5_EPISODE_IDS)


def test_runtime_signal_contract_contains_no_oracle_action_or_canonical_ref() -> None:
    signal_fields = {item.name for item in fields(P5Signal)}
    assert "oracle_action" not in signal_fields
    assert "canonical_ref" not in signal_fields

    population = build_p5_population()
    prohibited = ("benchmark", "memory instruction", "create a model", "retrieve memory")
    assert all(
        all(term not in signal.text.casefold() for term in prohibited)
        for signal in population.signals
    )


def test_population_digest_and_vertical_oracle_are_stable() -> None:
    first = build_p5_population()
    second = build_p5_population()

    assert first.population_digest == second.population_digest
    assert {
        first.oracle.atomic_signal_id,
        first.oracle.reuse_relation_signal_id,
        first.oracle.correction_signal_id,
    } == {"p5-b1-s13", "p5-b2-s10", "p5-b3-s16"}
    assert first.oracle.expected_relation_kind == "dependency_constraint"


def test_model_version_loader_reconstructs_digest_v2_fields() -> None:
    runner = Path("services/evaluation/epistemic_repair/p5_runner.py").read_text()
    for field in (
        "confidence",
        "semantic_digest_version",
        "falsifier",
        "evidential_weight",
        "supporting_model_ids",
        "visible_to_subjects",
        "resolution_outcome",
        "resolved_at",
        "temporal_scope",
    ):
        assert f'{field}=' in runner
    for scope_field in (
        "canonical_ref",
        "display_label",
        "canonical_ref_status",
        "normalization_version",
    ):
        assert f'{scope_field}=binding["{scope_field}"]' in runner
