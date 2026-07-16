from __future__ import annotations

import json
from pathlib import Path

import pytest

from lib.evaluation.company_learning_population import (
    HeldOutExactAliasPopulation,
    HeldOutPairObservation,
    build_exact_alias_heldout_population,
    evaluate_heldout_population,
)


FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_exact_alias_population_v1.jsonl"
)


def test_committed_population_is_deterministic_and_stratified() -> None:
    generated = build_exact_alias_heldout_population()
    fixture = HeldOutExactAliasPopulation(
        cases=tuple(
            json.loads(line)
            for line in FIXTURE.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    )

    assert len(generated.cases) == 60
    assert fixture == generated
    assert fixture.digest == generated.digest
    assert len({case.digest for case in fixture.cases}) == 60
    assert {case.entity_type for case in fixture.cases} == {
        "customer",
        "project",
        "system",
        "team",
    }
    assert {case.slack_context for case in fixture.cases} == {
        "cross_thread_recurrence",
        "private_channel",
        "public_channel",
    }


def test_population_report_has_continuous_intervals_and_complete_registry() -> (
    None
):
    population = build_exact_alias_heldout_population()
    observations = tuple(
        HeldOutPairObservation(
            case_id=case.case_id,
            adaptive_correct=True,
            frozen_correct=index % 5 == 0,
            adaptive_unsafe=False,
            frozen_unsafe=False,
            adaptive_llm_calls=0,
            frozen_llm_calls=1,
            adaptive_latency_ms=20.0 + index,
            frozen_latency_ms=40.0 + index,
        )
        for index, case in enumerate(population.cases)
    )

    first = evaluate_heldout_population(
        population=population,
        observations=observations,
        bootstrap_samples=500,
    )
    second = evaluate_heldout_population(
        population=population,
        observations=observations,
        bootstrap_samples=500,
    )

    assert first == second
    assert first.pair_count == 60
    assert first.complete_population is True
    assert first.adaptive_correctness.point_estimate == 1.0
    assert first.adaptive_correctness.lower_95 < 1.0
    assert first.frozen_correctness.point_estimate == 0.2
    assert first.adaptive_minus_frozen_correctness.point_estimate == 0.8
    assert first.adaptive_minus_frozen_correctness.lower_95 < 0.8
    assert first.mean_llm_calls_avoided.point_estimate == 1.0
    assert (
        first.adaptive_minus_frozen_latency_ms.point_estimate
        == -20.0
    )
    assert first.strata_counts["entity_type"] == {
        "customer": 15,
        "project": 15,
        "system": 15,
        "team": 15,
    }


def test_population_rejects_selective_reruns_and_duplicates() -> None:
    population = build_exact_alias_heldout_population()
    observations = tuple(
        HeldOutPairObservation(
            case_id=case.case_id,
            adaptive_correct=True,
            frozen_correct=False,
            adaptive_unsafe=False,
            frozen_unsafe=False,
            adaptive_llm_calls=0,
            frozen_llm_calls=1,
            adaptive_latency_ms=20.0,
            frozen_latency_ms=40.0,
        )
        for case in population.cases
    )

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_heldout_population(
            population=population,
            observations=observations[:-1],
        )
    with pytest.raises(ValueError, match="unique by case"):
        evaluate_heldout_population(
            population=population,
            observations=(*observations[:-1], observations[0]),
        )
