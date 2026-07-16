from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.company_learning_variant_population import (
    validate_variant_population_evidence_artifact,
)
from scripts.run_company_learning_variant_population_harness import (
    ARTIFACT_NAME,
    run_variant_population_experiment,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_variant_population_proves_candidate_memory_lift(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_variant_population_experiment(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="pytest-variant-alias-population",
        system_version="pytest",
        bootstrap_samples=200,
    )

    assert evidence.execution_mode == "full"
    assert len(evidence.selected_case_ids) == 24
    assert len(evidence.assignments) == 24
    assert len(evidence.raw_pairs) == 24
    assert len(evidence.mechanism_pairs) == 24
    assert evidence.population_report is not None
    assert evidence.population_report.observed_pair_count == 24
    assert evidence.population_report.unsupported_case_count == 0
    assert evidence.population_report.adaptive_correctness.point_estimate == 1.0
    assert evidence.population_report.frozen_correctness.point_estimate == 0.0
    assert (
        evidence.population_report.adaptive_minus_frozen_correctness.point_estimate
        == 1.0
    )
    assert evidence.experiment_report.incidents == ()

    metrics = evidence.mechanism_metrics
    assert metrics.candidate_memory_mediated_success_rate == 1.0
    assert metrics.adaptive_target_candidate_authorization_rate == 1.0
    assert metrics.frozen_target_candidate_exposure_rate == 0.0
    assert metrics.adaptive_closed_set_match_rate == 1.0
    assert metrics.frozen_closed_set_match_rate == 0.0
    assert metrics.both_arms_one_llm_call_rate == 1.0
    assert metrics.frozen_safe_review_or_abstention_rate == 1.0
    assert metrics.source_immutability_rate == 1.0
    assert metrics.hard_safety_incident_count == 0
    assert metrics.control_integrity_violation_count == 0

    persisted = validate_variant_population_evidence_artifact(
        json.loads((tmp_path / ARTIFACT_NAME).read_text(encoding="utf-8"))
    )
    assert persisted == evidence
