from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from scripts.run_company_learning_population_harness import (
    ARTIFACT_NAME,
    run_population_experiment,
)


pytestmark = pytest.mark.integration


async def test_population_runner_smoke_accounts_for_all_entity_types(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_population_experiment(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="pytest-heldout-population-smoke",
        system_version="pytest-system",
        case_limit=4,
        bootstrap_samples=200,
    )

    assert evidence.execution_mode == "smoke"
    assert len(evidence.execution_population.cases) == 4
    assert len(evidence.assignments) == 4
    assert len(
        {
            tenant_id
            for assignment in evidence.assignments
            for tenant_id in (
                assignment.adaptive_tenant_id,
                assignment.frozen_tenant_id,
            )
        }
    ) == 8
    assert {case.entity_type for case in evidence.execution_population.cases} == {
        "customer",
        "project",
        "system",
        "team",
    }
    assert len(evidence.raw_pairs) == 1
    assert evidence.raw_pairs[0].case_id == "heldout-exact-000"
    assert len(evidence.observations) == 4
    report = evidence.population_report
    assert report.pair_count == 4
    assert report.observed_pair_count == 1
    assert report.unsupported_case_count == 3
    assert report.complete_population is True
    assert report.adaptive_correctness.point_estimate == 1.0
    assert report.frozen_correctness.point_estimate == 0.0
    assert (
        report.adaptive_minus_frozen_correctness.point_estimate
        == 1.0
    )
    assert report.adaptive_unsafe_rate.point_estimate == 0.0
    assert report.frozen_unsafe_rate.point_estimate == 0.0
    assert report.mean_llm_calls_avoided.point_estimate == 1.0
    assert report.adaptive_correctness.sample_size == 1
    assert report.unsupported_strata_counts["entity_type"] == {
        "project": 1,
        "system": 1,
        "team": 1,
    }
    assert evidence.experiment_report.status == "observed"
    assert evidence.experiment_report.incidents == ()

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["evidence_digest"] == evidence.digest
    assert len(payload["raw_pairs"]) == 1
    assert len(payload["observations"]) == 4
