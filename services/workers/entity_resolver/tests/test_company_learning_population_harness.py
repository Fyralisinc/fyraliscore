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
    assert len(evidence.raw_pairs) == 4
    assert len(evidence.observations) == 4
    assert all(
        observation.execution_status == "observed"
        for observation in evidence.observations
    )
    assert {
        assignment.logical_entity_type: assignment.runtime_entity_type
        for assignment in evidence.assignments
    } == {
        "customer": "customer",
        "project": "resource",
        "system": "resource",
        "team": "actor",
    }
    assignment_by_case = {
        assignment.case_id: assignment for assignment in evidence.assignments
    }
    assert {
        pair.case_id: pair.adaptive.resolved_entity_ref.type
        for pair in evidence.raw_pairs
    } == {
        case_id: assignment.runtime_entity_type
        for case_id, assignment in assignment_by_case.items()
    }
    report = evidence.population_report
    assert report.pair_count == 4
    assert report.observed_pair_count == 4
    assert report.unsupported_case_count == 0
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
    assert report.adaptive_correctness.sample_size == 4
    assert report.observed_strata_counts == report.strata_counts
    assert report.unsupported_strata_counts["entity_type"] == {}
    assert evidence.experiment_report.status == "observed"
    assert evidence.experiment_report.incidents == ()

    async with resolver_db.acquire() as conn:
        for assignment in evidence.assignments:
            target_id = assignment.adaptive_target_id
            if assignment.runtime_entity_type == "actor":
                row = await conn.fetchrow(
                    """
                    SELECT type, metadata
                    FROM actors
                    WHERE tenant_id=$1 AND id=$2
                    """,
                    assignment.adaptive_tenant_id,
                    target_id,
                )
                assert row is not None
                assert row["type"] == "group"
            else:
                row = await conn.fetchrow(
                    """
                    SELECT kind, metadata
                    FROM resources
                    WHERE tenant_id=$1 AND id=$2
                    """,
                    assignment.adaptive_tenant_id,
                    target_id,
                )
                assert row is not None
            assert row["metadata"]["logical_entity_type"] == (
                assignment.logical_entity_type
            )

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["evidence_digest"] == evidence.digest
    assert len(payload["raw_pairs"]) == 4
    assert len(payload["observations"]) == 4
