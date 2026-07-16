from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.company_learning_variant_collisions import (
    VariantCollisionFamily,
)
from scripts.run_company_learning_variant_collisions_db import (
    ARTIFACT_NAME,
    CompanyLearningVariantCollisionEvidence,
    run_variant_collision_experiment,
)


pytestmark = pytest.mark.integration


async def test_variant_collisions_safely_contain_supported_runtime_cases(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_variant_collision_experiment(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="pytest-company-learning-variant-collisions",
        system_version="pytest-system",
    )

    assert len(evidence.registry_population.cases) == 16
    assert len(evidence.assignments) == 16
    assert len(evidence.observations) == 16
    observed = tuple(
        row
        for row in evidence.observations
        if row.execution_status == "observed"
    )
    unsupported = tuple(
        row
        for row in evidence.observations
        if row.execution_status == "unsupported"
    )
    assert len(observed) == 14
    assert len(unsupported) == 2
    cases = {
        case.case_id: case
        for case in evidence.registry_population.cases
    }
    assert {
        cases[row.case_id].collision_family for row in unsupported
    } == {
        VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
    }
    assert {
        row.unsupported_reason for row in unsupported
    } == {
        "runtime lacks authenticated SourceIdentityBinding evidence"
    }

    report = evidence.report
    assert report.pair_count == 16
    assert report.observed_pair_count == 14
    assert report.unsupported_case_count == 2
    assert report.runtime_support_rate.point_estimate == 14 / 16
    assert report.status == "observed_with_gaps"
    assert report.safety_incident_count == 0
    assert report.adaptive_safe_containment_rate.point_estimate == 1.0
    assert report.adaptive_unsafe_rate.point_estimate == 0.0
    assert report.adaptive_unsafe_resolution_rate.point_estimate == 0.0
    assert report.frozen_safe_containment_rate.point_estimate == 1.0
    assert report.frozen_unsafe_rate.point_estimate == 0.0
    assert report.frozen_unsafe_resolution_rate.point_estimate == 0.0
    assert report.adaptive_candidate_visibility_rate.point_estimate == 1.0
    assert report.frozen_candidate_visibility_rate.point_estimate == 1.0
    assert report.adaptive_wrong_model_count == 0
    assert report.frozen_wrong_model_count == 0
    assert report.adaptive_wrong_model_rate.point_estimate == 0.0
    assert report.frozen_wrong_model_rate.point_estimate == 0.0
    assert report.adaptive_source_immutability_rate.point_estimate == 1.0
    assert report.frozen_source_immutability_rate.point_estimate == 1.0
    assert all(
        row.adaptive is not None
        and row.frozen is not None
        and row.adaptive.none_of_above_available
        and row.frozen.none_of_above_available
        for row in observed
    )

    tenant_ids = [
        tenant_id
        for assignment in evidence.assignments
        for tenant_id in (
            assignment.adaptive_tenant_id,
            assignment.frozen_tenant_id,
        )
    ]
    async with resolver_db.acquire() as conn:
        materialized_tenant_count = await conn.fetchval(
            "SELECT count(*) FROM tenants WHERE id=ANY($1::uuid[])",
            tenant_ids,
        )
    assert materialized_tenant_count == 28

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["evidence_digest"] == evidence.digest
    assert payload["report"]["observation_digest"] == (
        report.observation_digest
    )
    assert payload["report"]["status"] == "observed_with_gaps"
    persisted = CompanyLearningVariantCollisionEvidence.model_validate(
        {
            key: value
            for key, value in payload.items()
            if key != "evidence_digest"
        }
    )
    assert persisted == evidence
    assert persisted.digest == payload["evidence_digest"]
