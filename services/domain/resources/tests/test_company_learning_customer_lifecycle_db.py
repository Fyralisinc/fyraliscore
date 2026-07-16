from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from scripts.run_company_learning_customer_lifecycle_db import (
    ARTIFACT_NAME,
    CompanyLearningCustomerLifecycleEvidence,
    run_customer_lifecycle_experiment,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_customer_lifecycle_runtime_produces_complete_continuous_evidence(
    resources_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_customer_lifecycle_experiment(
        pool=resources_db,
        output_dir=tmp_path,
        run_id="pytest-company-learning-customer-lifecycle",
        system_version="pytest-system",
    )

    assert len(evidence.registry_population.cases) == 8
    assert len(evidence.assignments) == 8
    assert len(evidence.observations) == 8
    assert all(
        observation.execution_status == "observed"
        for observation in evidence.observations
    )
    assert all(
        len(observation.resolution_probes) == (8 if case.reuse_initial_identity else 6)
        for case, observation in zip(
            evidence.registry_population.cases,
            evidence.observations,
            strict=True,
        )
    )

    report = evidence.report
    assert report.status == "observed"
    assert report.case_count == 8
    assert report.observed_case_count == 8
    assert report.unsupported_case_count == 0
    assert report.violating_case_count == 0
    assert report.runtime_support_rate.point_estimate == 1.0
    assert report.rename_continuity_rate.point_estimate == 1.0
    assert report.valid_time_resolution_accuracy.point_estimate == 1.0
    assert report.stale_alias_rejection_rate.point_estimate == 1.0
    assert report.current_alias_safety_rate.point_estimate == 1.0
    assert report.historical_name_reuse_accuracy.point_estimate == 1.0
    assert report.observation_immutability_rate.point_estimate == 1.0
    assert report.model_immutability_rate.point_estimate == 1.0
    assert report.archive_alias_rejection_rate.point_estimate == 1.0
    assert report.archived_mutation_rejection_rate.point_estimate == 1.0
    assert report.alias_interval_non_overlap_rate.point_estimate == 1.0
    assert report.tenant_isolation_rate.point_estimate == 1.0
    assert report.replay_idempotency_rate.point_estimate == 1.0

    tenant_ids = [
        tenant_id
        for assignment in evidence.assignments
        for tenant_id in (
            assignment.tenant_id,
            assignment.isolation_tenant_id,
        )
    ]
    materialized_tenant_count = await resources_db.fetchval(
        "SELECT count(*) FROM tenants WHERE id=ANY($1::uuid[])",
        tenant_ids,
    )
    assert materialized_tenant_count == 16

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["evidence_digest"] == evidence.digest
    assert payload["report"]["observation_digest"] == (report.observation_digest)
    persisted = CompanyLearningCustomerLifecycleEvidence.model_validate(
        {key: value for key, value in payload.items() if key != "evidence_digest"}
    )
    assert persisted == evidence
    assert persisted.digest == payload["evidence_digest"]
