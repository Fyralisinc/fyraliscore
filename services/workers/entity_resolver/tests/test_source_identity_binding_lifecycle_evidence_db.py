from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.source_identity_binding_lifecycle import (
    validate_source_identity_binding_lifecycle_artifact,
)
from scripts.run_source_identity_binding_lifecycle_db import (
    ARTIFACT_NAME,
    run_source_identity_binding_lifecycle_experiment,
)


pytestmark = pytest.mark.integration


async def test_database_lifecycle_runner_emits_complete_observed_evidence(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_source_identity_binding_lifecycle_experiment(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="source-binding-lifecycle-db-proof",
        system_version="test",
    )

    assert evidence.report.status == "observed"
    assert evidence.report.full_scope_complete is True
    assert evidence.report.expected_measurement_count == 12
    assert evidence.report.observed_measurement_count == 12
    assert evidence.report.unsupported_measurement_count == 0
    assert evidence.report.violating_measurement_count == 0
    assert evidence.report.safety_violation_count == 0
    assert evidence.report.immutability_violation_count == 0
    assert evidence.report.runtime_support_rate.point_estimate == 1.0
    assert evidence.report.overall_satisfaction_rate is not None
    assert evidence.report.overall_satisfaction_rate.point_estimate == 1.0
    assert all(
        estimate is not None and estimate.point_estimate == 1.0
        for estimate in evidence.report.measurement_rates.values()
    )
    assert evidence.observation.original_binding_version == 1
    assert evidence.observation.closure_binding_version == 2
    assert evidence.observation.successor_binding_version == 3

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert validate_source_identity_binding_lifecycle_artifact(payload) == evidence

    with pytest.raises(
        RuntimeError,
        match="requires fresh deterministic tenants",
    ):
        await run_source_identity_binding_lifecycle_experiment(
            pool=resolver_db,
            output_dir=tmp_path,
            run_id="source-binding-lifecycle-db-proof",
            system_version="test",
        )

    with pytest.raises(ValueError, match="run_id and system_version"):
        await run_source_identity_binding_lifecycle_experiment(
            pool=resolver_db,
            output_dir=tmp_path,
            run_id=" ",
            system_version="test",
        )
