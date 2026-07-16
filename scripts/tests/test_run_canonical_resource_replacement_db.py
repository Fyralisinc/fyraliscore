from __future__ import annotations

import json

import asyncpg
import pytest

from lib.evaluation.canonical_referent_replacement import (
    validate_canonical_resource_replacement_artifact,
)
from lib.shared.ids import uuid7
from scripts.run_canonical_resource_replacement_db import (
    ARTIFACT_NAME,
    run_canonical_resource_replacement_experiment,
    scenario_ids,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

MEASUREMENT_NAMES = {
    "transition_applied",
    "operation_replay_idempotent",
    "operation_conflict_rejected",
    "stale_head_rejected",
    "tenant_isolated",
    "predecessor_retired",
    "successor_active",
    "alias_current_successor_safe",
    "alias_asof_predecessor_safe",
    "exact_source_binding_boundary_safe",
    "delayed_event_attachment_fail_closed",
    "old_attachment_immutable",
    "source_observation_immutable",
    "model_scope_immutable",
    "projection_invalidated",
    "projection_single_refresh",
    "lineage_reason_correct",
    "lineage_time_boundary_safe",
    "hard_dependency_rejected",
    "transaction_atomic",
}


async def test_db_runner_emits_self_authenticating_replacement_evidence(
    db_pool: asyncpg.Pool,
    tmp_path,
) -> None:
    run_id = f"pytest-canonical-replacement-{uuid7()}"
    system_version = "pytest-system-version"

    first_ids = scenario_ids(
        run_id=run_id,
        system_version=system_version,
    )
    assert first_ids == scenario_ids(
        run_id=run_id,
        system_version=system_version,
    )

    evidence = await run_canonical_resource_replacement_experiment(
        pool=db_pool,
        output_dir=tmp_path,
        run_id=run_id,
        system_version=system_version,
    )

    assert evidence.run_id == run_id
    assert evidence.system_version == system_version
    assert set(evidence.observation.measurements) == MEASUREMENT_NAMES
    assert evidence.report.expected_measurement_count == 20
    assert evidence.report.observed_measurement_count == 20
    assert evidence.report.unsupported_measurement_count == 0
    assert evidence.report.violating_measurement_count == 0
    assert evidence.report.status == "observed"
    assert evidence.report.runtime_support_rate.point_estimate == 1.0
    assert evidence.report.full_scope_complete is True
    assert set(evidence.database_evidence.measurement_evidence) == MEASUREMENT_NAMES
    assert evidence.database_evidence.query_manifest
    assert evidence.database_evidence.snapshots
    for name, cell in evidence.observation.measurements.items():
        assert cell.status == "observed"
        assert cell.satisfied is True
        assert cell.artifact_refs

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["evidence_digest"] == evidence.digest
    assert validate_canonical_resource_replacement_artifact(payload) == evidence
    foreign_before = evidence.database_evidence.snapshots["foreign_tenant_before"]
    foreign_after = evidence.database_evidence.snapshots["foreign_tenant_after"]
    assert foreign_before == foreign_after
    assert foreign_after["transition_count"] == 0
    assert foreign_after["cross_tenant_operation_count"] == 0
    assert foreign_after["current_binding_ref"]["id"] == str(
        first_ids.isolation_predecessor_id
    )
    source_resolution = evidence.database_evidence.snapshots[
        "source_binding_resolution"
    ]
    assert source_resolution["delayed_attachment_resolution"] is None

    payload["observation"]["transaction_atomic"]["satisfied"] = False
    with pytest.raises(ValueError, match="report does not match"):
        validate_canonical_resource_replacement_artifact(payload)

    payload = evidence.artifact_payload()
    payload["database_evidence"]["snapshots"]["foreign_tenant_after"][
        "transition_count"
    ] = 1
    with pytest.raises(ValueError, match="evidence digest mismatch"):
        validate_canonical_resource_replacement_artifact(payload)

    assert (
        await db_pool.fetchval(
            """
        SELECT count(*)
        FROM canonical_referent_transitions
        WHERE tenant_id=$1
        """,
            first_ids.tenant_id,
        )
        == 1
    )
    assert (
        await db_pool.fetchval(
            """
        SELECT count(*)
        FROM canonical_referent_transitions
        WHERE tenant_id=ANY($1::uuid[])
        """,
            [
                first_ids.isolation_tenant_id,
                first_ids.atomicity_tenant_id,
            ],
        )
        == 0
    )
