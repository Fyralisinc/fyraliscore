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
    QUERY_MANIFEST_NAME,
    run_source_identity_binding_lifecycle_experiment,
    validate_source_identity_binding_query_manifest,
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

    query_manifest_path = tmp_path / QUERY_MANIFEST_NAME
    query_manifest = json.loads(
        query_manifest_path.read_text(encoding="utf-8")
    )
    assert (
        validate_source_identity_binding_query_manifest(query_manifest)
        == query_manifest
    )
    manifest_ref = (
        f"artifact:{query_manifest_path.resolve()}"
        f"#sha256:{query_manifest['manifest_digest']}"
    )
    assert manifest_ref in evidence.artifact_refs
    assert manifest_ref in evidence.observation.artifact_refs
    assert manifest_ref in evidence.observation.source_immutable.artifact_refs
    assert manifest_ref in evidence.observation.foreign_tenant_isolated.artifact_refs
    assert manifest_ref in evidence.observation.overlap_prevented.artifact_refs

    queries = {
        entry["name"]: entry
        for entry in query_manifest["queries"]
    }
    observation_columns = {
        row["column_name"]
        for row in await resolver_db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='observations'
            """
        )
    }
    attachment_columns = {
        row["column_name"]
        for row in await resolver_db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='observation_source_identity_bindings'
            """
        )
    }
    observation_before = queries["primary_observation_before"]
    observation_after = queries["primary_observation_after"]
    attachment_before = queries["primary_attachment_before"]
    attachment_after = queries["primary_attachment_after"]
    assert set(observation_before["rows"][0]) == observation_columns
    assert set(observation_after["rows"][0]) == observation_columns
    assert observation_before["row_digest"] == observation_after["row_digest"]
    assert set(attachment_before["rows"][0]) == attachment_columns
    assert set(attachment_after["rows"][0]) == attachment_columns
    assert attachment_before["row_digest"] == attachment_after["row_digest"]

    colliding_current = queries["colliding_tenant_current_bindings"]["rows"]
    assert len(colliding_current) == 2
    assert len({row["tenant_id"] for row in colliding_current}) == 2
    assert {row["source_system"] for row in colliding_current} == {"jira"}
    assert {
        row["source_native_identifier"] for row in colliding_current
    } == {"jira:system:eng"}
    assert len(
        {
            row["canonical_referent"]["id"]
            for row in colliding_current
        }
    ) == 2
    colliding_lineages = queries["colliding_tenant_binding_lineages"]["rows"]
    assert len(colliding_lineages) == 6
    assert {
        row["binding_version"] for row in colliding_lineages
    } == {1, 2, 3}
    assert len(
        {
            row["tenant_id"]
            for row in queries["colliding_tenant_attachments"]["rows"]
        }
    ) == 2

    direct_overlap = queries["direct_sql_overlap_rejection"]
    assert direct_overlap["operation"] == "rejected_write"
    assert direct_overlap["outcome"] == "rejected"
    assert direct_overlap["error"]["class"] == "ExclusionViolationError"
    assert direct_overlap["error"]["sqlstate"] == "23P01"
    assert direct_overlap["error"]["constraint_name"] == (
        "source_identity_bindings_no_valid_time_overlap"
    )

    tampered_manifest = json.loads(json.dumps(query_manifest))
    tampered_manifest["queries"][0]["rows"][0]["content_text"] = "tampered"
    with pytest.raises(
        ValueError,
        match="query manifest digest mismatch",
    ):
        validate_source_identity_binding_query_manifest(tampered_manifest)

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
