from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from lib.evaluation.correction_assurance import (
    CorrectionRuntimeEvidence,
    build_correction_assurance,
    validate_correction_assurance_artifact,
)
from lib.evaluation.correction_propagation import (
    CorrectionDependencyKind,
    CorrectionDependencyRecord,
    CorrectionPropagationAudit,
    CorrectionPropagationScope,
)
from scripts.run_correction_assurance import ARTIFACT_NAME, main


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def _complete_evidence() -> CorrectionRuntimeEvidence:
    return CorrectionRuntimeEvidence(
        expected_dependency_refs=(
            "model:old",
            "model:direct",
            "model:recursive",
            "relation:one",
            "projection:customers:nimbus",
        ),
        discovered_dependency_refs=(
            "model:old",
            "model:direct",
            "model:recursive",
            "relation:one",
            "projection:customers:nimbus",
        ),
        expected_immediate_fence_refs=("model:direct", "model:recursive"),
        immediate_fence_refs=("model:direct", "model:recursive"),
        expected_direct_repair_refs=("model:old",),
        direct_repair_refs=("model:old",),
        expected_recursive_repair_refs=("model:direct", "model:recursive"),
        recursive_repair_refs=("model:direct", "model:recursive"),
        expected_relation_retirement_refs=("relation:one",),
        relation_retirement_refs=("relation:one",),
        expected_projection_invalidation_refs=("projection:customers:nimbus",),
        projection_invalidation_refs=("projection:customers:nimbus",),
        expected_projection_rebuild_refs=("projection:customers:nimbus",),
        projection_rebuild_refs=("projection:customers:nimbus",),
        source_before_digest="a" * 64,
        source_after_digest="a" * 64,
        artifact_refs=("pytest:correction-runtime",),
    )


def test_complete_runtime_evidence_is_continuously_working() -> None:
    artifact = build_correction_assurance(
        run_id="pytest-correction-complete",
        system_version="pytest",
        created_at=NOW,
        runtime_evidence=_complete_evidence(),
        artifact_refs=("pytest:correction-complete",),
    )

    assert artifact.status == "working"
    assert artifact.metrics.dependency_discovery_rate == 1.0
    assert artifact.metrics.immediate_fence_rate == 1.0
    assert artifact.metrics.direct_repair_rate == 1.0
    assert artifact.metrics.recursive_repair_rate == 1.0
    assert artifact.metrics.relation_retirement_rate == 1.0
    assert artifact.metrics.projection_invalidation_rate == 1.0
    assert artifact.metrics.projection_rebuild_rate == 1.0
    assert artifact.metrics.convergence_ratio == 1.0
    assert artifact.metrics.residual_unsafe_debt_count == 0
    assert artifact.metrics.replay_idempotent is True
    assert artifact.metrics.source_immutable is True
    assert artifact.metrics.tenant_isolated is True
    assert artifact.metrics.converged is True
    assert artifact.incidents == ()
    assert set(artifact.component_digests) == {"evidence"}
    assert len(artifact.component_digests["evidence"]) == 64


def test_runtime_and_audit_debt_are_never_hidden() -> None:
    evidence = _complete_evidence().model_copy(
        update={
            "recursive_repair_refs": ("model:direct",),
            "residual_unsafe_refs": ("model:recursive",),
            "replay_new_work_refs": ("reeval:model:direct",),
            "source_after_digest": "b" * 64,
            "cross_tenant_change_refs": ("tenant:other:model:changed",),
        }
    )
    dependency = CorrectionDependencyRecord(
        kind=CorrectionDependencyKind.MODEL,
        object_ref="model:audit-unsafe",
        dependency_basis=("grounding-trace:old",),
        lifecycle_state="active",
        repair_required=True,
        read_surface=True,
        unsafe_readable=True,
    )
    audit = CorrectionPropagationAudit(
        scope=CorrectionPropagationScope(
            tenant_id=uuid4(),
            predecessor_grounding_trace_id=uuid4(),
            run_id="pytest-audit",
            observed_at=NOW,
        ),
        correction_grounding_trace_id=uuid4(),
        source_observation_id=uuid4(),
        correction_found=True,
        correction_changes_referent=True,
        discovered_dependency_count=1,
        component_counts={"model": 1},
        repair_required_dependency_count=1,
        fenced_dependency_count=0,
        repaired_or_superseded_count=0,
        unsafe_readable_count=1,
        repair_pending_count=0,
        residual_repair_debt_count=1,
        convergence_ratio=0.0,
        safe_containment_ratio=0.0,
        source_hash_reference_count=1,
        source_hash_match_count=1,
        source_immutable=True,
        audit_read_only=True,
        cross_tenant_reference_count=0,
        cross_tenant_change_count=0,
        dependencies=(dependency,),
        incidents=("unsafe_readable_corrected_dependency",),
        uncertainty=("audit proof boundary",),
        artifact_refs=("pytest:audit",),
    )

    artifact = build_correction_assurance(
        run_id="pytest-correction-debt",
        system_version="pytest",
        created_at=NOW,
        runtime_evidence=evidence,
        audit=audit,
        artifact_refs=("pytest:correction-debt",),
    )

    assert artifact.status == "failed"
    assert artifact.metrics.recursive_repair_rate == 0.5
    assert artifact.metrics.residual_unsafe_debt_count == 2
    assert artifact.metrics.replay_idempotent is False
    assert artifact.metrics.source_immutable is False
    assert artifact.metrics.tenant_isolated is False
    assert artifact.metrics.converged is False
    assert {
        "incomplete_recursive_repair",
        "residual_unsafe_correction_debt",
        "correction_replay_created_new_work",
        "source_observation_mutated",
        "tenant_isolation_violation",
    } <= set(artifact.incidents)
    assert set(artifact.component_digests) == {"evidence", "audit"}
    assert all(
        len(digest) == 64
        for digest in artifact.component_digests.values()
    )


def test_cli_writes_json_and_readable_markdown(tmp_path: Path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    evidence_path.write_text(
        json.dumps(_complete_evidence().model_dump(mode="json")),
        encoding="utf-8",
    )
    output_dir = tmp_path / "correction-assurance"

    exit_code = main(
        [
            "--runtime-evidence",
            str(evidence_path),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "pytest-correction-cli",
            "--system-version",
            "pytest",
        ]
    )

    assert exit_code == 0
    payload = json.loads(
        (output_dir / ARTIFACT_NAME).read_text(encoding="utf-8")
    )
    assert payload["status"] == "working"
    assert payload["metrics"]["converged"] is True
    assert len(payload["artifact_digest"]) == 64
    assert validate_correction_assurance_artifact(payload).digest == (
        payload["artifact_digest"]
    )
    markdown = (output_dir / "correction_assurance.md").read_text(
        encoding="utf-8"
    )
    assert "Dependency discovery: **5/5**" in markdown
    assert "Residual unsafe debt: **0**" in markdown
