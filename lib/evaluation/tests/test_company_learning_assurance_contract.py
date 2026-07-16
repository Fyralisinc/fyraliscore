from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_assurance import (
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    SlackAssurance,
    validate_company_learning_assurance_artifact,
    validate_correction_assurance_component,
)
from lib.evaluation.correction_assurance import (
    CorrectionAssuranceArtifact,
    CorrectionRuntimeEvidence,
    build_correction_assurance,
)
from lib.evaluation.correction_propagation import (
    CorrectionPropagationAudit,
    CorrectionPropagationScope,
)
from lib.evaluation.proof import EvidenceTier


_DIGEST = "a" * 64
_ARCHITECTURE_DIGEST = "b" * 64
_IMPLEMENTATION_PLAN_DIGEST = "c" * 64


def _correction_artifact(
    *,
    with_audit: bool = False,
) -> CorrectionAssuranceArtifact:
    audit = (
        CorrectionPropagationAudit(
            scope=CorrectionPropagationScope(
                tenant_id=uuid4(),
                predecessor_grounding_trace_id=uuid4(),
                run_id="pytest-assurance:correction",
                observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
            ),
            correction_grounding_trace_id=uuid4(),
            source_observation_id=uuid4(),
            correction_found=True,
            correction_changes_referent=True,
            discovered_dependency_count=0,
            component_counts={},
            repair_required_dependency_count=0,
            fenced_dependency_count=0,
            repaired_or_superseded_count=0,
            unsafe_readable_count=0,
            repair_pending_count=0,
            residual_repair_debt_count=0,
            convergence_ratio=1.0,
            safe_containment_ratio=1.0,
            source_hash_reference_count=1,
            source_hash_match_count=1,
            source_immutable=True,
            audit_read_only=True,
            cross_tenant_reference_count=0,
            cross_tenant_change_count=0,
            dependencies=(),
            incidents=(),
            uncertainty=(),
            artifact_refs=("pytest:correction-audit",),
        )
        if with_audit
        else None
    )
    return build_correction_assurance(
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
        created_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        runtime_evidence=CorrectionRuntimeEvidence(
            expected_dependency_refs=("model:old",),
            discovered_dependency_refs=("model:old",),
            expected_immediate_fence_refs=("model:old",),
            immediate_fence_refs=("model:old",),
            source_before_digest="d" * 64,
            source_after_digest="d" * 64,
            artifact_refs=("pytest:correction-runtime",),
        ),
        audit=audit,
        artifact_refs=("pytest:correction-assurance",),
    )


def _correction_summary(
    *,
    path: str = "/tmp/correction-assurance.json",
    evidence_digest: str | None = None,
    artifact: CorrectionAssuranceArtifact | None = None,
    audit_digest: str | None = None,
) -> CorrectionAssurance:
    artifact = artifact or _correction_artifact()
    metrics = artifact.metrics
    component_digests = {
        "evidence": evidence_digest or artifact.digest,
    }
    if artifact.audit is not None:
        component_digests["audit"] = (
            audit_digest
            or canonical_sha256(artifact.audit.model_dump(mode="json"))
        )
    return CorrectionAssurance(
        status=artifact.status,
        evidence_tier=EvidenceTier.E4,
        expected_dependency_count=metrics.expected_dependency_count,
        discovered_dependency_count=metrics.discovered_dependency_count,
        dependency_discovery_rate=metrics.dependency_discovery_rate,
        immediate_fence_rate=metrics.immediate_fence_rate,
        direct_repair_rate=metrics.direct_repair_rate,
        recursive_repair_rate=metrics.recursive_repair_rate,
        relation_retirement_rate=metrics.relation_retirement_rate,
        projection_invalidation_rate=metrics.projection_invalidation_rate,
        projection_rebuild_rate=metrics.projection_rebuild_rate,
        residual_unsafe_debt_count=metrics.residual_unsafe_debt_count,
        convergence_ratio=metrics.convergence_ratio,
        replay_idempotent=metrics.replay_idempotent,
        source_immutable=metrics.source_immutable,
        tenant_isolated=metrics.tenant_isolated,
        converged=metrics.converged,
        incidents=artifact.incidents,
        artifact_paths={"correction_evidence": path},
        component_digests=component_digests,
    )


def _slack_assurance(
    *,
    status: str = "observed",
    scope_complete: bool = True,
    open_world_complete: bool = False,
    blocking_for_active_slice: bool = True,
    evidence_tier: EvidenceTier = EvidenceTier.E4,
    supported_case_count: int = 9,
    correct_case_count: int = 9,
) -> SlackAssurance:
    return SlackAssurance(
        status=status,
        metrics={
            "case_count": 9,
            "supported_case_count": supported_case_count,
            "correct_case_count": correct_case_count,
            "contamination_rate": 0.0,
        },
        evidence_tier=evidence_tier,
        scope_complete=scope_complete,
        open_world_complete=open_world_complete,
        blocking_for_active_slice=blocking_for_active_slice,
        artifact_paths={
            "slack_observations": "/tmp/slack-observations.jsonl",
            "slack_report": "/tmp/slack-report.json",
        },
        component_digests={
            "report": _DIGEST,
            "gold_manifest": _DIGEST,
            "observations": _DIGEST,
        },
    )


def _summary(
    *,
    slack: SlackAssurance | None = None,
    correction: CorrectionAssurance | None = None,
    status: str = "working",
    blocking_failures: tuple[str, ...] = (),
    architecture_digest: str = _ARCHITECTURE_DIGEST,
    implementation_plan_digest: str = _IMPLEMENTATION_PLAN_DIGEST,
    excluded_capabilities: tuple[str, ...] = (
        "autonomous_task_planning",
        "autonomous_task_execution",
    ),
) -> CompanyLearningAssuranceSummary:
    positive = PositiveAssurance(
        status="observed",
        pair_count=3,
        adaptive_correctness_rate=1.0,
        frozen_correctness_rate=0.0,
        adaptive_minus_frozen_correctness=1.0,
        hard_failures=(),
        artifact_paths={
            "positive_pair": "/tmp/positive-pair.json",
            "positive_company_learning_evaluation": (
                "/tmp/positive-evaluation.json"
            ),
            "positive_company_learning_evidence_bundle": (
                "/tmp/positive-bundle.json"
            ),
        },
        component_digests={
            "report": _DIGEST,
            "company_learning_evaluation": _DIGEST,
            "company_learning_evidence_bundle": _DIGEST,
        },
    )
    negative = NegativeAssurance(
        status="observed",
        pair_count=4,
        safety_incident_count=0,
        adaptive_unsafe_count=0,
        frozen_unsafe_count=0,
        artifact_paths={"negative_evidence": "/tmp/negative.json"},
        component_digests={
            "evidence": _DIGEST,
            "report": _DIGEST,
            "plan": _DIGEST,
        },
    )
    population = PopulationAssurance(
        status="observed",
        registry_pair_count=60,
        observed_pair_count=60,
        unsupported_case_count=0,
        runtime_support_rate=1.0,
        metrics={
            "pair_count": 60,
            "observed_pair_count": 60,
            "unsupported_case_count": 0,
            "complete_population": True,
        },
        unsupported_strata_counts={"entity_type": {}},
        unsupported_reason_counts={},
        artifact_paths={"population_evidence": "/tmp/population.json"},
        component_digests={
            "evidence": _DIGEST,
            "registry": _DIGEST,
            "report": _DIGEST,
        },
    )
    slack = slack or _slack_assurance()
    correction = correction or _correction_summary()
    artifact_paths = {
        **positive.artifact_paths,
        **negative.artifact_paths,
        **slack.artifact_paths,
        **correction.artifact_paths,
        **population.artifact_paths,
    }
    component_digests = {
        **{
            f"positive_{key}": value
            for key, value in positive.component_digests.items()
        },
        **{
            f"negative_{key}": value
            for key, value in negative.component_digests.items()
        },
        **{
            f"slack_{key}": value
            for key, value in slack.component_digests.items()
        },
        **{
            f"correction_{key}": value
            for key, value in correction.component_digests.items()
        },
        **{
            f"population_{key}": value
            for key, value in population.component_digests.items()
        },
    }
    return CompanyLearningAssuranceSummary(
        run_id="pytest-assurance",
        system_version="pytest-system",
        architecture_digest=architecture_digest,
        implementation_plan_digest=implementation_plan_digest,
        excluded_capabilities=excluded_capabilities,
        created_at="2026-07-16T00:00:00+00:00",
        status=status,
        positive=positive,
        negative=negative,
        slack=slack,
        correction=correction,
        population=population,
        proof_gaps=("not open-world or task-autonomy proof",),
        blocking_failures=blocking_failures,
        component_digests=component_digests,
        artifact_paths=artifact_paths,
    )


def test_summary_v2_binds_reviewed_identity_and_active_scope() -> None:
    summary = _summary()

    assert summary.schema_version == "company-learning-assurance-summary-v2"
    assert summary.architecture_digest == _ARCHITECTURE_DIGEST
    assert summary.implementation_plan_digest == _IMPLEMENTATION_PLAN_DIGEST
    assert summary.evaluation_profile == "autonomous-company-learning-v1"
    assert summary.excluded_capabilities == (
        "autonomous_task_planning",
        "autonomous_task_execution",
    )
    assert validate_company_learning_assurance_artifact(
        summary.artifact_payload()
    ) == summary

    with pytest.raises(ValidationError, match="architecture_digest"):
        _summary(architecture_digest="not-a-digest")
    with pytest.raises(ValidationError, match="implementation_plan_digest"):
        _summary(implementation_plan_digest="not-a-digest")
    with pytest.raises(ValidationError, match="explicitly exclude"):
        _summary(excluded_capabilities=("autonomous_task_planning",))


def test_slack_proof_semantics_are_explicit_and_noncompensatory() -> None:
    with pytest.raises(ValidationError, match="at least E5"):
        _slack_assurance(
            open_world_complete=True,
            evidence_tier=EvidenceTier.E4,
        )

    incomplete = _slack_assurance(
        status="observed_with_gaps",
        scope_complete=False,
        supported_case_count=8,
        correct_case_count=8,
    )
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(slack=incomplete)

    failed = _summary(
        slack=incomplete,
        status="failed",
        blocking_failures=("Slack active slice is incomplete.",),
    )
    assert failed.status == "failed"
    assert failed.slack.blocking_for_active_slice is True

    old_payload = {
        "status": "observed",
        "metrics": {},
        "diagnostic_only": True,
        "artifact_paths": {"slack_report": "/tmp/slack.json"},
        "component_digests": {"report": _DIGEST},
    }
    with pytest.raises(ValidationError):
        SlackAssurance.model_validate(old_payload)


def test_correction_component_reopens_runtime_artifact_and_digest(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact()
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _correction_summary(
        path=str(artifact_path),
        evidence_digest=artifact.digest,
    )

    validated = validate_correction_assurance_component(
        assurance,
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
    )
    assert validated == artifact

    payload = artifact.artifact_payload()
    payload["metrics"]["discovered_dependency_count"] = 0
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="artifact digest mismatch"):
        validate_correction_assurance_component(
            assurance,
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )


def test_correction_component_rejects_summary_digest_or_identity_mismatch(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact()
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    wrong_digest = _correction_summary(
        path=str(artifact_path),
        evidence_digest="e" * 64,
    )

    with pytest.raises(ValueError, match="component digest mismatch"):
        validate_correction_assurance_component(
            wrong_digest,
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )
    with pytest.raises(ValueError, match="run identity mismatch"):
        validate_correction_assurance_component(
            _correction_summary(path=str(artifact_path)),
            run_id="another-run:correction",
            system_version="pytest-system",
        )


def test_correction_component_validates_optional_audit_digest(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact(with_audit=True)
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _correction_summary(
        path=str(artifact_path),
        artifact=artifact,
    )

    assert validate_correction_assurance_component(
        assurance,
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
    ) == artifact

    with pytest.raises(ValueError, match="audit digest mismatch"):
        validate_correction_assurance_component(
            _correction_summary(
                path=str(artifact_path),
                artifact=artifact,
                audit_digest="f" * 64,
            ),
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )
