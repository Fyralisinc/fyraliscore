from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.correction_propagation import (
    CorrectionPropagationScope,
    analyze_correction_propagation_rows,
    render_correction_propagation_markdown,
)


def _scope() -> CorrectionPropagationScope:
    return CorrectionPropagationScope(
        tenant_id=uuid4(),
        predecessor_grounding_trace_id=uuid4(),
        run_id="unit-correction-audit",
        observed_at=datetime.now(timezone.utc),
    )


def test_audit_counts_history_exposure_containment_and_residual_debt() -> None:
    scope = _scope()
    observation_id = uuid4()
    successor_id = uuid4()
    active_model = uuid4()
    archived_model = uuid4()
    content = "NBI is blocked"
    source_hash = canonical_sha256(content)
    audit = analyze_correction_propagation_rows(
        scope=scope,
        root={
            "id": scope.predecessor_grounding_trace_id,
            "source_observation_id": observation_id,
            "selected_referent": {"type": "customer", "id": str(uuid4())},
            "source_observation_mutated": False,
            "observation_content_text": content,
            "detection_source_content_hash": source_hash,
            "successor_id": successor_id,
            "successor_source_observation_id": observation_id,
            "successor_selected_referent": {
                "type": "customer",
                "id": str(uuid4()),
            },
            "successor_source_mutated": False,
            "successor_trace": {
                "supersedes_grounding_trace_id": str(
                    scope.predecessor_grounding_trace_id
                ),
                "correction_kind": "entity_clarification_adjudication",
            },
        },
        interpretations=(
            {
                "id": uuid4(),
                "source_content_hash": source_hash,
                "admission_id": uuid4(),
                "disposition": "belief_applied",
                "admitted_model_id": active_model,
            },
        ),
        models=(
            {
                "id": active_model,
                "status": "active",
                "visible_to_subjects": True,
                "archive_reason": None,
            },
            {
                "id": archived_model,
                "status": "archived",
                "visible_to_subjects": False,
                "archive_reason": "superseded",
            },
        ),
        edges=(
            {
                "id": uuid4(),
                "source_model_id": active_model,
                "target_model_id": archived_model,
                "edge_kind": "supports",
                "status": "inert",
            },
        ),
        relations=(
            {
                "id": uuid4(),
                "relation_kind": "depends_on",
                "status": "needs_review",
            },
        ),
        belief_addresses=(
            {
                "model_id": active_model,
                "model_status": "active",
                "visible_to_subjects": True,
                "fingerprint": "active-belief",
            },
        ),
        projection_snapshots=(
            {
                "projection_name": "customers",
                "projection_version": "v1",
                "subject_key": "customer:nbi",
                "source_model_ids": [active_model],
                "updated_at": datetime.now(timezone.utc),
            },
        ),
        projection_dependencies=(
            {
                "projection_name": "customers",
                "projection_version": "v1",
                "subject_key": "customer:nbi",
                "ref_kind": "model",
                "ref_value": str(active_model),
                "reason": "source_model",
            },
        ),
        projection_refresh_jobs=(
            {
                "id": uuid4(),
                "projection_name": "customers",
                "projection_version": "v1",
                "subject_key": "customer:nbi",
                "status": "pending",
                "attempts": 0,
                "last_error": None,
            },
        ),
        cross_tenant_reference_count=0,
        artifact_refs=("pytest:correction-propagation",),
    )

    assert audit.correction_found is True
    assert audit.correction_changes_referent is True
    assert audit.discovered_dependency_count == 10
    assert audit.repair_required_dependency_count == 7
    assert audit.repaired_or_superseded_count == 2
    assert audit.fenced_dependency_count == 1
    assert audit.unsafe_readable_count == 3
    assert audit.repair_pending_count == 1
    assert audit.residual_repair_debt_count == 5
    assert audit.convergence_ratio == 2 / 7
    assert audit.safe_containment_ratio == 3 / 7
    assert audit.source_immutable is True
    assert audit.source_hash_match_count == 2
    assert audit.cross_tenant_change_count == 0
    assert "unsafe_readable_corrected_dependency" in audit.incidents
    assert audit.converged is False
    rendered = render_correction_propagation_markdown(audit)
    assert "Residual repair debt: **5**" in rendered
    assert "Cross-tenant changes by this audit: **0**" in rendered


def test_audit_fails_closed_on_missing_correction_source_mutation_and_leakage() -> None:
    scope = _scope()
    content = "source changed"
    audit = analyze_correction_propagation_rows(
        scope=scope,
        root={
            "id": scope.predecessor_grounding_trace_id,
            "source_observation_id": uuid4(),
            "selected_referent": {"type": "customer", "id": str(uuid4())},
            "source_observation_mutated": True,
            "observation_content_text": content,
            "detection_source_content_hash": canonical_sha256("original"),
            "successor_id": None,
            "successor_source_observation_id": None,
            "successor_selected_referent": None,
            "successor_source_mutated": False,
            "successor_trace": {},
        },
        interpretations=(),
        models=(),
        edges=(),
        relations=(),
        belief_addresses=(),
        projection_snapshots=(),
        projection_dependencies=(),
        projection_refresh_jobs=(),
        cross_tenant_reference_count=2,
        artifact_refs=("pytest:correction-propagation",),
    )

    assert audit.correction_found is False
    assert audit.source_immutable is False
    assert audit.cross_tenant_reference_count == 2
    assert {
        "adjudicated_grounding_successor_missing",
        "cross_tenant_dependency_reference",
        "source_observation_hash_or_mutation_flag_mismatch",
    }.issubset(audit.incidents)


def test_missing_predecessor_produces_explicit_unavailable_audit() -> None:
    audit = analyze_correction_propagation_rows(
        scope=_scope(),
        root=None,
        interpretations=(),
        models=(),
        edges=(),
        relations=(),
        belief_addresses=(),
        projection_snapshots=(),
        projection_dependencies=(),
        projection_refresh_jobs=(),
        cross_tenant_reference_count=0,
        artifact_refs=("pytest:correction-propagation",),
    )

    assert audit.correction_found is False
    assert audit.source_immutable is None
    assert audit.discovered_dependency_count == 0
    assert audit.incidents == ("predecessor_grounding_trace_missing",)
