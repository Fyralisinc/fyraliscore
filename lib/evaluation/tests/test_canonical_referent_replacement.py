from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from lib.evaluation.canonical_referent_replacement import (
    CanonicalReplacementDatabaseEvidence,
    CanonicalResourceReplacementEvidence,
    CanonicalResourceReplacementObservation,
    ReplacementProofCell,
    evaluate_canonical_resource_replacement,
    validate_canonical_resource_replacement_artifact,
)
from lib.evaluation.company_learning_experiment import CanonicalEntityRef


EFFECTIVE_AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
TRANSACTION_AT = EFFECTIVE_AT + timedelta(hours=2)
DELAYED_EVENT_AT = EFFECTIVE_AT - timedelta(days=1)
MEASUREMENTS = (
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
)


def test_full_replacement_observation_is_continuous_and_noncompensatory() -> None:
    report = evaluate_canonical_resource_replacement(_observation())

    assert report.status == "observed"
    assert report.expected_measurement_count == 20
    assert report.observed_measurement_count == 20
    assert report.unsupported_measurement_count == 0
    assert report.violating_measurement_count == 0
    assert report.safety_violation_count == 0
    assert report.immutability_violation_count == 0
    assert report.runtime_support_rate.point_estimate == 1.0
    assert report.runtime_support_rate.numerator == 20
    assert report.runtime_support_rate.denominator == 20
    assert report.runtime_support_rate.method == "descriptive_checklist_ratio"
    assert report.overall_satisfaction_rate is not None
    assert report.overall_satisfaction_rate.point_estimate == 1.0
    assert report.overall_satisfaction_rate.numerator == 20
    assert report.overall_satisfaction_rate.denominator == 20
    assert report.transition_control_rate.point_estimate == 1.0
    assert report.lifecycle_alias_safety_rate.point_estimate == 1.0
    assert report.source_boundary_rate.point_estimate == 1.0
    assert report.immutability_rate.point_estimate == 1.0
    assert report.projection_coherence_rate.point_estimate == 1.0
    assert report.lineage_retrieval_rate.point_estimate == 1.0
    assert report.dependency_atomicity_rate.point_estimate == 1.0
    assert report.full_scope_complete is True


@pytest.mark.parametrize(
    "measurement",
    (
        "operation_conflict_rejected",
        "stale_head_rejected",
        "tenant_isolated",
        "alias_current_successor_safe",
        "alias_asof_predecessor_safe",
        "exact_source_binding_boundary_safe",
        "delayed_event_attachment_fail_closed",
        "lineage_time_boundary_safe",
        "hard_dependency_rejected",
        "transaction_atomic",
    ),
)
def test_each_safety_failure_contradicts_without_compensation(
    measurement: str,
) -> None:
    observation = _observation().model_copy(
        update={measurement: _observed(False, measurement)}
    )

    report = evaluate_canonical_resource_replacement(observation)

    assert report.status == "contradicted"
    assert report.violating_measurement_count == 1
    assert report.safety_violation_count == 1
    assert report.full_scope_complete is False
    assert report.measurement_rates[measurement].point_estimate == 0.0
    assert report.measurement_rates[measurement].numerator == 0
    assert report.measurement_rates[measurement].denominator == 1


@pytest.mark.parametrize(
    "measurement",
    (
        "old_attachment_immutable",
        "source_observation_immutable",
        "model_scope_immutable",
    ),
)
def test_each_immutability_failure_is_a_hard_contradiction(
    measurement: str,
) -> None:
    observation = _observation().model_copy(
        update={measurement: _observed(False, measurement)}
    )

    report = evaluate_canonical_resource_replacement(observation)

    assert report.status == "contradicted"
    assert report.immutability_violation_count == 1
    assert report.immutability_rate.point_estimate == pytest.approx(2 / 3)
    assert report.immutability_rate.numerator == 2
    assert report.immutability_rate.denominator == 3


def test_runtime_gaps_are_measured_without_fabricated_evidence() -> None:
    unsupported = {
        name: _unsupported("runtime surface does not emit replacement evidence")
        for name in (
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
        )
    }

    report = evaluate_canonical_resource_replacement(
        _observation().model_copy(update=unsupported)
    )

    assert report.status == "observed_with_gaps"
    assert report.observed_measurement_count == 5
    assert report.unsupported_measurement_count == 15
    assert report.runtime_support_rate.point_estimate == 0.25
    assert report.runtime_support_rate.numerator == 5
    assert report.runtime_support_rate.denominator == 20
    assert report.violating_measurement_count == 0
    assert report.lifecycle_alias_safety_rate is None
    assert report.immutability_rate is None
    assert report.unsupported_reason_counts == {
        "runtime surface does not emit replacement evidence": 15
    }


def test_completely_unsupported_runtime_remains_a_valid_gap_report() -> None:
    observation = _observation().model_copy(
        update={
            name: _unsupported("replacement proof runner not implemented")
            for name in MEASUREMENTS
        }
    )

    report = evaluate_canonical_resource_replacement(observation)

    assert report.status == "observed_with_gaps"
    assert report.observed_measurement_count == 0
    assert report.unsupported_measurement_count == 20
    assert report.runtime_support_rate.point_estimate == 0.0
    assert report.runtime_support_rate.numerator == 0
    assert report.runtime_support_rate.denominator == 20
    assert report.overall_satisfaction_rate is None
    assert report.full_scope_complete is False


def test_evidence_reopens_raw_measurements_and_digest() -> None:
    observation = _observation()
    evidence = CanonicalResourceReplacementEvidence(
        run_id="pytest-resource-replacement",
        system_version="pytest-system",
        created_at=TRANSACTION_AT.isoformat(),
        observation=observation,
        database_evidence=_database_evidence(observation),
        report=evaluate_canonical_resource_replacement(observation),
        artifact_refs=("pytest:resource-replacement",),
    )

    assert (
        validate_canonical_resource_replacement_artifact(evidence.artifact_payload())
        == evidence
    )

    tampered = evidence.artifact_payload()
    tampered["observation"]["transaction_atomic"]["satisfied"] = False
    with pytest.raises(ValueError, match="report does not match"):
        validate_canonical_resource_replacement_artifact(tampered)


def test_unsupported_cells_reject_fabricated_values_and_artifacts() -> None:
    with pytest.raises(ValidationError, match="cannot carry fabricated evidence"):
        ReplacementProofCell(
            status="unsupported",
            satisfied=True,
            unsupported_reason="not emitted",
            artifact_refs=("fabricated:proof",),
        )


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        (
            {
                "predecessor": CanonicalEntityRef(
                    type="customer",
                    id="customer:old",
                )
            },
            "requires resource referents",
        ),
        (
            {
                "successor": CanonicalEntityRef(
                    type="resource",
                    id="system:legacy-billing",
                    version=2,
                )
            },
            "predecessor and successor must differ",
        ),
        (
            {"delayed_event_occurred_at": EFFECTIVE_AT},
            "event before replacement effect",
        ),
    ),
)
def test_sealed_replacement_scope_rejects_invalid_raw_observations(
    updates: dict,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        CanonicalResourceReplacementObservation.model_validate(
            {
                **_observation().model_dump(mode="json"),
                **updates,
            }
        )


def _observation() -> CanonicalResourceReplacementObservation:
    return CanonicalResourceReplacementObservation(
        predecessor=CanonicalEntityRef(
            type="resource",
            id="system:legacy-billing",
            version=2,
        ),
        successor=CanonicalEntityRef(
            type="resource",
            id="system:billing-platform",
            version=1,
        ),
        effective_at=EFFECTIVE_AT,
        transaction_at=TRANSACTION_AT,
        delayed_event_occurred_at=DELAYED_EVENT_AT,
        replacement_reason=(
            "The governed system identity replaced the legacy resource."
        ),
        **{name: _observed(True, name) for name in MEASUREMENTS},
        artifact_refs=("pytest:replacement-observation",),
    )


def _database_evidence(
    observation: CanonicalResourceReplacementObservation,
) -> CanonicalReplacementDatabaseEvidence:
    snapshots = {
        name: {"satisfied": cell.satisfied}
        for name, cell in observation.measurements.items()
    }
    return CanonicalReplacementDatabaseEvidence(
        query_manifest={"pytest": "SELECT synthetic_test_evidence"},
        snapshots=snapshots,
        measurement_evidence={name: (name,) for name in observation.measurements},
    )


def _observed(satisfied: bool, name: str) -> ReplacementProofCell:
    return ReplacementProofCell(
        status="observed",
        satisfied=satisfied,
        artifact_refs=(f"pytest:{name}",),
    )


def _unsupported(reason: str) -> ReplacementProofCell:
    return ReplacementProofCell(
        status="unsupported",
        unsupported_reason=reason,
    )
