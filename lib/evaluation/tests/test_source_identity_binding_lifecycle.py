from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from lib.evaluation.source_identity_binding_lifecycle import (
    BindingLifecycleProofCell,
    SourceIdentityBindingLifecycleEvidence,
    SourceIdentityBindingLifecycleObservation,
    evaluate_source_identity_binding_lifecycle,
    validate_source_identity_binding_lifecycle_artifact,
)


ORIGINAL_FROM = datetime(2026, 7, 1, tzinfo=timezone.utc)
EFFECTIVE_AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
TRANSACTION_AT = EFFECTIVE_AT + timedelta(hours=1)
MEASUREMENTS = (
    "current_resolution_correct",
    "asof_resolution_correct",
    "exact_attachment_preserved",
    "closure_correct",
    "revocation_correct",
    "supersession_correct",
    "overlap_prevented",
    "stale_version_rejected",
    "replay_idempotent",
    "foreign_tenant_isolated",
    "source_immutable",
    "transaction_atomic",
)


def test_full_lifecycle_observation_is_continuous_and_complete() -> None:
    report = evaluate_source_identity_binding_lifecycle(_observation())

    assert report.status == "observed"
    assert report.expected_measurement_count == 12
    assert report.observed_measurement_count == 12
    assert report.unsupported_measurement_count == 0
    assert report.violating_measurement_count == 0
    assert report.safety_violation_count == 0
    assert report.immutability_violation_count == 0
    assert report.runtime_support_rate.point_estimate == 1.0
    assert report.runtime_support_rate.numerator == 12
    assert report.runtime_support_rate.denominator == 12
    assert (
        report.runtime_support_rate.method
        == "descriptive_checklist_ratio"
    )
    assert report.overall_satisfaction_rate.point_estimate == 1.0
    assert report.overall_satisfaction_rate.numerator == 12
    assert report.overall_satisfaction_rate.denominator == 12
    assert report.resolution_temporal_rate.point_estimate == 1.0
    assert report.exact_attachment_rate.point_estimate == 1.0
    assert report.lifecycle_transition_rate.point_estimate == 1.0
    assert report.overlap_stale_replay_rate.point_estimate == 1.0
    assert (
        report.isolation_immutability_atomicity_rate.point_estimate == 1.0
    )
    assert report.full_scope_complete is True


@pytest.mark.parametrize("measurement", MEASUREMENTS)
def test_each_lifecycle_failure_is_noncompensatory(
    measurement: str,
) -> None:
    observation = _observation().model_copy(
        update={measurement: _observed(False, measurement)}
    )

    report = evaluate_source_identity_binding_lifecycle(observation)

    assert report.status == "contradicted"
    assert report.violating_measurement_count == 1
    assert report.safety_violation_count == 1
    assert report.measurement_rates[measurement].point_estimate == 0.0
    assert report.measurement_rates[measurement].numerator == 0
    assert report.measurement_rates[measurement].denominator == 1
    assert report.full_scope_complete is False
    assert report.immutability_violation_count == int(
        measurement == "source_immutable"
    )


def test_unsupported_runtime_surfaces_are_precise_gaps() -> None:
    unsupported_names = (
        "closure_correct",
        "revocation_correct",
        "supersession_correct",
        "overlap_prevented",
        "stale_version_rejected",
        "foreign_tenant_isolated",
        "source_immutable",
        "transaction_atomic",
    )
    observation = _observation().model_copy(
        update={
            name: _unsupported("lifecycle runtime evidence not emitted")
            for name in unsupported_names
        }
    )

    report = evaluate_source_identity_binding_lifecycle(observation)

    assert report.status == "observed_with_gaps"
    assert report.observed_measurement_count == 4
    assert report.unsupported_measurement_count == 8
    assert report.runtime_support_rate.point_estimate == pytest.approx(1 / 3)
    assert report.runtime_support_rate.numerator == 4
    assert report.runtime_support_rate.denominator == 12
    assert report.violating_measurement_count == 0
    assert report.lifecycle_transition_rate is None
    assert report.unsupported_reason_counts == {
        "lifecycle runtime evidence not emitted": 8
    }


def test_completely_unsupported_runtime_does_not_fabricate_success() -> None:
    observation = _observation().model_copy(
        update={
            name: _unsupported("binding lifecycle runner not implemented")
            for name in MEASUREMENTS
        }
    )

    report = evaluate_source_identity_binding_lifecycle(observation)

    assert report.status == "observed_with_gaps"
    assert report.observed_measurement_count == 0
    assert report.unsupported_measurement_count == 12
    assert report.runtime_support_rate.point_estimate == 0.0
    assert report.overall_satisfaction_rate is None
    assert report.full_scope_complete is False


def test_evidence_reopens_raw_lifecycle_measurements_and_digest() -> None:
    observation = _observation()
    evidence = SourceIdentityBindingLifecycleEvidence(
        run_id="pytest-binding-lifecycle",
        system_version="pytest-system",
        created_at=TRANSACTION_AT.isoformat(),
        observation=observation,
        report=evaluate_source_identity_binding_lifecycle(observation),
        artifact_refs=("pytest:binding-lifecycle",),
    )

    assert (
        validate_source_identity_binding_lifecycle_artifact(
            evidence.artifact_payload()
        )
        == evidence
    )

    tampered = evidence.artifact_payload()
    tampered["observation"]["foreign_tenant_isolated"]["satisfied"] = False
    with pytest.raises(ValueError, match="report does not match"):
        validate_source_identity_binding_lifecycle_artifact(tampered)


def test_unsupported_cells_reject_fabricated_evidence() -> None:
    with pytest.raises(ValidationError, match="cannot carry fabricated evidence"):
        BindingLifecycleProofCell(
            status="unsupported",
            satisfied=True,
            unsupported_reason="not emitted",
            artifact_refs=("fabricated:evidence",),
        )


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        (
            {"closure_binding_version": 4},
            "closure binding version must immediately follow original",
        ),
        (
            {"successor_binding_version": 4},
            "successor binding version must immediately follow closure",
        ),
        (
            {
                "transition_effective_at": ORIGINAL_FROM
                - timedelta(seconds=1)
            },
            "cannot predate the original",
        ),
        (
            {"as_of_known_at": datetime(2026, 7, 17, 12, 0)},
            "as_of_known_at must be timezone-aware",
        ),
    ),
)
def test_sealed_lifecycle_scope_rejects_invalid_raw_observations(
    updates: dict,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        SourceIdentityBindingLifecycleObservation.model_validate(
            {
                **_observation().model_dump(mode="json"),
                **updates,
            }
        )


def _observation() -> SourceIdentityBindingLifecycleObservation:
    return SourceIdentityBindingLifecycleObservation(
        tenant_id=UUID("11111111-1111-4111-8111-111111111111"),
        binding_lineage_id=UUID("22222222-2222-4222-8222-222222222222"),
        source_system="jira",
        source_native_identifier="jira:acme:project:10000",
        source_surface="ENG",
        original_binding_version=1,
        closure_binding_version=2,
        successor_binding_version=3,
        original_valid_from=ORIGINAL_FROM,
        transition_effective_at=EFFECTIVE_AT,
        transaction_at=TRANSACTION_AT,
        as_of_valid_at=EFFECTIVE_AT - timedelta(seconds=1),
        as_of_known_at=TRANSACTION_AT,
        source_observation_ref="observation:pytest-jira-project",
        **{
            name: _observed(True, name)
            for name in MEASUREMENTS
        },
        artifact_refs=("pytest:binding-lifecycle-observation",),
    )


def _observed(satisfied: bool, name: str) -> BindingLifecycleProofCell:
    return BindingLifecycleProofCell(
        status="observed",
        satisfied=satisfied,
        artifact_refs=(f"pytest:{name}",),
    )


def _unsupported(reason: str) -> BindingLifecycleProofCell:
    return BindingLifecycleProofCell(
        status="unsupported",
        unsupported_reason=reason,
    )
