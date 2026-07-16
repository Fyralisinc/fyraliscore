from __future__ import annotations

import pytest

from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SEALED_ACTIVE_SURFACE_CLAIMS,
    SourceSalienceObservation,
    StructuredIdentitySurfaceObservation,
    evaluate_active_learning_surfaces,
    validate_active_learning_surfaces_artifact,
)


def test_active_surface_report_is_continuous_and_noncompensatory() -> None:
    report = evaluate_active_learning_surfaces(
        identity_observations=_safe_identity(),
        salience_observations=_safe_salience(),
    )

    assert report.status == "observed"
    assert report.structured_identity.observed_case_count == 6
    assert report.structured_identity.violating_case_count == 0
    assert report.structured_identity.runtime_support_rate.point_estimate == 1.0
    assert report.structured_identity.handler_non_authority_rate.point_estimate == 1.0
    assert report.structured_identity.forged_text_rejection_rate.point_estimate == 1.0
    assert report.source_salience.observed_case_count == 5
    assert report.source_salience.violating_case_count == 0
    assert report.source_salience.salience_direction_rate.point_estimate == 1.0
    assert (
        report.source_salience.canonical_truth_immutability_rate.point_estimate == 1.0
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("handler_created_authority", True),
        ("ingest_created_authority", True),
        ("forged_text_resolved", True),
        ("missing_binding_authoritative", True),
        ("cross_source_leak", True),
        ("cross_tenant_leak", True),
        ("source_observation_immutable", False),
    ),
)
def test_each_identity_safety_failure_contradicts(
    field: str,
    value: bool,
) -> None:
    rows = list(_safe_identity())
    rows[0] = rows[0].model_copy(update={field: value})

    report = evaluate_active_learning_surfaces(
        identity_observations=tuple(rows),
        salience_observations=_safe_salience(),
    )

    assert report.status == "contradicted"
    assert report.structured_identity.violating_case_count == 1


@pytest.mark.parametrize(
    ("case_id", "updates"),
    (
        ("settled_useful", {"learned_salience": 1.0}),
        ("corrected", {"learned_salience": 1.1}),
        ("pending", {"credit_observed": True}),
        ("foreign_tenant", {"foreign_tenant_learned": True}),
        ("profile_load", {"canonical_truth_immutable": False}),
        ("profile_load", {"grounding_truth_immutable": False}),
    ),
)
def test_each_salience_safety_failure_contradicts(
    case_id: str,
    updates: dict,
) -> None:
    rows = list(_safe_salience())
    index = next(i for i, row in enumerate(rows) if row.case_id == case_id)
    rows[index] = rows[index].model_copy(update=updates)

    report = evaluate_active_learning_surfaces(
        identity_observations=_safe_identity(),
        salience_observations=tuple(rows),
    )

    assert report.status == "contradicted"
    assert report.source_salience.violating_case_count >= 1


def test_unsupported_surface_is_a_continuous_support_failure() -> None:
    identity = list(_safe_identity())
    identity[1] = StructuredIdentitySurfaceObservation(
        case_id="linear_issue_bundle",
        execution_status="unsupported",
        unsupported_reason="linear runtime unavailable",
    )

    report = evaluate_active_learning_surfaces(
        identity_observations=tuple(identity),
        salience_observations=_safe_salience(),
    )

    assert report.status == "contradicted"
    assert report.structured_identity.runtime_support_rate.point_estimate == (
        5 / 6
    )
    assert report.structured_identity.unsupported_reason_counts == {
        "linear runtime unavailable": 1
    }


def test_selective_surface_reporting_is_rejected() -> None:
    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_active_learning_surfaces(
            identity_observations=_safe_identity()[:1],
            salience_observations=_safe_salience(),
        )


def test_evidence_envelope_reopens_raw_observations_and_digest() -> None:
    identity = _safe_identity()
    salience = _safe_salience()
    evidence = ActiveLearningSurfacesEvidence(
        run_id="pytest-active-surfaces",
        system_version="pytest-system",
        created_at="2026-07-16T00:00:00+00:00",
        identity_observations=identity,
        salience_observations=salience,
        report=evaluate_active_learning_surfaces(
            identity_observations=identity,
            salience_observations=salience,
        ),
        artifact_refs=("pytest:active-surfaces",),
    )

    assert (
        validate_active_learning_surfaces_artifact(evidence.artifact_payload())
        == evidence
    )

    tampered = evidence.artifact_payload()
    tampered["identity_observations"][0]["forged_text_resolved"] = True
    with pytest.raises(ValueError, match="report does not match"):
        validate_active_learning_surfaces_artifact(tampered)


def _safe_identity() -> tuple[StructuredIdentitySurfaceObservation, ...]:
    return tuple(
        StructuredIdentitySurfaceObservation(
            case_id=source,
            expected_claims=SEALED_ACTIVE_SURFACE_CLAIMS[source],
            observed_claims=SEALED_ACTIVE_SURFACE_CLAIMS[source],
            claim_emitted=True,
            claim_preserved=True,
            preexisting_binding_attached=True,
            handler_created_authority=False,
            ingest_created_authority=False,
            forged_text_resolved=False,
            missing_binding_authoritative=False,
            cross_source_leak=False,
            cross_tenant_leak=False,
            source_observation_immutable=True,
            artifact_refs=(f"pytest:{source}",),
        )
        for source in (
            "jira_project",
            "linear_issue_bundle",
            "google_drive_file",
            "google_drive_comment",
            "google_drive_revision",
            "gmail_thread",
        )
    )
def _safe_salience() -> tuple[SourceSalienceObservation, ...]:
    values = {
        "settled_useful": (1.0, 1.2, True, False),
        "corrected": (1.0, 0.8, False, False),
        "pending": (1.0, 1.0, False, False),
        "foreign_tenant": (1.0, 1.0, False, False),
        "profile_load": (1.0, 1.0, False, False),
    }
    return tuple(
        SourceSalienceObservation(
            case_id=case_id,
            baseline_salience=baseline,
            learned_salience=learned,
            credit_observed=credit,
            foreign_tenant_learned=foreign,
            canonical_truth_immutable=True,
            grounding_truth_immutable=True,
            artifact_refs=(f"pytest:{case_id}",),
        )
        for case_id, (baseline, learned, credit, foreign) in values.items()
    )
