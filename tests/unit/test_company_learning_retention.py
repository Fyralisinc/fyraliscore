from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    HardSafetyIncidentClass,
)
from lib.evaluation.company_learning_retention import (
    RetentionBehavior,
    RetentionCaseSpec,
    RetentionHorizon,
    RetentionObservation,
    RetentionRunSpec,
    evaluate_company_learning_retention,
)


HORIZONS = (
    RetentionHorizon(cycle_count=0, restart_count=0),
    RetentionHorizon(cycle_count=4, restart_count=1),
    RetentionHorizon(cycle_count=16, restart_count=2),
)
FINAL_HORIZON = (HORIZONS[-1],)
SAFE_FATES = (
    ConsumerTerminalFate.REVIEW,
    ConsumerTerminalFate.ABSTAINED,
    ConsumerTerminalFate.REJECTED,
    ConsumerTerminalFate.NO_ADMISSION,
)


def test_retention_report_measures_full_survival_across_horizons() -> None:
    spec = _spec()
    observations = _observations(spec)

    report = evaluate_company_learning_retention(
        spec=spec,
        observations=observations,
        artifact_refs=("pytest:retention-report",),
    )

    assert report.status == "observed"
    assert report.expected_observation_count == len(observations)
    assert report.exact_retention_rate == 1.0
    assert report.variant_retention_rate == 1.0
    assert report.corrected_retention_rate == 1.0
    assert report.overall_forgetting_rate == 0.0
    assert report.restart_survival_rate == 1.0
    assert report.correction_authority_rate == 1.0
    assert report.negative_control_safety_rate == 1.0
    assert report.collision_control_safety_rate == 1.0
    assert report.source_immutability_rate == 1.0
    assert report.model_consistency_rate == 1.0
    assert report.evidence_lineage_consistency_rate == 1.0
    assert report.retention_horizon_auc == 1.0
    assert [row.cycle_count for row in report.horizon_metrics] == [0, 4, 16]


def test_retention_report_exposes_continuous_forgetting_and_safety_regression() -> None:
    spec = _spec()
    observations = list(_observations(spec))
    variant_index = next(
        index
        for index, row in enumerate(observations)
        if row.case_id == "variant" and row.horizon.cycle_count == 16
    )
    correction_index = next(
        index for index, row in enumerate(observations) if row.case_id == "correction"
    )
    observations[variant_index] = observations[variant_index].model_copy(
        update={
            "consumer_fate": ConsumerTerminalFate.REVIEW,
            "observed_ref": None,
            "candidate_authorized": False,
        }
    )
    observations[correction_index] = observations[correction_index].model_copy(
        update={
            "unsafe_globalization": True,
            "correction_authoritative": False,
            "observed_safety_incidents": frozenset(
                {HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED}
            ),
        }
    )

    report = evaluate_company_learning_retention(
        spec=spec,
        observations=tuple(observations),
        artifact_refs=("pytest:retention-degradation",),
    )

    assert report.status == "contradicted"
    assert report.variant_retention_rate == pytest.approx(2 / 3)
    assert report.corrected_retention_rate == 0.0
    assert report.correction_authority_rate == 0.0
    assert report.unsafe_globalization_rate == pytest.approx(1 / 9)
    assert report.overall_forgetting_rate == pytest.approx(2 / 7)
    assert report.hard_safety_incident_rate == pytest.approx(1 / 9)
    final = next(row for row in report.horizon_metrics if row.cycle_count == 16)
    assert final.positive_retention_rate == pytest.approx(1 / 3)
    assert final.forgetting_rate == pytest.approx(2 / 3)


def test_retention_report_rejects_survivor_only_observations() -> None:
    spec = _spec()
    observations = _observations(spec)

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_company_learning_retention(
            spec=spec,
            observations=observations[:-1],
            artifact_refs=("pytest:missing-retention-row",),
        )


def _spec() -> RetentionRunSpec:
    exact_ref = CanonicalEntityRef(type="customer", id="exact-customer")
    variant_ref = CanonicalEntityRef(type="customer", id="variant-customer")
    corrected_ref = CanonicalEntityRef(type="customer", id="corrected-customer")
    return RetentionRunSpec(
        run_id="pytest-retention",
        system_version="pytest",
        created_at=datetime.now(timezone.utc).isoformat(),
        cases=(
            RetentionCaseSpec(
                case_id="exact",
                behavior=RetentionBehavior.EXACT_ALIAS,
                family="exact_alias_positive",
                expected_ref=exact_ref,
                horizons=HORIZONS,
                allowed_terminal_fates=(
                    ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                ),
            ),
            RetentionCaseSpec(
                case_id="variant",
                behavior=RetentionBehavior.VARIANT_ALIAS,
                family="governed_variant_positive",
                expected_ref=variant_ref,
                horizons=HORIZONS,
                allowed_terminal_fates=(
                    ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                ),
                candidate_authorization_required=True,
            ),
            RetentionCaseSpec(
                case_id="correction",
                behavior=RetentionBehavior.CORRECTED_ALIAS,
                family="authoritative_exact_correction",
                expected_ref=corrected_ref,
                horizons=FINAL_HORIZON,
                allowed_terminal_fates=(
                    ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                ),
                correction_authority_required=True,
            ),
            RetentionCaseSpec(
                case_id="negative",
                behavior=RetentionBehavior.NEGATIVE_CONTROL,
                family="contextual_phrase_negative",
                horizons=FINAL_HORIZON,
                allowed_terminal_fates=SAFE_FATES,
            ),
            RetentionCaseSpec(
                case_id="collision",
                behavior=RetentionBehavior.COLLISION_CONTROL,
                family="same_type_acronym_collision",
                horizons=FINAL_HORIZON,
                allowed_terminal_fates=SAFE_FATES,
            ),
        ),
        artifact_refs=("pytest:retention-spec",),
    )


def _observations(
    spec: RetentionRunSpec,
) -> tuple[RetentionObservation, ...]:
    rows: list[RetentionObservation] = []
    for case in spec.cases:
        for horizon in case.horizons:
            positive = case.expected_ref is not None
            rows.append(
                RetentionObservation(
                    case_id=case.case_id,
                    horizon=horizon,
                    intervening_learning_count=horizon.cycle_count,
                    consumer_fate=(
                        ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
                        if positive
                        else ConsumerTerminalFate.REVIEW
                    ),
                    observed_ref=case.expected_ref,
                    candidate_authorized=(
                        True
                        if case.behavior is RetentionBehavior.VARIANT_ALIAS
                        else None
                    ),
                    correction_authoritative=(
                        True
                        if case.behavior is RetentionBehavior.CORRECTED_ALIAS
                        else None
                    ),
                    source_observation_immutable=True,
                    models_consistent=True,
                    evidence_lineage_consistent=True,
                    artifact_refs=(
                        f"pytest:{case.case_id}:{horizon.cycle_count}",
                    ),
                )
            )
    return tuple(rows)
