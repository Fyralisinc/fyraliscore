from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    HardSafetyIncidentClass,
)
from lib.evaluation.company_learning_variant_collisions import (
    EntityLifecycle,
    HeldOutVariantCollisionCase,
    VariantCollisionArmObservation,
    VariantCollisionDecisionBasis,
    VariantCollisionFamily,
    VariantCollisionPairObservation,
    VariantCollisionTargetRole,
    build_variant_collision_population,
    evaluate_variant_collision_population,
    load_variant_collision_population,
)


FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_variant_collision_population_v1.jsonl"
)
_SAFE_FATES = (
    ConsumerTerminalFate.REVIEW,
    ConsumerTerminalFate.ABSTAINED,
    ConsumerTerminalFate.REJECTED,
    ConsumerTerminalFate.NO_ADMISSION,
)


def test_committed_collision_registry_is_deterministic_and_complete() -> None:
    generated = build_variant_collision_population()
    fixture = load_variant_collision_population(FIXTURE)

    assert fixture == generated
    assert fixture.digest == generated.digest
    assert len(fixture.cases) == 16
    assert len({case.digest for case in fixture.cases}) == 16
    assert {case.collision_family for case in fixture.cases} == set(
        VariantCollisionFamily
    )
    assert fixture.cases[0].entity_type_relation == "same_type"
    assert fixture.cases[2].entity_type_relation == "cross_type"
    assert fixture.cases[6].learned_surface != (
        fixture.cases[6].collision_surface
    )
    assert fixture.cases[12].learned_lifecycle is (
        EntityLifecycle.ARCHIVED
    )
    assert fixture.cases[13].learned_lifecycle is (
        EntityLifecycle.INACTIVE
    )
    assert all(
        set(case.allowed_safe_fates) == set(_SAFE_FATES)
        for case in fixture.cases
    )


def test_safe_collision_outcomes_preserve_uncertainty_continuously() -> None:
    population = build_variant_collision_population()
    report = evaluate_variant_collision_population(
        population=population,
        observations=_safe_observations(population),
    )

    assert report.status == "observed"
    assert report.pair_count == 16
    assert report.observed_pair_count == 16
    assert report.unsupported_case_count == 0
    assert report.runtime_support_rate.point_estimate == 1.0
    assert report.runtime_support_rate.lower_95 < 1.0
    assert report.adaptive_safe_containment_rate.point_estimate == 1.0
    assert report.frozen_safe_containment_rate.point_estimate == 1.0
    assert report.adaptive_unsafe_rate.point_estimate == 0.0
    assert report.frozen_unsafe_rate.point_estimate == 0.0
    assert report.adaptive_unsafe_resolution_rate.point_estimate == 0.0
    assert report.frozen_unsafe_resolution_rate.point_estimate == 0.0
    assert report.adaptive_authoritative_resolution_rate.point_estimate == 0.0
    assert report.frozen_authoritative_resolution_rate.point_estimate == 0.0
    assert report.adaptive_learned_promotion_rate.point_estimate == 0.0
    assert report.frozen_learned_promotion_rate.point_estimate == 0.0
    assert report.adaptive_candidate_visibility_rate.point_estimate == 1.0
    assert report.frozen_candidate_visibility_rate.point_estimate == 1.0
    assert (
        report.adaptive_none_of_above_availability_rate.point_estimate
        == 1.0
    )
    assert (
        report.frozen_none_of_above_availability_rate.point_estimate
        == 1.0
    )
    assert report.adaptive_wrong_model_rate.point_estimate == 0.0
    assert report.frozen_wrong_model_rate.point_estimate == 0.0
    assert report.adaptive_wrong_model_count == 0
    assert report.frozen_wrong_model_count == 0
    assert report.adaptive_source_immutability_rate.point_estimate == 1.0
    assert report.frozen_source_immutability_rate.point_estimate == 1.0
    assert report.safety_incident_count == 0
    assert report.adaptive_incident_class_counts == {}
    assert report.frozen_incident_class_counts == {}
    assert report.adaptive_outcome_counts == {
        fate.value: 4 for fate in _SAFE_FATES
    }
    assert report.frozen_outcome_counts == {
        fate.value: 4 for fate in _SAFE_FATES
    }
    assert report.strata_counts["collision_family"] == {
        family.value: 2 for family in VariantCollisionFamily
    }
    assert report.strata_counts["learned_entity_type"] == {
        "customer": 4,
        "project": 4,
        "system": 4,
        "team": 4,
    }
    assert all(
        stratum.sealed_case_count == 2
        and stratum.observed_case_count == 2
        and stratum.adaptive_safe_containment_rate is not None
        and stratum.adaptive_safe_containment_rate.point_estimate == 1.0
        for stratum in report.stratum_reports[
            "collision_family"
        ].values()
    )


def test_autonomous_resolution_and_promotion_are_typed_unsafe_evidence() -> (
    None
):
    population = build_variant_collision_population()
    observations = list(_safe_observations(population))
    collision = population.cases[8]
    safe_pair = observations[8]
    assert safe_pair.frozen is not None
    learned_ref, conflicting_ref = _collision_refs(collision)
    observations[8] = VariantCollisionPairObservation(
        case_id=collision.case_id,
        adaptive=VariantCollisionArmObservation(
            arm=CorrectiveMemoryArm.ADAPTIVE,
            consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            resolved_entity_ref=learned_ref,
            decision_basis=(
                VariantCollisionDecisionBasis.LEARNED_AMBIGUOUS_VARIANT
            ),
            resolved_target_role=VariantCollisionTargetRole.LEARNED,
            learned_alias_promoted=True,
            candidate_set_digest=canonical_sha256(
                [learned_ref.model_dump(mode="json")]
            ),
            candidate_set_size=1,
            visible_candidate_refs=(learned_ref,),
            learned_candidate_ref=learned_ref,
            conflicting_candidate_ref=conflicting_ref,
            both_colliding_candidates_visible=False,
            none_of_above_available=False,
            wrong_model_count=1,
            source_observation_immutable=False,
            artifact_refs=("pytest:unsafe-collision:adaptive",),
        ),
        frozen=safe_pair.frozen,
    )

    report = evaluate_variant_collision_population(
        population=population,
        observations=tuple(observations),
    )

    assert report.status == "contradicted"
    assert report.adaptive_safe_containment_rate.point_estimate == pytest.approx(
        15 / 16
    )
    assert report.adaptive_unsafe_rate.point_estimate == pytest.approx(
        1 / 16
    )
    assert (
        report.adaptive_unsafe_resolution_rate.point_estimate
        == pytest.approx(1 / 16)
    )
    assert report.adaptive_learned_promotion_rate.point_estimate == (
        pytest.approx(1 / 16)
    )
    assert report.adaptive_candidate_visibility_rate.point_estimate == (
        pytest.approx(15 / 16)
    )
    assert (
        report.adaptive_none_of_above_availability_rate.point_estimate
        == pytest.approx(15 / 16)
    )
    assert report.adaptive_wrong_model_rate.point_estimate == pytest.approx(
        1 / 16
    )
    assert report.adaptive_wrong_model_count == 1
    assert report.adaptive_source_immutability_rate.point_estimate == (
        pytest.approx(15 / 16)
    )
    assert report.frozen_unsafe_rate.point_estimate == 0.0
    assert report.adaptive_incident_class_counts[
        HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION.value
    ] == 1
    assert report.adaptive_incident_class_counts[
        HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED.value
    ] == 1
    assert report.adaptive_incident_class_counts[
        HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED.value
    ] == 1
    contextual = report.stratum_reports["collision_family"][
        VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME.value
    ]
    assert contextual.adaptive_unsafe_rate is not None
    assert contextual.adaptive_unsafe_rate.point_estimate == 0.5


def test_authenticated_source_identifier_can_safely_resolve_conflict() -> None:
    population = build_variant_collision_population()
    observations = list(_safe_observations(population))
    collision = population.cases[10]
    safe_pair = observations[10]
    assert collision.conflicting_source_native_id is not None
    assert safe_pair.frozen is not None
    learned_ref, conflicting_ref = _collision_refs(collision)
    visible_refs = (learned_ref, conflicting_ref)
    observations[10] = VariantCollisionPairObservation(
        case_id=collision.case_id,
        adaptive=VariantCollisionArmObservation(
            arm=CorrectiveMemoryArm.ADAPTIVE,
            consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            resolved_entity_ref=conflicting_ref,
            decision_basis=(
                VariantCollisionDecisionBasis.AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER
            ),
            resolved_target_role=VariantCollisionTargetRole.CONFLICTING,
            decisive_source_native_id=(
                collision.conflicting_source_native_id
            ),
            learned_alias_promoted=False,
            candidate_set_digest=canonical_sha256(
                [ref.model_dump(mode="json") for ref in visible_refs]
            ),
            candidate_set_size=2,
            visible_candidate_refs=visible_refs,
            learned_candidate_ref=learned_ref,
            conflicting_candidate_ref=conflicting_ref,
            both_colliding_candidates_visible=True,
            none_of_above_available=True,
            wrong_model_count=0,
            source_observation_immutable=True,
            artifact_refs=("pytest:source-authoritative-resolution",),
        ),
        frozen=safe_pair.frozen,
    )

    report = evaluate_variant_collision_population(
        population=population,
        observations=tuple(observations),
    )

    assert report.status == "observed"
    assert report.safety_incident_count == 0
    assert report.adaptive_safe_containment_rate.point_estimate == 1.0
    assert report.adaptive_unsafe_resolution_rate.point_estimate == 0.0
    assert (
        report.adaptive_authoritative_resolution_rate.point_estimate
        == pytest.approx(1 / 16)
    )


def test_unsupported_collision_cases_remain_in_sealed_strata() -> None:
    population = build_variant_collision_population()
    observations = list(_safe_observations(population))
    for index in (14, 15):
        observations[index] = VariantCollisionPairObservation(
            case_id=population.cases[index].case_id,
            execution_status="unsupported",
            unsupported_reason="historical lifecycle unavailable",
        )

    report = evaluate_variant_collision_population(
        population=population,
        observations=tuple(observations),
    )

    assert report.status == "observed_with_gaps"
    assert report.pair_count == 16
    assert report.observed_pair_count == 14
    assert report.unsupported_case_count == 2
    assert report.runtime_support_rate.point_estimate == 14 / 16
    assert report.unsupported_reason_counts == {
        "historical lifecycle unavailable": 2
    }
    assert report.unsupported_strata_counts["collision_family"] == {
        VariantCollisionFamily.HISTORICAL_NAME_REUSE.value: 2
    }
    historical = report.stratum_reports["collision_family"][
        VariantCollisionFamily.HISTORICAL_NAME_REUSE.value
    ]
    assert historical.sealed_case_count == 2
    assert historical.observed_case_count == 0
    assert historical.unsupported_case_count == 2
    assert historical.adaptive_safe_containment_rate is None


def test_collision_evaluator_rejects_selective_reruns_and_bad_gold() -> None:
    population = build_variant_collision_population()
    observations = _safe_observations(population)

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_variant_collision_population(
            population=population,
            observations=observations[:-1],
        )
    with pytest.raises(ValueError, match="unique by case"):
        evaluate_variant_collision_population(
            population=population,
            observations=(*observations[:-1], observations[0]),
        )

    payload = population.cases[2].model_dump(mode="json")
    payload["conflicting_entity_type"] = payload["learned_entity_type"]
    with pytest.raises(ValidationError, match="distinct entity types"):
        HeldOutVariantCollisionCase.model_validate(payload)


def _safe_observations(
    population,
) -> tuple[VariantCollisionPairObservation, ...]:
    return tuple(
        VariantCollisionPairObservation(
            case_id=case.case_id,
            adaptive=_safe_arm(
                arm=CorrectiveMemoryArm.ADAPTIVE,
                fate=_SAFE_FATES[index % len(_SAFE_FATES)],
                case=case,
            ),
            frozen=_safe_arm(
                arm=CorrectiveMemoryArm.FROZEN,
                fate=_SAFE_FATES[(index + 1) % len(_SAFE_FATES)],
                case=case,
            ),
        )
        for index, case in enumerate(population.cases)
    )


def _safe_arm(
    *,
    arm: CorrectiveMemoryArm,
    fate: ConsumerTerminalFate,
    case,
) -> VariantCollisionArmObservation:
    learned_ref, conflicting_ref = _collision_refs(case)
    visible_refs = (learned_ref, conflicting_ref)
    return VariantCollisionArmObservation(
        arm=arm,
        consumer_fate=fate,
        decision_basis=VariantCollisionDecisionBasis.UNRESOLVED_COLLISION,
        learned_alias_promoted=False,
        candidate_set_digest=canonical_sha256(
            [ref.model_dump(mode="json") for ref in visible_refs]
        ),
        candidate_set_size=2,
        visible_candidate_refs=visible_refs,
        learned_candidate_ref=learned_ref,
        conflicting_candidate_ref=conflicting_ref,
        both_colliding_candidates_visible=True,
        none_of_above_available=True,
        wrong_model_count=0,
        source_observation_immutable=True,
        artifact_refs=(f"pytest:collision:{case.case_id}:{arm.value}",),
    )


def _collision_refs(case) -> tuple[CanonicalEntityRef, CanonicalEntityRef]:
    return (
        CanonicalEntityRef(
            type=case.learned_entity_type,
            id=f"{case.case_id}:learned",
        ),
        CanonicalEntityRef(
            type=case.conflicting_entity_type,
            id=f"{case.case_id}:conflicting",
        ),
    )
