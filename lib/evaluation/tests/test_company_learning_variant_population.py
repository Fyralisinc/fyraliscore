from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmResult,
    CorrectiveMemoryExperimentReport,
    CorrectiveMemoryExperimentSpec,
    HardSafetyIncidentClass,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
    evaluate_corrective_memory_experiment,
)
from lib.evaluation.company_learning_variant_population import (
    CompanyLearningVariantPopulationEvidence,
    HeldOutVariantAliasPopulation,
    VARIANT_ALIAS_SCENARIO_ID,
    VariantAliasArmMechanismEvidence,
    VariantAliasCaseAssignment,
    VariantAliasExecutionObservation,
    VariantAliasFamily,
    VariantAliasMechanismMetrics,
    VariantAliasPairMechanismEvidence,
    build_variant_alias_population,
    evaluate_variant_alias_population,
    load_variant_alias_population,
    validate_variant_population_evidence_artifact,
)


FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_variant_alias_population_v1.jsonl"
)


def test_committed_variant_registry_is_deterministic_balanced_and_rankable() -> (
    None
):
    generated = build_variant_alias_population()
    fixture = load_variant_alias_population(FIXTURE)

    assert fixture == generated
    assert fixture.digest == generated.digest
    assert len(fixture.cases) == 24
    assert len({case.digest for case in fixture.cases}) == 24
    assert fixture.cases[0].recurrence_alias_surface == "NBI"
    assert fixture.cases[4].training_alias_surface == "Atlas.Pay"
    assert fixture.cases[-4].recurrence_alias_surface == "Acme's"
    assert {case.entity_type for case in fixture.cases} == {
        "customer",
        "project",
        "team",
        "system",
    }
    assert {case.variant_family for case in fixture.cases} == set(
        VariantAliasFamily
    )


def test_full_variant_population_has_continuous_overall_and_stratum_reports() -> (
    None
):
    population = build_variant_alias_population()
    report = _experiment_report(population)
    observations = tuple(
        VariantAliasExecutionObservation(case_id=case.case_id)
        for case in population.cases
    )

    first = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=500,
    )
    second = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=500,
    )

    assert first == second
    assert first.pair_count == 24
    assert first.observed_pair_count == 24
    assert first.unsupported_case_count == 0
    assert first.complete_population is True
    assert first.adaptive_correctness.point_estimate == 1.0
    assert first.adaptive_correctness.lower_95 < 1.0
    assert first.frozen_correctness.point_estimate == 0.0
    assert first.adaptive_minus_frozen_correctness.point_estimate == 1.0
    assert first.adaptive_minus_frozen_correctness.lower_95 == 1.0
    assert first.adaptive_unsafe_rate.point_estimate == 0.0
    assert first.frozen_unsafe_rate.point_estimate == 0.0
    assert first.strata_counts["entity_type"] == {
        "customer": 6,
        "project": 6,
        "system": 6,
        "team": 6,
    }
    assert first.strata_counts["variant_family"] == {
        family.value: 4 for family in VariantAliasFamily
    }
    assert all(
        stratum.sealed_case_count == 4
        and stratum.observed_case_count == 4
        and stratum.adaptive_correctness is not None
        and stratum.adaptive_correctness.sample_size == 4
        for stratum in first.family_reports.values()
    )
    assert all(
        stratum.sealed_case_count == 6
        and stratum.observed_case_count == 6
        and stratum.adaptive_correctness is not None
        and stratum.adaptive_correctness.sample_size == 6
        for stratum in first.entity_type_reports.values()
    )


def test_unsupported_cases_remain_in_registry_and_strata() -> None:
    population = build_variant_alias_population()
    observed_cases = population.cases[:20]
    report = _experiment_report(
        HeldOutVariantAliasPopulation(cases=population.cases),
        observed_case_ids={case.case_id for case in observed_cases},
    )
    observations = tuple(
        (
            VariantAliasExecutionObservation(case_id=case.case_id)
            if case in observed_cases
            else VariantAliasExecutionObservation(
                case_id=case.case_id,
                execution_status="unsupported",
                unsupported_reason="source mention locator unsupported",
            )
        )
        for case in population.cases
    )

    result = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=200,
    )

    assert result.pair_count == 24
    assert result.observed_pair_count == 20
    assert result.unsupported_case_count == 4
    assert result.unsupported_reason_counts == {
        "source mention locator unsupported": 4
    }
    possessive = result.family_reports[
        VariantAliasFamily.POSSESSIVE_OR_PLURAL.value
    ]
    assert possessive.sealed_case_count == 4
    assert possessive.observed_case_count == 0
    assert possessive.unsupported_case_count == 4
    assert possessive.adaptive_correctness is None
    assert result.unsupported_strata_counts["variant_family"] == {
        VariantAliasFamily.POSSESSIVE_OR_PLURAL.value: 4
    }


def test_variant_safety_incidents_remain_visible_in_continuous_rates() -> None:
    population = build_variant_alias_population()
    unsafe_case_id = population.cases[0].case_id
    report = _experiment_report(
        population,
        unsafe_case_id=unsafe_case_id,
    )
    observations = tuple(
        VariantAliasExecutionObservation(case_id=case.case_id)
        for case in population.cases
    )

    result = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=200,
    )

    assert report.status == "contradicted"
    assert len(report.incidents) == 1
    assert result.adaptive_unsafe_rate.point_estimate == pytest.approx(
        1 / 24
    )
    assert result.frozen_unsafe_rate.point_estimate == 0.0
    acronym = result.family_reports[
        VariantAliasFamily.ACRONYM_FROM_LONG_FORM.value
    ]
    assert acronym.adaptive_unsafe_rate is not None
    assert acronym.adaptive_unsafe_rate.point_estimate == 0.25


def test_variant_population_rejects_selective_reruns_and_wrong_report_kind() -> (
    None
):
    population = build_variant_alias_population()
    report = _experiment_report(population)
    observations = tuple(
        VariantAliasExecutionObservation(case_id=case.case_id)
        for case in population.cases
    )

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_variant_alias_population(
            population=population,
            experiment_report=report,
            observations=observations[:-1],
        )

    with pytest.raises(ValueError, match="unique by case"):
        evaluate_variant_alias_population(
            population=population,
            experiment_report=report,
            observations=(*observations[:-1], observations[0]),
        )

    wrong_kind = _experiment_report(
        population,
        case_kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
    )
    with pytest.raises(ValueError, match="not sealed to variant-alias"):
        evaluate_variant_alias_population(
            population=population,
            experiment_report=wrong_kind,
            observations=observations,
        )

    tampered = report.model_copy(
        update={"pair_results_digest": "f" * 64}
    )
    with pytest.raises(ValueError, match="pair digest mismatch"):
        evaluate_variant_alias_population(
            population=population,
            experiment_report=tampered,
            observations=observations,
        )


def test_variant_evidence_envelope_cross_binds_full_population() -> None:
    population = build_variant_alias_population()
    report = _experiment_report(population)
    observations = tuple(
        VariantAliasExecutionObservation(case_id=case.case_id)
        for case in population.cases
    )
    population_report = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=200,
    )
    assignments = _assignments(population, report)
    mechanisms = _mechanisms(report, assignments)
    evidence = CompanyLearningVariantPopulationEvidence(
        created_at="2026-07-16T00:00:00+00:00",
        run_id=report.run_id,
        system_version=report.system_version,
        execution_mode="full",
        selection_policy="full_registry_once_no_selective_reruns",
        registry_path=str(FIXTURE),
        registry_population=population,
        registry_population_digest=population.digest,
        selected_case_ids=tuple(case.case_id for case in population.cases),
        assignments=assignments,
        observations=observations,
        raw_pairs=report.pairs,
        experiment_report=report,
        population_report=population_report,
        mechanism_pairs=mechanisms,
        mechanism_metrics=_mechanism_metrics(population),
        artifact_refs=("pytest:variant-evidence",),
    )

    assert validate_variant_population_evidence_artifact(
        evidence.artifact_payload()
    ) == evidence

    tampered = evidence.artifact_payload()
    tampered["selected_case_ids"] = list(
        reversed(tampered["selected_case_ids"])
    )
    tampered["evidence_digest"] = canonical_sha256(
        {
            key: value
            for key, value in tampered.items()
            if key != "evidence_digest"
        }
    )
    with pytest.raises(ValidationError, match="deterministic registry prefix"):
        validate_variant_population_evidence_artifact(tampered)


def _experiment_report(
    population: HeldOutVariantAliasPopulation,
    *,
    observed_case_ids: set[str] | None = None,
    unsafe_case_id: str | None = None,
    case_kind: RecurrenceCaseKind = (
        RecurrenceCaseKind.VARIANT_ALIAS_POSITIVE
    ),
) -> CorrectiveMemoryExperimentReport:
    selected = tuple(
        case
        for case in population.cases
        if observed_case_ids is None or case.case_id in observed_case_ids
    )
    sealed_cases = []
    pairs = []
    for case in selected:
        adaptive_tenant_id = uuid4()
        frozen_tenant_id = uuid4()
        adaptive_target_id = uuid4()
        frozen_target_id = uuid4()
        canonical_type = {
            "customer": "customer",
            "project": "resource",
            "team": "actor",
            "system": "resource",
        }[case.entity_type]
        adaptive_ref = CanonicalEntityRef(
            type=canonical_type,
            id=str(adaptive_target_id),
        )
        frozen_ref = CanonicalEntityRef(
            type=canonical_type,
            id=str(frozen_target_id),
        )
        sealed_cases.append(
            SealedRecurrenceCase(
                case_id=case.case_id,
                case_version=case.case_version,
                kind=case_kind,
                alias_surface=case.recurrence_alias_surface,
                source_text_digest=canonical_sha256(case.recurrence_text),
                context_digest=case.digest,
                adaptive_expectation=SealedArmExpectation(
                    tenant_id=adaptive_tenant_id,
                    allowed_consumer_fates=(
                        ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                    ),
                    expected_entity_ref=adaptive_ref,
                    expected_model_count=0,
                    autonomous_resolution_permitted=True,
                ),
                frozen_expectation=SealedArmExpectation(
                    tenant_id=frozen_tenant_id,
                    allowed_consumer_fates=(
                        ConsumerTerminalFate.REVIEW,
                        ConsumerTerminalFate.ABSTAINED,
                    ),
                    expected_entity_ref=frozen_ref,
                    expected_model_count=0,
                    autonomous_resolution_permitted=False,
                ),
                artifact_refs=(f"pytest:variant-case:{case.case_id}",),
            )
        )
        pairs.append(
            PairedRecurrenceResult(
                case_id=case.case_id,
                adaptive=_arm_result(
                    case_id=case.case_id,
                    arm=CorrectiveMemoryArm.ADAPTIVE,
                    tenant_id=adaptive_tenant_id,
                    resolved_ref=adaptive_ref,
                    target_id=adaptive_target_id,
                    unsafe=case.case_id == unsafe_case_id,
                ),
                frozen=_arm_result(
                    case_id=case.case_id,
                    arm=CorrectiveMemoryArm.FROZEN,
                    tenant_id=frozen_tenant_id,
                    resolved_ref=None,
                    target_id=frozen_target_id,
                ),
                artifact_refs=(f"pytest:variant-pair:{case.case_id}",),
            )
        )
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id="pytest-variant-population",
        run_id="pytest-variant-population",
        system_version="pytest",
        created_at="2026-07-16T00:00:00+00:00",
        scenario_ids=(VARIANT_ALIAS_SCENARIO_ID,),
        company_foundation_digest=canonical_sha256(
            [case.model_dump(mode="json") for case in selected]
        ),
        provider_behavior_digest=canonical_sha256("pytest-provider"),
        cases=tuple(sealed_cases),
        artifact_refs=("pytest:variant-spec",),
    )
    return evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=tuple(pairs),
        artifact_refs=("pytest:variant-report",),
    )


def _arm_result(
    *,
    case_id: str,
    arm: CorrectiveMemoryArm,
    tenant_id,
    resolved_ref: CanonicalEntityRef | None,
    target_id,
    unsafe: bool = False,
) -> CorrectiveMemoryArmResult:
    adaptive = arm is CorrectiveMemoryArm.ADAPTIVE
    return CorrectiveMemoryArmResult(
        case_id=case_id,
        arm=arm,
        tenant_id=tenant_id,
        consumer_fate=(
            ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            if adaptive
            else ConsumerTerminalFate.REVIEW
        ),
        resolved_entity_ref=resolved_ref,
        decision_source=(
            "governed_variant_alias_replay" if adaptive else "llm"
        ),
        llm_call_count=1,
        latency_ms=10.0 if adaptive else 20.0,
        estimated_cost_usd=0.0 if adaptive else 0.001,
        source_semantic_admitted=False,
        lineage=ArmLineageRefs(
            training_observation_id=uuid4(),
            recurrence_observation_id=uuid4(),
            clarification_request_id=uuid4(),
            clarification_answer_digest=canonical_sha256(
                {
                    "case_id": case_id,
                    "arm": arm.value,
                    "target_id": str(target_id),
                }
            ),
            adjudicated_alias_id=uuid4(),
            artifact_refs=(f"pytest:variant-arm:{case_id}:{arm.value}",),
        ),
        observed_safety_incidents=(
            frozenset(
                {HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED}
            )
            if unsafe
            else frozenset()
        ),
    )


def _assignments(
    population: HeldOutVariantAliasPopulation,
    report: CorrectiveMemoryExperimentReport,
) -> tuple[VariantAliasCaseAssignment, ...]:
    pair_by_case = {pair.case_id: pair for pair in report.pairs}
    assessment = {
        (row.case_id, row.arm): row for row in report.assessments
    }
    runtime_type = {
        "customer": "customer",
        "project": "resource",
        "team": "actor",
        "system": "resource",
    }
    return tuple(
        VariantAliasCaseAssignment(
            case_id=case.case_id,
            logical_entity_type=case.entity_type,
            runtime_entity_type=runtime_type[case.entity_type],
            adaptive_tenant_id=pair_by_case[case.case_id].adaptive.tenant_id,
            frozen_tenant_id=pair_by_case[case.case_id].frozen.tenant_id,
            adaptive_target_id=UUID(
                str(
                    assessment[
                        (case.case_id, CorrectiveMemoryArm.ADAPTIVE)
                    ].expected_entity_ref.id
                )
            ),
            frozen_target_id=UUID(
                str(
                    assessment[
                        (case.case_id, CorrectiveMemoryArm.FROZEN)
                    ].expected_entity_ref.id
                )
            ),
            adaptive_conflicting_id=uuid4(),
            frozen_conflicting_id=uuid4(),
        )
        for case in population.cases
    )


def _mechanisms(
    report: CorrectiveMemoryExperimentReport,
    assignments: tuple[VariantAliasCaseAssignment, ...],
) -> tuple[VariantAliasPairMechanismEvidence, ...]:
    pair_by_case = {pair.case_id: pair for pair in report.pairs}
    rows = []
    for assignment in assignments:
        pair = pair_by_case[assignment.case_id]
        adaptive_ref = pair.adaptive.resolved_entity_ref
        assert adaptive_ref is not None
        frozen_target_ref = CanonicalEntityRef(
            type=adaptive_ref.type,
            id=str(assignment.frozen_target_id),
        )
        rows.append(
            VariantAliasPairMechanismEvidence(
                case_id=assignment.case_id,
                adaptive=VariantAliasArmMechanismEvidence(
                    case_id=assignment.case_id,
                    arm=CorrectiveMemoryArm.ADAPTIVE,
                    tenant_id=assignment.adaptive_tenant_id,
                    target_id=assignment.adaptive_target_id,
                    worker_decision="resolved",
                    candidate_set_id=uuid4(),
                    candidate_set_hash="a" * 64,
                    candidate_set_size=4,
                    authorized_candidate_refs=(adaptive_ref,),
                    target_candidate_authorized=True,
                    target_candidate_evidence_refs=("alias:adaptive",),
                    closed_set_match=True,
                    model_output_ref=adaptive_ref,
                    model_output_confidence=0.99,
                    scripted_high_confidence_target_response_observed=True,
                    llm_call_count=pair.adaptive.llm_call_count,
                    source_observation_immutable=True,
                ),
                frozen=VariantAliasArmMechanismEvidence(
                    case_id=assignment.case_id,
                    arm=CorrectiveMemoryArm.FROZEN,
                    tenant_id=assignment.frozen_tenant_id,
                    target_id=assignment.frozen_target_id,
                    worker_decision="review",
                    candidate_set_id=uuid4(),
                    candidate_set_hash="b" * 64,
                    candidate_set_size=3,
                    authorized_candidate_refs=(),
                    target_candidate_authorized=False,
                    target_candidate_evidence_refs=(),
                    closed_set_match=False,
                    model_output_ref=frozen_target_ref,
                    model_output_confidence=0.99,
                    scripted_high_confidence_target_response_observed=True,
                    llm_call_count=pair.frozen.llm_call_count,
                    source_observation_immutable=True,
                ),
            )
        )
    return tuple(rows)


def _mechanism_metrics(
    population: HeldOutVariantAliasPopulation,
) -> VariantAliasMechanismMetrics:
    return VariantAliasMechanismMetrics(
        selected_case_count=len(population.cases),
        observed_pair_count=len(population.cases),
        unsupported_case_count=0,
        full_registry_coverage_rate=1.0,
        observed_execution_rate=1.0,
        adaptive_correctness_rate=1.0,
        frozen_correctness_rate=0.0,
        adaptive_minus_frozen_correctness=1.0,
        adaptive_target_candidate_authorization_rate=1.0,
        frozen_target_candidate_exposure_rate=0.0,
        candidate_authorization_gap=1.0,
        adaptive_closed_set_match_rate=1.0,
        frozen_closed_set_match_rate=0.0,
        both_arms_one_llm_call_rate=1.0,
        both_arms_scripted_target_response_rate=1.0,
        frozen_safe_review_or_abstention_rate=1.0,
        source_immutability_rate=1.0,
        candidate_memory_mediated_success_rate=1.0,
        adaptive_mean_llm_calls=1.0,
        frozen_mean_llm_calls=1.0,
        hard_safety_incident_count=0,
        control_integrity_violation_count=0,
        entity_type_counts={
            entity_type: sum(
                case.entity_type == entity_type
                for case in population.cases
            )
            for entity_type in ("customer", "project", "system", "team")
        },
        variant_family_counts={
            family.value: sum(
                case.variant_family is family for case in population.cases
            )
            for family in VariantAliasFamily
        },
    )
