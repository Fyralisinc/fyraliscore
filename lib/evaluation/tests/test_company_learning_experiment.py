from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
    ArmTerminalFate,
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmResult,
    CorrectiveMemoryExperimentSpec,
    HardSafetyIncidentClass,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
    evaluate_corrective_memory_experiment,
)
from lib.evaluation.company_learning_experiment_proof import (
    CORRECTIVE_MEMORY_LIFT_METRIC_ID,
    CORRECTIVE_MEMORY_SCENARIO_ID,
    build_corrective_memory_experiment_evidence_manifest,
)
from lib.evaluation.proof import EvidenceTier


def _case(
    *,
    case_id: str = "held-out-exact-1",
    positive: bool = True,
    adaptive_model_count: int = 1,
    frozen_model_count: int = 0,
    adaptive_tenant_id: UUID | None = None,
    frozen_tenant_id: UUID | None = None,
) -> SealedRecurrenceCase:
    adaptive_ref = (
        CanonicalEntityRef(type="customer", id="adaptive-customer-nimbus")
        if positive
        else None
    )
    frozen_ref = (
        CanonicalEntityRef(type="customer", id="frozen-customer-nimbus")
        if positive
        else None
    )
    return SealedRecurrenceCase(
        case_id=case_id,
        case_version="v1",
        kind=(
            RecurrenceCaseKind.EXACT_ALIAS_POSITIVE
            if positive
            else RecurrenceCaseKind.CONTEXTUAL_PHRASE_NEGATIVE
        ),
        alias_surface="NBI" if positive else "the project",
        source_text_digest=canonical_sha256(
            "NBI is delayed" if positive else "the project is delayed"
        ),
        context_digest=canonical_sha256({"channel": f"slack:{case_id}"}),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=adaptive_tenant_id or uuid4(),
            allowed_consumer_fates=(
                (ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,)
                if positive
                else (
                    ConsumerTerminalFate.REVIEW,
                    ConsumerTerminalFate.ABSTAINED,
                )
            ),
            expected_entity_ref=adaptive_ref,
            expected_model_count=adaptive_model_count if positive else 0,
            autonomous_resolution_permitted=positive,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=frozen_tenant_id or uuid4(),
            allowed_consumer_fates=(
                (
                    ConsumerTerminalFate.REVIEW,
                    ConsumerTerminalFate.ABSTAINED,
                )
            ),
            expected_entity_ref=frozen_ref,
            expected_model_count=frozen_model_count if positive else 0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=(f"pytest://case/{case_id}",),
    )


def _spec(*cases: SealedRecurrenceCase) -> CorrectiveMemoryExperimentSpec:
    return CorrectiveMemoryExperimentSpec(
        experiment_id="corrective-memory-pair-1",
        run_id="pytest-pair-run",
        system_version="pytest-system",
        created_at=datetime.now(timezone.utc).isoformat(),
        scenario_ids=("ENTITY-CORRECTIVE-MEMORY-PAIR",),
        company_foundation_digest=canonical_sha256("same-company"),
        provider_behavior_digest=canonical_sha256("same-provider"),
        cases=tuple(cases),
        artifact_refs=("pytest://spec",),
    )


def _lineage(*, model_count: int) -> ArmLineageRefs:
    return ArmLineageRefs(
        training_observation_id=uuid4(),
        recurrence_observation_id=uuid4(),
        clarification_request_id=uuid4(),
        clarification_answer_digest=canonical_sha256("answer"),
        adjudicated_alias_id=uuid4(),
        grounding_trace_id=uuid4(),
        source_semantic_interpretation_id=uuid4(),
        source_semantic_admission_id=uuid4(),
        model_ids=tuple(uuid4() for _ in range(model_count)),
        artifact_refs=("pytest://lineage",),
    )


def _arm_result(
    case: SealedRecurrenceCase,
    *,
    arm: CorrectiveMemoryArm,
    consumer_fate: ConsumerTerminalFate,
    resolved_entity_ref: CanonicalEntityRef | None,
    model_count: int,
    decision_source: str,
    source_semantic_admitted: bool,
) -> CorrectiveMemoryArmResult:
    expectation = case.expectation_for(arm)
    return CorrectiveMemoryArmResult(
        case_id=case.case_id,
        arm=arm,
        tenant_id=expectation.tenant_id,
        consumer_fate=consumer_fate,
        resolved_entity_ref=resolved_entity_ref,
        decision_source=decision_source,
        llm_call_count=0 if arm is CorrectiveMemoryArm.ADAPTIVE else 1,
        latency_ms=8.0 if arm is CorrectiveMemoryArm.ADAPTIVE else 40.0,
        estimated_cost_usd=0.0 if arm is CorrectiveMemoryArm.ADAPTIVE else 0.01,
        source_semantic_admitted=source_semantic_admitted,
        lineage=_lineage(model_count=model_count),
    )


def _pair(case: SealedRecurrenceCase) -> PairedRecurrenceResult:
    return PairedRecurrenceResult(
        case_id=case.case_id,
        adaptive=_arm_result(
            case,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            resolved_entity_ref=case.adaptive_expectation.expected_entity_ref,
            model_count=case.adaptive_expectation.expected_model_count,
            decision_source="governed_exact_alias_replay",
            source_semantic_admitted=(
                case.adaptive_expectation.expected_model_count > 0
            ),
        ),
        frozen=_arm_result(
            case,
            arm=CorrectiveMemoryArm.FROZEN,
            consumer_fate=ConsumerTerminalFate.REVIEW,
            resolved_entity_ref=None,
            model_count=case.frozen_expectation.expected_model_count,
            decision_source="llm",
            source_semantic_admitted=False,
        ),
        artifact_refs=("pytest://pair",),
    )


def test_paired_experiment_derives_continuous_adaptive_lift() -> None:
    case = _case()
    spec = _spec(case)

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(_pair(case),),
        artifact_refs=("pytest://report",),
    )

    assert report.status == "observed"
    assert report.metrics.adaptive_correctness_rate == 1.0
    assert report.metrics.frozen_correctness_rate == 0.0
    assert report.metrics.adaptive_minus_frozen_correctness == 1.0
    assert report.metrics.llm_calls_avoided == 1
    assert report.metrics.adaptive_only_correct_count == 1
    assert report.metrics.lineage_coverage_rate == 1.0
    assert report.incidents == ()
    adaptive, frozen = report.assessments
    assert adaptive.terminal_fate is ArmTerminalFate.CORRECT_RESOLUTION
    assert adaptive.correct is True
    assert adaptive.entity_match is True
    assert frozen.terminal_fate is ArmTerminalFate.SAFE_REVIEW
    assert frozen.correct is False
    assert len(report.digest) == 64


def test_paired_report_translates_to_canonical_invariant_evidence() -> None:
    case = _case()
    report = evaluate_corrective_memory_experiment(
        spec=_spec(case),
        pairs=(_pair(case),),
        artifact_refs=("pytest://proof-report",),
    )

    manifest = build_corrective_memory_experiment_evidence_manifest(
        report,
        architecture_digest="a" * 64,
        experiment_manifest_ref="pytest://experiment-manifest",
        report_cutoff="2026-07-16T00:00:00+00:00",
    )

    assert len(manifest.evidence) == 1
    evidence = manifest.evidence[0]
    assert evidence.invariant_id == "INV-05"
    assert evidence.achieved_evidence_tier is EvidenceTier.E4
    assert evidence.executed_scenario_ids == frozenset(
        {CORRECTIVE_MEMORY_SCENARIO_ID}
    )
    metric = evidence.metric_observations[0]
    assert metric.metric_id == CORRECTIVE_MEMORY_LIFT_METRIC_ID
    assert metric.point_estimate == 1.0
    assert evidence.denominator.eligible == 2
    assert evidence.denominator.complete is True


def test_wrong_ref_cannot_be_labeled_as_correct_by_the_caller() -> None:
    case = _case()
    spec = _spec(case)
    pair = _pair(case)
    wrong_ref = CanonicalEntityRef(type="customer", id="wrong-customer")
    adversarial = pair.model_copy(
        update={
            "adaptive": pair.adaptive.model_copy(
                update={"resolved_entity_ref": wrong_ref}
            )
        }
    )

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(adversarial,),
        artifact_refs=("pytest://wrong-ref-report",),
    )

    adaptive = report.assessments[0]
    assert adaptive.consumer_fate is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
    assert adaptive.terminal_fate is ArmTerminalFate.WRONG_RESOLUTION
    assert adaptive.correct is False
    assert adaptive.entity_match is False
    assert report.metrics.adaptive_correctness_rate == 0.0
    assert report.metrics.adaptive_unsafe_count == 1
    assert {
        HardSafetyIncidentClass.WRONG_ENTITY_RESOLUTION,
        HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION,
        HardSafetyIncidentClass.WRONG_MODEL_FROM_REPLAY,
    }.issubset(adaptive.incident_classes)


def test_correct_resolution_is_independent_of_model_creation() -> None:
    case = _case(adaptive_model_count=0)
    spec = _spec(case)
    pair = _pair(case)

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(pair,),
        artifact_refs=("pytest://duplicate-evidence-report",),
    )

    adaptive = report.assessments[0]
    assert adaptive.terminal_fate is ArmTerminalFate.CORRECT_RESOLUTION
    assert adaptive.correct is True
    assert adaptive.model_cardinality_valid is True
    assert report.metrics.adaptive_correctness_rate == 1.0
    assert report.metrics.adaptive_semantic_admission_rate == 0.0
    assert report.metrics.adaptive_exactly_one_model_rate == 0.0
    assert report.incidents == ()


def test_model_cardinality_violation_does_not_invent_incorrect_identity() -> None:
    case = _case(adaptive_model_count=1)
    spec = _spec(case)
    pair = _pair(case)
    missing_model = pair.model_copy(
        update={
            "adaptive": pair.adaptive.model_copy(
                update={
                    "source_semantic_admitted": False,
                    "lineage": _lineage(model_count=0),
                }
            )
        }
    )

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(missing_model,),
        artifact_refs=("pytest://cardinality-report",),
    )

    adaptive = report.assessments[0]
    assert adaptive.correct is True
    assert adaptive.model_cardinality_valid is False
    assert adaptive.incident_classes == frozenset(
        {HardSafetyIncidentClass.MODEL_CARDINALITY_VIOLATION}
    )
    assert report.metrics.adaptive_correctness_rate == 1.0
    assert report.metrics.adaptive_unsafe_count == 1
    assert report.status == "contradicted"


def test_unpermitted_frozen_replay_can_be_identity_correct_but_unsafe() -> None:
    case = _case()
    spec = _spec(case)
    pair = _pair(case)
    frozen_replay = pair.model_copy(
        update={
            "frozen": _arm_result(
                case,
                arm=CorrectiveMemoryArm.FROZEN,
                consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                resolved_entity_ref=case.frozen_expectation.expected_entity_ref,
                model_count=0,
                decision_source="governed_exact_alias_replay",
                source_semantic_admitted=False,
            )
        }
    )

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(frozen_replay,),
        artifact_refs=("pytest://unsafe-frozen-replay",),
    )

    frozen = report.assessments[1]
    assert frozen.terminal_fate is ArmTerminalFate.CORRECT_RESOLUTION
    assert frozen.correct is True
    assert frozen.terminal_fate_allowed is False
    assert {
        HardSafetyIncidentClass.UNEXPECTED_TERMINAL_FATE,
        HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION,
    }.issubset(frozen.incident_classes)
    assert report.metrics.frozen_unsafe_rate == 1.0
    assert report.status == "contradicted"


def test_negative_control_safe_nonresolution_is_correct() -> None:
    case = _case(positive=False)
    spec = _spec(case)
    pair = PairedRecurrenceResult(
        case_id=case.case_id,
        adaptive=_arm_result(
            case,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            consumer_fate=ConsumerTerminalFate.REVIEW,
            resolved_entity_ref=None,
            model_count=0,
            decision_source="llm",
            source_semantic_admitted=False,
        ),
        frozen=_arm_result(
            case,
            arm=CorrectiveMemoryArm.FROZEN,
            consumer_fate=ConsumerTerminalFate.ABSTAINED,
            resolved_entity_ref=None,
            model_count=0,
            decision_source="llm",
            source_semantic_admitted=False,
        ),
        artifact_refs=("pytest://negative-pair",),
    )

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(pair,),
        artifact_refs=("pytest://negative-report",),
    )

    assert report.metrics.adaptive_correctness_rate == 1.0
    assert report.metrics.frozen_correctness_rate == 1.0
    assert report.metrics.both_correct_count == 1
    assert report.incidents == ()


def test_expected_fate_and_model_gold_must_be_coherent() -> None:
    with pytest.raises(ValidationError, match="positive expected model"):
        SealedArmExpectation(
            tenant_id=uuid4(),
            allowed_consumer_fates=(ConsumerTerminalFate.REVIEW,),
            expected_entity_ref=CanonicalEntityRef(
                type="customer",
                id="customer-nimbus",
            ),
            expected_model_count=1,
            autonomous_resolution_permitted=False,
        )

    with pytest.raises(ValidationError, match="cannot be sealed as expected"):
        SealedArmExpectation(
            tenant_id=uuid4(),
            allowed_consumer_fates=(ConsumerTerminalFate.INCOMPLETE,),
            expected_entity_ref=None,
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        )


def test_paired_arm_entity_gold_must_share_type_and_version() -> None:
    adaptive_tenant = uuid4()
    frozen_tenant = uuid4()
    with pytest.raises(ValidationError, match="canonical type and version"):
        SealedRecurrenceCase(
            case_id="mismatched-gold",
            case_version="v1",
            kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
            alias_surface="NBI",
            source_text_digest=canonical_sha256("NBI"),
            context_digest=canonical_sha256("context"),
            adaptive_expectation=SealedArmExpectation(
                tenant_id=adaptive_tenant,
                allowed_consumer_fates=(
                    ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                ),
                expected_entity_ref=CanonicalEntityRef(
                    type="customer",
                    id="adaptive-nimbus",
                ),
                expected_model_count=1,
                autonomous_resolution_permitted=True,
            ),
            frozen_expectation=SealedArmExpectation(
                tenant_id=frozen_tenant,
                allowed_consumer_fates=(ConsumerTerminalFate.REVIEW,),
                expected_entity_ref=CanonicalEntityRef(
                    type="project",
                    id="frozen-nimbus",
                ),
                expected_model_count=0,
                autonomous_resolution_permitted=False,
            ),
            artifact_refs=("pytest://mismatched-gold",),
        )


def test_result_tenant_must_match_per_case_sealed_assignment() -> None:
    case = _case()
    spec = _spec(case)
    pair = _pair(case)
    wrong_tenant = pair.model_copy(
        update={
            "adaptive": pair.adaptive.model_copy(
                update={"tenant_id": uuid4()}
            )
        }
    )

    with pytest.raises(ValueError, match="sealed assignment"):
        evaluate_corrective_memory_experiment(
            spec=spec,
            pairs=(wrong_tenant,),
            artifact_refs=("pytest://wrong-tenant",),
        )


def test_results_must_exactly_cover_sealed_population() -> None:
    first = _case()
    second = _case(case_id="held-out-exact-2")
    spec = _spec(first, second)

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_corrective_memory_experiment(
            spec=spec,
            pairs=(_pair(first),),
            artifact_refs=("pytest://incomplete-report",),
        )


def test_case_manifest_and_pair_digests_are_stable() -> None:
    case = _case()
    spec = _spec(case)
    pair = _pair(case)

    first = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(pair,),
        artifact_refs=("pytest://stable-report",),
    )
    second = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(pair,),
        artifact_refs=("pytest://stable-report",),
    )

    assert first.case_manifest_digest == second.case_manifest_digest
    assert first.gold_digest == second.gold_digest
    assert first.arm_assignment_digest == second.arm_assignment_digest
    assert first.pair_results_digest == second.pair_results_digest
    assert first.digest == second.digest
