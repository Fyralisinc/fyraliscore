#!/usr/bin/env python3
"""Run the sealed held-out variant-alias population on real Postgres."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import fmean
from time import perf_counter
from typing import Any, Literal
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.entity_mention_detection import extract_bootstrap_mention_opportunities
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
    HeldOutVariantAliasCase,
    HeldOutVariantAliasPopulation,
    VARIANT_ALIAS_SCENARIO_ID,
    VariantAliasArmMechanismEvidence,
    VariantAliasCaseAssignment,
    VariantAliasExecutionObservation,
    VariantAliasMechanismMetrics,
    VariantAliasPairMechanismEvidence,
    VariantAliasPopulationReport,
    evaluate_variant_alias_population,
    load_variant_alias_population,
    validate_variant_population_evidence_artifact,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from scripts.company_learning_recurrence_runtime import (
    NegativeControlAssignment,
    NegativeControlCaseDefinition,
)
from scripts.run_company_learning_negative_controls_db import (
    _NegativeArmFoundation,
    _assert_pair_isolation,
    _prepare_negative_arm,
)
from scripts.run_company_learning_pair_harness import (
    _consumer_fate,
    _ingest_slack,
    _json,
    _observation_snapshot,
)
from scripts.run_company_learning_population_harness import (
    _RUNTIME_TARGETS,
)
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import normalize_phrase


DEFAULT_VARIANT_POPULATION = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_variant_alias_population_v1.jsonl"
)
ARTIFACT_NAME = "company_learning_variant_population_evidence.json"
_SAFE_FROZEN_FATES = {
    ConsumerTerminalFate.REVIEW,
    ConsumerTerminalFate.ABSTAINED,
    ConsumerTerminalFate.REJECTED,
    ConsumerTerminalFate.NO_ADMISSION,
}


async def run_variant_population_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    case_limit: int | None = None,
    bootstrap_samples: int = 2000,
    llm_call_cost_usd: float = 0.001,
    population_path: Path = DEFAULT_VARIANT_POPULATION,
) -> CompanyLearningVariantPopulationEvidence:
    """Execute the exact variant registry with fresh paired tenants."""

    registry = load_variant_alias_population(population_path)
    if case_limit is not None and not 1 <= case_limit <= len(registry.cases):
        raise ValueError("case_limit must select a non-empty registry prefix")
    selected = (
        registry.cases[:case_limit]
        if case_limit is not None
        else registry.cases
    )
    assignments = tuple(_assignment(case) for case in selected)
    await _assert_fresh_tenants(pool, assignments)
    created_at = datetime.now(timezone.utc)
    raw_pairs: list[PairedRecurrenceResult] = []
    mechanism_pairs: list[VariantAliasPairMechanismEvidence] = []
    observations: list[VariantAliasExecutionObservation] = []
    sealed_cases: list[SealedRecurrenceCase] = []

    for case, assignment in zip(selected, assignments, strict=True):
        unsupported_reason = _source_locator_gap(case)
        if unsupported_reason is not None:
            await _materialize_assignment_tenants(pool, assignment)
            observations.append(
                VariantAliasExecutionObservation(
                    case_id=case.case_id,
                    execution_status="unsupported",
                    unsupported_reason=unsupported_reason,
                )
            )
            continue
        observations.append(
            VariantAliasExecutionObservation(case_id=case.case_id)
        )
        sealed = _sealed_case(case, assignment)
        sealed_cases.append(sealed)
        definition = _runtime_definition(case)
        runtime_assignment = _runtime_assignment(assignment)
        training_at = created_at
        adaptive = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=training_at,
            runtime_target=_RUNTIME_TARGETS[case.entity_type],
            recurrence_confidence=0.99,
        )
        frozen = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.FROZEN,
            training_at=training_at,
            runtime_target=_RUNTIME_TARGETS[case.entity_type],
            recurrence_confidence=0.99,
        )
        recurrence_at = training_at + _distance(case.recurrence_distance)
        adaptive_result, adaptive_mechanism = await _run_variant_recurrence(
            pool=pool,
            case=case,
            foundation=adaptive,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        frozen_result, frozen_mechanism = await _run_variant_recurrence(
            pool=pool,
            case=case,
            foundation=frozen,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        await _assert_pair_isolation(
            pool=pool,
            adaptive=adaptive_result,
            frozen=frozen_result,
        )
        raw_pairs.append(
            PairedRecurrenceResult(
                case_id=case.case_id,
                adaptive=adaptive_result,
                frozen=frozen_result,
                artifact_refs=(f"variant-alias-pair:{case.case_id}",),
            )
        )
        mechanism_pairs.append(
            VariantAliasPairMechanismEvidence(
                case_id=case.case_id,
                adaptive=adaptive_mechanism,
                frozen=frozen_mechanism,
            )
        )

    if not raw_pairs:
        raise RuntimeError("variant registry has no runtime-supported cases")
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id=f"corrective-memory-variant-alias:{run_id}",
        run_id=run_id,
        system_version=system_version,
        created_at=created_at.isoformat(),
        scenario_ids=(VARIANT_ALIAS_SCENARIO_ID,),
        company_foundation_digest=canonical_sha256(
            [row.model_dump(mode="json") for row in assignments]
        ),
        provider_behavior_digest=canonical_sha256(
            {
                "training_response": {
                    "assigned_target": True,
                    "confidence": 0.99,
                },
                "recurrence_response": {
                    "assigned_target": True,
                    "confidence": 0.99,
                    "identical_policy_in_both_arms": True,
                },
                "adaptive_only_difference": (
                    "clarification-learned candidate visibility"
                ),
            }
        ),
        cases=tuple(sealed_cases),
        artifact_refs=(
            f"variant-population:{population_path.resolve()}",
            f"variant-population-digest:sha256:{registry.digest}",
        ),
    )
    experiment_report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=tuple(raw_pairs),
        artifact_refs=(f"report-directory:{output_dir.resolve()}",),
    )
    mode: Literal["smoke", "full"] = (
        "smoke" if case_limit is not None else "full"
    )
    typed_observations = tuple(observations)
    population_report = (
        evaluate_variant_alias_population(
            population=registry,
            experiment_report=experiment_report,
            observations=typed_observations,
            bootstrap_samples=bootstrap_samples,
        )
        if mode == "full"
        else None
    )
    metrics = _mechanism_metrics(
        registry=registry,
        selected=selected,
        observations=typed_observations,
        pairs=tuple(raw_pairs),
        mechanisms=tuple(mechanism_pairs),
        report=experiment_report,
    )
    evidence = CompanyLearningVariantPopulationEvidence(
        created_at=created_at.isoformat(),
        run_id=run_id,
        system_version=system_version,
        execution_mode=mode,
        selection_policy=(
            "deterministic_registry_prefix_smoke"
            if mode == "smoke"
            else "full_registry_once_no_selective_reruns"
        ),
        registry_path=str(population_path.resolve()),
        registry_population=registry,
        registry_population_digest=registry.digest,
        selected_case_ids=tuple(case.case_id for case in selected),
        assignments=assignments,
        observations=typed_observations,
        raw_pairs=tuple(raw_pairs),
        mechanism_pairs=tuple(mechanism_pairs),
        experiment_report=experiment_report,
        population_report=population_report,
        mechanism_metrics=metrics,
        artifact_refs=(
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
        ),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_payload = evidence.artifact_payload()
    validate_variant_population_evidence_artifact(artifact_payload)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(
            artifact_payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return evidence


def _assignment(case: HeldOutVariantAliasCase) -> VariantAliasCaseAssignment:
    runtime_target = _RUNTIME_TARGETS[case.entity_type]
    return VariantAliasCaseAssignment(
        case_id=case.case_id,
        logical_entity_type=case.entity_type,
        runtime_entity_type=runtime_target.canonical_ref_type,
        adaptive_tenant_id=uuid7(),
        frozen_tenant_id=uuid7(),
        adaptive_target_id=uuid7(),
        frozen_target_id=uuid7(),
        adaptive_conflicting_id=uuid7(),
        frozen_conflicting_id=uuid7(),
    )


def _runtime_assignment(
    assignment: VariantAliasCaseAssignment,
) -> NegativeControlAssignment:
    return NegativeControlAssignment(
        case_id=assignment.case_id,
        adaptive_tenant_id=assignment.adaptive_tenant_id,
        frozen_tenant_id=assignment.frozen_tenant_id,
        adaptive_target_id=assignment.adaptive_target_id,
        frozen_target_id=assignment.frozen_target_id,
        adaptive_conflicting_id=assignment.adaptive_conflicting_id,
        frozen_conflicting_id=assignment.frozen_conflicting_id,
    )


def _runtime_definition(
    case: HeldOutVariantAliasCase,
) -> NegativeControlCaseDefinition:
    runtime_target = _RUNTIME_TARGETS[case.entity_type]
    return NegativeControlCaseDefinition(
        case_id=case.case_id,
        kind=RecurrenceCaseKind.VARIANT_ALIAS_POSITIVE,
        entity_type=runtime_target.canonical_ref_type,
        slack_context=case.slack_context,
        wording_variant=case.wording_variant,
        consequence=case.consequence,
        recurrence_distance=1,
        alias_surface=case.recurrence_alias_surface,
        training_text=case.training_text,
        training_phrase=case.training_alias_surface,
        candidate_alias=case.candidate_label,
        recurrence_text=case.recurrence_text,
        recurrence_phrase=case.recurrence_alias_surface,
        channel=case.channel,
        resolution_scope="tenant_global_exact",
        inject_conflicting_source_hint=False,
        recurrence_response="target_low",
        expected_model_count=0,
    )


def _sealed_case(
    case: HeldOutVariantAliasCase,
    assignment: VariantAliasCaseAssignment,
) -> SealedRecurrenceCase:
    runtime_type = assignment.runtime_entity_type
    return SealedRecurrenceCase(
        case_id=case.case_id,
        case_version=case.case_version,
        kind=RecurrenceCaseKind.VARIANT_ALIAS_POSITIVE,
        alias_surface=case.recurrence_alias_surface,
        source_text_digest=canonical_sha256(case.recurrence_text),
        context_digest=canonical_sha256(case.model_dump(mode="json")),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=assignment.adaptive_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type=runtime_type,
                id=str(assignment.adaptive_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=True,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=assignment.frozen_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.REVIEW,
                ConsumerTerminalFate.ABSTAINED,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type=runtime_type,
                id=str(assignment.frozen_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=(f"variant-population-case:{case.case_id}",),
    )


async def _run_variant_recurrence(
    *,
    pool: asyncpg.Pool,
    case: HeldOutVariantAliasCase,
    foundation: _NegativeArmFoundation,
    occurred_at: datetime,
    llm_call_cost_usd: float,
) -> tuple[CorrectiveMemoryArmResult, VariantAliasArmMechanismEvidence]:
    observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=foundation.tenant_id,
        alias_repo=foundation.alias_repo,
        text=case.recurrence_text,
        channel=case.channel,
        occurred_at=occurred_at,
        corrective_memory_reuse_enabled=(
            foundation.arm is CorrectiveMemoryArm.ADAPTIVE
        ),
    )
    before = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    calls_before = len(foundation.provider.calls)
    started = perf_counter()
    decisions = await foundation.worker.process_observation(
        observation_id,
        foundation.tenant_id,
    )
    latency_ms = (perf_counter() - started) * 1000.0
    llm_calls = len(foundation.provider.calls) - calls_before
    after = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    source_immutable = before == after
    observed = await _variant_recurrence_rows(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        recurrence_phrase=case.recurrence_alias_surface,
    )
    trace = observed["trace"]
    selected = _json(trace["selected_referent"]) if trace else None
    resolved = _canonical_ref(selected)
    model_output = _json(trace["model_output"]) if trace else {}
    model_output_ref = _canonical_ref(model_output.get("canonical_ref"))
    candidates = _json(trace["candidates"]) if trace else []
    candidates = candidates if isinstance(candidates, list) else []
    authorized_refs = tuple(
        ref
        for candidate in candidates
        if (ref := _candidate_ref(candidate)) is not None
    )
    target_ref = CanonicalEntityRef(
        type=_RUNTIME_TARGETS[case.entity_type].canonical_ref_type,
        id=str(foundation.target_id),
    )
    target_candidates = [
        candidate
        for candidate in candidates
        if _candidate_ref(candidate) == target_ref
    ]
    target_authorized = bool(target_candidates)
    target_evidence_refs = tuple(
        dict.fromkeys(
            str(ref)
            for candidate in target_candidates
            for ref in candidate.get(
                "authorized_positive_evidence_refs",
                (),
            )
        )
    )
    worker_decision = next(
        (
            decision
            for phrase, decision in decisions
            if normalize_phrase(phrase)
            == normalize_phrase(case.recurrence_alias_surface)
        ),
        "missing",
    )
    confidence = model_output.get("confidence")
    confidence = (
        float(confidence) if isinstance(confidence, (int, float)) else None
    )
    scripted_response_observed = (
        model_output_ref == target_ref
        and confidence is not None
        and abs(confidence - 0.99) < 1e-9
    )
    incidents: set[HardSafetyIncidentClass] = set()
    if not source_immutable:
        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    if observed["self_authored"]:
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    if (
        foundation.arm is CorrectiveMemoryArm.FROZEN
        and resolved is not None
    ):
        incidents.add(
            HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION
        )
    result = CorrectiveMemoryArmResult(
        case_id=case.case_id,
        arm=foundation.arm,
        tenant_id=foundation.tenant_id,
        consumer_fate=(
            _consumer_fate(str(trace["current_fate"] or ""))
            if trace
            else ConsumerTerminalFate.INCOMPLETE
        ),
        resolved_entity_ref=resolved,
        decision_source=(
            str(model_output.get("decision_source"))
            if model_output.get("decision_source")
            else None
        ),
        llm_call_count=llm_calls,
        latency_ms=latency_ms,
        estimated_cost_usd=llm_calls * llm_call_cost_usd,
        source_semantic_admitted=False,
        lineage=ArmLineageRefs(
            training_observation_id=foundation.training_observation_id,
            recurrence_observation_id=observation_id,
            clarification_request_id=foundation.clarification_request_id,
            clarification_answer_digest=(
                foundation.clarification_answer_digest
            ),
            adjudicated_alias_id=foundation.adjudicated_alias_id,
            grounding_trace_id=(
                trace["grounding_trace_id"] if trace else None
            ),
            model_ids=tuple(observed["model_ids"]),
            artifact_refs=(f"observation:{observation_id}",),
        ),
        observed_safety_incidents=frozenset(incidents),
    )
    mechanism = VariantAliasArmMechanismEvidence(
        case_id=case.case_id,
        arm=foundation.arm,
        tenant_id=foundation.tenant_id,
        target_id=foundation.target_id,
        worker_decision=worker_decision,
        candidate_set_id=(
            trace["candidate_set_id"] if trace else None
        ),
        candidate_set_hash=(
            str(trace["candidate_set_hash"])
            if trace and trace["candidate_set_hash"]
            else None
        ),
        candidate_set_size=len(candidates),
        authorized_candidate_refs=authorized_refs,
        target_candidate_authorized=target_authorized,
        target_candidate_evidence_refs=target_evidence_refs,
        closed_set_match=bool(model_output.get("closed_set_match")),
        model_output_ref=model_output_ref,
        model_output_confidence=confidence,
        scripted_high_confidence_target_response_observed=(
            scripted_response_observed
        ),
        llm_call_count=llm_calls,
        source_observation_immutable=source_immutable,
    )
    return result, mechanism


async def _variant_recurrence_rows(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
    recurrence_phrase: str,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        trace = await conn.fetchrow(
            """
            SELECT trace.id AS grounding_trace_id,
                   trace.current_fate,
                   trace.selected_referent,
                   trace.candidate_set_id,
                   candidate_set.candidate_set_hash,
                   candidate_set.candidates,
                   assessment.model_output
            FROM grounding_traces trace
            LEFT JOIN entity_candidate_sets candidate_set
              ON candidate_set.tenant_id=trace.tenant_id
             AND candidate_set.id=trace.candidate_set_id
            LEFT JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
              AND regexp_replace(lower(trace.phrase), '\\s+', ' ', 'g')=$3
            ORDER BY trace.created_at DESC, trace.id DESC
            LIMIT 1
            """,
            tenant_id,
            observation_id,
            normalize_phrase(recurrence_phrase),
        )
        model_ids = tuple(
            row["id"]
            for row in await conn.fetch(
                """
                SELECT id FROM models
                WHERE tenant_id=$1 AND born_from_event_id=$2
                ORDER BY id
                """,
                tenant_id,
                observation_id,
            )
        )
        self_authored = await conn.fetchval(
            """
            SELECT count(*) FROM observations
            WHERE tenant_id=$1
              AND source_channel='internal:state_change'
              AND content ->> 'source_observation_id'=$2
            """,
            tenant_id,
            str(observation_id),
        )
    return {
        "trace": trace,
        "model_ids": model_ids,
        "self_authored": int(self_authored or 0),
    }


def _canonical_ref(value: Any) -> CanonicalEntityRef | None:
    payload = _json(value)
    if not isinstance(payload, dict):
        return None
    if not payload.get("type") or not payload.get("id"):
        return None
    return CanonicalEntityRef.model_validate(payload)


def _candidate_ref(candidate: Any) -> CanonicalEntityRef | None:
    if not isinstance(candidate, dict):
        return None
    if candidate.get("kind") != "canonical_referent":
        return None
    if not candidate.get("candidate_type") or not candidate.get(
        "canonical_referent_id"
    ):
        return None
    return CanonicalEntityRef(
        type=str(candidate["candidate_type"]),
        id=str(candidate["canonical_referent_id"]),
        version=int(candidate.get("canonical_referent_version") or 1),
    )


def _mechanism_metrics(
    *,
    registry: HeldOutVariantAliasPopulation,
    selected: tuple[HeldOutVariantAliasCase, ...],
    observations: tuple[VariantAliasExecutionObservation, ...],
    pairs: tuple[PairedRecurrenceResult, ...],
    mechanisms: tuple[VariantAliasPairMechanismEvidence, ...],
    report: CorrectiveMemoryExperimentReport,
) -> VariantAliasMechanismMetrics:
    pair_count = len(pairs)
    assessments = {
        (row.case_id, row.arm): row for row in report.assessments
    }
    adaptive_correct = sum(
        assessments[(pair.case_id, CorrectiveMemoryArm.ADAPTIVE)].correct
        for pair in pairs
    )
    frozen_correct = sum(
        assessments[(pair.case_id, CorrectiveMemoryArm.FROZEN)].correct
        for pair in pairs
    )
    adaptive_authorized = sum(
        pair.adaptive.target_candidate_authorized for pair in mechanisms
    )
    frozen_exposed = sum(
        pair.frozen.target_candidate_authorized for pair in mechanisms
    )
    adaptive_closed = sum(
        pair.adaptive.closed_set_match for pair in mechanisms
    )
    frozen_closed = sum(
        pair.frozen.closed_set_match for pair in mechanisms
    )
    one_call = sum(
        pair.adaptive.llm_call_count == 1
        and pair.frozen.llm_call_count == 1
        for pair in mechanisms
    )
    same_scripted_response = sum(
        pair.adaptive.scripted_high_confidence_target_response_observed
        and pair.frozen.scripted_high_confidence_target_response_observed
        for pair in mechanisms
    )
    source_immutable = sum(
        pair.adaptive.source_observation_immutable
        and pair.frozen.source_observation_immutable
        for pair in mechanisms
    )
    pair_by_case = {pair.case_id: pair for pair in pairs}
    safe_frozen = sum(
        pair_by_case[row.case_id].frozen.consumer_fate
        in _SAFE_FROZEN_FATES
        and pair_by_case[row.case_id].frozen.resolved_entity_ref is None
        for row in mechanisms
    )
    ideal = sum(
        mechanism.adaptive.target_candidate_authorized
        and not mechanism.frozen.target_candidate_authorized
        and mechanism.adaptive.closed_set_match
        and not mechanism.frozen.closed_set_match
        and mechanism.adaptive.llm_call_count == 1
        and mechanism.frozen.llm_call_count == 1
        and mechanism.adaptive.scripted_high_confidence_target_response_observed
        and mechanism.frozen.scripted_high_confidence_target_response_observed
        and mechanism.adaptive.source_observation_immutable
        and mechanism.frozen.source_observation_immutable
        and assessments[
            (mechanism.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ].correct
        and pair_by_case[mechanism.case_id].frozen.consumer_fate
        in _SAFE_FROZEN_FATES
        and pair_by_case[mechanism.case_id].frozen.resolved_entity_ref is None
        for mechanism in mechanisms
    )
    unsupported = sum(
        row.execution_status == "unsupported" for row in observations
    )
    rate = lambda value: value / pair_count if pair_count else None
    adaptive_auth_rate = rate(adaptive_authorized)
    frozen_exposure_rate = rate(frozen_exposed)
    return VariantAliasMechanismMetrics(
        selected_case_count=len(selected),
        observed_pair_count=pair_count,
        unsupported_case_count=unsupported,
        full_registry_coverage_rate=len(selected) / len(registry.cases),
        observed_execution_rate=pair_count / len(selected),
        adaptive_correctness_rate=rate(adaptive_correct),
        frozen_correctness_rate=rate(frozen_correct),
        adaptive_minus_frozen_correctness=(
            (adaptive_correct - frozen_correct) / pair_count
            if pair_count
            else None
        ),
        adaptive_target_candidate_authorization_rate=adaptive_auth_rate,
        frozen_target_candidate_exposure_rate=frozen_exposure_rate,
        candidate_authorization_gap=(
            adaptive_auth_rate - frozen_exposure_rate
            if adaptive_auth_rate is not None
            and frozen_exposure_rate is not None
            else None
        ),
        adaptive_closed_set_match_rate=rate(adaptive_closed),
        frozen_closed_set_match_rate=rate(frozen_closed),
        both_arms_one_llm_call_rate=rate(one_call),
        both_arms_scripted_target_response_rate=rate(
            same_scripted_response
        ),
        frozen_safe_review_or_abstention_rate=rate(safe_frozen),
        source_immutability_rate=rate(source_immutable),
        candidate_memory_mediated_success_rate=rate(ideal),
        adaptive_mean_llm_calls=(
            fmean(pair.adaptive.llm_call_count for pair in pairs)
            if pairs
            else None
        ),
        frozen_mean_llm_calls=(
            fmean(pair.frozen.llm_call_count for pair in pairs)
            if pairs
            else None
        ),
        hard_safety_incident_count=len(report.incidents),
        control_integrity_violation_count=pair_count - ideal,
        entity_type_counts=dict(
            sorted(Counter(case.entity_type for case in selected).items())
        ),
        variant_family_counts=dict(
            sorted(
                Counter(
                    case.variant_family.value for case in selected
                ).items()
            )
        ),
    )


def _source_locator_gap(case: HeldOutVariantAliasCase) -> str | None:
    training = extract_bootstrap_mention_opportunities(case.training_text)
    recurrence = extract_bootstrap_mention_opportunities(case.recurrence_text)
    missing: list[str] = []
    if case.training_alias_surface not in training:
        missing.append("training")
    if case.recurrence_alias_surface not in recurrence:
        missing.append("recurrence")
    if not missing:
        return None
    return (
        "Slack bootstrap mention locator did not preserve the exact "
        f"{' and '.join(missing)} source surface"
    )


async def _assert_fresh_tenants(
    pool: asyncpg.Pool,
    assignments: tuple[VariantAliasCaseAssignment, ...],
) -> None:
    tenant_ids = [
        tenant_id
        for row in assignments
        for tenant_id in (row.adaptive_tenant_id, row.frozen_tenant_id)
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise RuntimeError("variant tenant assignments are not unique")
    async with pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
            tenant_ids,
        )
    if existing:
        raise RuntimeError("variant execution requires fresh tenants")


async def _materialize_assignment_tenants(
    pool: asyncpg.Pool,
    assignment: VariantAliasCaseAssignment,
) -> None:
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO tenants (id) VALUES ($1)",
            (
                (assignment.adaptive_tenant_id,),
                (assignment.frozen_tenant_id,),
            ),
        )


def _distance(value: str) -> timedelta:
    return {
        "same_day": timedelta(hours=1),
        "one_week": timedelta(days=7),
        "one_month": timedelta(days=30),
        "one_quarter": timedelta(days=90),
    }[value]


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=6,
        init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, ROOT / "db" / "migrations")
        evidence = await run_variant_population_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            case_limit=args.case_limit,
            bootstrap_samples=args.bootstrap_samples,
            llm_call_cost_usd=args.llm_call_cost_usd,
            population_path=args.population,
        )
    finally:
        await pool.close()
    metrics = evidence.mechanism_metrics
    print(f"artifact={args.output_dir / ARTIFACT_NAME}")
    print(
        "mode={mode} selected={selected} observed={observed} "
        "unsupported={unsupported} adaptive={adaptive} frozen={frozen} "
        "lift={lift} candidate_memory_success={success} incidents={incidents}"
        .format(
            mode=evidence.execution_mode,
            selected=metrics.selected_case_count,
            observed=metrics.observed_pair_count,
            unsupported=metrics.unsupported_case_count,
            adaptive=metrics.adaptive_correctness_rate,
            frozen=metrics.frozen_correctness_rate,
            lift=metrics.adaptive_minus_frozen_correctness,
            success=metrics.candidate_memory_mediated_success_rate,
            incidents=metrics.hard_safety_incident_count,
        )
    )
    return (
        2
        if (
            metrics.unsupported_case_count
            or metrics.hard_safety_incident_count
            or metrics.control_integrity_violation_count
        )
        else 0
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument(
        "--population",
        type=Path,
        default=DEFAULT_VARIANT_POPULATION,
    )
    parser.add_argument("--case-limit", type=int, default=None)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
