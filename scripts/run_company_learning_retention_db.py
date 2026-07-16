#!/usr/bin/env python3
"""Run a sealed retention/forgetting regression proof on real Postgres."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    HardSafetyIncidentClass,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
)
from lib.evaluation.company_learning_retention import (
    CompanyLearningRetentionReport,
    RetentionBehavior,
    RetentionCaseSpec,
    RetentionHorizon,
    RetentionObservation,
    RetentionRunSpec,
    evaluate_company_learning_retention,
)
from lib.evaluation.company_learning_variant_collisions import (
    VariantCollisionFamily,
    _evaluate_arm,
    load_variant_collision_population,
)
from lib.evaluation.company_learning_variant_population import (
    load_variant_alias_population,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from scripts.company_learning_recurrence_runtime import (
    DEFAULT_NEGATIVE_CONTROL_FIXTURE,
    NegativeControlAssignment,
    NegativeControlCaseDefinition,
    build_negative_control_plan,
    load_negative_control_fixture,
)
from scripts.run_company_learning_negative_controls_db import (
    _NegativeArmFoundation,
    _prepare_negative_arm,
    _run_negative_recurrence,
)
from scripts.run_company_learning_pair_harness import (
    _ScriptedResolver,
    _resolver_response,
)
from scripts.run_company_learning_variant_collisions_db import (
    DEFAULT_COLLISION_POPULATION,
    _assignment as _collision_assignment,
    _prepare_collision_arm,
    _run_collision_recurrence,
    _runtime_assignment as _collision_runtime_assignment,
    _runtime_definition as _collision_runtime_definition,
)
from scripts.run_company_learning_variant_population_harness import (
    DEFAULT_VARIANT_POPULATION,
    _assignment as _variant_assignment,
    _run_variant_recurrence,
    _runtime_assignment as _variant_runtime_assignment,
    _runtime_definition as _variant_runtime_definition,
)
from scripts.run_company_learning_population_harness import _RUNTIME_TARGETS
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import (
    validate_governed_alias_replay,
)
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.source_semantic_worker import SourceSemanticWorker


ARTIFACT_NAME = "company_learning_retention_evidence.json"
HORIZONS = (
    RetentionHorizon(cycle_count=0, restart_count=0),
    RetentionHorizon(cycle_count=4, restart_count=1),
    RetentionHorizon(cycle_count=16, restart_count=2),
)
FINAL_HORIZON = HORIZONS[-1]
SAFE_FATES = (
    ConsumerTerminalFate.REVIEW,
    ConsumerTerminalFate.ABSTAINED,
    ConsumerTerminalFate.REJECTED,
    ConsumerTerminalFate.NO_ADMISSION,
)
REPRESENTATIVE_COLLISION_FAMILIES = (
    VariantCollisionFamily.SAME_TYPE_ACRONYM_COLLISION,
    VariantCollisionFamily.PUNCTUATION_UNICODE_NORMALIZATION_COLLISION,
    VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME,
)


async def run_company_learning_retention_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    llm_call_cost_usd: float = 0.001,
) -> CompanyLearningRetentionReport:
    """Measure persistence across unrelated learning and worker restarts."""

    created_at = datetime.now(timezone.utc)
    exact_definition, exact_assignment, exact_case = _exact_runtime_inputs()
    exact_foundation = await _prepare_negative_arm(
        pool=pool,
        definition=exact_definition,
        assignment=exact_assignment,
        arm=CorrectiveMemoryArm.ADAPTIVE,
        training_at=created_at - timedelta(seconds=5),
    )

    variant_registry = load_variant_alias_population(DEFAULT_VARIANT_POPULATION)
    variant_case = variant_registry.cases[0]
    variant_assignment = _variant_assignment(variant_case)
    variant_definition = _variant_runtime_definition(variant_case)
    variant_foundation = await _prepare_negative_arm(
        pool=pool,
        definition=variant_definition,
        assignment=_variant_runtime_assignment(variant_assignment),
        arm=CorrectiveMemoryArm.ADAPTIVE,
        training_at=created_at - timedelta(seconds=4),
        runtime_target=_RUNTIME_TARGETS[variant_case.entity_type],
        recurrence_confidence=0.99,
        training_phrases=(variant_case.training_alias_surface,),
    )

    negative_fixture = load_negative_control_fixture(
        DEFAULT_NEGATIVE_CONTROL_FIXTURE
    )
    negative_plan = build_negative_control_plan(
        negative_fixture,
        run_id=f"{run_id}:negative-retention",
        system_version=system_version,
        created_at=created_at,
    )
    negative_cases = {
        case.case_id: case for case in negative_plan.spec.cases
    }
    negative_assignments = {
        row.case_id: row for row in negative_plan.assignments
    }
    negative_foundations: list[
        tuple[NegativeControlCaseDefinition, SealedRecurrenceCase, _NegativeArmFoundation]
    ] = []
    for definition in negative_fixture.cases:
        foundation = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=negative_assignments[definition.case_id],
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=created_at - timedelta(seconds=3),
        )
        negative_foundations.append(
            (definition, negative_cases[definition.case_id], foundation)
        )

    collision_registry = load_variant_collision_population(
        DEFAULT_COLLISION_POPULATION
    )
    collision_cases = tuple(
        next(
            case
            for case in collision_registry.cases
            if case.collision_family is family
        )
        for family in REPRESENTATIVE_COLLISION_FAMILIES
    )
    collision_foundations = []
    for case in collision_cases:
        assignment = _collision_assignment(case)
        definition = _collision_runtime_definition(case)
        foundation = await _prepare_collision_arm(
            pool=pool,
            case=case,
            definition=definition,
            assignment=_collision_runtime_assignment(assignment),
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=created_at - timedelta(seconds=2),
        )
        collision_foundations.append((case, definition, foundation))

    spec = _retention_spec(
        run_id=run_id,
        system_version=system_version,
        created_at=created_at,
        exact_foundation=exact_foundation,
        variant_case=variant_case,
        variant_foundation=variant_foundation,
        negative_fixture=negative_fixture,
        negative_cases=negative_cases,
        collision_cases=collision_cases,
    )
    observations: list[RetentionObservation] = []
    exact_final_result = None
    for horizon in HORIZONS:
        await _advance_unrelated_learning(
            pool=pool,
            foundation=exact_foundation,
            current_count=(
                HORIZONS[HORIZONS.index(horizon) - 1].cycle_count
                if HORIZONS.index(horizon) > 0
                else 0
            ),
            target_count=horizon.cycle_count,
        )
        await _advance_unrelated_learning(
            pool=pool,
            foundation=variant_foundation,
            current_count=(
                HORIZONS[HORIZONS.index(horizon) - 1].cycle_count
                if HORIZONS.index(horizon) > 0
                else 0
            ),
            target_count=horizon.cycle_count,
        )
        exact_probe_foundation = _restart_foundation(
            pool=pool,
            foundation=exact_foundation,
            canonical_type="customer",
            target_id=exact_foundation.target_id,
            restart_count=horizon.restart_count,
        )
        exact_result = await _run_negative_recurrence(
            pool=pool,
            definition=exact_definition,
            case=exact_case,
            foundation=exact_probe_foundation,
            occurred_at=created_at + timedelta(minutes=horizon.cycle_count + 1),
            llm_call_cost_usd=llm_call_cost_usd,
        )
        exact_final_result = exact_result
        exact_models, exact_lineage = await _result_consistency(
            pool=pool,
            result=exact_result,
        )
        observations.append(
            _result_observation(
                case_id="retention-exact",
                horizon=horizon,
                result=exact_result,
                models_consistent=exact_models,
                evidence_lineage_consistent=exact_lineage,
            )
        )

        variant_probe_foundation = _restart_foundation(
            pool=pool,
            foundation=variant_foundation,
            canonical_type=(
                _RUNTIME_TARGETS[variant_case.entity_type].canonical_ref_type
            ),
            target_id=variant_foundation.target_id,
            restart_count=horizon.restart_count,
            confidence=0.99,
        )
        variant_result, mechanism = await _run_variant_recurrence(
            pool=pool,
            case=variant_case,
            foundation=variant_probe_foundation,
            occurred_at=created_at + timedelta(
                minutes=horizon.cycle_count + 2
            ),
            llm_call_cost_usd=llm_call_cost_usd,
        )
        variant_models, variant_lineage = await _result_consistency(
            pool=pool,
            result=variant_result,
        )
        observations.append(
            _result_observation(
                case_id="retention-variant",
                horizon=horizon,
                result=variant_result,
                models_consistent=variant_models,
                evidence_lineage_consistent=(
                    variant_lineage
                    and mechanism.target_candidate_authorized
                    and bool(mechanism.target_candidate_evidence_refs)
                ),
                candidate_authorized=mechanism.target_candidate_authorized,
            )
        )

    assert exact_final_result is not None
    correction_authoritative = await _correction_is_authoritative(
        pool=pool,
        foundation=exact_foundation,
        phrase=exact_definition.training_phrase,
    )
    exact_models, exact_lineage = await _result_consistency(
        pool=pool,
        result=exact_final_result,
    )
    observations.append(
        _result_observation(
            case_id="retention-correction",
            horizon=FINAL_HORIZON,
            result=exact_final_result,
            models_consistent=exact_models,
            evidence_lineage_consistent=exact_lineage,
            correction_authoritative=correction_authoritative,
        )
    )

    for index, (definition, case, foundation) in enumerate(
        negative_foundations
    ):
        await _advance_unrelated_learning(
            pool=pool,
            foundation=foundation,
            current_count=0,
            target_count=FINAL_HORIZON.cycle_count,
        )
        probe_foundation = _restart_foundation(
            pool=pool,
            foundation=foundation,
            canonical_type=definition.entity_type,
            target_id=(
                foundation.conflicting_id
                if definition.recurrence_response == "conflicting_high"
                else foundation.target_id
            ),
            restart_count=FINAL_HORIZON.restart_count,
            confidence=(
                0.99
                if definition.recurrence_response == "conflicting_high"
                else 0.40
            ),
        )
        result = await _run_negative_recurrence(
            pool=pool,
            definition=definition,
            case=case,
            foundation=probe_foundation,
            occurred_at=created_at + timedelta(minutes=30 + index),
            llm_call_cost_usd=llm_call_cost_usd,
        )
        models_consistent, lineage_consistent = await _result_consistency(
            pool=pool,
            result=result,
        )
        observations.append(
            _result_observation(
                case_id=f"retention-negative:{definition.case_id}",
                horizon=FINAL_HORIZON,
                result=result,
                models_consistent=models_consistent,
                evidence_lineage_consistent=lineage_consistent,
            )
        )

    for index, (case, definition, foundation) in enumerate(
        collision_foundations
    ):
        await _advance_unrelated_learning(
            pool=pool,
            foundation=foundation,
            current_count=0,
            target_count=FINAL_HORIZON.cycle_count,
        )
        target_id = (
            foundation.conflicting_id
            if definition.recurrence_response == "conflicting_high"
            else foundation.target_id
        )
        canonical_type = (
            _RUNTIME_TARGETS[
                case.conflicting_entity_type
                if definition.recurrence_response == "conflicting_high"
                else case.learned_entity_type
            ].canonical_ref_type
        )
        probe_foundation = _restart_foundation(
            pool=pool,
            foundation=foundation,
            canonical_type=canonical_type,
            target_id=target_id,
            restart_count=FINAL_HORIZON.restart_count,
            confidence=0.99,
        )
        runtime = await _run_collision_recurrence(
            pool=pool,
            case=case,
            foundation=probe_foundation,
            occurred_at=created_at + timedelta(minutes=40 + index),
        )
        assessment = _evaluate_arm(case=case, observation=runtime.observation)
        observations.append(
            RetentionObservation(
                case_id=f"retention-collision:{case.case_id}",
                horizon=FINAL_HORIZON,
                intervening_learning_count=FINAL_HORIZON.cycle_count,
                consumer_fate=runtime.observation.consumer_fate,
                observed_ref=runtime.observation.resolved_entity_ref,
                unsafe_globalization=(
                    HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED
                    in assessment["incidents"]
                ),
                source_observation_immutable=(
                    runtime.observation.source_observation_immutable
                ),
                models_consistent=runtime.observation.wrong_model_count == 0,
                evidence_lineage_consistent=(
                    runtime.observation.both_colliding_candidates_visible
                    and runtime.observation.none_of_above_available
                ),
                observed_safety_incidents=assessment["incidents"],
                artifact_refs=(
                    f"observation:{runtime.observation_id}",
                    f"collision-family:{case.collision_family.value}",
                ),
            )
        )

    report = evaluate_company_learning_retention(
        spec=spec,
        observations=tuple(observations),
        artifact_refs=(
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
            "deferred:remaining-five-collision-families",
        ),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(
            {
                "spec": spec.model_dump(mode="json"),
                "observations": [
                    row.model_dump(mode="json") for row in observations
                ],
                "report": report.model_dump(mode="json"),
                "report_digest": report.digest,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return report


def _exact_runtime_inputs() -> tuple[
    NegativeControlCaseDefinition,
    NegativeControlAssignment,
    SealedRecurrenceCase,
]:
    adaptive_tenant_id = uuid7()
    frozen_tenant_id = uuid7()
    adaptive_target_id = uuid7()
    frozen_target_id = uuid7()
    assignment = NegativeControlAssignment(
        case_id="retention-exact",
        adaptive_tenant_id=adaptive_tenant_id,
        frozen_tenant_id=frozen_tenant_id,
        adaptive_target_id=adaptive_target_id,
        frozen_target_id=frozen_target_id,
        adaptive_conflicting_id=uuid7(),
        frozen_conflicting_id=uuid7(),
    )
    definition = NegativeControlCaseDefinition(
        case_id="retention-exact",
        kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
        entity_type="customer",
        slack_context="cross_thread_recurrence",
        wording_variant="retention_probe",
        consequence="high",
        recurrence_distance=1,
        alias_surface="NBI",
        training_text="NBI renewal is blocked",
        training_phrase="NBI",
        candidate_alias="Nimbus Bank",
        recurrence_text="NBI remains blocked",
        recurrence_phrase="NBI",
        channel="C-RETENTION",
        resolution_scope="tenant_global_exact",
        inject_conflicting_source_hint=False,
        recurrence_response="target_low",
        expected_model_count=0,
    )
    case = SealedRecurrenceCase(
        case_id=definition.case_id,
        case_version="retention-v1",
        kind=definition.kind,
        alias_surface=definition.alias_surface,
        source_text_digest="0" * 64,
        context_digest="1" * 64,
        adaptive_expectation=SealedArmExpectation(
            tenant_id=adaptive_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type="customer",
                id=str(adaptive_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=True,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=frozen_tenant_id,
            allowed_consumer_fates=(ConsumerTerminalFate.REVIEW,),
            expected_entity_ref=CanonicalEntityRef(
                type="customer",
                id=str(frozen_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=("retention:exact-case",),
    )
    return definition, assignment, case


def _retention_spec(
    *,
    run_id: str,
    system_version: str,
    created_at: datetime,
    exact_foundation: _NegativeArmFoundation,
    variant_case: Any,
    variant_foundation: _NegativeArmFoundation,
    negative_fixture: Any,
    negative_cases: dict[str, SealedRecurrenceCase],
    collision_cases: tuple[Any, ...],
) -> RetentionRunSpec:
    cases = [
        RetentionCaseSpec(
            case_id="retention-exact",
            behavior=RetentionBehavior.EXACT_ALIAS,
            family="exact_alias_positive",
            expected_ref=CanonicalEntityRef(
                type="customer",
                id=str(exact_foundation.target_id),
            ),
            horizons=HORIZONS,
            allowed_terminal_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
        ),
        RetentionCaseSpec(
            case_id="retention-variant",
            behavior=RetentionBehavior.VARIANT_ALIAS,
            family=variant_case.variant_family.value,
            expected_ref=CanonicalEntityRef(
                type=_RUNTIME_TARGETS[
                    variant_case.entity_type
                ].canonical_ref_type,
                id=str(variant_foundation.target_id),
            ),
            horizons=HORIZONS,
            allowed_terminal_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            candidate_authorization_required=True,
        ),
        RetentionCaseSpec(
            case_id="retention-correction",
            behavior=RetentionBehavior.CORRECTED_ALIAS,
            family="authoritative_exact_correction",
            expected_ref=CanonicalEntityRef(
                type="customer",
                id=str(exact_foundation.target_id),
            ),
            horizons=(FINAL_HORIZON,),
            allowed_terminal_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            correction_authority_required=True,
        ),
    ]
    cases.extend(
        RetentionCaseSpec(
            case_id=f"retention-negative:{definition.case_id}",
            behavior=RetentionBehavior.NEGATIVE_CONTROL,
            family=definition.kind.value,
            horizons=(FINAL_HORIZON,),
            allowed_terminal_fates=(
                negative_cases[definition.case_id]
                .adaptive_expectation.allowed_consumer_fates
            ),
        )
        for definition in negative_fixture.cases
    )
    cases.extend(
        RetentionCaseSpec(
            case_id=f"retention-collision:{case.case_id}",
            behavior=RetentionBehavior.COLLISION_CONTROL,
            family=case.collision_family.value,
            horizons=(FINAL_HORIZON,),
            allowed_terminal_fates=case.allowed_safe_fates,
        )
        for case in collision_cases
    )
    return RetentionRunSpec(
        run_id=run_id,
        system_version=system_version,
        created_at=created_at.isoformat(),
        cases=tuple(cases),
        artifact_refs=(
            f"variant-population:{DEFAULT_VARIANT_POPULATION}",
            f"negative-controls:{DEFAULT_NEGATIVE_CONTROL_FIXTURE}",
            f"collision-population:{DEFAULT_COLLISION_POPULATION}",
        ),
    )


def _restart_foundation(
    *,
    pool: asyncpg.Pool,
    foundation: _NegativeArmFoundation,
    canonical_type: str,
    target_id: UUID,
    restart_count: int,
    confidence: float = 0.40,
) -> _NegativeArmFoundation:
    if restart_count == 0:
        return foundation
    provider = _ScriptedResolver(
        [
            _resolver_response(
                target_id,
                confidence=confidence,
                canonical_type=canonical_type,
            )
        ]
    )
    return replace(
        foundation,
        provider=provider,
        worker=EntityResolverWorker(
            pool=pool,
            llm=provider,
            alias_repo=foundation.alias_repo,
            corrective_memory_reuse_enabled=True,
        ),
        semantic_worker=SourceSemanticWorker(
            pool=pool,
            worker_id=(
                f"retention-restart:{foundation.tenant_id}:{restart_count}"
            ),
        ),
    )


async def _advance_unrelated_learning(
    *,
    pool: asyncpg.Pool,
    foundation: _NegativeArmFoundation,
    current_count: int,
    target_count: int,
) -> None:
    if target_count <= current_count:
        return
    for cycle in range(current_count + 1, target_count + 1):
        entity_id = uuid7()
        phrase = f"RETENTION-NOISE-{foundation.tenant_id}-{cycle}"
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO resources (
                  id, tenant_id, kind, identity, current_value, metadata
                ) VALUES (
                  $1, $2, 'capacity', $3,
                  '{"source":"retention-interference"}'::jsonb,
                  '{"semantic_kind":"workstream"}'::jsonb
                )
                """,
                entity_id,
                foundation.tenant_id,
                phrase,
            )
        await foundation.alias_repo.insert_alias(
            phrase=phrase,
            resolved_entity_ref={"type": "resource", "id": str(entity_id)},
            source="manual",
            confidence=0.99,
            tenant_id=foundation.tenant_id,
            extra_metadata={
                "retention_interference_cycle": cycle,
                "identity_basis_class": "source_authoritative",
                "identity_basis_ref": f"retention-noise:{cycle}",
            },
        )


def _result_observation(
    *,
    case_id: str,
    horizon: RetentionHorizon,
    result: Any,
    models_consistent: bool,
    evidence_lineage_consistent: bool,
    candidate_authorized: bool | None = None,
    correction_authoritative: bool | None = None,
) -> RetentionObservation:
    return RetentionObservation(
        case_id=case_id,
        horizon=horizon,
        intervening_learning_count=horizon.cycle_count,
        consumer_fate=result.consumer_fate,
        observed_ref=result.resolved_entity_ref,
        candidate_authorized=candidate_authorized,
        correction_authoritative=correction_authoritative,
        unsafe_globalization=(
            HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED
            in result.observed_safety_incidents
        ),
        source_observation_immutable=(
            HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED
            not in result.observed_safety_incidents
        ),
        models_consistent=models_consistent,
        evidence_lineage_consistent=evidence_lineage_consistent,
        observed_safety_incidents=result.observed_safety_incidents,
        artifact_refs=(
            f"observation:{result.lineage.recurrence_observation_id}",
            f"clarification:{result.lineage.clarification_request_id}",
        ),
    )


async def _result_consistency(
    *,
    pool: asyncpg.Pool,
    result: Any,
) -> tuple[bool, bool]:
    async with pool.acquire() as conn:
        model_ids = tuple(
            row["id"]
            for row in await conn.fetch(
                """
                SELECT id FROM models
                WHERE tenant_id=$1 AND born_from_event_id=$2
                ORDER BY id
                """,
                result.tenant_id,
                result.lineage.recurrence_observation_id,
            )
        )
        lineage_count = await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM observations
               WHERE tenant_id=$1 AND id=$2)
              + (SELECT count(*) FROM clarification_requests
                 WHERE tenant_id=$1 AND id=$3 AND status='answered')
              + (SELECT count(*) FROM entity_aliases
                 WHERE tenant_id=$1 AND id=$4)
            """,
            result.tenant_id,
            result.lineage.recurrence_observation_id,
            result.lineage.clarification_request_id,
            result.lineage.adjudicated_alias_id,
        )
    return (
        model_ids == result.lineage.model_ids,
        int(lineage_count or 0) == 3,
    )


async def _correction_is_authoritative(
    *,
    pool: asyncpg.Pool,
    foundation: _NegativeArmFoundation,
    phrase: str,
) -> bool:
    async with pool.acquire() as conn:
        return await validate_governed_alias_replay(
            conn,
            tenant_id=foundation.tenant_id,
            alias_id=foundation.adjudicated_alias_id,
            phrase=phrase,
            canonical_ref={
                "type": "customer",
                "id": str(foundation.target_id),
                "version": 1,
            },
            identity_basis_ref=(
                f"clarification-request:{foundation.clarification_request_id}"
            ),
            adjudication_answer_digest=foundation.clarification_answer_digest,
        )


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=8,
        init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, ROOT / "db" / "migrations")
        report = await run_company_learning_retention_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            llm_call_cost_usd=args.llm_call_cost_usd,
        )
    finally:
        await pool.close()
    print(json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True))
    return 2 if report.status == "contradicted" else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
