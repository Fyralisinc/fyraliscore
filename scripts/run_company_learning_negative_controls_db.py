#!/usr/bin/env python3
"""Execute the sealed company-learning negative controls on real Postgres."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Literal
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmResult,
    HardSafetyIncidentClass,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedRecurrenceCase,
    evaluate_corrective_memory_experiment,
)
from lib.shared.migrations import apply_migrations_dir
from scripts.company_learning_recurrence_runtime import (
    DEFAULT_NEGATIVE_CONTROL_FIXTURE,
    NegativeControlAssignment,
    NegativeControlCaseDefinition,
    NegativeControlExecutionPlan,
    NegativeControlExperimentEvidence,
    build_negative_control_plan,
    load_negative_control_fixture,
)
from scripts.run_company_learning_pair_harness import (
    _FrozenCorrectiveMemoryAliasRepo,
    _ScriptedResolver,
    _consumer_fate,
    _drain_tenant_semantics,
    _ingest_slack,
    _json,
    _observation_snapshot,
    _resolver_response,
)
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.clarifications import (
    answer_clarification_request,
    list_clarification_requests,
)
from services.domain.entity_aliases.repo import EntityAliasRepo, normalize_phrase
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.source_semantic_worker import SourceSemanticWorker


ARTIFACT_NAME = "company_learning_negative_controls_evidence.json"


@dataclass(frozen=True)
class _NegativeArmFoundation:
    arm: CorrectiveMemoryArm
    tenant_id: UUID
    target_id: UUID
    conflicting_id: UUID
    alias_repo: EntityAliasRepo
    worker: EntityResolverWorker
    provider: _ScriptedResolver
    semantic_worker: SourceSemanticWorker
    training_observation_id: UUID
    clarification_request_id: UUID
    clarification_answer_digest: str
    adjudicated_alias_id: UUID


@dataclass(frozen=True)
class RuntimeEntityTarget:
    """Canonical runtime storage/ref mapping for one logical entity kind."""

    canonical_ref_type: Literal["actor", "customer", "resource"]
    logical_entity_type: str
    semantic_kind: str
    actor_type: str | None = None

    def canonical_ref(self, entity_id: UUID) -> dict[str, str]:
        return {
            "type": self.canonical_ref_type,
            "id": str(entity_id),
        }


async def run_negative_control_experiment_db(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    llm_call_cost_usd: float,
    fixture_path: Path = DEFAULT_NEGATIVE_CONTROL_FIXTURE,
) -> NegativeControlExperimentEvidence:
    """Measure every sealed negative control in fresh paired tenants."""

    output_dir.mkdir(parents=True, exist_ok=True)
    fixture = load_negative_control_fixture(fixture_path)
    created_at = datetime.now(timezone.utc)
    plan = build_negative_control_plan(
        fixture,
        run_id=run_id,
        system_version=system_version,
        created_at=created_at,
        fixture_path=fixture_path,
    )
    await _assert_fresh_assignments(pool=pool, plan=plan)

    definitions = {case.case_id: case for case in fixture.cases}
    sealed_cases = {case.case_id: case for case in plan.spec.cases}
    assignments = {row.case_id: row for row in plan.assignments}
    required_case_ids = set(definitions)
    if required_case_ids != set(sealed_cases) or required_case_ids != set(assignments):
        raise RuntimeError(
            "negative-control runtime inputs do not exactly cover sealed cases"
        )

    training_at = created_at - timedelta(seconds=5)
    recurrence_at = created_at + timedelta(seconds=1)
    pairs: list[PairedRecurrenceResult] = []
    for definition in fixture.cases:
        assignment = assignments[definition.case_id]
        case = sealed_cases[definition.case_id]
        adaptive = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=assignment,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=training_at,
        )
        frozen = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=assignment,
            arm=CorrectiveMemoryArm.FROZEN,
            training_at=training_at,
        )
        adaptive_result = await _run_negative_recurrence(
            pool=pool,
            definition=definition,
            case=case,
            foundation=adaptive,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        frozen_result = await _run_negative_recurrence(
            pool=pool,
            definition=definition,
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
        pairs.append(
            PairedRecurrenceResult(
                case_id=definition.case_id,
                adaptive=adaptive_result,
                frozen=frozen_result,
                artifact_refs=(f"negative-control-pair:{definition.case_id}",),
            )
        )

    typed_pairs = tuple(pairs)
    report = evaluate_corrective_memory_experiment(
        spec=plan.spec,
        pairs=typed_pairs,
        artifact_refs=(f"report-directory:{output_dir.resolve()}",),
    )
    evidence = NegativeControlExperimentEvidence(
        executed_at=datetime.now(timezone.utc).isoformat(),
        fixture_version=fixture.fixture_version,
        fixture_digest=fixture.digest,
        plan_digest=plan.digest,
        spec=plan.spec,
        pairs=typed_pairs,
        report=report,
        artifact_refs=(
            f"fixture:{fixture_path.resolve()}",
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
        ),
    )
    _write_evidence(evidence, output_dir / ARTIFACT_NAME)
    return evidence


async def _assert_fresh_assignments(
    *,
    pool: asyncpg.Pool,
    plan: NegativeControlExecutionPlan,
) -> None:
    tenant_ids = tuple(
        tenant_id
        for assignment in plan.assignments
        for tenant_id in (
            assignment.adaptive_tenant_id,
            assignment.frozen_tenant_id,
        )
    )
    if len(tenant_ids) != len(set(tenant_ids)):
        raise RuntimeError("negative-control tenant assignments are not isolated")
    async with pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
            list(tenant_ids),
        )
    if existing:
        raise RuntimeError("negative-control execution requires fresh, unused tenants")


async def _prepare_negative_arm(
    *,
    pool: asyncpg.Pool,
    definition: NegativeControlCaseDefinition,
    assignment: NegativeControlAssignment,
    arm: CorrectiveMemoryArm,
    training_at: datetime,
    runtime_target: RuntimeEntityTarget | None = None,
    recurrence_confidence: float | None = None,
    conflicting_runtime_target: RuntimeEntityTarget | None = None,
    conflicting_target_label: str | None = None,
    training_channel: str | None = None,
    training_phrases: tuple[str, ...] | None = None,
) -> _NegativeArmFoundation:
    runtime_target = runtime_target or RuntimeEntityTarget(
        canonical_ref_type="customer",
        logical_entity_type=definition.entity_type,
        semantic_kind="customer",
    )
    conflicting_runtime_target = conflicting_runtime_target or runtime_target
    tenant_id = (
        assignment.adaptive_tenant_id
        if arm is CorrectiveMemoryArm.ADAPTIVE
        else assignment.frozen_tenant_id
    )
    target_id = (
        assignment.adaptive_target_id
        if arm is CorrectiveMemoryArm.ADAPTIVE
        else assignment.frozen_target_id
    )
    conflicting_id = (
        assignment.adaptive_conflicting_id
        if arm is CorrectiveMemoryArm.ADAPTIVE
        else assignment.frozen_conflicting_id
    )
    adjudicator_id = UUID(int=target_id.int ^ conflicting_id.int)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("INSERT INTO tenants (id) VALUES ($1)", tenant_id)
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', $3, 'active')
            """,
            adjudicator_id,
            tenant_id,
            f"Negative Control Adjudicator {definition.case_id}",
        )
        await conn.execute(
            """
            INSERT INTO actor_roles (
                tenant_id, actor_id, entity_type, entity_id, role
            ) VALUES ($1, $2, 'tenant', NULL, 'admin')
            """,
            tenant_id,
            adjudicator_id,
        )
        await _materialize_runtime_targets(
            conn=conn,
            tenant_id=tenant_id,
            target_id=target_id,
            conflicting_id=conflicting_id,
            target_label=definition.candidate_alias,
            runtime_target=runtime_target,
            conflicting_target_label=conflicting_target_label,
            conflicting_runtime_target=conflicting_runtime_target,
        )

    alias_repo = EntityAliasRepo(pool)
    await alias_repo.insert_alias(
        phrase=definition.training_phrase,
        resolved_entity_ref=runtime_target.canonical_ref(target_id),
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "logical_entity_type": runtime_target.logical_entity_type,
            "semantic_kind": runtime_target.semantic_kind,
        },
    )
    await alias_repo.insert_alias(
        phrase=f"Conflicting {definition.candidate_alias}",
        resolved_entity_ref=conflicting_runtime_target.canonical_ref(conflicting_id),
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "logical_entity_type": (conflicting_runtime_target.logical_entity_type),
            "semantic_kind": conflicting_runtime_target.semantic_kind,
            "identity_basis_class": "source_authoritative",
            "identity_basis_ref": (
                f"negative-control-fixture:{definition.case_id}:conflicting"
            ),
        },
    )
    recurrence_entity_id = (
        conflicting_id
        if definition.recurrence_response == "conflicting_high"
        else target_id
    )
    recurrence_runtime_target = (
        conflicting_runtime_target
        if definition.recurrence_response == "conflicting_high"
        else runtime_target
    )
    if recurrence_confidence is None:
        recurrence_confidence = (
            0.99
            if (
                definition.recurrence_response == "conflicting_high"
                and arm is CorrectiveMemoryArm.ADAPTIVE
            )
            else 0.40
        )
    provider = _ScriptedResolver(
        [
            _resolver_response(
                target_id,
                confidence=0.99,
                canonical_type=runtime_target.canonical_ref_type,
            ),
            _resolver_response(
                recurrence_entity_id,
                confidence=recurrence_confidence,
                canonical_type=(recurrence_runtime_target.canonical_ref_type),
            ),
        ]
    )
    worker = EntityResolverWorker(
        pool=pool,
        llm=provider,
        alias_repo=alias_repo,
        corrective_memory_reuse_enabled=(arm is CorrectiveMemoryArm.ADAPTIVE),
    )
    semantic_worker = SourceSemanticWorker(
        pool=pool,
        worker_id=f"negative-controls:{definition.case_id}:{arm.value}:{tenant_id}",
    )
    training_observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=tenant_id,
        alias_repo=alias_repo,
        text=definition.training_text,
        channel=training_channel or f"{definition.channel}-TRAIN",
        occurred_at=training_at,
        corrective_memory_reuse_enabled=True,
    )
    if training_phrases is not None:
        await _set_observation_grounding_inputs(
            pool=pool,
            tenant_id=tenant_id,
            observation_id=training_observation_id,
            phrases=training_phrases,
        )
    decision = await worker.process_observation(
        training_observation_id,
        tenant_id,
    )
    if decision != [(definition.training_phrase, "review")]:
        raise RuntimeError(
            "negative-control training did not reach sealed review: "
            f"case={definition.case_id}, arm={arm.value}, observed={decision}"
        )
    async with pool.acquire() as conn:
        requests = await list_clarification_requests(
            conn,
            tenant_id=tenant_id,
            status="open",
        )
        request = next(
            item
            for item in requests
            if item.kind == "entity_resolution"
            and item.source_observation_id == training_observation_id
        )
        answer: dict[str, Any] = {
            "action": "accept_candidate",
            "canonical_ref": request.payload["candidates"][0]["canonical_ref"],
            "confidence": 0.99,
            "resolution_scope": definition.resolution_scope,
        }
        if definition.resolution_scope == "tenant_global_exact":
            answer["confirm_tenant_global_reuse"] = True
        async with conn.transaction():
            answered = await answer_clarification_request(
                conn,
                tenant_id=tenant_id,
                request_id=request.id,
                answer=answer,
                answered_by=adjudicator_id,
            )
            if answered is None:
                raise RuntimeError("negative-control clarification disappeared")
            await adjudicate_entity_resolution_clarification(
                conn,
                clarification=answered,
                answer=answer,
                tenant_id=tenant_id,
                answered_by=adjudicator_id,
            )
        alias = await conn.fetchrow(
            """
            SELECT id, resolved_entity_ref, entity_metadata
            FROM entity_aliases
            WHERE tenant_id=$1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')=$2
            """,
            tenant_id,
            normalize_phrase(definition.training_phrase),
        )
    if alias is None:
        raise RuntimeError("adjudication did not persist corrective memory")
    alias_ref = _json(alias["resolved_entity_ref"])
    if alias_ref.get("type") != runtime_target.canonical_ref_type or alias_ref.get(
        "id"
    ) != str(target_id):
        raise RuntimeError("training correction selected the wrong target")
    metadata = _json(alias["entity_metadata"])
    if metadata.get("resolution_scope") != definition.resolution_scope:
        raise RuntimeError("persisted correction changed sealed resolution scope")
    await _drain_tenant_semantics(
        pool=pool,
        tenant_id=tenant_id,
        worker=semantic_worker,
    )
    if arm is CorrectiveMemoryArm.FROZEN:
        visible = await _FrozenCorrectiveMemoryAliasRepo(pool).fast_path_resolve_many(
            [definition.training_phrase],
            tenant_id,
        )
        if normalize_phrase(definition.training_phrase) in visible:
            raise RuntimeError("frozen arm exposed clarification-learned memory")
    return _NegativeArmFoundation(
        arm=arm,
        tenant_id=tenant_id,
        target_id=target_id,
        conflicting_id=conflicting_id,
        alias_repo=alias_repo,
        worker=worker,
        provider=provider,
        semantic_worker=semantic_worker,
        training_observation_id=training_observation_id,
        clarification_request_id=request.id,
        clarification_answer_digest=str(metadata["adjudication_answer_digest"]),
        adjudicated_alias_id=alias["id"],
    )


async def _materialize_runtime_targets(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    target_id: UUID,
    conflicting_id: UUID,
    target_label: str,
    runtime_target: RuntimeEntityTarget,
    conflicting_target_label: str | None = None,
    conflicting_runtime_target: RuntimeEntityTarget | None = None,
) -> None:
    conflicting_runtime_target = conflicting_runtime_target or runtime_target
    await _materialize_runtime_target(
        conn=conn,
        tenant_id=tenant_id,
        entity_id=target_id,
        label=target_label,
        runtime_target=runtime_target,
    )
    await _materialize_runtime_target(
        conn=conn,
        tenant_id=tenant_id,
        entity_id=conflicting_id,
        label=(conflicting_target_label or f"Conflicting {target_label}"),
        runtime_target=conflicting_runtime_target,
    )


async def _materialize_runtime_target(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    entity_id: UUID,
    label: str,
    runtime_target: RuntimeEntityTarget,
) -> None:
    metadata = {
        "source": "company_learning_evaluation",
        "logical_entity_type": runtime_target.logical_entity_type,
        "semantic_kind": runtime_target.semantic_kind,
    }
    if runtime_target.canonical_ref_type == "actor":
        await conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, status, metadata
            ) VALUES ($1, $2, $3, $4, 'active', $5::jsonb)
            """,
            entity_id,
            tenant_id,
            runtime_target.actor_type or "group",
            label,
            json.dumps(metadata, sort_keys=True),
        )
        return
    resource_kind = {
        "customer": "relational",
        "system": "infrastructure",
        "workstream": "capacity",
    }.get(runtime_target.semantic_kind, "relational")
    await conn.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $5::jsonb)
        """,
        entity_id,
        tenant_id,
        resource_kind,
        label,
        json.dumps(metadata, sort_keys=True),
    )


async def _run_negative_recurrence(
    *,
    pool: asyncpg.Pool,
    definition: NegativeControlCaseDefinition,
    case: SealedRecurrenceCase,
    foundation: _NegativeArmFoundation,
    occurred_at: datetime,
    llm_call_cost_usd: float,
) -> CorrectiveMemoryArmResult:
    observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=foundation.tenant_id,
        alias_repo=foundation.alias_repo,
        text=definition.recurrence_text,
        channel=definition.channel,
        occurred_at=occurred_at,
        corrective_memory_reuse_enabled=(
            foundation.arm is CorrectiveMemoryArm.ADAPTIVE
        ),
    )
    if definition.inject_conflicting_source_hint:
        await _inject_conflicting_source_hint(
            pool=pool,
            tenant_id=foundation.tenant_id,
            observation_id=observation_id,
            phrase=definition.recurrence_phrase,
            entity_type=definition.entity_type,
            conflicting_id=foundation.conflicting_id,
        )
    before_snapshot = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    calls_before = len(foundation.provider.calls)
    started = perf_counter()
    await foundation.worker.process_observation(
        observation_id,
        foundation.tenant_id,
    )
    if await _has_semantic_work(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    ):
        await _drain_tenant_semantics(
            pool=pool,
            tenant_id=foundation.tenant_id,
            worker=foundation.semantic_worker,
        )
    latency_ms = (perf_counter() - started) * 1000.0
    llm_calls = len(foundation.provider.calls) - calls_before
    after_snapshot = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    observed = await _recurrence_rows(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        recurrence_phrase=definition.recurrence_phrase,
    )
    trace = observed["trace"]
    entities = observed["entities"]
    models = observed["models"]
    alias_rows = observed["aliases"]
    selected = _json(trace["selected_referent"]) if trace else None
    resolved_ref = (
        CanonicalEntityRef.model_validate(selected)
        if isinstance(selected, dict) and selected.get("type") and selected.get("id")
        else None
    )
    model_output = _json(trace["model_output"]) if trace else {}
    decision_source = (
        str(model_output.get("decision_source"))
        if model_output.get("decision_source")
        else None
    )
    consumer_fate = (
        _consumer_fate(str(trace["current_fate"] or ""))
        if trace
        else ConsumerTerminalFate.ABSTAINED
    )
    if trace is None and len(entities) == 1:
        source_ref = entities[0]
        if source_ref.get("type") and source_ref.get("id"):
            resolved_ref = CanonicalEntityRef.model_validate(source_ref)
            consumer_fate = ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            decision_source = "ingest_exact_alias_fast_path"

    incidents: set[HardSafetyIncidentClass] = set()
    if before_snapshot != after_snapshot:
        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    if observed["self_authored"]:
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    if _unsafe_alias_globalization(
        definition=definition,
        foundation=foundation,
        entities=entities,
        alias_rows=alias_rows,
        decision_source=decision_source,
    ):
        incidents.add(HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED)
    expected_ref = case.expectation_for(foundation.arm).expected_entity_ref
    if (
        definition.kind is RecurrenceCaseKind.CONFLICTING_SOURCE_HINT
        and foundation.arm is CorrectiveMemoryArm.ADAPTIVE
    ):
        learned_ref = CanonicalEntityRef(
            type=definition.entity_type,
            id=str(foundation.target_id),
        )
        conflicting_ref = CanonicalEntityRef(
            type=definition.entity_type,
            id=str(foundation.conflicting_id),
        )
        visible_refs = {
            ref
            for candidate in observed["candidates"]
            if (ref := _candidate_ref(candidate)) is not None
        }
        if (
            learned_ref not in visible_refs
            or conflicting_ref not in visible_refs
            or resolved_ref == learned_ref
            or decision_source == "governed_exact_alias_replay"
        ):
            incidents.add(HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED)
    if _wrong_models(models=models, expected_ref=expected_ref):
        incidents.add(HardSafetyIncidentClass.WRONG_MODEL_FROM_REPLAY)

    return CorrectiveMemoryArmResult(
        case_id=definition.case_id,
        arm=foundation.arm,
        tenant_id=foundation.tenant_id,
        consumer_fate=consumer_fate,
        resolved_entity_ref=resolved_ref,
        decision_source=decision_source,
        llm_call_count=llm_calls,
        latency_ms=latency_ms,
        estimated_cost_usd=llm_calls * llm_call_cost_usd,
        source_semantic_admitted=any(
            str(model["semantic_work_status"]) == "belief_applied" for model in models
        ),
        lineage=ArmLineageRefs(
            training_observation_id=foundation.training_observation_id,
            recurrence_observation_id=observation_id,
            clarification_request_id=foundation.clarification_request_id,
            clarification_answer_digest=(foundation.clarification_answer_digest),
            adjudicated_alias_id=foundation.adjudicated_alias_id,
            grounding_trace_id=(trace["grounding_trace_id"] if trace else None),
            source_semantic_interpretation_id=(
                trace["interpretation_id"] if trace else None
            ),
            source_semantic_admission_id=(
                trace["semantic_admission_id"] if trace else None
            ),
            model_ids=tuple(model["id"] for model in models),
            artifact_refs=(f"observation:{observation_id}",),
        ),
        observed_safety_incidents=frozenset(incidents),
    )


async def _inject_conflicting_source_hint(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    entity_type: str,
    conflicting_id: UUID,
) -> None:
    await _set_observation_grounding_inputs(
        pool=pool,
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrases=(phrase,),
        entities=(
            {
                "type": entity_type,
                "id": str(conflicting_id),
                "version": 1,
            },
        ),
    )


async def _set_observation_grounding_inputs(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
    phrases: tuple[str, ...],
    entities: tuple[dict[str, Any], ...] | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE observations
            SET entities_mentioned=COALESCE(
                    $3::jsonb,
                    entities_mentioned
                ),
                content=jsonb_set(
                    content,
                    '{_unresolved_phrases}',
                    $4::jsonb,
                    TRUE
                )
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            observation_id,
            json.dumps(entities) if entities is not None else None,
            json.dumps(phrases),
        )


async def _has_semantic_work(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
) -> bool:
    async with pool.acquire() as conn:
        return bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM source_semantic_work_items work
                  JOIN grounding_traces trace
                    ON trace.tenant_id=work.tenant_id
                   AND trace.id=work.grounding_trace_id
                  WHERE trace.tenant_id=$1
                    AND trace.source_observation_id=$2
                )
                """,
                tenant_id,
                observation_id,
            )
        )


async def _recurrence_rows(
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
                   candidate_set.candidates,
                   assessment.model_output,
                   interpretation.id AS interpretation_id,
                   admission.id AS semantic_admission_id
            FROM grounding_traces trace
            JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
            LEFT JOIN entity_candidate_sets candidate_set
              ON candidate_set.tenant_id=trace.tenant_id
             AND candidate_set.id=trace.candidate_set_id
            LEFT JOIN source_semantic_interpretations interpretation
              ON interpretation.tenant_id=trace.tenant_id
             AND interpretation.grounding_trace_id=trace.id
            LEFT JOIN source_semantic_admission_decisions admission
              ON admission.tenant_id=interpretation.tenant_id
             AND admission.interpretation_id=interpretation.id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
            ORDER BY trace.created_at DESC, trace.id DESC
            LIMIT 1
            """,
            tenant_id,
            observation_id,
        )
        entities = (
            _json(
                await conn.fetchval(
                    """
                SELECT entities_mentioned
                FROM observations
                WHERE tenant_id=$1 AND id=$2
                """,
                    tenant_id,
                    observation_id,
                )
            )
            or []
        )
        models = await conn.fetch(
            """
            SELECT model.id, model.scope_entities,
                   COALESCE(work.status, '') AS semantic_work_status
            FROM models model
            LEFT JOIN source_semantic_interpretations interpretation
              ON interpretation.tenant_id=model.tenant_id
             AND interpretation.source_observation_id=model.born_from_event_id
            LEFT JOIN source_semantic_admission_decisions admission
              ON admission.tenant_id=interpretation.tenant_id
             AND admission.interpretation_id=interpretation.id
             AND admission.admitted_model_id=model.id
            LEFT JOIN source_semantic_work_items work
              ON work.tenant_id=interpretation.tenant_id
             AND work.grounding_trace_id=interpretation.grounding_trace_id
            WHERE model.tenant_id=$1 AND model.born_from_event_id=$2
            ORDER BY model.id
            """,
            tenant_id,
            observation_id,
        )
        alias_rows = await conn.fetch(
            """
            SELECT resolved_entity_ref, entity_metadata
            FROM entity_aliases
            WHERE tenant_id=$1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')=$2
            """,
            tenant_id,
            normalize_phrase(recurrence_phrase),
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
        "entities": [_json(item) for item in entities],
        "models": models,
        "candidates": _json(trace["candidates"]) if trace else [],
        "aliases": alias_rows,
        "self_authored": int(self_authored or 0),
    }


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


def _unsafe_alias_globalization(
    *,
    definition: NegativeControlCaseDefinition,
    foundation: _NegativeArmFoundation,
    entities: list[dict[str, Any]],
    alias_rows: list[asyncpg.Record],
    decision_source: str | None,
) -> bool:
    if definition.kind is RecurrenceCaseKind.UNRELATED_NEGATIVE_CONTROL:
        return any(
            (
                _json(row["entity_metadata"]).get("identity_basis_class")
                == "independently_adjudicated"
            )
            for row in alias_rows
        )
    if definition.kind not in {
        RecurrenceCaseKind.CONTEXTUAL_PHRASE_NEGATIVE,
        RecurrenceCaseKind.HOMONYM_LOCAL_ASSOCIATION,
    }:
        return False
    target_ref = {
        "type": definition.entity_type,
        "id": str(foundation.target_id),
    }
    source_leaked = any(
        str(ref.get("type")) == target_ref["type"]
        and str(ref.get("id")) == target_ref["id"]
        for ref in entities
    )
    metadata_unsafe = any(
        (
            (_json(row["entity_metadata"]) or {}).get("resolution_scope")
            == "tenant_global_exact"
            or (_json(row["entity_metadata"]) or {}).get("autonomous_replay_eligible")
            is True
        )
        for row in alias_rows
    )
    return (
        source_leaked
        or metadata_unsafe
        or decision_source == "governed_exact_alias_replay"
    )


def _wrong_models(
    *,
    models: list[asyncpg.Record],
    expected_ref: CanonicalEntityRef | None,
) -> bool:
    if expected_ref is None:
        return bool(models)
    expected = expected_ref.model_dump(mode="json")
    return any(_json(model["scope_entities"]) != [expected] for model in models)


async def _assert_pair_isolation(
    *,
    pool: asyncpg.Pool,
    adaptive: CorrectiveMemoryArmResult,
    frozen: CorrectiveMemoryArmResult,
) -> None:
    if adaptive.tenant_id == frozen.tenant_id:
        raise RuntimeError("paired negative controls reused one tenant")
    async with pool.acquire() as conn:
        cross_count = await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM observations
               WHERE tenant_id=$1 AND id=$4)
              + (SELECT count(*) FROM observations
                 WHERE tenant_id=$2 AND id=$3)
              + (SELECT count(*) FROM grounding_traces
                 WHERE tenant_id=$1 AND source_observation_id=$4)
              + (SELECT count(*) FROM grounding_traces
                 WHERE tenant_id=$2 AND source_observation_id=$3)
              + (SELECT count(*) FROM models
                 WHERE tenant_id=$1 AND born_from_event_id=$4)
              + (SELECT count(*) FROM models
                 WHERE tenant_id=$2 AND born_from_event_id=$3)
            """,
            adaptive.tenant_id,
            frozen.tenant_id,
            adaptive.lineage.recurrence_observation_id,
            frozen.lineage.recurrence_observation_id,
        )
    if cross_count:
        raise RuntimeError("paired negative-control tenants influenced each other")


def _write_evidence(
    evidence: NegativeControlExperimentEvidence,
    path: Path,
) -> None:
    payload = {
        **evidence.model_dump(mode="json"),
        "evidence_digest": evidence.digest,
    }
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


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
        evidence = await run_negative_control_experiment_db(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            llm_call_cost_usd=args.llm_call_cost_usd,
            fixture_path=args.fixture,
        )
    finally:
        await pool.close()
    print(json.dumps(evidence.report.model_dump(mode="json"), indent=2))
    return 2 if evidence.report.incidents else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_NEGATIVE_CONTROL_FIXTURE,
    )
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
