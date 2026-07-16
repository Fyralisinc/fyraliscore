#!/usr/bin/env python3
"""Run independent matched adaptive-vs-frozen corrective-memory pairs."""

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
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
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
from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.clarifications import (
    answer_clarification_request,
    list_clarification_requests,
)
from services.domain.entity_aliases.repo import EntityAliasRepo, normalize_phrase
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)
from services.ingest.ingestion.core import ingest_from_draft
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.source_semantic_worker import SourceSemanticWorker


DEFAULT_CASES = (
    ("held-out-renewal", "NBI is delayed", "C-RENEWALS", 1, "belief_applied"),
    ("held-out-support", "NBI is blocked again", "C-SUPPORT", 0, "no_admission"),
    ("held-out-risk", "NBI is at risk", "C-RISK", 0, "no_admission"),
)
TRAINING_TEXT = "NBI renewal is blocked"
TRAINING_CHANNEL = "C-TRAIN"
ORDINARY_ALIAS = "OMS"
_TERMINAL_SEMANTIC_STATUSES = {"belief_applied", "no_admission", "failed_terminal"}


class _DeterministicEmbedder:
    class _Config:
        expected_dim = 768

    config = _Config()

    async def embed(self, _text: str) -> list[float]:
        return [0.01] * self.config.expected_dim


class _FrozenCorrectiveMemoryAliasRepo(EntityAliasRepo):
    """Expose ordinary aliases while hiding clarification-learned memory."""

    async def fast_path_resolve_many(
        self,
        phrases: list[str],
        tenant_id: UUID,
    ) -> dict[str, dict[str, Any]]:
        norms = tuple(
            dict.fromkeys(
                normalized
                for phrase in phrases
                if (normalized := normalize_phrase(phrase))
            )
        )
        if not norms:
            return {}
        rows = await self._pool.fetch(
            """
            SELECT
                regexp_replace(lower(alias_text), '\\s+', ' ', 'g') AS normalized,
                resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id=$1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
                  = ANY($2::text[])
              AND NOT (
                COALESCE(
                  entity_metadata ->> 'identity_basis_class'
                    = 'independently_adjudicated',
                  FALSE
                )
                AND COALESCE(
                  entity_metadata ->> 'identity_basis_ref'
                    LIKE 'clarification-request:%',
                  FALSE
                )
              )
            """,
            tenant_id,
            list(norms),
        )
        refs_by_norm: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            ref = _json(row["resolved_entity_ref"])
            key = json.dumps(ref, sort_keys=True)
            refs_by_norm.setdefault(row["normalized"], {}).setdefault(key, ref)
        return {
            norm: next(iter(refs.values()))
            for norm, refs in refs_by_norm.items()
            if len(refs) == 1
        }


class _ScriptedResolver(LLMProvider):
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        super().__init__(
            LLMConfig(provider="anthropic", api_key="synthetic", model="synthetic")
        )
        self._responses = [json.dumps(item) for item in responses]
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: dict[str, Any] | None,
    ) -> str:
        if not self._responses:
            raise AssertionError("paired harness exhausted scripted resolver responses")
        self.calls.append(
            {
                "system": system,
                "user": user,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema_hint": schema_hint,
            }
        )
        return self._responses.pop(0)


@dataclass(frozen=True)
class _CaseAssignment:
    case_id: str
    text: str
    channel: str
    adaptive_tenant_id: UUID
    frozen_tenant_id: UUID
    adaptive_customer_id: UUID
    frozen_customer_id: UUID
    expected_model_count: int
    expected_semantic_disposition: str


@dataclass(frozen=True)
class _ArmFoundation:
    arm: CorrectiveMemoryArm
    tenant_id: UUID
    adjudicator_id: UUID
    customer_id: UUID
    ordinary_resource_id: UUID
    alias_repo: EntityAliasRepo
    worker: EntityResolverWorker
    provider: _ScriptedResolver
    semantic_worker: SourceSemanticWorker
    training_observation_id: UUID
    clarification_request_id: UUID
    clarification_answer_digest: str
    adjudicated_alias_id: UUID


async def run_pair_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    llm_call_cost_usd: float,
) -> dict[str, Any]:
    """Run three independent matched pairs and persist typed evidence."""

    output_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc)
    training_at = created_at - timedelta(seconds=5)
    recurrence_at = created_at + timedelta(seconds=1)
    assignments = tuple(
        _CaseAssignment(
            case_id=case_id,
            text=text,
            channel=channel,
            adaptive_tenant_id=uuid7(),
            frozen_tenant_id=uuid7(),
            adaptive_customer_id=uuid7(),
            frozen_customer_id=uuid7(),
            expected_model_count=expected_model_count,
            expected_semantic_disposition=expected_semantic_disposition,
        )
        for (
            case_id,
            text,
            channel,
            expected_model_count,
            expected_semantic_disposition,
        ) in DEFAULT_CASES
    )

    foundation_inputs = {
        "training": {
            "text": TRAINING_TEXT,
            "channel": TRAINING_CHANNEL,
            "occurred_at": training_at.isoformat(),
        },
        "entities": (
            {"logical_id": "customer:nimbus", "identity": "Nimbus Bank"},
            {"logical_id": "resource:ordinary", "identity": "Order Management"},
        ),
        "aliases": (
            {"surface": "NBI", "logical_target": "customer:nimbus"},
            {"surface": ORDINARY_ALIAS, "logical_target": "resource:ordinary"},
        ),
    }
    provider_inputs = {
        "provider": "synthetic",
        "model": "synthetic",
        "temperature": 0.0,
        "training_response": {
            "logical_target": "customer:nimbus",
            "confidence": 0.99,
        },
        "recurrence_response": {
            "logical_target": "customer:nimbus",
            "confidence": 0.40,
        },
    }
    cases = tuple(_sealed_case(assignment) for assignment in assignments)
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id=f"corrective-memory-pair:{run_id}",
        run_id=run_id,
        system_version=system_version,
        created_at=created_at.isoformat(),
        scenario_ids=("ENTITY-CORRECTIVE-MEMORY-PAIR",),
        company_foundation_digest=canonical_sha256(foundation_inputs),
        provider_behavior_digest=canonical_sha256(provider_inputs),
        cases=cases,
        artifact_refs=(f"runner://{Path(__file__).name}",),
    )

    pairs: list[PairedRecurrenceResult] = []
    for assignment, case in zip(assignments, cases, strict=True):
        adaptive = await _prepare_arm(
            pool=pool,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            tenant_id=assignment.adaptive_tenant_id,
            customer_id=assignment.adaptive_customer_id,
            training_at=training_at,
            corrective_memory_reuse_enabled=True,
        )
        frozen = await _prepare_arm(
            pool=pool,
            arm=CorrectiveMemoryArm.FROZEN,
            tenant_id=assignment.frozen_tenant_id,
            customer_id=assignment.frozen_customer_id,
            training_at=training_at,
            corrective_memory_reuse_enabled=False,
        )
        adaptive_result = await _run_recurrence_case(
            pool=pool,
            foundation=adaptive,
            case=case,
            text=assignment.text,
            channel=assignment.channel,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
            expected_semantic_disposition=(
                assignment.expected_semantic_disposition
            ),
        )
        frozen_result = await _run_recurrence_case(
            pool=pool,
            foundation=frozen,
            case=case,
            text=assignment.text,
            channel=assignment.channel,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
            expected_semantic_disposition="no_admission",
        )
        await _assert_pair_tenant_noninterference(
            pool=pool,
            adaptive=adaptive_result,
            frozen=frozen_result,
            adaptive_customer_id=adaptive.customer_id,
            frozen_customer_id=frozen.customer_id,
        )
        pairs.append(
            PairedRecurrenceResult(
                case_id=assignment.case_id,
                adaptive=adaptive_result,
                frozen=frozen_result,
                artifact_refs=(f"pair://{assignment.case_id}",),
            )
        )

    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=tuple(pairs),
        artifact_refs=(f"report-directory:{output_dir.resolve()}",),
    )
    payload = {
        "spec": spec.model_dump(mode="json"),
        "report": report.model_dump(mode="json"),
        "report_digest": report.digest,
    }
    (output_dir / "company_learning_scenario_evidence.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )
    return payload


def _sealed_case(assignment: _CaseAssignment) -> SealedRecurrenceCase:
    return SealedRecurrenceCase(
        case_id=assignment.case_id,
        case_version="v1",
        kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
        alias_surface="NBI",
        source_text_digest=canonical_sha256(assignment.text),
        context_digest=canonical_sha256(
            {
                "channel": assignment.channel,
                "thread": None,
                "training_channel": TRAINING_CHANNEL,
            }
        ),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=assignment.adaptive_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type="customer",
                id=str(assignment.adaptive_customer_id),
            ),
            expected_model_count=assignment.expected_model_count,
            autonomous_resolution_permitted=True,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=assignment.frozen_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.REVIEW,
                ConsumerTerminalFate.ABSTAINED,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type="customer",
                id=str(assignment.frozen_customer_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=(f"case://{assignment.case_id}",),
    )


async def _prepare_arm(
    *,
    pool: asyncpg.Pool,
    arm: CorrectiveMemoryArm,
    tenant_id: UUID,
    customer_id: UUID,
    training_at: datetime,
    corrective_memory_reuse_enabled: bool,
) -> _ArmFoundation:
    adjudicator_id = uuid7()
    ordinary_resource_id = uuid7()
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING",
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'Synthetic Identity Admin', 'active')
            """,
            adjudicator_id,
            tenant_id,
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
        await conn.executemany(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value, metadata
            ) VALUES ($1, $2, 'relational', $3, $4::jsonb, $4::jsonb)
            """,
            (
                (
                    customer_id,
                    tenant_id,
                    "Nimbus Bank",
                    json.dumps({"semantic_kind": "customer"}),
                ),
                (
                    ordinary_resource_id,
                    tenant_id,
                    "Order Management",
                    json.dumps({"semantic_kind": "system"}),
                ),
            ),
        )
    alias_repo = EntityAliasRepo(pool)
    await alias_repo.insert_alias(
        phrase="NBI",
        resolved_entity_ref={"type": "customer", "id": str(customer_id)},
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
    )
    await alias_repo.insert_alias(
        phrase=ORDINARY_ALIAS,
        resolved_entity_ref={
            "type": "resource",
            "id": str(ordinary_resource_id),
        },
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
    )
    provider = _ScriptedResolver(_arm_responses(customer_id))
    worker = EntityResolverWorker(
        pool=pool,
        llm=provider,
        alias_repo=alias_repo,
        corrective_memory_reuse_enabled=corrective_memory_reuse_enabled,
    )
    semantic_worker = SourceSemanticWorker(
        pool=pool,
        worker_id=f"pair-harness:{arm.value}:{tenant_id}",
    )
    training_observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=tenant_id,
        alias_repo=alias_repo,
        text=TRAINING_TEXT,
        channel=TRAINING_CHANNEL,
        occurred_at=training_at,
        corrective_memory_reuse_enabled=True,
    )
    decision = await worker.process_observation(training_observation_id, tenant_id)
    if decision != [("NBI", "review")]:
        raise RuntimeError(f"training case did not reach review: {decision}")
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
        answer = {
            "action": "accept_candidate",
            "canonical_ref": request.payload["candidates"][0]["canonical_ref"],
            "confidence": 0.99,
            "resolution_scope": "tenant_global_exact",
            "confirm_tenant_global_reuse": True,
        }
        async with conn.transaction():
            answered = await answer_clarification_request(
                conn,
                tenant_id=tenant_id,
                request_id=request.id,
                answer=answer,
                answered_by=adjudicator_id,
            )
            if answered is None:
                raise RuntimeError("training clarification disappeared")
            await adjudicate_entity_resolution_clarification(
                conn,
                clarification=answered,
                answer=answer,
                tenant_id=tenant_id,
                answered_by=adjudicator_id,
            )
        alias = await conn.fetchrow(
            """
            SELECT id, entity_metadata
            FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='NBI'
            """,
            tenant_id,
        )
    metadata = _json(alias["entity_metadata"])
    await _drain_tenant_semantics(
        pool=pool,
        tenant_id=tenant_id,
        worker=semantic_worker,
    )
    if not corrective_memory_reuse_enabled:
        visible = await _FrozenCorrectiveMemoryAliasRepo(
            pool
        ).fast_path_resolve_many(["NBI", ORDINARY_ALIAS], tenant_id)
        if normalize_phrase("NBI") in visible:
            raise RuntimeError("frozen ingest view exposed corrective memory")
        ordinary = visible.get(normalize_phrase(ORDINARY_ALIAS))
        if ordinary != {
            "type": "resource",
            "id": str(ordinary_resource_id),
        }:
            raise RuntimeError("frozen ingest view hid an ordinary manual alias")
    return _ArmFoundation(
        arm=arm,
        tenant_id=tenant_id,
        adjudicator_id=adjudicator_id,
        customer_id=customer_id,
        ordinary_resource_id=ordinary_resource_id,
        alias_repo=alias_repo,
        worker=worker,
        provider=provider,
        semantic_worker=semantic_worker,
        training_observation_id=training_observation_id,
        clarification_request_id=request.id,
        clarification_answer_digest=str(metadata["adjudication_answer_digest"]),
        adjudicated_alias_id=alias["id"],
    )


async def _run_recurrence_case(
    *,
    pool: asyncpg.Pool,
    foundation: _ArmFoundation,
    case: SealedRecurrenceCase,
    text: str,
    channel: str,
    occurred_at: datetime,
    llm_call_cost_usd: float,
    expected_semantic_disposition: str,
) -> CorrectiveMemoryArmResult:
    observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=foundation.tenant_id,
        alias_repo=foundation.alias_repo,
        text=text,
        channel=channel,
        occurred_at=occurred_at,
        corrective_memory_reuse_enabled=(
            foundation.arm is CorrectiveMemoryArm.ADAPTIVE
        ),
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
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT trace.id AS grounding_trace_id,
                   trace.current_fate,
                   trace.selected_referent,
                   assessment.model_output,
                   interpretation.id AS interpretation_id,
                   admission.id AS semantic_admission_id,
                   admission.disposition
            FROM grounding_traces trace
            JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
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
            foundation.tenant_id,
            observation_id,
        )
        model_ids = tuple(
            record["id"]
            for record in await conn.fetch(
                """
                SELECT id FROM models
                WHERE tenant_id=$1 AND born_from_event_id=$2
                ORDER BY id
                """,
                foundation.tenant_id,
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
            foundation.tenant_id,
            str(observation_id),
        )
    selected = _json(row["selected_referent"]) if row else None
    resolved_ref = (
        CanonicalEntityRef.model_validate(selected)
        if isinstance(selected, dict) and selected.get("type") and selected.get("id")
        else None
    )
    model_output = _json(row["model_output"]) if row else {}
    disposition = str(row["disposition"] or "") if row else ""
    if disposition != expected_semantic_disposition:
        raise RuntimeError(
            "source-semantic disposition did not match sealed case gold: "
            f"expected={expected_semantic_disposition}, observed={disposition}"
        )
    incidents: set[HardSafetyIncidentClass] = set()
    if self_authored:
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    if before_snapshot != after_snapshot:
        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    return CorrectiveMemoryArmResult(
        case_id=case.case_id,
        arm=foundation.arm,
        tenant_id=foundation.tenant_id,
        consumer_fate=_consumer_fate(
            str(row["current_fate"] or "") if row else ""
        ),
        resolved_entity_ref=resolved_ref,
        decision_source=(
            str(model_output.get("decision_source"))
            if model_output.get("decision_source")
            else None
        ),
        llm_call_count=llm_calls,
        latency_ms=latency_ms,
        estimated_cost_usd=llm_calls * llm_call_cost_usd,
        source_semantic_admitted=disposition == "belief_applied",
        lineage=ArmLineageRefs(
            training_observation_id=foundation.training_observation_id,
            recurrence_observation_id=observation_id,
            clarification_request_id=foundation.clarification_request_id,
            clarification_answer_digest=foundation.clarification_answer_digest,
            adjudicated_alias_id=foundation.adjudicated_alias_id,
            grounding_trace_id=(
                row["grounding_trace_id"] if row is not None else None
            ),
            source_semantic_interpretation_id=(
                row["interpretation_id"] if row is not None else None
            ),
            source_semantic_admission_id=(
                row["semantic_admission_id"] if row is not None else None
            ),
            model_ids=model_ids,
            artifact_refs=(f"observation:{observation_id}",),
        ),
        observed_safety_incidents=frozenset(incidents),
    )


async def _drain_tenant_semantics(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    worker: SourceSemanticWorker,
) -> None:
    for _ in range(12):
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT status FROM source_semantic_work_items
                WHERE tenant_id=$1
                """,
                tenant_id,
            )
        statuses = {str(row["status"]) for row in rows}
        if statuses and statuses.issubset(_TERMINAL_SEMANTIC_STATUSES):
            if "failed_terminal" in statuses:
                raise RuntimeError("source-semantic work failed terminally")
            return
        await worker.process_batch(limit=1000)
    raise RuntimeError("source-semantic work did not reach a terminal barrier")


async def _observation_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT occurred_at, kind, source_channel, source_actor_ref,
                   content, content_text, entities_mentioned, trust_tier,
                   cause_id, external_id
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            observation_id,
        )
    if row is None:
        raise RuntimeError("recurrence observation disappeared")
    snapshot = dict(row)
    snapshot["content"] = _json(snapshot["content"])
    snapshot["entities_mentioned"] = _json(snapshot["entities_mentioned"])
    return snapshot


async def _assert_pair_tenant_noninterference(
    *,
    pool: asyncpg.Pool,
    adaptive: CorrectiveMemoryArmResult,
    frozen: CorrectiveMemoryArmResult,
    adaptive_customer_id: UUID,
    frozen_customer_id: UUID,
) -> None:
    adaptive_observation = adaptive.lineage.recurrence_observation_id
    frozen_observation = frozen.lineage.recurrence_observation_id
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
            adaptive_observation,
            frozen_observation,
        )
        adaptive_alias = await conn.fetchval(
            """
            SELECT resolved_entity_ref FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='NBI'
            """,
            adaptive.tenant_id,
        )
        frozen_alias = await conn.fetchval(
            """
            SELECT resolved_entity_ref FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='NBI'
            """,
            frozen.tenant_id,
        )
    if cross_count:
        raise RuntimeError("paired tenants influenced each other's recurrence rows")
    if _json(adaptive_alias).get("id") != str(adaptive_customer_id):
        raise RuntimeError("adaptive alias escaped its tenant assignment")
    if _json(frozen_alias).get("id") != str(frozen_customer_id):
        raise RuntimeError("frozen alias escaped its tenant assignment")


async def _ingest_slack(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    alias_repo: EntityAliasRepo,
    text: str,
    channel: str,
    occurred_at: datetime,
    corrective_memory_reuse_enabled: bool,
) -> UUID:
    payload = {
        "team_id": f"T-{tenant_id}",
        "event": {
            "type": "message",
            "user": "U-SYNTHETIC",
            "text": text,
            "ts": f"{occurred_at.timestamp():.6f}",
            "channel": channel,
            "channel_type": "channel",
        },
    }
    draft = await handle_slack_message(payload, {})
    result = await ingest_from_draft(
        channel="slack:message",
        draft=draft,
        pool=pool,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=(
            alias_repo
            if corrective_memory_reuse_enabled
            else _FrozenCorrectiveMemoryAliasRepo(pool)
        ),
        embedder=_DeterministicEmbedder(),
    )
    if result.trigger_queue_id is not None:
        raise RuntimeError("grounding-owned Slack signal enqueued competing Think")
    return result.observation.id


def _arm_responses(customer_id: UUID) -> list[dict[str, Any]]:
    return [
        _resolver_response(customer_id, confidence=0.99),
        _resolver_response(customer_id, confidence=0.40),
    ]


def _resolver_response(
    entity_id: UUID,
    *,
    confidence: float,
    canonical_type: str = "customer",
) -> dict[str, Any]:
    return {
        "canonical_ref": {"type": canonical_type, "id": str(entity_id)},
        "confidence": confidence,
        "reasoning": "matched paired corrective-memory case",
    }


def _consumer_fate(current_fate: str) -> ConsumerTerminalFate:
    if current_fate == "resolved_for_consumer":
        return ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
    if current_fate == "review":
        return ConsumerTerminalFate.REVIEW
    if current_fate in {"unresolved", "abstained"}:
        return ConsumerTerminalFate.ABSTAINED
    if current_fate == "rejected":
        return ConsumerTerminalFate.REJECTED
    return ConsumerTerminalFate.INCOMPLETE


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


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
        payload = await run_pair_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            llm_call_cost_usd=args.llm_call_cost_usd,
        )
    finally:
        await pool.close()
    print(json.dumps(payload["report"], indent=2, sort_keys=True))
    return 2 if payload["report"]["incidents"] else 0


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
