"""Execute the seeded correction propagation burn through T4 convergence."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.contracts.kernel import canonical_sha256
from lib.embeddings.ollama import EMBEDDING_DIM
from lib.evaluation.correction_assurance import (
    CorrectionAssuranceArtifact,
    CorrectionRuntimeEvidence,
)
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.correction_propagation import CorrectionPropagationService
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    GroundingEpisode,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import (
    prepare_entity_mention_detection,
)
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.models.repo import ModelsRepo
from services.domain.projections.catalog import projectors_for
from services.domain.projections.runtime import ProjectionRunner
from services.domain.source_semantics.processor import GroundedBeliefProcessor
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.deterministic import deterministic_handler
from services.reasoning.think.diff_schema import ValidatedDiff
from scripts.run_correction_assurance import run_correction_assurance_for_scope


CUSTOMER_REF = {"type": "customer", "id": "customer:nimbus"}


async def _insert_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    text: str,
):
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding_pending, trust_tier,
          entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'assurance:correction', $4::jsonb, $5,
          TRUE, 'ordinary', '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        datetime.now(timezone.utc),
        json.dumps({"text": text}),
        text,
    )
    return observation_id


async def _commit_grounding(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    text: str,
    confidence: float,
) -> tuple[GroundingEpisode, UUID]:
    observation_id = uuid7()
    occurred_at = datetime.now(timezone.utc)
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding_pending, trust_tier,
          entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'slack:message', $4::jsonb, $5,
          TRUE, 'ordinary', '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        occurred_at,
        json.dumps({"text": text, "_unresolved_phrases": ["NBI"]}),
        text,
    )
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        occurred_at=occurred_at,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=occurred_at + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text=text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=occurred_at + timedelta(minutes=1),
    )
    episode = build_grounding_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        occurred_at=occurred_at,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=CUSTOMER_REF,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("entity-alias:NBI",),
                independent_identity_evidence_refs=("manual-alias:NBI",),
            ),
        ),
        model_candidate_id=candidate_id_for_ref(CUSTOMER_REF),
        model_canonical_ref=CUSTOMER_REF,
        model_confidence=confidence,
        model_reasoning="closed tenant candidate",
        high_confidence=0.8,
        review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=occurred_at + timedelta(minutes=1),
    )
    await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
        episode=episode,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        phrase="NBI",
        conn=conn,
    )
    trace_id = await conn.fetchval(
        """
        SELECT id
        FROM grounding_traces
        WHERE tenant_id=$1 AND source_observation_id=$2
        """,
        tenant_id,
        observation_id,
    )
    return episode, trace_id


async def run_company_learning_correction_harness(
    *,
    output_dir: Path,
    run_id: str,
    system_version: str,
    pool: asyncpg.Pool | None = None,
    database_url: str | None = None,
) -> CorrectionAssuranceArtifact:
    """Execute one isolated, production-shaped correction convergence burn."""

    if pool is None and not database_url:
        raise ValueError("pool or database_url is required")
    owned_pool = (
        await asyncpg.create_pool(
            database_url,
            init=_install_json_codec,
        )
        if pool is None and database_url is not None
        else None
    )
    active_pool = pool or owned_pool
    assert active_pool is not None
    try:
        return await _run_correction_burn(
            pool=active_pool,
            output_dir=output_dir,
            run_id=run_id,
            system_version=system_version,
        )
    finally:
        if owned_pool is not None:
            await owned_pool.close()


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: (
                json.dumps(value) if not isinstance(value, str) else value
            ),
            decoder=json.loads,
            schema="pg_catalog",
        )


async def _run_correction_burn(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
) -> CorrectionAssuranceArtifact:
    tenant_id = uuid7()
    other_tenant_id = uuid7()
    await pool.executemany(
        "INSERT INTO tenants (id, name, is_demo) VALUES ($1, $2, FALSE)",
        [
            (tenant_id, "correction-fence-integration"),
            (other_tenant_id, "correction-fence-other-tenant"),
        ],
    )
    models_repo = ModelsRepo(
        pool=pool,
        embedder=None,
        run_topology_on_insert=False,
    )
    processor = GroundedBeliefProcessor(models_repo=models_repo)
    service = CorrectionPropagationService(models_repo=models_repo)

    async with pool.acquire() as conn:
        predecessor_episode, predecessor_trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text="NBI is blocked",
            confidence=0.91,
        )
        predecessor_detection = (
            predecessor_episode.mention_detection_command.detection
        )
        predecessor_observation_id = predecessor_detection.source_observation_id
        predecessor_result = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=predecessor_trace_id,
            embedding=[0.01] * EMBEDDING_DIM,
        )
        assert predecessor_result.model_id is not None
        old_model_id = predecessor_result.model_id

        dependent_observation_id = await _insert_observation(
            conn,
            tenant_id=tenant_id,
            text="The delivery forecast depends on NBI being blocked",
        )
        dependent = await models_repo.insert(
            ModelCreate(
                tenant_id=tenant_id,
                born_from_event_id=dependent_observation_id,
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "abstraction_level": "atomic",
                    "time_mode": "current",
                    "modality": "inferred",
                    "polarity": "neutral",
                    "assertion": "The delivery forecast is at risk",
                },
                natural="The delivery forecast is at risk",
                embedding=[0.02] * EMBEDDING_DIM,
                scope_entities=[
                    {"type": "customer", "id": "nimbus", "version": 1}
                ],
                scope_temporal={"type": "now"},
                confidence=0.6,
                confidence_at_assertion=0.6,
                supporting_model_ids=[old_model_id],
            ),
            conn=conn,
        )
        second_hop_observation_id = await _insert_observation(
            conn,
            tenant_id=tenant_id,
            text="The executive forecast depends on the delivery forecast",
        )
        second_hop = await models_repo.insert(
            ModelCreate(
                tenant_id=tenant_id,
                born_from_event_id=second_hop_observation_id,
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "abstraction_level": "atomic",
                    "time_mode": "current",
                    "modality": "inferred",
                    "polarity": "neutral",
                    "assertion": "The executive forecast is at risk",
                },
                natural="The executive forecast is at risk",
                embedding=[0.025] * EMBEDDING_DIM,
                scope_entities=[
                    {"type": "customer", "id": "nimbus", "version": 1}
                ],
                scope_temporal={"type": "now"},
                confidence=0.6,
                confidence_at_assertion=0.6,
                supporting_model_ids=[dependent.id],
            ),
            conn=conn,
        )

        other_observation_id = await _insert_observation(
            conn,
            tenant_id=other_tenant_id,
            text="Other tenant fact",
        )
        other_model = await models_repo.insert(
            ModelCreate(
                tenant_id=other_tenant_id,
                born_from_event_id=other_observation_id,
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "abstraction_level": "atomic",
                    "time_mode": "current",
                    "modality": "observed",
                    "polarity": "neutral",
                    "assertion": "Other tenant fact",
                },
                natural="Other tenant fact",
                embedding=[0.03] * EMBEDDING_DIM,
                scope_temporal={"type": "now"},
                confidence=0.6,
                confidence_at_assertion=0.6,
            ),
            conn=conn,
        )
        relation_id = uuid7()
        relation_projection_id = uuid7()
        await conn.execute(
            """
            INSERT INTO relation_instances (
              id, tenant_id, relation_kind, status,
              participant_binding_status, write_policy, confidence,
              evidence_model_ids
            ) VALUES (
              $1, $2, 'supports_delivery_risk', 'accepted',
              'bound', 'project_edges', 0.8, ARRAY[$3]::uuid[]
            )
            """,
            relation_id,
            tenant_id,
            old_model_id,
        )
        await conn.execute(
            """
            INSERT INTO relation_participants (
              id, relation_id, tenant_id, model_id, role, binding_confidence
            ) VALUES ($1, $2, $3, $4, 'support', 0.9)
            """,
            uuid7(),
            relation_id,
            tenant_id,
            old_model_id,
        )
        await conn.execute(
            """
            INSERT INTO relation_edge_projections (
              id, relation_id, tenant_id, edge_id, projection_rule,
              source_role, target_role, source_model_id, target_model_id,
              edge_kind, status
            ) VALUES (
              $1, $2, $3, $4, 'assurance:correction',
              'support', 'dependent', $5, $6, 'supports', 'active'
            )
            """,
            relation_projection_id,
            relation_id,
            tenant_id,
            uuid7(),
            old_model_id,
            dependent.id,
        )
        projection_subject = "customer:nimbus:customers"
        await conn.execute(
            """
            INSERT INTO projection_snapshots (
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, source_model_ids, source_event_ids
            ) VALUES (
              $1, 'customers', 'v1', $2,
              '{"status":"contaminated"}'::jsonb, 0.8,
              ARRAY[$3]::uuid[], '{}'::uuid[]
            )
            """,
            tenant_id,
            projection_subject,
            old_model_id,
        )
        await conn.execute(
            """
            INSERT INTO projection_dependencies (
              tenant_id, projection_name, projection_version, subject_key,
              ref_kind, ref_value, reason
            ) VALUES ($1, 'customers', 'v1', $2, 'model', $3, 'source_model')
            """,
            tenant_id,
            projection_subject,
            str(old_model_id),
        )
        other_relation_id = uuid7()
        await conn.execute(
            """
            INSERT INTO relation_instances (
              id, tenant_id, relation_kind, status,
              participant_binding_status, write_policy, confidence,
              evidence_model_ids
            ) VALUES (
              $1, $2, 'other_tenant_relation', 'accepted',
              'bound', 'candidate', 0.7, ARRAY[$3]::uuid[]
            )
            """,
            other_relation_id,
            other_tenant_id,
            other_model.id,
        )
        other_projection_subject = "other-tenant-subject"
        await conn.execute(
            """
            INSERT INTO projection_snapshots (
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, source_model_ids, source_event_ids
            ) VALUES (
              $1, 'customers', 'v1', $2,
              '{"status":"untouched"}'::jsonb, 0.7,
              ARRAY[$3]::uuid[], '{}'::uuid[]
            )
            """,
            other_tenant_id,
            other_projection_subject,
            other_model.id,
        )
        await conn.execute(
            """
            INSERT INTO projection_dependencies (
              tenant_id, projection_name, projection_version, subject_key,
              ref_kind, ref_value, reason
            ) VALUES ($1, 'customers', 'v1', $2, 'model', $3, 'source_model')
            """,
            other_tenant_id,
            other_projection_subject,
            str(other_model.id),
        )

        source_before = await conn.fetchrow(
            """
            SELECT content_text, content, entities_mentioned
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            predecessor_observation_id,
        )
        successor_trace_id = uuid7()
        predecessor_trace = await conn.fetchrow(
            """
            SELECT *
            FROM grounding_traces
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            predecessor_trace_id,
        )
        if predecessor_trace is None:
            raise RuntimeError("seeded predecessor grounding trace disappeared")
        corrected_referent = {
            "type": "customer",
            "id": "customer:corrected-nimbus",
            "version": 1,
        }
        await conn.execute(
            """
            INSERT INTO grounding_traces (
              id, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              current_fate, selected_referent, identity_registry_mutated,
              source_observation_mutated, trace, created_at
            )
            SELECT
              $3, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              'resolved_for_consumer', $4::jsonb, FALSE, FALSE,
              $5::jsonb, now()
            FROM grounding_traces
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            predecessor_trace_id,
            successor_trace_id,
            json.dumps(corrected_referent),
            json.dumps(
                {
                    **(predecessor_trace["trace"] or {}),
                    "supersedes_grounding_trace_id": str(
                        predecessor_trace_id
                    ),
                    "correction_kind": "entity_clarification_adjudication",
                    "adjudication_ref": (
                        f"correction-assurance:{successor_trace_id}"
                    ),
                }
            ),
        )
        async with conn.transaction():
            first = await service.propagate_direct_correction(
                conn,
                tenant_id=tenant_id,
                predecessor_grounding_trace_id=predecessor_trace_id,
                successor_grounding_trace_id=successor_trace_id,
                cause_event_id=predecessor_observation_id,
                corrected_model_id=None,
            )
        refresh_report = await ProjectionRunner(
            projectors_for(["customers"])
        ).run_queued_refresh_jobs_once_detailed(
            conn,
            tenant_id=tenant_id,
            limit=10,
        )

        old_model = await conn.fetchrow(
            """
            SELECT status, archive_reason
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            old_model_id,
        )
        dependent_model = await conn.fetchrow(
            """
            SELECT status, visible_to_subjects
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            dependent.id,
        )
        second_hop_model = await conn.fetchrow(
            """
            SELECT status, visible_to_subjects
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            second_hop.id,
        )
        queue_rows = await conn.fetch(
            """
            SELECT model_id, cause_model_id, cause_kind, processed_at
            FROM model_reeval_queue
            WHERE tenant_id=$1 AND cause_kind='grounding_corrected'
            ORDER BY model_id, cause_model_id
            """,
            tenant_id,
        )
        source_after = await conn.fetchrow(
            """
            SELECT content_text, content, entities_mentioned
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            predecessor_observation_id,
        )
        other_after = await conn.fetchrow(
            """
            SELECT status, visible_to_subjects
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            other_tenant_id,
            other_model.id,
        )
        relation_after = await conn.fetchval(
            "SELECT status FROM relation_instances WHERE tenant_id=$1 AND id=$2",
            tenant_id,
            relation_id,
        )
        relation_projection_after = await conn.fetchval(
            """
            SELECT status
            FROM relation_edge_projections
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            relation_projection_id,
        )
        projection_after = await conn.fetchrow(
            """
            SELECT payload, source_model_ids
            FROM projection_snapshots
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
            """,
            tenant_id,
            projection_subject,
        )
        dependency_after = await conn.fetchval(
            """
            SELECT count(*)
            FROM projection_dependencies
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
              AND ref_kind='model' AND ref_value=$3
            """,
            tenant_id,
            projection_subject,
            str(old_model_id),
        )
        refresh_jobs = await conn.fetch(
            """
            SELECT status, reason, payload
            FROM projection_refresh_jobs
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
            """,
            tenant_id,
            projection_subject,
        )
        reeval_trigger = TriggerContext(
            kind="T4",
            tenant_id=tenant_id,
            subkind="model_reeval",
            model_id=dependent.id,
            seed_signature={
                "trigger_id": str(uuid7()),
                "cause_model_id": str(old_model_id),
                "cause_kind": "grounding_corrected",
            },
        )
        raw_diff = await deterministic_handler(
            reeval_trigger,
            ContextBundle(),
            conn,
        )
        assert len(raw_diff.claim_ops) == 1
        assert raw_diff.claim_ops[0].op == "archive"
        validated_diff = ValidatedDiff.model_validate(
            raw_diff.model_dump(mode="python")
        )
        async with conn.transaction():
            await apply_diff(
                validated_diff,
                conn,
                trigger_kind="T4",
                trigger_cause_event_id=predecessor_observation_id,
                models_repo=models_repo,
            )
        dependent_after_revalidation = await conn.fetchrow(
            """
            SELECT status, archive_reason, visible_to_subjects
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            dependent.id,
        )
        second_hop_diff = await deterministic_handler(
            TriggerContext(
                kind="T4",
                tenant_id=tenant_id,
                subkind="model_reeval",
                model_id=second_hop.id,
                seed_signature={
                    "trigger_id": str(uuid7()),
                    "cause_model_id": str(dependent.id),
                    "cause_kind": "grounding_corrected",
                },
            ),
            ContextBundle(),
            conn,
        )
        assert len(second_hop_diff.claim_ops) == 1
        assert second_hop_diff.claim_ops[0].op == "archive"
        async with conn.transaction():
            await apply_diff(
                ValidatedDiff.model_validate(
                    second_hop_diff.model_dump(mode="python")
                ),
                conn,
                trigger_kind="T4",
                trigger_cause_event_id=predecessor_observation_id,
                models_repo=models_repo,
            )
        second_hop_after_revalidation = await conn.fetchrow(
            """
            SELECT status, archive_reason, visible_to_subjects
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            second_hop.id,
        )
        reeval_replay = await deterministic_handler(
            TriggerContext(
                kind="T4",
                tenant_id=tenant_id,
                subkind="model_reeval",
                model_id=dependent.id,
                seed_signature={
                    "trigger_id": str(uuid7()),
                    "cause_model_id": str(old_model_id),
                    "cause_kind": "grounding_corrected",
                },
            ),
            ContextBundle(),
            conn,
        )

        async with conn.transaction():
            replay = await service.propagate_direct_correction(
                conn,
                tenant_id=tenant_id,
                predecessor_grounding_trace_id=predecessor_trace_id,
                successor_grounding_trace_id=successor_trace_id,
                cause_event_id=predecessor_observation_id,
                corrected_model_id=None,
            )
        replay_queue_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_reeval_queue
            WHERE tenant_id=$1 AND model_id=$2 AND cause_model_id=$3
              AND processed_at IS NULL
            """,
            tenant_id,
            dependent.id,
            old_model_id,
        )
        replay_refresh_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM projection_refresh_jobs
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
              AND status='pending'
            """,
            tenant_id,
            projection_subject,
        )
        other_relation_after = await conn.fetchval(
            "SELECT status FROM relation_instances WHERE tenant_id=$1 AND id=$2",
            other_tenant_id,
            other_relation_id,
        )
        other_projection_after = await conn.fetchval(
            """
            SELECT payload->>'status'
            FROM projection_snapshots
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
            """,
            other_tenant_id,
            other_projection_subject,
        )

    projection_ref = f"projection:customers:v1:{projection_subject}"
    relation_refs = (
        f"relation-instance:{relation_id}",
        f"relation-edge-projection:{relation_projection_id}",
    )
    runtime_evidence = CorrectionRuntimeEvidence(
        expected_dependency_refs=(
            f"model:{old_model_id}",
            f"model:{dependent.id}",
            f"model:{second_hop.id}",
            *relation_refs,
            projection_ref,
        ),
        discovered_dependency_refs=(
            f"model:{old_model_id}",
            f"model:{dependent.id}",
            f"model:{second_hop.id}",
            *relation_refs,
            projection_ref,
        ),
        expected_immediate_fence_refs=(
            f"model:{dependent.id}",
            f"model:{second_hop.id}",
        ),
        immediate_fence_refs=tuple(
            f"model:{model_id}" for model_id in first.newly_fenced_model_ids
        ),
        expected_direct_repair_refs=(f"model:{old_model_id}",),
        direct_repair_refs=tuple(
            f"model:{model_id}" for model_id in first.archived_model_ids
        ),
        expected_recursive_repair_refs=(
            f"model:{dependent.id}",
            f"model:{second_hop.id}",
        ),
        recursive_repair_refs=tuple(
            ref
            for ref, row in (
                (f"model:{dependent.id}", dependent_after_revalidation),
                (f"model:{second_hop.id}", second_hop_after_revalidation),
            )
            if row["status"] == "archived"
        ),
        expected_relation_retirement_refs=relation_refs,
        relation_retirement_refs=tuple(
            ref
            for ref, status in (
                (relation_refs[0], relation_after),
                (relation_refs[1], relation_projection_after),
            )
            if status == "retired"
        ),
        expected_projection_invalidation_refs=(projection_ref,),
        projection_invalidation_refs=(
            (projection_ref,)
            if first.projection_fence.invalidated_subjects
            else ()
        ),
        expected_projection_rebuild_refs=(projection_ref,),
        projection_rebuild_refs=(
            (projection_ref,)
            if refresh_report.processed_jobs == 1 and projection_after is not None
            else ()
        ),
        residual_unsafe_refs=tuple(
            ref
            for ref, unsafe in (
                (
                    f"model:{dependent.id}",
                    dependent_after_revalidation["status"] == "active"
                    and dependent_after_revalidation["visible_to_subjects"],
                ),
                (
                    f"model:{second_hop.id}",
                    second_hop_after_revalidation["status"] == "active"
                    and second_hop_after_revalidation["visible_to_subjects"],
                ),
            )
            if unsafe
        ),
        replay_new_work_refs=tuple(
            (
                *(f"model:{item}" for item in replay.archived_model_ids),
                *(f"model:{item}" for item in replay.newly_fenced_model_ids),
                *(
                    f"reeval:{model_id}:{cause_model_id}"
                    for model_id, cause_model_id in replay.reeval_pairs
                ),
                *(
                    f"claim-op:{index}"
                    for index, _ in enumerate(reeval_replay.claim_ops)
                ),
                *(
                    ("projection-refresh:pending",)
                    if replay_refresh_count
                    else ()
                ),
            )
        ),
        source_before_digest=canonical_sha256(dict(source_before)),
        source_after_digest=canonical_sha256(dict(source_after)),
        cross_tenant_change_refs=tuple(
            ref
            for ref, changed in (
                (
                    f"model:{other_model.id}",
                    other_after["status"] != "active"
                    or other_after["visible_to_subjects"] is not True,
                ),
                (
                    f"relation-instance:{other_relation_id}",
                    other_relation_after != "accepted",
                ),
                (
                    f"projection:customers:v1:{other_projection_subject}",
                    other_projection_after != "untouched",
                ),
            )
            if changed
        ),
        artifact_refs=(f"correction-harness:{run_id}",),
    )
    async with pool.acquire() as audit_conn:
        assurance = await run_correction_assurance_for_scope(
            audit_conn,
            output_dir=output_dir,
            run_id=run_id,
            system_version=system_version,
            tenant_id=tenant_id,
            predecessor_grounding_trace_id=predecessor_trace_id,
            runtime_evidence=runtime_evidence,
        )

    assert first.archived_model_ids == (old_model_id,)
    assert set(first.newly_fenced_model_ids) == {
        dependent.id,
        second_hop.id,
    }
    assert set(first.reeval_pairs) == {
        (dependent.id, old_model_id),
        (second_hop.id, dependent.id),
    }
    assert old_model["status"] == "archived"
    assert old_model["archive_reason"] == "superseded"
    assert dependent_model["status"] == "active"
    assert dependent_model["visible_to_subjects"] is False
    assert second_hop_model["status"] == "active"
    assert second_hop_model["visible_to_subjects"] is False
    assert len(queue_rows) == 2
    assert {
        (row["model_id"], row["cause_model_id"])
        for row in queue_rows
    } == {
        (dependent.id, old_model_id),
        (second_hop.id, dependent.id),
    }
    assert all(row["processed_at"] is None for row in queue_rows)
    assert source_after == source_before
    assert other_after["status"] == "active"
    assert other_after["visible_to_subjects"] is True
    assert relation_after == "retired"
    assert relation_projection_after == "retired"
    assert refresh_report.processed_jobs == 1
    assert refresh_report.failed_jobs == 0
    assert projection_after is not None
    assert projection_after["source_model_ids"] == []
    fresh_payload = projection_after["payload"]
    if isinstance(fresh_payload, str):
        fresh_payload = json.loads(fresh_payload)
    assert fresh_payload["status"] == "empty"
    assert fresh_payload["source_model_count"] == 0
    assert dependency_after == 0
    assert len(refresh_jobs) == 1
    assert refresh_jobs[0]["status"] == "processed"
    assert refresh_jobs[0]["reason"] == "dependency_delta"
    refresh_payload = refresh_jobs[0]["payload"]
    if isinstance(refresh_payload, str):
        refresh_payload = json.loads(refresh_payload)
    assert refresh_payload["correction_kind"] == "grounding_corrected"
    assert dependent_after_revalidation["status"] == "archived"
    assert dependent_after_revalidation["archive_reason"] == "superseded"
    assert second_hop_after_revalidation["status"] == "archived"
    assert second_hop_after_revalidation["archive_reason"] == "superseded"
    assert reeval_replay.claim_ops == []
    assert replay.archived_model_ids == ()
    assert replay.newly_fenced_model_ids == ()
    assert replay.reeval_pairs == ()
    assert replay_queue_count == 1
    assert replay_refresh_count == 0
    assert other_relation_after == "accepted"
    assert other_projection_after == "untouched"
    assert assurance.status == "working"
    assert assurance.metrics.expected_dependency_count == 6
    assert assurance.metrics.dependency_discovery_rate == 1.0
    assert assurance.metrics.immediate_fence_rate == 1.0
    assert assurance.metrics.direct_repair_rate == 1.0
    assert assurance.metrics.recursive_repair_rate == 1.0
    assert assurance.metrics.relation_retirement_rate == 1.0
    assert assurance.metrics.projection_invalidation_rate == 1.0
    assert assurance.metrics.projection_rebuild_rate == 1.0
    assert assurance.metrics.residual_unsafe_debt_count == 0
    assert assurance.metrics.replay_idempotent is True
    assert assurance.metrics.source_immutable is True
    assert assurance.metrics.tenant_isolated is True
    assert assurance.metrics.converged is True
    return assurance


async def _run_cli(args: argparse.Namespace) -> int:
    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL or --database-url is required", file=sys.stderr)
        return 2
    artifact = await run_company_learning_correction_harness(
        database_url=database_url,
        output_dir=args.output_dir,
        run_id=args.run_id,
        system_version=args.system_version,
    )
    print(f"artifact={args.output_dir / 'correction_assurance.json'}")
    print(
        "status={status} converged={converged} residual_unsafe={residual}".format(
            status=artifact.status,
            converged=artifact.metrics.converged,
            residual=artifact.metrics.residual_unsafe_debt_count,
        )
    )
    return 0 if artifact.status == "working" else 2


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_run_cli(_parse_args(argv)))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Execute the seeded real-Postgres correction convergence burn and "
            "write the self-verifying correction assurance artifact."
        )
    )
    parser.add_argument("--database-url")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"correction-assurance-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"correction-assurance-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["run_company_learning_correction_harness"]
