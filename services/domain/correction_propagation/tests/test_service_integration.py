from __future__ import annotations

import json
from datetime import datetime, timezone

import asyncpg
import pytest

from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.correction_propagation import CorrectionPropagationService
from services.domain.models.repo import ModelsRepo
from services.domain.source_semantics.processor import GroundedBeliefProcessor
from services.domain.source_semantics.tests.test_grounded_belief_vertical import (
    CUSTOMER_REF,
    _commit_grounding,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


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
          $1, $2, $3, 'signal', 'pytest:correction', $4::jsonb, $5,
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


async def test_direct_correction_fence_is_atomic_isolated_and_idempotent(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    other_tenant_id = uuid7()
    await fresh_db.executemany(
        "INSERT INTO tenants (id, name, is_demo) VALUES ($1, $2, FALSE)",
        [
            (tenant_id, "correction-fence-integration"),
            (other_tenant_id, "correction-fence-other-tenant"),
        ],
    )
    models_repo = ModelsRepo(
        pool=fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    processor = GroundedBeliefProcessor(models_repo=models_repo)
    service = CorrectionPropagationService(models_repo=models_repo)

    async with fresh_db.acquire() as conn:
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
                scope_entities=[{**CUSTOMER_REF, "version": 1}],
                scope_temporal={"type": "now"},
                confidence=0.6,
                confidence_at_assertion=0.6,
                supporting_model_ids=[old_model_id],
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
        async with conn.transaction():
            first = await service.propagate_direct_correction(
                conn,
                tenant_id=tenant_id,
                predecessor_grounding_trace_id=predecessor_trace_id,
                successor_grounding_trace_id=successor_trace_id,
                cause_event_id=predecessor_observation_id,
                corrected_model_id=None,
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
        queue_rows = await conn.fetch(
            """
            SELECT model_id, cause_model_id, cause_kind, processed_at
            FROM model_reeval_queue
            WHERE tenant_id=$1 AND model_id=$2 AND cause_model_id=$3
            """,
            tenant_id,
            dependent.id,
            old_model_id,
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

    assert first.archived_model_ids == (old_model_id,)
    assert first.newly_fenced_model_ids == (dependent.id,)
    assert first.reeval_pairs == ((dependent.id, old_model_id),)
    assert old_model["status"] == "archived"
    assert old_model["archive_reason"] == "superseded"
    assert dependent_model["status"] == "active"
    assert dependent_model["visible_to_subjects"] is False
    assert len(queue_rows) == 1
    assert queue_rows[0]["cause_kind"] == "grounding_corrected"
    assert queue_rows[0]["processed_at"] is None
    assert source_after == source_before
    assert other_after["status"] == "active"
    assert other_after["visible_to_subjects"] is True
    assert replay.archived_model_ids == ()
    assert replay.newly_fenced_model_ids == ()
    assert replay.reeval_pairs == ()
    assert replay_queue_count == 1
