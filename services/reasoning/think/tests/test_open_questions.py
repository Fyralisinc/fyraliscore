"""Open-question facet tests for the Model layer."""
from __future__ import annotations

import json
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.models.open_questions import (
    ModelOpenQuestionCreate,
    ModelOpenQuestionsRepo,
)
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import ClaimOp, OpenQuestionOp, RawDiff, ValidatedDiff
from services.reasoning.think.tests.conftest import make_embedding
from services.reasoning.think.validator import validate


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _retrieval_result(tenant_id: UUID) -> RetrievalResult:
    return RetrievalResult(
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        models=[],
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        pathway_results=[],
        notes={},
        model_scores={},
    )


async def _insert_model(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    natural: str = "Cash runway constrains the operating plan.",
) -> tuple[UUID, UUID]:
    actor_id = uuid7()
    observation_id = uuid7()
    model_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Open Question Tester', 'active')
        """,
        actor_id,
        tenant_id,
    )
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                $4::jsonb, $5, $6, FALSE, 'authoritative')
        """,
        observation_id,
        tenant_id,
        actor_id,
        json.dumps({"text": natural}),
        natural,
        make_embedding(natural),
    )
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                $9::jsonb, 0.66, 1.0, 'active', 0.66, 1.0)
        """,
        model_id,
        tenant_id,
        observation_id,
        json.dumps(
            {
                "kind": "belief",
                "assertion": natural,
                "domain_tags": ["runway", "constraint"],
            },
        ),
        natural,
        make_embedding(natural),
        [],
        json.dumps([{"type": "company", "id": str(tenant_id)}]),
        json.dumps({"type": "current"}),
    )
    return model_id, observation_id


async def test_open_question_repo_dedup_search_and_resolution(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    repo = ModelOpenQuestionsRepo()
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            model_id, observation_id = await _insert_model(conn, tenant)
            proposed = ModelOpenQuestionCreate(
                tenant_id=tenant,
                model_id=model_id,
                question="Which concrete cash event resolves the runway constraint?",
                question_type="constraint_boundary",
                rationale="The belief needs a boundary before it can guide planning.",
                priority=0.82,
                expected_resolution_signal={"signal_shape": "cash receipt or burn update"},
                search_signature={"terms": ["runway", "cash receipt", "burn"]},
                source_event_id=observation_id,
            )

            first = await repo.insert(conn, proposed)
            duplicate = await repo.insert(conn, proposed)
            listed = await repo.list_for_model(
                conn,
                tenant_id=tenant,
                model_id=model_id,
            )
            due = await repo.list_due_for_search(
                conn,
                tenant_id=tenant,
                question_ids=[first.id],
            )
            marked = await repo.mark_searched(
                conn,
                question_ids=[first.id],
                backoff=timedelta(minutes=5),
            )
            due_after_mark = await repo.list_due_for_search(
                conn,
                tenant_id=tenant,
                question_ids=[first.id],
            )
            resolved = await repo.resolve(
                conn,
                tenant_id=tenant,
                question_id=first.id,
                resolution_note="A later model resolved the boundary.",
            )

            count = await conn.fetchval(
                """
                SELECT count(*)
                FROM model_open_questions
                WHERE tenant_id = $1 AND model_id = $2
                """,
                tenant,
                model_id,
            )

    assert first.id == duplicate.id
    assert count == 1
    assert listed == [first]
    assert due == [first]
    assert marked == 1
    assert due_after_mark == []
    assert resolved is not None
    assert resolved.status == "resolved"
    assert resolved.resolution_note == "A later model resolved the boundary."
    assert first.source_model_ids == [model_id]


async def test_validator_accepts_and_normalizes_open_question_ops(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            model_id, _ = await _insert_model(conn, tenant)
            diff = RawDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant,
                open_question_ops=[
                    OpenQuestionOp(
                        op="insert",
                        model_id=model_id,
                        question="Which leader owns resolving the runway constraint?",
                        question_type="OWNER or decision",
                        rationale="Ownership changes how the model projects into acts.",
                        priority=0.72,
                        expected_resolution_signal={"signal_shape": "named owner"},
                        search_signature={"terms": ["owner", "runway decision"]},
                        source_model_ids=[model_id],
                    ),
                ],
            )

            validated = await validate(
                diff,
                _retrieval_result(tenant),
                conn,
                allowed_region=None,
            )

    assert len(validated.open_question_ops) == 1
    op = validated.open_question_ops[0]
    assert op.question == "Which leader owns resolving the runway constraint?"
    assert op.question_type == "owner_or_decision"
    assert op.priority == 0.72
    assert op.source_model_ids == [model_id]


async def test_validator_drops_invalid_open_question_but_keeps_valid_ops(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            model_id, _ = await _insert_model(conn, tenant)
            diff = RawDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant,
                open_question_ops=[
                    OpenQuestionOp(
                        op="insert",
                        model_id=model_id,
                        question="Too short",
                        question_type="evidence_gap",
                    ),
                ],
                claim_ops=[
                    ClaimOp(
                        op="update",
                        model_id=model_id,
                        changes={"confidence": 0.64},
                    ),
                ],
            )

            validated = await validate(
                diff,
                _retrieval_result(tenant),
                conn,
                allowed_region=None,
            )

    assert validated.open_question_ops == []
    assert len(validated.claim_ops) == 1
    assert validated.dropped_op_count == 1
    assert validated.dropped_op_errors == [
        "open_question_op insert: open_question_op insert requires question text"
    ]


async def test_apply_open_question_insert_and_resolve_emit_model_events(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            model_id, observation_id = await _insert_model(conn, tenant)
            insert_diff = ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant,
                open_question_ops=[
                    OpenQuestionOp(
                        op="insert",
                        model_id=model_id,
                        question="What evidence narrows the cash runway constraint?",
                        question_type="constraint_boundary",
                        rationale="The belief needs a sharper boundary.",
                        priority=0.77,
                        search_signature={"terms": ["cash runway", "constraint boundary"]},
                    ),
                ],
            )

            insert_result = await apply_diff(
                insert_diff,
                conn,
                "T1",
                observation_id,
            )
            question_id = UUID(
                insert_result["open_question_ops"][0]["open_question_id"],
            )

            resolve_diff = ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant,
                open_question_ops=[
                    OpenQuestionOp(
                        op="resolve",
                        model_id=model_id,
                        question_id=question_id,
                        resolution_note="Resolved by a later finance signal.",
                    ),
                ],
            )
            resolve_result = await apply_diff(
                resolve_diff,
                conn,
                "T1",
                observation_id,
            )

            row = await conn.fetchrow(
                """
                SELECT status, resolution_note
                FROM model_open_questions
                WHERE id = $1 AND tenant_id = $2
                """,
                question_id,
                tenant,
            )
            event_count = await conn.fetchval(
                """
                SELECT count(*)
                FROM model_events
                WHERE tenant_id = $1
                  AND model_id = $2
                  AND event_type = 'model.open_question_changed'
                """,
                tenant,
                model_id,
            )

    assert insert_result["open_question_ops"][0]["op"] == "insert"
    assert resolve_result["open_question_ops"][0]["op"] == "resolve"
    assert row["status"] == "resolved"
    assert row["resolution_note"] == "Resolved by a later finance signal."
    assert event_count == 2
    assert insert_result["state_changes_emitted"] >= 1
    assert resolve_result["state_changes_emitted"] >= 1
