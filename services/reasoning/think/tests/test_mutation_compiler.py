"""Regression tests for the Think mutation compiler."""
from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.domain.models.edges_repo import EdgesRepo
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    RawDiff,
    RelationClaimOp,
)
from services.reasoning.think.mutation_compiler import compile_raw_diff_mutations
from services.reasoning.think.validator import validate

from .conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _seed_observation(conn, tenant_id: UUID, text: str = "signal") -> UUID:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Compiler Test', 'active')
        """,
        actor_id,
        tenant_id,
    )
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                '{}'::jsonb, $4, $5, FALSE, 'authoritative')
        """,
        observation_id,
        tenant_id,
        actor_id,
        text,
        make_embedding(text),
    )
    return observation_id


async def _seed_model(
    conn,
    tenant_id: UUID,
    *,
    natural: str,
    confidence: float = 0.78,
) -> tuple[UUID, UUID]:
    observation_id = await _seed_observation(conn, tenant_id, natural)
    model_id = uuid7()
    proposition = {
        "kind": "belief",
        "subject": natural[:80],
        "assertion": natural,
        "confidence": confidence,
    }
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, $7, 1.0, 'active', $7, 1.0)
        """,
        model_id,
        tenant_id,
        observation_id,
        json.dumps(proposition),
        natural,
        make_embedding(natural),
        confidence,
    )
    return model_id, observation_id


def _retrieval_result(tenant_id: UUID, *, models: list[object] | None = None):
    return RetrievalResult(
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        models=models or [],
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        pathway_results=[],
        notes={},
        model_scores={},
    )


async def test_compiler_rewrites_duplicate_insert_and_remaps_pending_basis(
    fresh_db,
    tenant,
):
    natural = "Beacon renewal risk is increasing because approval is late."
    async with fresh_db.acquire() as conn:
        existing_model_id, _ = await _seed_model(
            conn,
            tenant,
            natural=natural,
            confidence=0.82,
        )
        signal_id = await _seed_observation(conn, tenant, "new duplicate signal")
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(signal_id),
                        "proposition": {
                            "kind": "belief",
                            "subject": natural[:80],
                            "assertion": natural,
                        },
                        "natural": natural,
                        "embedding": make_embedding(natural),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.74,
                        "confidence_at_assertion": 0.74,
                    },
                )
            ],
            act_ops=[
                ActOp(
                    op="create_decision",
                    confidence_basis=signal_id,
                    entity={
                        "title": "Review Beacon renewal approval",
                        "decision_text": "Review the late approval path.",
                        "scope": {},
                    },
                )
            ],
        )

        compiled, summary = await compile_raw_diff_mutations(
            diff,
            conn=conn,
            retrieval_result=_retrieval_result(tenant),
            bundle=ContextBundle(),
        )
        validated = await validate(compiled, _retrieval_result(tenant), conn)

    assert summary.duplicate_inserts_rewritten == 1
    assert summary.model_refs_remapped == 1
    assert compiled.claim_ops == []
    assert len(compiled.memory_lifecycle_ops) == 1
    assert compiled.memory_lifecycle_ops[0].model_id == existing_model_id
    assert compiled.act_ops[0].confidence_basis == existing_model_id
    assert validated.dropped_op_count == 0
    assert len(validated.memory_lifecycle_ops) == 1
    assert len(validated.act_ops) == 1


async def test_compiler_binds_missing_act_basis_to_retrieved_model(
    fresh_db,
    tenant,
):
    async with fresh_db.acquire() as conn:
        model_id, _ = await _seed_model(
            conn,
            tenant,
            natural="Approval risk supports a decision review.",
            confidence=0.82,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="create_decision",
                    confidence_basis=None,
                    entity={
                        "title": "Review approval risk",
                        "decision_text": "Decide whether to escalate approval.",
                        "scope": {},
                    },
                )
            ],
        )
        retrieval = _retrieval_result(tenant, models=[SimpleNamespace(id=model_id)])

        compiled, summary = await compile_raw_diff_mutations(
            diff,
            conn=conn,
            retrieval_result=retrieval,
            bundle=ContextBundle(models=[SimpleNamespace(id=model_id)]),
        )
        validated = await validate(compiled, retrieval, conn)

    assert summary.act_ops_bound_confidence_basis == 1
    assert compiled.act_ops[0].confidence_basis == model_id
    assert validated.dropped_op_count == 0
    assert len(validated.act_ops) == 1


async def test_compiler_downgrades_relation_claim_cycle_to_needs_review(
    fresh_db,
    tenant,
):
    async with fresh_db.acquire() as conn:
        source_id, source_obs = await _seed_model(
            conn,
            tenant,
            natural="Source approval depends on target progress.",
        )
        target_id, _ = await _seed_model(
            conn,
            tenant,
            natural="Target progress already depends on source approval.",
        )
        await EdgesRepo().link(
            conn,
            source=target_id,
            target=source_id,
            kind="supports",
            tenant_id=tenant,
            detected_by="manual",
            created_by_event_id=source_obs,
            explanation="Existing direction.",
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    source_model_id=source_id,
                    target_model_id=target_id,
                    subject_ref={"kind": "model", "model_id": str(source_id)},
                    object_ref={"kind": "model", "model_id": str(target_id)},
                    predicate="supports",
                    edge_kind="supports",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.9,
                    binding_confidence=0.9,
                    evidence_event_ids=[source_obs],
                    explanation="Would close a supports cycle.",
                )
            ],
        )

        compiled, summary = await compile_raw_diff_mutations(
            diff,
            conn=conn,
            retrieval_result=_retrieval_result(tenant),
            bundle=ContextBundle(),
        )
        validated = await validate(compiled, _retrieval_result(tenant), conn)

    assert summary.relation_claims_downgraded_for_cycle == 1
    assert compiled.relation_claim_ops[0].write_policy == "needs_review"
    assert compiled.relation_claim_ops[0].metadata["mutation_compiler_cycle_guard"]
    assert validated.dropped_op_count == 0
    assert validated.relation_claim_ops[0].write_policy == "needs_review"


async def test_compiler_removes_self_edges_before_validation(
    fresh_db,
    tenant,
):
    async with fresh_db.acquire() as conn:
        model_id, observation_id = await _seed_model(
            conn,
            tenant,
            natural="A self relation should not become graph structure.",
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=model_id,
                    target_model_id=model_id,
                    edge_kind="supports",
                    evidence_event_ids=[observation_id],
                    explanation="Invalid self edge.",
                )
            ],
            relation_claim_ops=[
                RelationClaimOp(
                    source_model_id=model_id,
                    target_model_id=model_id,
                    subject_ref={"kind": "model", "model_id": str(model_id)},
                    object_ref={"kind": "model", "model_id": str(model_id)},
                    predicate="supports",
                    edge_kind="supports",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    evidence_event_ids=[observation_id],
                    explanation="Invalid self relation.",
                )
            ],
            claim_ops=[
                ClaimOp(op="update", model_id=model_id, changes={"confidence": 0.8})
            ],
        )

        compiled, summary = await compile_raw_diff_mutations(
            diff,
            conn=conn,
            retrieval_result=_retrieval_result(tenant),
            bundle=ContextBundle(),
        )
        validated = await validate(compiled, _retrieval_result(tenant), conn)

    assert summary.edge_ops_dropped_self_edge == 1
    assert summary.relation_claims_dropped_self_edge == 1
    assert compiled.edge_ops == []
    assert compiled.relation_claim_ops == []
    assert validated.dropped_op_count == 0
    assert len(validated.claim_ops) == 1
