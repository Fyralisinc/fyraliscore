from __future__ import annotations

from services.domain.models.repo import ModelsRepo
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryDecisionSet,
    PriorMemoryEffectDecision,
    build_compiled_batch_memory_decision_request,
)
from services.reasoning.think.diff_schema import ClaimOp, ValidatedDiff
from services.reasoning.think.tests.conftest import _insert_observation, make_embedding
from services.reasoning.think.validator import validate
from lib.shared.ids import uuid7

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_prior_memory_effect_survives_compile_validate_apply(
    fresh_db,
    tenant,
    tenant_cleanup,
) -> None:
    scope_ref = "workstream:atlas-release"
    scope = [{
        "type": "workstream",
        "id": scope_ref,
        "canonical_ref": scope_ref,
        "display_label": "Atlas release",
    }]
    prior_text = "Atlas release has no recorded certificate owner."
    new_text = "Atlas release still has no recorded certificate owner after review."

    async with fresh_db.acquire() as conn:
        prior_observation_id = await _insert_observation(
            conn,
            tenant,
            content_text=prior_text,
        )
        async with conn.transaction():
            seed_result = await apply_diff(
                ValidatedDiff(
                    trigger_ref=uuid7(),
                    tenant_id=tenant,
                    claim_ops=[ClaimOp(op="insert", entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(prior_observation_id),
                        "supporting_event_ids": [str(prior_observation_id)],
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "fact",
                            "abstraction_level": "atomic",
                            "subject": "Atlas release",
                            "assertion": prior_text,
                            "scope_ref": scope_ref,
                            "scope_label": "Atlas release",
                            "evidence_event_ids": [str(prior_observation_id)],
                        },
                        "natural": prior_text,
                        "embedding": make_embedding(prior_text),
                        "scope_actors": [],
                        "scope_entities": scope,
                        "scope_temporal": {},
                        "confidence": 0.65,
                        "confidence_at_assertion": 0.65,
                    })],
                ),
                conn,
                trigger_kind="T1:event_batch",
                trigger_supporting_event_ids=[prior_observation_id],
            )
        prior_model_id = seed_result["applied_model_ids"][0]
        prior_model = await ModelsRepo().get_by_id(prior_model_id, conn=conn)
        assert prior_model is not None
        prior_head = await conn.fetchrow(
            """SELECT version, version_id
                 FROM model_truth_heads
                WHERE tenant_id=$1 AND model_id=$2""",
            tenant,
            prior_model_id,
        )

        new_observation_id = await _insert_observation(
            conn,
            tenant,
            content_text=new_text,
        )
        candidate_id = "MDC_ATOM_atlas_owner_review"
        candidate = {
            "candidate_id": candidate_id,
            "candidate_kind": "atomic",
            "allowed_operations": ["claim", "no_op"],
            "entailed_claim_text": new_text,
            "proposed_text": new_text,
            "canonical_scope_ref": scope_ref,
            "semantic_scope": ["Atlas release"],
            "source_observation_ids": [str(new_observation_id)],
            "member_observation_ids": [str(new_observation_id)],
            "observation_evidence": [{
                "observation_id": str(new_observation_id),
                "body": new_text,
            }],
        }
        trigger = TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant,
            observation_id=new_observation_id,
            observation_ids=[new_observation_id],
            seed_natural_text=new_text,
        )
        context = ContextBundle(
            models=[prior_model],
            notes={
                "inquiry_context_packet": {
                    "signal_summary": new_text,
                    "memory_decision_candidates": [candidate],
                }
            },
        )
        request = build_compiled_batch_memory_decision_request(trigger, context)
        assert request is not None
        assert request.candidates[0]["prior_same_scope_model_ids"] == [
            str(prior_model_id)
        ]

        raw_diff = request.to_raw_diff(
            BatchMemoryDecisionSet(prior_memory_effects=[
                PriorMemoryEffectDecision(
                    candidate_id=candidate_id,
                    prior_model_id=prior_model_id,
                    relation="supports",
                    claim_local_evidence_event_ids=[new_observation_id],
                    reason=(
                        "The claim-local review observation directly supports the "
                        "same Atlas ownership state."
                    ),
                )
            ]),
            trigger=trigger,
            trigger_ref=uuid7(),
        )
        assert len(raw_diff.claim_ops) == 1
        assert len(raw_diff.memory_lifecycle_ops) == 1
        assert raw_diff.memory_lifecycle_ops[0].metadata["source"] == (
            "prior_memory_effect"
        )

        validated = await validate(
            raw_diff,
            RetrievalResult(trigger=trigger, models=[prior_model]),
            conn,
        )
        assert len(validated.claim_ops) == 1
        assert len(validated.memory_lifecycle_ops) == 1
        assert validated.memory_lifecycle_ops[0].claim_local_evidence_event_ids == [
            new_observation_id
        ]

        async with conn.transaction():
            apply_result = await apply_diff(
                validated,
                conn,
                trigger_kind="T1:event_batch",
                trigger_supporting_event_ids=[new_observation_id],
            )

        current_heads = await conn.fetch(
            """SELECT model_id, version, version_id
                 FROM model_truth_heads
                WHERE tenant_id=$1
                ORDER BY model_id""",
            tenant,
        )
        head_by_model = {row["model_id"]: row for row in current_heads}
        inserted_model_ids = [
            model_id
            for model_id in apply_result["applied_model_ids"]
            if model_id != prior_model_id
        ]
        assert len(inserted_model_ids) == 1
        inserted_model_id = inserted_model_ids[0]

        prior_row = await conn.fetchrow(
            """SELECT confirmed_count, supporting_event_ids
                 FROM models WHERE tenant_id=$1 AND id=$2""",
            tenant,
            prior_model_id,
        )
        inserted_row = await conn.fetchrow(
            """SELECT proposition->>'assertion' AS assertion,
                      born_from_event_id
                 FROM models WHERE tenant_id=$1 AND id=$2""",
            tenant,
            inserted_model_id,
        )

    assert head_by_model[prior_model_id]["version"] == prior_head["version"] + 1
    assert head_by_model[prior_model_id]["version_id"] != prior_head["version_id"]
    assert inserted_model_id in head_by_model
    assert head_by_model[inserted_model_id]["version"] == 1
    assert set(prior_row["supporting_event_ids"]) == {
        prior_observation_id,
        new_observation_id,
    }
    assert prior_row["confirmed_count"] == 1
    assert inserted_row["assertion"] == new_text
    assert inserted_row["born_from_event_id"] == new_observation_id
    assert apply_result["memory_lifecycle_ops"][0]["action"] == "confirm"
    assert apply_result["memory_lifecycle_ops"][0][
        "claim_local_evidence_event_ids"
    ] == [str(new_observation_id)]
