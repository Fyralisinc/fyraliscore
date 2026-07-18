from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think import applier
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    RelationObligation,
)
from services.reasoning.think.diff_schema import ClaimOp, ValidatedDiff
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_coupled_composite_rolls_back_when_governed_relation_apply_fails(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
) -> None:
    member_events = [uuid7(), uuid7()]
    synthesis_event, relation_event = uuid7(), uuid7()
    event_text = {
        member_events[0]: "Atlas approval has no owner.",
        member_events[1]: "Atlas release cannot proceed without approval.",
        synthesis_event: "Missing approval ownership blocks Atlas release.",
        relation_event: "The open approval dependency blocks release completion.",
    }
    scope = [{
        "type": "workstream",
        "id": "workstream:atlas-release",
        "canonical_ref": "workstream:atlas-release",
        "display_label": "Atlas release",
    }]

    async with fresh_db.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO observations
              (id,tenant_id,occurred_at,kind,source_channel,content,
               content_text,embedding_pending,trust_tier)
            VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,TRUE,
                    'authoritative')
            """,
            [(event_id, tenant, text) for event_id, text in event_text.items()],
        )
        seed_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(event_id),
                    "supporting_event_ids": [str(event_id)],
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "abstraction_level": "atomic",
                        "subject": "Atlas release",
                        "assertion": event_text[event_id],
                        "scope_ref": "workstream:atlas-release",
                        "scope_label": "Atlas release",
                        "evidence_event_ids": [str(event_id)],
                    },
                    "natural": event_text[event_id],
                    "embedding": make_embedding(event_text[event_id]),
                    "scope_actors": [],
                    "scope_entities": scope,
                    "scope_temporal": {},
                    "confidence": 0.7,
                    "confidence_at_assertion": 0.7,
                })
                for event_id in member_events
            ],
        )
        async with conn.transaction():
            seed = await apply_diff(
                seed_diff,
                conn,
                trigger_kind="T1:event_batch",
                trigger_supporting_event_ids=member_events,
            )
        member_ids = [UUID(str(value)) for value in seed["applied_model_ids"]]
        head_rows = await conn.fetch(
            """SELECT model_id,version_id FROM model_truth_heads
                 WHERE tenant_id=$1 AND model_id=ANY($2::uuid[])""",
            tenant,
            member_ids,
        )
        version_by_model = {
            row["model_id"]: row["version_id"] for row in head_rows
        }

        candidate = {
            "candidate_id": "MDC_SYNTH_ATLAS",
            "candidate_kind": "synthesis",
            "allowed_operations": ["situation_and_edge", "no_op"],
            "op_family": "claim_insert",
            "proposed_text": event_text[synthesis_event],
            "semantic_scope": ["Atlas release"],
            "canonical_scope_ref": "workstream:atlas-release",
            "member_observation_ids": [str(synthesis_event)],
            "relation_evidence_observation_ids": [str(relation_event)],
            "evidence_model_ids": [str(value) for value in member_ids],
            "endpoint_model_versions": {
                str(model_id): str(version_by_model[model_id])
                for model_id in member_ids
            },
            "confidence": 0.8,
        }
        request = CompiledBatchMemoryDecisionRequest(
            system="system",
            user="user",
            candidates=(candidate,),
            relation_obligations=(RelationObligation(
                candidate_id="MDC_SYNTH_ATLAS",
                edge_kind="blocks",
                confidence=0.8,
                source_model_id=member_ids[0],
                target_model_id=member_ids[1],
                evidence_event_ids=(relation_event,),
                evidence_model_ids=tuple(member_ids),
                evidence_text=event_text[relation_event],
                explanation="The approval dependency blocks completion.",
                matched_markers=("blocks",),
            ),),
        )
        raw = request.to_raw_diff(
            BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
                candidate_id="MDC_SYNTH_ATLAS",
                decision="accept",
                operation="situation_and_edge",
                confidence=0.8,
                claim_role="situation",
                claim_text=event_text[synthesis_event],
                situation_member_model_ids=member_ids,
                source_model_id=member_ids[0],
                target_model_id=member_ids[1],
                reason="Exact accepted heads support the dependency.",
            )]),
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                observation_ids=[synthesis_event, relation_event],
            ),
            trigger_ref=uuid7(),
        )
        diff = ValidatedDiff.model_validate(raw.model_dump())
        assert diff.relation_claim_ops[0].metadata["atomic_with_synthesis"] is True

        async def fail_governed_relation(*_args, **_kwargs):
            raise ValidationError("injected governed relation admission failure")

        monkeypatch.setattr(applier, "_apply_relation_claim_op", fail_governed_relation)

        with pytest.raises(
            ValidationError,
            match="injected governed relation admission failure",
        ):
            async with conn.transaction():
                await apply_diff(
                    diff,
                    conn,
                    trigger_kind="T1:event_batch",
                    trigger_supporting_event_ids=[synthesis_event, relation_event],
                )

        assert await conn.fetchval(
            """SELECT count(*) FROM model_truth_versions
                 WHERE tenant_id=$1
                   AND proposition->>'abstraction_level'='composite'""",
            tenant,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM relation_truth_versions WHERE tenant_id=$1",
            tenant,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM applied_triggers WHERE trigger_id=$1",
            diff.trigger_ref,
        ) == 0
