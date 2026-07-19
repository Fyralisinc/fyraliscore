from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.reasoning.think import applier
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import ClaimOp, ValidatedDiff
from services.reasoning.think.synthesis_contract import (
    HandleBinding,
    SynthesisCompileContext,
    SynthesisDecisionEnvelope,
    compile_synthesis_decision,
)
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_ti2_composite_rolls_back_when_sole_relation_path_fails(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
) -> None:
    member_event, member_event_2, support_event, effect_event = (
        uuid7(), uuid7(), uuid7(), uuid7()
    )
    scope_ref = "workstream:atlas-release"
    async with fresh_db.acquire() as conn:
        await conn.executemany(
            """INSERT INTO observations
                 (id,tenant_id,occurred_at,kind,source_channel,content,
                  content_text,embedding_pending,trust_tier)
               VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,TRUE,'authoritative')""",
            [(member_event, tenant, "Certificate ownership is unresolved."),
             (member_event_2, tenant, "Atlas rollout missed its planned window."),
             (support_event, tenant, "Ownership delay preceded rollout delay."),
             (effect_event, tenant, "Atlas rollout missed its planned window.")],
        )
        seed_diff = ValidatedDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[ClaimOp(op="insert", entry={
                "tenant_id": str(tenant), "born_from_event_id": str(event_id),
                "supporting_event_ids": [str(event_id)],
                "proposition": {"kind": "belief", "claim_role": "fact",
                    "abstraction_level": "atomic", "subject": "Atlas certificate",
                    "assertion": text, "scope_ref": scope_ref,
                    "evidence_event_ids": [str(event_id)]},
                "natural": text, "embedding": make_embedding(text),
                "scope_actors": [], "scope_entities": [{"type": "workstream", "id": scope_ref}],
                "scope_temporal": {}, "confidence": .8, "confidence_at_assertion": .8,
            }) for event_id, text in (
                (member_event, "Certificate ownership is unresolved."),
                (member_event_2, "Atlas rollout missed its planned window."),
            )],
        )
        async with conn.transaction():
            seed = await apply_diff(seed_diff, conn, trigger_kind="T1:event_batch",
                                    trigger_supporting_event_ids=[member_event, member_event_2])
        model_ids = [UUID(str(value)) for value in seed["applied_model_ids"]]
        rows = await conn.fetch(
            "SELECT model_id,version_id FROM model_truth_heads WHERE tenant_id=$1 AND model_id=ANY($2::uuid[])",
            tenant, model_ids,
        )
        versions = {row["model_id"]: row["version_id"] for row in rows}
        digest = "a" * 64
        context = SynthesisCompileContext(
            "atlas", digest, tenant, scope_ref, uuid7(),
            frozenset({support_event, effect_event}),
            (HandleBinding("M1", "accepted_model_head", model_ids[0], versions[model_ids[0]], tenant,
                           scope_ref, "authoritative", frozenset({"cause"})),
             HandleBinding("M2", "accepted_model_head", model_ids[1], versions[model_ids[1]], tenant,
                           scope_ref, "authoritative", frozenset({"effect"})),
             HandleBinding("O1", "observation", support_event, None, tenant, scope_ref,
                           "authoritative", frozenset({"support"})),
             HandleBinding("O2", "observation", effect_event, None, tenant, scope_ref,
                           "authoritative", frozenset({"effect"}))),
        )
        envelope = SynthesisDecisionEnvelope.model_validate({
            "schema_version": "think-synthesis-decision-v1", "dossier_id": "atlas",
            "dossier_digest": digest, "decision": {"kind": "synthesis",
                "thesis": "Unowned certificate state delayed Atlas rollout.",
                "mechanism": "The missing owner prevented certificate completion.",
                "cause_condition_handles": ["M1"], "effect_handles": ["M2"],
                "supporting_evidence_handles": ["O1"], "counterevidence": [],
                "strongest_alternative": {"thesis": "Capacity delayed rollout.",
                    "mechanism": "Capacity could constrain rollout.", "supporting_handles": [],
                    "why_weaker": "No capacity evidence."},
                "novelty": {"classification": "novel", "relative_to_model_handles": [],
                    "explanation": "No accepted mechanism Model."}, "confidence": .82,
                "falsifying_evidence": ["A completed certificate before the delay."],
                "relation": {"relation_kind": "causes", "source_handles": ["M1"],
                    "target": "synthesis_output", "direction": "source_to_target",
                    "explanation": "The prerequisite state caused the delay."}}})
        raw = compile_synthesis_decision(envelope, context=context)
        diff = ValidatedDiff.model_validate(raw.model_dump())

        async def fail_relation(*_args, **_kwargs):
            raise ValidationError("injected TI2 relation failure")

        monkeypatch.setattr(applier, "_apply_relation_claim_op", fail_relation)
        with pytest.raises(ValidationError, match="injected TI2 relation failure"):
            async with conn.transaction():
                await apply_diff(diff, conn, trigger_kind="T1:event_batch",
                                 trigger_supporting_event_ids=[support_event, effect_event])

        assert await conn.fetchval(
            """SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1
                 AND proposition->>'synthesis_contract_version'='think-synthesis-decision-v1'""",
            tenant,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM applied_triggers WHERE trigger_id=$1", diff.trigger_ref,
        ) == 0
