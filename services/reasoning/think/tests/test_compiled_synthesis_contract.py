from __future__ import annotations

from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
)


def test_accepted_synthesis_edge_also_materializes_hypothesis_model() -> None:
    tenant_id, trigger_id = uuid4(), uuid4()
    observation_ids = [uuid4(), uuid4()]
    prior_models = [uuid4(), uuid4()]
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant_id, observation_ids=observation_ids,
        seed_signature={"trigger_id": str(trigger_id)},
    )
    candidate = {
        "candidate_id": "MDC_SYNTH_scope",
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation", "situation_and_edge", "no_op"],
        "op_family": "claim_insert",
        "proposed_text": "Harbor renewal may have a coherent cross-time risk pattern.",
        "semantic_scope": ["Harbor renewal"],
        "member_observation_ids": [str(value) for value in observation_ids],
        "evidence_model_ids": [str(value) for value in prior_models],
        "suggested_edge_kinds": ["weakens"],
        "confidence": 0.7,
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system", user="user", candidates=(candidate,),
    )
    decisions = BatchMemoryDecisionSet(decisions=[
        BatchMemoryCandidateDecision(
            candidate_id="MDC_SYNTH_scope", decision="accept",
            operation="edge", confidence=0.72,
            claim_text=(
                "Harbor renewal shows a persistent ownership gap whose status "
                "signals have repeatedly overstated readiness."
            ),
            edge_kind="weakens", source_model_id=prior_models[0],
            target_model_id=prior_models[1],
            reason="The scope warrants both a thesis and a weakens relation.",
        )
    ])

    diff = request.to_raw_diff(
        decisions, trigger=trigger, trigger_ref=trigger_id,
    )

    assert len(diff.claim_ops) == 1
    proposition = diff.claim_ops[0].entry["proposition"]
    assert proposition["claim_role"] == "hypothesis"
    assert proposition["synthesis_contract"] is True
    assert set(proposition["member_model_ids"]) == set(map(str, prior_models))
    assert set(diff.claim_ops[0].entry["supporting_event_ids"]) == set(
        map(str, observation_ids)
    )
    assert len(diff.relation_claim_ops) == 1


def test_rejected_synthesis_emits_neither_model_nor_relation() -> None:
    tenant_id, trigger_id = uuid4(), uuid4()
    request = CompiledBatchMemoryDecisionRequest(
        system="system", user="user", candidates=({
            "candidate_id": "MDC_SYNTH_scope",
            "candidate_kind": "synthesis",
            "proposed_text": "Harbor renewal may have a coherent state.",
        },),
    )
    decisions = BatchMemoryDecisionSet(decisions=[
        BatchMemoryCandidateDecision(
            candidate_id="MDC_SYNTH_scope", decision="reject",
            operation="no_op", confidence=0.6,
            reason="Evidence is not yet coherent enough.",
        )
    ])

    diff = request.to_raw_diff(
        decisions,
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        trigger_ref=trigger_id,
    )

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
