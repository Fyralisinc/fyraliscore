from __future__ import annotations

from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    RelationObligation,
    _bind_synthesis_endpoint_versions,
)


def test_scoped_hydration_receipt_binds_exact_synthesis_head_versions() -> None:
    models = [uuid4(), uuid4()]
    versions = [uuid4(), uuid4()]
    candidates = [{
        "candidate_id": "MDC_SYNTH_scope",
        "candidate_kind": "synthesis",
        "evidence_model_ids": [str(value) for value in models],
    }]

    bound = _bind_synthesis_endpoint_versions(candidates, packet={
        "synthesis_scope_hydration": {
            "endpoint_model_versions": {
                str(model_id): str(version_id)
                for model_id, version_id in zip(models, versions, strict=True)
            },
            "endpoint_model_cards": {
                str(model_id): {
                    "id": str(model_id), "version_id": str(version_id),
                    "natural": f"Scoped model {index}",
                    "proposition": {"kind": "belief"},
                    "canonical_scope": {"label": "Scope", "ref": "scope:one"},
                }
                for index, (model_id, version_id) in enumerate(
                    zip(models, versions, strict=True), start=1,
                )
            },
        },
    })

    assert bound[0]["endpoint_model_versions"] == {
        str(model_id): str(version_id)
        for model_id, version_id in zip(models, versions, strict=True)
    }


def test_accepted_synthesis_edge_also_materializes_composite_situation() -> None:
    tenant_id, trigger_id = uuid4(), uuid4()
    observation_ids = [uuid4()]
    relation_observation_ids = [uuid4(), uuid4()]
    prior_models = [uuid4(), uuid4()]
    prior_versions = [uuid4(), uuid4()]
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
        "relation_evidence_observation_ids": [
            str(value) for value in relation_observation_ids
        ],
        "evidence_model_ids": [str(value) for value in prior_models],
        "suggested_edge_kinds": ["blocks"],
        "endpoint_model_versions": {
            str(model_id): str(version_id)
            for model_id, version_id in zip(prior_models, prior_versions, strict=True)
        },
        "confidence": 0.7,
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system", user="user", candidates=(candidate,),
        relation_obligations=(RelationObligation(
            candidate_id="MDC_SYNTH_scope", edge_kind="blocks", confidence=0.72,
            source_model_id=prior_models[0], target_model_id=prior_models[1],
            evidence_event_ids=tuple(relation_observation_ids),
            evidence_model_ids=tuple(prior_models),
            evidence_text="Ownership gaps block Harbor renewal completion.",
            explanation="Exact conclusion opener names the blocking mechanism.",
            matched_markers=("block",),
        ),),
    )
    decisions = BatchMemoryDecisionSet(decisions=[
        BatchMemoryCandidateDecision(
            candidate_id="MDC_SYNTH_scope", decision="accept",
            operation="situation_and_edge", confidence=0.72,
            source_model_id=prior_models[0], target_model_id=prior_models[1],
            situation_member_model_ids=prior_models,
            claim_text=(
                "Harbor renewal shows a persistent ownership gap whose status "
                "signals have repeatedly overstated readiness."
            ),
            reason="The scope warrants both a thesis and a weakens relation.",
        )
    ])

    diff = request.to_raw_diff(
        decisions, trigger=trigger, trigger_ref=trigger_id,
    )

    assert len(diff.claim_ops) == 1
    proposition = diff.claim_ops[0].entry["proposition"]
    assert proposition["claim_role"] == "situation"
    assert proposition["abstraction_level"] == "composite"
    assert proposition["synthesis_contract"] is True
    assert set(proposition["member_model_ids"]) == set(map(str, prior_models))
    assert set(diff.claim_ops[0].entry["supporting_event_ids"]) == set(
        map(str, observation_ids)
    )
    assert len(diff.relation_claim_ops) == 1
    assert diff.relation_claim_ops, diff.model_dump()
    relation = diff.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert relation.predicate == "dependency_constraint"
    assert relation.write_policy == "accepted_edge"
    assert relation.status == "accepted"
    assert relation.source_model_version_id == prior_versions[0]
    assert relation.target_model_version_id == prior_versions[1]
    assert relation.semantic_scope == ["Harbor renewal"]
    assert set(relation.evidence_event_ids) == set(relation_observation_ids)
    assert set(relation.evidence_model_ids) == set(prior_models)
    supported = proposition["supported_relation"]
    assert supported["kind"] == "dependency_constraint"
    assert supported["source_model_id"] == str(relation.source_model_id)
    assert supported["target_model_id"] == str(relation.target_model_id)
    assert supported["source_model_version_id"] == str(prior_versions[0])
    assert supported["target_model_version_id"] == str(prior_versions[1])


def test_ambiguous_synthesis_stays_pretruth_without_exact_endpoint_versions() -> None:
    tenant_id, trigger_id = uuid4(), uuid4()
    observations = [uuid4()]
    models = [uuid4(), uuid4()]
    candidate = {
        "candidate_id": "MDC_SYNTH_candidate",
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation", "situation_and_edge", "no_op"],
        "op_family": "claim_insert",
        "proposed_text": "A possible dependency needs exact endpoint validation.",
        "semantic_scope": ["Beacon migration"],
        "member_observation_ids": [str(value) for value in observations],
        "evidence_model_ids": [str(value) for value in models],
        "suggested_edge_kinds": ["blocks"],
        "confidence": 0.7,
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system", user="user", candidates=(candidate,),
    )
    decisions = BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
        candidate_id="MDC_SYNTH_candidate", decision="accept", operation="edge",
        confidence=0.82, edge_kind="blocks", source_model_id=models[0],
        target_model_id=models[1],
        claim_text="Beacon completion may depend on access review.",
        reason="Dependency is plausible but versions are absent.",
    )])

    diff = request.to_raw_diff(
        decisions,
        trigger=TriggerContext(
            kind="T1", tenant_id=tenant_id, observation_ids=observations,
            seed_signature={"trigger_id": str(trigger_id)},
        ),
        trigger_ref=trigger_id,
    )

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert "missing explicit relation obligation" in diff.reasoning_trace


def test_synthesis_endpoint_outside_closed_members_emits_zero_writes() -> None:
    tenant_id, trigger_id = uuid4(), uuid4()
    opener, relation_event = uuid4(), uuid4()
    members, versions = [uuid4(), uuid4()], [uuid4(), uuid4()]
    outsider = uuid4()
    candidate = {
        "candidate_id": "MDC_SYNTH_closed", "candidate_kind": "synthesis",
        "allowed_operations": ["situation_and_edge", "no_op"],
        "op_family": "claim_insert", "proposed_text": "Atlas release is blocked.",
        "semantic_scope": ["Atlas release"],
        "member_observation_ids": [str(opener)],
        "relation_evidence_observation_ids": [str(relation_event)],
        "evidence_model_ids": [str(value) for value in members],
        "endpoint_model_versions": dict(zip(
            map(str, members), map(str, versions), strict=True,
        )),
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system", user="user", candidates=(candidate,),
        relation_obligations=(RelationObligation(
            candidate_id="MDC_SYNTH_closed", edge_kind="blocks", confidence=0.8,
            source_model_id=None, target_model_id=None,
            evidence_event_ids=(relation_event,), evidence_model_ids=tuple(members),
            evidence_text="The open certificate blocks the rollout.",
            explanation="Explicit dependency", matched_markers=("blocks",),
        ),),
    )
    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
            candidate_id="MDC_SYNTH_closed", decision="accept",
            operation="situation_and_edge", confidence=0.8,
            source_model_id=members[0], target_model_id=outsider,
            situation_member_model_ids=[members[0], outsider],
            claim_text="Atlas release is blocked by certificate ownership.",
            reason="Bind the explicit dependency.",
        )]),
        trigger=TriggerContext(
            kind="T1", tenant_id=tenant_id, observation_ids=[opener, relation_event],
        ),
        trigger_ref=trigger_id,
    )
    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert "invalid or stale closed-set relation binding" in diff.reasoning_trace


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
