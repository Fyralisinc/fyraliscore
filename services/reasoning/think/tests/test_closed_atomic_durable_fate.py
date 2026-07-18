from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.applier import _lifecycle_confidence
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    RelationObligation,
    _bind_exact_closed_atomic_targets,
    _candidate_scope_coordinate,
    relation_obligations_from_packet,
)


def _trigger(tenant_id, observation_id) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id],
        seed_signature={"event_batch": True},
        seed_natural_text="Atlas release is waiting for approval.",
    )


def _candidate(observation_id, *, source_ids=None) -> dict:
    return {
        "candidate_id": "MDC_ATOM_GENERIC",
        "op_family": "claim_insert",
        "proposed_text": "Atlas release is waiting for approval.",
        "entailed_claim_text": "Atlas release is waiting for approval.",
        "source_observation_ids": source_ids or [str(observation_id)],
        "member_observation_ids": [str(observation_id)],
        "semantic_scope": ["Atlas release"],
        "confidence": 0.61,
    }


def _request(candidate: dict) -> CompiledBatchMemoryDecisionRequest:
    return CompiledBatchMemoryDecisionRequest(
        system="system",
        user="user",
        candidates=(candidate,),
    )


def test_candidate_scope_coordinate_prefers_well_formed_canonical_scope_ref() -> None:
    detection_id = uuid7()
    candidate = _candidate(uuid7())
    candidate["canonical_scope_ref"] = f"mention:{detection_id}"

    assert _candidate_scope_coordinate(candidate) == (
        "mention", f"mention:{detection_id}",
    )


def test_candidate_scope_coordinate_rejects_malformed_ref_before_label_fallback() -> None:
    candidate = _candidate(uuid7())
    candidate["canonical_scope_ref"] = "mention:bad scope"

    assert _candidate_scope_coordinate(candidate) == (
        "workstream", "workstream:atlas-release",
    )


def test_closed_atomic_without_bound_target_is_deterministic_insert() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()

    diff = _request(_candidate(observation_id)).to_raw_diff(
        BatchMemoryDecisionSet(decisions=[]),
        trigger=_trigger(tenant_id, observation_id),
        trigger_ref=uuid7(),
    )

    assert len(diff.claim_ops) == 1
    assert diff.claim_ops[0].op == "insert"
    assert diff.claim_ops[0].entry["supporting_event_ids"] == [str(observation_id)]
    assert diff.memory_lifecycle_ops == []


def test_mention_scoped_atomic_preserves_nonidentity_authority_contract() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    detection_id = uuid7()
    candidate = _candidate(observation_id)
    candidate["canonical_scope_ref"] = f"mention:{detection_id}"

    diff = _request(candidate).to_raw_diff(
        BatchMemoryDecisionSet(decisions=[]),
        trigger=_trigger(tenant_id, observation_id),
        trigger_ref=uuid7(),
    )

    entry = diff.claim_ops[0].entry
    assert entry["scope_entities"] == [
        {"type": "mention", "id": f"mention:{detection_id}"}
    ]
    assert entry["proposition"]["mention_scope_contract"] == {
        "version": "v1",
        "detection_ref": f"mention:{detection_id}",
        "canonical_identity_authority": False,
        "cross_observation_grouping_authority": False,
    }


def test_llm_noop_cannot_suppress_closed_atomic_durable_fate() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    decision = BatchMemoryCandidateDecision(
        candidate_id="MDC_ATOM_GENERIC",
        decision="reject",
        operation="no_op",
        confidence=0.9,
        reason="Looks duplicative.",
    )

    diff = _request(_candidate(observation_id)).to_raw_diff(
        BatchMemoryDecisionSet(decisions=[decision]),
        trigger=_trigger(tenant_id, observation_id),
        trigger_ref=uuid7(),
    )

    assert len(diff.claim_ops) == 1
    assert "deterministic atomic insert" in (diff.reasoning_trace or "")
    assert "rejected - Looks duplicative" not in (diff.reasoning_trace or "")


def test_exact_same_scope_binding_compiles_confirm_instead_of_insert() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    model_id = uuid7()
    candidate = _candidate(observation_id)
    model = SimpleNamespace(
        id=model_id,
        tenant_id=tenant_id,
        status="active",
        abstraction_level="atomic",
        natural="  Atlas release is waiting for approval. ",
        scope_entities=[{"display_label": "Atlas release"}],
        proposition={"subject": "Atlas release"},
    )
    [bound] = _bind_exact_closed_atomic_targets(
        [candidate], models=[model], tenant_id=tenant_id,
    )

    diff = _request(bound).to_raw_diff(
        BatchMemoryDecisionSet(decisions=[]),
        trigger=_trigger(tenant_id, observation_id),
        trigger_ref=uuid7(),
    )

    assert diff.claim_ops == []
    assert len(diff.memory_lifecycle_ops) == 1
    op = diff.memory_lifecycle_ops[0]
    assert op.model_id == model_id
    assert op.action == "confirm"
    assert op.claim_local_evidence_event_ids == [observation_id]
    assert op.confidence is None
    assert _lifecycle_confidence(op, current_confidence=0.90) == 0.95


def test_same_surface_different_mention_scope_never_confirms_existing_model() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    first_detection, second_detection = uuid7(), uuid7()
    candidate = _candidate(observation_id)
    candidate["canonical_scope_ref"] = f"mention:{second_detection}"
    model = SimpleNamespace(
        id=uuid7(),
        tenant_id=tenant_id,
        status="active",
        abstraction_level="atomic",
        natural="Atlas release is waiting for approval.",
        scope_entities=[{
            "type": "mention",
            "id": f"mention:{first_detection}",
            "display_label": "Atlas release",
        }],
        proposition={"scope_label": "Atlas release"},
    )

    [bound] = _bind_exact_closed_atomic_targets(
        [candidate], models=[model], tenant_id=tenant_id,
    )

    assert "target_model_ids" not in bound
    assert "allowed_operations" not in bound


def test_mention_scope_cannot_open_relation_obligation() -> None:
    observation_id = uuid7()
    candidate = _candidate(observation_id)
    candidate.update({
        "canonical_scope_ref": f"mention:{uuid7()}",
        "evidence_model_ids": [str(uuid7()), str(uuid7())],
        "suggested_edge_kinds": ["blocks"],
    })

    assert relation_obligations_from_packet({}, [candidate]) == ()


def test_exact_bound_confirm_excludes_transport_sibling() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    sibling_id = uuid7()
    model_id = uuid7()
    candidate = _candidate(
        observation_id,
        source_ids=[str(observation_id), str(sibling_id)],
    )
    candidate["target_model_ids"] = [str(model_id)]
    candidate["allowed_operations"] = ["memory_lifecycle"]

    diff = _request(candidate).to_raw_diff(
        BatchMemoryDecisionSet(decisions=[]),
        trigger=_trigger(tenant_id, observation_id),
        trigger_ref=uuid7(),
    )

    op = diff.memory_lifecycle_ops[0]
    assert op.evidence_event_ids == [observation_id]
    assert op.claim_local_evidence_event_ids == [observation_id]
    assert sibling_id not in op.evidence_event_ids


def test_exact_confirm_cannot_stale_new_synthesis_member_in_same_diff() -> None:
    tenant_id = uuid7()
    confirmation_id, synthesis_id, relation_id = uuid7(), uuid7(), uuid7()
    member_ids, version_ids = [uuid7(), uuid7()], [uuid7(), uuid7()]
    closed = _candidate(confirmation_id)
    closed.update({
        "candidate_id": "MDC_ATOM_ATLAS_CONFIRM",
        "target_model_ids": [str(member_ids[0])],
        "allowed_operations": ["memory_lifecycle"],
        "canonical_scope_ref": "workstream:atlas-release",
    })
    synthesis = {
        "candidate_id": "MDC_SYNTH_ATLAS",
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation_and_edge", "no_op"],
        "op_family": "claim_insert",
        "proposed_text": "Missing approval ownership blocks Atlas release.",
        "semantic_scope": ["Atlas release"],
        "canonical_scope_ref": "workstream:atlas-release",
        "member_observation_ids": [str(synthesis_id)],
        "relation_evidence_observation_ids": [str(relation_id)],
        "evidence_model_ids": [str(value) for value in member_ids],
        "endpoint_model_versions": {
            str(model_id): str(version_id)
            for model_id, version_id in zip(member_ids, version_ids, strict=True)
        },
        "confidence": 0.8,
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system",
        user="user",
        candidates=(closed, synthesis),
        relation_obligations=(RelationObligation(
            candidate_id="MDC_SYNTH_ATLAS",
            edge_kind="blocks",
            confidence=0.8,
            source_model_id=member_ids[0],
            target_model_id=member_ids[1],
            evidence_event_ids=(relation_id,),
            evidence_model_ids=tuple(member_ids),
            evidence_text="Missing approval ownership blocks Atlas release.",
            explanation="The approval dependency blocks completion.",
            matched_markers=("blocks",),
        ),),
    )
    decisions = BatchMemoryDecisionSet(decisions=[
        BatchMemoryCandidateDecision(
            candidate_id="MDC_SYNTH_ATLAS",
            decision="accept",
            operation="situation_and_edge",
            confidence=0.8,
            claim_role="situation",
            claim_text="Missing approval ownership blocks Atlas release.",
            situation_member_model_ids=member_ids,
            source_model_id=member_ids[0],
            target_model_id=member_ids[1],
            reason="The exact accepted heads support the dependency.",
        )
    ])

    diff = request.to_raw_diff(
        decisions,
        trigger=TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            observation_ids=[confirmation_id, synthesis_id, relation_id],
        ),
        trigger_ref=uuid7(),
    )

    assert diff.memory_lifecycle_ops == []
    assert [
        op.entry["proposition"]["claim_role"] for op in diff.claim_ops
    ].count("fact") == 1
    assert [
        op.entry["proposition"]["claim_role"] for op in diff.claim_ops
    ].count("situation") == 1
    assert len(diff.relation_claim_ops) == 1
    assert "preserve a new synthesis member head" in diff.reasoning_trace
