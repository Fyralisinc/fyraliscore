from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    _bind_exact_closed_atomic_targets,
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
