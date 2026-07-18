from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryDecisionSet,
    PriorMemoryEffectDecision,
    build_compiled_batch_memory_decision_request,
)


def _model(
    *,
    tenant_id: UUID,
    scope_ref: str = "workstream:atlas-release",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        tenant_id=tenant_id,
        status="active",
        natural="Atlas release has no recorded certificate owner.",
        proposition={
            "kind": "belief",
            "assertion": "Atlas release has no recorded certificate owner.",
        },
        confidence=0.68,
        scope_entities=[{"type": "workstream", "id": scope_ref}],
        abstraction_level="atomic",
    )


def _request(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    models: list[SimpleNamespace],
):
    claim = "Atlas release now has a recorded certificate owner."
    candidate = {
        "candidate_id": "MDC_ATOM_atlas_owner",
        "candidate_kind": "atomic",
        "allowed_operations": ["claim", "no_op"],
        "entailed_claim_text": claim,
        "proposed_text": claim,
        "canonical_scope_ref": "workstream:atlas-release",
        "semantic_scope": ["Atlas release"],
        "source_observation_ids": [str(observation_id)],
        "member_observation_ids": [str(observation_id)],
        "observation_evidence": [
            {"observation_id": str(observation_id), "body": claim}
        ],
    }
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id],
        seed_natural_text=claim,
    )
    request = build_compiled_batch_memory_decision_request(
        trigger,
        ContextBundle(
            models=models,
            notes={
                "inquiry_context_packet": {
                    "signal_summary": claim,
                    "memory_decision_candidates": [candidate],
                }
            },
        ),
    )
    assert request is not None
    return trigger, request


def test_memory_present_exposes_exact_scope_prior_and_compiles_effect() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    prior = _model(tenant_id=tenant_id)
    trigger, request = _request(
        tenant_id=tenant_id,
        observation_id=observation_id,
        models=[prior],
    )

    candidate = request.candidates[0]
    assert candidate["prior_same_scope_model_ids"] == [str(prior.id)]
    assert candidate["prior_same_scope_model_cards"][0]["canonical_scope"] == {
        "type": "workstream",
        "ref": "workstream:atlas-release",
    }
    assert str(prior.id) in request.user
    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(
            prior_memory_effects=[
                PriorMemoryEffectDecision(
                    candidate_id=candidate["candidate_id"],
                    prior_model_id=prior.id,
                    relation="contradicts",
                    claim_local_evidence_event_ids=[observation_id],
                    reason="The new exact assertion directly reverses the prior owner state.",
                )
            ]
        ),
        trigger=trigger,
        trigger_ref=uuid4(),
    )

    assert len(diff.claim_ops) == 1
    assert len(diff.memory_lifecycle_ops) == 1
    effect = diff.memory_lifecycle_ops[0]
    assert effect.model_id == prior.id
    assert effect.action == "falsify"
    assert effect.claim_local_evidence_event_ids == [observation_id]
    assert effect.metadata == {
        "source": "prior_memory_effect",
        "prior_model_id": str(prior.id),
        "effect_scope": "candidate",
        "candidate_id": candidate["candidate_id"],
        "relation": "contradicts",
    }


def test_memory_ablated_keeps_singleton_insert_without_prior_effect_surface() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    trigger, request = _request(
        tenant_id=tenant_id,
        observation_id=observation_id,
        models=[],
    )

    candidate = request.candidates[0]
    assert "prior_same_scope_model_ids" not in candidate
    assert "prior_same_scope_model_cards" not in candidate
    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(),
        trigger=trigger,
        trigger_ref=uuid4(),
    )
    assert len(diff.claim_ops) == 1
    assert diff.memory_lifecycle_ops == []


def test_prior_effect_rejects_cross_scope_prior_and_foreign_evidence() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    exact_prior = _model(tenant_id=tenant_id)
    cross_scope = _model(
        tenant_id=tenant_id,
        scope_ref="workstream:beacon-migration",
    )
    trigger, request = _request(
        tenant_id=tenant_id,
        observation_id=observation_id,
        models=[exact_prior, cross_scope],
    )
    candidate = request.candidates[0]
    assert candidate["prior_same_scope_model_ids"] == [str(exact_prior.id)]

    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(
            prior_memory_effects=[
                PriorMemoryEffectDecision(
                    candidate_id=candidate["candidate_id"],
                    prior_model_id=cross_scope.id,
                    relation="supports",
                    claim_local_evidence_event_ids=[observation_id],
                    reason="Attempted cross-scope mutation.",
                ),
                PriorMemoryEffectDecision(
                    candidate_id=candidate["candidate_id"],
                    prior_model_id=exact_prior.id,
                    relation="weakens",
                    claim_local_evidence_event_ids=[uuid4()],
                    reason="Attempted foreign-evidence mutation.",
                ),
            ]
        ),
        trigger=trigger,
        trigger_ref=uuid4(),
    )

    assert len(diff.claim_ops) == 1
    assert diff.memory_lifecycle_ops == []
    assert "prior Model is not candidate-authorized" in diff.reasoning_trace
    assert "evidence is not candidate-authorized" in diff.reasoning_trace


def test_none_effect_is_explicit_trace_without_mutation() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    prior = _model(tenant_id=tenant_id)
    trigger, request = _request(
        tenant_id=tenant_id,
        observation_id=observation_id,
        models=[prior],
    )
    candidate = request.candidates[0]
    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(
            prior_memory_effects=[
                PriorMemoryEffectDecision(
                    candidate_id=candidate["candidate_id"],
                    prior_model_id=prior.id,
                    relation="none",
                    reason="The prior concerns ownership while this claim is only timing.",
                )
            ]
        ),
        trigger=trigger,
        trigger_ref=uuid4(),
    )

    assert diff.memory_lifecycle_ops == []
    assert "explicit no mutation" in diff.reasoning_trace


def test_supersession_waits_for_an_accepted_successor_model() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    prior = _model(tenant_id=tenant_id)
    trigger, request = _request(
        tenant_id=tenant_id,
        observation_id=observation_id,
        models=[prior],
    )
    candidate = request.candidates[0]

    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(
            prior_memory_effects=[
                PriorMemoryEffectDecision(
                    candidate_id=candidate["candidate_id"],
                    prior_model_id=prior.id,
                    relation="supersedes",
                    claim_local_evidence_event_ids=[observation_id],
                    reason="The new state would replace the prior if admitted.",
                )
            ]
        ),
        trigger=trigger,
        trigger_ref=uuid4(),
    )

    assert diff.memory_lifecycle_ops == []
    assert "supersession requires an accepted successor Model" in diff.reasoning_trace
