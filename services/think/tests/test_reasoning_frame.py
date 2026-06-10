from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import RetrievalResult, TriggerContext
from services.think.prompt import build_prompt
from services.think.reasoning_frame import (
    ReasoningFrame,
    reasoning_job_from_trigger,
)


def test_reasoning_frame_normalizes_topology_trigger() -> None:
    tenant_id = uuid7()
    member_a = uuid7()
    member_b = uuid7()
    retrieved_id = uuid7()
    trigger = TriggerContext(
        kind="T6",
        tenant_id=tenant_id,
        topology_event_kind="emergence",
        neighborhood_id=uuid7(),
        member_model_ids=[member_a, member_b],
    )
    retrieval = RetrievalResult(
        trigger=trigger,
        models=[SimpleNamespace(id=retrieved_id)],
    )

    frame = ReasoningFrame.from_trigger(trigger, retrieval_result=retrieval)

    assert frame.frame_kind == "topology_shift"
    assert frame.stimulus_kind == "T6:emergence"
    assert frame.job_source == "topology"
    assert frame.job_intent == "integrate_topology_shift"
    assert str(member_a) in frame.seed_model_ids
    assert str(member_b) in frame.seed_model_ids
    assert str(retrieved_id) in frame.candidate_model_ids
    assert "act_ops" not in frame.allowed_ops
    assert "structural_explanation" in frame.priority_dimensions
    assert frame.policy["emit_situation_for_composite_conditions"] is True


def test_reasoning_frame_renders_dynamic_signals() -> None:
    frame = ReasoningFrame.from_trigger(
        TriggerContext(kind="T4", tenant_id=uuid7())
    ).with_dynamic_signals([
        {
            "dynamic_kind": "oscillating",
            "summary": "Model has re-asserted a prior state.",
            "strength": 0.8,
            "confidence": 0.7,
        }
    ])

    section = frame.to_prompt_section()

    assert "dynamic_signals:" in section
    assert "oscillating" in section
    assert "re-asserted" in section


def test_reasoning_frame_normalizes_latent_topology_candidate() -> None:
    member_a = uuid7()
    member_b = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=uuid7(),
        member_model_ids=[member_a, member_b],
    )

    frame = ReasoningFrame.from_trigger(trigger)

    assert frame.frame_kind == "internal_reflection"
    assert frame.stimulus_kind == "T4:latent_relationship_candidate"
    assert frame.job_source == "topology"
    assert frame.job_intent == "adjudicate_candidate"
    assert str(member_a) in frame.seed_model_ids
    assert str(member_b) in frame.seed_model_ids
    assert "impact_signature_interaction" in frame.priority_dimensions
    assert frame.policy["treat_topology_as_evidence_not_truth"] is True
    assert frame.budget["act_ops"] == 0


def test_build_prompt_renders_reasoning_frame_section() -> None:
    tenant_id = uuid7()
    trigger = TriggerContext(
        kind="T3",
        tenant_id=tenant_id,
        seed_entity_ids=[{"type": "customer", "id": str(uuid7())}],
    )
    frame = ReasoningFrame.from_trigger(trigger)

    user = build_prompt(
        trigger,
        ContextBundle(),
        reasoning_frame=frame,
    ).user

    assert "<reasoning_frame>" in user
    assert "frame_kind: internal_reflection" in user
    assert "job_intent: explain_inconsistency" in user
    assert "composite_situations" in user
    assert "situation_requires_multiple_existing_models" in user
    assert user.index("<reasoning_frame>") < user.index("<retrieved_context>")


def test_build_prompt_renders_context_accountability_guidance() -> None:
    model_id = uuid7()
    trigger = TriggerContext(kind="T3", tenant_id=uuid7(), model_id=model_id)
    bundle = ContextBundle(
        notes={
            "model_selection": {
                "selected_model_ids": [str(model_id)],
                "pathway_survival": {
                    "G": {"selected_model_ids": [str(model_id)]}
                },
            }
        }
    )

    prompt = build_prompt(trigger, bundle)

    assert "Context accountability" in prompt.user
    assert "Never silently ignore selected context" in prompt.system
    assert "contributes_to_resolution" in prompt.user


def test_build_prompt_renders_relationship_candidate_section() -> None:
    candidate_id = uuid7()
    left = uuid7()
    right = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=uuid7(),
        member_model_ids=[left, right],
        seed_signature={
            "relationship_candidate_id": str(candidate_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge",
                "basis": "topology_suggested",
                "edge_kind": "blocks",
                "source_model_id": str(left),
                "target_model_id": str(right),
                "member_model_ids": [str(left), str(right)],
                "explanation": "Shared revenue and compliance pressure.",
                "metadata": {
                    "topology": {
                        "kind": "latent_relationship_field",
                        "score_components": {"total": 0.81},
                    }
                },
            },
        },
    )

    user = build_prompt(trigger, ContextBundle()).user

    assert "<relationship_candidate>" in user
    assert "edge_kind: blocks" in user
    assert "latent_relationship_field" in user


def test_build_prompt_renders_batched_relationship_candidates() -> None:
    left = uuid7()
    middle = uuid7()
    right = uuid7()
    first = uuid7()
    second = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=uuid7(),
        member_model_ids=[left, middle, right],
        seed_signature={
            "relationship_candidate_ids": [str(first), str(second)],
            "relationship_candidates": [
                {
                    "id": str(first),
                    "candidate_kind": "edge",
                    "edge_kind": "blocks",
                    "source_model_id": str(left),
                    "target_model_id": str(middle),
                    "member_model_ids": [str(left), str(middle)],
                    "explanation": "First candidate.",
                },
                {
                    "id": str(second),
                    "candidate_kind": "edge",
                    "edge_kind": "explains",
                    "source_model_id": str(middle),
                    "target_model_id": str(right),
                    "member_model_ids": [str(middle), str(right)],
                    "explanation": "Second candidate.",
                },
            ],
        },
    )

    user = build_prompt(trigger, ContextBundle()).user

    assert user.count("<relationship_candidate>") == 2
    assert "edge_kind: blocks" in user
    assert "edge_kind: explains" in user
    assert "First candidate." in user
    assert "Second candidate." in user


def test_t2_t3_t4_share_internal_reflection_family() -> None:
    cases = [
        (
            TriggerContext(kind="T2", tenant_id=uuid7()),
            "due_timer",
            "evaluate_existing_belief",
        ),
        (
            TriggerContext(kind="T3", tenant_id=uuid7()),
            "anomaly_detector",
            "explain_inconsistency",
        ),
        (
            TriggerContext(kind="T4", tenant_id=uuid7()),
            "maintenance",
            "reorganize_memory",
        ),
    ]

    for trigger, source, intent in cases:
        job = reasoning_job_from_trigger(trigger)
        frame = ReasoningFrame.from_trigger(trigger)

        assert job.family == "internal_reflection"
        assert frame.frame_kind == "internal_reflection"
        assert frame.job_source == source
        assert frame.job_intent == intent
