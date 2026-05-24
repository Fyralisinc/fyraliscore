from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import RetrievalResult, TriggerContext
from services.think.prompt import build_prompt
from services.think.reasoning_frame import ReasoningFrame


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
    assert "frame_kind: anomaly_explanation" in user
    assert "composite_situations" in user
    assert "situation_requires_multiple_existing_models" in user
    assert user.index("<reasoning_frame>") < user.index("<retrieved_context>")
