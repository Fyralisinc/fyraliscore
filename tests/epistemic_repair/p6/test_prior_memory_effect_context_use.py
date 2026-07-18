from __future__ import annotations

from uuid import uuid4

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import MemoryLifecycleOp, RawDiff


def _bundle(prior_model):
    return ContextBundle(
        notes={
            "model_selection": {
                "selected_model_ids": [str(prior_model)],
                "pathway_survival": {"G": {"selected_model_ids": []}},
            }
        }
    )


def _effect(prior_model, *, action="confirm", source="prior_memory_effect"):
    return MemoryLifecycleOp(
        model_id=prior_model,
        action=action,
        rationale="Candidate c-1 was compared with selected prior memory.",
        metadata={
            "source": source,
            "effect_scope": "candidate",
            "prior_model_id": str(prior_model),
            "candidate_id": "c-1",
            "relation": "supports" if action == "confirm" else action,
        },
    )


def test_candidate_effect_requires_explicit_decision_reasoning() -> None:
    prior_model = uuid4()
    diff = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=uuid4(),
        reasoning_trace=(
            f"Candidate c-1 supports {prior_model}; prior memory already captures "
            "the same company state."
        ),
        memory_lifecycle_ops=[_effect(prior_model)],
    )

    report = summarize_context_use(_bundle(prior_model), diff)

    assert report["authorized_prior_memory_effect_count"] == 1
    assert report["material_prior_memory_effect_count"] == 1
    assert report["reasoning_accounted_prior_memory_effect_count"] == 1
    assert report["material_prior_model_ids"] == [str(prior_model)]


def test_unchanged_and_generic_lifecycle_ops_never_receive_material_credit() -> None:
    prior_model = uuid4()
    diff = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=uuid4(),
        reasoning_trace=f"Model {prior_model} is not materially changed.",
        memory_lifecycle_ops=[
            _effect(prior_model, action="unchanged"),
            _effect(prior_model, source="representation_contract"),
        ],
    )

    report = summarize_context_use(_bundle(prior_model), diff)

    assert report["authorized_prior_memory_effect_count"] == 1
    assert report["material_prior_memory_effect_count"] == 0
    assert report["reasoning_accounted_prior_memory_effect_count"] == 0
    assert report["material_prior_model_ids"] == []


def test_malformed_candidate_envelope_is_not_authorized() -> None:
    prior_model = uuid4()
    other_model = uuid4()
    op = _effect(prior_model)
    op.metadata["prior_model_id"] = str(other_model)
    diff = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=uuid4(),
        reasoning_trace=f"Candidate c-1 confirms {prior_model}; it already captures it.",
        memory_lifecycle_ops=[op],
    )

    report = summarize_context_use(_bundle(prior_model), diff)

    assert report["prior_memory_effects"] == []
    assert report["authorized_prior_memory_effect_count"] == 0
