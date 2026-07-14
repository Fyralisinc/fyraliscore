"""Cost-plan §1.1 — cache-friendly prompt layout.

Think now always keeps a stable system prefix (static base +
per-trigger-kind operating instructions) and moves the dynamic reasoning
profile to the top of the user message.
"""
from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.prompt import build_prompt


_PROFILE_MARKER = "Reasoning profile for this call:"
_INSTRUCTIONS_MARKER = "<operating_instructions>"


def _t1(source: str) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        subkind="event_arrival",
        tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_signature={"source_channel": source, "trust_tier": "verified"},
    )


def test_prompt_layout_stabilizes_system_prefix():
    a = build_prompt(_t1("slack"), ContextBundle(), triggering_content="alpha")
    b = build_prompt(_t1("email"), ContextBundle(), triggering_content="beta")

    # Static base + per-kind instructions form the system prefix; the dynamic
    # profile is no longer in system.
    assert _INSTRUCTIONS_MARKER in a.system
    assert _PROFILE_MARKER not in a.system
    # Profile leads the user message instead.
    assert a.user.startswith(_PROFILE_MARKER)
    # The cache property: two different T1 triggers share a byte-identical
    # system prefix (same kind × schema-variant bucket).
    assert a.system == b.system


def test_candidates_char_budget_caps_tail(monkeypatch):
    # Cost-plan §1.3: the relationship-candidates section drops its tail into
    # the omitted-count marker once the char budget is exceeded, but always
    # emits at least one.
    import services.reasoning.think.prompt as prompt_mod
    monkeypatch.setattr(prompt_mod, "_CANDIDATES_CHAR_BUDGET", 50)
    cands = [{"id": str(uuid7()), "explanation": "x" * 200} for _ in range(5)]
    trigger = TriggerContext(
        kind="T1", subkind="event_arrival", tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_signature={"relationship_candidates": cands},
    )
    user = build_prompt(trigger, ContextBundle()).user
    assert "relationship_candidate_omitted_count:" in user
    assert 1 <= user.count("<relationship_candidate>") < 5


def test_pattern_review_prompt_renders_candidate_and_rubric():
    candidate_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="pattern_review",
        tenant_id=uuid7(),
        seed_signature={
            "pattern_candidate_id": str(candidate_id),
            "source": "precipitation_cluster",
            "review_mode": "semantic_required",
            "cluster_size": 3,
            "density": 0.74,
            "constituent_model_ids": [str(uuid7()), str(uuid7()), str(uuid7())],
            "proposed_signature": {"kind": "cluster_signature"},
            "observed_tendency": {
                "exemplars": ["approval review blocked"],
                "review_features": {
                    "feature_axes": ["lexical_recurrence", "shared_actors"],
                    "evidence_axis_count": 2,
                },
            },
        },
    )

    prompt = build_prompt(trigger, ContextBundle())

    assert "<pattern_review_candidate>" in prompt.user
    assert f"id: {candidate_id}" in prompt.user
    assert "weak_evidence_requires_semantic_review" in prompt.user
    assert "review_features" in prompt.user
    assert "lexical_recurrence" in prompt.user
    assert "This is a T4 pattern_review trigger" in prompt.system
    assert "stable: repeated behavior" in prompt.system
    assert "Do not promote solely from cluster_size, density, or candidate_id" in (
        prompt.system
    )
