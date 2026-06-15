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
