"""Cost-plan §1.2 — lean strict-schema prompt + enforces_output_schema().

The lean variant drops ONLY the JSON-shape skeleton that a server-enforced
strict schema already constrains, and only on providers that enforce the
schema (DeepSeek strict) with the flag on. Codex/OpenAI/Anthropic keep the
full prose because they send the schema as a non-binding hint.
"""
from __future__ import annotations

from lib.llm.provider import (
    CodexProvider,
    DeepSeekProvider,
    LLMConfig,
)
from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import RawDiff
from services.reasoning.think.prompt import build_prompt


_SKELETON_MARKER = '"ontology_gap_ops": [],'


def _cfg(provider: str, model: str) -> LLMConfig:
    return LLMConfig(provider=provider, api_key="x", model=model)


def _t1() -> TriggerContext:
    return TriggerContext(
        kind="T1", subkind="event_arrival", tenant_id=uuid7(),
        observation_id=uuid7(),
    )


def test_enforces_output_schema_deepseek_strict_true():
    p = DeepSeekProvider(_cfg("deepseek", "deepseek-chat"))
    assert p.enforces_output_schema(RawDiff) is True


def test_enforces_output_schema_deepseek_reasoner_false():
    # Reasoner-class models route through JSON mode, not strict tool-calling.
    p = DeepSeekProvider(_cfg("deepseek", "deepseek-reasoner"))
    assert p.enforces_output_schema(RawDiff) is False


def test_enforces_output_schema_codex_false():
    # Codex sends the schema as a non-binding hint — prose must stay.
    p = CodexProvider(_cfg("codex", "gpt-5.3-codex"))
    assert p.enforces_output_schema(RawDiff) is False


def test_lean_drops_skeleton_when_enforced_and_flag_on(monkeypatch):
    monkeypatch.setenv("THINK_STRICT_LEAN_PROMPT", "1")
    full = build_prompt(_t1(), ContextBundle(), lean_output_contract=False)
    lean = build_prompt(_t1(), ContextBundle(), lean_output_contract=True)
    assert _SKELETON_MARKER in full.system
    assert _SKELETON_MARKER not in lean.system
    # The pointer replaces it; semantic field names remain referenced.
    assert "the strict tool schema enforces the exact top-level shape" in lean.system
    # Lean is strictly shorter (we only ever remove).
    assert len(lean.system) < len(full.system)


def test_lean_noop_when_flag_off(monkeypatch):
    monkeypatch.delenv("THINK_STRICT_LEAN_PROMPT", raising=False)
    # Even with lean_output_contract=True, flag-off keeps full prose.
    p = build_prompt(_t1(), ContextBundle(), lean_output_contract=True)
    assert _SKELETON_MARKER in p.system


def test_lean_noop_when_provider_does_not_enforce(monkeypatch):
    monkeypatch.setenv("THINK_STRICT_LEAN_PROMPT", "1")
    # lean_output_contract=False (hint-only provider) keeps full prose.
    p = build_prompt(_t1(), ContextBundle(), lean_output_contract=False)
    assert _SKELETON_MARKER in p.system


def test_lean_keeps_edge_kind_vocabulary(monkeypatch):
    # Critical: the 16-kind edge vocabulary is regex-only in the strict schema,
    # so it must survive the lean transform.
    monkeypatch.setenv("THINK_STRICT_LEAN_PROMPT", "1")
    lean = build_prompt(_t1(), ContextBundle(), lean_output_contract=True)
    assert "superseded_by" in lean.system
    assert "contributes_to_resolution" in lean.system


def test_prompt_requests_specific_top_level_semantic_terms() -> None:
    full = build_prompt(_t1(), ContextBundle(), lean_output_contract=False)
    compact = build_prompt(_t1(), ContextBundle(), claims_only=True)

    for system in (full.system, compact.system):
        assert '"semantic_terms": ["<specific lexical phrase>", ...]' in system
        assert "top-level" in system
        assert "exact `domain_tags`" in system
        assert "UUIDs" in system
        assert "scope" in system
    assert "specific belief phrases" in compact.system
