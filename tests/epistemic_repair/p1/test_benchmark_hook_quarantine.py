"""P1 benchmark-blindness scanner for production Think reachability."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.context_planner import _augment_active_acts
from services.reasoning.think.diff_schema import RawDiff
from services.reasoning.think.llm_reason import llm_reason


ROOT = Path(__file__).resolve().parents[3]
THINK = ROOT / "services/reasoning/think"


def _text(name: str) -> str:
    return (THINK / name).read_text(encoding="utf-8")


def test_production_pipeline_has_no_benchmark_semantic_injectors() -> None:
    source = _text("run_pipeline.py")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    forbidden = {
        "maybe_inject_capability_probe_ops",
        "maybe_inject_latent_bridge",
    }
    assert forbidden.isdisjoint(imported)
    assert forbidden.isdisjoint(called)


def test_production_reasoning_has_no_fixture_phrase_short_circuit() -> None:
    source = "\n".join(
        _text(name)
        for name in (
            "llm_reason.py",
            "reason.py",
            "applier.py",
            "compiled_reasoning.py",
        )
    )
    forbidden = {
        "build_noise_only_raw_diff",
        "_trigger_is_noise_only",
        "_build_noise_noop_fast_path",
        "general operational chatter",
        "lunch logistics",
        "duplicated dashboard links",
        "non-actionable reminder",
        "should not dominate memory",
        "think.noise_noop_fast_path",
    }
    assert all(marker not in source for marker in forbidden)


def test_context_planner_does_not_discover_external_augmentors() -> None:
    source = _text("context_planner.py")
    tree = ast.parse(source)
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "services.reasoning.think.hooks" not in imported_modules
    assert "augment_context(" not in source


@pytest.mark.asyncio
async def test_context_authority_stays_strict_retrieval_owned() -> None:
    allowed = [("customer", "canonical-1")]
    result = await _augment_active_acts(
        object(),
        TriggerContext(kind="T1", tenant_id=uuid7()),
        ContextBundle(),
        allowed_region=allowed,
    )
    assert result is allowed


@pytest.mark.asyncio
async def test_fixture_noise_phrases_are_sent_through_reasoning() -> None:
    tenant_id = uuid7()
    observation_id = uuid7()
    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id],
        seed_natural_text=(
            "General operational chatter: lunch logistics and duplicated "
            "dashboard links. This should not dominate memory."
        ),
        seed_signature={"trigger_id": str(trigger_id)},
    )

    class RecordingProvider:
        calls = 0

        def enforces_output_schema(self, _schema):
            return False

        async def structured(self, **_kwargs):
            self.calls += 1
            return RawDiff(
                trigger_ref=trigger_id,
                tenant_id=tenant_id,
                reasoning_trace="Independent reasoner chose a no-op.",
            )

    provider = RecordingProvider()
    diff, elapsed_ms = await llm_reason(trigger, ContextBundle(), provider)  # type: ignore[arg-type]

    assert provider.calls == 1
    assert elapsed_ms >= 0
    assert diff.reasoning_trace
    assert "discard_as_noise" not in diff.reasoning_trace


def test_quarantined_helpers_have_no_production_callers() -> None:
    callers: dict[str, list[str]] = {
        "maybe_inject_capability_probe_ops": [],
        "maybe_inject_latent_bridge": [],
        "augment_context": [],
    }
    quarantined_definitions = {"capability_probes.py", "bridge_inference.py", "hooks.py"}
    for path in THINK.glob("*.py"):
        if path.name in quarantined_definitions:
            continue
        source = path.read_text(encoding="utf-8")
        for symbol in callers:
            if symbol in source:
                callers[symbol].append(path.name)
    assert callers == {key: [] for key in callers}
