"""services/think/tests/test_llm_reason.py — LLM reasoning + failures.

Covers spec §7 llm_reason + build_prompt:
  * build_prompt emits the required <triggering_event> /
    <retrieved_context>/<observations>/<models>/<acts>/<resources> /
    <operating_instructions> sections.
  * Happy-path ScriptedProvider returns a RawDiff via llm_reason.
  * LLMParseError → ReasoningFailure (terminal — provider exhausted
    retries).
  * Transient LLMError backs off with exponential sleep and retries up
    to max_attempts, then raises ReasoningFailure.
  * 5+ consecutive LLMError failures: the outer worker layer is tested
    for dead-letter routing separately in test_worker.py.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from lib.llm.provider import LLMError, LLMParseError, LLMConfig
from lib.shared.ids import uuid7
from lib.shared.types import ResourceRow

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.llm_reason import llm_reason, ReasoningFailure
from services.think.prompt import build_prompt
from services.think.tests.conftest import ScriptedProvider


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# build_prompt — section coverage
# =====================================================================


async def test_build_prompt_emits_all_sections():
    trigger = TriggerContext(
        kind="T1", tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_natural_text="Alice shipped feature X.",
        seed_occurred_at=datetime.now(timezone.utc),
    )
    bundle = ContextBundle()
    pair = build_prompt(
        trigger, bundle,
        triggering_content="Alice PR #187 merged",
        reason_for_trigger="PR merge webhook",
    )
    user = pair.user

    # Triggering event section
    assert "<triggering_event>" in user
    assert "</triggering_event>" in user
    assert "kind: T1" in user
    assert "Alice PR #187 merged" in user

    # Retrieved context container
    assert "<retrieved_context>" in user
    assert "</retrieved_context>" in user

    # Four required subsections per brief.
    assert "<observations>" in user and "</observations>" in user
    assert "<models>" in user and "</models>" in user
    assert "<acts>" in user and "</acts>" in user
    assert "<resources>" in user and "</resources>" in user

    # Operating instructions
    assert "<operating_instructions>" in user
    assert "</operating_instructions>" in user

    # System prompt carries the falsifier schema + diff schema.
    assert "Falsifier schema" in pair.system
    assert "observation_pattern" in pair.system
    assert "commitment_outcome" in pair.system
    assert "prediction_deadline" in pair.system
    assert "Diff schema" in pair.system
    assert "`confidence_basis` MUST be either an existing Model id" in pair.system


async def test_build_prompt_triggering_kind_instructions():
    """Each trigger kind gets its kind-specific operating instructions."""
    bundle = ContextBundle()
    for kind, needle in [
        ("T1", "new signal"),
        ("T2", "prediction Model"),
        ("T3", "anomaly region"),
        ("T4", "background"),
    ]:
        t = TriggerContext(kind=kind, tenant_id=uuid7())
        pair = build_prompt(t, bundle)
        assert needle in pair.user, f"T-kind {kind} missing '{needle}'"


async def test_build_prompt_adds_source_tuned_reasoning_profile():
    """T1 prompts include provenance/type metadata and a matching stance."""
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_signature={
            "source_channel": "github:webhook",
            "signal_type": "github:webhook/pull_request.closed",
            "observation_kind": "state_change",
            "trust_tier": "authoritative",
        },
    )
    pair = build_prompt(trigger, ContextBundle(), claims_only=True)

    assert "Reasoning profile for this call" in pair.system
    assert "Working personality: ledger clerk" in pair.system
    assert "Model surface: claim triage" in pair.system
    assert "Abstraction level: low and exact" in pair.system
    assert "source_channel: github:webhook" in pair.user
    assert "signal_type: github:webhook/pull_request.closed" in pair.user
    assert "trust_tier: authoritative" in pair.user


async def test_build_prompt_profiles_graph_aware_surface():
    tenant_id = uuid7()
    graph_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_signature={
            "source_channel": "slack:message",
            "signal_type": "slack:message/message",
            "observation_kind": "signal",
            "trust_tier": "attested_agent",
        },
    )
    bundle = ContextBundle(
        notes={
            "model_selection": {
                "selected_model_ids": [str(graph_id)],
                "pathway_survival": {
                    "G": {"selected_model_ids": [str(graph_id)]},
                },
            }
        }
    )

    pair = build_prompt(trigger, bundle)

    assert "Working personality: contextual listener" in pair.system
    assert "Model surface: graph cartographer" in pair.system
    assert "Abstraction level: relationship level" in pair.system


async def test_build_prompt_respects_char_truncation():
    """Long content_text is truncated so the per-item char limit holds."""
    huge_text = "x" * 5000
    trigger = TriggerContext(
        kind="T1", tenant_id=uuid7(),
        seed_natural_text=huge_text,
    )
    bundle = ContextBundle()
    pair = build_prompt(
        trigger, bundle,
        triggering_content=huge_text,
    )
    # The per-item limit is 1500; the message must be < 5000-chars for the
    # triggering content line specifically.
    assert "..." in pair.user  # truncation marker was inserted
    # The whole user prompt should not exceed the sum of budgets + some slack.
    assert len(pair.user) < 25000


async def test_build_prompt_resources_include_identity_for_scope_resolution():
    tenant_id = uuid7()
    resource_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id)
    bundle = ContextBundle(
        resources_summary=[
            ResourceRow(
                id=resource_id,
                tenant_id=tenant_id,
                kind="relational",
                identity="Globex Inc",
                description="Customer: Globex Inc",
                current_value={"health": "at_risk", "arr_usd": 60000},
                created_at=datetime.now(timezone.utc),
                last_updated_at=datetime.now(timezone.utc),
            )
        ]
    )

    pair = build_prompt(trigger, bundle)

    assert f"resource id={resource_id}" in pair.user
    assert "identity=Globex Inc" in pair.user
    assert "description=Customer: Globex Inc" in pair.user


async def test_build_prompt_surfaces_selected_graph_memory_priority():
    tenant_id = uuid7()
    selected_id = uuid7()
    graph_id = uuid7()
    trigger = TriggerContext(kind="T2", tenant_id=tenant_id, model_id=selected_id)
    bundle = ContextBundle(
        notes={
            "model_selection": {
                "selected_count": 2,
                "selected_model_ids": [str(selected_id), str(graph_id)],
                "pathway_survival": {
                    "G": {"selected_model_ids": [str(graph_id)]}
                },
            }
        }
    )

    pair = build_prompt(trigger, bundle)

    assert "<retrieval_priority>" in pair.user
    assert "selected_model_ids (2)" in pair.user
    assert "graph_anchor_model_ids (1)" in pair.user
    assert str(selected_id) in pair.user
    assert str(graph_id) in pair.user
    assert "Graph anchors are the memory layer's strongest candidate" in pair.user
    assert "Edge endpoints must be existing Model ids from <models>" in pair.user
    assert "born_from_event_id of a claim_ops.insert" in pair.user
    assert "reasoning_trace must name at least one relevant selected/graph Model" in pair.user


async def test_build_prompt_claims_only_profile_uses_smaller_system_prompt():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7(), observation_id=uuid7())
    bundle = ContextBundle()

    full = build_prompt(trigger, bundle)
    compact = build_prompt(trigger, bundle, claims_only=True)

    assert "This compact pass can only emit" in compact.system
    assert "edge_ops entry shape" not in compact.system
    assert len(compact.system) < len(full.system)


# =====================================================================
# llm_reason — happy path
# =====================================================================


def _minimal_raw_diff_json(trigger_id: str, tenant_id: str) -> str:
    return json.dumps({
        "trigger_ref": trigger_id,
        "tenant_id": tenant_id,
        "claim_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "test scripted diff",
    })


async def test_llm_reason_happy_path_returns_raw_diff():
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )
    diff, latency_ms = await llm_reason(
        trigger, bundle, provider,
        triggering_content="x",
    )
    assert diff.tenant_id == tid
    assert diff.trigger_ref == trig_id
    assert diff.claim_ops == []
    assert latency_ms >= 0


async def test_llm_reason_uses_claims_only_schema_when_edges_are_impossible():
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, ContextBundle(), provider)

    assert "edge_ops" not in provider.calls[0]["schema_hint"]
    assert "This compact pass can only emit" in provider.calls[0]["system"]


async def test_llm_reason_keeps_edge_schema_when_models_are_available():
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=model_id,
                proposition_kind="state",
                confidence=0.8,
                activation=0.5,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural="Existing graph memory.",
            )
        ]
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, bundle, provider)

    assert "edge_ops" in provider.calls[0]["schema_hint"]
    assert "This compact pass can only emit" not in provider.calls[0]["system"]


async def test_llm_reason_keeps_full_schema_when_acts_are_available():
    tid = uuid7()
    trig_id = uuid7()
    commitment_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="PR merged for the migration.",
    )
    bundle = ContextBundle(
        acts_summary={
            "goals": [],
            "commitments": [
                SimpleNamespace(
                    id=commitment_id,
                    state="active",
                    owner_id=None,
                    due_date=None,
                    title="Finish the migration",
                )
            ],
            "decisions": [],
        }
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, bundle, provider)

    assert "act_ops" in provider.calls[0]["schema_hint"]
    assert "This compact pass can only emit" not in provider.calls[0]["system"]


async def test_llm_reason_keeps_full_schema_for_t2_graph_anchors():
    tid = uuid7()
    trig_id = uuid7()
    selected_id = uuid7()
    graph_id = uuid7()
    trigger = TriggerContext(
        kind="T2",
        subkind="belief_updated",
        tenant_id=tid,
        model_id=selected_id,
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="hidden warning",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=selected_id,
                proposition_kind="state",
                confidence=0.8,
                activation=0.5,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural="Selected memory.",
            ),
            SimpleNamespace(
                id=graph_id,
                proposition_kind="concern",
                confidence=0.78,
                activation=0.6,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural="Graph anchor memory.",
            ),
        ],
        notes={
            "model_selection": {
                "selected_model_ids": [str(selected_id), str(graph_id)],
                "pathway_survival": {
                    "G": {"selected_model_ids": [str(graph_id)]},
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, bundle, provider)

    assert "edge_ops" in provider.calls[0]["schema_hint"]
    assert "This compact pass can only emit" not in provider.calls[0]["system"]


async def test_llm_reason_parse_error_terminal():
    """
    Scripted provider returns ONLY malformed JSON for all attempts the
    provider's internal retry uses. The LLMParseError bubbles out of
    provider.structured and llm_reason treats it as terminal.
    """
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    # LLMConfig.max_retries defaults to 2 → 3 attempts inside structured().
    # Queue enough malformed responses for each internal retry.
    cfg = LLMConfig(provider="anthropic", api_key="test", model="m", max_retries=2)
    provider = ScriptedProvider(
        responses=["not json at all", "still not json", "and not json"],
        cfg=cfg,
    )
    with pytest.raises(ReasoningFailure):
        await llm_reason(trigger, bundle, provider)


async def test_llm_reason_transient_error_retries_then_fails():
    """
    All attempts raise LLMError. llm_reason retries with exponential
    backoff and raises ReasoningFailure after max_attempts.
    """
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[
            LLMError("transient 500"),
            LLMError("transient 500"),
            LLMError("transient 500"),
        ],
    )
    # max_attempts=3, with backoff 1s + 2s on first two retries.
    # That would take ~3s total — we keep max_attempts small but still
    # exercise the retry loop.
    t0 = time.monotonic()
    with pytest.raises(ReasoningFailure):
        await llm_reason(
            trigger, bundle, provider,
            max_attempts=2,  # 1 retry → backoff 2^0 = 1s
        )
    elapsed = time.monotonic() - t0
    # Two attempts total; one sleep of 1s between them.
    assert elapsed >= 1.0
    assert elapsed < 10.0


async def test_llm_reason_transient_then_success_recovers():
    """First attempt raises LLMError; second attempt returns a valid diff."""
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[
            LLMError("transient"),
            _minimal_raw_diff_json(str(trig_id), str(tid)),
        ],
    )
    diff, _ = await llm_reason(
        trigger, bundle, provider,
        max_attempts=3,
    )
    assert diff.tenant_id == tid


async def test_llm_reason_records_call_count():
    """The ScriptedProvider records calls; each retry logs one call."""
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )
    await llm_reason(trigger, bundle, provider)
    assert len(provider.calls) == 1
    assert "system" in provider.calls[0]
    assert "user" in provider.calls[0]
    assert "<triggering_event>" in provider.calls[0]["user"]
