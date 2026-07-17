"""services/reasoning/think/tests/test_llm_reason.py — LLM reasoning + failures.

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
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lib.llm.provider import LLMError, LLMConfig
from lib.shared.ids import uuid7
from lib.shared.types import ResourceRow

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    apply_relation_lifecycle_kernel,
    grounding_claim_ops_from_obligations,
    grounding_obligations_from_packet,
    relation_claim_ops_from_obligations,
    relation_frame_obligations_from_obligations,
    relation_frame_ops_from_obligations,
    relation_obligations_from_packet,
)
from services.reasoning.think.diff_schema import (
    ClaimOp,
    EdgeOp,
    FormationResolutionOp,
    RawDiff,
    RawDiffClaimsOnly,
    ValidatedDiff,
)
from services.reasoning.think.llm_reason import llm_reason, ReasoningFailure
from services.reasoning.think.llm_reason import _coerce_raw_diff
from services.reasoning.think import applier as applier_module
from services.reasoning.think.applier import (
    _emit_question_policy_valid_diff_feedback,
    _emit_valid_diff_outcome_events,
    _upsert_question_policy_valid_diff_stats,
)
from services.reasoning.think.prompt import build_prompt
from services.reasoning.think.tests.conftest import ScriptedProvider


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# build_prompt — section coverage
# =====================================================================


async def test_build_prompt_emits_all_sections():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_natural_text="Alice shipped feature X.",
        seed_occurred_at=datetime.now(timezone.utc),
    )
    bundle = ContextBundle()
    pair = build_prompt(
        trigger,
        bundle,
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

    # Operating instructions live in the stable system prefix.
    assert "<operating_instructions>" in pair.system
    assert "</operating_instructions>" in pair.system

    # System prompt carries the falsifier schema + diff schema.
    assert "Falsifier schema" in pair.system
    assert "observation_pattern" in pair.system
    assert "commitment_outcome" in pair.system
    assert "prediction_deadline" in pair.system
    assert "Diff schema" in pair.system
    assert "`confidence_basis` MUST be either an existing Model id" in pair.system


async def test_build_prompt_surfaces_model_formation_candidates():
    tenant_id = uuid7()
    actor_id = uuid7()
    observations = [
        SimpleNamespace(
            id=uuid7(),
            actor_id=actor_id,
            trust_tier="attested_agent",
            source_channel="slack:message",
            content_text="Alice needs clearer owner boundaries before starting.",
            occurred_at=datetime.now(timezone.utc),
        ),
        SimpleNamespace(
            id=uuid7(),
            actor_id=actor_id,
            trust_tier="attested_agent",
            source_channel="slack:message",
            content_text="Alice was blocked waiting for a product decision.",
            occurred_at=datetime.now(timezone.utc),
        ),
    ]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
    )
    pair = build_prompt(trigger, ContextBundle(observations=observations))

    assert "<model_formation_candidates>" in pair.user
    assert "employee.support_need" in pair.user
    assert "required_decision_count: 1" in pair.user
    assert "formation_resolutions" in pair.system
    assert "already_covered" in pair.system


async def test_claims_only_diff_preserves_formation_resolutions_on_coerce():
    candidate_id = f"formation:employee.capability:{uuid7()}:abc123"
    compact = RawDiffClaimsOnly(
        trigger_ref=uuid7(),
        tenant_id=uuid7(),
        claim_ops=[],
        formation_resolutions=[
            FormationResolutionOp(
                candidate_id=candidate_id,
                resolution="rejected",
                rationale="The evidence repeats words but not a stable capability.",
            )
        ],
        reasoning_trace="resolved formation candidate",
    )

    full = _coerce_raw_diff(compact)

    assert full.formation_resolutions[0].candidate_id == candidate_id
    assert full.formation_resolutions[0].resolution == "rejected"


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
        assert needle in pair.system, f"T-kind {kind} missing '{needle}'"


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

    assert "Reasoning profile for this call" in pair.user
    assert "Working personality: ledger clerk" in pair.user
    assert "Model surface: claim triage" in pair.user
    assert "Abstraction level: low and exact" in pair.user
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

    assert "Working personality: contextual listener" in pair.user
    assert "Model surface: graph cartographer" in pair.user
    assert "Abstraction level: relationship level" in pair.user


async def test_build_prompt_respects_char_truncation():
    """Long content_text is truncated so the per-item char limit holds."""
    huge_text = "x" * 5000
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid7(),
        seed_natural_text=huge_text,
    )
    bundle = ContextBundle()
    pair = build_prompt(
        trigger,
        bundle,
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
                "pathway_survival": {"G": {"selected_model_ids": [str(graph_id)]}},
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
    assert "reasoning_trace must name at least one selected/graph Model" in pair.user


async def test_build_prompt_surfaces_inquiry_context_packet():
    tenant_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id)
    bundle = ContextBundle(
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Acme cannot launch without SSO.",
                "sufficiency_verdict": {
                    "status": "sufficient_for_reasoning",
                    "reason": "counterevidence checked",
                },
                "hypotheses": [
                    {
                        "id": "H1",
                        "claim": "Acme is blocked by SSO.",
                        "confidence": 0.74,
                    },
                    {"id": "H0", "claim": "No update needed."},
                ],
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_H1",
                        "op_family": "edge_insert",
                        "proposed_text": (
                            "Decide whether SSO blocks the Acme launch model."
                        ),
                        "target_model_ids": ["model-a", "model-b"],
                        "source_observation_ids": ["obs-a"],
                        "supporting_evidence_ids": ["ev1"],
                        "uncertainty_slots": [
                            "whether the relationship is explicit enough to store"
                        ],
                        "suggested_edge_kinds": ["blocks", "explains", "supports"],
                        "write_preconditions": [
                            "Use blocks only when source evidence gates target progress."
                        ],
                        "answer_summary": "Q1:DEPENDENCY=supported support=2 counter=0",
                        "confidence": 0.66,
                        "reason": "Dependency question implies a possible edge op",
                    }
                ],
                "model_residual_spine": [
                    {
                        "residual_id": "residual-1",
                        "residual_kind": "compression_uncertain",
                        "source_observation_id": "obs-a",
                        "compact_summary": (
                            "Think previously succeeded but did not produce a "
                            "durable fate for this signal."
                        ),
                        "reason": "think_success_without_durable_fate",
                        "non_canonical": True,
                    }
                ],
                "question_path": [
                    {
                        "question_id": "Q1",
                        "primitive": "DEPENDENCY",
                        "question": "Is SSO on the critical path?",
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "observation",
                            "summary": "CRM says SSO is required.",
                        }
                    ],
                    "supporting_evidence_groups": [],
                    "omission_ledger": [],
                },
            }
        }
    )

    pair = build_prompt(trigger, bundle)

    assert "<inquiry_context_packet>" in pair.user
    assert "Acme cannot launch without SSO" in pair.user
    assert "sufficient_for_reasoning" in pair.user
    assert "memory_decision_candidates are advisory" in pair.user
    assert "id=MDC_H1 op=edge_insert" in pair.user
    assert "whether the relationship is explicit enough to store" in pair.user
    assert "suggested_edge_kinds" in pair.user
    assert "Q1:DEPENDENCY=supported" in pair.user
    assert "model_residual_spine: non-canonical compact compression debt" in pair.user
    assert "compression_uncertain" in pair.user
    assert "think_success_without_durable_fate" in pair.user
    assert "Q1 [DEPENDENCY]" in pair.user


async def test_build_prompt_suppresses_t1_batch_raw_text_for_model_only_packet():
    tenant_id = uuid7()
    observation_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id, uuid7()],
        seed_natural_text="RAW_BATCH_SEED_MARKER " + ("raw observation text " * 80),
        seed_signature={
            "batch": True,
            "batch_observation_ids": [str(observation_id), str(uuid7())],
        },
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(
                id=uuid7(),
                actor_id=uuid7(),
                trust_tier="verified",
                source_channel="slack",
                occurred_at=datetime.now(timezone.utc),
                content_text=(
                    "RAW_CONTEXT_OBSERVATION_BODY_MARKER "
                    + ("retrieved observation body " * 40)
                ),
            )
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "RAW_PACKET_SIGNAL_SUMMARY_MARKER " + ("raw signal summary " * 40)
                ),
                "source_metadata": {"trigger_kind": "T1"},
                "budget": {
                    "evidence_policy": {
                        "mode": "models_only",
                        "fallback_reason": None,
                    }
                },
                "tiers": {
                    "decisive_evidence": [],
                    "supporting_evidence_groups": [],
                    "omission_ledger": [],
                },
            }
        },
    )

    pair = build_prompt(
        trigger,
        bundle,
        triggering_content="TRIGGERING_RAW_BATCH_MARKER " + ("signal " * 80),
    )

    assert "RAW_BATCH_SEED_MARKER" not in pair.user
    assert "RAW_PACKET_SIGNAL_SUMMARY_MARKER" not in pair.user
    assert "TRIGGERING_RAW_BATCH_MARKER" not in pair.user
    assert "RAW_CONTEXT_OBSERVATION_BODY_MARKER" not in pair.user
    assert "raw batch text suppressed by model-only Think evidence policy" in pair.user
    assert (
        "raw observation bodies omitted by model-only Think evidence policy"
        in pair.user
    )
    assert "retrieved_observation_count: 1" in pair.user
    assert "batch_observation_count: 2" in pair.user


async def test_build_prompt_raw_evidence_floor_overrides_model_only_suppression():
    tenant_id = uuid7()
    observation_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id, uuid7()],
        seed_natural_text="RAW_BATCH_SEED_MARKER " + ("raw observation text " * 20),
        seed_signature={
            "batch": True,
            "batch_observation_ids": [str(observation_id), str(uuid7())],
        },
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(
                id=observation_id,
                actor_id=None,
                trust_tier="verified",
                source_channel="signal:message",
                occurred_at=datetime.now(timezone.utc),
                content_text="RAW_CONTEXT_OBSERVATION_BODY_MARKER Atlas blocker update",
            )
        ],
        notes={
            "observation_selection": {
                "floor_reason": "explicit_t1_event_batch_raw_evidence_floor",
                "selected_count": 1,
            },
            "inquiry_context_packet": {
                "signal_summary": "RAW_PACKET_SIGNAL_SUMMARY_MARKER",
                "source_metadata": {"trigger_kind": "T1"},
                "budget": {
                    "evidence_policy": {
                        "mode": "models_only",
                        "fallback_reason": None,
                    }
                },
            },
        },
    )

    pair = build_prompt(
        trigger,
        bundle,
        triggering_content="TRIGGERING_RAW_BATCH_MARKER signal floor active",
    )

    assert "RAW_CONTEXT_OBSERVATION_BODY_MARKER" in pair.user
    assert "TRIGGERING_RAW_BATCH_MARKER" in pair.user
    assert "RAW_BATCH_SEED_MARKER" in pair.user
    assert "raw observation bodies omitted by model-only Think evidence policy" not in pair.user


async def test_build_prompt_compiles_batch_memory_decision_packet(monkeypatch):
    monkeypatch.setenv("THINK_COMPILED_MEMORY_DECISION_PROMPT", "1")
    tenant_id = uuid7()
    observation_id = uuid7()
    second_observation_id = uuid7()
    actor_id = uuid7()
    target_model_id = uuid7()
    background_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=observation_id,
        observation_ids=[observation_id, second_observation_id],
        seed_natural_text="RAW_BATCH_SEED_MARKER " + ("raw batch text " * 80),
    )
    notes = {
        "model_selection": {
            "selected_count": 2,
            "selected_model_ids": [str(target_model_id), str(background_model_id)],
            "pathway_survival": {"G": {"selected_model_ids": [str(target_model_id)]}},
        },
        "inquiry_context_packet": {
            "signal_summary": "Acme has a repeat SSO launch blocker.",
            "sufficiency_verdict": {
                "status": "sufficient_for_reasoning",
                "reason": "candidate evidence is enough",
            },
            "hypotheses": [
                {
                    "id": "H1",
                    "claim": "VERBOSE_HYPOTHESIS_MARKER Acme is blocked by SSO.",
                    "confidence": 0.78,
                }
            ],
            "memory_decision_candidates": [
                {
                    "candidate_id": "MDC_H1",
                    "op_family": "edge_insert",
                    "proposed_text": "SSO readiness blocks Acme launch.",
                    "target_model_ids": [str(target_model_id)],
                    "evidence_model_ids": [str(target_model_id)],
                    "source_observation_ids": [str(observation_id)],
                    "supporting_evidence_ids": ["ev1"],
                    "uncertainty_slots": ["new edge versus no-op"],
                    "confidence": 0.72,
                    "reason": "Dependency evidence implies a possible edge.",
                }
            ],
            "question_path": [
                {
                    "question_id": "Q1",
                    "primitive": "DEPENDENCY",
                    "question": "QUESTION_PATH_MARKER Is SSO on the path?",
                }
            ],
            "tiers": {
                "decisive_evidence": [
                    {
                        "evidence_id": "ev1",
                        "source_type": "model",
                        "source_ref": f"model:{target_model_id}",
                        "summary": "CANDIDATE_EVIDENCE_SUMMARY SSO is launch-critical.",
                        "supports_hypotheses": ["H1"],
                        "weakens_hypotheses": [],
                        "contradicts_hypotheses": [],
                    }
                ],
                "supporting_evidence_groups": [
                    {
                        "claim_supported": "H1",
                        "evidence_count": 1,
                        "sources": ["model"],
                        "summary": "SUPPORTING_GROUP_MARKER additional planner text.",
                        "evidence_ids": ["ev1"],
                        "source_refs": [f"model:{target_model_id}"],
                    }
                ],
                "omission_ledger": [
                    {"reason": "OMISSION_LEDGER_MARKER redundant evidence"}
                ],
            },
            "budget": {
                "reservoir_evidence_count": 12,
                "packet_evidence_count": 3,
                "evidence_policy": {"mode": "model_first"},
            },
        },
    }
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(
                id=observation_id,
                actor_id=actor_id,
                trust_tier="authoritative",
                source_channel="slack",
                occurred_at=datetime.now(timezone.utc),
                content_text="FULL_OBSERVATION_BODY_MARKER " + ("body " * 200),
            )
        ],
        models=[
            SimpleNamespace(
                id=background_model_id,
                proposition_kind="state",
                confidence=0.7,
                activation=0.4,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural=(
                    "Background compact intro "
                    + ("background " * 120)
                    + "BACKGROUND_MODEL_TAIL_MARKER"
                ),
            ),
            SimpleNamespace(
                id=target_model_id,
                proposition_kind="concern",
                confidence=0.86,
                activation=0.9,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural="TARGET_MODEL_DETAIL_MARKER SSO blocks Acme launch.",
            ),
        ],
        notes=notes,
    )

    compact = build_prompt(
        trigger,
        bundle,
        triggering_content="TRIGGERING_BATCH_FULL_MARKER " + ("signal " * 200),
    )
    monkeypatch.setenv("THINK_COMPILED_MEMORY_DECISION_PROMPT", "0")
    full = build_prompt(
        trigger,
        bundle,
        triggering_content="TRIGGERING_BATCH_FULL_MARKER " + ("signal " * 200),
    )

    assert "mode: compiled_memory_decision_boundary" in compact.user
    assert "planner_artifacts: omitted from prompt" in compact.user
    assert "id=MDC_H1 op=edge_insert" in compact.user
    assert "CANDIDATE_EVIDENCE_SUMMARY" in compact.user
    assert "candidate_source_observation_ids" in compact.user
    assert "FULL_OBSERVATION_BODY_MARKER" not in compact.user
    assert "VERBOSE_HYPOTHESIS_MARKER" not in compact.user
    assert "QUESTION_PATH_MARKER" not in compact.user
    assert "OMISSION_LEDGER_MARKER" not in compact.user
    assert "TARGET_MODEL_DETAIL_MARKER" in compact.user
    assert "BACKGROUND_MODEL_TAIL_MARKER" not in compact.user

    assert "VERBOSE_HYPOTHESIS_MARKER" in full.user
    assert "QUESTION_PATH_MARKER" in full.user
    assert "FULL_OBSERVATION_BODY_MARKER" in full.user
    assert len(compact.user) < len(full.user) * 0.75


async def test_build_prompt_uses_compact_model_manifest_with_inquiry_packet():
    tenant_id = uuid7()
    actionable_id = uuid7()
    background_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id)
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=actionable_id,
                proposition_kind="concern",
                confidence=0.84,
                activation=0.91,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural="ACTIONABLE_FULL_DETAIL_MARKER " + ("primary context " * 80),
            ),
            SimpleNamespace(
                id=background_id,
                proposition_kind="state",
                confidence=0.72,
                activation=0.51,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[],
                scope_entities=[],
                natural=(
                    "Background compact intro "
                    + ("redundant detail " * 40)
                    + "NONACTIONABLE_TAIL_MARKER"
                ),
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Acme launch is blocked.",
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "tiers": {
                    "decisive_evidence": [
                        {
                            "source_type": "observation",
                            "source_ref": "observation:00000000-0000-7000-8000-000000000001",
                            "summary": "Authoritative observation is decisive.",
                        }
                    ],
                    "supporting_evidence_groups": [
                        {
                            "claim_supported": "H1",
                            "evidence_count": 1,
                            "sources": ["model"],
                            "summary": "Actionable model supports the leading hypothesis.",
                            "evidence_ids": ["ev-model-1"],
                            "source_refs": [f"model:{actionable_id}"],
                        }
                    ],
                    "omission_ledger": [],
                },
            }
        },
    )

    pair = build_prompt(trigger, bundle)
    start = pair.user.index("  <models>")
    end = pair.user.index("  </models>") + len("  </models>")
    models_section = pair.user[start:end]

    assert "manifest_mode: compact" in models_section
    assert f"id={actionable_id} detail=full" in models_section
    assert "ACTIONABLE_FULL_DETAIL_MARKER" in models_section
    assert f"id={background_id} detail=manifest" in models_section
    assert "Background compact intro" in models_section
    assert "NONACTIONABLE_TAIL_MARKER" not in models_section
    assert len(models_section) < 1800


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
    return json.dumps(
        {
            "trigger_ref": trigger_id,
            "tenant_id": tenant_id,
            "claim_ops": [],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": "test scripted diff",
        }
    )


async def test_llm_reason_happy_path_returns_raw_diff():
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )
    diff, latency_ms = await llm_reason(
        trigger,
        bundle,
        provider,
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
        kind="T1",
        tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, ContextBundle(), provider)

    assert "edge_ops" not in provider.calls[0]["schema_hint"]
    assert "This compact pass can only emit" in provider.calls[0]["system"]
    assert provider.calls[0]["max_tokens"] == 1024


async def test_llm_reason_claims_only_output_cap_is_configurable(monkeypatch):
    monkeypatch.setenv("THINK_CLAIMS_ONLY_MAX_TOKENS", "512")
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, ContextBundle(), provider, max_tokens=2048)

    assert provider.calls[0]["max_tokens"] == 512


async def test_llm_reason_keeps_edge_schema_when_models_are_available():
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
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
    assert provider.calls[0]["max_tokens"] == 2048


async def test_llm_reason_compiled_batch_memory_emits_code_built_ops(monkeypatch):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    target_model_id = uuid7()
    commitment_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Acme is waiting on SSO readiness before launch.",
        seed_entity_ids=[{"type": "customer_resource", "id": str(uuid7())}],
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=target_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="Acme launch depends on SSO readiness.",
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "Acme launch",
                    "assertion": "Acme launch depends on SSO readiness",
                },
            )
        ],
        acts_summary={
            "goals": [],
            "commitments": [
                SimpleNamespace(
                    id=commitment_id,
                    state="active",
                    title="Launch Acme SSO",
                )
            ],
            "decisions": [],
        },
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Acme is waiting on SSO readiness.",
                "sufficiency_verdict": {
                    "status": "sufficient_for_reasoning",
                    "reason": "candidate evidence is enough",
                },
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_H1",
                        "op_family": "claim_update",
                        "proposed_text": "Acme launch is blocked by SSO readiness.",
                        "target_model_ids": [str(target_model_id)],
                        "evidence_model_ids": [str(target_model_id)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "suggested_edge_kinds": ["blocks", "explains", "supports"],
                        "write_preconditions": [
                            "Use blocks only when source evidence gates target progress.",
                            "Use same_issue_as or analogous_to only as candidate/review similarity.",
                        ],
                        "answer_summary": "Q_CRITICAL_PATH:DEPENDENCY=supported support=1",
                        "confidence": 0.72,
                        "reason": "Batch-level dependency evidence.",
                    },
                    {
                        "candidate_id": "MDC_ACT_H1",
                        "op_family": "act_update",
                        "proposed_text": "Pause the Acme launch commitment while SSO is not ready.",
                        "target_act_ids": [str(commitment_id)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev2"],
                        "confidence": 0.7,
                        "reason": "Commitment is waiting on external readiness.",
                    },
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "model",
                            "source_ref": f"model:{target_model_id}",
                            "summary": "SSO readiness is on the launch path.",
                            "supports_hypotheses": ["H1"],
                        },
                        {
                            "evidence_id": "ev2",
                            "source_type": "commitment",
                            "source_ref": f"commitment:{commitment_id}",
                            "summary": "The launch commitment is still active.",
                            "supports_hypotheses": ["H1"],
                        },
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": "MDC_H1",
                            "decision": "accept",
                            "operation": "claim_and_edge",
                            "confidence": 0.74,
                            "claim_role": "concern",
                            "claim_text": "Acme launch is blocked by SSO readiness.",
                            "edge_kind": "blocks",
                            "target_model_id": str(target_model_id),
                            "reason": "The batch states a concrete launch dependency.",
                        },
                        {
                            "candidate_id": "MDC_ACT_H1",
                            "decision": "accept",
                            "operation": "claim_and_act",
                            "confidence": 0.71,
                            "claim_role": "concern",
                            "claim_text": "The Acme launch commitment is waiting on SSO readiness.",
                            "act_type": "commitment",
                            "act_target_id": str(commitment_id),
                            "act_new_state": "paused",
                            "reason": "The active launch commitment should pause until SSO is ready.",
                        },
                    ],
                    "reasoning_trace": "Accepted two closed-world candidates.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert provider.calls[0]["max_tokens"] == 1200
    assert "<compiled_batch_memory_task>" in provider.calls[0]["user"]
    assert "Operational edge kinds to prefer when evidenced" in provider.calls[0]["user"]
    assert "suggested_edge_kinds" in provider.calls[0]["user"]
    assert "Use blocks only when source evidence gates target progress." in provider.calls[0]["user"]
    assert "Q_CRITICAL_PATH:DEPENDENCY=supported" in provider.calls[0]["user"]
    assert "claim_and_edge" in provider.calls[0]["schema_hint"]
    assert "claim_ops" not in provider.calls[0]["schema_hint"]
    assert len(diff.claim_ops) == 2
    assert len(diff.relation_claim_ops) == 1
    assert diff.edge_ops == []
    assert len(diff.act_ops) == 1
    first_claim = diff.claim_ops[0]
    assert first_claim.entry["proposition"]["claim_role"] == "concern"
    assert first_claim.entry["confidence"] == 0.69
    assert "compiled_memory_candidate_id" not in first_claim.entry
    relation = diff.relation_claim_ops[0]
    assert str(relation.source_model_id) == first_claim.entry["born_from_event_id"]
    assert relation.target_model_id == target_model_id
    assert relation.edge_kind == "blocks"
    assert relation.write_policy == "accepted_edge"
    act = diff.act_ops[0]
    assert act.op == "transition_commitment"
    assert act.entity["id"] == commitment_id
    assert act.entity["new_state"] == "paused"
    assert "compiled_memory_candidate_id" not in act.entity
    assert str(act.confidence_basis) == diff.claim_ops[1].entry["born_from_event_id"]


async def test_llm_reason_compiled_batch_memory_emits_relation_from_hinted_update(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Acme is blocked by SSO readiness before launch.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=source_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="SSO readiness is not complete.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=target_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="Acme launch depends on SSO readiness.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Acme launch is blocked by SSO readiness.",
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_H1",
                        "op_family": "claim_update",
                        "proposed_text": "Acme launch is blocked by SSO readiness.",
                        "target_model_ids": [str(target_model_id)],
                        "evidence_model_ids": [
                            str(source_model_id),
                            str(target_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "suggested_edge_kinds": ["blocks", "supports"],
                        "confidence": 0.75,
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "model",
                            "source_ref": f"model:{source_model_id}",
                            "summary": "SSO readiness gates the Acme launch.",
                            "supports_hypotheses": ["H1"],
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": "MDC_H1",
                            "decision": "accept",
                            "operation": "claim_update",
                            "confidence": 0.75,
                            "reason": (
                                "The existing launch model is reinforced by a "
                                "concrete blocker."
                            ),
                        }
                    ],
                    "reasoning_trace": "Accepted the hinted update.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert len(diff.claim_ops) == 1
    assert len(diff.relation_claim_ops) == 1
    assert diff.edge_ops == []
    relation = diff.relation_claim_ops[0]
    assert relation.source_model_id == source_model_id
    assert relation.target_model_id == target_model_id
    assert relation.edge_kind == "blocks"
    assert relation.write_policy == "candidate"
    assert relation.metadata["relation_claim_origin"] == "compiled_batch_relation_hint"


async def test_llm_reason_compiled_batch_memory_emits_mandatory_relation_for_no_op(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    blocker_model_id = uuid7()
    launch_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Legal approval is blocking the launch.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=blocker_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="Legal approval is still pending.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=launch_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="Launch cannot proceed without legal approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Launch is blocked by legal approval.",
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_BLOCKER",
                        "op_family": "claim_update",
                        "proposed_text": "Legal approval blocks the launch.",
                        "target_model_ids": [str(launch_model_id)],
                        "evidence_model_ids": [
                            str(blocker_model_id),
                            str(launch_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "suggested_edge_kinds": ["blocks", "supports"],
                        "confidence": 0.76,
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "model",
                            "source_ref": f"model:{blocker_model_id}",
                            "summary": "Legal approval gates launch progress.",
                            "supports_hypotheses": ["H1"],
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": "MDC_BLOCKER",
                            "decision": "reject",
                            "operation": "no_op",
                            "confidence": 0.61,
                            "reason": "No node update is needed.",
                        }
                    ],
                    "reasoning_trace": "Rejected the memory update.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert diff.claim_ops == []
    assert len(diff.relation_claim_ops) == 1
    relation = diff.relation_claim_ops[0]
    assert relation.metadata["relation_claim_origin"] == "mandatory_relation_obligation"
    assert relation.edge_kind == "blocks"
    assert relation.source_model_id == blocker_model_id
    assert relation.target_model_id == launch_model_id
    assert relation.write_policy == "needs_review"
    assert "mandatory_relation_obligations=perceived:1,emitted:1" in (
        diff.reasoning_trace or ""
    )


async def test_relation_obligations_use_candidate_local_clause_not_batch_blocker():
    explainer_model_id = uuid7()
    controls_model_id = uuid7()
    weakener_model_id = uuid7()
    confidence_model_id = uuid7()
    resolution_model_id = uuid7()
    approval_model_id = uuid7()
    packet = {
        "signal_summary": (
            "Batch says procurement is waiting status until audit evidence is "
            "available, but individual candidates carry sharper relation types."
        ),
        "memory_decision_candidates": [],
        "tiers": {
            "decisive_evidence": [
                {
                    "evidence_id": "ev_explains",
                    "summary": (
                        "SOC2 evidence helps explain why enterprise controls "
                        "remain the top renewal lever."
                    ),
                },
                {
                    "evidence_id": "ev_weakens",
                    "summary": "Incident opacity contradicts sponsor confidence.",
                },
                {
                    "evidence_id": "ev_resolution",
                    "summary": "Audit export unblocks renewal approval.",
                },
            ]
        },
    }
    candidates = [
        {
            "candidate_id": "MDC_EXPLAINS",
            "op_family": "claim_update",
            "proposed_text": (
                "SOC2 evidence helps explain why enterprise controls remain "
                "the top renewal lever."
            ),
            "target_model_ids": [str(controls_model_id)],
            "evidence_model_ids": [str(explainer_model_id), str(controls_model_id)],
            "supporting_evidence_ids": ["ev_explains"],
            "suggested_edge_kinds": ["explains", "blocks", "supports"],
            "confidence": 0.76,
        },
        {
            "candidate_id": "MDC_WEAKENS",
            "op_family": "claim_update",
            "proposed_text": "Incident opacity contradicts sponsor confidence.",
            "target_model_ids": [str(confidence_model_id)],
            "evidence_model_ids": [str(weakener_model_id), str(confidence_model_id)],
            "supporting_evidence_ids": ["ev_weakens"],
            "suggested_edge_kinds": ["weakens", "blocks", "supports"],
            "confidence": 0.74,
        },
        {
            "candidate_id": "MDC_RESOLUTION",
            "op_family": "claim_update",
            "proposed_text": "Audit export unblocks renewal approval.",
            "target_model_ids": [str(approval_model_id)],
            "evidence_model_ids": [str(resolution_model_id), str(approval_model_id)],
            "supporting_evidence_ids": ["ev_resolution"],
            "suggested_edge_kinds": [
                "contributes_to_resolution",
                "blocks",
                "supports",
            ],
            "confidence": 0.74,
        },
    ]

    obligations = relation_obligations_from_packet(packet, candidates)

    by_candidate = {obligation.candidate_id: obligation for obligation in obligations}
    assert by_candidate["MDC_EXPLAINS"].edge_kind == "explains"
    assert by_candidate["MDC_WEAKENS"].edge_kind == "weakens"
    assert by_candidate["MDC_RESOLUTION"].edge_kind == "contributes_to_resolution"
    assert all(obligation.edge_kind != "blocks" for obligation in obligations)

    ops, _summary = relation_claim_ops_from_obligations(obligations)
    by_kind = {op.edge_kind: op for op in ops}
    assert by_kind["weakens"].weight == by_candidate["MDC_WEAKENS"].confidence
    assert by_kind["contributes_to_resolution"].weight is None


async def test_relation_obligations_ignore_write_preconditions_as_evidence():
    source_model_id = uuid7()
    target_model_id = uuid7()
    packet = {"tiers": {}}
    candidates = [
        {
            "candidate_id": "MDC_SUPPORT",
            "op_family": "claim_update",
            "proposed_text": "Security packet supports renewal review.",
            "target_model_ids": [str(target_model_id)],
            "evidence_model_ids": [str(source_model_id), str(target_model_id)],
            "suggested_edge_kinds": ["supports"],
            "write_preconditions": [
                "Use blocks only when source evidence gates target progress.",
            ],
            "confidence": 0.71,
        }
    ]

    obligations = relation_obligations_from_packet(packet, candidates)

    assert len(obligations) == 1
    assert obligations[0].edge_kind == "supports"
    assert "blocks" not in obligations[0].matched_markers


async def test_relation_frame_obligations_compile_blocked_workstream():
    tid = uuid7()
    blocker_model_id = uuid7()
    work_model_id = uuid7()
    risk_model_id = uuid7()
    resolution_model_id = uuid7()
    obs_id = uuid7()
    packet = {
        "signal_summary": (
            "DPA approval blocks the HubSpot import, the import is an early "
            "warning for Friday launch risk, and security evidence can unblock "
            "the DPA approval."
        ),
        "tiers": {},
    }
    candidates = [
        {
            "candidate_id": "MDC_BLOCKER",
            "op_family": "claim_update",
            "proposed_text": "DPA approval blocks the HubSpot import.",
            "target_model_ids": [str(work_model_id)],
            "evidence_model_ids": [str(blocker_model_id), str(work_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["blocks"],
            "confidence": 0.78,
        },
        {
            "candidate_id": "MDC_RISK",
            "op_family": "claim_update",
            "proposed_text": (
                "The HubSpot import is an early warning for Friday launch risk."
            ),
            "target_model_ids": [str(risk_model_id)],
            "evidence_model_ids": [str(work_model_id), str(risk_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["early_warning_for"],
            "confidence": 0.76,
        },
        {
            "candidate_id": "MDC_RESOLUTION",
            "op_family": "claim_update",
            "proposed_text": (
                "Security evidence contributes to resolution of the DPA approval."
            ),
            "target_model_ids": [str(blocker_model_id)],
            "evidence_model_ids": [
                str(resolution_model_id),
                str(blocker_model_id),
            ],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["contributes_to_resolution"],
            "confidence": 0.74,
        },
    ]

    obligations = relation_obligations_from_packet(packet, candidates)
    frame_obligations = relation_frame_obligations_from_obligations(
        obligations,
        candidates=candidates,
    )
    frame_ops, frame_summary = relation_frame_ops_from_obligations(
        frame_obligations,
        tenant_id=tid,
    )
    claim_ops, claim_summary = relation_claim_ops_from_obligations(
        obligations,
        covered_edges={
            ("blocks", blocker_model_id, work_model_id),
            ("early_warning_for", work_model_id, risk_model_id),
            ("contributes_to_resolution", resolution_model_id, blocker_model_id),
        },
    )

    assert len(frame_obligations) == 1
    assert len(frame_ops) == 1
    assert "mandatory_relation_frame_obligations=perceived:1,emitted:1" in (
        frame_summary or ""
    )
    assert claim_ops == []
    assert "deduped:3" in (claim_summary or "")

    frame = frame_ops[0]
    assert frame.relation_kind == "blocked_workstream"
    assert frame.status == "accepted"
    assert frame.write_policy == "project_edges"
    assert {
        (participant.role, participant.model_id)
        for participant in frame.participants
    } == {
        ("blocker", blocker_model_id),
        ("blocked_work", work_model_id),
        ("downstream_risk", risk_model_id),
        ("possible_resolution", resolution_model_id),
    }


async def test_relation_frame_completion_binds_missing_projectable_roles():
    tid = uuid7()
    blocker_model_id = uuid7()
    work_model_id = uuid7()
    risk_model_id = uuid7()
    resolution_model_id = uuid7()
    obs_id = uuid7()
    packet = {
        "signal_summary": (
            "DPA approval blocks the HubSpot import. Friday launch may slip, "
            "and the security packet can unblock DPA approval."
        ),
        "tiers": {},
    }
    candidates = [
        {
            "candidate_id": "MDC_BLOCKER",
            "op_family": "claim_update",
            "proposed_text": "DPA approval blocks the HubSpot import.",
            "target_model_ids": [str(work_model_id)],
            "evidence_model_ids": [str(blocker_model_id), str(work_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["blocks"],
            "confidence": 0.78,
        },
        {
            "candidate_id": "MDC_RISK_ROLE",
            "op_family": "claim_update",
            "proposed_text": "Friday launch may slip.",
            "target_model_ids": [str(risk_model_id)],
            "evidence_model_ids": [str(risk_model_id)],
            "source_observation_ids": [str(obs_id)],
            "confidence": 0.72,
        },
        {
            "candidate_id": "MDC_RESOLUTION_ROLE",
            "op_family": "claim_update",
            "proposed_text": "The security packet is ready for review.",
            "target_model_ids": [str(resolution_model_id)],
            "evidence_model_ids": [str(resolution_model_id)],
            "source_observation_ids": [str(obs_id)],
            "confidence": 0.74,
        },
    ]
    model_cards = [
        SimpleNamespace(
            id=blocker_model_id,
            natural="DPA approval is missing.",
            proposition={"kind": "belief", "claim_role": "concern"},
            confidence=0.84,
        ),
        SimpleNamespace(
            id=work_model_id,
            natural="HubSpot import depends on DPA approval.",
            proposition={"kind": "belief", "claim_role": "fact"},
            confidence=0.82,
        ),
        SimpleNamespace(
            id=risk_model_id,
            natural="Friday launch may slip.",
            proposition={"kind": "belief", "claim_role": "concern"},
            confidence=0.8,
        ),
        SimpleNamespace(
            id=resolution_model_id,
            natural="Security packet is ready for Cobalt review.",
            proposition={"kind": "belief", "claim_role": "fact"},
            confidence=0.8,
        ),
    ]

    obligations = relation_obligations_from_packet(packet, candidates)
    frame_obligations = relation_frame_obligations_from_obligations(
        obligations,
        candidates=candidates,
        packet=packet,
        model_cards=model_cards,
    )
    frame_ops, _ = relation_frame_ops_from_obligations(
        frame_obligations,
        tenant_id=tid,
    )
    claim_ops, claim_summary = relation_claim_ops_from_obligations(
        obligations,
        covered_edges={
            ("blocks", blocker_model_id, work_model_id),
            ("early_warning_for", work_model_id, risk_model_id),
            ("contributes_to_resolution", resolution_model_id, blocker_model_id),
        },
    )

    assert [obligation.edge_kind for obligation in obligations] == ["blocks"]
    assert len(frame_obligations) == 1
    assert len(frame_ops) == 1
    assert claim_ops == []
    assert "deduped:1" in (claim_summary or "")
    assert {
        (participant.role, participant.model_id)
        for participant in frame_ops[0].participants
    } == {
        ("blocker", blocker_model_id),
        ("blocked_work", work_model_id),
        ("downstream_risk", risk_model_id),
        ("possible_resolution", resolution_model_id),
    }


async def test_relation_frame_completion_rejects_stale_unanchored_endpoints():
    old_blocker_model_id = uuid7()
    old_work_model_id = uuid7()
    current_risk_model_id = uuid7()
    current_resolution_model_id = uuid7()
    obs_id = uuid7()
    packet = {
        "signal_summary": (
            "DeltaFleet implementation is blocked by owner handoff capacity. "
            "Onboarding may slip, and capacity coverage can unblock the work."
        ),
        "tiers": {},
    }
    candidates = [
        {
            "candidate_id": "MDC_STALE_BLOCKS",
            "op_family": "edge_insert",
            "proposed_text": (
                "DeltaFleet implementation is blocked by owner handoff capacity."
            ),
            "target_model_ids": [str(old_work_model_id)],
            "evidence_model_ids": [str(old_blocker_model_id), str(old_work_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["blocks"],
            "confidence": 0.76,
        },
        {
            "candidate_id": "MDC_CURRENT_RISK",
            "op_family": "claim_update",
            "proposed_text": "DeltaFleet onboarding may slip.",
            "target_model_ids": [str(current_risk_model_id)],
            "evidence_model_ids": [str(current_risk_model_id)],
            "source_observation_ids": [str(obs_id)],
            "confidence": 0.72,
        },
        {
            "candidate_id": "MDC_CURRENT_RESOLUTION",
            "op_family": "claim_update",
            "proposed_text": "Capacity coverage is available for implementation.",
            "target_model_ids": [str(current_resolution_model_id)],
            "evidence_model_ids": [str(current_resolution_model_id)],
            "source_observation_ids": [str(obs_id)],
            "confidence": 0.74,
        },
    ]
    model_cards = [
        SimpleNamespace(
            id=old_blocker_model_id,
            natural="Borealis renewal risk remains high.",
            proposition={"kind": "belief", "claim_role": "concern"},
            confidence=0.84,
        ),
        SimpleNamespace(
            id=old_work_model_id,
            natural="Borealis executive confidence recovery is active.",
            proposition={"kind": "belief", "claim_role": "situation"},
            confidence=0.82,
        ),
        SimpleNamespace(
            id=current_risk_model_id,
            natural="DeltaFleet onboarding may slip.",
            proposition={"kind": "belief", "claim_role": "concern"},
            confidence=0.8,
        ),
        SimpleNamespace(
            id=current_resolution_model_id,
            natural="Capacity coverage can unblock DeltaFleet implementation.",
            proposition={"kind": "belief", "claim_role": "fact"},
            confidence=0.8,
        ),
    ]

    obligations = relation_obligations_from_packet(packet, candidates)
    frame_obligations = relation_frame_obligations_from_obligations(
        obligations,
        candidates=candidates,
        packet=packet,
        model_cards=model_cards,
    )

    assert [obligation.edge_kind for obligation in obligations] == ["blocks"]
    assert frame_obligations == ()


async def test_grounding_obligation_preserves_current_batch_anchors():
    tid = uuid7()
    obs_id = uuid7()
    stale_model_id = uuid7()
    old_context_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_natural_text=(
            "DeltaFleet implementation is blocked by owner handoff capacity."
        ),
    )
    packet = {
        "signal_summary": (
            "DeltaFleet implementation is blocked by owner handoff capacity. "
            "Onboarding may slip because throughput coverage is thin."
        ),
        "tiers": {
            "decisive_evidence": [
                {
                    "evidence_id": "ev_deltafleet",
                    "summary": (
                        "DeltaFleet handoff capacity threatens onboarding "
                        "throughput coverage."
                    ),
                }
            ]
        },
    }
    candidates = [
        {
            "candidate_id": "MDC_STALE_UPDATE",
            "op_family": "claim_update",
            "proposed_text": "Capacity pressure reinforces the renewal risk model.",
            "target_model_ids": [str(stale_model_id)],
            "evidence_model_ids": [str(stale_model_id), str(old_context_model_id)],
            "source_observation_ids": [str(obs_id)],
            "supporting_evidence_ids": ["ev_deltafleet"],
            "confidence": 0.72,
        }
    ]
    model_cards = [
        SimpleNamespace(
            id=stale_model_id,
            natural="Borealis renewal risk remains high.",
            proposition={"kind": "belief", "claim_role": "concern"},
            confidence=0.84,
        ),
        SimpleNamespace(
            id=old_context_model_id,
            natural="Borealis executive confidence recovery is active.",
            proposition={"kind": "belief", "claim_role": "situation"},
            confidence=0.82,
        ),
    ]

    obligations = grounding_obligations_from_packet(
        packet,
        candidates,
        model_cards=model_cards,
    )
    ops, summary = grounding_claim_ops_from_obligations(
        obligations,
        trigger=trigger,
        existing_ops=[
            ClaimOp(
                op="update",
                model_id=stale_model_id,
                changes={"confidence": 0.86},
            )
        ],
    )

    assert len(obligations) == 1
    obligation = obligations[0]
    assert obligation.entity_tokens == ("deltafleet",)
    assert {"implementation", "capacity", "handoff", "onboarding"} <= set(
        obligation.grounding_tokens
    )
    assert len(ops) == 1
    assert "emitted:1" in (summary or "")
    op = ops[0]
    assert op.op == "insert"
    assert op.entry is not None
    proposition = op.entry["proposition"]
    assert proposition["claim_role"] == "situation"
    assert proposition["compiled_grounding_obligation"] is True
    assert proposition["affected_customers"] == ["deltafleet"]
    assert "DeltaFleet" in op.entry["natural"]
    assert "handoff capacity" in op.entry["natural"]


async def test_grounding_obligation_dedupes_existing_situation_insert():
    tid = uuid7()
    obs_id = uuid7()
    member_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_natural_text="DeltaFleet onboarding is blocked by capacity handoff.",
    )
    packet = {
        "signal_summary": (
            "DeltaFleet onboarding is blocked by capacity handoff and "
            "implementation throughput coverage is thin."
        ),
        "tiers": {},
    }
    candidates = [
        {
            "candidate_id": "MDC_SITUATION",
            "op_family": "claim_update",
            "proposed_text": (
                "DeltaFleet onboarding is blocked by capacity handoff pressure."
            ),
            "target_model_ids": [str(member_model_id)],
            "evidence_model_ids": [str(member_model_id)],
            "source_observation_ids": [str(obs_id)],
            "confidence": 0.72,
        }
    ]
    existing = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(tid),
            "born_from_event_id": str(uuid7()),
            "natural": (
                "DeltaFleet onboarding is blocked by capacity and handoff pressure."
            ),
            "proposition": {
                "kind": "belief",
                "claim_role": "situation",
                "member_model_ids": [str(member_model_id)],
                "grounding_tokens": ["deltafleet", "capacity", "handoff"],
            },
            "confidence": 0.72,
            "confidence_at_assertion": 0.72,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "falsifier": None,
        },
    )

    obligations = grounding_obligations_from_packet(packet, candidates)
    ops, summary = grounding_claim_ops_from_obligations(
        obligations,
        trigger=trigger,
        existing_ops=[existing],
    )

    assert len(obligations) == 1
    assert ops == []
    assert "deduped:1" in (summary or "")


async def test_relation_lifecycle_kernel_canonicalizes_legacy_edge_ops():
    tid = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(kind="T3", tenant_id=tid, observation_id=obs_id)
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tid,
        edge_ops=[
            EdgeOp(
                op="add",
                source_model_id=source_id,
                target_model_id=target_id,
                edge_kind="blocks",
                confidence=0.82,
                evidence_event_ids=[obs_id],
                evidence_model_ids=[source_id, target_id],
                explanation="The approval blocks the import.",
                review_status="accepted",
                detected_by="think_edge_op",
            ),
            EdgeOp(
                op="retire",
                source_model_id=target_id,
                target_model_id=source_id,
                edge_kind="supports",
                confidence=0.7,
                reason="Future evidence retired the support relation.",
            ),
        ],
        reasoning_trace="raw graph writes from broad Think",
    )

    canonical = apply_relation_lifecycle_kernel(
        diff,
        trigger=trigger,
        bundle=ContextBundle(),
    )

    assert canonical.edge_ops == []
    assert len(canonical.relation_claim_ops) == 2
    add, retire = canonical.relation_claim_ops
    assert add.edge_kind == "blocks"
    assert add.write_policy == "accepted_edge"
    assert add.status == "accepted"
    assert add.metadata["relation_claim_origin"] == (
        "relation_lifecycle_kernel_legacy_edge_op"
    )
    assert retire.edge_kind == "supports"
    assert retire.write_policy == "no_edge"
    assert retire.status == "retired"
    assert "legacy_edge_ops:2,canonicalized:2" in (
        canonical.reasoning_trace or ""
    )


async def test_llm_reason_compiled_batch_memory_adds_grounding_when_only_update(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    stale_model_id = uuid7()
    context_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text=(
            "DeltaFleet implementation is blocked by owner handoff capacity."
        ),
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=stale_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="Generic renewal risk remains high.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=context_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="Generic executive confidence recovery is active.",
                proposition={"kind": "belief", "claim_role": "situation"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "DeltaFleet implementation is blocked by owner handoff "
                    "capacity. Onboarding may slip because throughput coverage "
                    "is thin."
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_STALE_UPDATE",
                        "op_family": "claim_update",
                        "proposed_text": (
                            "Capacity pressure reinforces the renewal risk model."
                        ),
                        "target_model_ids": [str(stale_model_id)],
                        "evidence_model_ids": [
                            str(stale_model_id),
                            str(context_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev_deltafleet"],
                        "confidence": 0.72,
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev_deltafleet",
                            "summary": (
                                "DeltaFleet handoff capacity threatens "
                                "onboarding throughput coverage."
                            ),
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": "MDC_STALE_UPDATE",
                            "decision": "accept",
                            "operation": "claim_update",
                            "confidence": 0.72,
                            "reason": "The batch reinforces the existing risk model.",
                        }
                    ],
                    "reasoning_trace": "Accepted stale update only.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "<mandatory_grounding_obligations>" in provider.calls[0]["user"]
    assert len(diff.claim_ops) == 2
    assert diff.claim_ops[0].op == "update"
    grounding = diff.claim_ops[1]
    assert grounding.op == "insert"
    assert grounding.entry is not None
    assert grounding.entry["proposition"]["claim_role"] == "situation"
    assert grounding.entry["proposition"]["compiled_grounding_obligation"] is True
    assert "DeltaFleet" in grounding.entry["natural"]
    assert "mandatory_grounding_obligations=perceived:1,emitted:1" in (
        diff.reasoning_trace or ""
    )


async def test_llm_reason_compiled_batch_memory_falls_back_for_open_writer_surfaces(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Future validation says the forecast should resolve today.",
    )
    bundle = ContextBundle(
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "future_validation evidence should update a prediction lifecycle"
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_PREDICTION",
                        "op_family": "claim_update",
                        "proposed_text": "The forecast prediction resolved today.",
                        "source_observation_ids": [str(obs_id)],
                        "confidence": 0.72,
                    },
                ],
            }
        },
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "<compiled_batch_memory_task>" not in provider.calls[0]["user"]
    assert "claim_ops" in provider.calls[0]["schema_hint"]
    assert diff.reasoning_trace == "test scripted diff"


async def test_llm_reason_broad_path_adds_mandatory_relation_obligation(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    blocker_model_id = uuid7()
    launch_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="DPA approval blocks the HubSpot import.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=blocker_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="DPA approval is missing.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=launch_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="HubSpot import depends on DPA approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": "HubSpot import is blocked by DPA approval.",
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_DPA_BLOCKER",
                        "op_family": "claim_update",
                        "proposed_text": "DPA approval blocks the HubSpot import.",
                        "target_model_ids": [str(launch_model_id)],
                        "evidence_model_ids": [
                            str(blocker_model_id),
                            str(launch_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "suggested_edge_kinds": ["blocks", "supports"],
                        "confidence": 0.78,
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "model",
                            "source_ref": f"model:{blocker_model_id}",
                            "summary": "DPA approval is the prerequisite for import.",
                            "supports_hypotheses": ["H1"],
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "relation_claim_ops": [],
                    "edge_ops": [],
                    "ontology_gap_ops": [],
                    "act_ops": [],
                    "resource_ops": [],
                    "new_predictions": [],
                    "reasoning_trace": "LLM emitted no relation.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "<compiled_batch_memory_task>" not in provider.calls[0]["user"]
    assert len(diff.relation_claim_ops) == 1
    relation = diff.relation_claim_ops[0]
    assert relation.metadata["relation_claim_origin"] == "mandatory_relation_obligation"
    assert relation.edge_kind == "blocks"
    assert relation.source_model_id == blocker_model_id
    assert relation.target_model_id == launch_model_id
    assert relation.write_policy == "accepted_edge"


async def test_llm_reason_broad_path_adds_mandatory_relation_frame(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    blocker_model_id = uuid7()
    work_model_id = uuid7()
    risk_model_id = uuid7()
    resolution_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text=(
            "DPA approval blocks the HubSpot import, which threatens Friday launch."
        ),
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=blocker_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="DPA approval is missing.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=work_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="HubSpot import depends on DPA approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
            SimpleNamespace(
                id=risk_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="Friday launch may slip.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=resolution_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="Security packet is ready for review.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "DPA approval blocks HubSpot import; the import is an early "
                    "warning for Friday launch risk, and the security packet can "
                    "unblock DPA approval."
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_DPA_BLOCKER",
                        "op_family": "claim_update",
                        "proposed_text": "DPA approval blocks the HubSpot import.",
                        "target_model_ids": [str(work_model_id)],
                        "evidence_model_ids": [
                            str(blocker_model_id),
                            str(work_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev_block"],
                        "suggested_edge_kinds": ["blocks"],
                        "confidence": 0.78,
                    },
                    {
                        "candidate_id": "MDC_LAUNCH_RISK",
                        "op_family": "claim_update",
                        "proposed_text": (
                            "HubSpot import is an early warning for Friday launch risk."
                        ),
                        "target_model_ids": [str(risk_model_id)],
                        "evidence_model_ids": [
                            str(work_model_id),
                            str(risk_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev_risk"],
                        "suggested_edge_kinds": ["early_warning_for"],
                        "confidence": 0.76,
                    },
                    {
                        "candidate_id": "MDC_SECURITY_PACKET",
                        "op_family": "claim_update",
                        "proposed_text": "Security packet is ready for review.",
                        "target_model_ids": [str(resolution_model_id)],
                        "evidence_model_ids": [str(resolution_model_id)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev_resolution"],
                        "confidence": 0.74,
                    },
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev_block",
                            "summary": "DPA approval is the prerequisite for import.",
                        },
                        {
                            "evidence_id": "ev_risk",
                            "summary": (
                                "HubSpot import is an early warning for Friday launch risk."
                            ),
                        },
                        {
                            "evidence_id": "ev_resolution",
                            "summary": "Security packet can unblock DPA approval.",
                        },
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "relation_claim_ops": [],
                    "relation_frame_ops": [],
                    "edge_ops": [],
                    "ontology_gap_ops": [],
                    "act_ops": [],
                    "resource_ops": [],
                    "new_predictions": [],
                    "reasoning_trace": "LLM emitted no relation frame.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "<compiled_batch_memory_task>" not in provider.calls[0]["user"]
    assert diff.relation_claim_ops == []
    assert len(diff.relation_frame_ops) == 1
    frame = diff.relation_frame_ops[0]
    assert frame.relation_kind == "blocked_workstream"
    assert frame.write_policy == "project_edges"
    assert frame.status == "accepted"
    assert {
        (participant.role, participant.model_id)
        for participant in frame.participants
    } == {
        ("blocker", blocker_model_id),
        ("blocked_work", work_model_id),
        ("downstream_risk", risk_model_id),
        ("possible_resolution", resolution_model_id),
    }
    assert "mandatory_relation_frame_obligations=perceived:1,emitted:1" in (
        diff.reasoning_trace or ""
    )


async def test_relation_lifecycle_kernel_skips_packet_obligations_for_noise_noop():
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    blocker_model_id = uuid7()
    work_model_id = uuid7()
    risk_model_id = uuid7()
    resolution_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="General operational chatter and lunch logistics.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=blocker_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="DPA approval is missing.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=work_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="HubSpot import depends on DPA approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
            SimpleNamespace(
                id=risk_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="Friday launch may slip.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=resolution_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="Security packet is ready for review.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "General operational chatter: lunch logistics, duplicated "
                    "dashboard links, and a non-actionable reminder."
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_STALE_BLOCKER",
                        "op_family": "claim_update",
                        "proposed_text": "DPA approval blocks the HubSpot import.",
                        "target_model_ids": [str(work_model_id)],
                        "evidence_model_ids": [
                            str(blocker_model_id),
                            str(work_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "suggested_edge_kinds": ["blocks"],
                        "confidence": 0.78,
                    },
                    {
                        "candidate_id": "MDC_STALE_RISK",
                        "op_family": "claim_update",
                        "proposed_text": (
                            "HubSpot import is an early warning for Friday launch risk."
                        ),
                        "target_model_ids": [str(risk_model_id)],
                        "evidence_model_ids": [
                            str(work_model_id),
                            str(risk_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "suggested_edge_kinds": ["early_warning_for"],
                        "confidence": 0.76,
                    },
                    {
                        "candidate_id": "MDC_STALE_RESOLUTION",
                        "op_family": "claim_update",
                        "proposed_text": "Security packet can unblock DPA approval.",
                        "target_model_ids": [str(resolution_model_id)],
                        "evidence_model_ids": [
                            str(resolution_model_id),
                            str(blocker_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "suggested_edge_kinds": ["contributes_to_resolution"],
                        "confidence": 0.74,
                    },
                ],
            }
        },
    )
    raw = RawDiff(
        trigger_ref=trig_id,
        tenant_id=tid,
        reasoning_trace=(
            "Empty diff: the batch is described only as general operational "
            "chatter/lunch logistics/duplicates, so it does not provide a "
            "durable evidenced operational claim."
        ),
    )

    diff = apply_relation_lifecycle_kernel(raw, trigger=trigger, bundle=bundle)

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert diff.relation_frame_ops == []
    assert diff.edge_ops == []
    assert "packet_obligations_skipped:explicit_noop" in (diff.reasoning_trace or "")
    assert "mandatory_relation_obligations=" not in (diff.reasoning_trace or "")
    assert "mandatory_relation_frame_obligations=" not in (diff.reasoning_trace or "")


async def test_compiled_batch_skips_preapplied_mandatory_relations_for_noise_noop():
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    blocker_model_id = uuid7()
    work_model_id = uuid7()
    risk_model_id = uuid7()
    resolution_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Background chatter, lunch logistics, duplicate links.",
    )
    candidates = [
        {
            "candidate_id": "MDC_STALE_BLOCKER",
            "op_family": "claim_update",
            "proposed_text": "DPA approval blocks the HubSpot import.",
            "target_model_ids": [str(work_model_id)],
            "evidence_model_ids": [str(blocker_model_id), str(work_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["blocks"],
            "confidence": 0.78,
        },
        {
            "candidate_id": "MDC_STALE_RISK",
            "op_family": "claim_update",
            "proposed_text": "HubSpot import is an early warning for launch risk.",
            "target_model_ids": [str(risk_model_id)],
            "evidence_model_ids": [str(work_model_id), str(risk_model_id)],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["early_warning_for"],
            "confidence": 0.76,
        },
        {
            "candidate_id": "MDC_STALE_RESOLUTION",
            "op_family": "claim_update",
            "proposed_text": "Security packet can unblock DPA approval.",
            "target_model_ids": [str(resolution_model_id)],
            "evidence_model_ids": [
                str(resolution_model_id),
                str(blocker_model_id),
            ],
            "source_observation_ids": [str(obs_id)],
            "suggested_edge_kinds": ["contributes_to_resolution"],
            "confidence": 0.74,
        },
    ]
    packet = {
        "signal_summary": (
            "Background noise wave: general operational chatter, lunch "
            "logistics, duplicate dashboard links, and no durable diff."
        ),
        "memory_decision_candidates": candidates,
    }
    obligations = relation_obligations_from_packet(packet, candidates)
    frame_obligations = relation_frame_obligations_from_obligations(
        obligations,
        candidates=candidates,
        packet=packet,
    )
    request = CompiledBatchMemoryDecisionRequest(
        system="system",
        user="user",
        candidates=tuple(candidates),
        relation_obligations=obligations,
        relation_frame_obligations=frame_obligations,
        packet_obligation_gate=packet,
    )
    decisions = BatchMemoryDecisionSet(
        decisions=[
            BatchMemoryCandidateDecision(
                candidate_id=str(candidate["candidate_id"]),
                decision="reject",
                operation="no_op",
                confidence=0.62,
                reason="No durable write; background chatter only.",
            )
            for candidate in candidates
        ],
        reasoning_trace=(
            "No durable diff emitted. The batch is general operational "
            "chatter and lunch logistics with duplicate dashboard links."
        ),
    )

    diff = request.to_raw_diff(decisions, trigger=trigger, trigger_ref=trig_id)

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert diff.relation_frame_ops == []
    assert diff.edge_ops == []
    assert "packet_obligations_skipped:explicit_noop" in (diff.reasoning_trace or "")
    assert "mandatory_relation_obligations=" not in (diff.reasoning_trace or "")
    assert "mandatory_relation_frame_obligations=" not in (diff.reasoning_trace or "")


async def test_compiled_batch_memory_to_raw_diff_emits_memory_lifecycle_ops():
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    evidence_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="Support confirmed that the escalation is resolved.",
    )
    candidate = {
        "candidate_id": "MDC_LIFECYCLE",
        "op_family": "memory_lifecycle",
        "proposed_text": "Support confirmed that the escalation is resolved.",
        "target_model_ids": [str(model_id)],
        "evidence_model_ids": [str(model_id), str(evidence_model_id)],
        "source_observation_ids": [str(obs_id)],
        "suggested_edge_kinds": ["supports"],
        "confidence": 0.74,
    }
    request = CompiledBatchMemoryDecisionRequest(
        system="system",
        user="user",
        candidates=(candidate,),
    )
    decisions = BatchMemoryDecisionSet(
        decisions=[
            BatchMemoryCandidateDecision(
                candidate_id="MDC_LIFECYCLE",
                decision="accept",
                operation="memory_lifecycle",
                confidence=0.74,
                lifecycle_action="confirm",
                model_id=model_id,
                resolution_outcome=True,
                reason="The latest evidence directly confirms this memory.",
            )
        ],
        reasoning_trace="Accepted lifecycle reconciliation.",
    )

    diff = request.to_raw_diff(decisions, trigger=trigger, trigger_ref=trig_id)

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert len(diff.memory_lifecycle_ops) == 1
    op = diff.memory_lifecycle_ops[0]
    assert op.model_id == model_id
    assert op.action == "confirm"
    assert op.evidence_event_ids == [obs_id]
    assert op.evidence_model_ids == [evidence_model_id]
    assert op.resolution_outcome is True
    assert op.metadata["source"] == "compiled_batch_memory_candidate"
    assert op.metadata["candidate_id"] == "MDC_LIFECYCLE"
    assert "accepted memory_lifecycle" in (diff.reasoning_trace or "")


async def test_llm_reason_broad_path_skips_mandatory_relations_for_noise_noop(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="General operational chatter and duplicated links.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=source_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="DPA approval is missing.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=target_model_id,
                proposition_kind="belief",
                confidence=0.82,
                activation=0.7,
                status="active",
                natural="HubSpot import depends on DPA approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "General operational chatter: lunch logistics, duplicated "
                    "dashboard links, and a non-actionable reminder."
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_STALE_RELATION",
                        "op_family": "claim_update",
                        "proposed_text": "DPA approval blocks the HubSpot import.",
                        "target_model_ids": [str(target_model_id)],
                        "evidence_model_ids": [
                            str(source_model_id),
                            str(target_model_id),
                        ],
                        "source_observation_ids": [str(obs_id)],
                        "suggested_edge_kinds": ["blocks"],
                        "confidence": 0.78,
                    }
                ],
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "relation_claim_ops": [],
                    "relation_frame_ops": [],
                    "edge_ops": [],
                    "ontology_gap_ops": [],
                    "act_ops": [],
                    "resource_ops": [],
                    "new_predictions": [],
                    "reasoning_trace": (
                        "Empty diff: general operational chatter/lunch logistics "
                        "does not provide a durable evidenced operational claim."
                    ),
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert diff.relation_frame_ops == []
    assert diff.edge_ops == []
    assert "packet_obligations_skipped:explicit_noop" in (diff.reasoning_trace or "")


async def test_llm_reason_fixture_noise_phrases_do_not_bypass_llm(monkeypatch):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={
            "trigger_id": str(trig_id),
            "source_channels": ["slack:storyline-noise"],
            "batch_signal_fragments": [
                {
                    "text": (
                        "General operational chatter: lunch logistics, "
                        "duplicated dashboard links, and a non-actionable "
                        "reminder. This should not dominate memory."
                    ),
                }
            ],
        },
        seed_natural_text=(
            "Evidence window containing 1 source signal:\n"
            "- signal: General operational chatter: lunch logistics, "
            "duplicated dashboard links, and a non-actionable reminder. "
            "This should not dominate memory."
        ),
    )
    bundle = ContextBundle(
        notes={
            "inquiry_context_packet": {
                "signal_summary": trigger.seed_natural_text,
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_STALE_RELATION",
                        "op_family": "claim_update",
                        "proposed_text": "Stale selected context should not revive.",
                        "source_observation_ids": [str(obs_id)],
                        "suggested_edge_kinds": ["blocks"],
                        "confidence": 0.78,
                    }
                ],
            }
        }
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [
                        {
                            "op": "insert",
                            "entry": {
                                "natural": "This response should not be used."
                            },
                        }
                    ],
                }
            )
        ]
    )

    diff, latency_ms = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert latency_ms >= 0
    assert len(provider.calls) == 1
    assert len(diff.claim_ops) == 1
    assert "discard_as_noise" not in (diff.reasoning_trace or "")


async def test_llm_reason_noise_word_with_actionable_signal_still_uses_llm():
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={
            "trigger_id": str(trig_id),
            "source_channels": ["slack:storyline-noise"],
            "batch_signal_fragments": [
                {
                    "text": (
                        "The latest escalation is framed as support noise, "
                        "but the blocker named by the buyer is evidence readiness."
                    ),
                }
            ],
        },
        seed_natural_text=(
            "The latest escalation is framed as support noise, but the blocker "
            "named by the buyer is evidence readiness."
        ),
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "reasoning_trace": "No durable write in scripted response.",
                }
            )
        ]
    )

    await llm_reason(trigger, ContextBundle(), provider, max_tokens=2048)

    assert len(provider.calls) == 1


async def test_question_policy_feedback_upserts_policy_stats_without_optimizer():
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    session_id = uuid7()
    executed = []
    emitted = []

    class FakeSavepoint:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def transaction(self):
            return FakeSavepoint()

        async def fetchval(self, query, *args):
            del args
            if "sage_reader_decision_attributions" in query:
                return "sage_reader_decision_attributions"
            if "sage_question_policy_stats" in query:
                return "sage_question_policy_stats"
            return None

        async def executemany(self, query, args):
            executed.append(("executemany", query, args))

        async def execute(self, query, *args):
            executed.append(("execute", query, args))

    async def fake_emit_event(event_type, payload, *, ctx):
        emitted.append((event_type, payload, ctx))

    ops_summary = {
        "claim_ops": [
            {
                "model_id": str(model_id),
                "domain_tags": [
                    "question_policy",
                    "learning",
                    "capability_probe",
                ],
            }
        ]
    }
    diff = ValidatedDiff(trigger_ref=trig_id, tenant_id=tid)

    await _emit_question_policy_valid_diff_feedback(
        diff,
        conn=FakeConn(),
        ctx=SimpleNamespace(inquiry_session_id=session_id),
        ops_summary=ops_summary,
        emit_event=fake_emit_event,
        signal_type="T1",
        question_primitive="DEPENDENCY",
        entities=["customer:enterprise-control"],
    )

    stat_execs = [
        call for call in executed
        if call[0] == "execute" and "sage_question_policy_stats" in call[1]
    ]
    assert len(stat_execs) == 1
    assert stat_execs[0][2][1:] == (tid, "T1", "DEPENDENCY")
    assert ops_summary["question_policy_updates"] == 1
    assert emitted[0][0] == "reader_decision_used_in_valid_diff"
    assert emitted[0][1]["model_id"] == str(model_id)


async def test_question_policy_stats_do_not_require_trace_attribution_table():
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    session_id = uuid7()
    executed = []
    emitted = []

    class FakeSavepoint:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def transaction(self):
            return FakeSavepoint()

        async def fetchval(self, query, *args):
            del args
            if "sage_question_policy_stats" in query:
                return "sage_question_policy_stats"
            return None

        async def executemany(self, query, args):
            executed.append(("executemany", query, args))

        async def execute(self, query, *args):
            executed.append(("execute", query, args))

    async def fake_emit_event(event_type, payload, *, ctx):
        emitted.append((event_type, payload, ctx))

    ops_summary = {
        "claim_ops": [
            {
                "model_id": str(model_id),
                "domain_tags": [
                    "question_policy",
                    "learning",
                    "capability_probe",
                ],
            }
        ]
    }
    diff = ValidatedDiff(trigger_ref=trig_id, tenant_id=tid)

    await _emit_question_policy_valid_diff_feedback(
        diff,
        conn=FakeConn(),
        ctx=SimpleNamespace(inquiry_session_id=session_id),
        ops_summary=ops_summary,
        emit_event=fake_emit_event,
        signal_type="T1",
        question_primitive="DEPENDENCY",
        entities=["customer:enterprise-control"],
    )

    stat_execs = [
        call for call in executed
        if call[0] == "execute" and "sage_question_policy_stats" in call[1]
    ]
    attribution_execs = [
        call for call in executed
        if call[0] == "executemany"
        and "sage_reader_decision_attributions" in call[1]
    ]
    assert len(stat_execs) == 1
    assert attribution_execs == []
    assert emitted == []
    assert ops_summary["question_policy_updates"] == 1


async def test_question_policy_stats_helper_records_without_trace_context():
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    executed = []

    class FakeSavepoint:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def transaction(self):
            return FakeSavepoint()

        async def fetchval(self, query, *args):
            del query, args
            return "sage_question_policy_stats"

        async def execute(self, query, *args):
            executed.append((query, args))

    ops_summary = {
        "claim_ops": [
            {
                "model_id": str(model_id),
                "domain_tags": ["question_policy", "lifecycle_obligation"],
            }
        ]
    }
    diff = ValidatedDiff(trigger_ref=trig_id, tenant_id=tid)

    recorded = await _upsert_question_policy_valid_diff_stats(
        diff,
        conn=FakeConn(),
        ops_summary=ops_summary,
        signal_type="unknown",
        question_primitive=None,
    )

    assert recorded is True
    assert len(executed) == 1
    assert executed[0][1][1:] == (tid, "unknown", "DEPENDENCY")
    assert ops_summary["question_policy_updates"] == 1


async def test_question_policy_stats_survive_disabled_trace_emission(monkeypatch):
    tid = uuid7()
    trig_id = uuid7()
    model_id = uuid7()
    executed = []

    class FakeSavepoint:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def transaction(self):
            return FakeSavepoint()

        async def fetchval(self, query, *args):
            del query, args
            return "sage_question_policy_stats"

        async def execute(self, query, *args):
            executed.append((query, args))

    from services.reasoning.sage.inquiry_traces import emitter as trace_emitter

    monkeypatch.setattr(trace_emitter, "emission_enabled", lambda: False)
    monkeypatch.setattr(
        trace_emitter,
        "current_trace_context",
        lambda: SimpleNamespace(
            metadata={
                "signal_type": "T1",
                "question_primitives": ["DEPENDENCY"],
                "entities": ["customer:enterprise-control"],
            }
        ),
    )

    async def fail_emit_event(*_args, **_kwargs):
        raise AssertionError("trace emission is disabled")

    monkeypatch.setattr(trace_emitter, "emit_event", fail_emit_event)
    ops_summary = {
        "claim_ops": [
            {
                "model_id": str(model_id),
                "domain_tags": ["question_policy", "capability_probe"],
            }
        ]
    }

    await _emit_valid_diff_outcome_events(
        ValidatedDiff(trigger_ref=trig_id, tenant_id=tid),
        applied_model_ids=[],
        conn=FakeConn(),
        ops_summary=ops_summary,
    )

    assert len(executed) == 1
    assert executed[0][1][1:] == (tid, "T1", "DEPENDENCY")
    assert ops_summary["question_policy_updates"] == 1


async def test_llm_reason_broad_path_adds_mandatory_grounding_obligation(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    context_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text=(
            "FoundryWorks connector reliability has repeat freshness incidents."
        ),
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=context_model_id,
                proposition_kind="belief",
                confidence=0.8,
                activation=0.7,
                status="active",
                natural="Customer renewal risk needs review.",
                proposition={"kind": "belief", "claim_role": "concern"},
            )
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": (
                    "FoundryWorks connector reliability has repeat freshness "
                    "incidents and creates churn risk."
                ),
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_FOUNDRYWORKS",
                        "op_family": "claim_update",
                        "proposed_text": (
                            "FoundryWorks connector reliability has repeat "
                            "freshness incidents."
                        ),
                        "target_model_ids": [str(context_model_id)],
                        "evidence_model_ids": [str(context_model_id)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev_foundryworks"],
                        "confidence": 0.7,
                    }
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev_foundryworks",
                            "summary": (
                                "FoundryWorks connector reliability shows "
                                "repeat data freshness incidents."
                            ),
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "relation_claim_ops": [],
                    "relation_frame_ops": [],
                    "edge_ops": [],
                    "ontology_gap_ops": [],
                    "act_ops": [],
                    "resource_ops": [],
                    "new_predictions": [],
                    "reasoning_trace": "LLM emitted no durable model.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "<compiled_batch_memory_task>" not in provider.calls[0]["user"]
    assert len(diff.claim_ops) == 1
    grounding = diff.claim_ops[0]
    assert grounding.op == "insert"
    assert grounding.entry is not None
    assert grounding.entry["proposition"]["claim_role"] == "situation"
    assert grounding.entry["proposition"]["compiled_grounding_obligation"] is True
    assert "FoundryWorks" in grounding.entry["natural"]
    assert "connector reliability" in grounding.entry["natural"]
    assert "mandatory_grounding_obligations=perceived:1,emitted:1" in (
        diff.reasoning_trace or ""
    )


async def test_llm_reason_broad_path_canonicalizes_raw_edge_ops(monkeypatch):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text="DPA approval blocks the HubSpot import.",
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=source_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="DPA approval is missing.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=target_model_id,
                proposition_kind="belief",
                confidence=0.84,
                activation=0.7,
                status="active",
                natural="HubSpot import is waiting on DPA approval.",
                proposition={"kind": "belief", "claim_role": "fact"},
            ),
        ]
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "trigger_ref": str(trig_id),
                    "tenant_id": str(tid),
                    "claim_ops": [],
                    "relation_claim_ops": [],
                    "relation_frame_ops": [],
                    "edge_ops": [
                        {
                            "op": "add",
                            "source_model_id": str(source_model_id),
                            "target_model_id": str(target_model_id),
                            "edge_kind": "blocks",
                            "confidence": 0.82,
                            "evidence_event_ids": [str(obs_id)],
                            "evidence_model_ids": [
                                str(source_model_id),
                                str(target_model_id),
                            ],
                            "explanation": "DPA approval gates the import.",
                            "review_status": "accepted",
                        }
                    ],
                    "ontology_gap_ops": [],
                    "act_ops": [],
                    "resource_ops": [],
                    "new_predictions": [],
                    "reasoning_trace": "LLM emitted a raw edge.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert diff.edge_ops == []
    assert len(diff.relation_claim_ops) == 1
    relation = diff.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert relation.source_model_id == source_model_id
    assert relation.target_model_id == target_model_id
    assert relation.write_policy == "accepted_edge"
    assert "relation_lifecycle_kernel=legacy_edge_ops:1,canonicalized:1" in (
        diff.reasoning_trace or ""
    )


async def test_llm_reason_compiled_batch_memory_supports_updates_situations_and_default_edges(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_BATCH_MEMORY_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    obs_id = uuid7()
    model_a = uuid7()
    model_b = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_signature={"trigger_id": str(trig_id)},
        seed_natural_text=(
            "DeltaFleet onboarding is blocked by owner handoff and capacity."
        ),
        seed_entity_ids=[{"type": "customer_resource", "id": str(uuid7())}],
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=model_a,
                proposition_kind="belief",
                confidence=0.72,
                activation=0.9,
                status="active",
                natural="DeltaFleet onboarding capacity is already tight.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
            SimpleNamespace(
                id=model_b,
                proposition_kind="belief",
                confidence=0.7,
                activation=0.8,
                status="active",
                natural="DeltaFleet owner handoff has slipped.",
                proposition={"kind": "belief", "claim_role": "concern"},
            ),
        ],
        notes={
            "inquiry_context_packet": {
                "signal_summary": "DeltaFleet has capacity and owner-handoff pressure.",
                "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
                "memory_decision_candidates": [
                    {
                        "candidate_id": "MDC_UPDATE",
                        "op_family": "claim_update",
                        "proposed_text": "Capacity pressure is reinforced.",
                        "target_model_ids": [str(model_a)],
                        "evidence_model_ids": [str(model_a), str(model_b)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "confidence": 0.66,
                    },
                    {
                        "candidate_id": "MDC_SIT",
                        "op_family": "claim_update",
                        "proposed_text": (
                            "DeltaFleet onboarding is blocked by capacity and "
                            "handoff pressure."
                        ),
                        "target_model_ids": [str(model_a), str(model_b)],
                        "evidence_model_ids": [str(model_a), str(model_b)],
                        "source_observation_ids": [str(obs_id)],
                        "supporting_evidence_ids": ["ev1"],
                        "confidence": 0.68,
                    },
                ],
                "tiers": {
                    "decisive_evidence": [
                        {
                            "evidence_id": "ev1",
                            "source_type": "model",
                            "source_ref": f"model:{model_a}",
                            "summary": "Capacity and handoff pressure jointly block onboarding.",
                            "supports_hypotheses": ["H1"],
                        }
                    ]
                },
            }
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": "MDC_UPDATE",
                            "decision": "accept",
                            "operation": "claim_update",
                            "confidence": 0.66,
                            "reason": "The new batch reinforces the existing capacity model.",
                        },
                        {
                            "candidate_id": "MDC_SIT",
                            "decision": "accept",
                            "operation": "situation_and_edge",
                            "confidence": 0.68,
                            "claim_role": "situation",
                            "claim_text": (
                                "DeltaFleet onboarding is blocked by capacity and "
                                "handoff pressure."
                            ),
                            "situation_member_model_ids": [str(model_a), str(model_b)],
                            "reason": "The two selected models are symptoms of one blocker.",
                        },
                    ],
                    "reasoning_trace": "Accepted update and situation.",
                }
            )
        ]
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert "claim_update" in provider.calls[0]["user"]
    assert "memory_lifecycle" in provider.calls[0]["user"]
    assert "situation_and_edge" in provider.calls[0]["schema_hint"]
    assert provider.calls[0]["max_tokens"] == 1200
    assert len(diff.claim_ops) == 2
    update = diff.claim_ops[0]
    situation = diff.claim_ops[1]
    assert update.op == "update"
    assert update.model_id == model_a
    assert update.changes["supporting_event_ids"] == [obs_id]
    assert "supporting_model_ids" not in update.changes
    assert situation.op == "insert"
    assert situation.entry["proposition"]["claim_role"] == "situation"
    assert situation.entry["proposition"]["member_model_ids"] == [
        str(model_a),
        str(model_b),
    ]
    assert len(diff.relation_claim_ops) == 2
    assert diff.edge_ops == []
    relation = diff.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert str(relation.source_model_id) == situation.entry["born_from_event_id"]
    assert relation.target_model_id == model_a
    mandatory = diff.relation_claim_ops[1]
    assert mandatory.metadata["relation_claim_origin"] == "mandatory_relation_obligation"
    assert mandatory.edge_kind == "blocks"
    assert mandatory.source_model_id == model_b
    assert mandatory.target_model_id == model_a


async def test_llm_reason_compiled_relationship_candidate_accepts_edge(monkeypatch):
    monkeypatch.delenv("THINK_COMPILED_RELATIONSHIP_REASONING", raising=False)
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate_id": str(candidate_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge",
                "basis": "topology",
                "edge_kind": "blocks",
                "source_model_id": str(source_id),
                "target_model_id": str(target_id),
                "member_model_ids": [str(source_id), str(target_id)],
                "evidence_model_ids": [str(source_id), str(target_id)],
                "judgment_leverage_score": 0.91,
                "explanation": "The integration gap blocks the launch plan.",
                "metadata": {"mechanism": "Launch requires the integration."},
            },
        },
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=source_id,
                proposition_kind="state",
                confidence=0.84,
                activation=0.5,
                status="active",
                natural="The SSO integration is still incomplete.",
                proposition={"kind": "state", "assertion": "SSO incomplete"},
            ),
            SimpleNamespace(
                id=target_id,
                proposition_kind="commitment_outcome",
                confidence=0.79,
                activation=0.5,
                status="active",
                natural="Acme launch depends on SSO being ready.",
                proposition={"kind": "state", "assertion": "Launch depends on SSO"},
            ),
        ]
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": str(candidate_id),
                            "decision": "accept",
                            "confidence": 0.82,
                            "reason": "The dependency is concrete and durable.",
                        }
                    ],
                    "reasoning_trace": "Accepted one concrete relationship.",
                }
            )
        ],
    )

    diff, _ = await llm_reason(trigger, bundle, provider, max_tokens=2048)

    assert provider.calls[0]["max_tokens"] == 768
    assert "decisions" in provider.calls[0]["schema_hint"]
    assert "claim_ops" not in provider.calls[0]["schema_hint"]
    assert "<compiled_relationship_candidate_task>" in provider.calls[0]["user"]
    assert len(diff.relation_claim_ops) == 1
    assert diff.edge_ops == []
    relation = diff.relation_claim_ops[0]
    assert relation.source_model_id == source_id
    assert relation.target_model_id == target_id
    assert relation.edge_kind == "blocks"
    assert relation.write_policy == "accepted_edge"
    assert relation.status == "accepted"
    assert relation.metadata["relationship_candidate_id"] == str(candidate_id)


async def test_llm_reason_can_disable_compiled_relationship_candidate(monkeypatch):
    monkeypatch.setenv("THINK_COMPILED_RELATIONSHIP_REASONING", "0")
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge",
                "basis": "topology",
                "edge_kind": "blocks",
                "source_model_id": str(source_id),
                "target_model_id": str(target_id),
                "explanation": "The integration gap blocks launch.",
                "metadata": {"mechanism": "Launch requires the integration."},
            },
        },
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, ContextBundle(), provider)

    assert "<compiled_relationship_candidate_task>" not in provider.calls[0]["user"]
    assert "claim_ops" in provider.calls[0]["schema_hint"]


async def test_llm_reason_compiled_candidate_requires_structural_evidence(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_RELATIONSHIP_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge",
                "basis": "topology",
                "edge_kind": "blocks",
                "source_model_id": str(source_id),
                "target_model_id": str(target_id),
                "explanation": "The same customer appears in both memories.",
                "metadata": {},
            },
        },
    )
    provider = ScriptedProvider(
        responses=[
            json.dumps(
                {
                    "decisions": [
                        {
                            "candidate_id": str(candidate_id),
                            "decision": "accept",
                            "confidence": 0.91,
                            "reason": "Looks related.",
                        }
                    ],
                    "reasoning_trace": "LLM wanted to accept.",
                }
            )
        ],
    )

    diff, _ = await llm_reason(trigger, ContextBundle(), provider)

    assert provider.calls == []
    assert diff.edge_ops == []
    assert len(diff.relation_claim_ops) == 1
    relation = diff.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert relation.write_policy == "needs_review"
    assert relation.status == "needs_review"
    assert relation.source_model_id == source_id
    assert relation.target_model_id == target_id
    assert "missing structural evidence" in (diff.reasoning_trace or "")


async def test_llm_reason_compiled_candidate_gate_drops_low_score_noise(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_RELATIONSHIP_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge",
                "basis": "topology_suggested",
                "edge_kind": "supports",
                "source_model_id": str(source_id),
                "target_model_id": str(target_id),
                "judgment_leverage_score": 0.12,
                "explanation": "Weak topical overlap.",
            },
        },
    )
    provider = ScriptedProvider()

    diff, latency_ms = await llm_reason(trigger, ContextBundle(), provider)

    assert latency_ms == 0
    assert provider.calls == []
    assert diff.claim_ops == []
    assert diff.relation_claim_ops == []
    assert diff.edge_ops == []
    assert "below the pre-LLM usefulness floor" in (diff.reasoning_trace or "")


async def test_llm_reason_compiled_edge_type_candidate_uses_ontology_lane(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_RELATIONSHIP_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "edge_type",
                "basis": "ontology_gap",
                "member_model_ids": [str(source_id), str(target_id)],
                "proposed_proposition": {
                    "proposed_edge_kind": "gated_by_decision",
                    "description": "Progress depends on a decision gate.",
                    "relationship_summary": (
                        "The launch blocker depends on approval."
                    ),
                    "nearest_existing_kind": "blocks",
                    "dropped_dimensions": ["approval authority"],
                },
                "explanation": "The relation needs a decision-gate edge kind.",
            },
        },
    )
    provider = ScriptedProvider()

    diff, latency_ms = await llm_reason(trigger, ContextBundle(), provider)

    assert latency_ms == 0
    assert provider.calls == []
    assert diff.relation_claim_ops == []
    assert diff.ontology_gap_ops == []
    assert "edge_type candidate routed to ontology workflow" in (
        diff.reasoning_trace or ""
    )


async def test_llm_reason_compiled_relationship_candidate_skips_situations(
    monkeypatch,
):
    monkeypatch.setenv("THINK_COMPILED_RELATIONSHIP_REASONING", "1")
    tid = uuid7()
    trig_id = uuid7()
    candidate_id = uuid7()
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tid,
        seed_signature={
            "trigger_id": str(trig_id),
            "relationship_candidate": {
                "id": str(candidate_id),
                "candidate_kind": "situation",
                "basis": "topology",
                "member_model_ids": [str(uuid7()), str(uuid7())],
                "explanation": "Composite pressure candidate.",
            },
        },
    )
    provider = ScriptedProvider(
        responses=[_minimal_raw_diff_json(str(trig_id), str(tid))],
    )

    await llm_reason(trigger, ContextBundle(), provider)

    assert "<compiled_relationship_candidate_task>" not in provider.calls[0]["user"]
    assert "claim_ops" in provider.calls[0]["schema_hint"]


async def test_llm_reason_keeps_full_schema_when_acts_are_available():
    tid = uuid7()
    trig_id = uuid7()
    commitment_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
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
        kind="T1",
        tenant_id=tid,
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
        kind="T1",
        tenant_id=tid,
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
            trigger,
            bundle,
            provider,
            max_attempts=2,  # 1 retry → backoff 2^0 = 1s
        )
    elapsed = time.monotonic() - t0
    # Two attempts total; one sleep of 1s between them.
    assert elapsed >= 1.0
    assert elapsed < 10.0


async def test_llm_reason_permanent_error_does_not_retry():
    """Provider quota/billing failures should not burn the full retry budget."""
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
        observation_id=trig_id,
        seed_natural_text="x",
    )
    bundle = ContextBundle()
    provider = ScriptedProvider(
        responses=[
            LLMError(
                "Error code: 402 - {'error': {'message': 'Insufficient Balance'}}"
            ),
            _minimal_raw_diff_json(str(trig_id), str(tid)),
        ],
    )

    with pytest.raises(ReasoningFailure):
        await llm_reason(trigger, bundle, provider, max_attempts=3)

    assert len(provider.calls) == 1


async def test_llm_reason_transient_then_success_recovers():
    """First attempt raises LLMError; second attempt returns a valid diff."""
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
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
        trigger,
        bundle,
        provider,
        max_attempts=3,
    )
    assert diff.tenant_id == tid


async def test_llm_reason_records_call_count():
    """The ScriptedProvider records calls; each retry logs one call."""
    tid = uuid7()
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tid,
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
