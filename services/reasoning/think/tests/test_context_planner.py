from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.context_planner import (
    _restrict_stage1_context_bundle,
    _should_emit_missing_transition_triggers,
    _think_inquiry_config_for_trigger,
    stage1_inquiry_config_for_trigger,
)


def test_missing_transition_emission_only_runs_for_t1() -> None:
    tenant_id = uuid7()

    assert _should_emit_missing_transition_triggers(
        TriggerContext(kind="T1", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T2", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T3", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T4", tenant_id=tenant_id)
    )


def test_think_inquiry_config_uses_t1_triage_profile(monkeypatch) -> None:
    monkeypatch.delenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", raising=False)
    config = _think_inquiry_config_for_trigger(
        TriggerContext(kind="T1", tenant_id=uuid7())
    )

    assert config.planner_profile == "triage"
    assert config.max_rounds == 1
    assert config.questions_per_round == 2
    assert config.llm_question_planning_trigger_kinds == ("T1",)
    assert config.question_primitive_weights["COMMITMENT"] > 0
    assert config.context_packet_evidence_mode == "models_only"


def test_stage1_inquiry_config_disables_every_learned_controller() -> None:
    config = stage1_inquiry_config_for_trigger(
        TriggerContext(kind="T1", tenant_id=uuid7())
    )

    assert config.learned_policy_enabled is False
    assert config.llm_question_planning_enabled is False
    assert config.llm_question_planning_trigger_kinds == ()
    assert config.utility_governor_enabled is False
    assert config.adaptive_question_budget_enabled is False
    assert config.retrieval_motifs_enabled is False
    assert config.reflective_rules_enabled is False
    assert config.sage_reader_enabled is False
    assert config.sage_retrieval_policy_enabled is False
    assert config.persist is False


def test_stage1_context_packet_contains_only_models_and_observations() -> None:
    bundle = ContextBundle(
        acts_summary={
            "goals": [object()],
            "commitments": [object()],
            "decisions": [object()],
        },
        resources_summary=[object()],  # type: ignore[list-item]
        customer_context={"customer": "context"},
        topology_context={"graph": "context"},
    )

    _restrict_stage1_context_bundle(bundle)

    assert bundle.acts_summary == {"goals": [], "commitments": [], "decisions": []}
    assert bundle.resources_summary == []
    assert bundle.customer_context is None
    assert bundle.topology_context is None
    assert bundle.notes["stage1_company_memory"] is True


def test_think_inquiry_config_allows_investigative_t4_profile(monkeypatch) -> None:
    monkeypatch.delenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", raising=False)
    config = _think_inquiry_config_for_trigger(
        TriggerContext(
            kind="T4",
            subkind="open_question_search",
            tenant_id=uuid7(),
        )
    )

    assert config.planner_profile == "investigative_pattern"
    assert config.max_rounds == 4
    assert config.questions_per_round == 3
    assert config.llm_question_planning_trigger_kinds == ("T4",)
    assert config.utility_governor_planner_skip_threshold == 0.54
    assert config.question_primitive_weights["RECURRENCE"] > 0
    assert config.context_packet_evidence_mode == "model_first"


def test_think_inquiry_config_keeps_repair_t4_deterministic(monkeypatch) -> None:
    monkeypatch.delenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", raising=False)
    config = _think_inquiry_config_for_trigger(
        TriggerContext(
            kind="T4",
            subkind="representation_repair",
            tenant_id=uuid7(),
            seed_signature={
                "repair_intent": "repair_validation_dropped_value",
                "residual_kind": "validation_dropped_value",
            },
        )
    )

    assert config.planner_profile == "verification"
    assert config.llm_question_planning_trigger_kinds == ()
    assert config.question_primitive_weights == {}
    assert config.context_packet_evidence_mode == "models_only"


def test_think_inquiry_config_respects_explicit_evidence_mode_override(
    monkeypatch,
) -> None:
    monkeypatch.setenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", "all")
    config = _think_inquiry_config_for_trigger(
        TriggerContext(
            kind="T4",
            subkind="open_question_search",
            tenant_id=uuid7(),
        )
    )

    assert config.planner_profile == "investigative_pattern"
    assert config.context_packet_evidence_mode == "all"
