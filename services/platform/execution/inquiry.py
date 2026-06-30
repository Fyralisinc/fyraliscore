"""Adaptive inquiry retrieval runtime.

This module is the active implementation of the proposal's routed
retrieval loop. It keeps the existing retrieval pathways as low-level
executors, but wraps them in the production shape the architecture
calls for: baseline seeding, hypotheses, question planning, evidence
reservoir, sufficiency, and a compact context packet for reasoning.
"""

from __future__ import annotations

import os
import time
from typing import Any, Literal

import asyncpg

from lib.llm.provider import LLMProvider
from services.reasoning.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
    primary_retrieve,
)

from .action_execution import (
    _ActionExecutionRecord,  # noqa: F401
    _QuestionRetrievalPlan,  # noqa: F401
    _action_timing_note,  # noqa: F401
    _execute_action,  # noqa: F401
    _execute_question_retrieval_actions,  # noqa: F401
    _execute_question_retrieval_actions_serial,  # noqa: F401
    _execute_question_retrieval_actions_staged,  # noqa: F401
)
from .action_cache import (
    action_seed_entities as _action_seed_entities,  # noqa: F401
    action_seed_model_ids as _action_seed_model_ids,  # noqa: F401
    bind_action_to_previous_results as _bind_action_to_previous_results,  # noqa: F401
    clone_pathway_result as _clone_pathway_result,  # noqa: F401
    dedupe_seed_entities as _dedupe_seed_entities,  # noqa: F401
    retrieval_action_cache_key as _retrieval_action_cache_key,  # noqa: F401
    seed_action_cache_from_baseline as _seed_action_cache_from_baseline,  # noqa: F401
    seed_entities_from_pathway_results as _seed_entities_from_pathway_results,  # noqa: F401
    seed_model_ids_from_pathway_results as _seed_model_ids_from_pathway_results,  # noqa: F401
    stable_cache_value as _stable_cache_value,  # noqa: F401
)
from .answer_evaluation import (
    answer_question as _answer_question,  # noqa: F401
    classify_hypothesis_links as _classify_hypothesis_links,  # noqa: F401
    has_missing_owner_language as _has_missing_owner_language,  # noqa: F401
    has_premise_challenge_language as _has_premise_challenge_language,  # noqa: F401
    resolved_unknowns_for_answer as _resolved_unknowns_for_answer,  # noqa: F401
    sufficiency_gate as _sufficiency_gate,  # noqa: F401
)
from .config import InquiryConfig
from .context_packet import (
    background_summaries as _background_summaries,  # noqa: F401
    candidate_state_changes as _candidate_state_changes,  # noqa: F401
    compile_context_packet as _compile_context_packet,  # noqa: F401
    coverage_share as _coverage_share,  # noqa: F401
    evidence_card_confidence as _evidence_card_confidence,  # noqa: F401
    evidence_sort_key as _evidence_sort_key,  # noqa: F401
    evidence_value as _evidence_value,  # noqa: F401
    filter_context_packet_evidence as _filter_context_packet_evidence,  # noqa: F401
    marginal_evidence_value as _marginal_evidence_value,  # noqa: F401
    memory_decision_candidates as _memory_decision_candidates,  # noqa: F401
    minimal_evidence_target as _minimal_evidence_target,  # noqa: F401
    minimal_floor as _minimal_floor,  # noqa: F401
    protected_answer_ref_count as _protected_answer_ref_count,  # noqa: F401
    rank_evidence as _rank_evidence,  # noqa: F401
    redundancy_penalty as _redundancy_penalty,  # noqa: F401
    select_minimal_sufficient_evidence as _select_minimal_sufficient_evidence,  # noqa: F401
    state_contract_for_context_packet as _state_contract_for_context_packet,  # noqa: F401
)
from .evidence_utils import (
    compact as _compact,  # noqa: F401
    declares_unrelated_to_trigger as _declares_unrelated_to_trigger,  # noqa: F401
    estimate_tokens as _estimate_tokens,  # noqa: F401
    evidence_supports_ownership as _evidence_supports_ownership,  # noqa: F401
    evidence_to_dict as _evidence_to_dict,  # noqa: F401
    has_material_trigger_overlap as _has_material_trigger_overlap,  # noqa: F401
    is_counterevidence_for_leading_hypothesis as _is_counterevidence_for_leading_hypothesis,  # noqa: F401
    is_stale_relative_to_trigger as _is_stale_relative_to_trigger,  # noqa: F401
    jsonable as _jsonable,  # noqa: F401
    material_tokens as _material_tokens,  # noqa: F401
    sensitivity as _sensitivity,  # noqa: F401
    stable_hash as _stable_hash,  # noqa: F401
    timestamp_sort_value as _timestamp_sort_value,  # noqa: F401
    trust_score as _trust_score,  # noqa: F401
)
from .language_signals import (
    has_act_affecting_language as _has_act_affecting_language,  # noqa: F401
    has_broad_signal_language as _has_broad_signal_language,  # noqa: F401
    has_commitment_language as _has_commitment_language,  # noqa: F401
    has_constraint_language as _has_constraint_language,  # noqa: F401
    has_dependency_language as _has_dependency_language,  # noqa: F401
    has_revenue_impact_language as _has_revenue_impact_language,  # noqa: F401
    has_risk_language as _has_risk_language,  # noqa: F401
    mentions_recurrence as _mentions_recurrence,  # noqa: F401
    scrub_negated_signal_language as _scrub_negated_signal_language,  # noqa: F401
    signal_has_material_update_intent as _signal_has_material_update_intent,  # noqa: F401
)
from .lexical_terms import (
    SPARSE_STRONG_SINGLE_MATCH_MAX_DF as _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,  # noqa: F401
    focused_index_lookup_groups as _focused_index_lookup_groups,  # noqa: F401
    focused_index_terms as _focused_index_terms,  # noqa: F401
    focused_material_tokens as _focused_material_tokens,  # noqa: F401
    hybrid_lookup_terms as _hybrid_lookup_terms,  # noqa: F401
    hybrid_lexical_terms as _hybrid_lexical_terms,  # noqa: F401
    hybrid_sparse_lookup_groups as _hybrid_sparse_lookup_groups,  # noqa: F401
    hybrid_sparse_lookup_terms as _hybrid_sparse_lookup_terms,  # noqa: F401
    hybrid_sparse_strong_single_match_terms as _hybrid_sparse_strong_single_match_terms,  # noqa: F401
    is_focused_strong_token as _is_focused_strong_token,  # noqa: F401
    like_patterns_for_terms as _like_patterns_for_terms,  # noqa: F401
    relevance_tokens as _relevance_tokens,  # noqa: F401
)
from .motif_utils import (
    action_motif_uuid as _action_motif_uuid,  # noqa: F401
    json_obj as _json_obj,  # noqa: F401
    motif_domain_terms as _motif_domain_terms,  # noqa: F401
    motif_plan_from_actions as _motif_plan_from_actions,  # noqa: F401
    motif_signature_for as _motif_signature_for,  # noqa: F401
    motif_signature_match_score as _motif_signature_match_score,  # noqa: F401
    packet_used_evidence_ids as _packet_used_evidence_ids,  # noqa: F401
    safe_int as _safe_int,  # noqa: F401
    safe_uuid as _safe_uuid,  # noqa: F401
    set_overlap_ratio as _set_overlap_ratio,  # noqa: F401
)
from .question_planning_runtime import (
    question_planning_max_tokens as _question_planning_max_tokens,  # noqa: F401
    question_planning_schema_name as _question_planning_schema_name,  # noqa: F401
    question_planning_timeout_seconds as _question_planning_timeout_seconds,  # noqa: F401
    use_compact_question_planning_schema as _use_compact_question_planning_schema,  # noqa: F401
)
from .question_generation import (
    candidate_questions as _candidate_questions,  # noqa: F401
    dedupe_unknowns as _dedupe_unknowns,  # noqa: F401
    deterministic_delta_uncertainties as _deterministic_delta_uncertainties,  # noqa: F401
    generate_hypotheses as _generate_hypotheses,  # noqa: F401
    initial_unknowns as _initial_unknowns,  # noqa: F401
)
from .question_planning import (
    ALLOWED_DELTA_TYPES as _ALLOWED_DELTA_TYPES,  # noqa: F401
    ALLOWED_QUESTION_PRIMITIVES as _ALLOWED_QUESTION_PRIMITIVES,  # noqa: F401
    DEFAULT_COST_BY_PRIMITIVE as _DEFAULT_COST_BY_PRIMITIVE,  # noqa: F401
    DEFAULT_STOP_BY_PRIMITIVE as _DEFAULT_STOP_BY_PRIMITIVE,  # noqa: F401
    DEFAULT_TARGET_BY_PRIMITIVE as _DEFAULT_TARGET_BY_PRIMITIVE,  # noqa: F401
    QUESTION_ID_BY_PRIMITIVE as _QUESTION_ID_BY_PRIMITIVE,  # noqa: F401
    baseline_snapshot_for_question_planning as _baseline_snapshot_for_question_planning,  # noqa: F401
    candidate_questions_for_round as _candidate_questions_for_round,  # noqa: F401
    candidate_questions_from_belief_deltas as _candidate_questions_from_belief_deltas,  # noqa: F401
    clean_delta_items as _clean_delta_items,  # noqa: F401
    compact_baseline_snapshot_for_question_planning as _compact_baseline_snapshot_for_question_planning,  # noqa: F401
    delta_question_expected_value as _delta_question_expected_value,  # noqa: F401
    delta_question_focus as _delta_question_focus,  # noqa: F401
    expand_compact_question_plan as _expand_compact_question_plan,  # noqa: F401
    fallback_uncertainty_slots_for_delta as _fallback_uncertainty_slots_for_delta,  # noqa: F401
    generate_llm_question_plan as _generate_llm_question_plan,  # noqa: F401
    merge_llm_and_safety_questions as _merge_llm_and_safety_questions,  # noqa: F401
    normalize_delta_type as _normalize_delta_type,  # noqa: F401
    normalize_impact_label as _normalize_impact_label,  # noqa: F401
    normalize_llm_belief_delta_hypotheses as _normalize_llm_belief_delta_hypotheses,  # noqa: F401
    normalize_llm_questions as _normalize_llm_questions,  # noqa: F401
    primitive_for_delta_slot as _primitive_for_delta_slot,  # noqa: F401
    punctuate_question_text as _punctuate_question_text,  # noqa: F401
    quality_control_question_text as _quality_control_question_text,  # noqa: F401
    question_from_delta_slot as _question_from_delta_slot,  # noqa: F401
    question_quality_failure_reason as _question_quality_failure_reason,  # noqa: F401
    question_quality_summary as _question_quality_summary,  # noqa: F401
    repair_question_text as _repair_question_text,  # noqa: F401
    tests_for_delta_question as _tests_for_delta_question,  # noqa: F401
)
from .question_policy import (
    apply_question_policy as _apply_question_policy,  # noqa: F401
    clamp_float as _clamp_float,  # noqa: F401
    policy_budget as _policy_budget,  # noqa: F401
    question_information_facets as _question_information_facets,  # noqa: F401
    question_marginal_score as _question_marginal_score,  # noqa: F401
    question_policy_budget_multiplier as _question_policy_budget_multiplier,  # noqa: F401
    question_policy_score_boost as _question_policy_score_boost,  # noqa: F401
    question_policy_success_rate as _question_policy_success_rate,  # noqa: F401
    question_target_overlap as _question_target_overlap,  # noqa: F401
    question_target_tokens as _question_target_tokens,  # noqa: F401
    select_questions as _select_questions,  # noqa: F401
)
from .question_text import (
    QuestionAnchors as _QuestionAnchors,  # noqa: F401
    capitalized_anchor_spans as _capitalized_anchor_spans,  # noqa: F401
    claim_from_text as _claim_from_text,  # noqa: F401
    clean_question_anchor as _clean_question_anchor,  # noqa: F401
    clean_question_focus_phrase as _clean_question_focus_phrase,  # noqa: F401
    counterevidence_focus as _counterevidence_focus,  # noqa: F401
    domain_keyword_focus as _domain_keyword_focus,  # noqa: F401
    entity_label_from_seed as _entity_label_from_seed,  # noqa: F401
    fallback_focus_from_delta_claim as _fallback_focus_from_delta_claim,  # noqa: F401
    focus_from_preface as _focus_from_preface,  # noqa: F401
    focus_sentence_score as _focus_sentence_score,  # noqa: F401
    focus_sentences as _focus_sentences,  # noqa: F401
    is_specific_focus_phrase as _is_specific_focus_phrase,  # noqa: F401
    looks_like_company_overview as _looks_like_company_overview,  # noqa: F401
    looks_like_machine_identifier as _looks_like_machine_identifier,  # noqa: F401
    question_anchors as _question_anchors,  # noqa: F401
    question_constraint_phrase as _question_constraint_phrase,  # noqa: F401
    question_entity_labels as _question_entity_labels,  # noqa: F401
    question_focus_phrase as _question_focus_phrase,  # noqa: F401
    question_subject as _question_subject,  # noqa: F401
    safe_question_focus as _safe_question_focus,  # noqa: F401
    specific_question as _specific_question,  # noqa: F401
    truncate_text as _truncate_text,  # noqa: F401
)
from .retrieval_plan import (
    compile_motif_retrieval_plan as _compile_motif_retrieval_plan,  # noqa: F401
    compile_retrieval_plan as _compile_retrieval_plan,  # noqa: F401
    compile_static_retrieval_plan as _compile_static_retrieval_plan,  # noqa: F401
    focused_index_actions as _focused_index_actions,  # noqa: F401
)
from .retrieval_actions import (
    FocusedIndexHit as _FocusedIndexHit,  # noqa: F401
    cap_pathway_models as _cap_pathway_models,  # noqa: F401
    execute_focused_index_action as _execute_focused_index_action,  # noqa: F401
    execute_semantic_hybrid_action as _execute_semantic_hybrid_action,  # noqa: F401
    execute_semantic_terms_action as _execute_semantic_terms_action,  # noqa: F401
    fetch_bounded_lookup_rows as _fetch_bounded_lookup_rows,  # noqa: F401
    fetch_hybrid_lexical_fallback_rows as _fetch_hybrid_lexical_fallback_rows,  # noqa: F401
    focused_answerability_index_scan as _focused_answerability_index_scan,  # noqa: F401
    focused_answerability_primitives_for as _focused_answerability_primitives_for,  # noqa: F401
    focused_direct_scope_scan as _focused_direct_scope_scan,  # noqa: F401
    focused_scope_sparse_scan as _focused_scope_sparse_scan,  # noqa: F401
    focused_seed_entity_pairs as _focused_seed_entity_pairs,  # noqa: F401
    hybrid_lexical_model_scan as _hybrid_lexical_model_scan,  # noqa: F401
    hybrid_sparse_model_scan as _hybrid_sparse_model_scan,  # noqa: F401
    merge_hybrid_semantic_lexical_models as _merge_hybrid_semantic_lexical_models,  # noqa: F401
)
from .retrieval_learning import (
    RetrievalMotifPenalty as _RetrievalMotifPenalty,  # noqa: F401
    decay_sage_route_utilities as _decay_sage_route_utilities,  # noqa: F401
    is_low_value_model_noise as _is_low_value_model_noise,  # noqa: F401
    learn_retrieval_motifs as _learn_retrieval_motifs,  # noqa: F401
    learn_sage_route_utilities as _learn_sage_route_utilities,  # noqa: F401
    load_question_policy_stats as _load_question_policy_stats,  # noqa: F401
    load_retrieval_motifs_for_questions as _load_retrieval_motifs_for_questions,  # noqa: F401
    load_sage_route_utilities as _load_sage_route_utilities,  # noqa: F401
    motif_failure_penalties as _motif_failure_penalties,  # noqa: F401
    penalize_retrieval_motifs as _penalize_retrieval_motifs,  # noqa: F401
)
from .reconstruction_state import (
    apply_reconstruction_to_actions as _apply_reconstruction_to_actions,  # noqa: F401
    build_reconstruction_state as _build_reconstruction_state,  # noqa: F401
    evidence_state_for_reader as _evidence_state_for_reader,  # noqa: F401
    planner_reconstruction_payload as _planner_reconstruction_payload,  # noqa: F401
    reader_reconstruction_payload as _reader_reconstruction_payload,  # noqa: F401
    reconstruction_gate_decision as _reconstruction_gate_decision,  # noqa: F401
    reconstruction_state_for_purpose as _reconstruction_state_for_purpose,  # noqa: F401
    reconstruction_state_note as _reconstruction_state_note,  # noqa: F401
    reconstruction_state_payload as _reconstruction_state_payload,  # noqa: F401
    serialized_payload_size as _serialized_reconstruction_payload_size,  # noqa: F401
)
from .reflective_learning import (  # noqa: F401
    ReflectiveRuleAttribution as _ReflectiveRuleAttribution,
    ReflectiveRuleCandidate as _ReflectiveRuleCandidate,
    ReflectiveRuleReplayResult as _ReflectiveRuleReplayResult,
    apply_reflective_rule_credit as _apply_reflective_rule_credit,
    learn_reflective_rules as _learn_reflective_rules,
    persist_reflective_rule_attributions as _persist_reflective_rule_attributions,
    persist_reflective_rule_replay as _persist_reflective_rule_replay,
    propose_reflective_rule_candidates as _propose_reflective_rule_candidates,
    reflective_rule_attributions_from_result as _reflective_rule_attributions_from_result,
    replay_reflective_rule_candidate as _replay_reflective_rule_candidate,
    upsert_reflective_rule_candidate as _upsert_reflective_rule_candidate,
)
from .result_composition import (
    _add_result_to_reservoir,  # noqa: F401
    _append_structural_closure,  # noqa: F401
    _apply_relevance_diversity,  # noqa: F401
    _canonical_entity_pairs,  # noqa: F401
    _coverage_compaction_target,  # noqa: F401
    _coverage_selection_utility,  # noqa: F401
    _entity_coverage_pressure,  # noqa: F401
    _has_counterevidence_qualifier_language,  # noqa: F401
    _has_selected_model_link,  # noqa: F401
    _has_uncovered_answer_obligation,  # noqa: F401
    _is_structural_detail_model,  # noqa: F401
    _lexical_relevance_score,  # noqa: F401
    _linked_anchor_ids,  # noqa: F401
    _merge_results,  # noqa: F401
    _model_answer_obligation_features,  # noqa: F401
    _model_coverage_features,  # noqa: F401
    _model_evidence_relevance_score,  # noqa: F401
    _model_member_ids,  # noqa: F401
    _model_relevance_cluster_key,  # noqa: F401
    _pack_structural_links,  # noqa: F401
    _relevance_diversity_candidate_cap,  # noqa: F401
    _result_from_pathway,  # noqa: F401
    _role_coverage_pressure,  # noqa: F401
    _scope_relevance_score,  # noqa: F401
    _score_model_relevance,  # noqa: F401
    _select_relevant_models,  # noqa: F401
    _structural_closure_reason,  # noqa: F401
    _upsert_evidence,  # noqa: F401
)
from .inquiry_persistence import (
    _classify_omission_reason,  # noqa: F401
    _emit_phase1_traces,  # noqa: F401
    _packet_evidence_refs_by_question,  # noqa: F401
    _persist_inquiry,
    _persist_sage_reader_activation_traces,  # noqa: F401
    _persist_sage_reader_decision_attributions,  # noqa: F401
    _reader_attribution_nonselected_limit,  # noqa: F401
    _reader_attribution_nonselected_min_score,  # noqa: F401
)
from .inquiry_bootstrap import _bootstrap_inquiry_run
from .inquiry_finalization import _finalize_inquiry_run
from .inquiry_rounds import (
    _BROAD_DISCOVERY_ACTION_PATHS,  # noqa: F401
    _InquiryRoundStatus,  # noqa: F401
    _execute_inquiry_rounds,
)
from .question_planning_schemas import (
    LLMBeliefDeltaSpec,
    LLMCompactBeliefDeltaSpec,
    LLMCompactQuestionPlan,
    LLMCompactQuestionSpec,
    LLMInquiryQuestionPlan,
    LLMInquiryQuestionSpec,
)
from .runtime_metrics import (
    append_stage_timing as _append_stage_timing,
    elapsed_ms as _elapsed_ms,
    runtime_residual_summary as _runtime_residual_summary,
)
from .sage_reader_execution import (
    _build_sage_reader,  # noqa: F401
    _execute_sage_reader_action,  # noqa: F401
    _execute_sage_reader_actions_for_round,  # noqa: F401
)
from .sage_reader_notes import (
    action_cache_summary as _action_cache_summary,  # noqa: F401
    compact_inquiry_notes_for_persistence as _compact_inquiry_notes_for_persistence,  # noqa: F401
    compact_sage_question_note_for_persistence as _compact_sage_question_note_for_persistence,  # noqa: F401
    compact_sage_reader_debug_for_persistence as _compact_sage_reader_debug_for_persistence,  # noqa: F401
    compact_sage_reader_notes_for_persistence as _compact_sage_reader_notes_for_persistence,  # noqa: F401
    record_sage_reader_notes as _record_sage_reader_notes,  # noqa: F401
    sage_only_retrieval_results as _sage_only_retrieval_results,  # noqa: F401
    sage_reader_action_gate as _sage_reader_action_gate,  # noqa: F401
    sage_reader_controller_summary as _sage_reader_controller_summary,  # noqa: F401
    sage_reader_plan_from_read_note as _sage_reader_plan_from_read_note,  # noqa: F401
    sage_reader_plan_from_result as _sage_reader_plan_from_result,  # noqa: F401
    sage_reader_plan_hard_abstained as _sage_reader_plan_hard_abstained,  # noqa: F401
    sage_reader_total_ms as _sage_reader_total_ms,  # noqa: F401
    trigger_has_explicit_model_anchor as _trigger_has_explicit_model_anchor,  # noqa: F401
)
from .routing import (
    adaptive_baseline_top_n as _adaptive_baseline_top_n,  # noqa: F401
    adaptive_evidence_limit as _adaptive_evidence_limit,  # noqa: F401
    cold_weak_noop_gate as _cold_weak_noop_gate,  # noqa: F401
    declares_no_material_update as _declares_no_material_update,  # noqa: F401
    route_for_trigger as _route_for_trigger,  # noqa: F401
    signal_class_for_trigger as _signal_class_for_trigger,  # noqa: F401
    trigger_text as _trigger_text,  # noqa: F401
)
from .types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    InquiryResult,
    InquiryStopStatus,
    LearnedRetrievalMotif,
    ModelRelevance,
    QuestionAnswer,
    QuestionPolicySignal,
    ReconstructionState,  # noqa: F401
    RetrievalAction,
    RetrievalActionPath,
    SignalRoute,
    SufficiencyVerdict,
)


def execution_retrieval_engine() -> str:
    return os.environ.get("EXECUTION_RETRIEVAL_ENGINE", "inquiry").strip().lower()


def inquiry_enabled() -> bool:
    return execution_retrieval_engine() not in {"legacy", "primary", "old"}


async def retrieve_for_execution(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    embedder: Any | None = None,
    llm_provider: LLMProvider | None = None,
    read_pool: asyncpg.Pool | None = None,
    route: SignalRoute | None = None,
    mode: Literal["deep", "fast"] = "deep",
    top_n: int = 80,
    config: InquiryConfig | None = None,
) -> InquiryResult | RetrievalResult:
    """Return the active retrieval result for Think/query callers.

    `EXECUTION_RETRIEVAL_ENGINE=legacy` gives an operator rollback path.
    The default is the new inquiry runtime.
    """
    cfg = config or InquiryConfig.from_env()
    if not inquiry_enabled():
        return await primary_retrieve(
            trigger,
            conn,
            embedder=embedder,
            read_pool=read_pool,
            structural_read_fanout_enabled=cfg.structural_read_fanout_enabled,
            structural_read_fanout_min_seeds=cfg.structural_read_fanout_min_seeds,
            structural_read_fanout_chunk_size=cfg.structural_read_fanout_chunk_size,
            top_n=top_n,
        )
    return await run_inquiry_retrieval(
        trigger,
        conn,
        embedder=embedder,
        llm_provider=llm_provider,
        read_pool=read_pool,
        route=route,
        mode=mode,
        top_n=top_n,
        config=cfg,
    )


async def run_inquiry_retrieval(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    embedder: Any | None = None,
    llm_provider: LLMProvider | None = None,
    read_pool: asyncpg.Pool | None = None,
    route: SignalRoute | None = None,
    mode: Literal["deep", "fast"] = "deep",
    top_n: int = 80,
    config: InquiryConfig | None = None,
) -> InquiryResult:
    total_started = time.perf_counter()
    bootstrap = await _bootstrap_inquiry_run(
        trigger=trigger,
        conn=conn,
        embedder=embedder,
        read_pool=read_pool,
        route=route,
        mode=mode,
        top_n=top_n,
        config=config,
    )
    round_status = await _execute_inquiry_rounds(
        bootstrap,
        trigger=trigger,
        conn=conn,
        embedder=embedder,
        llm_provider=llm_provider,
        read_pool=read_pool,
    )

    result = _finalize_inquiry_run(
        trigger=trigger,
        cfg=bootstrap.cfg,
        session_id=bootstrap.session_id,
        route=bootstrap.route,
        mode=mode,
        top_n=top_n,
        candidate_top_n=bootstrap.candidate_top_n,
        effective_top_n=bootstrap.effective_top_n,
        baseline_top_n=bootstrap.baseline_top_n,
        signal_class=bootstrap.signal_class,
        weak_signal=bootstrap.weak_signal,
        cold_weak_noop_gate=bootstrap.cold_weak_noop_gate,
        max_rounds=bootstrap.max_rounds,
        hypotheses=bootstrap.hypotheses,
        all_questions=bootstrap.all_questions,
        all_actions=bootstrap.all_actions,
        answers=bootstrap.answers,
        evidence_by_key=bootstrap.evidence_by_key,
        retrieval_results=bootstrap.retrieval_results,
        unknowns=bootstrap.unknowns,
        stop_status=round_status.stop_status,
        stop_reason=round_status.stop_reason,
        action_timing_notes=bootstrap.action_timing_notes,
        stage_timing_notes=bootstrap.stage_timing_notes,
        question_planning_notes=bootstrap.question_planning_notes,
        reconstruction_notes=bootstrap.reconstruction_notes,
        baseline_action_cache_notes=bootstrap.baseline_action_cache_notes,
        sage_reader_notes=bootstrap.sage_reader_notes,
        total_started=total_started,
    )
    if bootstrap.cfg.persist:
        stage_started = time.perf_counter()
        await _persist_inquiry(
            conn,
            result,
            trigger,
            persist_full_sage_reader_notes=bootstrap.cfg.persist_full_sage_reader_notes,
        )
        _append_stage_timing(
            bootstrap.stage_timing_notes,
            "persist_inquiry",
            stage_started,
        )
        result.notes["retrieval_runtime"] = _runtime_residual_summary(
            total_ms=_elapsed_ms(total_started),
            action_timings=bootstrap.action_timing_notes,
            stage_timings=bootstrap.stage_timing_notes,
        )
    return result


__all__ = [
    "EvidenceCard",
    "Hypothesis",
    "InquiryConfig",
    "InquiryQuestion",
    "InquiryResult",
    "InquiryStopStatus",
    "LLMBeliefDeltaSpec",
    "LLMCompactBeliefDeltaSpec",
    "LLMCompactQuestionPlan",
    "LLMCompactQuestionSpec",
    "LLMInquiryQuestionPlan",
    "LLMInquiryQuestionSpec",
    "LearnedRetrievalMotif",
    "ModelRelevance",
    "QuestionAnswer",
    "QuestionPolicySignal",
    "RetrievalAction",
    "RetrievalActionPath",
    "SignalRoute",
    "SufficiencyVerdict",
    "execution_retrieval_engine",
    "inquiry_enabled",
    "retrieve_for_execution",
    "run_inquiry_retrieval",
]
