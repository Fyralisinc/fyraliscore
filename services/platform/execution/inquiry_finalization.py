"""Final assembly phase for adaptive inquiry retrieval."""

from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any, Literal
from uuid import UUID

from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .answer_evaluation import sufficiency_gate as _sufficiency_gate
from .config import InquiryConfig
from .context_packet import (
    compile_context_packet as _compile_context_packet,
    rank_evidence as _rank_evidence,
    select_minimal_sufficient_evidence as _select_minimal_sufficient_evidence,
)
from .evidence_utils import jsonable as _jsonable
from .result_composition import _merge_results
from .routing import adaptive_evidence_limit as _adaptive_evidence_limit
from .runtime_metrics import (
    append_stage_timing as _append_stage_timing,
    elapsed_ms as _elapsed_ms,
    runtime_residual_summary as _runtime_residual_summary,
)
from .sage_reader_notes import (
    action_cache_summary as _action_cache_summary,
    sage_only_retrieval_results as _sage_only_retrieval_results,
    sage_reader_controller_summary as _sage_reader_controller_summary,
)
from .types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    InquiryResult,
    InquiryStopStatus,
    QuestionAnswer,
    RetrievalAction,
    SignalRoute,
    SufficiencyVerdict,
)


def _build_inquiry_notes(
    *,
    cfg: InquiryConfig,
    session_id: UUID,
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    top_n: int,
    candidate_top_n: int,
    effective_top_n: int,
    baseline_top_n: int,
    signal_class: str,
    weak_signal: bool,
    cold_weak_noop_gate: dict[str, Any],
    max_rounds: int,
    all_questions: list[InquiryQuestion],
    all_actions: list[RetrievalAction],
    action_timing_notes: list[dict[str, Any]],
    stage_timing_notes: list[dict[str, Any]],
    question_planning_notes: list[dict[str, Any]],
    reconstruction_notes: list[dict[str, Any]],
    baseline_action_cache_notes: Any,
    sage_reader_notes: dict[str, Any],
    sage_controller_notes: dict[str, Any],
    evidence_cards: list[EvidenceCard],
    evidence_before_rank: int,
    ranked_evidence_cards: list[EvidenceCard],
    evidence_limit: int,
    evidence_minimization: dict[str, Any],
    verdict: SufficiencyVerdict,
    packet: dict[str, Any],
    runtime_ms: int,
) -> dict[str, Any]:
    return {
        "execution_engine": "inquiry",
        "route": route,
        "mode": mode,
        "session_id": str(session_id),
        "max_rounds": max_rounds,
        "question_count": len(all_questions),
        "retrieval_action_count": len(all_actions),
        "weak_signal_budgeted": weak_signal,
        "requested_top_n": top_n,
        "candidate_top_n": candidate_top_n,
        "effective_top_n": effective_top_n,
        "baseline_top_n": baseline_top_n,
        "candidate_model_limit": cfg.candidate_model_limit,
        "result_model_limit": cfg.result_model_limit,
        "signal_class": signal_class,
        "action_model_budget_limit": cfg.action_model_budget_limit,
        "action_observation_budget_limit": cfg.action_observation_budget_limit,
        "llm_question_planning_enabled": cfg.llm_question_planning_enabled,
        "question_planning": question_planning_notes,
        "reconstruction": reconstruction_notes,
        "cold_weak_noop_gate": cold_weak_noop_gate,
        "retrieval_action_timings": action_timing_notes,
        "retrieval_stage_timings": stage_timing_notes,
        "retrieval_runtime": _runtime_residual_summary(
            total_ms=runtime_ms,
            action_timings=action_timing_notes,
            stage_timings=stage_timing_notes,
        ),
        "retrieval_action_cache": _action_cache_summary(action_timing_notes),
        "retrieval_action_cache_seeded_from_baseline": baseline_action_cache_notes,
        "sage_reader": sage_reader_notes,
        "sage_reader_controller": sage_controller_notes,
        "evidence_count": len(evidence_cards),
        "evidence_before_rank": evidence_before_rank,
        "evidence_after_rank": len(ranked_evidence_cards),
        "evidence_limit": evidence_limit,
        "evidence_minimization": evidence_minimization,
        "sufficiency": _jsonable(asdict(verdict)),
        "context_packet": packet,
    }


def _base_final_sufficiency_verdict(
    *,
    route: SignalRoute,
    hypotheses: tuple[Hypothesis, ...],
    ranked_evidence_cards: list[EvidenceCard],
    answers: list[QuestionAnswer],
    max_rounds: int,
    cold_weak_noop_gate: dict[str, Any],
    sage_controller_notes: dict[str, Any],
    unknowns: set[str],
    stop_status: InquiryStopStatus,
    stop_reason: str,
) -> SufficiencyVerdict:
    if max_rounds == 0:
        return _sufficiency_gate(
            route,
            hypotheses,
            ranked_evidence_cards,
            answers,
            round_index=0,
            max_rounds=0,
            unknowns=unknowns,
        )
    if cold_weak_noop_gate["used"]:
        return SufficiencyVerdict(
            status="no_update_needed",
            reason=str(cold_weak_noop_gate["reason"]),
            evidence_count=0,
            answered_questions=0,
            remaining_unknowns=(),
        )
    if sage_controller_notes["global_negative_route_gate"]:
        return SufficiencyVerdict(
            status="no_update_needed",
            reason=(
                "sage reader learned this route as negative and every "
                "selected question abstained"
            ),
            evidence_count=0,
            answered_questions=0,
            remaining_unknowns=tuple(sorted(unknowns)[:10]),
        )
    return SufficiencyVerdict(
        status=stop_status,
        reason=stop_reason,
        evidence_count=len(ranked_evidence_cards),
        answered_questions=len(answers),
        remaining_unknowns=tuple(sorted(unknowns)[:10]),
    )


def _finalize_inquiry_run(
    *,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    session_id: UUID,
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    top_n: int,
    candidate_top_n: int,
    effective_top_n: int,
    baseline_top_n: int,
    signal_class: str,
    weak_signal: bool,
    cold_weak_noop_gate: dict[str, Any],
    max_rounds: int,
    hypotheses: tuple[Hypothesis, ...],
    all_questions: list[InquiryQuestion],
    all_actions: list[RetrievalAction],
    answers: list[QuestionAnswer],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    retrieval_results: list[RetrievalResult],
    unknowns: set[str],
    stop_status: InquiryStopStatus,
    stop_reason: str,
    action_timing_notes: list[dict[str, Any]],
    stage_timing_notes: list[dict[str, Any]],
    question_planning_notes: list[dict[str, Any]],
    reconstruction_notes: list[dict[str, Any]],
    baseline_action_cache_notes: Any,
    sage_reader_notes: dict[str, Any],
    total_started: float,
) -> InquiryResult:
    evidence_before_rank = len(evidence_by_key)
    sage_controller_notes = _sage_reader_controller_summary(
        sage_reader_notes,
        trigger=trigger,
    )
    evidence_limit = _adaptive_evidence_limit(
        cfg,
        route=route,
        mode=mode,
        signal_class=signal_class,
    )
    stage_started = time.perf_counter()
    if (
        cold_weak_noop_gate["used"]
        or sage_controller_notes["global_negative_route_gate"]
    ):
        ranked_evidence_cards = []
    else:
        ranked_evidence_cards = _rank_evidence(
            list(evidence_by_key.values()),
            limit=evidence_limit,
        )
    _append_stage_timing(
        stage_timing_notes,
        "evidence_rank",
        stage_started,
        evidence_before_rank=evidence_before_rank,
        evidence_after_rank=len(ranked_evidence_cards),
    )
    verdict = _base_final_sufficiency_verdict(
        route=route,
        hypotheses=hypotheses,
        ranked_evidence_cards=ranked_evidence_cards,
        answers=answers,
        max_rounds=max_rounds,
        cold_weak_noop_gate=cold_weak_noop_gate,
        sage_controller_notes=sage_controller_notes,
        unknowns=unknowns,
        stop_status=stop_status,
        stop_reason=stop_reason,
    )
    stage_started = time.perf_counter()
    evidence_cards, evidence_minimization = _select_minimal_sufficient_evidence(
        ranked_evidence_cards,
        hypotheses=hypotheses,
        questions=all_questions,
        answers=answers,
        route=route,
        mode=mode,
        evidence_limit=evidence_limit,
    )
    _append_stage_timing(
        stage_timing_notes,
        "evidence_minimize",
        stage_started,
        evidence_after_minimize=len(evidence_cards),
    )
    verdict = SufficiencyVerdict(
        status=verdict.status,
        reason=verdict.reason,
        evidence_count=len(evidence_cards),
        answered_questions=verdict.answered_questions,
        remaining_unknowns=verdict.remaining_unknowns,
    )

    retrieval_results_for_merge = (
        []
        if cold_weak_noop_gate["used"]
        else (
            _sage_only_retrieval_results(retrieval_results)
            if sage_controller_notes["global_negative_route_gate"]
            else retrieval_results
        )
    )
    stage_started = time.perf_counter()
    combined = _merge_results(
        trigger,
        retrieval_results_for_merge,
        top_n=effective_top_n,
        note_prefix="inquiry",
        config=cfg,
        relevance_gate=True,
    )
    _append_stage_timing(
        stage_timing_notes,
        "final_result_merge",
        stage_started,
        models=len(combined.models),
        observations=len(combined.observations),
    )
    stage_started = time.perf_counter()
    packet = _compile_context_packet(
        trigger,
        route,
        hypotheses,
        all_questions,
        answers,
        evidence_cards,
        verdict,
        token_budget=cfg.reasoning_packet_token_budget,
        evidence_mode=cfg.context_packet_evidence_mode,
    )
    _append_stage_timing(
        stage_timing_notes,
        "context_packet_compile",
        stage_started,
        evidence=len(evidence_cards),
    )
    runtime_ms = _elapsed_ms(total_started)
    notes = _build_inquiry_notes(
        cfg=cfg,
        session_id=session_id,
        route=route,
        mode=mode,
        top_n=top_n,
        candidate_top_n=candidate_top_n,
        effective_top_n=effective_top_n,
        baseline_top_n=baseline_top_n,
        signal_class=signal_class,
        weak_signal=weak_signal,
        cold_weak_noop_gate=cold_weak_noop_gate,
        max_rounds=max_rounds,
        all_questions=all_questions,
        all_actions=all_actions,
        action_timing_notes=action_timing_notes,
        stage_timing_notes=stage_timing_notes,
        question_planning_notes=question_planning_notes,
        reconstruction_notes=reconstruction_notes,
        baseline_action_cache_notes=baseline_action_cache_notes,
        sage_reader_notes=sage_reader_notes,
        sage_controller_notes=sage_controller_notes,
        evidence_cards=evidence_cards,
        evidence_before_rank=evidence_before_rank,
        ranked_evidence_cards=ranked_evidence_cards,
        evidence_limit=evidence_limit,
        evidence_minimization=evidence_minimization,
        verdict=verdict,
        packet=packet,
        runtime_ms=runtime_ms,
    )
    combined.notes["inquiry"] = notes
    combined.notes["execution_engine"] = "inquiry"

    return InquiryResult(
        session_id=session_id,
        route=route,
        retrieval_result=combined,
        hypotheses=hypotheses,
        questions=tuple(all_questions),
        retrieval_actions=tuple(all_actions),
        question_answers=tuple(answers),
        evidence_cards=tuple(evidence_cards),
        sufficiency=verdict,
        context_packet=packet,
        notes=notes,
    )


__all__ = ["_finalize_inquiry_run"]
