"""Adaptive inquiry retrieval runtime.

This module is the active implementation of the proposal's routed
retrieval loop. It keeps the existing retrieval pathways as low-level
executors, but wraps them in the production shape the architecture
calls for: baseline seeding, hypotheses, question planning, evidence
reservoir, sufficiency, and a compact context packet for reasoning.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, Field

from lib.llm.provider import LLMProvider
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import (
    CommitmentRow,
    DecisionRow,
    GoalRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)
from services.domain.models.address import belief_address_from_model_like
from services.domain.models.repo import ModelsRepo
from services.reasoning.retrieval.pathways import (
    PathwayResult,
    RetrievalPathwayError,
    pathway_a_structural,
    pathway_b_semantic,
    pathway_c_temporal,
    pathway_d_pattern,
    pathway_g_model_edges,
)
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext, primary_retrieve
from services.reasoning.synthesis.state_contract import StateSource, compile_state_contract

from .contracts import SignalRoute


InquiryStopStatus = Literal[
    "sufficient_for_reasoning",
    "insufficient_continue",
    "insufficient_defer",
    "human_validation_required",
    "no_update_needed",
    "budget_exhausted",
]

RetrievalActionPath = Literal[
    "structural",
    "semantic",
    "temporal",
    "pattern",
    "model_edge",
    "sage_reader",
]

_BROAD_DISCOVERY_ACTION_PATHS = frozenset({"semantic", "temporal", "pattern"})
_READER_ATTRIBUTION_NONSELECTED_LIMIT_DEFAULT = 16
_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE_DEFAULT = 0.55


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, float(raw))
    except ValueError:
        return default


def _reader_attribution_nonselected_limit() -> int:
    """Operational cap for trace pressure.

    Selected reader decisions are always persisted because the evaluator uses
    them for positive and negative credit. Non-selected high-score decisions are
    useful diagnostics, but at company scale they can dominate storage without
    improving the feedback loop, so deployments can tune this without a code
    deploy.
    """

    return _env_int(
        "SAGE_READER_ATTRIBUTION_NONSELECTED_LIMIT",
        _READER_ATTRIBUTION_NONSELECTED_LIMIT_DEFAULT,
    )


def _reader_attribution_nonselected_min_score() -> float:
    return _env_float(
        "SAGE_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE",
        _READER_ATTRIBUTION_NONSELECTED_MIN_SCORE_DEFAULT,
    )


@dataclass(frozen=True, slots=True)
class InquiryConfig:
    max_rounds: int = 2
    questions_per_round: int = 3
    evidence_reservoir_limit: int = 500
    fast_path_evidence_limit: int = 50
    candidate_model_limit: int = 160
    result_model_limit: int = 64
    action_model_budget_limit: int = 24
    action_observation_budget_limit: int = 24
    relevance_min_score: float = 0.30
    relevance_weak_signal_min_score: float = 0.44
    relevance_broad_signal_min_score: float = 0.24
    relevance_score_cliff: float = 0.18
    relevance_min_material_models: int = 3
    reasoning_packet_token_budget: int = 24000
    temporal_window_days: int = 30
    semantic_budget: int = 30
    structural_max_hops: int = 2
    model_edge_max_hops: int = 2
    llm_question_planning_enabled: bool = True
    llm_question_temperature: float = 0.1
    llm_question_max_tokens: int = 900
    sage_reader_enabled: bool = True
    persist: bool = True

    @classmethod
    def from_env(cls) -> "InquiryConfig":
        return cls(
            max_rounds=int(os.environ.get("INQUIRY_MAX_ROUNDS", "2")),
            questions_per_round=int(os.environ.get("INQUIRY_QUESTIONS_PER_ROUND", "3")),
            evidence_reservoir_limit=int(
                os.environ.get("INQUIRY_EVIDENCE_RESERVOIR_LIMIT", "500")
            ),
            fast_path_evidence_limit=int(
                os.environ.get("INQUIRY_FAST_PATH_EVIDENCE_LIMIT", "50")
            ),
            candidate_model_limit=int(
                os.environ.get("INQUIRY_CANDIDATE_MODEL_LIMIT", "160")
            ),
            result_model_limit=int(
                os.environ.get("INQUIRY_RESULT_MODEL_LIMIT", "64")
            ),
            relevance_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_MIN_SCORE", "0.30")
            ),
            relevance_weak_signal_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_WEAK_SIGNAL_MIN_SCORE", "0.44")
            ),
            relevance_broad_signal_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_BROAD_SIGNAL_MIN_SCORE", "0.24")
            ),
            relevance_score_cliff=float(
                os.environ.get("INQUIRY_RELEVANCE_SCORE_CLIFF", "0.18")
            ),
            relevance_min_material_models=int(
                os.environ.get("INQUIRY_RELEVANCE_MIN_MATERIAL_MODELS", "3")
            ),
            action_model_budget_limit=int(
                os.environ.get("INQUIRY_ACTION_MODEL_BUDGET_LIMIT", "24")
            ),
            action_observation_budget_limit=int(
                os.environ.get("INQUIRY_ACTION_OBSERVATION_BUDGET_LIMIT", "24")
            ),
            reasoning_packet_token_budget=int(
                os.environ.get("INQUIRY_REASONING_PACKET_TOKENS", "24000")
            ),
            temporal_window_days=int(os.environ.get("INQUIRY_TEMPORAL_WINDOW_DAYS", "30")),
            semantic_budget=int(os.environ.get("INQUIRY_SEMANTIC_BUDGET", "30")),
            structural_max_hops=int(os.environ.get("INQUIRY_STRUCTURAL_MAX_HOPS", "2")),
            model_edge_max_hops=int(os.environ.get("INQUIRY_MODEL_EDGE_MAX_HOPS", "2")),
            llm_question_planning_enabled=os.environ.get(
                "INQUIRY_LLM_QUESTION_PLANNING_ENABLED",
                "1",
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off"},
            llm_question_temperature=float(
                os.environ.get("INQUIRY_LLM_QUESTION_TEMPERATURE", "0.1")
            ),
            llm_question_max_tokens=int(
                os.environ.get("INQUIRY_LLM_QUESTION_MAX_TOKENS", "900")
            ),
            sage_reader_enabled=os.environ.get(
                "SAGE_READER_ENABLED", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            persist=os.environ.get("INQUIRY_PERSIST", "1").strip().lower()
            not in {"0", "false", "no", "off"},
        )


class LLMInquiryQuestionSpec(BaseModel):
    primitive: str = Field(
        description=(
            "One of DEPENDENCY, COMMITMENT, CONSTRAINT, COUNTEREVIDENCE, "
            "OWNERSHIP, GOAL_IMPACT, RECURRENCE."
        )
    )
    question: str = Field(
        min_length=8,
        max_length=240,
        description="The concrete retrieval question to ask next.",
    )
    retrieval_target: str | None = Field(
        default=None,
        max_length=120,
        description="Compact target such as active_commitments or pattern+model_edges.",
    )
    expected_value: float = Field(ge=0.0, le=1.0)
    expected_cost: float = Field(ge=0.0, le=1.0)
    tests_hypotheses: list[str] = Field(default_factory=list, max_length=4)
    stop_condition: str | None = Field(default=None, max_length=180)


class LLMInquiryQuestionPlan(BaseModel):
    rationale: str | None = Field(default=None, max_length=500)
    questions: list[LLMInquiryQuestionSpec] = Field(default_factory=list, max_length=6)


@dataclass(frozen=True, slots=True)
class Hypothesis:
    id: str
    claim: str
    confidence: float
    impact_if_true: str


@dataclass(frozen=True, slots=True)
class InquiryQuestion:
    question_id: str
    question: str
    primitive: str
    tests_hypotheses: tuple[str, ...]
    expected_value: float
    expected_cost: float
    retrieval_target: str
    stop_condition: str
    score: float
    round_index: int = 0


@dataclass(frozen=True, slots=True)
class RetrievalAction:
    question_id: str
    path: RetrievalActionPath
    target: str
    query: str | None = None
    filters: dict[str, Any] = field(default_factory=dict)
    budget: int = 25


@dataclass(frozen=True, slots=True)
class QuestionPolicySignal:
    signal_type: str
    question_primitive: str
    attempts: int
    successes: int
    utility_score: float
    total_credit: float
    total_cost: float


@dataclass(slots=True)
class EvidenceCard:
    evidence_id: UUID
    source_type: str
    source_ref: str
    source_ref_id: UUID | None
    summary: str
    trust_tier: str | None
    timestamp: datetime | None
    retrieval_paths: set[str] = field(default_factory=set)
    retrieved_for_questions: set[str] = field(default_factory=set)
    supports_hypotheses: set[str] = field(default_factory=set)
    weakens_hypotheses: set[str] = field(default_factory=set)
    contradicts_hypotheses: set[str] = field(default_factory=set)
    raw_content_ref: str | None = None
    token_estimate: int = 1
    access_scope: str = "tenant"
    sensitivity: str = "normal"
    score: float = 0.0

    def merge(
        self,
        *,
        path: str,
        question_id: str,
        supports: set[str] | None = None,
        weakens: set[str] | None = None,
        contradicts: set[str] | None = None,
        score: float = 0.0,
    ) -> None:
        self.retrieval_paths.add(path)
        self.retrieved_for_questions.add(question_id)
        self.supports_hypotheses.update(supports or set())
        self.weakens_hypotheses.update(weakens or set())
        self.contradicts_hypotheses.update(contradicts or set())
        self.score = max(self.score, float(score))


@dataclass(frozen=True, slots=True)
class QuestionAnswer:
    question_id: str
    answer_status: str
    summary: str
    supporting_evidence: tuple[str, ...] = ()
    counterevidence: tuple[str, ...] = ()
    new_uncertainties: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SufficiencyVerdict:
    status: InquiryStopStatus
    reason: str
    evidence_count: int
    answered_questions: int
    remaining_unknowns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class InquiryResult:
    session_id: UUID
    route: SignalRoute
    retrieval_result: RetrievalResult
    hypotheses: tuple[Hypothesis, ...]
    questions: tuple[InquiryQuestion, ...]
    retrieval_actions: tuple[RetrievalAction, ...]
    question_answers: tuple[QuestionAnswer, ...]
    evidence_cards: tuple[EvidenceCard, ...]
    sufficiency: SufficiencyVerdict
    context_packet: dict[str, Any]
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelRelevance:
    model_id: UUID
    final_score: float
    base_score: float
    lexical_score: float
    scope_score: float
    path_score: float
    evidence_score: float
    provenance_score: float
    penalty: float
    reasons: tuple[str, ...]


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
    route: SignalRoute | None = None,
    mode: Literal["deep", "fast"] = "deep",
    top_n: int = 80,
    config: InquiryConfig | None = None,
) -> InquiryResult | RetrievalResult:
    """Return the active retrieval result for Think/query callers.

    `EXECUTION_RETRIEVAL_ENGINE=legacy` gives an operator rollback path.
    The default is the new inquiry runtime.
    """
    if not inquiry_enabled():
        return await primary_retrieve(trigger, conn, embedder=embedder, top_n=top_n)
    return await run_inquiry_retrieval(
        trigger,
        conn,
        embedder=embedder,
        llm_provider=llm_provider,
        route=route,
        mode=mode,
        top_n=top_n,
        config=config,
    )


async def run_inquiry_retrieval(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    embedder: Any | None = None,
    llm_provider: LLMProvider | None = None,
    route: SignalRoute | None = None,
    mode: Literal["deep", "fast"] = "deep",
    top_n: int = 80,
    config: InquiryConfig | None = None,
) -> InquiryResult:
    cfg = config or InquiryConfig.from_env()
    route = route or ("FAST_PATH" if mode == "fast" else _route_for_trigger(trigger))
    session_id = uuid7()
    candidate_top_n = min(top_n, max(1, int(cfg.candidate_model_limit)))
    effective_top_n = min(candidate_top_n, max(1, int(cfg.result_model_limit)))
    signal_class = _signal_class_for_trigger(trigger)
    weak_signal = signal_class == "weak"
    cold_weak_noop_gate = _cold_weak_noop_gate(trigger, signal_class)
    baseline_top_n = _adaptive_baseline_top_n(candidate_top_n, signal_class)

    if cold_weak_noop_gate["used"]:
        baseline = _merge_results(
            trigger,
            [],
            top_n=0,
            note_prefix="cold_weak_noop",
            config=cfg,
        )
    else:
        baseline = await primary_retrieve(
            trigger,
            conn,
            embedder=embedder,
            top_n=baseline_top_n,
        )
    hypotheses = tuple(_generate_hypotheses(trigger, baseline))
    evidence_by_key: dict[tuple[str, str], EvidenceCard] = {}
    _add_result_to_reservoir(
        evidence_by_key,
        baseline,
        path="baseline",
        question_id="Q0",
        hypotheses=hypotheses,
    )

    all_questions: list[InquiryQuestion] = []
    all_actions: list[RetrievalAction] = []
    action_cache: dict[tuple[Any, ...], PathwayResult] = {}
    baseline_action_cache_notes = _seed_action_cache_from_baseline(
        action_cache,
        baseline,
        trigger,
        cfg,
    )
    action_timing_notes: list[dict[str, Any]] = []
    answers: list[QuestionAnswer] = []
    retrieval_results = [baseline]
    unknowns: set[str] = set(_initial_unknowns(trigger, baseline))
    question_planning_notes: list[dict[str, Any]] = []
    question_policy = await _load_question_policy_stats(
        conn,
        tenant_id=trigger.tenant_id,
        signal_type=trigger.kind,
    )
    sage_reader_notes: dict[str, Any] = {
        "enabled": bool(cfg.sage_reader_enabled),
        "questions": {},
        "signatures": [],
        "selected_model_ids": [],
        "projected_evidence_count": 0,
        "activation_trace_count": 0,
    }
    max_rounds = (
        0
        if (
            cold_weak_noop_gate["used"]
            or mode == "fast"
            or route in {"FAST_PATH", "HUMAN_VALIDATION_PATH"}
        )
        else cfg.max_rounds
    )
    if weak_signal and max_rounds > 1:
        max_rounds = 1
    stop_status: InquiryStopStatus = "insufficient_continue"
    stop_reason = "inquiry has not run"

    for round_index in range(1, max_rounds + 1):
        candidate_questions, planning_note = await _candidate_questions_for_round(
            trigger,
            baseline,
            hypotheses,
            evidence_by_key,
            unknowns,
            llm_provider=llm_provider,
            config=cfg,
            round_index=round_index,
        )
        candidate_questions = _apply_question_policy(
            candidate_questions,
            question_policy=question_policy,
        )
        if question_policy:
            planning_note["policy_stats_applied"] = {
                primitive: {
                    "utility_score": round(signal.utility_score, 4),
                    "attempts": signal.attempts,
                    "successes": signal.successes,
                }
                for primitive, signal in sorted(question_policy.items())
            }
        question_planning_notes.append(planning_note)
        selected = _select_questions(
            candidate_questions,
            questions_per_round=(
                min(cfg.questions_per_round, 2)
                if weak_signal else cfg.questions_per_round
            ),
            round_index=round_index,
            already_asked={q.primitive for q in all_questions},
        )
        if not selected:
            stop_status = "insufficient_defer"
            stop_reason = "no high-value unanswered questions remained"
            break

        for question in selected:
            all_questions.append(question)
            policy_signal = question_policy.get(question.primitive)
            actions = _compile_retrieval_plan(
                question,
                trigger,
                cfg,
                policy_signal=policy_signal,
            )
            action_results: list[RetrievalResult] = []
            sage_result = await _execute_sage_reader_action(
                question,
                trigger,
                conn,
                cfg,
                hypotheses=hypotheses,
            )
            action_gate_scope: str | None = None
            action_gate_reason: str | None = None
            if sage_result is not None:
                action_timing_notes.append({
                    "question_id": question.question_id,
                    "path": "sage_reader",
                    "target": "synthesis_reader",
                    "elapsed_ms": _sage_reader_total_ms(sage_result),
                    "cache_hit": False,
                    "returned": True,
                    "models": len(sage_result.models),
                    "observations": len(sage_result.observations),
                    "resources": len(sage_result.resources),
                    "source_pathway": "SAGE",
                })
                sage_action = RetrievalAction(
                    question.question_id,
                    "sage_reader",
                    "synthesis_reader",
                    query=question.question,
                    budget=cfg.result_model_limit,
                )
                all_actions.append(sage_action)
                action_results.append(sage_result)
                _add_result_to_reservoir(
                    evidence_by_key,
                    sage_result,
                    path="sage_reader",
                    question_id=question.question_id,
                    hypotheses=hypotheses,
                    score_hint=max(0.0, question.score),
                )
                _record_sage_reader_notes(
                    sage_reader_notes, question, sage_result,
                )
                action_gate_scope, action_gate_reason = _sage_reader_action_gate(
                    sage_result
                )

            actions_to_run: list[RetrievalAction] = []
            for action in actions:
                skip_reason: str | None = None
                if action_gate_scope == "all":
                    skip_reason = action_gate_reason or "sage_reader_abstained"
                elif (
                    action_gate_scope == "broad"
                    and action.path in _BROAD_DISCOVERY_ACTION_PATHS
                ):
                    skip_reason = action_gate_reason or "sage_reader_focused_route"
                if skip_reason is not None:
                    action_timing_notes.append({
                        "question_id": question.question_id,
                        "path": action.path,
                        "target": action.target,
                        "elapsed_ms": 0,
                        "cache_hit": False,
                        "returned": False,
                        "skipped": True,
                        "skip_reason": skip_reason,
                    })
                    continue
                actions_to_run.append(action)

            all_actions.extend(actions_to_run)
            for action in actions_to_run:
                cache_key = _retrieval_action_cache_key(action, trigger, cfg)
                path_result = action_cache.get(cache_key)
                cache_hit = path_result is not None
                action_started = time.perf_counter()
                if path_result is None:
                    path_result = await _execute_action(action, trigger, conn, embedder, cfg)
                    if path_result is not None:
                        action_cache[cache_key] = path_result
                elapsed_ms = int((time.perf_counter() - action_started) * 1000)
                if path_result is None:
                    action_timing_notes.append({
                        "question_id": question.question_id,
                        "path": action.path,
                        "target": action.target,
                        "elapsed_ms": elapsed_ms,
                        "cache_hit": cache_hit,
                        "returned": False,
                    })
                    continue
                action_timing_notes.append({
                    "question_id": question.question_id,
                    "path": action.path,
                    "target": action.target,
                    "elapsed_ms": elapsed_ms,
                    "cache_hit": cache_hit,
                    "returned": True,
                    "models": len(path_result.models),
                    "observations": len(path_result.observations),
                    "resources": len(path_result.resources),
                    "source_pathway": path_result.source_pathway,
                })
                rr = _result_from_pathway(trigger, path_result, action)
                action_results.append(rr)
                _add_result_to_reservoir(
                    evidence_by_key,
                    rr,
                    path=action.path,
                    question_id=question.question_id,
                    hypotheses=hypotheses,
                    score_hint=max(0.0, question.score),
                )
            if action_results:
                merged_for_question = _merge_results(
                    trigger,
                    action_results,
                    top_n=candidate_top_n,
                    note_prefix=f"question_{question.question_id}",
                )
                retrieval_results.append(merged_for_question)
            answer = _answer_question(
                question,
                evidence_by_key,
                trigger_occurred_at=trigger.seed_occurred_at,
                stale_after_days=cfg.temporal_window_days,
            )
            answers.append(answer)
            unknowns.difference_update(_resolved_unknowns_for_answer(question, answer))
            unknowns.update(answer.new_uncertainties)
            interim_verdict = _sufficiency_gate(
                route,
                hypotheses,
                list(evidence_by_key.values()),
                answers,
                round_index=round_index,
                max_rounds=max_rounds,
                unknowns=unknowns,
            )
            if interim_verdict.status == "sufficient_for_reasoning":
                stop_status = interim_verdict.status
                stop_reason = interim_verdict.reason
                break

        verdict = _sufficiency_gate(
            route,
            hypotheses,
            list(evidence_by_key.values()),
            answers,
            round_index=round_index,
            max_rounds=max_rounds,
            unknowns=unknowns,
        )
        stop_status = verdict.status
        stop_reason = verdict.reason
        if verdict.status != "insufficient_continue":
            break

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
    if cold_weak_noop_gate["used"] or sage_controller_notes["global_negative_route_gate"]:
        ranked_evidence_cards = []
    else:
        ranked_evidence_cards = _rank_evidence(
            list(evidence_by_key.values()),
            limit=evidence_limit,
        )
    if max_rounds == 0:
        verdict = _sufficiency_gate(
            route,
            hypotheses,
            ranked_evidence_cards,
            answers,
            round_index=0,
            max_rounds=0,
            unknowns=unknowns,
        )
    elif cold_weak_noop_gate["used"]:
        verdict = SufficiencyVerdict(
            status="no_update_needed",
            reason=str(cold_weak_noop_gate["reason"]),
            evidence_count=0,
            answered_questions=0,
            remaining_unknowns=(),
        )
    elif sage_controller_notes["global_negative_route_gate"]:
        verdict = SufficiencyVerdict(
            status="no_update_needed",
            reason=(
                "sage reader learned this route as negative and every "
                "selected question abstained"
            ),
            evidence_count=0,
            answered_questions=0,
            remaining_unknowns=tuple(sorted(unknowns)[:10]),
        )
    else:
        verdict = SufficiencyVerdict(
            status=stop_status,
            reason=stop_reason,
            evidence_count=len(ranked_evidence_cards),
            answered_questions=len(answers),
            remaining_unknowns=tuple(sorted(unknowns)[:10]),
        )
    evidence_cards, evidence_minimization = _select_minimal_sufficient_evidence(
        ranked_evidence_cards,
        hypotheses=hypotheses,
        questions=all_questions,
        answers=answers,
        route=route,
        mode=mode,
        evidence_limit=evidence_limit,
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
    combined = _merge_results(
        trigger,
        retrieval_results_for_merge,
        top_n=effective_top_n,
        note_prefix="inquiry",
        config=cfg,
        relevance_gate=True,
    )
    packet = _compile_context_packet(
        trigger,
        route,
        hypotheses,
        all_questions,
        answers,
        evidence_cards,
        verdict,
        token_budget=cfg.reasoning_packet_token_budget,
    )
    notes = {
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
        "cold_weak_noop_gate": cold_weak_noop_gate,
        "retrieval_action_timings": action_timing_notes,
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
    combined.notes["inquiry"] = notes
    combined.notes["execution_engine"] = "inquiry"

    result = InquiryResult(
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
    if cfg.persist:
        await _persist_inquiry(conn, result, trigger)
    return result


def _route_for_trigger(trigger: TriggerContext) -> SignalRoute:
    if trigger.kind == "T2":
        return "DETERMINISTIC_UPDATE"
    if trigger.kind == "T3":
        return "BACKGROUND_PATH"
    if trigger.kind == "T4":
        return "BACKGROUND_PATH"
    return "DEEP_INQUIRY_PATH"


def _signal_class_for_trigger(trigger: TriggerContext) -> str:
    lower = _trigger_text(trigger).casefold()
    if trigger.kind == "T1" and _declares_no_material_update(lower):
        return "weak"
    if _has_broad_signal_language(lower):
        return "broad"
    if trigger.kind == "T1" and not _signal_has_material_update_intent(lower):
        return "weak"
    return "material"


def _cold_weak_noop_gate(
    trigger: TriggerContext,
    signal_class: str,
) -> dict[str, Any]:
    if signal_class != "weak":
        return {"used": False, "reason": "not_weak_signal"}
    lower = _trigger_text(trigger).casefold()
    if not _declares_no_material_update(lower):
        return {"used": False, "reason": "weak_signal_needs_disambiguation"}
    return {
        "used": True,
        "reason": (
            "weak signal is non-actionable workspace chatter or explicitly "
            "declares no material update"
        ),
    }


def _declares_no_material_update(lower: str) -> bool:
    grouped_no_update_phrases = (
        "no blocker, owner change, decision, customer risk, or commitment update",
        "no blocker, no owner change, no decision, no customer risk, or no commitment update",
    )
    no_update_phrases = (
        "no blocker",
        "no owner change",
        "no decision",
        "no customer risk",
        "no commitment update",
        "no risk",
        "no action",
        "no actionable",
    )
    weak_chatter_phrases = (
        "workspace chatter",
        "weak workspace noise",
        "lunch notes",
        "travel plans",
        "general team coordination",
    )
    no_update_count = sum(1 for phrase in no_update_phrases if phrase in lower)
    chatter_count = sum(1 for phrase in weak_chatter_phrases if phrase in lower)
    grouped_declared = any(phrase in lower for phrase in grouped_no_update_phrases)
    if chatter_count >= 3:
        return True
    return (grouped_declared or no_update_count >= 2) and chatter_count >= 1


def _adaptive_baseline_top_n(candidate_top_n: int, signal_class: str) -> int:
    if signal_class == "weak":
        return min(candidate_top_n, 80)
    if signal_class == "broad":
        return min(candidate_top_n, 220)
    return min(candidate_top_n, 150)


def _adaptive_evidence_limit(
    cfg: InquiryConfig,
    *,
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    signal_class: str,
) -> int:
    if mode == "fast" or route == "FAST_PATH" or signal_class == "weak":
        return max(1, int(cfg.fast_path_evidence_limit))
    configured = max(1, int(cfg.evidence_reservoir_limit))
    if signal_class == "broad":
        return min(configured, max(320, min(560, configured)))
    return min(configured, max(160, min(360, configured)))


def _trigger_text(trigger: TriggerContext) -> str:
    return (trigger.seed_natural_text or "").strip()


def _generate_hypotheses(
    trigger: TriggerContext,
    baseline: RetrievalResult,
) -> list[Hypothesis]:
    text = _trigger_text(trigger)
    lower = text.casefold()
    hypotheses: list[Hypothesis] = []
    risk = _has_risk_language(lower)
    commitment = _has_commitment_language(lower) or bool(
        baseline.acts.get("commitments")
    )
    if risk:
        hypotheses.append(
            Hypothesis(
                id="H1",
                claim=_claim_from_text(
                    text,
                    fallback="The signal describes a real operational blocker or risk.",
                ),
                confidence=0.46,
                impact_if_true="high",
            )
        )
    if commitment:
        hypotheses.append(
            Hypothesis(
                id="H2",
                claim="An active commitment, owner, or promised outcome is affected.",
                confidence=0.36,
                impact_if_true="medium",
            )
        )
    if _mentions_recurrence(lower) or len(baseline.models) >= 3:
        hypotheses.append(
            Hypothesis(
                id="H3",
                claim="The signal may be part of a broader recurring pattern.",
                confidence=0.29,
                impact_if_true="high" if risk else "medium",
            )
        )
    if not hypotheses:
        hypotheses.append(
            Hypothesis(
                id="H1",
                claim="The signal may add localized context to existing memory.",
                confidence=0.30,
                impact_if_true="medium",
            )
        )
    hypotheses.append(
        Hypothesis(
            id="H0",
            claim="The signal is local noise or already captured and requires no Synthesis update.",
            confidence=0.16 if risk or commitment else 0.32,
            impact_if_true="low",
        )
    )
    return hypotheses


def _claim_from_text(text: str, *, fallback: str) -> str:
    clean = re.sub(r"\s+", " ", text or "").strip()
    if not clean:
        return fallback
    if len(clean) <= 140:
        return clean
    return clean[:137].rstrip() + "..."


def _initial_unknowns(trigger: TriggerContext, baseline: RetrievalResult) -> list[str]:
    unknowns: list[str] = []
    lower = _trigger_text(trigger).casefold()
    if _has_risk_language(lower):
        unknowns.append("whether the blocker is on the critical path")
    if _has_dependency_language(lower):
        unknowns.append("whether the dependency is binding")
    if _has_constraint_language(lower):
        unknowns.append("blocking constraint")
    if not baseline.acts.get("commitments"):
        unknowns.append("affected commitment")
    if not baseline.acts.get("goals"):
        unknowns.append("affected goal")
    if "owner" in lower or "who" in lower or _has_risk_language(lower):
        unknowns.append("responsible owner")
    if _mentions_recurrence(lower):
        unknowns.append("whether this is part of a broader recurring pattern")
    unknowns.append("counterevidence")
    return unknowns


def _candidate_questions(
    trigger: TriggerContext,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
) -> list[InquiryQuestion]:
    text = _trigger_text(trigger)
    lower = text.casefold()
    broad = _has_broad_signal_language(lower)
    hids = tuple(h.id for h in hypotheses if h.id != "H0")
    out = [
        InquiryQuestion(
            question_id="Q_CRITICAL_PATH",
            question="Is the named issue actually on the critical path?",
            primitive="DEPENDENCY",
            tests_hypotheses=hids[:2] or ("H1",),
            expected_value=(
                0.60
                if broad else (
                    0.90
                    if _has_risk_language(lower) or _has_dependency_language(lower)
                    else 0.55
                )
            ),
            expected_cost=0.24,
            retrieval_target="commitment_graph+recent_observations",
            stop_condition="critical-path evidence or counterevidence found",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_ACTIVE_COMMITMENT",
            question="Is there an active commitment or promised outcome involved?",
            primitive="COMMITMENT",
            tests_hypotheses=("H2", "H0"),
            expected_value=(
                0.84 if broad else (0.78 if "affected commitment" in unknowns else 0.42)
            ),
            expected_cost=0.18,
            retrieval_target="active_commitments",
            stop_condition="matching active commitment found or ruled out",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_CONSTRAINT",
            question="What scarce resource, policy, or constraint is blocking progress?",
            primitive="CONSTRAINT",
            tests_hypotheses=hids[:2] or ("H1",),
            expected_value=(
                0.90
                if _has_constraint_language(lower)
                else (0.76 if _has_dependency_language(lower) else 0.40)
            ),
            expected_cost=0.24,
            retrieval_target="constraints+resource_edges",
            stop_condition="binding constraint identified or ruled out",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_COUNTEREVIDENCE",
            question="What evidence would weaken the leading interpretation?",
            primitive="COUNTEREVIDENCE",
            tests_hypotheses=("H1", "H0"),
            expected_value=0.84 if _has_risk_language(lower) else 0.74,
            expected_cost=0.30,
            retrieval_target="semantic_counterevidence+recent_observations",
            stop_condition="credible alternate explanation found or absent",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_OWNER",
            question="Who owns the affected dependency, decision, or commitment?",
            primitive="OWNERSHIP",
            tests_hypotheses=("H2",),
            expected_value=0.72 if "responsible owner" in unknowns else 0.36,
            expected_cost=0.22,
            retrieval_target="commitment_owners+actor_scope",
            stop_condition="owner identified or human validation required",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_GOAL_IMPACT",
            question="Which goal, customer, or resource is affected?",
            primitive="GOAL_IMPACT",
            tests_hypotheses=hids[:3] or ("H1",),
            expected_value=(
                0.94
                if broad else (
                    0.86
                    if _has_revenue_impact_language(lower)
                    else (0.68 if "affected goal" in unknowns else 0.38)
                )
            ),
            expected_cost=0.20,
            retrieval_target="goal_resource_bridge",
            stop_condition="goal/customer/resource impact identified",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_RECURRENCE",
            question="Is this part of a broader recurring pattern?",
            primitive="RECURRENCE",
            tests_hypotheses=("H3", "H0"),
            expected_value=(
                0.80 if broad else (0.92 if _mentions_recurrence(lower) else 0.44)
            ),
            expected_cost=0.36,
            retrieval_target="pattern+model_edges",
            stop_condition="pattern support or absence established",
            score=0.0,
        ),
    ]
    if len(evidence_by_key) < 5:
        for q in out:
            q_score = q.expected_value - q.expected_cost + 0.15
            object.__setattr__(q, "score", round(q_score, 4))
    else:
        for q in out:
            q_score = q.expected_value - q.expected_cost
            object.__setattr__(q, "score", round(q_score, 4))
    return out


_ALLOWED_QUESTION_PRIMITIVES = {
    "DEPENDENCY",
    "COMMITMENT",
    "CONSTRAINT",
    "COUNTEREVIDENCE",
    "OWNERSHIP",
    "GOAL_IMPACT",
    "RECURRENCE",
}

_QUESTION_ID_BY_PRIMITIVE = {
    "DEPENDENCY": "Q_CRITICAL_PATH",
    "COMMITMENT": "Q_ACTIVE_COMMITMENT",
    "CONSTRAINT": "Q_CONSTRAINT",
    "COUNTEREVIDENCE": "Q_COUNTEREVIDENCE",
    "OWNERSHIP": "Q_OWNER",
    "GOAL_IMPACT": "Q_GOAL_IMPACT",
    "RECURRENCE": "Q_RECURRENCE",
}

_DEFAULT_TARGET_BY_PRIMITIVE = {
    "DEPENDENCY": "commitment_graph+recent_observations",
    "COMMITMENT": "active_commitments",
    "CONSTRAINT": "constraints+resource_edges",
    "COUNTEREVIDENCE": "semantic_counterevidence+recent_observations",
    "OWNERSHIP": "commitment_owners+actor_scope",
    "GOAL_IMPACT": "goal_resource_bridge",
    "RECURRENCE": "pattern+model_edges",
}

_DEFAULT_STOP_BY_PRIMITIVE = {
    "DEPENDENCY": "critical-path evidence or counterevidence found",
    "COMMITMENT": "matching active commitment found or ruled out",
    "CONSTRAINT": "binding constraint identified or ruled out",
    "COUNTEREVIDENCE": "credible alternate explanation found or absent",
    "OWNERSHIP": "owner identified or human validation required",
    "GOAL_IMPACT": "goal/customer/resource impact identified",
    "RECURRENCE": "pattern support or absence established",
}

_QUESTION_MARGINAL_MIN_SCORE = 0.52
_QUESTION_PRIORITY_MARGINAL_MIN_SCORE = 0.46


async def _candidate_questions_for_round(
    trigger: TriggerContext,
    baseline: RetrievalResult,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
    *,
    llm_provider: LLMProvider | None,
    config: InquiryConfig,
    round_index: int,
) -> tuple[list[InquiryQuestion], dict[str, Any]]:
    deterministic = _candidate_questions(trigger, hypotheses, evidence_by_key, unknowns)
    if trigger.kind != "T1":
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "non_t1_trigger_uses_seeded_retrieval",
            "candidate_count": len(deterministic),
        }
    if not config.llm_question_planning_enabled:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "disabled_by_config",
            "candidate_count": len(deterministic),
        }
    if llm_provider is None:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "llm_provider_missing",
            "candidate_count": len(deterministic),
        }

    try:
        plan_call = _generate_llm_question_plan(
            trigger,
            baseline,
            hypotheses,
            evidence_by_key,
            unknowns,
            llm_provider=llm_provider,
            config=config,
        )
        timeout_s = float(
            os.environ.get("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS", "30")
        )
        if timeout_s > 0:
            plan = await asyncio.wait_for(plan_call, timeout=timeout_s)
        else:
            plan = await plan_call
        llm_questions = _normalize_llm_questions(plan.questions, hypotheses)
        if not llm_questions:
            return deterministic, {
                "round": round_index,
                "mode": "deterministic_fallback",
                "reason": "llm_returned_no_valid_questions",
                "candidate_count": len(deterministic),
                "llm_rationale": plan.rationale,
            }
        merged, safety_added = _merge_llm_and_safety_questions(
            llm_questions,
            deterministic,
        )
        return merged, {
            "round": round_index,
            "mode": "llm",
            "llm_candidate_count": len(llm_questions),
            "safety_candidate_count": safety_added,
            "candidate_count": len(merged),
            "llm_rationale": plan.rationale,
            "llm_primitives": [q.primitive for q in llm_questions],
        }
    except Exception as exc:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": type(exc).__name__,
            "detail": str(exc)[:240],
            "candidate_count": len(deterministic),
        }


async def _generate_llm_question_plan(
    trigger: TriggerContext,
    baseline: RetrievalResult,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
    *,
    llm_provider: LLMProvider,
    config: InquiryConfig,
) -> LLMInquiryQuestionPlan:
    system = (
        "You plan retrieval questions for Fyralis' model-update pipeline. "
        "Choose only the few questions that will decide what existing models "
        "must be updated or whether a new model should be created. Prefer "
        "specific, discriminating questions over broad searches. Always include "
        "counterevidence when the signal makes a material claim. Return JSON only."
    )
    user = json.dumps(
        {
            "task": "Generate the next retrieval questions for this signal.",
            "allowed_primitives": sorted(_ALLOWED_QUESTION_PRIMITIVES),
            "signal": {
                "kind": trigger.kind,
                "text": _trigger_text(trigger),
                "seed_entities": trigger.seed_entity_ids[:12],
                "scope_actor_count": len(trigger.scope_actors),
                "occurred_at": (
                    trigger.seed_occurred_at.isoformat()
                    if trigger.seed_occurred_at
                    else None
                ),
            },
            "hypotheses": [
                {
                    "id": h.id,
                    "claim": h.claim,
                    "confidence": h.confidence,
                    "impact_if_true": h.impact_if_true,
                }
                for h in hypotheses
            ],
            "unknowns": sorted(unknowns)[:12],
            "baseline_snapshot": _baseline_snapshot_for_question_planning(
                baseline,
                evidence_by_key,
            ),
            "guidance": [
                "Return 2 to 5 questions.",
                "Use primitive names exactly as provided.",
                "Use expected_value for likely decision value, not topicality.",
                "Use expected_cost for retrieval breadth/cost; broad searches cost more.",
                "Avoid questions whose answer is already clear from baseline evidence.",
                "For weak chatter/no-op signals, ask narrow disambiguation and counterevidence questions only.",
            ],
        },
        default=str,
    )
    return await llm_provider.structured(
        system=system,
        user=user,
        schema=LLMInquiryQuestionPlan,
        temperature=config.llm_question_temperature,
        max_tokens=config.llm_question_max_tokens,
    )


def _baseline_snapshot_for_question_planning(
    baseline: RetrievalResult,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
) -> dict[str, Any]:
    cards = sorted(
        evidence_by_key.values(),
        key=lambda c: (-float(c.score), c.source_type, c.summary),
    )
    return {
        "model_count": len(baseline.models),
        "observation_count": len(baseline.observations),
        "commitment_count": len(baseline.acts.get("commitments", [])),
        "goal_count": len(baseline.acts.get("goals", [])),
        "decision_count": len(baseline.acts.get("decisions", [])),
        "top_models": [
            {
                "id": str(model.id),
                "summary": _truncate_text(
                    getattr(model, "natural", "") or json.dumps(
                        getattr(model, "proposition", {}) or {},
                        default=str,
                    ),
                    220,
                ),
                "confidence": getattr(model, "confidence", None),
                "score": float(baseline.model_scores.get(model.id, 0.0)),
            }
            for model in baseline.models[:10]
        ],
        "top_evidence": [
            {
                "source_type": card.source_type,
                "summary": _truncate_text(card.summary, 220),
                "score": round(float(card.score), 4),
            }
            for card in cards[:12]
        ],
    }


def _normalize_llm_questions(
    specs: list[LLMInquiryQuestionSpec],
    hypotheses: tuple[Hypothesis, ...],
) -> list[InquiryQuestion]:
    hypothesis_ids = {h.id for h in hypotheses}
    fallback_hids = tuple(h.id for h in hypotheses if h.id != "H0")[:2] or ("H1",)
    out: list[InquiryQuestion] = []
    seen_primitives: set[str] = set()
    for spec in specs:
        primitive = spec.primitive.strip().upper()
        if primitive not in _ALLOWED_QUESTION_PRIMITIVES:
            continue
        if primitive in seen_primitives:
            continue
        question = " ".join(spec.question.split())
        if len(question) < 8:
            continue
        expected_value = _clamp_float(spec.expected_value, 0.0, 1.0)
        expected_cost = _clamp_float(spec.expected_cost, 0.0, 1.0)
        tests = tuple(
            hid
            for hid in spec.tests_hypotheses
            if isinstance(hid, str) and hid in hypothesis_ids
        )[:4]
        if not tests:
            tests = fallback_hids
        score = round(expected_value - expected_cost + 0.12, 4)
        out.append(
            InquiryQuestion(
                question_id=_QUESTION_ID_BY_PRIMITIVE[primitive],
                question=question[:240],
                primitive=primitive,
                tests_hypotheses=tests,
                expected_value=expected_value,
                expected_cost=expected_cost,
                retrieval_target=(
                    " ".join((spec.retrieval_target or "").split())[:120]
                    or _DEFAULT_TARGET_BY_PRIMITIVE[primitive]
                ),
                stop_condition=(
                    " ".join((spec.stop_condition or "").split())[:180]
                    or _DEFAULT_STOP_BY_PRIMITIVE[primitive]
                ),
                score=score,
            )
        )
        seen_primitives.add(primitive)
    return out


def _merge_llm_and_safety_questions(
    llm_questions: list[InquiryQuestion],
    deterministic: list[InquiryQuestion],
) -> tuple[list[InquiryQuestion], int]:
    by_primitive = {q.primitive: q for q in llm_questions}
    safety_added = 0
    for q in deterministic:
        existing = by_primitive.get(q.primitive)
        if existing is not None:
            if q.score > existing.score or q.expected_value > existing.expected_value:
                by_primitive[q.primitive] = replace(
                    existing,
                    expected_value=max(existing.expected_value, q.expected_value),
                    expected_cost=min(existing.expected_cost, q.expected_cost),
                    tests_hypotheses=(
                        existing.tests_hypotheses or q.tests_hypotheses
                    ),
                    score=max(existing.score, q.score),
                )
            continue
        force_high_value_safety = (
            q.primitive in {"CONSTRAINT", "DEPENDENCY", "GOAL_IMPACT", "RECURRENCE"}
            and (q.expected_value >= 0.86 or q.score >= 0.75)
        )
        if (
            q.primitive == "COUNTEREVIDENCE"
            or len(by_primitive) < 4
            or force_high_value_safety
        ):
            by_primitive[q.primitive] = q
            safety_added += 1
    ordered: list[InquiryQuestion] = []
    for primitive in (
        "COUNTEREVIDENCE",
        "DEPENDENCY",
        "CONSTRAINT",
        "COMMITMENT",
        "OWNERSHIP",
        "GOAL_IMPACT",
        "RECURRENCE",
    ):
        question = by_primitive.get(primitive)
        if question is not None:
            ordered.append(question)
    return ordered, safety_added


def _clamp_float(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _question_marginal_score(
    question: InquiryQuestion,
    selected: list[InquiryQuestion],
) -> float:
    score = float(question.score)
    if not selected:
        return round(score, 4)

    selected_hypotheses = {
        hypothesis
        for prior in selected
        for hypothesis in prior.tests_hypotheses
    }
    shared_hypotheses = set(question.tests_hypotheses) & selected_hypotheses
    if shared_hypotheses:
        score -= min(0.16, 0.07 * len(shared_hypotheses))

    selected_facets = {
        facet
        for prior in selected
        for facet in _question_information_facets(prior)
    }
    shared_facets = _question_information_facets(question) & selected_facets
    if shared_facets:
        score -= min(0.18, 0.08 * len(shared_facets))

    target_overlap = _question_target_overlap(question, selected)
    if target_overlap >= 0.50:
        score -= 0.12
    elif target_overlap >= 0.25:
        score -= 0.06

    score -= min(0.12, max(0.0, question.expected_cost - 0.22) * 0.35)
    if question.primitive == "COUNTEREVIDENCE":
        score += 0.06
    return round(score, 4)


def _question_information_facets(question: InquiryQuestion) -> set[str]:
    primitive = question.primitive
    if primitive == "DEPENDENCY":
        return {"critical_path", "dependency"}
    if primitive == "COMMITMENT":
        return {"commitment", "promise"}
    if primitive == "CONSTRAINT":
        return {"constraint", "resource", "dependency"}
    if primitive == "COUNTEREVIDENCE":
        return {"counterevidence", "falsification"}
    if primitive == "OWNERSHIP":
        return {"ownership", "actor"}
    if primitive == "GOAL_IMPACT":
        return {"goal", "impact", "customer"}
    if primitive == "RECURRENCE":
        return {"recurrence", "pattern"}
    return {primitive.casefold()}


def _question_target_overlap(
    question: InquiryQuestion,
    selected: list[InquiryQuestion],
) -> float:
    tokens = _question_target_tokens(question)
    if not tokens:
        return 0.0
    max_overlap = 0.0
    for prior in selected:
        prior_tokens = _question_target_tokens(prior)
        if not prior_tokens:
            continue
        overlap = len(tokens & prior_tokens) / max(len(tokens), 1)
        max_overlap = max(max_overlap, overlap)
    return max_overlap


def _question_target_tokens(question: InquiryQuestion) -> set[str]:
    text = f"{question.retrieval_target} {question.stop_condition}".casefold()
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", text)
        if len(token) > 2
        and token not in {"and", "the", "for", "with", "found", "ruled"}
    }


def _truncate_text(text: str, limit: int) -> str:
    clean = " ".join((text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


async def _load_question_policy_stats(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signal_type: str,
) -> dict[str, QuestionPolicySignal]:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_question_policy_stats')"
    )
    if table_name is None:
        return {}
    rows = await conn.fetch(
        """
        SELECT signal_type, question_primitive, attempts, successes,
               utility_score, total_credit, total_cost
        FROM sage_question_policy_stats
        WHERE tenant_id = $1
          AND signal_type = $2
        """,
        tenant_id,
        signal_type,
    )
    out: dict[str, QuestionPolicySignal] = {}
    for row in rows:
        primitive = str(row["question_primitive"] or "").upper()
        if not primitive:
            continue
        out[primitive] = QuestionPolicySignal(
            signal_type=str(row["signal_type"] or signal_type),
            question_primitive=primitive,
            attempts=int(row["attempts"] or 0),
            successes=int(row["successes"] or 0),
            utility_score=float(row["utility_score"] or 0.0),
            total_credit=float(row["total_credit"] or 0.0),
            total_cost=float(row["total_cost"] or 0.0),
        )
    return out


def _apply_question_policy(
    candidates: list[InquiryQuestion],
    *,
    question_policy: dict[str, QuestionPolicySignal],
) -> list[InquiryQuestion]:
    if not question_policy:
        return candidates
    out: list[InquiryQuestion] = []
    for question in candidates:
        signal = question_policy.get(question.primitive)
        if signal is None or signal.attempts <= 0:
            out.append(question)
            continue
        policy_boost = _question_policy_score_boost(signal)
        value = _clamp_float(
            question.expected_value + policy_boost * 0.35,
            0.0,
            1.0,
        )
        cost = _clamp_float(
            question.expected_cost - max(0.0, policy_boost) * 0.12
            + max(0.0, -policy_boost) * 0.10,
            0.02,
            1.0,
        )
        score = round(value - cost + policy_boost, 4)
        out.append(replace(
            question,
            expected_value=value,
            expected_cost=cost,
            score=score,
        ))
    return out


def _question_policy_score_boost(signal: QuestionPolicySignal) -> float:
    success_rate = _question_policy_success_rate(signal)
    utility = float(signal.utility_score)
    raw = 0.16 * utility + 0.20 * (success_rate - 0.35)
    return _clamp_float(raw, -0.24, 0.34)


def _question_policy_success_rate(signal: QuestionPolicySignal) -> float:
    # `successes` is credited at reader-decision grain, so it can exceed
    # question attempts. Cap to a probability before using it for policy.
    return _clamp_float(signal.successes / max(signal.attempts, 1), 0.0, 1.0)


def _question_policy_budget_multiplier(
    signal: QuestionPolicySignal | None,
) -> float:
    if signal is None or signal.attempts <= 0:
        return 1.0
    success_rate = _question_policy_success_rate(signal)
    utility = float(signal.utility_score)
    if utility > 0.0 and success_rate >= 0.55:
        compaction = min(0.35, 0.06 * utility + 0.18 * (success_rate - 0.55))
        return _clamp_float(1.0 - compaction, 0.65, 1.0)
    if utility < -0.25 or success_rate < 0.20:
        return 0.75
    return 1.0


def _policy_budget(
    value: int,
    signal: QuestionPolicySignal | None,
) -> int:
    return max(1, int(round(float(value) * _question_policy_budget_multiplier(signal))))


def _select_questions(
    candidates: list[InquiryQuestion],
    *,
    questions_per_round: int,
    round_index: int,
    already_asked: set[str],
) -> list[InquiryQuestion]:
    selected: list[InquiryQuestion] = []
    seen_targets: set[str] = set()

    def add(question: InquiryQuestion, *, priority: bool = False) -> bool:
        if question.primitive in already_asked:
            return False
        if question.retrieval_target in seen_targets:
            return False
        if selected:
            marginal_score = _question_marginal_score(question, selected)
            floor = (
                _QUESTION_PRIORITY_MARGINAL_MIN_SCORE
                if priority
                else _QUESTION_MARGINAL_MIN_SCORE
            )
            if marginal_score < floor:
                return False
        selected.append(replace(question, round_index=round_index))
        seen_targets.add(question.retrieval_target)
        return True

    by_id = {q.question_id: q for q in candidates}
    priority_ids: list[str] = []
    constraint = by_id.get("Q_CONSTRAINT")
    if (
        constraint is not None
        and constraint.expected_value >= 0.86
        and "CONSTRAINT" not in already_asked
    ):
        priority_ids.append("Q_CONSTRAINT")
    owner = by_id.get("Q_OWNER")
    if (
        owner is not None
        and owner.expected_value >= 0.70
        and "OWNERSHIP" not in already_asked
    ):
        priority_ids.append("Q_OWNER")
    recurrence = by_id.get("Q_RECURRENCE")
    if (
        recurrence is not None
        and recurrence.expected_value >= 0.9
        and "RECURRENCE" not in already_asked
    ):
        priority_ids.append("Q_RECURRENCE")
    counter = by_id.get("Q_COUNTEREVIDENCE")
    if (
        questions_per_round >= 2
        and counter is not None
        and counter.expected_value >= 0.82
        and "COUNTEREVIDENCE" not in already_asked
    ):
        priority_ids.append("Q_COUNTEREVIDENCE")
    dependency = by_id.get("Q_CRITICAL_PATH")
    if (
        dependency is not None
        and dependency.expected_value >= 0.86
        and "DEPENDENCY" not in already_asked
    ):
        priority_ids.append("Q_CRITICAL_PATH")
    goal_impact = by_id.get("Q_GOAL_IMPACT")
    if (
        goal_impact is not None
        and goal_impact.expected_value >= 0.86
        and "GOAL_IMPACT" not in already_asked
    ):
        priority_ids.append("Q_GOAL_IMPACT")

    for question_id in priority_ids:
        question = by_id.get(question_id)
        if question is None:
            continue
        add(question, priority=True)
        if len(selected) >= questions_per_round:
            return selected

    for question in sorted(candidates, key=lambda q: (-q.score, q.expected_cost, q.question_id)):
        if question.primitive in already_asked:
            continue
        if question.retrieval_target in seen_targets:
            continue
        add(question)
        if len(selected) >= questions_per_round:
            break
    return selected


def _compile_retrieval_plan(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    policy_signal: QuestionPolicySignal | None = None,
) -> list[RetrievalAction]:
    q = question.question
    seed_text = _trigger_text(trigger)
    semantic_query = f"{q} {seed_text}".strip()
    common = {"seed_entities": list(trigger.seed_entity_ids)}
    semantic_budget = _policy_budget(cfg.semantic_budget, policy_signal)
    if question.primitive == "DEPENDENCY":
        return [
            RetrievalAction(question.question_id, "structural", "commitment_graph", filters=common),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "dependency_model_edges",
                filters=common,
                budget=_policy_budget(60, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_observations",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=_policy_budget(40, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "dependency_evidence",
                query=semantic_query,
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "COMMITMENT":
        return [
            RetrievalAction(question.question_id, "structural", "active_commitments", filters=common),
            RetrievalAction(
                question.question_id,
                "semantic",
                "commitment_evidence",
                query=f"active commitment promised outcome {seed_text}",
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "COUNTEREVIDENCE":
        return [
            RetrievalAction(
                question.question_id,
                "semantic",
                "counterevidence",
                query=f"alternate explanation counterevidence not blocked not caused {seed_text}",
                budget=semantic_budget,
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_counterevidence",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=_policy_budget(30, policy_signal),
            ),
        ]
    if question.primitive == "CONSTRAINT":
        return [
            RetrievalAction(
                question.question_id,
                "structural",
                "goal_resource_bridge",
                filters=common,
            ),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "constraint_resource_edges",
                filters=common,
                budget=_policy_budget(60, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_constraint_observations",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=_policy_budget(30, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "constraint_evidence",
                query=(
                    "constraint scarce resource capacity quota policy blocker "
                    f"{seed_text}"
                ),
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "OWNERSHIP":
        return [
            RetrievalAction(question.question_id, "structural", "ownership_graph", filters=common),
            RetrievalAction(
                question.question_id,
                "semantic",
                "owner_evidence",
                query=f"owner responsible assigned owns dependency {seed_text}",
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "RECURRENCE":
        return [
            RetrievalAction(
                question.question_id,
                "pattern",
                "pattern_models",
                query=semantic_query,
                budget=_policy_budget(80, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "related_model_edges",
                filters=common,
                budget=_policy_budget(80, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "recurrence_evidence",
                query=f"recurring pattern repeated similar issue {seed_text}",
                budget=semantic_budget,
            ),
        ]
    return [
        RetrievalAction(question.question_id, "structural", "goal_resource_bridge", filters=common),
        RetrievalAction(
            question.question_id,
            "model_edge",
            "goal_resource_edges",
            filters=common,
            budget=_policy_budget(60, policy_signal),
        ),
        RetrievalAction(
            question.question_id,
            "semantic",
            "goal_customer_resource_evidence",
            query=f"goal customer resource impact {seed_text}",
            budget=semantic_budget,
        ),
    ]


def _seed_action_cache_from_baseline(
    action_cache: dict[tuple[Any, ...], PathwayResult],
    baseline: RetrievalResult,
    trigger: TriggerContext,
    cfg: InquiryConfig,
) -> dict[str, Any]:
    """Reuse baseline graph reads for question actions when the scope matches."""
    notes: dict[str, Any] = {
        "seeded": 0,
        "paths": [],
        "skipped": [],
    }
    by_source = {result.source_pathway: result for result in baseline.pathway_results}

    if int(getattr(trigger, "max_hops", 0) or 0) == int(cfg.structural_max_hops):
        source = by_source.get("A")
        if source is not None:
            action = RetrievalAction("Q0", "structural", "baseline_structural")
            key = _retrieval_action_cache_key(action, trigger, cfg)
            action_cache[key] = _clone_pathway_result(
                source,
                model_limit=min(action.budget, cfg.action_model_budget_limit),
                cap_models_by_activation=True,
                note="baseline_A",
            )
            notes["seeded"] += 1
            notes["paths"].append("structural:A")
    else:
        notes["skipped"].append("structural_hop_mismatch")

    g_hops = min(max(int(getattr(trigger, "max_hops", 0) or 0), 0), 3)
    if g_hops == int(cfg.model_edge_max_hops):
        source = by_source.get("G")
        if source is not None:
            action = RetrievalAction(
                "Q0",
                "model_edge",
                "baseline_model_edges",
                budget=cfg.action_model_budget_limit,
            )
            key = _retrieval_action_cache_key(action, trigger, cfg)
            action_cache[key] = _clone_pathway_result(
                source,
                model_limit=cfg.action_model_budget_limit,
                note="baseline_G",
            )
            notes["seeded"] += 1
            notes["paths"].append("model_edge:G")
    else:
        notes["skipped"].append("model_edge_hop_mismatch")

    return notes


def _clone_pathway_result(
    result: PathwayResult,
    *,
    model_limit: int | None = None,
    observation_limit: int | None = None,
    cap_models_by_activation: bool = False,
    note: str,
) -> PathwayResult:
    models = list(result.models)
    if model_limit is not None:
        limit = max(0, int(model_limit))
        if cap_models_by_activation:
            models = sorted(
                models,
                key=lambda model: (
                    -float(getattr(model, "activation", 0.0) or 0.0),
                    str(getattr(model, "id", "")),
                ),
            )
        models = models[:limit]
    observations = list(result.observations)
    if observation_limit is not None:
        observations = observations[: max(0, int(observation_limit))]
    notes = dict(result.notes or {})
    notes["cache_seeded_from"] = note
    notes["models_after_cache_seed_cap"] = len(models)
    if observation_limit is not None:
        notes["observations_after_cache_seed_cap"] = len(observations)
    return PathwayResult(
        models=models,
        observations=observations,
        acts={key: list(value) for key, value in (result.acts or {}).items()},
        resources=list(result.resources),
        source_pathway=result.source_pathway,
        notes=notes,
    )


def _retrieval_action_cache_key(
    action: RetrievalAction,
    trigger: TriggerContext,
    cfg: InquiryConfig,
) -> tuple[Any, ...]:
    model_budget = min(max(1, int(action.budget)), max(1, int(cfg.action_model_budget_limit)))
    observation_budget = min(
        max(1, int(action.budget)),
        max(1, int(cfg.action_observation_budget_limit)),
    )
    scope_actors = tuple(sorted(str(actor) for actor in (trigger.scope_actors or [])))
    scope_entities = _stable_cache_value(trigger.seed_entity_ids or [])
    if action.path == "structural":
        return (
            "structural",
            cfg.structural_max_hops,
            model_budget,
            scope_actors,
            scope_entities,
        )
    if action.path == "temporal":
        return (
            "temporal",
            str(trigger.seed_occurred_at),
            int(action.filters.get("window_days") or cfg.temporal_window_days),
            model_budget,
            observation_budget,
            scope_actors,
            scope_entities,
        )
    if action.path == "model_edge":
        return (
            "model_edge",
            cfg.model_edge_max_hops,
            model_budget,
            str(trigger.model_id or ""),
            scope_actors,
            scope_entities,
        )
    if action.path == "pattern":
        return (
            "pattern",
            model_budget,
            _stable_cache_value(trigger.seed_signature or {}),
        )
    return (
        action.path,
        action.target,
        action.query or _trigger_text(trigger),
        model_budget,
        observation_budget,
        scope_actors,
        scope_entities,
    )


def _stable_cache_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def _sage_reader_total_ms(result: RetrievalResult) -> int | None:
    read_note = (result.notes or {}).get("sage_reader") or {}
    if not isinstance(read_note, dict):
        return None
    debug = read_note.get("debug") or {}
    if not isinstance(debug, dict):
        return None
    timings = debug.get("stage_timings_ms") or {}
    if not isinstance(timings, dict):
        return None
    try:
        return int(timings.get("reader_total_ms"))
    except (TypeError, ValueError):
        return None


def _sage_reader_action_gate(
    result: RetrievalResult,
) -> tuple[Literal["all", "broad"] | None, str | None]:
    plan = _sage_reader_plan_from_result(result)
    if not plan:
        return None, None
    mode = str(plan.get("mode") or "")
    if _sage_reader_plan_hard_abstained(plan):
        return "all", "sage_reader_negative_memory_abstain"
    if bool(plan.get("skip_broad_discovery")) and mode in {
        "focused",
        "guarded_negative_memory",
    }:
        return "broad", f"sage_reader_{mode}_broad_gate"
    return None, None


def _sage_reader_controller_summary(
    notes: dict[str, Any],
    *,
    trigger: TriggerContext,
) -> dict[str, Any]:
    raw_questions = notes.get("questions")
    questions = raw_questions if isinstance(raw_questions, dict) else {}
    question_summaries: dict[str, dict[str, Any]] = {}
    hard_abstain_count = 0
    skipped_broad_count = 0
    selected_model_count = 0
    for qid, read_note in questions.items():
        if not isinstance(read_note, dict):
            continue
        plan = _sage_reader_plan_from_read_note(read_note)
        selected_ids = read_note.get("selected_model_ids") or []
        selected_count = len(selected_ids) if isinstance(selected_ids, list) else 0
        selected_model_count += selected_count
        hard_abstained = _sage_reader_plan_hard_abstained(plan)
        if hard_abstained:
            hard_abstain_count += 1
        if bool(plan.get("skip_broad_discovery")):
            skipped_broad_count += 1
        question_summaries[str(qid)] = {
            "mode": plan.get("mode"),
            "confidence": plan.get("confidence"),
            "abstain_early": bool(plan.get("abstain_early")),
            "skip_broad_discovery": bool(plan.get("skip_broad_discovery")),
            "selected_model_count": selected_count,
            "hard_abstained": hard_abstained,
        }

    question_count = len(question_summaries)
    explicit_anchor = _trigger_has_explicit_model_anchor(trigger)
    global_negative_route_gate = (
        question_count > 0
        and hard_abstain_count == question_count
        and selected_model_count == 0
        and not explicit_anchor
    )
    return {
        "used": question_count > 0,
        "question_count": question_count,
        "hard_abstain_count": hard_abstain_count,
        "skipped_broad_count": skipped_broad_count,
        "selected_model_count": selected_model_count,
        "explicit_model_anchor": explicit_anchor,
        "global_negative_route_gate": global_negative_route_gate,
        "questions": question_summaries,
    }


def _sage_reader_plan_from_result(result: RetrievalResult) -> dict[str, Any]:
    read_note = (result.notes or {}).get("sage_reader") or {}
    if not isinstance(read_note, dict):
        return {}
    return _sage_reader_plan_from_read_note(read_note)


def _sage_reader_plan_from_read_note(read_note: dict[str, Any]) -> dict[str, Any]:
    debug = read_note.get("debug") or {}
    if not isinstance(debug, dict):
        return {}
    plan = debug.get("learned_read_plan") or {}
    return plan if isinstance(plan, dict) else {}


def _sage_reader_plan_hard_abstained(plan: dict[str, Any]) -> bool:
    return str(plan.get("mode") or "") == "abstain" and bool(
        plan.get("abstain_early")
    )


def _trigger_has_explicit_model_anchor(trigger: TriggerContext) -> bool:
    return trigger.model_id is not None or bool(trigger.member_model_ids)


def _sage_only_retrieval_results(
    results: list[RetrievalResult],
) -> list[RetrievalResult]:
    return [
        result
        for result in results
        if "sage_reader" in set(result.notes.get("pathways_run", []) or [])
        or any(pr.source_pathway == "SAGE" for pr in result.pathway_results)
    ]


def _action_cache_summary(action_timings: list[dict[str, Any]]) -> dict[str, Any]:
    hits = sum(1 for note in action_timings if note.get("cache_hit"))
    misses = sum(
        1
        for note in action_timings
        if not note.get("cache_hit") and note.get("path") != "sage_reader"
    )
    elapsed_by_path: Counter[str] = Counter()
    cached_by_path: Counter[str] = Counter()
    for note in action_timings:
        path = str(note.get("path") or "")
        if path:
            elapsed_by_path[path] += int(note.get("elapsed_ms") or 0)
            if note.get("cache_hit"):
                cached_by_path[path] += 1
    return {
        "hits": hits,
        "misses": misses,
        "elapsed_ms_by_path": dict(sorted(elapsed_by_path.items())),
        "cache_hits_by_path": dict(sorted(cached_by_path.items())),
    }


async def _execute_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
) -> PathwayResult | None:
    def capped_budget(value: int) -> int:
        return min(max(1, int(value)), max(1, int(cfg.action_model_budget_limit)))

    def capped_observation_budget(value: int) -> int:
        return min(
            max(1, int(value)),
            max(1, int(cfg.action_observation_budget_limit)),
        )

    try:
        if action.path == "structural":
            seeds = list(trigger.seed_entity_ids)
            if not seeds and trigger.scope_actors:
                seeds = [{"type": "actor", "id": str(a)} for a in trigger.scope_actors]
            result = await pathway_a_structural(
                seeds,
                trigger.tenant_id,
                conn,
                max_hops=cfg.structural_max_hops,
            )
            _cap_pathway_models(result, capped_budget(action.budget))
            return result
        if action.path == "semantic":
            query_text = action.query or _trigger_text(trigger)
            # Question-conditioned retrieval needs a question-conditioned
            # vector. The trigger vector is only a fallback for tests or
            # offline runs without an embedder.
            precomputed_vector = (
                trigger.precomputed_seed_vector
                if embedder is None or query_text == _trigger_text(trigger)
                else None
            )
            result = await pathway_b_semantic(
                query_text,
                trigger.tenant_id,
                conn,
                k=capped_budget(action.budget),
                embedder=embedder,
                precomputed_vector=precomputed_vector,
                event_actors=trigger.scope_actors,
                event_entities=trigger.seed_entity_ids,
            )
            if _has_broad_signal_language(_trigger_text(trigger).casefold()):
                broad_lexical_limit = capped_budget(action.budget) * 2
                lexical_models = await _broad_lexical_model_scan(
                    trigger,
                    query_text,
                    conn,
                    limit=broad_lexical_limit,
                )
                if lexical_models:
                    by_id = {model.id: model for model in result.models}
                    for model in lexical_models:
                        by_id.setdefault(model.id, model)
                    result.models = list(by_id.values())
                    result.notes["broad_lexical_models"] = len(lexical_models)
                    result.notes["broad_lexical_limit"] = broad_lexical_limit
                    _cap_pathway_models(result, capped_budget(action.budget) + broad_lexical_limit)
            return result
        if action.path == "temporal":
            if trigger.seed_occurred_at is None:
                return None
            return await pathway_c_temporal(
                trigger.seed_occurred_at,
                timedelta(days=int(action.filters.get("window_days") or cfg.temporal_window_days)),
                trigger.tenant_id,
                conn,
                scope_actors=trigger.scope_actors,
                scope_entities=trigger.seed_entity_ids,
                max_observations=capped_observation_budget(action.budget),
                max_models=capped_budget(action.budget),
            )
        if action.path == "pattern":
            return await pathway_d_pattern(
                trigger.seed_signature,
                trigger.tenant_id,
                conn,
                limit=capped_budget(action.budget),
            )
        if action.path == "model_edge":
            return await pathway_g_model_edges(
                trigger.tenant_id,
                conn,
                seed_model_ids=[trigger.model_id] if trigger.model_id else [],
                seed_entity_ids=trigger.seed_entity_ids,
                scope_actors=trigger.scope_actors,
                max_hops=cfg.model_edge_max_hops,
                limit=capped_budget(action.budget),
            )
    except (RetrievalPathwayError, ValidationError):
        return None
    return None


async def _execute_sage_reader_action(
    question: InquiryQuestion,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    hypotheses: tuple[Hypothesis, ...],
) -> RetrievalResult | None:
    if not cfg.sage_reader_enabled:
        return None
    try:
        from services.reasoning.sage.reader import ReaderBudget, SynthesisReader
    except Exception:  # noqa: BLE001
        return None
    try:
        reader = SynthesisReader(
            budget=ReaderBudget(
                max_nodes=max(8, int(cfg.result_model_limit)),
                max_edges=max(16, int(cfg.result_model_limit) * 2),
                max_evidence_items=max(10, int(cfg.evidence_reservoir_limit)),
                lexical_candidates=max(10, int(cfg.candidate_model_limit // 3)),
                shortcut_candidates=12,
                affordance_candidates=max(10, int(cfg.candidate_model_limit // 4)),
                propagation_neighbors=max(24, int(cfg.candidate_model_limit // 2)),
            )
        )
        read = await reader.read(
            conn=conn,
            tenant_id=trigger.tenant_id,
            trigger=trigger,
            question_id=question.question_id,
            question=question.question,
            question_primitive=question.primitive,
            hypotheses=hypotheses,
        )
    except (asyncpg.PostgresError, ValidationError):
        raise
    except Exception:
        import structlog
        structlog.get_logger(__name__).warning(
            "sage_reader.failed",
            question_id=question.question_id,
            exc_info=True,
        )
        return None

    return RetrievalResult(
        trigger=trigger,
        observations=list(read.observations),
        models=list(read.models),
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        pathway_results=[read.pathway_result],
        notes={
            "action": _jsonable(asdict(RetrievalAction(
                question.question_id,
                "sage_reader",
                "synthesis_reader",
                query=question.question,
                budget=cfg.result_model_limit,
            ))),
            "pathways_run": ["sage_reader"],
            "sage_reader": {
                "question_id": question.question_id,
                "question_primitive": read.question_primitive,
                "signature": read.signature,
                "selected_model_ids": [str(m.id) for m in read.models],
                "projected_evidence_count": len(read.projected_evidence),
                "activation_trace_count": len(read.activations),
                "debug": read.debug,
                "activations": [
                    _jsonable(asdict(trace))
                    for trace in read.activations
                ],
            },
        },
        model_scores=dict(read.model_scores),
    )


def _record_sage_reader_notes(
    notes: dict[str, Any],
    question: InquiryQuestion,
    result: RetrievalResult,
) -> None:
    read_note = (result.notes or {}).get("sage_reader")
    if not isinstance(read_note, dict):
        return
    qid = question.question_id
    notes.setdefault("questions", {})[qid] = read_note
    signature = read_note.get("signature")
    if isinstance(signature, dict) and signature not in notes["signatures"]:
        notes["signatures"].append(signature)
    for mid in read_note.get("selected_model_ids", []) or []:
        if mid not in notes["selected_model_ids"]:
            notes["selected_model_ids"].append(mid)
    notes["projected_evidence_count"] = int(
        notes.get("projected_evidence_count") or 0
    ) + int(read_note.get("projected_evidence_count") or 0)
    notes["activation_trace_count"] = int(
        notes.get("activation_trace_count") or 0
    ) + int(read_note.get("activation_trace_count") or 0)


def _cap_pathway_models(result: PathwayResult, limit: int) -> None:
    limit = max(0, int(limit))
    before = len(result.models)
    if limit <= 0 or before <= limit:
        return
    result.models = sorted(
        result.models,
        key=lambda model: (
            -float(getattr(model, "activation", 0.0) or 0.0),
            str(getattr(model, "id", "")),
        ),
    )[:limit]
    result.notes["models_before_adaptive_cap"] = before
    result.notes["models_after_adaptive_cap"] = len(result.models)


async def _broad_lexical_model_scan(
    trigger: TriggerContext,
    query_text: str,
    conn: asyncpg.Connection,
    *,
    limit: int,
) -> list[ModelRow]:
    terms = [
        term
        for term in sorted(
            _relevance_tokens(f"{query_text} {_trigger_text(trigger)}")
        )
        if len(term) >= 4
    ]
    if not terms or limit <= 0:
        return []
    terms = terms[:12]
    conditions = " OR ".join(
        f'"natural" ILIKE ${idx}' for idx in range(3, 3 + len(terms))
    )
    rows = await conn.fetch(
        f"""
        SELECT id
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND ({conditions})
        ORDER BY activation DESC, created_at DESC
        LIMIT $2
        """,
        trigger.tenant_id,
        int(limit),
        *[f"%{term}%" for term in terms],
    )
    ids = [row["id"] for row in rows]
    if not ids:
        return []
    return await ModelsRepo(None, run_topology_on_insert=False).retrieve(ids, conn=conn)


def _result_from_pathway(
    trigger: TriggerContext,
    pr: PathwayResult,
    action: RetrievalAction,
) -> RetrievalResult:
    scores: dict[UUID, float] = {}
    for rank, model in enumerate(pr.models, start=1):
        scores[model.id] = max(0.01, 1.0 / (rank + 1))
    return RetrievalResult(
        trigger=trigger,
        observations=list(pr.observations),
        models=list(pr.models),
        acts={k: list(v) for k, v in pr.acts.items()},
        resources=list(pr.resources),
        pathway_results=[pr],
        notes={
            "action": _jsonable(asdict(action)),
            "pathways_run": [pr.source_pathway],
            "models_merged": len(pr.models),
            "observations_merged": len(pr.observations),
        },
        model_scores=scores,
    )


def _merge_results(
    trigger: TriggerContext,
    results: list[RetrievalResult],
    *,
    top_n: int,
    note_prefix: str,
    config: InquiryConfig | None = None,
    relevance_gate: bool = False,
) -> RetrievalResult:
    models_by_id: dict[UUID, ModelRow] = {}
    model_scores: dict[UUID, float] = {}
    model_pathways: dict[UUID, set[str]] = {}
    model_questions: dict[UUID, set[str]] = {}
    observations_by_id: dict[UUID, ObservationRow] = {}
    resources_by_id: dict[UUID, ResourceRow] = {}
    goals_by_id: dict[UUID, GoalRow] = {}
    commitments_by_id: dict[UUID, CommitmentRow] = {}
    decisions_by_id: dict[UUID, DecisionRow] = {}
    pathway_results: list[PathwayResult] = []
    pathways_run: list[str] = []
    skipped: list[Any] = []

    for result in results:
        pathway_results.extend(result.pathway_results)
        action_note = result.notes.get("action")
        action_question = None
        action_path = None
        if isinstance(action_note, dict):
            action_question = action_note.get("question_id")
            action_path = action_note.get("path")
        for pr in result.pathway_results:
            pathway = pr.source_pathway
            for model in pr.models:
                model_pathways.setdefault(model.id, set()).add(pathway)
                if isinstance(action_path, str):
                    model_pathways[model.id].add(action_path)
                if isinstance(action_question, str):
                    model_questions.setdefault(model.id, set()).add(action_question)
        for pathway in result.notes.get("pathways_run", []):
            if pathway not in pathways_run:
                pathways_run.append(pathway)
        skipped.extend(result.notes.get("pathways_skipped", []))
        for model in result.models:
            models_by_id.setdefault(model.id, model)
            model_scores[model.id] = model_scores.get(model.id, 0.0) + float(
                result.model_scores.get(model.id, 0.01)
            )
        for obs in result.observations:
            observations_by_id.setdefault(obs.id, obs)
        for res in result.resources:
            resources_by_id.setdefault(res.id, res)
        for goal in result.acts.get("goals", []):
            goals_by_id.setdefault(goal.id, goal)
        for commitment in result.acts.get("commitments", []):
            commitments_by_id.setdefault(commitment.id, commitment)
        for decision in result.acts.get("decisions", []):
            decisions_by_id.setdefault(decision.id, decision)

    ranked_models = sorted(
        models_by_id.values(),
        key=lambda m: (-model_scores.get(m.id, 0.0), -m.activation, str(m.id)),
    )
    relevance_notes: dict[str, Any] | None = None
    if relevance_gate:
        ranked_models, relevance_notes = _select_relevant_models(
            trigger,
            ranked_models,
            model_scores,
            top_n=top_n,
            config=config or InquiryConfig(),
            model_pathways=model_pathways,
            model_questions=model_questions,
        )
    else:
        ranked_models = ranked_models[:top_n]
    models = ranked_models
    observations = sorted(
        observations_by_id.values(),
        key=lambda o: (o.occurred_at, o.id),
        reverse=True,
    )
    resources = sorted(
        resources_by_id.values(),
        key=lambda r: (r.last_updated_at, r.id),
        reverse=True,
    )
    acts = {
        "goals": sorted(goals_by_id.values(), key=lambda g: g.created_at, reverse=True),
        "commitments": sorted(
            commitments_by_id.values(),
            key=lambda c: c.last_state_change_at,
            reverse=True,
        ),
        "decisions": sorted(
            decisions_by_id.values(),
            key=lambda d: d.last_state_change_at,
            reverse=True,
        ),
    }
    return RetrievalResult(
        trigger=trigger,
        observations=observations,
        models=models,
        acts=acts,
        resources=resources,
        pathway_results=pathway_results,
        notes={
            "kind": trigger.kind,
            "pathways_run": pathways_run,
            "pathways_skipped": skipped,
            "models_merged": len(models),
            "observations_merged": len(observations),
            "acts_merged": {k: len(v) for k, v in acts.items()},
            "resources_merged": len(resources),
            "merge_source": note_prefix,
            "candidate_model_count": len(models_by_id),
            **({"relevance_gate": relevance_notes} if relevance_notes is not None else {}),
        },
        model_scores={mid: score for mid, score in model_scores.items() if mid in {m.id for m in models}},
    )


def _select_relevant_models(
    trigger: TriggerContext,
    ranked_models: list[ModelRow],
    model_scores: dict[UUID, float],
    *,
    top_n: int,
    config: InquiryConfig,
    model_pathways: dict[UUID, set[str]],
    model_questions: dict[UUID, set[str]],
) -> tuple[list[ModelRow], dict[str, Any]]:
    if not ranked_models or top_n <= 0:
        return [], {
            "used": True,
            "candidate_count": len(ranked_models),
            "selected_count": 0,
            "reason": "no candidates or non-positive top_n",
        }

    trigger_text = _trigger_text(trigger)
    lower = trigger_text.casefold()
    material_signal = trigger.kind != "T1" or _signal_has_material_update_intent(lower)
    broad_signal = _has_broad_signal_language(lower)
    weak_signal = not material_signal and not broad_signal
    threshold = (
        float(config.relevance_broad_signal_min_score)
        if broad_signal
        else (
            float(config.relevance_weak_signal_min_score)
            if weak_signal
            else float(config.relevance_min_score)
        )
    )
    max_raw = max((float(model_scores.get(m.id, 0.0)) for m in ranked_models), default=0.0)
    scored: list[tuple[ModelRow, ModelRelevance]] = []
    for model in ranked_models:
        rel = _score_model_relevance(
            trigger,
            model,
            raw_score=float(model_scores.get(model.id, 0.0)),
            max_raw_score=max_raw,
            model_pathways=model_pathways.get(model.id, set()),
            model_questions=model_questions.get(model.id, set()),
            weak_signal=weak_signal,
            broad_signal=broad_signal,
        )
        scored.append((model, rel))
    scored.sort(key=lambda item: (-item[1].final_score, -item[0].activation, str(item[0].id)))

    min_material = (
        min(max(0, int(config.relevance_min_material_models)), top_n, len(scored))
        if material_signal or broad_signal
        else 0
    )
    diversity_candidate_cap = _relevance_diversity_candidate_cap(
        len(scored),
        top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
    )
    selected_pairs: list[tuple[ModelRow, ModelRelevance]] = []
    dropped_below_threshold = 0
    cutoff_reason = "candidate list exhausted"
    prev_score: float | None = None
    for idx, pair in enumerate(scored):
        score = pair[1].final_score
        below_threshold = score < threshold
        if below_threshold and len(selected_pairs) >= min_material:
            dropped_below_threshold += 1
            cutoff_reason = "score below relevance threshold"
            continue
        if (
            prev_score is not None
            and not broad_signal
            and len(selected_pairs) >= min_material
            and float(config.relevance_score_cliff) > 0
            and prev_score - score >= float(config.relevance_score_cliff)
        ):
            cutoff_reason = "score cliff detected"
            if weak_signal or len(selected_pairs) >= diversity_candidate_cap:
                dropped_below_threshold += len(scored) - idx
                break
        selected_pairs.append(pair)
        prev_score = score
        if len(selected_pairs) >= diversity_candidate_cap:
            cutoff_reason = (
                "top_n cap reached after relevance gate"
                if diversity_candidate_cap == top_n
                else "diversity reservoir cap reached after relevance gate"
            )
            break

    selected_pairs_before_compaction = len(selected_pairs)
    selected_pairs, duplicate_drops, compaction_notes = _apply_relevance_diversity(
        selected_pairs,
        top_n=top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
        threshold=threshold,
        min_keep=min_material,
        model_pathways=model_pathways,
        model_questions=model_questions,
    )
    selected_pairs, closure_notes = _append_structural_closure(
        selected_pairs,
        scored,
        top_n=top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
        threshold=threshold,
        model_pathways=model_pathways,
        model_questions=model_questions,
    )
    selected_pairs, packing_notes = _pack_structural_links(selected_pairs)
    selected = [model for model, _ in selected_pairs]
    notes = {
        "used": True,
        "candidate_count": len(ranked_models),
        "selected_count": len(selected),
        "threshold": round(threshold, 4),
        "signal_class": (
            "broad" if broad_signal else ("weak" if weak_signal else "material")
        ),
        "min_material_models": min_material,
        "dropped_below_threshold": dropped_below_threshold,
        "dropped_redundant": duplicate_drops,
        "cutoff_reason": cutoff_reason,
        "selected_before_compaction": selected_pairs_before_compaction,
        "diversity_candidate_cap": diversity_candidate_cap,
        "coverage_compaction": compaction_notes,
        "structural_closure": closure_notes,
        "structural_packing": packing_notes,
        "top_scores": [
            _jsonable(
                {
                    "model_id": rel.model_id,
                    "score": round(rel.final_score, 4),
                    "base": round(rel.base_score, 4),
                    "lexical": round(rel.lexical_score, 4),
                    "scope": round(rel.scope_score, 4),
                    "path": round(rel.path_score, 4),
                    "evidence": round(rel.evidence_score, 4),
                    "provenance": round(rel.provenance_score, 4),
                    "penalty": round(rel.penalty, 4),
                    "reasons": list(rel.reasons),
                }
            )
            for _, rel in scored[:12]
        ],
        "selected_model_ids": [str(model.id) for model in selected],
    }
    return selected, notes


def _pack_structural_links(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
) -> tuple[list[tuple[ModelRow, ModelRelevance]], dict[str, Any]]:
    """Place explanatory relation/counterevidence models next to their anchor."""
    notes: dict[str, Any] = {
        "used": True,
        "moved": 0,
        "moved_model_ids": [],
    }
    if len(selected_pairs) < 3:
        return selected_pairs, notes

    positions = {model.id: idx for idx, (model, _rel) in enumerate(selected_pairs)}
    selected_by_id = {model.id: model for model, _rel in selected_pairs}
    dependents_by_anchor: dict[UUID, list[tuple[ModelRow, ModelRelevance]]] = {}
    moved_ids: set[UUID] = set()

    for model, rel in selected_pairs:
        if not _is_structural_detail_model(model):
            continue
        anchors = [
            anchor_id
            for anchor_id in _linked_anchor_ids(model, selected_by_id)
            if anchor_id in positions and anchor_id != model.id
        ]
        if not anchors:
            continue
        anchor_id = min(anchors, key=lambda mid: positions[mid])
        if positions[anchor_id] + 1 == positions[model.id]:
            continue
        dependents_by_anchor.setdefault(anchor_id, []).append((model, rel))
        moved_ids.add(model.id)

    if not moved_ids:
        return selected_pairs, notes

    repacked: list[tuple[ModelRow, ModelRelevance]] = []
    emitted: set[UUID] = set()
    for pair in selected_pairs:
        model, _rel = pair
        if model.id in moved_ids:
            continue
        repacked.append(pair)
        emitted.add(model.id)
        for dependent_pair in sorted(
            dependents_by_anchor.get(model.id, []),
            key=lambda item: positions[item[0].id],
        ):
            dependent = dependent_pair[0]
            if dependent.id in emitted:
                continue
            repacked.append(dependent_pair)
            emitted.add(dependent.id)

    # Preserve any model whose anchor was itself moved behind another anchor.
    for pair in selected_pairs:
        if pair[0].id not in emitted:
            repacked.append(pair)
            emitted.add(pair[0].id)

    notes["moved"] = len(moved_ids)
    notes["moved_model_ids"] = [str(mid) for mid in sorted(moved_ids, key=str)]
    return repacked, notes


def _is_structural_detail_model(model: ModelRow) -> bool:
    role = str(getattr(model, "claim_role", "") or "").casefold()
    level = str(getattr(model, "abstraction_level", "") or "").casefold()
    polarity = str(getattr(model, "polarity", "") or "").casefold()
    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    return (
        role == "relation"
        or level in {"relationship", "composite"}
        or polarity == "mixed"
        or bool(_model_member_ids(model))
        or _has_counterevidence_qualifier_language(text)
    )


def _linked_anchor_ids(model: ModelRow, selected_by_id: dict[UUID, ModelRow]) -> set[UUID]:
    anchors: set[UUID] = set()
    for raw in getattr(model, "supporting_model_ids", []) or ():
        try:
            anchors.add(UUID(str(raw)))
        except (TypeError, ValueError):
            continue
    anchors.update(_model_member_ids(model))
    for selected_id, selected_model in selected_by_id.items():
        if model.id in set(getattr(selected_model, "supporting_model_ids", []) or []):
            anchors.add(selected_id)
        if model.id in _model_member_ids(selected_model):
            anchors.add(selected_id)
    return anchors


def _relevance_diversity_candidate_cap(
    scored_count: int,
    top_n: int,
    *,
    weak_signal: bool,
    broad_signal: bool,
) -> int:
    if top_n <= 0:
        return 0
    if weak_signal:
        return min(scored_count, top_n)
    multiplier = 3 if broad_signal else 2
    additive_floor = 48 if broad_signal else 32
    return min(scored_count, max(top_n, min(top_n * multiplier, top_n + additive_floor)))


def _append_structural_closure(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
    candidate_pairs: list[tuple[ModelRow, ModelRelevance]],
    *,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
    threshold: float,
    model_pathways: dict[UUID, set[str]] | None = None,
    model_questions: dict[UUID, set[str]] | None = None,
) -> tuple[list[tuple[ModelRow, ModelRelevance]], dict[str, Any]]:
    """Keep structurally necessary belief siblings in the final model list.

    The relevance scorer is intentionally conservative: a graph-only relation
    or counterevidence model may have weak surface text even when it explains
    or qualifies a selected belief. This pass is a small closure over already
    retrieved candidates, not an expansion query.
    """
    notes: dict[str, Any] = {
        "used": True,
        "added": 0,
        "added_model_ids": [],
        "reasons": {},
    }
    if weak_signal or not selected_pairs or top_n <= len(selected_pairs):
        return selected_pairs, notes

    model_pathways = model_pathways or {}
    model_questions = model_questions or {}
    selected_by_id = {model.id: (model, rel) for model, rel in selected_pairs}
    candidate_by_id = {model.id: (model, rel) for model, rel in candidate_pairs}
    max_added = 2 if broad_signal else 4

    for model, rel in candidate_pairs:
        if model.id in selected_by_id:
            continue
        if len(selected_by_id) >= top_n or notes["added"] >= max_added:
            break
        reason = _structural_closure_reason(
            model,
            rel,
            selected_by_id,
            model_pathways=model_pathways.get(model.id, set()),
            model_questions=model_questions.get(model.id, set()),
            threshold=threshold,
        )
        if reason is None:
            continue
        selected_pairs.append(candidate_by_id[model.id])
        selected_by_id[model.id] = candidate_by_id[model.id]
        mid = str(model.id)
        notes["added"] += 1
        notes["added_model_ids"].append(mid)
        notes["reasons"][mid] = reason

    return selected_pairs, notes


def _structural_closure_reason(
    model: ModelRow,
    rel: ModelRelevance,
    selected_by_id: dict[UUID, tuple[ModelRow, ModelRelevance]],
    *,
    model_pathways: set[str],
    model_questions: set[str],
    threshold: float,
) -> str | None:
    if not _has_selected_model_link(model, selected_by_id):
        return None

    focused_graph_path = bool(model_pathways & {"G", "model_edge", "sage_reader"})
    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    role = str(getattr(model, "claim_role", "") or "").casefold()
    level = str(getattr(model, "abstraction_level", "") or "").casefold()
    polarity = str(getattr(model, "polarity", "") or "").casefold()
    is_relation = (
        role == "relation"
        or level in {"relationship", "composite"}
        or bool(_model_member_ids(model))
    )
    if is_relation and focused_graph_path:
        return "linked_relation"

    is_counter = (
        "Q_COUNTEREVIDENCE" in model_questions
        or polarity == "mixed"
        or _has_counterevidence_qualifier_language(text)
    )
    if is_counter and (focused_graph_path or rel.final_score >= max(0.20, threshold * 0.75)):
        return "linked_counterevidence"

    return None


def _has_selected_model_link(
    model: ModelRow,
    selected_by_id: dict[UUID, tuple[ModelRow, ModelRelevance]],
) -> bool:
    selected_ids = set(selected_by_id)
    if set(getattr(model, "supporting_model_ids", []) or []) & selected_ids:
        return True
    candidate_members = _model_member_ids(model)
    if candidate_members & selected_ids:
        return True
    for selected_model, _rel in selected_by_id.values():
        if model.id in set(getattr(selected_model, "supporting_model_ids", []) or []):
            return True
        if model.id in _model_member_ids(selected_model):
            return True
    return False


def _model_member_ids(model: ModelRow) -> set[UUID]:
    prop = getattr(model, "proposition", {}) or {}
    if not isinstance(prop, dict):
        return set()
    out: set[UUID] = set()
    for raw in prop.get("member_model_ids") or ():
        try:
            out.add(UUID(str(raw)))
        except (TypeError, ValueError):
            continue
    return out


def _has_counterevidence_qualifier_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"counterevidence|mitigation\s+exists|mitigated\s+but|"
            r"does\s+not\s+remove|doesn't\s+remove|should\s+not\s+erase|"
            r"risk\s+remains|blocker\s+remains|alternate\s+explanation|"
            r"weaken(?:s|ed)?|contradict(?:s|ed)?|premise\s+(?:is\s+)?"
            r"(?:stale|incomplete|unsupported)"
            r")\b",
            lower,
        )
    )


def _score_model_relevance(
    trigger: TriggerContext,
    model: ModelRow,
    *,
    raw_score: float,
    max_raw_score: float,
    model_pathways: set[str],
    model_questions: set[str],
    weak_signal: bool,
    broad_signal: bool,
) -> ModelRelevance:
    reasons: list[str] = []
    trigger_text = _trigger_text(trigger)
    model_text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    )
    raw_norm = raw_score / max_raw_score if max_raw_score > 0 else 0.0
    base_score = min(0.22, 0.22 * raw_norm)
    lexical_score = _lexical_relevance_score(trigger_text, model_text)
    scope_score, scope_reasons = _scope_relevance_score(trigger, model)
    path_score = min(
        0.14,
        0.035 * len(model_pathways) + 0.025 * len(model_questions),
    )
    explicit_model_ids = set(trigger.member_model_ids or [])
    if trigger.model_id is not None:
        explicit_model_ids.add(trigger.model_id)
    explicit_model_score = 0.0
    if model.id in explicit_model_ids:
        explicit_model_score = 0.55
        reasons.append("explicit trigger model")
    elif explicit_model_ids and ("G" in model_pathways or "model_edge" in model_pathways):
        explicit_model_score = 0.26
        reasons.append("graph neighbor of explicit trigger model")
    evidence_score = _model_evidence_relevance_score(trigger_text, model_text)
    provenance_score = min(
        0.10,
        0.035 * min(len(getattr(model, "supporting_event_ids", []) or []), 2)
        + 0.025 * min(len(getattr(model, "supporting_model_ids", []) or []), 2)
        + 0.025 * max(0.0, min(1.0, float(getattr(model, "confidence", 0.0) or 0.0))),
    )
    penalty = 0.0
    if lexical_score <= 0.0 and scope_score <= 0.0:
        penalty += 0.28
        reasons.append("no lexical or scope overlap")
    if model_pathways and model_pathways <= {"C", "temporal"} and lexical_score < 0.08:
        penalty += 0.16
        reasons.append("temporal-only weak lexical match")
    if _declares_unrelated_to_trigger(model_text.casefold()):
        penalty += 0.40
        reasons.append("declares unrelated to trigger")

    if weak_signal:
        base_score *= 0.45
        scope_score *= 0.55
        path_score *= 0.50
        evidence_score *= 0.65
        reasons.append("weak signal dampening")

    if lexical_score > 0:
        reasons.append("material lexical overlap")
    reasons.extend(scope_reasons)
    if path_score > 0:
        reasons.append("retrieved by focused inquiry path")
    if evidence_score > 0:
        reasons.append("hypothesis/counterevidence language")
    if provenance_score > 0:
        reasons.append("model provenance/confidence")

    final_score = max(
        0.0,
        base_score
        + lexical_score
        + scope_score
        + path_score
        + explicit_model_score
        + evidence_score
        + provenance_score
        - penalty,
    )
    if (
        not weak_signal
        and not broad_signal
        and explicit_model_score <= 0.0
        and scope_score > 0.0
        and lexical_score < 0.08
        and evidence_score <= 0.0
    ):
        final_score = min(final_score, 0.24)
        reasons.append("scope-only match capped")
    return ModelRelevance(
        model_id=model.id,
        final_score=final_score,
        base_score=base_score,
        lexical_score=lexical_score,
        scope_score=scope_score,
        path_score=path_score,
        evidence_score=evidence_score,
        provenance_score=provenance_score,
        penalty=penalty,
        reasons=tuple(reasons[:8]),
    )


def _scope_relevance_score(
    trigger: TriggerContext,
    model: ModelRow,
) -> tuple[float, list[str]]:
    trigger_entities = _canonical_entity_pairs(trigger.seed_entity_ids)
    model_entities = _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    score = 0.0
    reasons: list[str] = []
    overlap = trigger_entities & model_entities
    if overlap:
        type_priority = {etype for etype, _ in overlap}
        if "commitment" in type_priority:
            score += 0.42
            reasons.append("same commitment")
        if type_priority & {"customer", "customer_resource", "resource"}:
            score += 0.32
            reasons.append("same customer/resource")
        if "goal" in type_priority:
            score += 0.24
            reasons.append("same goal")
        if "decision" in type_priority:
            score += 0.22
            reasons.append("same decision")
    actor_overlap = set(trigger.scope_actors or []) & set(getattr(model, "scope_actors", []) or [])
    if actor_overlap:
        score += 0.12
        reasons.append("same actor")
    return min(0.52, score), reasons


def _canonical_entity_pairs(raw_entities: Any) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    if not isinstance(raw_entities, list):
        return pairs
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        etype = raw.get("type")
        eid = raw.get("id")
        if etype is None or eid is None:
            continue
        t = str(etype)
        i = str(eid)
        if t in {"customer", "customer_resource", "resource"}:
            pairs.add(("customer", i))
            pairs.add(("customer_resource", i))
            pairs.add(("resource", i))
        else:
            pairs.add((t, i))
    return pairs


def _lexical_relevance_score(trigger_text: str, model_text: str) -> float:
    trigger_tokens = _relevance_tokens(trigger_text)
    if not trigger_tokens:
        return 0.0
    model_tokens = _relevance_tokens(model_text)
    if not model_tokens:
        return 0.0
    overlap = trigger_tokens & model_tokens
    if not overlap:
        return 0.0
    recall = len(overlap) / max(1, len(trigger_tokens))
    precision = len(overlap) / max(1, len(model_tokens))
    score = 0.22 * recall + 0.10 * min(1.0, precision * 3.0)
    if len(overlap) >= 3:
        score += 0.06
    return min(0.34, score)


_RELEVANCE_STOPWORDS = {
    "about", "after", "also", "and", "are", "around", "because", "been",
    "before", "case", "company", "context", "from", "has", "have", "into",
    "need", "needs", "now", "only", "signal", "that", "the", "their",
    "there", "this", "today", "with", "without",
}


def _relevance_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", str(text).casefold())
        if token not in _RELEVANCE_STOPWORDS and not token.isdigit()
    }


def _model_evidence_relevance_score(trigger_text: str, model_text: str) -> float:
    lower = model_text.casefold()
    trigger_lower = trigger_text.casefold()
    if not _has_material_trigger_overlap(lower, trigger_lower):
        return 0.0
    score = 0.0
    if _has_risk_language(lower):
        score += 0.08
    if _has_act_affecting_language(lower):
        score += 0.06
    if _mentions_recurrence(lower):
        score += 0.05
    if re.search(r"\b(resolved|unblocked|not blocked|launched|mitigated)\b", lower):
        score += 0.07
    return min(0.18, score)


def _apply_relevance_diversity(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
    *,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
    threshold: float,
    min_keep: int,
    model_pathways: dict[UUID, set[str]] | None = None,
    model_questions: dict[UUID, set[str]] | None = None,
) -> tuple[list[tuple[ModelRow, ModelRelevance]], int, dict[str, Any]]:
    if not selected_pairs or top_n <= 0:
        return [], len(selected_pairs), {
            "strategy": "coverage_aware",
            "target_limit": 0,
            "selected_before": len(selected_pairs),
            "selected_after": 0,
        }

    target_limit = min(
        top_n,
        _coverage_compaction_target(len(selected_pairs), top_n, weak_signal, broad_signal),
    )
    floor = min(target_limit, max(1, int(min_keep or 0)))
    if broad_signal:
        # Broad portfolio questions need representative breadth before
        # redundancy pruning. A same-cluster set can still describe many
        # independent customers, constraints, or instances of a trend.
        floor = min(target_limit, max(floor, min(20, len(selected_pairs))))
    model_pathways = model_pathways or {}
    model_questions = model_questions or {}
    remaining = list(selected_pairs)
    out: list[tuple[ModelRow, ModelRelevance]] = []
    covered: Counter[str] = Counter()
    cluster_counts: Counter[tuple[Any, ...]] = Counter()

    def add_pair(pair: tuple[ModelRow, ModelRelevance]) -> None:
        model, rel = pair
        out.append(pair)
        cluster_counts[_model_relevance_cluster_key(model)] += 1
        for feature, _weight in _model_coverage_features(
            model,
            model_pathways.get(model.id, set()),
            model_questions.get(model.id, set()),
        ):
            covered[feature] += 1

    while remaining and len(out) < floor:
        add_pair(remaining.pop(0))

    while remaining and len(out) < target_limit:
        best_idx = 0
        best_utility = float("-inf")
        for idx, pair in enumerate(remaining):
            model, rel = pair
            utility = _coverage_selection_utility(
                model,
                rel,
                covered,
                cluster_counts,
                broad_signal=broad_signal,
                weak_signal=weak_signal,
                model_pathways=model_pathways.get(model.id, set()),
                model_questions=model_questions.get(model.id, set()),
            )
            # A tiny position prior keeps ties stable and favors the relevance
            # ordering produced by the scorer.
            utility -= idx * 0.0005
            if utility > best_utility:
                best_utility = utility
                best_idx = idx

        if (
            len(out) >= floor
            and len(out) >= 8
            and best_utility < max(0.20, threshold + (0.03 if broad_signal else 0.05))
            and not _has_uncovered_answer_obligation(
                remaining[best_idx][0],
                covered,
                model_questions=model_questions.get(remaining[best_idx][0].id, set()),
            )
        ):
            break
        add_pair(remaining.pop(best_idx))

    dropped = max(0, len(selected_pairs) - len(out))
    notes = {
        "strategy": "coverage_aware",
        "target_limit": target_limit,
        "floor": floor,
        "selected_before": len(selected_pairs),
        "selected_after": len(out),
        "dropped": dropped,
        "coverage_features": len(covered),
        "cluster_count": len(cluster_counts),
    }
    return out, dropped, notes


def _coverage_compaction_target(
    selected_count: int,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
) -> int:
    if weak_signal:
        return min(top_n, 8)
    if broad_signal:
        return min(top_n, max(20, min(32, selected_count)))
    if selected_count >= 32:
        return min(top_n, 18)
    return min(top_n, selected_count)


def _coverage_selection_utility(
    model: ModelRow,
    rel: ModelRelevance,
    covered: Counter[str],
    cluster_counts: Counter[tuple[Any, ...]],
    *,
    broad_signal: bool,
    weak_signal: bool,
    model_pathways: set[str],
    model_questions: set[str],
) -> float:
    features = _model_coverage_features(model, model_pathways, model_questions)
    novelty = sum(weight / (1 + covered[feature]) for feature, weight in features)
    cluster_count = cluster_counts[_model_relevance_cluster_key(model)]
    answer_novelty = _has_uncovered_answer_obligation(
        model,
        covered,
        model_questions=model_questions,
    )
    redundancy_penalty = 0.0
    if cluster_count:
        redundancy_penalty += 0.07 * cluster_count
    if weak_signal and cluster_count:
        redundancy_penalty += 0.12
    entity_pressure = _entity_coverage_pressure(model, covered)
    role_pressure = _role_coverage_pressure(model, covered)
    if entity_pressure:
        redundancy_penalty += (0.012 if answer_novelty else 0.04) * entity_pressure
    if role_pressure and not broad_signal and not answer_novelty:
        redundancy_penalty += 0.03 * role_pressure
    return rel.final_score + min(0.28, novelty) - redundancy_penalty


def _has_uncovered_answer_obligation(
    model: ModelRow,
    covered: Counter[str],
    *,
    model_questions: set[str] | None = None,
) -> bool:
    for feature in _model_answer_obligation_features(model, model_questions or set()):
        if covered[feature] <= 0:
            return True
    return False


def _model_answer_obligation_features(
    model: ModelRow,
    model_questions: set[str],
) -> tuple[str, ...]:
    """Coarse answer slots a Model can satisfy for coverage-aware stopping."""
    features: list[str] = []

    def add(value: str) -> None:
        clean = value.strip()
        if clean and clean not in features:
            features.append(clean)

    belief_address = belief_address_from_model_like(model)
    role = str(
        getattr(model, "claim_role", "") or belief_address.get("claim_role") or ""
    ).casefold()
    level = str(
        getattr(model, "abstraction_level", "") or belief_address.get("abstraction_level") or ""
    ).casefold()
    polarity = str(
        getattr(model, "polarity", "") or belief_address.get("polarity") or ""
    ).casefold()
    primitives = tuple(
        str(primitive).upper()
        for primitive in (belief_address.get("answerable_primitives") or ())
    )

    for primitive in primitives[:6]:
        add(f"answer_slot:primitive:{primitive}")
        if role:
            add(f"answer_slot:role_primitive:{role}:{primitive}")
    if role:
        add(f"answer_slot:role:{role}")
    if level in {"relationship", "composite"}:
        add(f"answer_slot:level:{level}")
    for entity_type, entity_id in sorted(
        _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    )[:8]:
        add(f"answer_slot:entity:{entity_type}:{entity_id}")
    for question in sorted(str(question) for question in model_questions)[:6]:
        add(f"answer_slot:question:{question}")
    for key in tuple(belief_address.get("obligation_keys") or ())[:12]:
        key_text = str(key)
        if key_text.startswith(("spo:", "qualifier:")):
            add(f"answer_slot:object_obligation:{key_text[:140]}")

    structural = _is_structural_detail_model(model)
    link_tokens: set[str] = set()
    for raw in getattr(model, "supporting_model_ids", []) or ():
        try:
            link_tokens.add(str(UUID(str(raw))))
        except (TypeError, ValueError):
            continue
    link_tokens.update(str(mid) for mid in _model_member_ids(model))
    if structural:
        add("answer_slot:structural_detail")
        for linked_id in sorted(link_tokens)[:4]:
            add(f"answer_slot:structural_link:{linked_id}")

    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    if polarity == "mixed" or _has_counterevidence_qualifier_language(text):
        subject = str(belief_address.get("subject") or "").strip().casefold()
        add(f"answer_slot:counterevidence:{subject[:96] or 'linked'}")

    subject = str(belief_address.get("subject") or "").strip().casefold()
    predicate = str(belief_address.get("predicate") or "").strip().casefold()
    if subject and (
        structural
        or not getattr(model, "scope_entities", None)
        or role in {"pattern", "prediction", "recommendation", "capability", "situation"}
    ):
        add(f"answer_slot:subject:{subject[:96]}")
        if predicate:
            add(f"answer_slot:subject_predicate:{subject[:96]}:{predicate[:64]}")
    return tuple(features)


def _model_coverage_features(
    model: ModelRow,
    model_pathways: set[str],
    model_questions: set[str],
) -> list[tuple[str, float]]:
    features: list[tuple[str, float]] = []
    belief_address = belief_address_from_model_like(model)
    fingerprint = str(belief_address.get("fingerprint") or "").strip()
    if fingerprint:
        features.append((f"belief_fingerprint:{fingerprint}", 0.055))
    for key in tuple(belief_address.get("obligation_keys") or ())[:12]:
        features.append((f"belief_obligation:{key}", 0.095))
    for primitive in tuple(belief_address.get("answerable_primitives") or ())[:6]:
        features.append((f"answerable:{primitive}", 0.045))
    for feature in _model_answer_obligation_features(model, model_questions):
        features.append((feature, 0.075))
    kind = getattr(model, "proposition_kind", None)
    if kind:
        features.append((f"kind:{kind}", 0.035))
    role = getattr(model, "claim_role", None)
    if role:
        features.append((f"role:{role}", 0.055))
    level = getattr(model, "abstraction_level", None)
    if level:
        features.append((f"level:{level}", 0.025))
    time_mode = getattr(model, "time_mode", None)
    if time_mode:
        features.append((f"time:{time_mode}", 0.02))
    polarity = getattr(model, "polarity", None)
    if polarity:
        features.append((f"polarity:{polarity}", 0.02))
    for tag in sorted(str(tag) for tag in (getattr(model, "domain_tags", []) or []))[:5]:
        features.append((f"domain:{tag}", 0.035))
    for entity_type, entity_id in sorted(_canonical_entity_pairs(getattr(model, "scope_entities", []) or []))[:8]:
        features.append((f"entity:{entity_type}:{entity_id}", 0.075))
        features.append((f"entity_type:{entity_type}", 0.025))
    for actor_id in sorted(str(actor) for actor in (getattr(model, "scope_actors", []) or []))[:4]:
        features.append((f"actor:{actor_id}", 0.035))
    for support_id in sorted(str(mid) for mid in (getattr(model, "supporting_model_ids", []) or []))[:4]:
        features.append((f"support:{support_id}", 0.06))
    for path in sorted(str(path) for path in model_pathways)[:6]:
        features.append((f"path:{path}", 0.04))
    for question in sorted(str(question) for question in model_questions)[:6]:
        features.append((f"question:{question}", 0.035))
    if not features:
        token_key = tuple(sorted(_relevance_tokens(getattr(model, "natural", "") or ""))[:3])
        if token_key:
            features.append((f"text:{token_key}", 0.02))
    return features


def _entity_coverage_pressure(model: ModelRow, covered: Counter[str]) -> int:
    pairs = _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    return max((covered[f"entity:{entity_type}:{entity_id}"] for entity_type, entity_id in pairs), default=0)


def _role_coverage_pressure(model: ModelRow, covered: Counter[str]) -> int:
    role = getattr(model, "claim_role", None)
    if role:
        return covered[f"role:{role}"]
    kind = getattr(model, "proposition_kind", None)
    if kind:
        return covered[f"kind:{kind}"]
    return 0


def _model_relevance_cluster_key(model: ModelRow) -> tuple[Any, ...]:
    entities = sorted(_canonical_entity_pairs(getattr(model, "scope_entities", []) or []))[:3]
    belief_address = belief_address_from_model_like(model)
    fingerprint = str(belief_address.get("fingerprint") or "").strip()
    if fingerprint:
        return (
            getattr(model, "proposition_kind", None),
            tuple(entities),
            fingerprint,
        )
    text_tokens = sorted(_relevance_tokens(getattr(model, "natural", "") or ""))[:4]
    return (
        getattr(model, "proposition_kind", None),
        tuple(entities),
        tuple(text_tokens),
    )


def _signal_has_material_update_intent(lower: str) -> bool:
    scrubbed = _scrub_negated_signal_language(lower)
    return (
        _has_risk_language(scrubbed)
        or _has_commitment_language(scrubbed)
        or _has_act_affecting_language(scrubbed)
        or _mentions_recurrence(scrubbed)
    )


def _has_broad_signal_language(lower: str) -> bool:
    scrubbed = _scrub_negated_signal_language(lower)
    broad_terms = bool(
        re.search(
            r"\b(all|portfolio|company-wide|team-wide|board|exec|"
            r"customers|renewals|fleet|global)\b",
            scrubbed,
        )
    )
    every_scope = bool(
        re.search(
            r"\bevery\s+(?:customer|account|renewal|team|segment|"
            r"pipeline|portfolio|region|department|business)\b",
            scrubbed,
        )
    )
    broad_across = bool(
        re.search(
            r"\bacross\s+(?:all\s+|the\s+)?(?:enterprise\s+)?"
            r"(?:customers|accounts|renewals|pipeline|portfolio|teams|"
            r"company|org|organization|business|segments)\b",
            scrubbed,
        )
    )
    return broad_terms or every_scope or broad_across


def _scrub_negated_signal_language(lower: str) -> str:
    text = re.sub(
        r"\b(no|not|without)\b[^.;\n]{0,90}\b("
        r"blocker|blocked|blocking|risk|owner|decision|commitment|"
        r"commitments|customer|customers|delivery|deliver|launch|"
        r"incident|escalation|renewal|renewals"
        r")\w*",
        " ",
        lower,
    )
    text = re.sub(
        r"\bnot related to\b[^.;\n]{0,120}",
        " ",
        text,
    )
    return text


def _add_result_to_reservoir(
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    result: RetrievalResult,
    *,
    path: str,
    question_id: str,
    hypotheses: tuple[Hypothesis, ...],
    score_hint: float = 0.0,
) -> None:
    trigger_text = _trigger_text(result.trigger)
    for model in result.models:
        _upsert_evidence(
            evidence_by_key,
            key=("model", str(model.id)),
            source_type="model",
            source_ref_id=model.id,
            summary=model.natural or json.dumps(model.proposition, default=str),
            trust_tier="model",
            timestamp=model.created_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + float(result.model_scores.get(model.id, 0.0)),
            raw_content_ref=f"model:{model.id}",
            trigger_text=trigger_text,
        )
    for obs in result.observations:
        _upsert_evidence(
            evidence_by_key,
            key=("observation", str(obs.id)),
            source_type="observation",
            source_ref_id=obs.id,
            summary=obs.content_text,
            trust_tier=obs.trust_tier,
            timestamp=obs.occurred_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + _trust_score(obs.trust_tier),
            raw_content_ref=f"observation:{obs.id}",
            trigger_text=trigger_text,
        )
    for kind, rows in result.acts.items():
        for row in rows:
            title = getattr(row, "title", None) or str(getattr(row, "id", ""))
            if kind == "commitments":
                owner = getattr(row, "owner_id", None)
                title = f"{title} owner={owner}" if owner else f"{title} owner=unassigned"
            _upsert_evidence(
                evidence_by_key,
                key=(kind.rstrip("s"), str(row.id)),
                source_type=kind.rstrip("s"),
                source_ref_id=row.id,
                summary=f"{kind.rstrip('s')} {title}",
                trust_tier="authoritative",
                timestamp=getattr(row, "last_state_change_at", None)
                or getattr(row, "created_at", None),
                path=path,
                question_id=question_id,
                hypotheses=hypotheses,
                score=score_hint + 0.4,
                raw_content_ref=f"{kind.rstrip('s')}:{row.id}",
                trigger_text=trigger_text,
            )
    for res in result.resources:
        _upsert_evidence(
            evidence_by_key,
            key=("resource", str(res.id)),
            source_type="resource",
            source_ref_id=res.id,
            summary=f"{res.kind} resource {res.identity}: {res.description or ''}",
            trust_tier="authoritative",
            timestamp=res.last_updated_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + 0.32,
            raw_content_ref=f"resource:{res.id}",
            trigger_text=trigger_text,
        )


def _upsert_evidence(
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    *,
    key: tuple[str, str],
    source_type: str,
    source_ref_id: UUID | None,
    summary: str,
    trust_tier: str | None,
    timestamp: datetime | None,
    path: str,
    question_id: str,
    hypotheses: tuple[Hypothesis, ...],
    score: float,
    raw_content_ref: str,
    trigger_text: str | None = None,
) -> None:
    supports, weakens, contradicts = _classify_hypothesis_links(
        summary,
        hypotheses,
        trigger_text=trigger_text,
    )
    if key not in evidence_by_key:
        evidence_by_key[key] = EvidenceCard(
            evidence_id=uuid7(),
            source_type=source_type,
            source_ref=f"{source_type}:{key[1]}",
            source_ref_id=source_ref_id,
            summary=_compact(summary, 700),
            trust_tier=trust_tier,
            timestamp=timestamp,
            raw_content_ref=raw_content_ref,
            token_estimate=_estimate_tokens(summary),
            sensitivity=_sensitivity(summary),
        )
    evidence_by_key[key].merge(
        path=path,
        question_id=question_id,
        supports=supports,
        weakens=weakens,
        contradicts=contradicts,
        score=score,
    )


def _classify_hypothesis_links(
    summary: str,
    hypotheses: tuple[Hypothesis, ...],
    *,
    trigger_text: str | None = None,
) -> tuple[set[str], set[str], set[str]]:
    lower = (summary or "").casefold()
    related_to_trigger = _has_material_trigger_overlap(
        lower,
        (trigger_text or "").casefold(),
    ) and not _declares_unrelated_to_trigger(lower)
    supports: set[str] = set()
    weakens: set[str] = set()
    contradicts: set[str] = set()
    if related_to_trigger and _has_risk_language(lower):
        supports.add("H1")
        weakens.add("H0")
    if related_to_trigger and _has_act_affecting_language(lower):
        supports.add("H2")
    if related_to_trigger and _mentions_recurrence(lower):
        supports.add("H3")
    if related_to_trigger and any(
        word in lower for word in ("resolved", "unblocked", "not blocked", "launched")
    ):
        contradicts.add("H1")
        supports.add("H0")
    if related_to_trigger and _has_premise_challenge_language(lower):
        weakens.add("H1")
    if related_to_trigger and _has_missing_owner_language(lower):
        weakens.add("H2")
    known_ids = {h.id for h in hypotheses}
    return supports & known_ids, weakens & known_ids, contradicts & known_ids


def _has_premise_challenge_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"not\s+the\s+only\s+blocker|one\s+blocker\s*,?\s*but|"
            r"also\s+(?:active|blocking|a\s+blocker|at\s+risk)|"
            r"additional\s+blocker|another\s+blocker|"
            r"premise\s+(?:is\s+)?(?:wrong|stale|incomplete|unsupported)|"
            r"assumption\s+(?:is\s+)?(?:wrong|stale|incomplete|unsupported)|"
            r"does\s+not\s+support|not\s+supported\s+by|unsupported\s+by|"
            r"evidence\s+(?:does\s+not|doesn't)\s+support|"
            r"marked\s+commit\s+but|crm\s+says\s+commit\s+but|"
            r"stale\s+(?:premise|assumption|status|stage|model)|"
            r"superseded\s+by|no\s+longer\s+(?:true|current|active)"
            r")\b",
            lower,
        )
    )


def _has_missing_owner_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"no\s+(?:explicit|recorded|accountable)\s+owner|"
            r"owner\s+(?:is\s+)?(?:missing|unassigned|unknown|unclear|unresolved)|"
            r"missing\s+owner|unassigned\s+owner"
            r")\b",
            lower,
        )
    )


def _answer_question(
    question: InquiryQuestion,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    *,
    trigger_occurred_at: datetime | None = None,
    stale_after_days: int = 30,
) -> QuestionAnswer:
    candidates = [
        card for card in evidence_by_key.values()
        if question.question_id in card.retrieved_for_questions
    ]
    if question.primitive == "COUNTEREVIDENCE":
        fresh_counter = [
            str(card.evidence_id)
            for card in candidates
            if _is_counterevidence_for_leading_hypothesis(card)
            and not _is_stale_relative_to_trigger(
                card,
                trigger_occurred_at=trigger_occurred_at,
                stale_after_days=stale_after_days,
            )
        ][:8]
        stale_counter = [
            str(card.evidence_id)
            for card in candidates
            if _is_counterevidence_for_leading_hypothesis(card)
            and _is_stale_relative_to_trigger(
                card,
                trigger_occurred_at=trigger_occurred_at,
                stale_after_days=stale_after_days,
            )
        ][:8]
        supporting = [
            str(card.evidence_id)
            for card in candidates
            if card.supports_hypotheses or card.source_type in {"commitment", "goal", "resource"}
        ][:8]
        if fresh_counter and supporting:
            status = "partially_supported"
            summary = "Retrieved both supporting evidence and fresh counterevidence."
        elif fresh_counter:
            status = "supported"
            summary = "Retrieved fresh counterevidence for this question."
        elif candidates:
            status = "inconclusive"
            summary = (
                "Only stale counterevidence was retrieved for this question."
                if stale_counter
                else "Retrieved related evidence but no credible counterevidence."
            )
        else:
            status = "unanswered"
            summary = "No usable evidence was retrieved for this question."
        unknowns = ("fresh counterevidence",) if stale_counter and not fresh_counter else ()
        return QuestionAnswer(
            question_id=question.question_id,
            answer_status=status,
            summary=summary,
            supporting_evidence=tuple(supporting),
            counterevidence=tuple(fresh_counter),
            new_uncertainties=unknowns,
        )

    supporting = [
        str(card.evidence_id)
        for card in candidates
        if (
            _evidence_supports_ownership(card)
            if question.primitive == "OWNERSHIP"
            else card.supports_hypotheses
            or card.source_type in {"commitment", "goal", "resource"}
        )
    ][:8]
    counter = [
        str(card.evidence_id)
        for card in candidates
        if card.contradicts_hypotheses or card.weakens_hypotheses
    ][:8]
    if supporting and counter:
        status = "partially_supported"
        summary = "Retrieved both supporting evidence and counterevidence."
    elif supporting:
        status = "supported"
        summary = "Retrieved supporting evidence for this question."
    elif candidates:
        status = "inconclusive"
        summary = "Retrieved related evidence but no decisive answer."
    else:
        status = "unanswered"
        summary = "No usable evidence was retrieved for this question."
    unknowns = ()
    if question.primitive == "OWNERSHIP" and not supporting:
        unknowns = ("responsible owner",)
    return QuestionAnswer(
        question_id=question.question_id,
        answer_status=status,
        summary=summary,
        supporting_evidence=tuple(supporting),
        counterevidence=tuple(counter),
        new_uncertainties=unknowns,
    )


def _resolved_unknowns_for_answer(
    question: InquiryQuestion,
    answer: QuestionAnswer,
) -> set[str]:
    if question.primitive == "COUNTEREVIDENCE":
        if answer.answer_status == "unanswered" or "fresh counterevidence" in answer.new_uncertainties:
            return set()
        return {"counterevidence"}
    if answer.answer_status not in {"supported", "partially_supported"}:
        return set()
    primitive_to_unknowns = {
        "DEPENDENCY": {"whether the blocker is on the critical path"},
        "COMMITMENT": {"affected commitment"},
        "OWNERSHIP": {"responsible owner"},
        "GOAL_IMPACT": {"affected goal"},
        "RECURRENCE": {"whether this is part of a broader recurring pattern"},
    }
    return set(primitive_to_unknowns.get(question.primitive, set()))


def _sufficiency_gate(
    route: SignalRoute,
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard],
    answers: list[QuestionAnswer],
    *,
    round_index: int,
    max_rounds: int,
    unknowns: set[str],
) -> SufficiencyVerdict:
    evidence_count = len(evidence)
    answered = sum(1 for a in answers if a.answer_status in {"supported", "partially_supported"})
    has_support = any(card.supports_hypotheses for card in evidence)
    has_counter_check = any(
        a.question_id == "Q_COUNTEREVIDENCE" and a.answer_status != "unanswered"
        and "fresh counterevidence" not in a.new_uncertainties
        for a in answers
    )
    has_act = any(card.source_type in {"commitment", "goal", "decision", "resource"} for card in evidence)

    if route == "HUMAN_VALIDATION_PATH":
        return SufficiencyVerdict(
            "human_validation_required",
            "routing indicated a human-resolvable missing fact",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if route in {"FAST_PATH", "DETERMINISTIC_UPDATE"} and max_rounds == 0:
        status: InquiryStopStatus = (
            "sufficient_for_reasoning" if evidence_count else "no_update_needed"
        )
        return SufficiencyVerdict(
            status,
            "fast/bounded path compiled baseline context",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if evidence_count == 0:
        return SufficiencyVerdict(
            "no_update_needed",
            "no related evidence survived baseline or inquiry retrieval",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if (
        route == "DEEP_INQUIRY_PATH"
        and round_index >= max_rounds
        and "responsible owner" in unknowns
        and any(a.question_id == "Q_OWNER" for a in answers)
    ):
        return SufficiencyVerdict(
            "human_validation_required",
            "the affected region is visible but ownership remains unresolved",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if has_support and has_counter_check and (has_act or evidence_count >= 6):
        return SufficiencyVerdict(
            "sufficient_for_reasoning",
            "supporting evidence exists, counterevidence was checked, and an affected region is visible",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if round_index >= max_rounds:
        return SufficiencyVerdict(
            "budget_exhausted",
            "inquiry round budget reached before all uncertainty closed",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    return SufficiencyVerdict(
        "insufficient_continue",
        "more retrieval has expected value",
        evidence_count,
        answered,
        tuple(sorted(unknowns)[:10]),
    )


def _rank_evidence(cards: list[EvidenceCard], *, limit: int) -> list[EvidenceCard]:
    return sorted(
        cards,
        key=lambda c: (
            -_evidence_value(c),
            -_timestamp_sort_value(c.timestamp),
            str(c.evidence_id),
        ),
        reverse=False,
    )[:limit]


def _select_minimal_sufficient_evidence(
    cards: list[EvidenceCard],
    *,
    hypotheses: tuple[Hypothesis, ...],
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    evidence_limit: int,
) -> tuple[list[EvidenceCard], dict[str, Any]]:
    """Compress ranked evidence to the smallest writer-useful packet.

    The reservoir is intentionally broad; this stage optimizes the
    writer-facing packet for marginal utility. It protects evidence that
    answers selected questions, preserves falsification/action anchors,
    then fills remaining space only with non-redundant high-value cards.
    """
    if not cards:
        return [], {
            "enabled": True,
            "input_count": 0,
            "selected_count": 0,
            "target_count": 0,
            "dropped_count": 0,
            "drop_ratio": 0.0,
            "protected_count": 0,
            "coverage": {},
        }

    by_id = {str(card.evidence_id): card for card in cards}
    selected: list[EvidenceCard] = []
    selected_ids: set[str] = set()
    protected_ids: set[str] = set()
    protected_reasons: dict[str, set[str]] = {}

    def add(card: EvidenceCard | None, reason: str, *, protected: bool) -> bool:
        if card is None:
            return False
        cid = str(card.evidence_id)
        if cid in selected_ids:
            if protected:
                protected_ids.add(cid)
                protected_reasons.setdefault(cid, set()).add(reason)
            return False
        if len(selected) >= max(1, int(evidence_limit)):
            return False
        selected.append(card)
        selected_ids.add(cid)
        if protected:
            protected_ids.add(cid)
            protected_reasons.setdefault(cid, set()).add(reason)
        return True

    target = _minimal_evidence_target(
        cards,
        questions=questions,
        answers=answers,
        route=route,
        mode=mode,
        evidence_limit=evidence_limit,
    )
    hard_cap = max(target, _protected_answer_ref_count(answers))
    hard_cap = min(max(1, int(evidence_limit)), max(hard_cap, target))

    for answer in answers:
        support_cards = [
            by_id[eid]
            for eid in answer.supporting_evidence
            if eid in by_id
        ]
        counter_cards = [
            by_id[eid]
            for eid in answer.counterevidence
            if eid in by_id
        ]
        for card in sorted(support_cards, key=_evidence_sort_key)[:2]:
            add(card, f"answer_support:{answer.question_id}", protected=True)
        for card in sorted(counter_cards, key=_evidence_sort_key)[:2]:
            add(card, f"answer_counter:{answer.question_id}", protected=True)

    for question in questions:
        q_cards = [
            card
            for card in cards
            if question.question_id in card.retrieved_for_questions
        ]
        if q_cards:
            add(
                sorted(q_cards, key=_evidence_sort_key)[0],
                f"question_coverage:{question.question_id}",
                protected=True,
            )

    for hyp in hypotheses:
        support = [
            card
            for card in cards
            if hyp.id in card.supports_hypotheses
        ]
        if support:
            add(
                sorted(support, key=_evidence_sort_key)[0],
                f"hypothesis_support:{hyp.id}",
                protected=True,
            )

    counters = [
        card for card in cards
        if card.weakens_hypotheses or card.contradicts_hypotheses
    ]
    for card in sorted(counters, key=_evidence_sort_key)[:2]:
        add(card, "falsification_guard", protected=True)

    for source_type in ("commitment", "goal", "decision", "resource"):
        typed = [card for card in cards if card.source_type == source_type]
        if typed:
            add(
                sorted(typed, key=_evidence_sort_key)[0],
                f"action_anchor:{source_type}",
                protected=True,
            )

    if len(selected) > hard_cap:
        selected.sort(key=lambda card: (
            str(card.evidence_id) not in protected_ids,
            *_evidence_sort_key(card),
        ))
        selected = selected[:hard_cap]
        selected_ids = {str(card.evidence_id) for card in selected}

    while len(selected) < target:
        best: EvidenceCard | None = None
        best_score = float("-inf")
        for card in cards:
            cid = str(card.evidence_id)
            if cid in selected_ids:
                continue
            marginal = _marginal_evidence_value(card, selected)
            if marginal > best_score:
                best = card
                best_score = marginal
        if best is None:
            break
        if len(selected) >= _minimal_floor(questions, answers) and best_score <= 0.0:
            break
        add(best, "marginal_value", protected=False)

    selected.sort(key=_evidence_sort_key)
    selected_ids = {str(card.evidence_id) for card in selected}
    coverage = {
        "questions": _coverage_share(
            [q.question_id for q in questions],
            lambda qid: any(qid in c.retrieved_for_questions for c in selected),
        ),
        "supported_answers": _coverage_share(
            [
                a.question_id for a in answers
                if a.answer_status in {"supported", "partially_supported"}
            ],
            lambda qid: any(qid in c.retrieved_for_questions for c in selected),
        ),
        "hypotheses": _coverage_share(
            [h.id for h in hypotheses],
            lambda hid: any(
                hid in c.supports_hypotheses
                or hid in c.weakens_hypotheses
                or hid in c.contradicts_hypotheses
                for c in selected
            ),
        ),
        "has_counterevidence": any(
            c.weakens_hypotheses or c.contradicts_hypotheses for c in selected
        ),
        "has_action_anchor": any(
            c.source_type in {"commitment", "goal", "decision", "resource"}
            for c in selected
        ),
    }
    return selected, {
        "enabled": True,
        "input_count": len(cards),
        "selected_count": len(selected),
        "target_count": target,
        "dropped_count": max(0, len(cards) - len(selected)),
        "drop_ratio": round(
            (len(cards) - len(selected)) / max(1, len(cards)),
            4,
        ),
        "protected_count": len(protected_ids & selected_ids),
        "protected_reasons": {
            eid: sorted(reasons)
            for eid, reasons in protected_reasons.items()
            if eid in selected_ids
        },
        "coverage": coverage,
    }


def _minimal_evidence_target(
    cards: list[EvidenceCard],
    *,
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    evidence_limit: int,
) -> int:
    supported = sum(
        1 for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    question_count = len(questions)
    counter_bonus = 2 if any(
        c.weakens_hypotheses or c.contradicts_hypotheses for c in cards
    ) else 0
    action_bonus = 2 if any(
        c.source_type in {"commitment", "goal", "decision", "resource"}
        for c in cards
    ) else 0
    base = 8 if mode == "fast" or route == "FAST_PATH" else 9
    target = base + question_count * 2 + supported + counter_bonus + action_bonus
    if route == "BACKGROUND_PATH":
        target = min(target, 16)
    if mode == "fast" or route == "FAST_PATH":
        target = min(target, 14)
    else:
        target = min(target, 22)
    return min(max(1, int(evidence_limit)), max(1, target), len(cards))


def _protected_answer_ref_count(answers: list[QuestionAnswer]) -> int:
    refs: set[str] = set()
    for answer in answers:
        refs.update(answer.supporting_evidence[:2])
        refs.update(answer.counterevidence[:2])
    return len(refs)


def _minimal_floor(
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
) -> int:
    supported = sum(
        1 for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    return max(4, min(12, len(questions) + supported + 2))


def _evidence_sort_key(card: EvidenceCard) -> tuple[float, float, str]:
    return (
        -_evidence_value(card),
        -_timestamp_sort_value(card.timestamp),
        str(card.evidence_id),
    )


def _marginal_evidence_value(
    card: EvidenceCard,
    selected: list[EvidenceCard],
) -> float:
    value = _evidence_value(card)
    if _is_low_value_model_noise(card):
        value -= 0.32
    if card.source_type == "observation":
        value += 0.08
    if "sage_reader" in card.retrieval_paths:
        value += 0.06
    if card.supports_hypotheses or card.weakens_hypotheses or card.contradicts_hypotheses:
        value += 0.12
    value -= _redundancy_penalty(card, selected)
    return value


def _redundancy_penalty(
    card: EvidenceCard,
    selected: list[EvidenceCard],
) -> float:
    if not selected:
        return 0.0
    penalty = 0.0
    card_tokens = _material_tokens(card.summary.casefold())
    card_links = (
        frozenset(card.supports_hypotheses),
        frozenset(card.weakens_hypotheses),
        frozenset(card.contradicts_hypotheses),
    )
    for kept in selected:
        if card.source_ref == kept.source_ref:
            penalty += 1.0
            continue
        if card.source_type == kept.source_type and card_links == (
            frozenset(kept.supports_hypotheses),
            frozenset(kept.weakens_hypotheses),
            frozenset(kept.contradicts_hypotheses),
        ):
            penalty += 0.10
        kept_tokens = _material_tokens(kept.summary.casefold())
        if card_tokens and kept_tokens:
            overlap = len(card_tokens & kept_tokens) / max(
                1,
                min(len(card_tokens), len(kept_tokens)),
            )
            if overlap >= 0.82:
                penalty += 0.45
            elif overlap >= 0.58:
                penalty += 0.16
    return min(1.25, penalty)


def _coverage_share(
    items: list[str],
    predicate: Callable[[str], bool],
) -> float:
    if not items:
        return 1.0
    unique = list(dict.fromkeys(items))
    return round(
        sum(1 for item in unique if predicate(item)) / max(1, len(unique)),
        4,
    )


def _evidence_value(card: EvidenceCard) -> float:
    usefulness = card.score
    usefulness += 0.35 if card.supports_hypotheses else 0.0
    usefulness += 0.30 if card.contradicts_hypotheses or card.weakens_hypotheses else 0.0
    usefulness += 0.25 if card.source_type in {"commitment", "goal", "resource"} else 0.0
    usefulness += _trust_score(card.trust_tier)
    penalty = min(0.35, card.token_estimate / 5000.0)
    return usefulness - penalty


def _state_contract_for_context_packet(
    trigger: TriggerContext,
    evidence: list[EvidenceCard],
) -> dict[str, Any]:
    sources = [
        StateSource(
            source_kind=card.source_type,
            source_ref=card.raw_content_ref or f"{card.source_type}:{card.source_ref}",
            text=card.summary,
            occurred_at=card.timestamp,
            confidence=_evidence_card_confidence(card),
            metadata={
                "evidence_id": str(card.evidence_id),
                "retrieval_paths": sorted(card.retrieval_paths),
                "supports_hypotheses": sorted(card.supports_hypotheses),
                "weakens_hypotheses": sorted(card.weakens_hypotheses),
                "contradicts_hypotheses": sorted(card.contradicts_hypotheses),
                "trust_tier": card.trust_tier,
            },
        )
        for card in evidence
    ]
    return compile_state_contract(_trigger_text(trigger), sources).to_dict()


def _evidence_card_confidence(card: EvidenceCard) -> float:
    base = {
        "authoritative": 0.92,
        "reputable": 0.78,
        "model": 0.62,
        "low": 0.38,
    }.get(str(card.trust_tier or "").casefold(), 0.55)
    if card.contradicts_hypotheses or card.weakens_hypotheses:
        base += 0.06
    if card.supports_hypotheses:
        base += 0.04
    return round(max(0.05, min(0.99, base)), 2)


def _compile_context_packet(
    trigger: TriggerContext,
    route: SignalRoute,
    hypotheses: tuple[Hypothesis, ...],
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    evidence: list[EvidenceCard],
    sufficiency: SufficiencyVerdict,
    *,
    token_budget: int,
) -> dict[str, Any]:
    decisive: list[dict[str, Any]] = []
    supporting_groups: dict[str, list[EvidenceCard]] = {}
    omitted: list[dict[str, Any]] = []
    used_tokens = 0
    for card in evidence:
        item = _evidence_to_dict(card)
        cost = int(item.get("token_estimate") or 1)
        if used_tokens + cost <= token_budget and (
            card.contradicts_hypotheses
            or card.weakens_hypotheses
            or card.source_type in {"observation", "commitment", "goal", "decision", "resource"}
        ) and len(decisive) < 30:
            decisive.append(item)
            used_tokens += cost
            if card.supports_hypotheses:
                key = ",".join(sorted(card.supports_hypotheses))
                supporting_groups.setdefault(key, []).append(card)
        else:
            if _is_low_value_model_noise(card):
                omitted.append(
                    {
                        "source_ref": card.source_ref,
                        "reason": "retrieved model had no hypothesis link",
                        "expand_if": "debugging semantic recall or investigating missed classifier links",
                    }
                )
                continue
            key = ",".join(sorted(card.supports_hypotheses)) or card.source_type
            supporting_groups.setdefault(key, []).append(card)
    supporting = []
    for claim, cards in supporting_groups.items():
        shown = cards[:8]
        summary = _compact("; ".join(c.summary for c in shown), 600)
        cost = _estimate_tokens(summary)
        group = {
            "claim_supported": claim,
            "evidence_count": len(cards),
            "sources": sorted({c.source_type for c in cards}),
            "summary": summary,
            "evidence_ids": [str(c.evidence_id) for c in shown],
            "source_refs": [c.source_ref for c in shown],
        }
        if len(supporting) < 12 and used_tokens + cost <= token_budget:
            supporting.append(group)
            used_tokens += cost
        else:
            omitted.append(
                {
                    "group": claim,
                    "count": len(cards),
                    "reason": "context packet token budget reached",
                    "expand_if": "reasoning needs additional supporting evidence for this claim",
                }
            )
            continue
        if len(cards) > len(shown):
            omitted.append(
                {
                    "group": claim,
                    "count": len(cards) - len(shown),
                    "reason": "redundant with stronger selected evidence",
                    "expand_if": "deep reasoning needs additional provenance for this claim",
                }
            )
    background = []
    for item in _background_summaries(evidence):
        cost = _estimate_tokens(item.get("summary", ""))
        if used_tokens + cost <= token_budget:
            background.append(item)
            used_tokens += cost
        else:
            omitted.append(
                {
                    "group": f"background:{item.get('path')}",
                    "count": item.get("count", 0),
                    "reason": "context packet token budget reached",
                    "expand_if": "debugging retrieval pathway breadth",
                }
            )
    state_contract = _state_contract_for_context_packet(trigger, evidence)
    important_unknowns = _dedupe_unknowns(
        [
            *list(sufficiency.remaining_unknowns),
            *[
                slot
                for slot in state_contract.get("missing_slots", [])
                if slot != "premise_challenge"
            ],
        ]
    )
    return {
        "signal_summary": _compact(_trigger_text(trigger), 1000),
        "source_metadata": {
            "trigger_kind": trigger.kind,
            "observation_id": str(trigger.observation_id) if trigger.observation_id else None,
            "model_id": str(trigger.model_id) if trigger.model_id else None,
            "route": route,
        },
        "resolved_entities": _jsonable(trigger.seed_entity_ids),
        "hypotheses": [_jsonable(asdict(h)) for h in hypotheses],
        "question_path": [_jsonable(asdict(q)) for q in questions],
        "question_answers": [_jsonable(asdict(a)) for a in answers],
        "sufficiency_verdict": _jsonable(asdict(sufficiency)),
        "candidate_state_changes": _candidate_state_changes(hypotheses, evidence, sufficiency),
        "important_unknowns": important_unknowns,
        "state_contract": state_contract,
        "answer_obligations": {
            "required_slots": state_contract.get("required_slots", []),
            "covered_slots": state_contract.get("covered_slots", []),
            "missing_slots": state_contract.get("missing_slots", []),
            "premise_status": state_contract.get("premise_check", {}).get("status"),
        },
        "tiers": {
            "decisive_evidence": decisive,
            "supporting_evidence_groups": supporting,
            "background_summaries": background,
            "omission_ledger": omitted[:12],
        },
        "budget": {
            "token_budget": token_budget,
            "estimated_tokens_used": used_tokens,
            "reservoir_evidence_count": len(evidence),
        },
    }


def _candidate_state_changes(
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard],
    sufficiency: SufficiencyVerdict,
) -> list[dict[str, Any]]:
    if sufficiency.status not in {"sufficient_for_reasoning", "budget_exhausted"}:
        return []
    changes: list[dict[str, Any]] = []
    if any(c.source_type == "commitment" for c in evidence) and any(
        "H1" in c.supports_hypotheses for c in evidence
    ):
        changes.append(
            {
                "kind": "possible_act_update",
                "target": "commitment",
                "operation": "transition_or_risk_update",
                "reason": "risk evidence touches an existing commitment",
            }
        )
    if any(h.id == "H3" for h in hypotheses) and any(
        c.source_type == "model" and "H3" in c.supports_hypotheses for c in evidence
    ):
        changes.append(
            {
                "kind": "possible_model",
                "target": "pattern_or_situation",
                "operation": "create_or_update",
                "reason": "retrieved evidence suggests recurrence",
            }
        )
    return changes[:5]


def _dedupe_unknowns(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        clean = str(value or "").strip()
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _background_summaries(evidence: list[EvidenceCard]) -> list[dict[str, Any]]:
    by_path: dict[str, list[EvidenceCard]] = {}
    for card in evidence:
        for path in card.retrieval_paths:
            by_path.setdefault(path, []).append(card)
    out: list[dict[str, Any]] = []
    for path, cards in sorted(by_path.items()):
        summarizable = [card for card in cards if not _is_low_value_model_noise(card)]
        out.append(
            {
                "path": path,
                "count": len(cards),
                "sources": sorted({c.source_type for c in cards}),
                "summary": _compact("; ".join(c.summary for c in summarizable[:5]), 500),
            }
        )
    return out[:8]


def _is_low_value_model_noise(card: EvidenceCard) -> bool:
    return (
        card.source_type == "model"
        and not card.supports_hypotheses
        and not card.weakens_hypotheses
        and not card.contradicts_hypotheses
    )


async def _persist_inquiry(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    table_name = await conn.fetchval("SELECT to_regclass('public.inquiry_sessions')")
    if table_name is None:
        return
    tenant_exists = await conn.fetchval(
        "SELECT 1 FROM tenants WHERE id = $1",
        trigger.tenant_id,
    )
    if tenant_exists is None:
        return
    signal_ref_type = "observation" if trigger.observation_id else (
        "internal" if trigger.model_id is None else "internal"
    )
    signal_ref_id = trigger.observation_id or trigger.model_id
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
          id, tenant_id, signal_ref_type, signal_ref_id, route,
          status, stop_status, round_count, question_count,
          evidence_count, context_packet, notes, completed_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9,
          $10, $11::jsonb, $12::jsonb, now()
        )
        """,
        result.session_id,
        trigger.tenant_id,
        signal_ref_type,
        signal_ref_id,
        result.route,
        "completed",
        result.sufficiency.status,
        max((q.round_index for q in result.questions), default=0),
        len(result.questions),
        len(result.evidence_cards),
        json.dumps(result.context_packet, default=str),
        json.dumps(result.notes, default=str),
    )
    await _persist_sage_reader_activation_traces(conn, result, trigger)
    if result.questions:
        actions_by_question: dict[str, list[dict[str, Any]]] = {}
        for action in result.retrieval_actions:
            actions_by_question.setdefault(action.question_id, []).append(
                _jsonable(asdict(action))
            )
        answers_by_question = {
            answer.question_id: _jsonable(asdict(answer))
            for answer in result.question_answers
        }
        await conn.executemany(
            """
            INSERT INTO inquiry_question_runs (
              id, session_id, tenant_id, question_id, round_index,
              primitive, question, score, retrieval_actions, answer
            ) VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9::jsonb, $10::jsonb
            )
            """,
            [
                (
                    uuid7(),
                    result.session_id,
                    trigger.tenant_id,
                    question.question_id,
                    question.round_index,
                    question.primitive,
                    question.question,
                    float(question.score),
                    json.dumps(actions_by_question.get(question.question_id, [])),
                    json.dumps(answers_by_question.get(question.question_id, {})),
                )
                for question in result.questions
            ],
        )
    if not result.evidence_cards:
        await _emit_phase1_traces(conn, result, trigger)
        return
    await conn.executemany(
        """
        INSERT INTO inquiry_evidence_items (
          id, session_id, tenant_id, source_type, source_ref,
          source_ref_id, summary, trust_tier, occurred_at,
          retrieval_paths, retrieved_for_questions, supports_hypotheses,
          weakens_hypotheses, contradicts_hypotheses, raw_content_ref,
          token_estimate, access_scope, sensitivity, score
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9,
          $10::jsonb, $11::jsonb, $12::jsonb,
          $13::jsonb, $14::jsonb, $15,
          $16, $17, $18, $19
        )
        """,
        [
            (
                card.evidence_id,
                result.session_id,
                trigger.tenant_id,
                card.source_type,
                card.source_ref,
                card.source_ref_id,
                card.summary,
                card.trust_tier,
                card.timestamp,
                json.dumps(sorted(card.retrieval_paths)),
                json.dumps(sorted(card.retrieved_for_questions)),
                json.dumps(sorted(card.supports_hypotheses)),
                json.dumps(sorted(card.weakens_hypotheses)),
                json.dumps(sorted(card.contradicts_hypotheses)),
                card.raw_content_ref,
                card.token_estimate,
                card.access_scope,
                card.sensitivity,
                float(card.score),
            )
            for card in result.evidence_cards
        ],
    )
    await _emit_phase1_traces(conn, result, trigger)


async def _persist_sage_reader_activation_traces(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_reader_activations')"
    )
    if table_name is None:
        return
    try:
        from services.reasoning.sage.reader import activation_trace_insert_params
    except Exception:  # noqa: BLE001
        return
    sage_notes = (result.notes or {}).get("sage_reader")
    if not isinstance(sage_notes, dict):
        return
    questions = sage_notes.get("questions")
    if not isinstance(questions, dict):
        return
    params: list[tuple[Any, ...]] = []
    for qnote in questions.values():
        if not isinstance(qnote, dict):
            continue
        for raw_trace in qnote.get("activations", []) or []:
            if not isinstance(raw_trace, dict):
                continue
            try:
                from services.reasoning.sage.reader import ReaderActivationTrace
                trace = ReaderActivationTrace(
                    question_id=str(raw_trace["question_id"]),
                    model_id=UUID(str(raw_trace["model_id"])),
                    activation_score=float(raw_trace["activation_score"]),
                    activation_reasons=tuple(
                        str(r) for r in raw_trace.get("activation_reasons", [])
                    ),
                    selected=bool(raw_trace.get("selected", False)),
                    selection_rank=(
                        int(raw_trace["selection_rank"])
                        if raw_trace.get("selection_rank") is not None else None
                    ),
                    source_breakdown=dict(raw_trace.get("source_breakdown") or {}),
                )
            except (KeyError, TypeError, ValueError):
                continue
            params.append(
                activation_trace_insert_params(
                    tenant_id=trigger.tenant_id,
                    inquiry_session_id=result.session_id,
                    trace=trace,
                )
            )
    if not params:
        return
    await conn.executemany(
        """
        INSERT INTO sage_reader_activations (
          id, tenant_id, inquiry_session_id, question_id, model_id,
          activation_score, activation_reasons, selected, selection_rank,
          source_breakdown
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7::jsonb, $8, $9,
          $10::jsonb
        )
        ON CONFLICT (inquiry_session_id, question_id, model_id)
        DO UPDATE SET
          activation_score = EXCLUDED.activation_score,
          activation_reasons = EXCLUDED.activation_reasons,
          selected = EXCLUDED.selected,
          selection_rank = EXCLUDED.selection_rank,
          source_breakdown = EXCLUDED.source_breakdown
        """,
        params,
    )
    await _persist_sage_reader_decision_attributions(conn, result, trigger)


async def _persist_sage_reader_decision_attributions(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_reader_decision_attributions')"
    )
    if table_name is None:
        return
    sage_notes = (result.notes or {}).get("sage_reader")
    if not isinstance(sage_notes, dict):
        return
    questions = sage_notes.get("questions")
    if not isinstance(questions, dict):
        return

    question_by_id = {q.question_id: q for q in result.questions}
    actions_by_question: dict[str, list[dict[str, Any]]] = {}
    for action in result.retrieval_actions:
        actions_by_question.setdefault(action.question_id, []).append(
            _jsonable(asdict(action))
        )
    evidence_by_question = _packet_evidence_refs_by_question(result.evidence_cards)
    entities = _jsonable(trigger.seed_entity_ids)
    params: list[tuple[Any, ...]] = []
    nonselected_limit = _reader_attribution_nonselected_limit()
    nonselected_min_score = _reader_attribution_nonselected_min_score()

    for qid, qnote in questions.items():
        if not isinstance(qnote, dict):
            continue
        question = question_by_id.get(str(qid))
        if question is None:
            continue
        evidence_refs = evidence_by_question.get(str(qid), [])
        nonselected_kept = 0
        for raw_trace in qnote.get("activations", []) or []:
            if not isinstance(raw_trace, dict):
                continue
            try:
                model_id = UUID(str(raw_trace["model_id"]))
                activation_score = float(raw_trace["activation_score"])
                activation_reasons = [
                    str(r) for r in raw_trace.get("activation_reasons", [])
                ]
                selected = bool(raw_trace.get("selected", False))
                selection_rank = (
                    int(raw_trace["selection_rank"])
                    if raw_trace.get("selection_rank") is not None else None
                )
                source_breakdown = dict(raw_trace.get("source_breakdown") or {})
            except (KeyError, TypeError, ValueError):
                continue
            if not selected:
                if (
                    activation_score < nonselected_min_score
                    or nonselected_kept >= nonselected_limit
                ):
                    continue
                nonselected_kept += 1
            model_evidence_refs = [
                ref for ref in evidence_refs
                if ref.get("source_ref_id") == str(model_id)
                or ref.get("source_type") == "observation"
            ]
            params.append(
                (
                    uuid7(),
                    trigger.tenant_id,
                    result.session_id,
                    question.question_id,
                    question.primitive,
                    question.question,
                    float(question.score),
                    float(question.expected_value),
                    float(question.expected_cost),
                    trigger.kind,
                    json.dumps(entities, default=str),
                    model_id,
                    selected,
                    selection_rank,
                    activation_score,
                    json.dumps(activation_reasons, default=str),
                    json.dumps(source_breakdown, default=str),
                    json.dumps(actions_by_question.get(question.question_id, [])),
                    json.dumps(model_evidence_refs, default=str),
                    len(model_evidence_refs),
                )
            )
    if not params:
        return
    await conn.executemany(
        """
        INSERT INTO sage_reader_decision_attributions (
          id, tenant_id, inquiry_session_id,
          question_id, question_primitive, question,
          question_score, expected_value, expected_cost,
          signal_type, entities, model_id,
          selected, selection_rank, activation_score,
          activation_reasons, source_breakdown, retrieval_actions,
          projected_evidence_refs, evidence_in_packet_count
        ) VALUES (
          $1, $2, $3,
          $4, $5, $6,
          $7, $8, $9,
          $10, $11::jsonb, $12,
          $13, $14, $15,
          $16::jsonb, $17::jsonb, $18::jsonb,
          $19::jsonb, $20
        )
        ON CONFLICT (inquiry_session_id, question_id, model_id)
        DO UPDATE SET
          question_primitive = EXCLUDED.question_primitive,
          question = EXCLUDED.question,
          question_score = EXCLUDED.question_score,
          expected_value = EXCLUDED.expected_value,
          expected_cost = EXCLUDED.expected_cost,
          signal_type = EXCLUDED.signal_type,
          entities = EXCLUDED.entities,
          selected = EXCLUDED.selected,
          selection_rank = EXCLUDED.selection_rank,
          activation_score = EXCLUDED.activation_score,
          activation_reasons = EXCLUDED.activation_reasons,
          source_breakdown = EXCLUDED.source_breakdown,
          retrieval_actions = EXCLUDED.retrieval_actions,
          projected_evidence_refs = EXCLUDED.projected_evidence_refs,
          evidence_in_packet_count = EXCLUDED.evidence_in_packet_count,
          updated_at = now()
        """,
        params,
    )


def _packet_evidence_refs_by_question(
    evidence_cards: tuple[EvidenceCard, ...],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for card in evidence_cards:
        ref = {
            "evidence_id": str(card.evidence_id),
            "source_type": card.source_type,
            "source_ref": card.source_ref,
            "source_ref_id": str(card.source_ref_id) if card.source_ref_id else None,
            "score": float(card.score),
        }
        for question_id in card.retrieved_for_questions:
            out.setdefault(str(question_id), []).append(ref)
    return out


async def _emit_phase1_traces(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    """Write Phase 1 trace rows: retrieval_plans, omitted_evidence, and
    the packet inclusion/omission outcome events.

    Best-effort by design — the emitter helpers swallow per-row errors
    with a warning so a Sage write hiccup never aborts the inquiry
    persistence path. We still wrap the whole batch in a try/except
    because an unexpected import-time error (e.g. missing migration in
    a test DB) should NOT bring the existing pipeline down.
    """
    # Local import keeps the inquiry runtime free of an import-cycle
    # risk against services.reasoning.sage and lets the trace surface stay
    # optional in environments that haven't installed migration 0084.
    try:
        from services.reasoning.sage.inquiry_traces.emitter import (
            TraceContext,
            emission_enabled,
            emit_event,
            emit_omitted_evidence,
            emit_retrieval_plan,
            reset_trace_context,
            set_trace_context,
        )
    except Exception as exc:  # noqa: BLE001 — never block the pipeline
        import structlog
        structlog.get_logger(__name__).warning(
            "sage_trace.import_failed",
            session_id=str(result.session_id),
            error=str(exc),
        )
        return

    if not emission_enabled():
        return

    # Confirm the Phase 1 tables exist before any write attempts. The
    # repo path already swallows errors, but skipping early avoids
    # adding noise to every legacy / pre-0084 deployment's logs.
    plans_table = await conn.fetchval(
        "SELECT to_regclass('public.retrieval_plans')"
    )
    if plans_table is None:
        return

    ctx = TraceContext(
        tenant_id=trigger.tenant_id,
        inquiry_session_id=result.session_id,
        conn=conn,
        metadata={
            "trigger_kind": getattr(trigger, "kind", None),
            "route": result.route,
        },
    )
    token = set_trace_context(ctx)
    try:
        # --- 1. retrieval_plans (one per question, revision 0) -------
        # We reuse the planning data already computed on the question +
        # the action set the planner compiled — no second pass over the
        # LLM, no new fields on the question struct.
        actions_by_question: dict[str, list[RetrievalAction]] = {}
        for action in result.retrieval_actions:
            actions_by_question.setdefault(action.question_id, []).append(action)
        for question in result.questions:
            actions = actions_by_question.get(question.question_id, [])
            paths_payload = [
                {
                    "path": a.path,
                    "target": a.target,
                    "budget": int(a.budget),
                }
                for a in actions
            ]
            intents_payload = [
                {
                    "primitive": question.primitive,
                    "question": question.question,
                    "retrieval_target": question.retrieval_target,
                    "expected_value": float(question.expected_value),
                    "expected_cost": float(question.expected_cost),
                    "tests_hypotheses": list(question.tests_hypotheses),
                }
            ]
            budgets_payload = {
                "action_count": len(actions),
                "total_budget": sum(int(a.budget) for a in actions),
            }
            success_conditions_payload = (
                [{"stop_condition": question.stop_condition}]
                if question.stop_condition else []
            )
            notes_payload = {
                "round_index": int(question.round_index),
                "score": round(float(question.score), 4),
            }
            await emit_retrieval_plan(
                question_id=question.question_id,
                plan_revision=0,
                intents=intents_payload,
                paths=paths_payload,
                budgets=budgets_payload,
                success_conditions=success_conditions_payload,
                notes=notes_payload,
                ctx=ctx,
            )

        # --- 2. omitted_evidence + packet inclusion/omission events --
        # The packet builder already computes which cards are decisive
        # vs. supporting (grouped) vs. omitted via the tiers structure
        # and the omission_ledger. We re-derive the per-evidence-id set
        # of "made the packet" using the same packet dict so the trace
        # stays consistent with what the LLM actually saw.
        packet = result.context_packet or {}
        tiers = packet.get("tiers", {}) or {}
        decisive_ids: set[str] = set()
        for item in tiers.get("decisive_evidence", []) or []:
            ev_id = item.get("evidence_id")
            if ev_id:
                decisive_ids.add(str(ev_id))
        grouped_ids: set[str] = set()
        for group in tiers.get("supporting_evidence_groups", []) or []:
            for ev_id in group.get("evidence_ids", []) or []:
                grouped_ids.add(str(ev_id))
        used_ids = decisive_ids | grouped_ids
        budget_used = (packet.get("budget") or {}).get(
            "estimated_tokens_used", 0,
        )
        budget_cap = (packet.get("budget") or {}).get(
            "token_budget", 0,
        )

        for card in result.evidence_cards:
            ev_id_str = str(card.evidence_id)
            paths_payload = [
                {"path": p} for p in sorted(card.retrieval_paths)
            ]
            common_payload: dict[str, Any] = {
                "evidence_id": ev_id_str,
                "source_type": card.source_type,
                "source_ref_id": (
                    str(card.source_ref_id) if card.source_ref_id else None
                ),
                "source_ref": card.source_ref,
                "score": round(float(card.score), 4),
                "retrieval_paths": sorted(card.retrieval_paths),
            }
            if ev_id_str in used_ids:
                tier = (
                    "decisive" if ev_id_str in decisive_ids
                    else "supporting"
                )
                await emit_event(
                    "retrieved_evidence_used_in_packet",
                    {**common_payload, "tier": tier},
                    ctx=ctx,
                )
            else:
                # Pick the most specific omission reason we can infer
                # from the card. The packet compiler's omission_ledger
                # uses free text; we map to the closed enum so the
                # topology optimizer can group on a stable key.
                reason = _classify_omission_reason(
                    card,
                    packet_budget_cap=budget_cap,
                    packet_budget_used=budget_used,
                )
                first_question = (
                    sorted(card.retrieved_for_questions)[0]
                    if card.retrieved_for_questions else None
                )
                await emit_omitted_evidence(
                    source_type=card.source_type,
                    source_ref=card.source_ref,
                    source_ref_id=card.source_ref_id,
                    question_id=first_question,
                    retrieval_paths=paths_payload,
                    omission_reason=reason,
                    reason_detail="dropped during packet compilation",
                    score=float(card.score),
                    metadata={
                        "trust_tier": card.trust_tier,
                        "retrieved_for_questions": sorted(
                            card.retrieved_for_questions
                        ),
                        "supports_hypotheses": sorted(card.supports_hypotheses),
                        "weakens_hypotheses": sorted(card.weakens_hypotheses),
                        "contradicts_hypotheses": sorted(
                            card.contradicts_hypotheses
                        ),
                    },
                    ctx=ctx,
                )
                await emit_event(
                    "retrieved_evidence_omitted",
                    {**common_payload, "omission_reason": reason},
                    ctx=ctx,
                )
    finally:
        reset_trace_context(token)


def _classify_omission_reason(
    card: "EvidenceCard",
    *,
    packet_budget_cap: int,
    packet_budget_used: int,
) -> str:
    """Map an evidence card to one of the closed `OMISSION_REASONS`.

    The packet compiler's own logic is the source of truth for "what
    landed in the packet"; here we just produce a stable categorical
    tag for the topology optimizer. Rules:

      * model row with no hypothesis link → `generic_hub`
        (matches `_is_low_value_model_noise`)
      * cards crowded out when the packet is at/near its token cap →
        `budget_exhausted`
      * everything else → `redundant` (this is the fallback for
        supporting-evidence groups capped at N items per group, which
        is the dominant exclusion path in practice).
    """
    is_model_noise = (
        card.source_type == "model"
        and not card.supports_hypotheses
        and not card.weakens_hypotheses
        and not card.contradicts_hypotheses
    )
    if is_model_noise:
        return "generic_hub"
    if (
        packet_budget_cap > 0
        and packet_budget_used >= int(packet_budget_cap * 0.95)
    ):
        return "budget_exhausted"
    return "redundant"


def _evidence_to_dict(card: EvidenceCard) -> dict[str, Any]:
    return {
        "evidence_id": str(card.evidence_id),
        "source_type": card.source_type,
        "source_ref": card.source_ref,
        "summary": card.summary,
        "trust_tier": card.trust_tier,
        "timestamp": card.timestamp.isoformat() if card.timestamp else None,
        "retrieval_paths": sorted(card.retrieval_paths),
        "retrieved_for_questions": sorted(card.retrieved_for_questions),
        "supports_hypotheses": sorted(card.supports_hypotheses),
        "weakens_hypotheses": sorted(card.weakens_hypotheses),
        "contradicts_hypotheses": sorted(card.contradicts_hypotheses),
        "raw_content_ref": card.raw_content_ref,
        "token_estimate": card.token_estimate,
        "access_scope": card.access_scope,
        "sensitivity": card.sensitivity,
        "score": round(card.score, 4),
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(_jsonable(v) for v in value)
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return value


def _compact(text: Any, limit: int) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def _estimate_tokens(text: Any) -> int:
    return max(1, len(str(text or "")) // 4)


def _timestamp_sort_value(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _is_stale_relative_to_trigger(
    card: EvidenceCard,
    *,
    trigger_occurred_at: datetime | None,
    stale_after_days: int,
) -> bool:
    if card.timestamp is None or trigger_occurred_at is None:
        return False
    card_time = card.timestamp
    trigger_time = trigger_occurred_at
    if card_time.tzinfo is None:
        card_time = card_time.replace(tzinfo=timezone.utc)
    if trigger_time.tzinfo is None:
        trigger_time = trigger_time.replace(tzinfo=timezone.utc)
    return card_time < trigger_time - timedelta(days=max(1, stale_after_days))


def _is_counterevidence_for_leading_hypothesis(card: EvidenceCard) -> bool:
    return (
        "H1" in card.contradicts_hypotheses
        or "H1" in card.weakens_hypotheses
        or "H0" in card.supports_hypotheses
    )


def _evidence_supports_ownership(card: EvidenceCard) -> bool:
    lower = card.summary.casefold()
    if "owner=unassigned" in lower:
        return False
    if re.search(
        r"\b(no recorded|no accountable|missing|unresolved|unknown|unclear)\b.{0,40}\bowner\b",
        lower,
    ):
        return False
    if re.search(r"\bowner=[0-9a-f]{8}-[0-9a-f-]{27,}\b", lower):
        return True
    if card.source_type in {"observation", "model"}:
        if re.search(r"\b(owner|owns|responsible|assigned to|dri)\b", lower):
            return not any(
                marker in lower
                for marker in (
                    "owner unknown",
                    "owner unresolved",
                    "no owner",
                    "no recorded",
                    "missing owner",
                    "unassigned",
                    "pending owner",
                )
            )
    return False


_OVERLAP_STOPWORDS = {
    "about",
    "active",
    "again",
    "blocked",
    "blocker",
    "blocking",
    "commitment",
    "critical",
    "customer",
    "deadline",
    "delay",
    "deliver",
    "dependency",
    "goal",
    "issue",
    "launch",
    "missing",
    "owner",
    "promised",
    "resolved",
    "risk",
    "same",
    "ship",
    "signal",
    "the",
    "this",
    "unable",
    "unblocked",
}


def _material_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.casefold())
        if token not in _OVERLAP_STOPWORDS and not token.isdigit()
    }


def _has_material_trigger_overlap(summary_lower: str, trigger_lower: str) -> bool:
    trigger_tokens = _material_tokens(trigger_lower)
    if not trigger_tokens:
        return True
    summary_tokens = _material_tokens(summary_lower)
    return bool(trigger_tokens & summary_tokens)


def _declares_unrelated_to_trigger(summary_lower: str) -> bool:
    return bool(
        re.search(
            r"\b(unrelated to|not related to|different customer|wrong tenant|wrong account)\b",
            summary_lower,
        )
    )


def _trust_score(trust: str | None) -> float:
    if trust == "authoritative":
        return 0.30
    if trust == "authoritative_external":
        return 0.26
    if trust == "attested_agent":
        return 0.22
    if trust == "reputable":
        return 0.14
    if trust in {"inferential", "inferential_external"}:
        return 0.06
    if trust == "model":
        return 0.12
    return 0.04


def _sensitivity(text: Any) -> str:
    lower = str(text or "").casefold()
    if any(word in lower for word in ("password", "secret", "api key", "private key", "ssn")):
        return "sensitive"
    return "normal"


def _has_risk_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(blocked|blocker|cannot|can't|unable|risk|churn|escalat|incident|"
            r"outage|breach|failed|failure|delay|slip|overdue|urgent|critical)\b",
            lower,
        )
    )


def _has_dependency_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(depends?|dependency|critical path|binding constraint|"
            r"blocked by|tied to|requires?|reversed?|exception|policy|"
            r"approval depends|review depends)\b",
            lower,
        )
    )


def _has_constraint_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(constraint|constrained|capacity|quota|scarce|limited|"
            r"bottleneck|blocked by|shortage|policy exception|approval|"
            r"resourc(?:e|ing)|sandbox|license|budget|rate limit)\b",
            lower,
        )
    )


def _has_revenue_impact_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(revenue|renewal|churn|invoice|finance|pricing|arr|"
            r"sponsor|commercial|forecast|expansion)\b",
            lower,
        )
    )


def _has_commitment_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(promised|committed|commitment|deadline|due|deliver|ship|launch|"
            r"go-live|owner|approved|decision|agreed)\b",
            lower,
        )
    )


def _has_act_affecting_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(promised|committed|commitment|deadline|due|deliver|ship|launch|"
            r"go-live|owner|goal)\b",
            lower,
        )
    )


def _mentions_recurrence(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(recur(?:s|red|ring)?|repeated|again|several|multiple|pattern|systemic|"
            r"another|also|same issue|broader)\b",
            lower,
        )
    )


__all__ = [
    "EvidenceCard",
    "Hypothesis",
    "InquiryConfig",
    "InquiryQuestion",
    "InquiryResult",
    "QuestionAnswer",
    "RetrievalAction",
    "SufficiencyVerdict",
    "execution_retrieval_engine",
    "inquiry_enabled",
    "retrieve_for_execution",
    "run_inquiry_retrieval",
]
