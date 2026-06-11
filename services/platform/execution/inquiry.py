"""Adaptive inquiry retrieval runtime.

This module is the active implementation of the proposal's routed
retrieval loop. It keeps the existing retrieval pathways as low-level
executors, but wraps them in the production shape the architecture
calls for: baseline seeding, hypotheses, question planning, evidence
reservoir, sufficiency, and a compact context packet for reasoning.
"""
from __future__ import annotations

import asyncio
import hashlib
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

from lib.llm.provider import LLMProvider, using_usage_purpose
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

from .question_planning_provider import (
    question_planning_provider_metadata,
    select_question_planning_provider,
)


SignalRoute = Literal[
    "IGNORE_OR_ARCHIVE",
    "DETERMINISTIC_UPDATE",
    "FAST_PATH",
    "DEEP_INQUIRY_PATH",
    "BACKGROUND_PATH",
    "HUMAN_VALIDATION_PATH",
]

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
    "focused_index",
    "semantic",
    "temporal",
    "pattern",
    "model_edge",
    "sage_reader",
]

_BROAD_DISCOVERY_ACTION_PATHS = frozenset({"semantic", "temporal", "pattern"})
_READER_ATTRIBUTION_NONSELECTED_LIMIT_DEFAULT = 16
_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE_DEFAULT = 0.55
_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS = 1500
_SPARSE_STRONG_SINGLE_MATCH_MAX_DF = 32


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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_literal(name: str, default: str, allowed: set[str]) -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in allowed:
        return value
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


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _append_stage_timing(
    timings: list[dict[str, Any]],
    stage: str,
    started: float,
    **extra: Any,
) -> None:
    note = {
        "stage": stage,
        "elapsed_ms": _elapsed_ms(started),
    }
    for key, value in extra.items():
        if value is not None:
            note[key] = value
    timings.append(note)


def _sum_elapsed_ms(notes: list[dict[str, Any]]) -> int:
    total = 0
    for note in notes:
        try:
            total += int(note.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _runtime_residual_summary(
    *,
    total_ms: int,
    action_timings: list[dict[str, Any]],
    stage_timings: list[dict[str, Any]],
) -> dict[str, Any]:
    action_total = _sum_elapsed_ms(action_timings)
    stage_total = _sum_elapsed_ms(stage_timings)
    return {
        "total_ms": total_ms,
        "retrieval_action_timings_ms_total": action_total,
        "retrieval_stage_timings_ms_total": stage_total,
        "measured_ms_total": action_total + stage_total,
        "unaccounted_ms": max(0, total_ms - action_total - stage_total),
    }


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
    context_packet_evidence_mode: str = "model_first"
    temporal_window_days: int = 30
    semantic_budget: int = 30
    semantic_hybrid_lexical_enabled: bool = True
    semantic_hybrid_lexical_max_candidates: int = 24
    semantic_hybrid_lexical_terms: int = 8
    semantic_hybrid_lexical_per_term_limit: int = 12
    focused_index_enabled: bool = True
    focused_index_terms: int = 12
    focused_index_max_candidates: int = 48
    focused_index_scope_candidates: int = 18
    retrieval_motifs_enabled: bool = True
    retrieval_motif_min_successes: int = 1
    retrieval_motif_max_actions: int = 5
    retrieval_motif_match_threshold: float = 0.34
    question_action_parallel_enabled: bool = True
    question_action_parallelism: int = 6
    structural_max_hops: int = 2
    structural_read_fanout_enabled: bool = False
    structural_read_fanout_min_seeds: int = 16
    structural_read_fanout_chunk_size: int = 8
    model_edge_max_hops: int = 2
    llm_question_planning_enabled: bool = True
    llm_question_temperature: float = 0.0
    llm_question_max_tokens: int = 900
    sage_reader_enabled: bool = True
    sage_reader_row_cache_enabled: bool = True
    sage_reader_shared_substrate_enabled: bool = True
    sage_reader_parallel_enabled: bool = True
    sage_reader_parallelism: int = 2
    sage_reader_gate_broad_actions: bool = True
    persist_full_sage_reader_notes: bool = False
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
            context_packet_evidence_mode=_env_literal(
                "INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE",
                "model_first",
                {"all", "model_first", "models_only"},
            ),
            temporal_window_days=int(os.environ.get("INQUIRY_TEMPORAL_WINDOW_DAYS", "30")),
            semantic_budget=int(os.environ.get("INQUIRY_SEMANTIC_BUDGET", "30")),
            semantic_hybrid_lexical_enabled=_env_bool(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_ENABLED", True
            ),
            semantic_hybrid_lexical_max_candidates=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_MAX_CANDIDATES",
                24,
                minimum=1,
            ),
            semantic_hybrid_lexical_terms=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_TERMS",
                8,
                minimum=1,
            ),
            semantic_hybrid_lexical_per_term_limit=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_PER_TERM_LIMIT",
                12,
                minimum=1,
            ),
            focused_index_enabled=_env_bool(
                "INQUIRY_FOCUSED_INDEX_ENABLED", True
            ),
            focused_index_terms=_env_int(
                "INQUIRY_FOCUSED_INDEX_TERMS",
                12,
                minimum=1,
            ),
            focused_index_max_candidates=_env_int(
                "INQUIRY_FOCUSED_INDEX_MAX_CANDIDATES",
                48,
                minimum=1,
            ),
            focused_index_scope_candidates=_env_int(
                "INQUIRY_FOCUSED_INDEX_SCOPE_CANDIDATES",
                18,
                minimum=1,
            ),
            retrieval_motifs_enabled=_env_bool(
                "INQUIRY_RETRIEVAL_MOTIFS_ENABLED", True
            ),
            retrieval_motif_min_successes=_env_int(
                "INQUIRY_RETRIEVAL_MOTIF_MIN_SUCCESSES",
                1,
                minimum=0,
            ),
            retrieval_motif_max_actions=_env_int(
                "INQUIRY_RETRIEVAL_MOTIF_MAX_ACTIONS",
                5,
                minimum=1,
            ),
            retrieval_motif_match_threshold=float(
                os.environ.get("INQUIRY_RETRIEVAL_MOTIF_MATCH_THRESHOLD", "0.34")
            ),
            question_action_parallel_enabled=_env_bool(
                "INQUIRY_QUESTION_ACTION_PARALLEL_ENABLED", True
            ),
            question_action_parallelism=_env_int(
                "INQUIRY_QUESTION_ACTION_PARALLELISM",
                6,
                minimum=1,
            ),
            structural_max_hops=int(os.environ.get("INQUIRY_STRUCTURAL_MAX_HOPS", "2")),
            structural_read_fanout_enabled=os.environ.get(
                "INQUIRY_STRUCTURAL_READ_FANOUT_ENABLED",
                "0",
            ).strip().lower() in {"1", "true", "yes", "on"},
            structural_read_fanout_min_seeds=int(
                os.environ.get("INQUIRY_STRUCTURAL_READ_FANOUT_MIN_SEEDS", "16")
            ),
            structural_read_fanout_chunk_size=int(
                os.environ.get("INQUIRY_STRUCTURAL_READ_FANOUT_CHUNK_SIZE", "8")
            ),
            model_edge_max_hops=int(os.environ.get("INQUIRY_MODEL_EDGE_MAX_HOPS", "2")),
            llm_question_planning_enabled=os.environ.get(
                "INQUIRY_LLM_QUESTION_PLANNING_ENABLED",
                "1",
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off"},
            llm_question_temperature=float(
                os.environ.get("INQUIRY_LLM_QUESTION_TEMPERATURE", "0.0")
            ),
            llm_question_max_tokens=int(
                os.environ.get("INQUIRY_LLM_QUESTION_MAX_TOKENS", "900")
            ),
            sage_reader_enabled=os.environ.get(
                "SAGE_READER_ENABLED", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            sage_reader_row_cache_enabled=os.environ.get(
                "SAGE_READER_ROW_CACHE_ENABLED", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            sage_reader_shared_substrate_enabled=os.environ.get(
                "SAGE_READER_SHARED_SUBSTRATE_ENABLED", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            sage_reader_parallel_enabled=os.environ.get(
                "SAGE_READER_PARALLEL_ENABLED", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            sage_reader_parallelism=max(
                1,
                int(os.environ.get("SAGE_READER_PARALLELISM", "2")),
            ),
            sage_reader_gate_broad_actions=os.environ.get(
                "SAGE_READER_GATE_BROAD_ACTIONS", "1"
            ).strip().lower() not in {"0", "false", "no", "off", ""},
            persist_full_sage_reader_notes=os.environ.get(
                "SAGE_READER_PERSIST_FULL_NOTES", "0"
            ).strip().lower() in {"1", "true", "yes", "on"},
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


class LLMBeliefDeltaSpec(BaseModel):
    delta_id: str | None = Field(default=None, max_length=96)
    claim_atom: str = Field(
        min_length=8,
        max_length=240,
        description="Atomic belief candidate implied by the signal.",
    )
    delta_type: str = Field(
        default="update",
        max_length=32,
        description=(
            "One of create, update, weaken, split, merge, supersede, no_op."
        ),
    )
    target_model_ids: list[str] = Field(default_factory=list, max_length=5)
    affected_entities: list[str] = Field(default_factory=list, max_length=8)
    uncertainty_slots: list[str] = Field(default_factory=list, max_length=8)
    evidence_needed: list[str] = Field(default_factory=list, max_length=8)
    impact_if_true: str = Field(default="medium", max_length=16)
    confidence: float = Field(default=0.45, ge=0.0, le=1.0)


class LLMInquiryQuestionPlan(BaseModel):
    rationale: str | None = Field(default=None, max_length=500)
    belief_deltas: list[LLMBeliefDeltaSpec] = Field(
        default_factory=list,
        max_length=5,
    )
    questions: list[LLMInquiryQuestionSpec] = Field(default_factory=list, max_length=6)


class LLMCompactQuestionSpec(BaseModel):
    p: str = Field(max_length=32)
    q: str = Field(min_length=8, max_length=180)
    v: float = Field(default=0.74, ge=0.0, le=1.0)
    c: float = Field(default=0.24, ge=0.0, le=1.0)


class LLMCompactBeliefDeltaSpec(BaseModel):
    i: str | None = Field(default=None, max_length=96)
    claim: str = Field(min_length=8, max_length=220)
    type: str = Field(default="update", max_length=32)
    entities: list[str] = Field(default_factory=list, max_length=6)
    slots: list[str] = Field(default_factory=list, max_length=5)
    evidence: list[str] = Field(default_factory=list, max_length=5)
    impact: str = Field(default="medium", max_length=16)
    conf: float = Field(default=0.45, ge=0.0, le=1.0)


class LLMCompactQuestionPlan(BaseModel):
    r: str | None = Field(default=None, max_length=300)
    d: list[LLMCompactBeliefDeltaSpec] = Field(default_factory=list, max_length=4)
    q: list[LLMCompactQuestionSpec] = Field(default_factory=list, max_length=3)


@dataclass(frozen=True, slots=True)
class Hypothesis:
    id: str
    claim: str
    confidence: float
    impact_if_true: str
    delta_type: str | None = None
    target_model_ids: tuple[str, ...] = ()
    affected_entities: tuple[str, ...] = ()
    uncertainty_slots: tuple[str, ...] = ()
    evidence_needed: tuple[str, ...] = ()
    source: str = "deterministic"


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
class LearnedRetrievalMotif:
    id: UUID
    signature: dict[str, Any]
    question_primitive: str
    plan: dict[str, Any]
    utility_score: float
    success_count: int
    match_score: float


@dataclass(frozen=True, slots=True)
class _RetrievalMotifPenalty:
    motif_id: UUID
    question_id: str
    cost: float
    reasons: tuple[str, ...]
    selected_evidence: int = 0
    omitted_evidence: int = 0
    returned_models: int = 0
    returned_observations: int = 0


@dataclass(slots=True)
class _QuestionRetrievalPlan:
    question: InquiryQuestion
    sage_result: RetrievalResult | None = None
    sage_action: RetrievalAction | None = None
    action_gate_scope: Literal["all", "broad"] | None = None
    action_gate_reason: str | None = None
    actions_to_run: list[RetrievalAction] = field(default_factory=list)
    skipped_timing_notes: list[dict[str, Any]] = field(default_factory=list)
    learned_motif: LearnedRetrievalMotif | None = None


@dataclass(slots=True)
class _ActionExecutionRecord:
    action: RetrievalAction
    path_result: PathwayResult | None
    timing_note: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _FocusedIndexHit:
    model_id: UUID
    score: float
    source: str
    match_count: int = 0
    scope_overlap: int = 0


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
    cfg = config or InquiryConfig.from_env()
    route = route or ("FAST_PATH" if mode == "fast" else _route_for_trigger(trigger))
    session_id = uuid7()
    candidate_top_n = min(top_n, max(1, int(cfg.candidate_model_limit)))
    effective_top_n = min(candidate_top_n, max(1, int(cfg.result_model_limit)))
    signal_class = _signal_class_for_trigger(trigger)
    weak_signal = signal_class == "weak"
    cold_weak_noop_gate = _cold_weak_noop_gate(trigger, signal_class)
    baseline_top_n = _adaptive_baseline_top_n(candidate_top_n, signal_class)
    stage_timing_notes: list[dict[str, Any]] = []

    stage_started = time.perf_counter()
    if cold_weak_noop_gate["used"]:
        baseline = _merge_results(
            trigger,
            [],
            top_n=0,
            note_prefix="cold_weak_noop",
            config=cfg,
        )
        _append_stage_timing(
            stage_timing_notes,
            "primary_retrieve",
            stage_started,
            skipped=True,
            reason=str(cold_weak_noop_gate["reason"]),
            models=len(baseline.models),
            observations=len(baseline.observations),
        )
    else:
        baseline = await primary_retrieve(
            trigger,
            conn,
            embedder=embedder,
            read_pool=read_pool,
            structural_read_fanout_enabled=cfg.structural_read_fanout_enabled,
            structural_read_fanout_min_seeds=cfg.structural_read_fanout_min_seeds,
            structural_read_fanout_chunk_size=cfg.structural_read_fanout_chunk_size,
            top_n=baseline_top_n,
        )
        _append_stage_timing(
            stage_timing_notes,
            "primary_retrieve",
            stage_started,
            models=len(baseline.models),
            observations=len(baseline.observations),
            pathways_run=list((baseline.notes or {}).get("pathways_run", []) or []),
            primary_pathway_timings=list(
                (baseline.notes or {}).get("pathway_timings", []) or []
            ),
        )
    stage_started = time.perf_counter()
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
    _append_stage_timing(
        stage_timing_notes,
        "baseline_reservoir_seed",
        stage_started,
        hypotheses=len(hypotheses),
        evidence=len(evidence_by_key),
    )
    stage_started = time.perf_counter()
    question_policy = await _load_question_policy_stats(
        conn,
        tenant_id=trigger.tenant_id,
        signal_type=trigger.kind,
    )
    _append_stage_timing(
        stage_timing_notes,
        "question_policy_load",
        stage_started,
        policies=len(question_policy),
    )
    sage_reader_notes: dict[str, Any] = {
        "enabled": bool(cfg.sage_reader_enabled),
        "row_cache_enabled": bool(cfg.sage_reader_row_cache_enabled),
        "shared_substrate_enabled": bool(cfg.sage_reader_shared_substrate_enabled),
        "parallel_enabled": bool(cfg.sage_reader_parallel_enabled),
        "parallelism": int(cfg.sage_reader_parallelism),
        "gate_broad_actions": bool(cfg.sage_reader_gate_broad_actions),
        "questions": {},
        "signatures": [],
        "selected_model_ids": [],
        "projected_evidence_count": 0,
        "activation_trace_count": 0,
    }
    sage_reader_runtime: Any | None = None
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
    if max_rounds > 0:
        sage_reader_runtime = _build_sage_reader(cfg)
        if cfg.sage_reader_enabled and sage_reader_runtime is None:
            sage_reader_notes["enabled"] = False
            sage_reader_notes["initialization_failed"] = True
    sage_reader_substrate: Any | None = None
    if (
        sage_reader_runtime is not None
        and cfg.sage_reader_shared_substrate_enabled
        and hasattr(sage_reader_runtime, "prepare_substrate")
    ):
        stage_started = time.perf_counter()
        try:
            sage_reader_substrate = await sage_reader_runtime.prepare_substrate(
                conn=conn,
                tenant_id=trigger.tenant_id,
                trigger=trigger,
                baseline_models=tuple(baseline.models[:candidate_top_n]),
            )
            sage_reader_notes["substrate"] = {
                "prepared": True,
                "model_count": int(getattr(sage_reader_substrate, "model_count", 0)),
                "counters": dict(
                    sorted(getattr(sage_reader_substrate, "counters", {}).items())
                ),
                "timings_ms": dict(
                    getattr(sage_reader_substrate, "timings_ms", {}) or {}
                ),
            }
        except Exception as exc:  # noqa: BLE001
            sage_reader_substrate = None
            sage_reader_notes["substrate"] = {
                "prepared": False,
                "error": type(exc).__name__,
            }
        _append_stage_timing(
            stage_timing_notes,
            "sage_substrate_prepare",
            stage_started,
            prepared=sage_reader_substrate is not None,
            models=int(getattr(sage_reader_substrate, "model_count", 0))
            if sage_reader_substrate is not None else 0,
        )
    stop_status: InquiryStopStatus = "insufficient_continue"
    stop_reason = "inquiry has not run"

    for round_index in range(1, max_rounds + 1):
        stage_started = time.perf_counter()
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
        _append_stage_timing(
            stage_timing_notes,
            "question_planning",
            stage_started,
            round_index=round_index,
            candidates=len(candidate_questions),
            selected=len(selected),
            mode=planning_note.get("mode"),
        )
        if not selected:
            stop_status = "insufficient_defer"
            stop_reason = "no high-value unanswered questions remained"
            break

        sage_results_by_qid, sage_batch_note = await _execute_sage_reader_actions_for_round(
            selected,
            trigger,
            conn,
            cfg,
            reader=sage_reader_runtime,
            substrate=sage_reader_substrate,
            hypotheses=hypotheses,
            read_pool=read_pool,
        )
        sage_reader_notes.setdefault("batches", []).append({
            **sage_batch_note,
            "round_index": round_index,
        })
        learned_motifs = await _load_retrieval_motifs_for_questions(
            conn,
            trigger,
            selected,
            cfg,
        )

        question_read_plans: list[_QuestionRetrievalPlan] = []
        for question in selected:
            all_questions.append(question)
            policy_signal = question_policy.get(question.primitive)
            actions = _compile_retrieval_plan(
                question,
                trigger,
                cfg,
                policy_signal=policy_signal,
                learned_motif=learned_motifs.get(question.question_id),
            )
            sage_result = sage_results_by_qid.get(question.question_id)
            sage_action: RetrievalAction | None = None
            action_gate_scope: Literal["all", "broad"] | None = None
            action_gate_reason: str | None = None
            if sage_result is not None:
                sage_action = RetrievalAction(
                    question.question_id,
                    "sage_reader",
                    "synthesis_reader",
                    query=question.question,
                    budget=cfg.result_model_limit,
                )
                action_gate_scope, action_gate_reason = _sage_reader_action_gate(
                    sage_result,
                    gate_broad_actions=cfg.sage_reader_gate_broad_actions,
                )

            actions_to_run: list[RetrievalAction] = []
            skipped_timing_notes: list[dict[str, Any]] = []
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
                    skipped_timing_notes.append({
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

            question_read_plans.append(_QuestionRetrievalPlan(
                question=question,
                sage_result=sage_result,
                sage_action=sage_action,
                action_gate_scope=action_gate_scope,
                action_gate_reason=action_gate_reason,
                actions_to_run=actions_to_run,
                skipped_timing_notes=skipped_timing_notes,
                learned_motif=learned_motifs.get(question.question_id),
            ))

        action_records_by_qid = await _execute_question_retrieval_actions(
            question_read_plans,
            trigger,
            conn,
            embedder,
            cfg,
            action_cache,
            read_pool=read_pool,
        )

        for plan in question_read_plans:
            question = plan.question
            action_results: list[RetrievalResult] = []
            if plan.sage_result is not None:
                action_timing_notes.append({
                    "question_id": question.question_id,
                    "path": "sage_reader",
                    "target": "synthesis_reader",
                    "elapsed_ms": _sage_reader_total_ms(plan.sage_result),
                    "cache_hit": False,
                    "returned": True,
                    "models": len(plan.sage_result.models),
                    "observations": len(plan.sage_result.observations),
                    "resources": len(plan.sage_result.resources),
                    "source_pathway": "SAGE",
                })
                if plan.sage_action is not None:
                    all_actions.append(plan.sage_action)
                action_results.append(plan.sage_result)
                _add_result_to_reservoir(
                    evidence_by_key,
                    plan.sage_result,
                    path="sage_reader",
                    question_id=question.question_id,
                    hypotheses=hypotheses,
                    score_hint=max(0.0, question.score),
                )
                _record_sage_reader_notes(
                    sage_reader_notes, question, plan.sage_result,
                )
            action_timing_notes.extend(plan.skipped_timing_notes)
            all_actions.extend(plan.actions_to_run)
            for record in action_records_by_qid.get(question.question_id, []):
                action_timing_notes.append(record.timing_note)
                if record.path_result is None:
                    continue
                rr = _result_from_pathway(trigger, record.path_result, record.action)
                action_results.append(rr)
                _add_result_to_reservoir(
                    evidence_by_key,
                    rr,
                    path=record.action.path,
                    question_id=question.question_id,
                    hypotheses=hypotheses,
                    score_hint=max(0.0, question.score),
                )
            if action_results:
                stage_started = time.perf_counter()
                merged_for_question = _merge_results(
                    trigger,
                    action_results,
                    top_n=candidate_top_n,
                    note_prefix=f"question_{question.question_id}",
                )
                _append_stage_timing(
                    stage_timing_notes,
                    "question_result_merge",
                    stage_started,
                    question_id=question.question_id,
                    models=len(merged_for_question.models),
                    observations=len(merged_for_question.observations),
                )
                retrieval_results.append(merged_for_question)
            stage_started = time.perf_counter()
            answer = _answer_question(
                question,
                evidence_by_key,
                trigger_occurred_at=trigger.seed_occurred_at,
                stale_after_days=cfg.temporal_window_days,
            )
            _append_stage_timing(
                stage_timing_notes,
                "question_answer",
                stage_started,
                question_id=question.question_id,
                evidence=len(evidence_by_key),
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
    stage_started = time.perf_counter()
    if cold_weak_noop_gate["used"] or sage_controller_notes["global_negative_route_gate"]:
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
        stage_started = time.perf_counter()
        await _persist_inquiry(
            conn,
            result,
            trigger,
            persist_full_sage_reader_notes=cfg.persist_full_sage_reader_notes,
        )
        _append_stage_timing(
            stage_timing_notes,
            "persist_inquiry",
            stage_started,
        )
        result.notes["retrieval_runtime"] = _runtime_residual_summary(
            total_ms=_elapsed_ms(total_started),
            action_timings=action_timing_notes,
            stage_timings=stage_timing_notes,
        )
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
    anchors = _question_anchors(trigger)
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
                    anchors.focus or text,
                    fallback="The signal describes a real operational blocker or risk.",
                ),
                confidence=0.46,
                impact_if_true="high",
                delta_type="update",
                affected_entities=tuple(
                    _question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=tuple(_deterministic_delta_uncertainties(lower)),
                evidence_needed=(
                    "fresh signal evidence",
                    "related active commitments",
                    "recent counterevidence",
                ),
            )
        )
    if commitment:
        hypotheses.append(
            Hypothesis(
                id="H2",
                claim=(
                    f"An active commitment, owner, or promised outcome is affected by "
                    f"{anchors.focus}."
                ),
                confidence=0.36,
                impact_if_true="medium",
                delta_type="update",
                affected_entities=tuple(
                    _question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "which active commitment is affected",
                    "who owns the next action",
                    "which deadline or promised outcome is at risk",
                ),
                evidence_needed=(
                    "active commitments",
                    "commitment owners",
                    "recent owner or decision evidence",
                ),
            )
        )
    if _mentions_recurrence(lower) or len(baseline.models) >= 3:
        hypotheses.append(
            Hypothesis(
                id="H3",
                claim=f"{anchors.focus} may be part of a broader recurring pattern.",
                confidence=0.29,
                impact_if_true="high" if risk else "medium",
                delta_type="create" if not baseline.models else "update",
                affected_entities=tuple(
                    _question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "whether this pattern has appeared before",
                    "which prior models support or weaken the recurrence claim",
                ),
                evidence_needed=(
                    "similar prior observations",
                    "related pattern models",
                    "model edges to comparable situations",
                ),
            )
        )
    if not hypotheses:
        hypotheses.append(
            Hypothesis(
                id="H1",
                claim=f"{anchors.focus} may add localized context to existing memory.",
                confidence=0.30,
                impact_if_true="medium",
                delta_type="update",
                affected_entities=tuple(
                    _question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "which existing model, if any, should absorb this signal",
                    "whether this is already captured",
                ),
                evidence_needed=("nearby existing models", "recent observations"),
            )
        )
    hypotheses.append(
        Hypothesis(
            id="H0",
            claim="The signal is local noise or already captured and requires no Synthesis update.",
            confidence=0.16 if risk or commitment else 0.32,
            impact_if_true="low",
            delta_type="no_op",
            uncertainty_slots=(
                "whether the signal is already captured",
                "whether no model update is needed",
            ),
            evidence_needed=("existing matching models", "counterevidence"),
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


def _deterministic_delta_uncertainties(lower: str) -> list[str]:
    slots: list[str] = []
    if _has_dependency_language(lower) or _has_risk_language(lower):
        slots.append("whether the blocker is actually on the critical path")
    if _has_constraint_language(lower):
        slots.append("which resource, policy, or capacity constraint is binding")
    if _has_revenue_impact_language(lower):
        slots.append("which customer goal or revenue path is at risk")
    if _has_commitment_language(lower):
        slots.append("which active commitment or promised outcome is affected")
    if "owner" in lower or "who" in lower or _has_risk_language(lower):
        slots.append("who owns the next action")
    if _mentions_recurrence(lower):
        slots.append("whether this has appeared before")
    slots.append("what evidence would weaken this interpretation")
    return _dedupe_unknowns(slots)


@dataclass(frozen=True, slots=True)
class _QuestionAnchors:
    subject: str
    claim: str
    focus: str
    constraint: str | None = None


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
    anchors = _question_anchors(trigger)
    out = [
        InquiryQuestion(
            question_id="Q_CRITICAL_PATH",
            question=_specific_question("DEPENDENCY", anchors),
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
            question=_specific_question("COMMITMENT", anchors),
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
            question=_specific_question("CONSTRAINT", anchors),
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
            question=_specific_question("COUNTEREVIDENCE", anchors),
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
            question=_specific_question("OWNERSHIP", anchors),
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
            question=_specific_question("GOAL_IMPACT", anchors),
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
            question=_specific_question("RECURRENCE", anchors),
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


def _question_anchors(trigger: TriggerContext) -> _QuestionAnchors:
    text = _trigger_text(trigger)
    claim = _claim_from_text(text, fallback="this signal")
    entity_labels = _question_entity_labels(trigger)
    subject = _question_subject(text, entity_labels)
    focus = _question_focus_phrase(text, subject=subject)
    constraint = _question_constraint_phrase(text)
    return _QuestionAnchors(
        subject=subject,
        claim=claim,
        focus=focus,
        constraint=constraint,
    )


def _question_entity_labels(trigger: TriggerContext) -> tuple[str, ...]:
    labels: list[str] = []
    for raw_entity in trigger.seed_entity_ids[:8]:
        if not isinstance(raw_entity, dict):
            continue
        label = _entity_label_from_seed(raw_entity)
        if not label:
            continue
        if label.casefold() in {existing.casefold() for existing in labels}:
            continue
        labels.append(label)
    return tuple(labels[:4])


def _entity_label_from_seed(entity: dict[str, Any]) -> str | None:
    for key in ("label", "name", "title", "natural", "slug", "id"):
        value = entity.get(key)
        if value is None:
            continue
        label = _clean_question_anchor(str(value))
        if not label or _looks_like_machine_identifier(label):
            continue
        return label
    return None


def _question_subject(text: str, entity_labels: tuple[str, ...]) -> str:
    if entity_labels:
        return _clean_question_anchor(", ".join(entity_labels[:3])) or "this signal"
    spans = _capitalized_anchor_spans(text)
    if spans:
        return _clean_question_anchor(", ".join(spans[:3])) or "this signal"
    return "this signal"


def _capitalized_anchor_spans(text: str) -> tuple[str, ...]:
    spans: list[str] = []
    pattern = re.compile(
        r"\b(?:[A-Z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*"
        r"(?:\s+[A-Z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*){0,2})\b"
    )
    stop = {
        "Board",
        "Company",
        "Customer",
        "Customers",
        "Data",
        "Goal",
        "Issue",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    }
    for match in pattern.finditer(text or ""):
        span = _clean_question_anchor(match.group(0))
        if not span or span in stop or _looks_like_machine_identifier(span):
            continue
        if span.casefold() not in {existing.casefold() for existing in spans}:
            spans.append(span)
        if len(spans) >= 4:
            break
    return tuple(spans)


def _question_constraint_phrase(text: str) -> str | None:
    clean = " ".join((text or "").split())
    if not clean:
        return None
    patterns = (
        r"\bblocked by\s+([^.;,]+)",
        r"\bconstrained by\s+([^.;,]+)",
        r"\bdepends on\s+([^.;,]+)",
        r"\bwaiting on\s+([^.;,]+)",
        r"\bdue to\s+([^.;,]+)",
        r"\bbecause\s+([^.;,]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, clean, flags=re.IGNORECASE)
        if not match:
            continue
        phrase = re.split(r"\s+(?:and|but|while|which|that)\s+", match.group(1))[0]
        phrase = _clean_question_anchor(phrase)
        if phrase:
            return _truncate_text(phrase, 90)
    return None


def _question_focus_phrase(text: str, *, subject: str) -> str:
    clean = " ".join((text or "").split())
    if not clean:
        return subject
    clean = re.sub(r"^\[[^\]]+\]\s*", "", clean).strip()
    candidates: list[tuple[float, str]] = []

    before_context, sep, after_context = clean.partition("Company context:")
    preface_focus = _focus_from_preface(before_context)
    if preface_focus:
        candidates.append((1.2, preface_focus))
    candidate_text = after_context if sep else clean
    for index, sentence in enumerate(_focus_sentences(candidate_text)):
        if _looks_like_company_overview(sentence):
            continue
        score = _focus_sentence_score(sentence) - index * 0.05
        if score <= 0.0:
            continue
        candidates.append((score, sentence))

    if not candidates:
        return _truncate_text(subject, 120)
    _, best = max(candidates, key=lambda item: (item[0], len(item[1])))
    return _truncate_text(best, 140)


def _focus_from_preface(text: str) -> str | None:
    clean = _clean_question_anchor(text)
    if not clean:
        return None
    match = re.search(r"\brelates to\s+(.+)$", clean, flags=re.IGNORECASE)
    if match:
        return _clean_question_anchor(match.group(1))
    return _truncate_text(clean, 120)


def _focus_sentences(text: str) -> tuple[str, ...]:
    out: list[str] = []
    for raw in re.split(r"(?<=[.!?])\s+", text or ""):
        sentence = _clean_question_anchor(raw)
        if sentence:
            out.append(sentence)
    return tuple(out)


def _looks_like_company_overview(sentence: str) -> bool:
    lower = sentence.casefold()
    return (
        "post-product-market fit" in lower
        or "months runway" in lower
        or "people in " in lower
        or "series " in lower
    )


def _focus_sentence_score(sentence: str) -> float:
    lower = sentence.casefold()
    keywords = (
        "approve",
        "approval",
        "asked",
        "at risk",
        "blocker",
        "blocked",
        "capacity",
        "cannot",
        "conflict",
        "constrained",
        "delayed",
        "dependency",
        "edge case",
        "expansion",
        "falsifier",
        "gap",
        "incident",
        "owner",
        "procurement",
        "redline",
        "repeats",
        "review",
        "risk",
        "saml",
        "security",
        "stage",
        "stale",
        "terms",
        "visible",
    )
    score = sum(1.0 for keyword in keywords if keyword in lower)
    if "$" in sentence or "arr" in lower:
        score += 1.0
    if len(sentence) >= 40:
        score += 0.3
    return score


def _specific_question(primitive: str, anchors: _QuestionAnchors) -> str:
    subject = anchors.subject or "this signal"
    focus = _safe_question_focus(anchors.focus or anchors.claim, subject)
    constraint = anchors.constraint

    if primitive == "DEPENDENCY":
        if constraint:
            question = (
                f"Is {constraint} the dependency that puts {subject} "
                "on the critical path?"
            )
        else:
            question = f"Is {focus} the critical-path issue for {subject}?"
    elif primitive == "COMMITMENT":
        question = (
            f"Which active promise, deadline, or expected outcome does {focus} "
            f"put at risk for {subject}?"
        )
    elif primitive == "CONSTRAINT":
        if constraint:
            question = (
                f"What resource, policy, or capacity constraint behind {constraint} "
                f"is blocking {subject}?"
            )
        else:
            question = f"What resource, policy, or capacity constraint is driving {focus} for {subject}?"
    elif primitive == "COUNTEREVIDENCE":
        counter_focus = _counterevidence_focus(
            anchors.claim,
            fallback=focus,
            subject=subject,
        )
        question = f"What evidence would weaken the interpretation that {counter_focus}?"
    elif primitive == "OWNERSHIP":
        if constraint:
            question = (
                f"Who owns resolving {constraint} for {subject}, and who owns "
                "the affected commitment?"
            )
        else:
            question = f"Who owns the next action on {focus} for {subject}?"
    elif primitive == "GOAL_IMPACT":
        question = f"Which customer goal, revenue path, or scarce resource does {focus} threaten for {subject}?"
    elif primitive == "RECURRENCE":
        question = f"Has {focus} appeared before for {subject}, or is this a one-off signal?"
    else:
        question = f"What does {subject} require us to verify next?"
    return _truncate_text(" ".join(question.split()), 240)


def _counterevidence_focus(
    claim: str,
    *,
    fallback: str,
    subject: str,
) -> str:
    clean = _clean_question_anchor(claim)
    subject_parts = [
        part.strip().casefold()
        for part in re.split(r"[,/]| and ", subject or "")
        if part.strip()
    ]
    if clean and any(part in clean.casefold() for part in subject_parts):
        return _truncate_text(clean, 150)
    return fallback


def _safe_question_focus(value: str, subject: str) -> str:
    focus = _clean_question_anchor(value)
    if _is_specific_focus_phrase(focus):
        return _truncate_text(focus, 100)
    keyword_focus = _domain_keyword_focus(focus)
    if keyword_focus:
        return _truncate_text(keyword_focus, 100)
    before_verb = re.split(
        r"\b(should|is|are|was|were|has|have|will|may|might|must|needs?|requires?|involves?|creates?|reports?|shows?|indicates?|threatens?)\b",
        focus,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    before_verb = _clean_question_anchor(before_verb)
    if len(before_verb) >= 8:
        return _truncate_text(before_verb, 100)
    return _truncate_text(subject or "this signal", 100)


def _clean_question_anchor(value: str) -> str:
    cleaned = re.sub(r"[_]+", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t\r\n'\"`.,;:()[]{}")
    return cleaned


def _looks_like_machine_identifier(value: str) -> bool:
    clean = value.strip()
    if not clean:
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        clean,
        flags=re.IGNORECASE,
    ):
        return True
    if re.fullmatch(r"[0-9a-f]{16,}", clean, flags=re.IGNORECASE):
        return True
    return False


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
    planning_provider = select_question_planning_provider(llm_provider)
    provider_metadata = question_planning_provider_metadata(planning_provider)

    try:
        plan_call = _generate_llm_question_plan(
            trigger,
            baseline,
            hypotheses,
            evidence_by_key,
            unknowns,
            llm_provider=planning_provider,
            config=config,
            max_tokens=_question_planning_max_tokens(config, planning_provider),
        )
        timeout_s = _question_planning_timeout_seconds(planning_provider)
        # Cost-plan §0.1: tag planning LLM spend as 'question_planning' in the
        # cost ledger (the task spawned by wait_for inherits this contextvar).
        with using_usage_purpose("question_planning"):
            if timeout_s > 0:
                plan = await asyncio.wait_for(plan_call, timeout=timeout_s)
            else:
                plan = await plan_call
        belief_delta_hypotheses = _normalize_llm_belief_delta_hypotheses(
            plan.belief_deltas,
            trigger=trigger,
        )
        question_quality_notes: list[dict[str, Any]] = []
        belief_delta_questions = _candidate_questions_from_belief_deltas(
            trigger,
            belief_delta_hypotheses,
            hypotheses=hypotheses,
            quality_notes=question_quality_notes,
        )
        llm_questions = _normalize_llm_questions(
            plan.questions,
            hypotheses,
            trigger=trigger,
            quality_notes=question_quality_notes,
        )
        if not llm_questions and not belief_delta_questions:
            return deterministic, {
                "round": round_index,
                "mode": "deterministic_fallback",
                "reason": "llm_returned_no_valid_questions",
                "candidate_count": len(deterministic),
                "llm_rationale": plan.rationale,
                "belief_delta_count": len(belief_delta_hypotheses),
                **provider_metadata,
            }
        primary_questions = llm_questions or belief_delta_questions
        safety_questions = (
            [*belief_delta_questions, *deterministic]
            if llm_questions else deterministic
        )
        merged, safety_added = _merge_llm_and_safety_questions(
            primary_questions,
            safety_questions,
        )
        return merged, {
            "round": round_index,
            "mode": "llm" if llm_questions else "llm_delta",
            "llm_candidate_count": len(llm_questions),
            "belief_delta_count": len(belief_delta_hypotheses),
            "belief_delta_question_count": len(belief_delta_questions),
            "belief_delta_types": [
                h.delta_type for h in belief_delta_hypotheses if h.delta_type
            ],
            "belief_delta_claims": [
                h.claim for h in belief_delta_hypotheses[:5]
            ],
            "safety_candidate_count": safety_added,
            "candidate_count": len(merged),
            "llm_rationale": plan.rationale,
            "llm_primitives": [q.primitive for q in llm_questions],
            "llm_schema": _question_planning_schema_name(planning_provider),
            "question_quality": _question_quality_summary(question_quality_notes),
            **provider_metadata,
        }
    except Exception as exc:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": type(exc).__name__,
            "detail": str(exc)[:240],
            "candidate_count": len(deterministic),
            **provider_metadata,
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
    max_tokens: int | None = None,
) -> LLMInquiryQuestionPlan:
    if _use_compact_question_planning_schema(llm_provider):
        system = (
            "You compile retrieval questions for Fyralis model updates. "
            "Extract atomic belief deltas, then ask only the few specific "
            "questions needed to decide which models change. Keep text short. "
            "Never copy a full claim into a question. Return JSON only."
        )
        user = json.dumps(
            {
                "task": "compile retrieval question plan",
                "p": sorted(_ALLOWED_QUESTION_PRIMITIVES),
                "types": [
                    "create",
                    "update",
                    "weaken",
                    "split",
                    "merge",
                    "supersede",
                    "no_op",
                ],
                "signal": {
                    "kind": trigger.kind,
                    "text": _truncate_text(_trigger_text(trigger), 700),
                    "entities": trigger.seed_entity_ids[:8],
                    "actors": len(trigger.scope_actors),
                    "at": (
                        trigger.seed_occurred_at.isoformat()
                        if trigger.seed_occurred_at
                        else None
                    ),
                },
                "h": [
                    {
                        "id": h.id,
                        "claim": _truncate_text(h.claim, 180),
                        "conf": h.confidence,
                        "impact": h.impact_if_true,
                    }
                    for h in hypotheses[:4]
                ],
                "u": sorted(unknowns)[:8],
                "base": _compact_baseline_snapshot_for_question_planning(
                    baseline,
                    evidence_by_key,
                ),
                "rules": [
                    "d: 1-4 atomic belief deltas",
                    "q: 2-3 questions",
                    "q[].p must be one allowed primitive",
                    "q[].q must be grammatical and under 22 words",
                    "ask about missing context, counterevidence, ownership, recurrence, dependencies, or constraints",
                    "avoid questions already answered by base evidence",
                ],
            },
            default=str,
            separators=(",", ":"),
        )
        compact = await llm_provider.structured(
            system=system,
            user=user,
            schema=LLMCompactQuestionPlan,
            temperature=config.llm_question_temperature,
            max_tokens=max_tokens or config.llm_question_max_tokens,
        )
        return _expand_compact_question_plan(compact)

    system = (
        "You are a bounded semantic compiler for Fyralis' model-update "
        "pipeline. First extract atomic belief-delta candidates from the "
        "signal, then choose only the few retrieval questions that will decide "
        "which existing models must be created, updated, weakened, split, "
        "merged, superseded, or left unchanged. Prefer specific, "
        "discriminating questions over broad searches. Always include "
        "counterevidence when the signal makes a material claim. Keep outputs "
        "short. Never paste a whole belief_delta claim into a question; turn "
        "it into a compact noun phrase first. Return JSON only."
    )
    user = json.dumps(
        {
            "task": (
                "Compile belief deltas and generate the next retrieval "
                "questions for this signal."
            ),
            "allowed_primitives": sorted(_ALLOWED_QUESTION_PRIMITIVES),
            "allowed_delta_types": [
                "create",
                "update",
                "weaken",
                "split",
                "merge",
                "supersede",
                "no_op",
            ],
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
                "Return 1 to 4 belief_deltas before questions.",
                "Each belief_delta should be an atomic claim, not a summary of the whole signal.",
                "Use uncertainty_slots to name what retrieval must resolve.",
                "Use evidence_needed to name the source/evidence type that would resolve each slot.",
                "Return 2 to 3 questions.",
                "Use primitive names exactly as provided.",
                "Each question must be one grammatical sentence under 22 words.",
                "Do not copy claim_atom verbatim into any question.",
                "Avoid question starts like 'Has <full sentence>' or 'Is <full sentence> actually'.",
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
        max_tokens=max_tokens or config.llm_question_max_tokens,
    )


def _question_planning_max_tokens(
    config: InquiryConfig,
    llm_provider: LLMProvider,
) -> int:
    provider_name = getattr(llm_provider.config, "provider", "")
    if provider_name == "codex":
        raw = os.environ.get("INQUIRY_CODEX_QUESTION_MAX_TOKENS")
        if raw:
            try:
                return max(320, int(raw))
            except ValueError:
                pass
        if _use_compact_question_planning_schema(llm_provider):
            return min(config.llm_question_max_tokens, 420)
        model_name = str(getattr(llm_provider.config, "model", "") or "").casefold()
        if "spark" in model_name:
            return min(config.llm_question_max_tokens, 650)
    return config.llm_question_max_tokens


def _question_planning_schema_name(llm_provider: LLMProvider) -> str:
    if _use_compact_question_planning_schema(llm_provider):
        return "compact_v1"
    return "full_v1"


def _use_compact_question_planning_schema(llm_provider: LLMProvider) -> bool:
    provider_name = str(getattr(llm_provider.config, "provider", "") or "")
    if provider_name != "codex":
        return False
    raw = os.environ.get("INQUIRY_CODEX_COMPACT_QUESTION_SCHEMA", "1")
    if raw.strip().lower() in {"0", "false", "no", "off"}:
        return False
    model_name = str(getattr(llm_provider.config, "model", "") or "").casefold()
    return "spark" in model_name


def _question_planning_timeout_seconds(llm_provider: LLMProvider) -> float:
    provider_name = str(getattr(llm_provider.config, "provider", "") or "")
    if provider_name == "codex":
        raw = os.environ.get("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS")
        if raw:
            try:
                return max(1.0, float(raw))
            except ValueError:
                pass
        return float(getattr(llm_provider.config, "timeout_s", 30) or 30)
    raw = os.environ.get("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS")
    if raw:
        try:
            return max(1.0, float(raw))
        except ValueError:
            pass
    return float(getattr(llm_provider.config, "timeout_s", 30) or 30)


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


def _compact_baseline_snapshot_for_question_planning(
    baseline: RetrievalResult,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
) -> dict[str, Any]:
    cards = sorted(
        evidence_by_key.values(),
        key=lambda c: (-float(c.score), c.source_type, c.summary),
    )
    return {
        "models": len(baseline.models),
        "obs": len(baseline.observations),
        "acts": {
            "commitments": len(baseline.acts.get("commitments", [])),
            "goals": len(baseline.acts.get("goals", [])),
            "decisions": len(baseline.acts.get("decisions", [])),
        },
        "m": [
            {
                "id": str(model.id)[:8],
                "s": _truncate_text(
                    getattr(model, "natural", "") or json.dumps(
                        getattr(model, "proposition", {}) or {},
                        default=str,
                    ),
                    150,
                ),
                "score": round(float(baseline.model_scores.get(model.id, 0.0)), 3),
            }
            for model in baseline.models[:6]
        ],
        "e": [
            {
                "src": card.source_type,
                "s": _truncate_text(card.summary, 150),
                "score": round(float(card.score), 3),
            }
            for card in cards[:8]
        ],
    }


def _expand_compact_question_plan(
    plan: LLMCompactQuestionPlan,
) -> LLMInquiryQuestionPlan:
    deltas = [
        LLMBeliefDeltaSpec(
            delta_id=delta.i,
            claim_atom=delta.claim,
            delta_type=delta.type,
            affected_entities=delta.entities,
            uncertainty_slots=delta.slots,
            evidence_needed=delta.evidence,
            impact_if_true=delta.impact,
            confidence=delta.conf,
        )
        for delta in plan.d
    ]
    questions = [
        LLMInquiryQuestionSpec(
            primitive=question.p,
            question=question.q,
            retrieval_target=None,
            expected_value=question.v,
            expected_cost=question.c,
            tests_hypotheses=[],
            stop_condition=None,
        )
        for question in plan.q
    ]
    return LLMInquiryQuestionPlan(
        rationale=plan.r,
        belief_deltas=deltas,
        questions=questions,
    )


_ALLOWED_DELTA_TYPES = {
    "create",
    "update",
    "weaken",
    "split",
    "merge",
    "supersede",
    "no_op",
}


def _normalize_llm_belief_delta_hypotheses(
    specs: list[LLMBeliefDeltaSpec],
    *,
    trigger: TriggerContext,
) -> list[Hypothesis]:
    anchors = _question_anchors(trigger)
    out: list[Hypothesis] = []
    seen_claims: set[str] = set()
    for index, spec in enumerate(specs[:5], start=1):
        claim = _clean_question_anchor(spec.claim_atom)
        if len(claim) < 8:
            continue
        claim = _truncate_text(claim, 240)
        claim_key = claim.casefold()
        if claim_key in seen_claims:
            continue
        seen_claims.add(claim_key)
        delta_type = _normalize_delta_type(spec.delta_type)
        entities = _clean_delta_items(spec.affected_entities, limit=8)
        if not entities and anchors.subject != "this signal":
            entities = (anchors.subject,)
        uncertainties = _clean_delta_items(spec.uncertainty_slots, limit=8)
        if not uncertainties:
            uncertainties = _fallback_uncertainty_slots_for_delta(delta_type)
        evidence_needed = _clean_delta_items(spec.evidence_needed, limit=8)
        hypothesis_id = _clean_question_anchor(spec.delta_id or "") or f"D{index}"
        hypothesis_id = re.sub(r"[^A-Za-z0-9_:-]+", "_", hypothesis_id)[:24]
        out.append(
            Hypothesis(
                id=hypothesis_id or f"D{index}",
                claim=claim,
                confidence=_clamp_float(spec.confidence, 0.0, 1.0),
                impact_if_true=_normalize_impact_label(spec.impact_if_true),
                delta_type=delta_type,
                target_model_ids=_clean_delta_items(spec.target_model_ids, limit=5),
                affected_entities=entities,
                uncertainty_slots=uncertainties,
                evidence_needed=evidence_needed,
                source="llm_delta",
            )
        )
    return out


def _candidate_questions_from_belief_deltas(
    trigger: TriggerContext,
    belief_deltas: list[Hypothesis],
    *,
    hypotheses: tuple[Hypothesis, ...],
    quality_notes: list[dict[str, Any]] | None = None,
) -> list[InquiryQuestion]:
    known_hypothesis_ids = {h.id for h in hypotheses}
    questions: list[InquiryQuestion] = []
    seen: set[tuple[str, str]] = set()
    for delta in belief_deltas:
        slots = delta.uncertainty_slots or _fallback_uncertainty_slots_for_delta(
            delta.delta_type
        )
        for slot in slots[:4]:
            primitive = _primitive_for_delta_slot(slot, delta.delta_type)
            question = _question_from_delta_slot(delta, slot, primitive, trigger)
            question = _quality_control_question_text(
                question,
                primitive,
                trigger,
                source="belief_delta",
                quality_notes=quality_notes,
                delta=delta,
                slot=slot,
            )
            key = (primitive, question.casefold())
            if key in seen:
                continue
            seen.add(key)
            expected_cost = _DEFAULT_COST_BY_PRIMITIVE.get(primitive, 0.24)
            expected_value = _delta_question_expected_value(delta, primitive)
            tests = _tests_for_delta_question(
                primitive,
                delta,
                known_hypothesis_ids=known_hypothesis_ids,
            )
            questions.append(
                InquiryQuestion(
                    question_id=_QUESTION_ID_BY_PRIMITIVE[primitive],
                    question=question,
                    primitive=primitive,
                    tests_hypotheses=tests,
                    expected_value=expected_value,
                    expected_cost=expected_cost,
                    retrieval_target=_DEFAULT_TARGET_BY_PRIMITIVE[primitive],
                    stop_condition=_DEFAULT_STOP_BY_PRIMITIVE[primitive],
                    score=round(expected_value - expected_cost + 0.12, 4),
                )
            )
    return sorted(
        questions,
        key=lambda q: (-q.score, q.expected_cost, q.primitive, q.question),
    )[:12]


_DEFAULT_COST_BY_PRIMITIVE = {
    "DEPENDENCY": 0.24,
    "COMMITMENT": 0.18,
    "CONSTRAINT": 0.24,
    "COUNTEREVIDENCE": 0.30,
    "OWNERSHIP": 0.22,
    "GOAL_IMPACT": 0.20,
    "RECURRENCE": 0.36,
}


def _normalize_delta_type(value: str | None) -> str:
    delta_type = re.sub(r"[^a-z_]+", "_", str(value or "update").casefold()).strip("_")
    aliases = {
        "create_new": "create",
        "new": "create",
        "modify": "update",
        "weaken_existing": "weaken",
        "retire": "supersede",
        "obsolete": "supersede",
        "none": "no_op",
        "noop": "no_op",
        "no_update": "no_op",
    }
    delta_type = aliases.get(delta_type, delta_type)
    return delta_type if delta_type in _ALLOWED_DELTA_TYPES else "update"


def _normalize_impact_label(value: str | None) -> str:
    label = str(value or "medium").casefold().strip()
    if label in {"high", "medium", "low"}:
        return label
    if label in {"critical", "severe"}:
        return "high"
    if label in {"minor", "small"}:
        return "low"
    return "medium"


def _clean_delta_items(values: list[Any] | tuple[Any, ...], *, limit: int) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values[:limit]:
        clean = _clean_question_anchor(str(value or ""))
        if not clean or _looks_like_machine_identifier(clean):
            continue
        clean = _truncate_text(clean, 140)
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return tuple(out)


def _fallback_uncertainty_slots_for_delta(delta_type: str | None) -> tuple[str, ...]:
    if delta_type in {"weaken", "supersede"}:
        return (
            "what evidence contradicts or weakens the existing belief",
            "which prior model is now stale",
        )
    if delta_type in {"split", "merge"}:
        return (
            "which existing beliefs should be separated or combined",
            "what evidence distinguishes the competing interpretations",
        )
    if delta_type == "no_op":
        return (
            "whether this signal is already captured",
            "what evidence supports no model update",
        )
    return (
        "which existing model should change",
        "what evidence would weaken this interpretation",
        "who owns the next action",
    )


def _primitive_for_delta_slot(slot: str, delta_type: str | None) -> str:
    lower = slot.casefold()
    if re.search(r"\b(owner|owns|accountable|who|assignee|responsible)\b", lower):
        return "OWNERSHIP"
    if re.search(r"\b(commitment|promise|deadline|outcome|deliverable)\b", lower):
        return "COMMITMENT"
    if re.search(r"\b(resource|policy|capacity|constraint|quota|blocked by)\b", lower):
        return "CONSTRAINT"
    if re.search(r"\b(recur|recurrence|pattern|before|similar|repeat)\b", lower):
        return "RECURRENCE"
    if re.search(r"\b(goal|customer|revenue|arr|resource|impact|risk)\b", lower):
        return "GOAL_IMPACT"
    if re.search(
        r"\b(counter|weaken|contradict|falsif|stale|supersede|wrong|obsolete)\b",
        lower,
    ):
        return "COUNTEREVIDENCE"
    if re.search(r"\b(dependency|critical path|blocker|blocking|depends)\b", lower):
        return "DEPENDENCY"
    if delta_type in {"weaken", "supersede", "no_op"}:
        return "COUNTEREVIDENCE"
    if delta_type in {"create", "update"}:
        return "DEPENDENCY"
    return "GOAL_IMPACT"


def _question_from_delta_slot(
    delta: Hypothesis,
    slot: str,
    primitive: str,
    trigger: TriggerContext,
) -> str:
    subject = (
        ", ".join(delta.affected_entities[:3])
        if delta.affected_entities else _question_anchors(trigger).subject
    )
    focus = _delta_question_focus(delta, slot, trigger)
    if primitive == "OWNERSHIP":
        question = f"Who owns resolving {focus} for {subject}?"
    elif primitive == "COMMITMENT":
        question = f"Which active commitment or promised outcome would change if {focus} is true for {subject}?"
    elif primitive == "CONSTRAINT":
        question = f"Which resource, policy, or capacity constraint is blocking {focus} for {subject}?"
    elif primitive == "RECURRENCE":
        pattern_focus = focus if "pattern" in focus.casefold() else f"{focus} pattern"
        question = f"Has this {pattern_focus} appeared before for {subject}, or is it new?"
    elif primitive == "GOAL_IMPACT":
        question = f"Which customer goal, revenue path, or scarce resource is affected by {focus} for {subject}?"
    elif primitive == "COUNTEREVIDENCE":
        question = f"What evidence would weaken or falsify the {focus} interpretation for {subject}?"
    else:
        question = f"Is {focus} a blocking dependency or critical-path issue for {subject}?"
    return _truncate_text(" ".join(question.split()), 240)


_GENERIC_DELTA_SLOT_PATTERNS = (
    "blocker",
    "constraint",
    "critical path",
    "critical path status",
    "dependency",
    "goal impact",
    "issue type",
    "owner",
    "ownership",
    "recurrence",
    "status",
    "which existing model should change",
    "what evidence would weaken this interpretation",
    "who owns the next action",
    "whether this signal is already captured",
    "what evidence supports no model update",
    "which prior model is now stale",
    "which existing beliefs should be separated or combined",
    "whether this appeared before",
    "this appeared before",
    "appeared before",
)


def _delta_question_focus(
    delta: Hypothesis,
    slot: str,
    trigger: TriggerContext,
) -> str:
    candidates = [
        slot,
        *delta.evidence_needed,
        *delta.uncertainty_slots,
        delta.claim,
    ]
    for candidate in candidates:
        focus = _clean_question_focus_phrase(candidate)
        if _is_specific_focus_phrase(focus):
            return _truncate_text(focus, 120)
    return _fallback_focus_from_delta_claim(delta.claim, trigger)


def _clean_question_focus_phrase(value: str) -> str:
    phrase = _clean_question_anchor(value)
    phrase = re.sub(
        r"^(commitment|constraint|counterevidence|dependency|goal[_\s-]*impact|ownership|recurrence)\s*[:/-]\s*",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"^(whether|if|what|which|who|how|is|are|does|do|has|have|should|would|can|could)\s+",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"^(the|a|an)\s+(evidence|source|question|signal)\s+(that|for|about)\s+",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(r"\b(actually|explicitly|currently)\b", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\bpattern\s+frequency\b", "pattern", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\bpattern\s+pattern\b", "pattern", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\s+", " ", phrase).strip(" .,:;?") or "the signal"
    return phrase


def _is_specific_focus_phrase(phrase: str) -> bool:
    lower = phrase.casefold()
    if len(phrase) < 8 or lower in {"the signal", "this signal", "this interpretation"}:
        return False
    if any(pattern in lower for pattern in _GENERIC_DELTA_SLOT_PATTERNS):
        words = set(re.findall(r"[a-z0-9_-]+", lower))
        domain_words = {
            "audit",
            "customer",
            "data",
            "export",
            "incident",
            "mapping",
            "permission",
            "procurement",
            "renewal",
            "saml",
            "security",
            "soc2",
            "trail",
        }
        if not words.intersection(domain_words):
            return False
    sentence_verbs = (
        " should ",
        " is ",
        " are ",
        " was ",
        " were ",
        " has ",
        " have ",
        " will ",
        " may ",
        " might ",
        " must ",
    )
    if len(phrase) > 72 and any(marker in f" {lower} " for marker in sentence_verbs):
        return False
    if phrase.count(" ") > 13:
        return False
    return True


def _fallback_focus_from_delta_claim(
    claim: str,
    trigger: TriggerContext,
) -> str:
    clean = _clean_question_anchor(claim)
    quoted = re.findall(r"'([^']{8,90})'|\"([^\"]{8,90})\"", clean)
    for left, right in quoted:
        phrase = _clean_question_focus_phrase(left or right)
        if _is_specific_focus_phrase(phrase):
            return _truncate_text(phrase, 120)

    before_verb = re.split(
        r"\b(should|is|are|was|were|has|have|will|may|might|must|needs?|requires?|involves?|creates?|reports?|shows?|indicates?)\b",
        clean,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    before_verb = _clean_question_focus_phrase(before_verb)
    keyword_phrase = _domain_keyword_focus(clean)
    if _is_specific_focus_phrase(before_verb):
        if keyword_phrase and keyword_phrase.casefold() not in before_verb.casefold():
            return _truncate_text(f"{before_verb} {keyword_phrase}", 120)
        return _truncate_text(before_verb, 120)
    if keyword_phrase:
        return _truncate_text(keyword_phrase, 120)

    anchors = _question_anchors(trigger)
    if anchors.focus:
        return _truncate_text(anchors.focus, 120)
    return "the signal"


def _domain_keyword_focus(text: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", text)
    if not tokens:
        return ""
    keywords = {
        "blocker",
        "blockers",
        "capacity",
        "commitment",
        "constraint",
        "dependency",
        "evidence",
        "incident",
        "onboarding",
        "permission",
        "policy",
        "procurement",
        "renewal",
        "replay",
        "risk",
        "saml",
        "timeline",
    }
    for idx, token in enumerate(tokens):
        if token.casefold() not in keywords:
            continue
        start = max(0, idx - 2)
        end = min(len(tokens), idx + 4)
        phrase = _clean_question_focus_phrase(" ".join(tokens[start:end]))
        if len(phrase) >= 8:
            return phrase
    return ""


def _delta_question_expected_value(delta: Hypothesis, primitive: str) -> float:
    impact_boost = {"high": 0.18, "medium": 0.10, "low": 0.02}.get(
        delta.impact_if_true,
        0.10,
    )
    delta_boost = {
        "weaken": 0.08,
        "supersede": 0.08,
        "split": 0.06,
        "merge": 0.06,
        "update": 0.05,
        "create": 0.04,
        "no_op": 0.02,
    }.get(delta.delta_type or "update", 0.04)
    primitive_boost = 0.04 if primitive in {"COUNTEREVIDENCE", "OWNERSHIP"} else 0.0
    return round(
        _clamp_float(0.55 + float(delta.confidence) * 0.22 + impact_boost + delta_boost + primitive_boost, 0.0, 0.98),
        4,
    )


def _tests_for_delta_question(
    primitive: str,
    delta: Hypothesis,
    *,
    known_hypothesis_ids: set[str],
) -> tuple[str, ...]:
    preferred: tuple[str, ...]
    if primitive == "COUNTEREVIDENCE" or delta.delta_type in {"weaken", "supersede", "no_op"}:
        preferred = ("H1", "H0")
    elif primitive in {"OWNERSHIP", "COMMITMENT", "GOAL_IMPACT"}:
        preferred = ("H2", "H1")
    elif primitive == "RECURRENCE":
        preferred = ("H3", "H0")
    else:
        preferred = ("H1",)
    tests = tuple(hid for hid in preferred if hid in known_hypothesis_ids)
    return tests or tuple(sorted(known_hypothesis_ids))[:1] or ("H1",)


def _quality_control_question_text(
    question: str,
    primitive: str,
    trigger: TriggerContext,
    *,
    source: str,
    quality_notes: list[dict[str, Any]] | None = None,
    delta: Hypothesis | None = None,
    slot: str | None = None,
) -> str:
    clean = " ".join(question.split()).strip()
    reason = _question_quality_failure_reason(clean, primitive)
    if reason is None:
        return _truncate_text(clean, 240)
    if reason == "missing_question_mark":
        repaired = _punctuate_question_text(clean)
        if quality_notes is not None:
            quality_notes.append({
                "source": source,
                "primitive": primitive,
                "repair_reason": "punctuation_added",
                "original": _truncate_text(clean, 160),
                "repaired": _truncate_text(repaired, 160),
            })
        return repaired

    repaired = _repair_question_text(
        primitive,
        trigger,
        delta=delta,
        slot=slot,
    )
    if quality_notes is not None:
        quality_notes.append({
            "source": source,
            "primitive": primitive,
            "repair_reason": reason,
            "original": _truncate_text(clean, 160),
            "repaired": _truncate_text(repaired, 160),
        })
    return repaired


def _punctuate_question_text(question: str) -> str:
    clean = question.rstrip(" .,:;")
    return _truncate_text(f"{clean}?", 240)


def _question_quality_failure_reason(question: str, primitive: str) -> str | None:
    lower = f" {question.casefold()} "
    if len(question) > 240:
        return "too_long"
    if not question.endswith("?"):
        return "missing_question_mark"
    if re.search(r"\b(has|is|are|does|do)\s+[A-Z][^.?!]{0,90}\s+(is|are|has|should|must|will|may)\b", question):
        return "nested_clause_subject"
    if primitive == "CONSTRAINT" and re.search(
        r"what resource, policy, or capacity constraint determines\s+(is|are|whether|if|should|does)\b",
        lower,
    ):
        return "constraint_template_clause_leak"
    if primitive == "DEPENDENCY" and re.search(
        r"^is\s+.+\s+(should|is|are|has|will|must|may)\s+.+actually on the critical path",
        lower.strip(),
    ):
        return "dependency_template_clause_leak"
    if primitive == "RECURRENCE" and re.search(
        r"^has\s+.+\s+(is|are|has|should|must|will|may)\s+.+appeared before",
        lower.strip(),
    ):
        return "recurrence_template_clause_leak"
    if re.search(
        r"\bblocking\s+(blocker|constraint|counterevidence|dependency|goal impact|issue type|ownership|recurrence|status)\s+for\b",
        lower,
    ):
        return "generic_focus_leak"
    if re.search(
        r"\bblocking\s+(blocker|constraint|counterevidence|dependency|goal impact|issue type|ownership|recurrence|status)\s*:",
        lower,
    ):
        return "generic_focus_leak"
    if re.search(
        r"^is\s+(blocker|constraint|dependency|issue type|status)\s+a blocking dependency",
        lower.strip(),
    ):
        return "generic_focus_leak"
    if "..." in question and len(question) > 180:
        return "truncated_clause"
    return None


def _repair_question_text(
    primitive: str,
    trigger: TriggerContext,
    *,
    delta: Hypothesis | None = None,
    slot: str | None = None,
) -> str:
    if delta is None:
        return _specific_question(primitive, _question_anchors(trigger))
    return _question_from_delta_slot(delta, slot or "", primitive, trigger)


def _question_quality_summary(
    quality_notes: list[dict[str, Any]],
) -> dict[str, Any]:
    if not quality_notes:
        return {"repairs": 0}
    by_reason = Counter(str(note.get("repair_reason") or "unknown") for note in quality_notes)
    by_source = Counter(str(note.get("source") or "unknown") for note in quality_notes)
    return {
        "repairs": len(quality_notes),
        "by_reason": dict(by_reason.most_common()),
        "by_source": dict(by_source.most_common()),
        "examples": quality_notes[:3],
    }


def _normalize_llm_questions(
    specs: list[LLMInquiryQuestionSpec],
    hypotheses: tuple[Hypothesis, ...],
    *,
    trigger: TriggerContext,
    quality_notes: list[dict[str, Any]] | None = None,
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
        question = _quality_control_question_text(
            question,
            primitive,
            trigger,
            source="llm_question",
            quality_notes=quality_notes,
        )
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
    learned_motif: LearnedRetrievalMotif | None = None,
) -> list[RetrievalAction]:
    static_actions = _compile_static_retrieval_plan(
        question,
        trigger,
        cfg,
        policy_signal=policy_signal,
    )
    if learned_motif is None:
        return static_actions
    return _compile_motif_retrieval_plan(
        question,
        static_actions,
        learned_motif,
        cfg,
    ) or static_actions


def _compile_static_retrieval_plan(
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
    focused_actions = _focused_index_actions(
        question,
        trigger,
        cfg,
        policy_signal=policy_signal,
    )

    if question.primitive == "DEPENDENCY":
        return focused_actions + [
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
        return focused_actions + [
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
        return focused_actions + [
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
        return focused_actions + [
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
        return focused_actions + [
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
        return focused_actions + [
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
    return focused_actions + [
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


def _compile_motif_retrieval_plan(
    question: InquiryQuestion,
    static_actions: list[RetrievalAction],
    motif: LearnedRetrievalMotif,
    cfg: InquiryConfig,
) -> list[RetrievalAction]:
    raw_actions = motif.plan.get("actions")
    if not isinstance(raw_actions, list):
        return []
    by_exact = {
        (action.path, action.target): action
        for action in static_actions
    }
    by_path: dict[str, RetrievalAction] = {}
    for action in static_actions:
        by_path.setdefault(action.path, action)

    compiled: list[RetrievalAction] = []
    seen: set[tuple[str, str, int]] = set()
    for raw in raw_actions[: max(1, int(cfg.retrieval_motif_max_actions))]:
        if not isinstance(raw, dict):
            continue
        path = str(raw.get("path") or "")
        target = str(raw.get("target") or "")
        base = by_exact.get((path, target)) or by_path.get(path)
        if base is None:
            continue
        try:
            stage = max(1, int(raw.get("stage") or 1))
        except (TypeError, ValueError):
            stage = 1
        key = (base.path, base.target, stage)
        if key in seen:
            continue
        seen.add(key)
        try:
            budget = int(raw.get("budget") or base.budget)
        except (TypeError, ValueError):
            budget = base.budget
        filters = dict(base.filters or {})
        filters.update({
            "_motif_id": str(motif.id),
            "_motif_stage": stage,
            "_motif_match_score": round(float(motif.match_score), 4),
            "_motif_utility_score": round(float(motif.utility_score), 4),
        })
        if bool(raw.get("bind_previous_scope")) and stage > 1:
            filters["_bind_previous_scope"] = True
        compiled.append(
            RetrievalAction(
                question_id=question.question_id,
                path=base.path,
                target=base.target,
                query=base.query,
                filters=filters,
                budget=max(1, budget),
            )
        )
    return compiled


async def _load_retrieval_motifs_for_questions(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    questions: list[InquiryQuestion],
    cfg: InquiryConfig,
) -> dict[str, LearnedRetrievalMotif]:
    if not cfg.retrieval_motifs_enabled or not questions:
        return {}
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.retrieval_motifs')"
    )
    if table_name is None:
        return {}
    primitives = sorted({q.primitive for q in questions})
    rows = await conn.fetch(
        """
        SELECT id, signature, question_primitive, plan,
               utility_score, success_count
        FROM retrieval_motifs
        WHERE tenant_id = $1
          AND question_primitive = ANY($2::text[])
          AND maturity = 'active'
          AND utility_score > 0
          AND success_count >= $3
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY utility_score DESC, success_count DESC, updated_at DESC
        LIMIT 64
        """,
        trigger.tenant_id,
        primitives,
        int(cfg.retrieval_motif_min_successes),
    )
    if not rows:
        return {}

    by_primitive: dict[str, LearnedRetrievalMotif] = {}
    current_by_primitive = {
        primitive: _motif_signature_for(trigger, primitive)
        for primitive in primitives
    }
    for row in rows:
        primitive = str(row["question_primitive"] or "").upper()
        current = current_by_primitive.get(primitive)
        if not current:
            continue
        signature = _json_obj(row["signature"])
        score = _motif_signature_match_score(signature, current)
        if score < float(cfg.retrieval_motif_match_threshold):
            continue
        motif = LearnedRetrievalMotif(
            id=row["id"],
            signature=signature,
            question_primitive=primitive,
            plan=_json_obj(row["plan"]),
            utility_score=float(row["utility_score"] or 0.0),
            success_count=int(row["success_count"] or 0),
            match_score=score,
        )
        prior = by_primitive.get(primitive)
        if prior is None or (
            motif.match_score,
            motif.utility_score,
            motif.success_count,
        ) > (
            prior.match_score,
            prior.utility_score,
            prior.success_count,
        ):
            by_primitive[primitive] = motif

    return {
        question.question_id: by_primitive[question.primitive]
        for question in questions
        if question.primitive in by_primitive
    }


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _motif_signature_for(
    trigger: TriggerContext,
    question_primitive: str,
) -> dict[str, Any]:
    return {
        "signal_type": trigger.kind,
        "signal_class": _signal_class_for_trigger(trigger),
        "question_primitive": question_primitive,
        "entity_types": sorted({
            str(entity.get("type") or "").casefold()
            for entity in trigger.seed_entity_ids
            if isinstance(entity, dict) and entity.get("type")
        }),
        "domain_terms": _motif_domain_terms(_trigger_text(trigger)),
    }


_MOTIF_DOMAIN_KEYWORDS = frozenset({
    "arr", "audit", "blocker", "capacity", "churn", "commitment",
    "compliance", "customer", "data", "dependency", "evidence",
    "export", "freshness", "incident", "liability", "mapping",
    "onboarding", "permission", "policy", "procurement", "renewal",
    "replay", "risk", "saml", "security", "soc2", "terms", "trail",
})


def _motif_domain_terms(text: str) -> list[str]:
    terms = {
        token.casefold()
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", text or "")
        if token.casefold() in _MOTIF_DOMAIN_KEYWORDS
    }
    return sorted(terms)[:16]


def _motif_signature_match_score(
    stored: dict[str, Any],
    current: dict[str, Any],
) -> float:
    score = 0.0
    if stored.get("signal_type") == current.get("signal_type"):
        score += 0.24
    if stored.get("signal_class") == current.get("signal_class"):
        score += 0.16
    if stored.get("question_primitive") == current.get("question_primitive"):
        score += 0.20
    score += 0.20 * _set_overlap_ratio(
        stored.get("entity_types"),
        current.get("entity_types"),
    )
    domain_overlap = _set_overlap_ratio(
        stored.get("domain_terms"),
        current.get("domain_terms"),
    )
    if domain_overlap == 0.0 and not stored.get("domain_terms"):
        domain_overlap = 0.5
    score += 0.20 * domain_overlap
    return round(min(score, 1.0), 4)


def _set_overlap_ratio(left: Any, right: Any) -> float:
    left_set = {str(v) for v in left or [] if str(v)}
    right_set = {str(v) for v in right or [] if str(v)}
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / max(len(left_set | right_set), 1)


def _focused_index_actions(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    policy_signal: QuestionPolicySignal | None,
) -> list[RetrievalAction]:
    if not cfg.focused_index_enabled:
        return []
    terms = _focused_index_terms(
        question.question,
        trigger,
        max_terms=int(cfg.focused_index_terms),
    )
    return [
        RetrievalAction(
            question.question_id,
            "focused_index",
            "question_answerability_scope",
            query=question.question,
            filters={
                "seed_entities": list(trigger.seed_entity_ids),
                "primitive": question.primitive,
                "terms": terms,
            },
            budget=_policy_budget(cfg.focused_index_max_candidates, policy_signal),
        )
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
    scope_entities = _stable_cache_value(_action_seed_entities(action, trigger))
    seed_model_ids = tuple(sorted(str(mid) for mid in _action_seed_model_ids(action)))
    if action.path == "structural":
        return (
            "structural",
            cfg.structural_max_hops,
            model_budget,
            scope_actors,
            scope_entities,
        )
    if action.path == "focused_index":
        return (
            "focused_index",
            model_budget,
            scope_actors,
            scope_entities,
            str(action.filters.get("primitive") or ""),
            _stable_cache_value(action.filters.get("terms") or []),
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
            seed_model_ids,
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


def _action_seed_entities(
    action: RetrievalAction,
    trigger: TriggerContext,
) -> list[dict[str, Any]]:
    raw = action.filters.get("seed_entities")
    if isinstance(raw, list):
        out = [item for item in raw if isinstance(item, dict)]
        if out:
            return out
    return list(trigger.seed_entity_ids or [])


def _action_seed_model_ids(action: RetrievalAction) -> list[UUID]:
    out: list[UUID] = []
    raw = action.filters.get("seed_model_ids")
    if not isinstance(raw, list):
        return out
    for value in raw:
        try:
            out.append(UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return out


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
    *,
    gate_broad_actions: bool = True,
) -> tuple[Literal["all", "broad"] | None, str | None]:
    if not gate_broad_actions:
        return None, None
    plan = _sage_reader_plan_from_result(result)
    if not plan:
        return None, None
    mode = str(plan.get("mode") or "")
    if _sage_reader_plan_hard_abstained(plan):
        return "all", "sage_reader_negative_memory_abstain"
    if not bool(plan.get("gate_broad_actions")):
        return None, None
    if bool(plan.get("skip_broad_discovery")) and mode in {
        "focused",
        "guarded_negative_memory",
        "rerank",
    }:
        return "broad", f"sage_reader_{mode}_broad_gate"
    read_note = (result.notes or {}).get("sage_reader") or {}
    if (
        mode == "rerank"
        and len(result.models) >= 1
        and int(read_note.get("projected_evidence_count") or 0) >= 1
    ):
        return "broad", "sage_reader_rerank_sufficient_evidence"
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
            "gate_broad_actions": bool(plan.get("gate_broad_actions")),
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
    *,
    read_pool: asyncpg.Pool | None = None,
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
            seeds = _action_seed_entities(action, trigger)
            if not seeds and trigger.scope_actors:
                seeds = [{"type": "actor", "id": str(a)} for a in trigger.scope_actors]
            result = await pathway_a_structural(
                seeds,
                trigger.tenant_id,
                conn,
                max_hops=cfg.structural_max_hops,
                read_pool=read_pool,
                read_fanout_enabled=cfg.structural_read_fanout_enabled,
                read_fanout_min_seeds=cfg.structural_read_fanout_min_seeds,
                read_fanout_chunk_size=cfg.structural_read_fanout_chunk_size,
            )
            _cap_pathway_models(result, capped_budget(action.budget))
            return result
        if action.path == "focused_index":
            return await _execute_focused_index_action(
                action,
                trigger,
                conn,
                cfg,
                model_limit=capped_budget(action.budget),
            )
        if action.path == "semantic":
            return await _execute_semantic_hybrid_action(
                action,
                trigger,
                conn,
                embedder,
                cfg,
                model_limit=capped_budget(action.budget),
            )
        if action.path == "temporal":
            if trigger.seed_occurred_at is None:
                return None
            return await pathway_c_temporal(
                trigger.seed_occurred_at,
                timedelta(days=int(action.filters.get("window_days") or cfg.temporal_window_days)),
                trigger.tenant_id,
                conn,
                scope_actors=trigger.scope_actors,
                scope_entities=_action_seed_entities(action, trigger),
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
            seed_model_ids = _action_seed_model_ids(action)
            if trigger.model_id:
                seed_model_ids.append(trigger.model_id)
            return await pathway_g_model_edges(
                trigger.tenant_id,
                conn,
                seed_model_ids=seed_model_ids,
                seed_entity_ids=_action_seed_entities(action, trigger),
                scope_actors=trigger.scope_actors,
                max_hops=cfg.model_edge_max_hops,
                limit=capped_budget(action.budget),
            )
    except (RetrievalPathwayError, ValidationError):
        return None
    return None


async def _execute_question_retrieval_actions(
    plans: list[_QuestionRetrievalPlan],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool | None,
) -> dict[str, list[_ActionExecutionRecord]]:
    if not plans:
        return {}
    if any(
        "_motif_stage" in action.filters
        for plan in plans
        for action in plan.actions_to_run
    ):
        return await _execute_question_retrieval_actions_staged(
            plans,
            trigger,
            conn,
            embedder,
            cfg,
            action_cache,
            read_pool=read_pool,
        )
    action_slots: list[tuple[str, RetrievalAction, tuple[Any, ...]]] = []
    for plan in plans:
        for action in plan.actions_to_run:
            action_slots.append((
                plan.question.question_id,
                action,
                _retrieval_action_cache_key(action, trigger, cfg),
            ))
    if not action_slots:
        return {plan.question.question_id: [] for plan in plans}

    if (
        not cfg.question_action_parallel_enabled
        or read_pool is None
        or int(cfg.question_action_parallelism) <= 1
    ):
        return await _execute_question_retrieval_actions_serial(
            action_slots,
            trigger,
            conn,
            embedder,
            cfg,
            action_cache,
            read_pool=read_pool,
        )

    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {
        plan.question.question_id: [] for plan in plans
    }
    first_slot_by_key: dict[tuple[Any, ...], tuple[str, RetrievalAction]] = {}
    duplicate_slots: list[tuple[str, RetrievalAction, tuple[Any, ...]]] = []
    for qid, action, cache_key in action_slots:
        cached = action_cache.get(cache_key)
        if cached is not None:
            records_by_qid.setdefault(qid, []).append(_ActionExecutionRecord(
                action=action,
                path_result=cached,
                timing_note=_action_timing_note(action, cached, elapsed_ms=0, cache_hit=True),
            ))
            continue
        if cache_key in first_slot_by_key:
            duplicate_slots.append((qid, action, cache_key))
            continue
        first_slot_by_key[cache_key] = (qid, action)

    semaphore = asyncio.Semaphore(max(1, int(cfg.question_action_parallelism)))

    async def run_one(cache_key: tuple[Any, ...], qid: str, action: RetrievalAction):
        async with semaphore:
            started = time.perf_counter()
            async with read_pool.acquire() as action_conn:
                path_result = await _execute_action(
                    action,
                    trigger,
                    action_conn,
                    embedder,
                    cfg,
                    read_pool=read_pool,
                )
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return cache_key, qid, action, path_result, elapsed_ms

    task_results = await asyncio.gather(*(
        run_one(cache_key, qid, action)
        for cache_key, (qid, action) in first_slot_by_key.items()
    ))
    for cache_key, qid, action, path_result, elapsed_ms in task_results:
        if path_result is not None:
            action_cache[cache_key] = path_result
        records_by_qid.setdefault(qid, []).append(_ActionExecutionRecord(
            action=action,
            path_result=path_result,
            timing_note=_action_timing_note(
                action,
                path_result,
                elapsed_ms=elapsed_ms,
                cache_hit=False,
            ),
        ))

    for qid, action, cache_key in duplicate_slots:
        path_result = action_cache.get(cache_key)
        records_by_qid.setdefault(qid, []).append(_ActionExecutionRecord(
            action=action,
            path_result=path_result,
            timing_note=_action_timing_note(
                action,
                path_result,
                elapsed_ms=0,
                cache_hit=True,
            ),
        ))
    return records_by_qid


async def _execute_question_retrieval_actions_staged(
    plans: list[_QuestionRetrievalPlan],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool | None,
) -> dict[str, list[_ActionExecutionRecord]]:
    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {
        plan.question.question_id: [] for plan in plans
    }
    for plan in plans:
        prior_results: list[PathwayResult] = []
        actions_by_stage: dict[int, list[RetrievalAction]] = {}
        for action in plan.actions_to_run:
            try:
                stage = max(1, int(action.filters.get("_motif_stage") or 1))
            except (TypeError, ValueError):
                stage = 1
            actions_by_stage.setdefault(stage, []).append(action)

        for stage in sorted(actions_by_stage):
            for raw_action in actions_by_stage[stage]:
                action = _bind_action_to_previous_results(
                    raw_action,
                    trigger,
                    prior_results,
                )
                cache_key = _retrieval_action_cache_key(action, trigger, cfg)
                path_result = action_cache.get(cache_key)
                cache_hit = path_result is not None
                started = time.perf_counter()
                if path_result is None:
                    path_result = await _execute_action(
                        action,
                        trigger,
                        conn,
                        embedder,
                        cfg,
                        read_pool=read_pool,
                    )
                    if path_result is not None:
                        action_cache[cache_key] = path_result
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                if path_result is not None:
                    prior_results.append(path_result)
                records_by_qid.setdefault(plan.question.question_id, []).append(
                    _ActionExecutionRecord(
                        action=action,
                        path_result=path_result,
                        timing_note=_action_timing_note(
                            action,
                            path_result,
                            elapsed_ms=elapsed_ms,
                            cache_hit=cache_hit,
                        ),
                    )
                )
    return records_by_qid


def _bind_action_to_previous_results(
    action: RetrievalAction,
    trigger: TriggerContext,
    prior_results: list[PathwayResult],
) -> RetrievalAction:
    if not action.filters.get("_bind_previous_scope") or not prior_results:
        return action
    seed_entities = _dedupe_seed_entities([
        *_action_seed_entities(action, trigger),
        *_seed_entities_from_pathway_results(prior_results),
    ])[:24]
    seed_model_ids = [
        str(model_id)
        for model_id in _seed_model_ids_from_pathway_results(prior_results)[:24]
    ]
    filters = dict(action.filters)
    if seed_entities:
        filters["seed_entities"] = seed_entities
    if seed_model_ids:
        filters["seed_model_ids"] = seed_model_ids
    filters["_bound_scope"] = {
        "model_count": len(seed_model_ids),
        "entity_count": len(seed_entities),
    }
    return replace(action, filters=filters)


def _seed_model_ids_from_pathway_results(
    results: list[PathwayResult],
) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for result in results:
        for model in result.models:
            mid = model.id
            if mid not in seen:
                seen.add(mid)
                out.append(mid)
    return out


def _seed_entities_from_pathway_results(
    results: list[PathwayResult],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for result in results:
        for model in result.models[:24]:
            raw_entities = getattr(model, "scope_entities", None) or []
            if isinstance(raw_entities, list):
                out.extend(item for item in raw_entities if isinstance(item, dict))
        for resource in result.resources[:12]:
            rid = getattr(resource, "id", None)
            if rid is not None:
                out.append({"type": "resource", "id": str(rid)})
        for key, entity_type in (
            ("commitments", "commitment"),
            ("goals", "goal"),
            ("decisions", "decision"),
        ):
            for act in (result.acts or {}).get(key, [])[:12]:
                aid = getattr(act, "id", None)
                if aid is not None:
                    out.append({"type": entity_type, "id": str(aid)})
    return _dedupe_seed_entities(out)


def _dedupe_seed_entities(
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        etype = str(entity.get("type") or "").strip()
        eid = str(entity.get("id") or "").strip()
        if not etype or not eid:
            continue
        key = (etype.casefold(), eid)
        if key in seen:
            continue
        seen.add(key)
        out.append({"type": etype, "id": eid})
    return out


async def _execute_question_retrieval_actions_serial(
    action_slots: list[tuple[str, RetrievalAction, tuple[Any, ...]]],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool | None,
) -> dict[str, list[_ActionExecutionRecord]]:
    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {}
    for qid, action, cache_key in action_slots:
        path_result = action_cache.get(cache_key)
        cache_hit = path_result is not None
        started = time.perf_counter()
        if path_result is None:
            path_result = await _execute_action(
                action,
                trigger,
                conn,
                embedder,
                cfg,
                read_pool=read_pool,
            )
            if path_result is not None:
                action_cache[cache_key] = path_result
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        records_by_qid.setdefault(qid, []).append(_ActionExecutionRecord(
            action=action,
            path_result=path_result,
            timing_note=_action_timing_note(
                action,
                path_result,
                elapsed_ms=elapsed_ms,
                cache_hit=cache_hit,
            ),
        ))
    return records_by_qid


def _action_timing_note(
    action: RetrievalAction,
    path_result: PathwayResult | None,
    *,
    elapsed_ms: int,
    cache_hit: bool,
) -> dict[str, Any]:
    note: dict[str, Any] = {
        "question_id": action.question_id,
        "path": action.path,
        "target": action.target,
        "elapsed_ms": int(elapsed_ms),
        "cache_hit": bool(cache_hit),
        "returned": path_result is not None,
    }
    if path_result is not None:
        note.update({
            "models": len(path_result.models),
            "observations": len(path_result.observations),
            "resources": len(path_result.resources),
            "source_pathway": path_result.source_pathway,
        })
    if action.filters.get("_motif_id"):
        note["motif_id"] = action.filters.get("_motif_id")
        note["motif_stage"] = action.filters.get("_motif_stage")
        note["motif_match_score"] = action.filters.get("_motif_match_score")
        note["motif_utility_score"] = action.filters.get("_motif_utility_score")
        if action.filters.get("_bound_scope"):
            note["bound_scope"] = action.filters.get("_bound_scope")
    return note


def _build_sage_reader(cfg: InquiryConfig) -> Any | None:
    if not cfg.sage_reader_enabled:
        return None
    try:
        from services.reasoning.sage.reader import ReaderBudget, SynthesisReader
    except Exception:  # noqa: BLE001
        return None
    return SynthesisReader(
        budget=ReaderBudget(
            max_nodes=max(8, int(cfg.result_model_limit)),
            max_edges=max(16, int(cfg.result_model_limit) * 2),
            max_evidence_items=max(10, int(cfg.evidence_reservoir_limit)),
            lexical_candidates=max(10, int(cfg.candidate_model_limit // 3)),
            shortcut_candidates=12,
            affordance_candidates=max(10, int(cfg.candidate_model_limit // 4)),
            propagation_neighbors=max(24, int(cfg.candidate_model_limit // 2)),
            activation_seed_limit=max(
                24,
                min(
                    int(cfg.candidate_model_limit),
                    max(
                        int(cfg.result_model_limit) * 2,
                        int(cfg.candidate_model_limit // 2),
                    ),
                ),
            ),
            row_cache_enabled=bool(cfg.sage_reader_row_cache_enabled),
            shared_substrate_enabled=bool(cfg.sage_reader_shared_substrate_enabled),
            substrate_model_limit=max(24, min(96, int(cfg.candidate_model_limit))),
            substrate_edge_seed_limit=max(12, min(48, int(cfg.candidate_model_limit // 3))),
            substrate_edge_limit=max(32, min(96, int(cfg.candidate_model_limit // 2))),
            rerank_min_substrate_models=8,
            rerank_lexical_candidates=6,
            lexical_microquery_enabled=True,
            lexical_microquery_terms=8,
            lexical_microquery_per_term_limit=16,
        )
    )


async def _execute_sage_reader_actions_for_round(
    questions: list[InquiryQuestion],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    reader: Any | None,
    substrate: Any | None,
    hypotheses: tuple[Hypothesis, ...],
    read_pool: asyncpg.Pool | None,
) -> tuple[dict[str, RetrievalResult], dict[str, Any]]:
    if not questions or not cfg.sage_reader_enabled or reader is None:
        return {}, {
            "used": False,
            "reason": "disabled_or_empty",
            "question_count": len(questions),
        }

    parallel = (
        bool(cfg.sage_reader_parallel_enabled)
        and read_pool is not None
        and len(questions) > 1
        and int(cfg.sage_reader_parallelism) > 1
    )
    started = time.perf_counter()
    results: dict[str, RetrievalResult] = {}
    if not parallel:
        for question in questions:
            result = await _execute_sage_reader_action(
                question,
                trigger,
                conn,
                cfg,
                reader=reader,
                substrate=substrate,
                hypotheses=hypotheses,
            )
            if result is not None:
                results[question.question_id] = result
        return results, {
            "used": True,
            "parallel": False,
            "question_count": len(questions),
            "returned": len(results),
            "elapsed_ms": _elapsed_ms(started),
        }

    semaphore = asyncio.Semaphore(max(1, int(cfg.sage_reader_parallelism)))

    async def run_one(question: InquiryQuestion) -> tuple[str, RetrievalResult | None]:
        async with semaphore:
            async with read_pool.acquire() as read_conn:
                result = await _execute_sage_reader_action(
                    question,
                    trigger,
                    read_conn,
                    cfg,
                    reader=reader,
                    substrate=substrate,
                    hypotheses=hypotheses,
                )
                return question.question_id, result

    gathered = await asyncio.gather(*(run_one(question) for question in questions))
    for qid, result in gathered:
        if result is not None:
            results[qid] = result
    return results, {
        "used": True,
        "parallel": True,
        "parallelism": max(1, int(cfg.sage_reader_parallelism)),
        "question_count": len(questions),
        "returned": len(results),
        "elapsed_ms": _elapsed_ms(started),
    }


async def _execute_sage_reader_action(
    question: InquiryQuestion,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    hypotheses: tuple[Hypothesis, ...],
    reader: Any | None = None,
    substrate: Any | None = None,
) -> RetrievalResult | None:
    if not cfg.sage_reader_enabled:
        return None
    if reader is None:
        reader = _build_sage_reader(cfg)
    if reader is None:
        return None
    try:
        read = await reader.read(
            conn=conn,
            tenant_id=trigger.tenant_id,
            trigger=trigger,
            question_id=question.question_id,
            question=question.question,
            question_primitive=question.primitive,
            hypotheses=hypotheses,
            substrate=substrate,
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


_FOCUSED_INDEX_EXTRA_STOPWORDS = {
    "accountable", "active", "assigned", "before", "block", "blocked",
    "blocking", "blocker", "currently", "critical", "evidence", "existing",
    "found", "issue", "likely", "matching", "model", "models", "next",
    "owner", "owns", "path", "question", "recent", "related", "responsible",
    "risk", "same", "specific", "stable", "showing", "currently", "today",
    "whether", "which", "who", "what", "where", "when", "why", "how",
}


async def _execute_focused_index_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    model_limit: int,
) -> PathwayResult | None:
    raw_terms = action.filters.get("terms")
    terms = (
        [str(term) for term in raw_terms if str(term).strip()]
        if isinstance(raw_terms, list)
        else _focused_index_terms(
            action.query or _trigger_text(trigger),
            trigger,
            max_terms=int(cfg.focused_index_terms),
        )
    )
    primitive = str(action.filters.get("primitive") or "").upper()
    primitives = _focused_answerability_primitives_for(primitive)
    seed_pairs = _focused_seed_entity_pairs(_action_seed_entities(action, trigger))
    model_limit = max(1, int(model_limit))
    scope_limit = min(
        max(1, int(cfg.focused_index_scope_candidates)),
        model_limit,
    )

    hit_sources: dict[UUID, set[str]] = {}
    hit_counts: dict[UUID, int] = {}
    scope_overlaps: dict[UUID, int] = {}
    scores: dict[UUID, float] = {}

    def add_hits(hits: list[_FocusedIndexHit]) -> None:
        for hit in hits:
            hit_sources.setdefault(hit.model_id, set()).add(hit.source)
            hit_counts[hit.model_id] = max(
                hit_counts.get(hit.model_id, 0),
                int(hit.match_count),
            )
            scope_overlaps[hit.model_id] = max(
                scope_overlaps.get(hit.model_id, 0),
                int(hit.scope_overlap),
            )
            scores[hit.model_id] = scores.get(hit.model_id, 0.0) + float(hit.score)

    answerability_hits = await _focused_answerability_index_scan(
        conn,
        tenant_id=trigger.tenant_id,
        primitives=primitives,
        terms=terms,
        seed_pairs=seed_pairs,
        limit=model_limit,
    )
    add_hits(answerability_hits)
    scoped_sparse_hits = await _focused_scope_sparse_scan(
        conn,
        tenant_id=trigger.tenant_id,
        terms=terms,
        seed_pairs=seed_pairs,
        limit=model_limit,
    )
    add_hits(scoped_sparse_hits)
    direct_scope_hits = await _focused_direct_scope_scan(
        conn,
        tenant_id=trigger.tenant_id,
        seed_pairs=seed_pairs,
        limit=scope_limit,
    )
    add_hits(direct_scope_hits)

    if not scores:
        return None

    ordered_ids = sorted(
        scores,
        key=lambda model_id: (
            -scores[model_id],
            -scope_overlaps.get(model_id, 0),
            -hit_counts.get(model_id, 0),
            str(model_id),
        ),
    )[:model_limit]
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(
        ordered_ids,
        conn=conn,
    )
    by_id = {model.id: model for model in models}
    ordered_models = [by_id[mid] for mid in ordered_ids if mid in by_id]
    if not ordered_models:
        return None

    return PathwayResult(
        models=ordered_models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="focused_index",
        notes={
            "target": action.target,
            "primitive": primitive,
            "primitives": list(primitives),
            "terms": terms,
            "term_groups": _focused_index_lookup_groups(terms),
            "seed_scope_pairs": len(seed_pairs),
            "answerability_hits": len(answerability_hits),
            "scoped_sparse_hits": len(scoped_sparse_hits),
            "direct_scope_hits": len(direct_scope_hits),
            "merged_hits": len(scores),
            "returned_models": len(ordered_models),
            "top_hits": [
                {
                    "model_id": str(mid),
                    "score": round(scores.get(mid, 0.0), 4),
                    "sources": sorted(hit_sources.get(mid, set())),
                    "match_count": hit_counts.get(mid, 0),
                    "scope_overlap": scope_overlaps.get(mid, 0),
                }
                for mid in ordered_ids[:8]
            ],
        },
    )


def _focused_answerability_primitives_for(primitive: str) -> tuple[str, ...]:
    normalized = str(primitive or "").strip().upper()
    aliases = {
        "COMMITMENT": ("COMMITMENT", "DEPENDENCY"),
        "CONSTRAINT": ("CONSTRAINT", "COUNTEREVIDENCE"),
        "COUNTEREVIDENCE": ("COUNTEREVIDENCE", "CONSTRAINT"),
        "DEPENDENCY": ("DEPENDENCY", "COMMITMENT"),
        "GOAL_IMPACT": ("GOAL_IMPACT", "COMMITMENT"),
        "OWNERSHIP": ("OWNERSHIP", "COMMITMENT", "DEPENDENCY"),
        "RECURRENCE": ("RECURRENCE", "DEPENDENCY", "COUNTEREVIDENCE"),
    }
    return aliases.get(normalized, (normalized,)) if normalized else ()


def _focused_seed_entity_pairs(raw_entities: Any) -> list[tuple[str, UUID]]:
    pairs: list[tuple[str, UUID]] = []
    seen: set[tuple[str, UUID]] = set()
    if not isinstance(raw_entities, list):
        return pairs
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        raw_type = raw.get("type")
        raw_id = raw.get("id")
        if raw_type is None or raw_id is None:
            continue
        try:
            entity_id = UUID(str(raw_id))
        except (TypeError, ValueError):
            continue
        entity_type = str(raw_type)
        candidates = (
            ("customer", "customer_resource", "resource")
            if entity_type in {"customer", "customer_resource", "resource"}
            else (entity_type,)
        )
        for candidate_type in candidates:
            pair = (candidate_type, entity_id)
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)
    return pairs


def _focused_index_terms(
    question_text: str,
    trigger: TriggerContext,
    *,
    max_terms: int,
) -> list[str]:
    max_terms = max(1, int(max_terms))
    combined = f"{question_text}\n{_trigger_text(trigger)}"
    terms: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        clean = " ".join(str(value or "").strip(" '\"`.,;:()[]{}").split())
        if not clean:
            return
        tokens = _focused_material_tokens(clean)
        if not tokens:
            return
        normalized = " ".join(tokens[:4])
        if normalized in seen:
            return
        seen.add(normalized)
        terms.append(normalized)

    for quoted in re.findall(r"['\"]([^'\"]{4,100})['\"]", combined):
        add(quoted)

    for match in re.finditer(
        r"\b(?:[A-Z][A-Za-z0-9_-]{2,}|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9_-]{2,}|[A-Z]{2,})){1,4}",
        combined,
    ):
        phrase = match.group(0)
        if phrase.casefold().startswith(("who ", "what ", "which ", "does ", "is ")):
            continue
        add(phrase)

    tokens = _focused_material_tokens(combined)
    for width in (3, 2):
        for index in range(0, max(0, len(tokens) - width + 1)):
            window = tokens[index:index + width]
            if any(_is_focused_strong_token(token) for token in window):
                add(" ".join(window))
            if len(terms) >= max_terms:
                return terms[:max_terms]
    for token in tokens:
        if _is_focused_strong_token(token):
            add(token)
        if len(terms) >= max_terms:
            break
    return terms[:max_terms]


def _focused_material_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{2,}", str(text)):
        token = raw.casefold()
        if (
            token in _RELEVANCE_STOPWORDS
            or token in _FOCUSED_INDEX_EXTRA_STOPWORDS
            or token.isdigit()
        ):
            continue
        if len(token) < 4 and not raw.isupper():
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _is_focused_strong_token(token: str) -> bool:
    value = str(token or "")
    return (
        len(value) >= 6
        or "-" in value
        or "_" in value
        or any(ch.isdigit() for ch in value)
    )


def _focused_index_lookup_groups(terms: list[str] | tuple[str, ...]) -> list[list[str]]:
    groups = _hybrid_sparse_lookup_groups(terms)
    if groups:
        return groups
    out: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for token in _focused_material_tokens(" ".join(str(t) for t in terms)):
        if not _is_focused_strong_token(token):
            continue
        key = (token,)
        if key in seen:
            continue
        seen.add(key)
        out.append([token])
        if len(out) >= 8:
            break
    return out


async def _focused_answerability_index_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitives: tuple[str, ...],
    terms: list[str] | tuple[str, ...],
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[_FocusedIndexHit]:
    groups = _focused_index_lookup_groups(terms)
    if not primitives or not groups or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_answerability_index')")
    if table is None:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($4::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 count(DISTINCT term)::int AS token_count
          FROM group_tokens
          GROUP BY group_ord
        ),
        matched AS MATERIALIZED (
          SELECT mai.model_id,
                 mai.primitive,
                 gt.group_ord,
                 count(DISTINCT gt.term)::int AS matched_terms
          FROM group_tokens gt
          JOIN model_answerability_index mai
            ON mai.tenant_id = $1
           AND mai.status = 'active'
           AND mai.primitive = ANY($3::text[])
           AND mai.term = gt.term
          GROUP BY mai.model_id, mai.primitive, gt.group_ord
        ),
        group_hits AS MATERIALIZED (
          SELECT matched.model_id,
                 matched.primitive,
                 matched.group_ord,
                 group_sizes.token_count
          FROM matched
          JOIN group_sizes
            ON group_sizes.group_ord = matched.group_ord
          WHERE matched.matched_terms = group_sizes.token_count
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT primitive)::int AS primitive_match_count,
                 sum(token_count)::int AS match_count,
                 min(group_ord)::int AS first_group_ord
          FROM group_hits
          GROUP BY model_id
        ),
        scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($5::text[], $6::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               scored.match_count,
               scored.primitive_match_count,
               coalesce(scope_overlap.overlap, 0)::int AS scope_overlap
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        LEFT JOIN scope_overlap
          ON scope_overlap.model_id = m.id
        WHERE m.status = 'active'
        ORDER BY coalesce(scope_overlap.overlap, 0) DESC,
                 scored.match_count DESC,
                 scored.primitive_match_count DESC,
                 scored.first_group_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        list(primitives),
        json.dumps(groups),
        scope_types,
        scope_ids,
        label="focused_answerability_index",
    )
    return [
        _FocusedIndexHit(
            model_id=row["id"],
            score=0.78
            + min(0.18, int(row["match_count"] or 0) * 0.025)
            + min(0.18, int(row["scope_overlap"] or 0) * 0.05),
            source="answerability_index",
            match_count=int(row["match_count"] or 0),
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def _focused_scope_sparse_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    terms: list[str] | tuple[str, ...],
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[_FocusedIndexHit]:
    groups = _focused_index_lookup_groups(terms)
    if not groups or not seed_pairs or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($3::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 count(DISTINCT term)::int AS token_count
          FROM group_tokens
          GROUP BY group_ord
        ),
        lexical AS MATERIALIZED (
          SELECT mst.model_id,
                 gt.group_ord,
                 count(DISTINCT gt.term)::int AS matched_terms
          FROM group_tokens gt
          JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = gt.term
          GROUP BY mst.model_id, gt.group_ord
        ),
        lexical_hits AS MATERIALIZED (
          SELECT lexical.model_id,
                 lexical.group_ord,
                 group_sizes.token_count
          FROM lexical
          JOIN group_sizes
            ON group_sizes.group_ord = lexical.group_ord
          WHERE lexical.matched_terms = group_sizes.token_count
        ),
        lexical_scored AS MATERIALIZED (
          SELECT model_id,
                 sum(token_count)::int AS match_count,
                 min(group_ord)::int AS first_group_ord
          FROM lexical_hits
          GROUP BY model_id
        ),
        scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($4::text[], $5::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               lexical_scored.match_count,
               scope_overlap.overlap::int AS scope_overlap
        FROM lexical_scored
        JOIN scope_overlap
          ON scope_overlap.model_id = lexical_scored.model_id
        JOIN models m
          ON m.id = lexical_scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scope_overlap.overlap DESC,
                 lexical_scored.match_count DESC,
                 lexical_scored.first_group_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        json.dumps(groups),
        scope_types,
        scope_ids,
        label="focused_scope_sparse",
    )
    return [
        _FocusedIndexHit(
            model_id=row["id"],
            score=0.70
            + min(0.20, int(row["scope_overlap"] or 0) * 0.055)
            + min(0.16, int(row["match_count"] or 0) * 0.025),
            source="scope_sparse",
            match_count=int(row["match_count"] or 0),
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def _focused_direct_scope_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[_FocusedIndexHit]:
    if not seed_pairs or limit <= 0:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($3::text[], $4::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               scope_overlap.overlap::int AS scope_overlap
        FROM scope_overlap
        JOIN models m
          ON m.id = scope_overlap.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scope_overlap.overlap DESC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        scope_types,
        scope_ids,
        label="focused_direct_scope",
    )
    return [
        _FocusedIndexHit(
            model_id=row["id"],
            score=0.42 + min(0.18, int(row["scope_overlap"] or 0) * 0.04),
            source="direct_scope",
            match_count=0,
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def _execute_semantic_hybrid_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    model_limit: int,
) -> PathwayResult:
    query_text = action.query or _trigger_text(trigger)
    trigger_text = _trigger_text(trigger)
    precomputed_vector = (
        trigger.precomputed_seed_vector
        if embedder is None or query_text == trigger_text
        else None
    )
    result = await pathway_b_semantic(
        query_text,
        trigger.tenant_id,
        conn,
        k=model_limit,
        embedder=embedder,
        precomputed_vector=precomputed_vector,
        event_actors=trigger.scope_actors,
        event_entities=_action_seed_entities(action, trigger),
    )
    semantic_ids = {model.id for model in result.models}
    hybrid_note: dict[str, Any] = {
        "enabled": bool(cfg.semantic_hybrid_lexical_enabled),
        "used": False,
        "semantic_count": len(result.models),
        "lexical_count": 0,
        "merged_count": len(result.models),
    }
    if not cfg.semantic_hybrid_lexical_enabled:
        hybrid_note["reason"] = "disabled"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    terms = _hybrid_lexical_terms(
        query_text,
        trigger,
        max_terms=max(1, int(cfg.semantic_hybrid_lexical_terms)),
    )
    hybrid_note["terms"] = terms
    if not terms:
        hybrid_note["reason"] = "no_lexical_terms"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    lexical_limit = min(
        max(1, int(cfg.semantic_hybrid_lexical_max_candidates)),
        max(1, int(model_limit) * 2),
    )
    per_term_limit = max(1, int(cfg.semantic_hybrid_lexical_per_term_limit))
    lexical_hits = await _hybrid_lexical_model_scan(
        trigger,
        conn,
        terms=terms,
        limit=lexical_limit,
        per_term_limit=per_term_limit,
    )
    hybrid_note.update({
        "lexical_limit": lexical_limit,
        "lexical_per_term_limit": per_term_limit,
        "lexical_count": len(lexical_hits),
    })
    if not lexical_hits:
        hybrid_note["reason"] = "no_lexical_hits"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    result.models = _merge_hybrid_semantic_lexical_models(
        result.models,
        lexical_hits,
        limit=max(1, int(model_limit)),
    )
    hybrid_note["used"] = True
    hybrid_note["merged_count"] = len(result.models)
    hybrid_note["lexical_only_selected"] = sum(
        1 for model in result.models if model.id not in semantic_ids
    )
    result.notes["semantic_hybrid_lexical"] = hybrid_note
    return result


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


def _hybrid_lexical_terms(
    query_text: str,
    trigger: TriggerContext,
    *,
    max_terms: int,
) -> list[str]:
    max_terms = max(1, int(max_terms))
    strong: list[str] = []
    weak: list[str] = []

    def add(raw_text: str, *, trigger_side: bool = False) -> None:
        for raw in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{2,}", str(raw_text)):
            token = raw.casefold()
            if token in _RELEVANCE_STOPWORDS or token.isdigit():
                continue
            has_symbol = "-" in token or "_" in token or any(ch.isdigit() for ch in token)
            is_acronym = len(raw) <= 6 and raw.upper() == raw and any(ch.isalpha() for ch in raw)
            is_strong = has_symbol or is_acronym or len(token) >= (5 if trigger_side else 4)
            target = strong if is_strong else weak
            if token not in strong and token not in weak:
                target.append(token)

    add(query_text, trigger_side=False)
    add(_trigger_text(trigger), trigger_side=True)
    return (strong + weak)[:max_terms]


def _like_patterns_for_terms(terms: list[str] | tuple[str, ...]) -> list[str]:
    patterns: list[str] = []
    for term in terms:
        value = str(term or "").casefold().strip()
        if not value:
            continue
        escaped = (
            value
            .replace("!", "!!")
            .replace("%", "!%")
            .replace("_", "!_")
        )
        pattern = f"%{escaped}%"
        if pattern not in patterns:
            patterns.append(pattern)
    return patterns


async def _hybrid_lexical_model_scan(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    terms: list[str] | tuple[str, ...],
    limit: int,
    per_term_limit: int,
) -> list[tuple[ModelRow, int]]:
    sparse_hits = await _hybrid_sparse_model_scan(
        trigger,
        conn,
        terms=terms,
        limit=limit,
        per_term_limit=per_term_limit,
    )
    if sparse_hits:
        return sparse_hits

    patterns = _like_patterns_for_terms(terms)
    if not patterns or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_search_documents')")
    if table is None:
        return []
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH patterns AS (
          SELECT pattern, ord
          FROM unnest($3::text[]) WITH ORDINALITY AS p(pattern, ord)
        ),
        per_pattern AS MATERIALIZED (
          SELECT hit.model_id,
                 p.ord::int AS pattern_ord
          FROM patterns p
          CROSS JOIN LATERAL (
            SELECT msd.model_id
            FROM model_search_documents msd
            JOIN models m
              ON m.id = msd.model_id
             AND m.tenant_id = msd.tenant_id
            WHERE msd.tenant_id = $1
              AND msd.status = 'active'
              AND m.status = 'active'
              AND msd.search_text LIKE p.pattern ESCAPE '!'
            ORDER BY m.activation DESC, m.created_at DESC, m.id
            LIMIT $4
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(*)::int AS match_count,
                 min(pattern_ord)::int AS first_pattern_ord
          FROM per_pattern
          GROUP BY model_id
        )
        SELECT m.id,
               scored.match_count
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.match_count DESC,
                 scored.first_pattern_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        trigger.tenant_id,
        max(1, int(limit)),
        patterns,
        max(1, int(per_term_limit)),
        label="hybrid_lexical",
    )
    ids = [row["id"] for row in rows]
    if not ids:
        return []
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(ids, conn=conn)
    by_id = {model.id: model for model in models}
    return [
        (by_id[row["id"]], int(row["match_count"] or 1))
        for row in rows
        if row["id"] in by_id
    ]


async def _hybrid_sparse_model_scan(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    terms: list[str] | tuple[str, ...],
    limit: int,
    per_term_limit: int,
) -> list[tuple[ModelRow, int]]:
    lookup_terms = _hybrid_sparse_lookup_terms(terms)
    if not lookup_terms or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return []
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH query_terms AS MATERIALIZED (
          SELECT term::text,
                 ord::int AS term_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS q(term, ord)
        ),
        query_meta AS MATERIALIZED (
          SELECT count(*)::int AS query_term_count
          FROM query_terms
        ),
        active_models AS MATERIALIZED (
          SELECT greatest(1, count(*)::int)::float8 AS active_model_count
          FROM models
          WHERE tenant_id = $1
            AND status = 'active'
        ),
        term_stats AS MATERIALIZED (
          SELECT qt.term,
                 qt.term_ord,
                 count(mstat.id)::int AS term_df
          FROM query_terms qt
          LEFT JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = qt.term
          LEFT JOIN models mstat
            ON mstat.id = mst.model_id
           AND mstat.tenant_id = mst.tenant_id
           AND mstat.status = 'active'
          GROUP BY qt.term, qt.term_ord
        ),
        term_hits AS MATERIALIZED (
          SELECT ts.term,
                 ts.term_ord,
                 ts.term_df,
                 hit.model_id,
                 hit.weight,
                 (
                   ln((am.active_model_count + 1.0) / (ts.term_df::float8 + 1.0))
                   + 1.0
                 )::float8 AS idf
          FROM term_stats ts
          CROSS JOIN active_models am
          CROSS JOIN LATERAL (
            SELECT mst.model_id,
                   mst.weight
            FROM model_sparse_terms mst
            JOIN models mhit
              ON mhit.id = mst.model_id
             AND mhit.tenant_id = mst.tenant_id
             AND mhit.status = 'active'
            WHERE mst.tenant_id = $1
              AND mst.status = 'active'
              AND mst.term = ts.term
            ORDER BY mst.weight DESC,
                     mhit.activation DESC,
                     mhit.created_at DESC,
                     mst.model_id
            LIMIT $4
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT term)::int AS match_count,
                 sum(weight * idf)::real AS weighted_score,
                 min(term_ord)::int AS first_term_ord,
                 bool_or(
                   term = ANY($5::text[])
                   AND term_df <= $6::int
                 ) AS has_strong_singleton
          FROM term_hits
          GROUP BY model_id
        )
        SELECT m.id,
               scored.match_count
        FROM scored
        CROSS JOIN query_meta
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
          AND (
            query_meta.query_term_count <= 1
            OR scored.match_count >= LEAST(2, query_meta.query_term_count)
            OR scored.has_strong_singleton
          )
        ORDER BY scored.match_count DESC,
                 scored.weighted_score DESC,
                 scored.first_term_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        trigger.tenant_id,
        max(1, int(limit)),
        lookup_terms,
        max(1, int(per_term_limit)),
        _hybrid_sparse_strong_single_match_terms(lookup_terms),
        _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
        label="hybrid_sparse",
    )
    ids = [row["id"] for row in rows]
    if not ids:
        return []
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(ids, conn=conn)
    by_id = {model.id: model for model in models}
    return [
        (by_id[row["id"]], int(row["match_count"] or 1))
        for row in rows
        if row["id"] in by_id
    ]


def _hybrid_sparse_lookup_terms(terms: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for raw in terms:
        for token in re.findall(
            r"[a-z0-9][a-z0-9_-]{2,}",
            str(raw or "").casefold(),
        ):
            if token in _RELEVANCE_STOPWORDS or token.isdigit():
                continue
            if token not in out:
                out.append(token)
                if len(out) >= 8:
                    return out
    return out[:8]


def _hybrid_sparse_strong_single_match_terms(terms: list[str]) -> list[str]:
    return [
        term
        for term in terms
        if len(term) >= 4
        and any(ch.isdigit() or ch in {"-", "_"} for ch in term)
    ]


def _hybrid_sparse_lookup_groups(terms: list[str] | tuple[str, ...]) -> list[list[str]]:
    out: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for raw in terms:
        tokens = [
            token
            for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", str(raw or "").casefold())
            if token not in _RELEVANCE_STOPWORDS and not token.isdigit()
        ]
        tokens = list(dict.fromkeys(tokens))
        if len(tokens) >= 2:
            group = tokens[:4]
        elif tokens and len(tokens[0]) >= 6:
            group = tokens
        else:
            continue
        key = tuple(group)
        if key in seen:
            continue
        seen.add(key)
        out.append(group)
        if len(out) >= 8:
            break
    return out


async def _fetch_bounded_lookup_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    label: str = "lookup",
) -> list[asyncpg.Record]:
    try:
        async with conn.transaction():
            await conn.execute(
                "SET LOCAL statement_timeout = "
                f"{_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS}"
            )
            return list(await conn.fetch(query, *args))
    except asyncpg.QueryCanceledError:
        import structlog

        structlog.get_logger(__name__).warning(
            "inquiry.bounded_lookup_statement_timeout",
            label=label,
            timeout_ms=_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS,
        )
        return []


async def _fetch_hybrid_lexical_fallback_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
) -> list[asyncpg.Record]:
    return await _fetch_bounded_lookup_rows(
        conn,
        query,
        *args,
        label="hybrid_lexical",
    )


def _merge_hybrid_semantic_lexical_models(
    semantic_models: list[ModelRow],
    lexical_hits: list[tuple[ModelRow, int]],
    *,
    limit: int,
) -> list[ModelRow]:
    scores: dict[UUID, float] = {}
    by_id: dict[UUID, ModelRow] = {}
    ranks: dict[UUID, tuple[int, int]] = {}
    rrf_k = 60.0

    for rank, model in enumerate(semantic_models, start=1):
        by_id[model.id] = model
        scores[model.id] = scores.get(model.id, 0.0) + 1.0 / (rrf_k + rank)
        old = ranks.get(model.id, (10_000, 10_000))
        ranks[model.id] = (min(old[0], rank), old[1])

    for rank, (model, match_count) in enumerate(lexical_hits, start=1):
        by_id.setdefault(model.id, model)
        lexical_score = 0.92 / (rrf_k + rank)
        lexical_score += min(0.008, max(1, int(match_count)) * 0.002)
        scores[model.id] = scores.get(model.id, 0.0) + lexical_score
        old = ranks.get(model.id, (10_000, 10_000))
        ranks[model.id] = (old[0], min(old[1], rank))

    ordered_ids = sorted(
        by_id,
        key=lambda model_id: (
            -scores.get(model_id, 0.0),
            ranks.get(model_id, (10_000, 10_000))[0],
            ranks.get(model_id, (10_000, 10_000))[1],
            str(model_id),
        ),
    )
    return [by_id[model_id] for model_id in ordered_ids[: max(1, int(limit))]]


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


def _filter_context_packet_evidence(
    evidence: list[EvidenceCard],
    mode: str,
    answers: list[QuestionAnswer],
) -> tuple[list[EvidenceCard], dict[str, Any], set[str]]:
    normalized = str(mode or "model_first").strip().lower()
    if normalized not in {"all", "model_first", "models_only"}:
        normalized = "model_first"
    model_cards = [card for card in evidence if card.source_type == "model"]
    model_ref_ids = {str(card.evidence_id) for card in model_cards}
    answer_questions_by_ref: dict[str, set[str]] = {}
    model_answer_questions: set[str] = set()
    for answer in answers:
        refs = [*answer.supporting_evidence, *answer.counterevidence]
        if any(ref in model_ref_ids for ref in refs):
            model_answer_questions.add(answer.question_id)
        for ref in refs:
            answer_questions_by_ref.setdefault(ref, set()).add(answer.question_id)

    def required_answer_ref(card: EvidenceCard) -> bool:
        question_ids = answer_questions_by_ref.get(str(card.evidence_id), set())
        return any(qid not in model_answer_questions for qid in question_ids)

    answer_required_ids = {
        str(card.evidence_id)
        for card in evidence
        if card.source_type != "model" and required_answer_ref(card)
    }
    if normalized == "all":
        selected = list(evidence)
        fallback_reason = None
    elif not model_cards:
        selected = list(evidence)
        fallback_reason = "no_model_evidence"
    elif normalized == "models_only":
        selected = model_cards
        fallback_reason = None
    else:
        selected = [
            card
            for card in evidence
            if card.source_type == "model"
            or card.source_type in {"commitment", "goal", "decision", "resource"}
            or bool(card.weakens_hypotheses or card.contradicts_hypotheses)
            or required_answer_ref(card)
        ]
        fallback_reason = None

    selected_ids = {id(card) for card in selected}
    suppressed = [card for card in evidence if id(card) not in selected_ids]
    return selected, {
        "mode": normalized,
        "input_evidence_count": len(evidence),
        "packet_evidence_count": len(selected),
        "model_evidence_count": len(model_cards),
        "non_model_evidence_count": len(evidence) - len(model_cards),
        "suppressed_observation_count": sum(
            1 for card in suppressed if card.source_type == "observation"
        ),
        "suppressed_non_model_count": sum(
            1 for card in suppressed if card.source_type != "model"
        ),
        "answer_required_non_model_count": sum(
            1
            for card in selected
            if card.source_type != "model" and required_answer_ref(card)
        ),
        "fallback_reason": fallback_reason,
    }, answer_required_ids


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
    evidence_mode: str = "model_first",
) -> dict[str, Any]:
    packet_evidence, evidence_policy, answer_required_ids = _filter_context_packet_evidence(
        evidence,
        evidence_mode,
        answers,
    )
    observation_fallback = (
        evidence_policy.get("fallback_reason") == "no_model_evidence"
    )
    decisive: list[dict[str, Any]] = []
    supporting_groups: dict[str, list[EvidenceCard]] = {}
    omitted: list[dict[str, Any]] = []
    used_tokens = 0
    for card in packet_evidence:
        item = _evidence_to_dict(card)
        cost = int(item.get("token_estimate") or 1)
        if used_tokens + cost <= token_budget and (
            card.contradicts_hypotheses
            or card.weakens_hypotheses
            or card.source_type in {"commitment", "goal", "decision", "resource"}
            or (card.source_type == "observation" and observation_fallback)
            or str(card.evidence_id) in answer_required_ids
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
    for item in _background_summaries(packet_evidence):
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
    state_contract = _state_contract_for_context_packet(trigger, packet_evidence)
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
        "candidate_state_changes": _candidate_state_changes(
            hypotheses,
            packet_evidence,
            sufficiency,
        ),
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
            "packet_evidence_count": len(packet_evidence),
            "evidence_policy": evidence_policy,
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


def _compact_inquiry_notes_for_persistence(
    notes: dict[str, Any],
    *,
    persist_full_sage_reader_notes: bool,
) -> dict[str, Any]:
    if persist_full_sage_reader_notes:
        return notes
    compact = dict(notes)
    if isinstance(compact.get("context_packet"), dict):
        compact["context_packet"] = {"stored_in_context_packet_column": True}
    sage_notes = compact.get("sage_reader")
    if isinstance(sage_notes, dict):
        compact["sage_reader"] = _compact_sage_reader_notes_for_persistence(
            sage_notes
        )
    compact["persist_compaction"] = {
        "sage_reader_full_notes": False,
        "context_packet_stored_once": True,
    }
    return compact


def _compact_sage_reader_notes_for_persistence(
    sage_notes: dict[str, Any],
) -> dict[str, Any]:
    compact = dict(sage_notes)
    questions = sage_notes.get("questions")
    if isinstance(questions, dict):
        compact["questions"] = {
            str(qid): _compact_sage_question_note_for_persistence(qnote)
            for qid, qnote in questions.items()
            if isinstance(qnote, dict)
        }
    compact["persist_compacted"] = True
    compact["omitted_payloads"] = [
        "questions.*.activations",
        "questions.*.debug.gate_scores",
        "questions.*.debug.activation_reasons",
    ]
    return compact


def _compact_sage_question_note_for_persistence(
    qnote: dict[str, Any],
) -> dict[str, Any]:
    activations = qnote.get("activations")
    activation_count = (
        len(activations)
        if isinstance(activations, list)
        else int(qnote.get("activation_trace_count") or 0)
    )
    selected_model_ids = [
        str(mid) for mid in (qnote.get("selected_model_ids") or [])
    ]
    return {
        "question_id": qnote.get("question_id"),
        "question_primitive": qnote.get("question_primitive"),
        "signature": qnote.get("signature"),
        "selected_model_ids": selected_model_ids,
        "selected_model_count": len(selected_model_ids),
        "projected_evidence_count": int(
            qnote.get("projected_evidence_count") or 0
        ),
        "activation_trace_count": activation_count,
        "debug": _compact_sage_reader_debug_for_persistence(
            qnote.get("debug")
        ),
        "activations_stored_in": "sage_reader_activations",
    }


def _compact_sage_reader_debug_for_persistence(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    selector = raw.get("selector") if isinstance(raw.get("selector"), dict) else {}
    intents = raw.get("intents") if isinstance(raw.get("intents"), list) else []
    gate_scores = raw.get("gate_scores")
    activation_reasons = raw.get("activation_reasons")
    return {
        "stage_timings_ms": raw.get("stage_timings_ms") or {},
        "learned_read_plan": raw.get("learned_read_plan") or {},
        "projection_budget": raw.get("projection_budget") or {},
        "projection_coverage": raw.get("projection_coverage") or {},
        "candidate_pool": raw.get("candidate_pool") or {},
        "row_cache": raw.get("row_cache") or {},
        "cue_extraction": raw.get("cue_extraction") or {},
        "intents": [
            {
                "intent": item.get("intent"),
                "paths": item.get("paths"),
                "expected_cost": item.get("expected_cost"),
                "expected_value": item.get("expected_value"),
                "target": _compact(item.get("target"), 180),
            }
            for item in intents
            if isinstance(item, dict)
        ],
        "selector": {
            "selected_node_count": len(selector.get("selected_nodes") or []),
            "selected_edge_count": len(selector.get("selected_edges") or []),
            "bridge_node_count": len(selector.get("bridge_nodes") or []),
            "coverage_metrics": selector.get("coverage_metrics") or {},
        },
        "gate_score_count": (
            len(gate_scores) if isinstance(gate_scores, dict) else 0
        ),
        "activation_reason_count": (
            len(activation_reasons)
            if isinstance(activation_reasons, (dict, list))
            else 0
        ),
    }


async def _persist_inquiry(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
    *,
    persist_full_sage_reader_notes: bool = False,
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
        json.dumps(
            _compact_inquiry_notes_for_persistence(
                result.notes,
                persist_full_sage_reader_notes=persist_full_sage_reader_notes,
            ),
            default=str,
        ),
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
        await _penalize_retrieval_motifs(conn, result, trigger)
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
    await _learn_retrieval_motifs(conn, result, trigger)
    await _penalize_retrieval_motifs(conn, result, trigger)
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


async def _learn_retrieval_motifs(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    if not _env_bool("INQUIRY_RETRIEVAL_MOTIFS_LEARNING_ENABLED", True):
        return
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.retrieval_motifs')"
    )
    if table_name is None:
        return
    actions_by_question: dict[str, list[RetrievalAction]] = {}
    for action in result.retrieval_actions:
        if action.path == "sage_reader":
            continue
        actions_by_question.setdefault(action.question_id, []).append(action)

    for question in result.questions:
        cards = [
            card for card in result.evidence_cards
            if question.question_id in card.retrieved_for_questions
        ]
        if not cards:
            continue
        raw_actions = actions_by_question.get(question.question_id, [])
        if not raw_actions:
            continue
        used_paths = {
            path
            for card in cards
            for path in card.retrieval_paths
            if path != "sage_reader"
        }
        useful_actions = [
            action for action in raw_actions
            if not used_paths or action.path in used_paths
        ]
        if len(useful_actions) < 2:
            continue
        plan = _motif_plan_from_actions(useful_actions)
        if not plan.get("actions"):
            continue
        signature = _motif_signature_for(trigger, question.primitive)
        signature_hash = _stable_hash(signature)
        plan_hash = _stable_hash(plan)
        credit = float(len(cards))
        cost = (
            0.08 * len(plan["actions"])
            + sum(float(action.get("budget") or 0) for action in plan["actions"]) / 500.0
        )
        utility = credit - cost
        if utility <= 0:
            continue
        await conn.execute(
            """
            INSERT INTO retrieval_motifs (
              id, tenant_id, signature, signature_hash,
              question_primitive, plan, plan_hash,
              maturity, utility_score, success_count,
              total_credit, total_cost, last_success_at, updated_at
            ) VALUES (
              $1, $2, $3::jsonb, $4,
              $5, $6::jsonb, $7,
              'active', $8, 1,
              $9, $10, now(), now()
            )
            ON CONFLICT (
              tenant_id, question_primitive, signature_hash, plan_hash
            )
            DO UPDATE SET
              success_count = retrieval_motifs.success_count + 1,
              total_credit = retrieval_motifs.total_credit + EXCLUDED.total_credit,
              total_cost = retrieval_motifs.total_cost + EXCLUDED.total_cost,
              utility_score = (
                (retrieval_motifs.total_credit + EXCLUDED.total_credit)
                - (retrieval_motifs.total_cost + EXCLUDED.total_cost)
              ) / GREATEST(
                retrieval_motifs.success_count
                + retrieval_motifs.failure_count
                + 1,
                1
              ),
              maturity = CASE
                WHEN retrieval_motifs.maturity = 'quarantined'
                THEN retrieval_motifs.maturity
                ELSE 'active'
              END,
              last_success_at = now(),
              updated_at = now()
            """,
            uuid7(),
            trigger.tenant_id,
            json.dumps(signature, default=str),
            signature_hash,
            question.primitive,
            json.dumps(plan, default=str),
            plan_hash,
            utility,
            credit,
            cost,
        )


async def _penalize_retrieval_motifs(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    if not _env_bool("INQUIRY_RETRIEVAL_MOTIF_FAILURE_LEARNING_ENABLED", True):
        return
    penalties = _motif_failure_penalties(result)
    if not penalties:
        return
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.retrieval_motifs')"
    )
    if table_name is None:
        return
    quarantine_failures = _env_int(
        "INQUIRY_RETRIEVAL_MOTIF_QUARANTINE_FAILURES",
        3,
        minimum=1,
    )
    quarantine_utility = _env_float(
        "INQUIRY_RETRIEVAL_MOTIF_QUARANTINE_UTILITY",
        0.0,
        minimum=-10.0,
    )
    for penalty in penalties:
        await conn.execute(
            """
            UPDATE retrieval_motifs
            SET
              failure_count = failure_count + 1,
              total_cost = total_cost + $3,
              utility_score = (
                total_credit - (total_cost + $3)
              ) / GREATEST(success_count + failure_count + 1, 1),
              maturity = CASE
                WHEN maturity = 'quarantined'
                THEN maturity
                WHEN failure_count + 1 >= $4
                  AND (
                    total_credit - (total_cost + $3)
                  ) / GREATEST(success_count + failure_count + 1, 1) <= $5
                THEN 'quarantined'
                ELSE maturity
              END,
              last_failure_at = now(),
              updated_at = now()
            WHERE tenant_id = $1
              AND id = $2
            """,
            trigger.tenant_id,
            penalty.motif_id,
            float(penalty.cost),
            quarantine_failures,
            quarantine_utility,
        )


def _motif_failure_penalties(result: InquiryResult) -> list[_RetrievalMotifPenalty]:
    motif_actions: dict[tuple[str, UUID], list[RetrievalAction]] = {}
    for action in result.retrieval_actions:
        motif_id = _action_motif_uuid(action)
        if motif_id is None:
            continue
        motif_actions.setdefault((action.question_id, motif_id), []).append(action)
    if not motif_actions:
        return []

    used_ids = _packet_used_evidence_ids(result.context_packet)
    timings = [
        note for note in (result.notes or {}).get("retrieval_action_timings", [])
        if isinstance(note, dict)
    ]
    output_by_motif: dict[tuple[str, UUID], dict[str, int]] = {}
    for note in timings:
        motif_id = _safe_uuid(note.get("motif_id"))
        question_id = str(note.get("question_id") or "")
        if motif_id is None or not question_id:
            continue
        bucket = output_by_motif.setdefault(
            (question_id, motif_id),
            {"models": 0, "observations": 0},
        )
        bucket["models"] += _safe_int(note.get("models"))
        bucket["observations"] += _safe_int(note.get("observations"))

    penalties: list[_RetrievalMotifPenalty] = []
    for (question_id, motif_id), actions in motif_actions.items():
        paths = {action.path for action in actions}
        cards = [
            card for card in result.evidence_cards
            if question_id in card.retrieved_for_questions
            and bool(card.retrieval_paths & paths)
        ]
        selected = [
            card for card in cards
            if str(card.evidence_id) in used_ids
        ]
        omitted = [card for card in cards if str(card.evidence_id) not in used_ids]
        low_value_omitted = [card for card in omitted if _is_low_value_model_noise(card)]
        outputs = output_by_motif.get((question_id, motif_id), {})
        returned_models = int(outputs.get("models") or 0)
        returned_observations = int(outputs.get("observations") or 0)
        selected_count = len(selected)
        omitted_count = len(omitted)

        reasons: list[str] = []
        if selected_count == 0 and (cards or returned_models >= 20 or returned_observations >= 8):
            reasons.append("no_packet_evidence")
        if (
            omitted_count >= _env_int(
                "INQUIRY_RETRIEVAL_MOTIF_NOISY_OMISSION_MIN",
                6,
                minimum=1,
            )
            and omitted_count >= max(3, selected_count * 3)
        ):
            reasons.append("noisy_omission_ratio")
        if returned_models >= _env_int(
            "INQUIRY_RETRIEVAL_MOTIF_WIDE_MODEL_THRESHOLD",
            80,
            minimum=1,
        ) and selected_count <= 2:
            reasons.append("wide_motif_selection")
        if len(low_value_omitted) >= 4:
            reasons.append("low_value_model_noise")
        if not reasons:
            continue

        raw_cost = 0.0
        if "no_packet_evidence" in reasons:
            raw_cost += 1.2
        if "noisy_omission_ratio" in reasons:
            raw_cost += min(3.0, 0.25 * omitted_count)
        if "wide_motif_selection" in reasons:
            raw_cost += min(2.5, returned_models / 80.0)
        if "low_value_model_noise" in reasons:
            raw_cost += min(2.0, 0.35 * len(low_value_omitted))
        benefit_discount = min(0.8, 0.12 * selected_count)
        cost = max(0.15, raw_cost - benefit_discount)
        penalties.append(
            _RetrievalMotifPenalty(
                motif_id=motif_id,
                question_id=question_id,
                cost=round(min(6.0, cost), 4),
                reasons=tuple(sorted(set(reasons))),
                selected_evidence=selected_count,
                omitted_evidence=omitted_count,
                returned_models=returned_models,
                returned_observations=returned_observations,
            )
        )
    return penalties


def _action_motif_uuid(action: RetrievalAction) -> UUID | None:
    return _safe_uuid((action.filters or {}).get("_motif_id"))


def _safe_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _packet_used_evidence_ids(packet: dict[str, Any]) -> set[str]:
    tiers = (packet or {}).get("tiers") or {}
    used: set[str] = set()
    for item in tiers.get("decisive_evidence", []) or []:
        if isinstance(item, dict) and item.get("evidence_id"):
            used.add(str(item["evidence_id"]))
    for group in tiers.get("supporting_evidence_groups", []) or []:
        if not isinstance(group, dict):
            continue
        for evidence_id in group.get("evidence_ids", []) or []:
            used.add(str(evidence_id))
    return used


def _motif_plan_from_actions(
    actions: list[RetrievalAction],
) -> dict[str, Any]:
    initializer_paths = {"focused_index", "structural"}
    has_initializer = any(action.path in initializer_paths for action in actions)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for action in actions:
        key = (action.path, action.target)
        if key in seen:
            continue
        seen.add(key)
        if action.filters.get("_motif_stage"):
            try:
                stage = max(1, int(action.filters.get("_motif_stage") or 1))
            except (TypeError, ValueError):
                stage = 1
            bind_previous = bool(action.filters.get("_bind_previous_scope"))
        else:
            stage = 1 if action.path in initializer_paths or not has_initializer else 2
            bind_previous = stage > 1 and action.path in {
                "focused_index",
                "model_edge",
                "semantic",
                "temporal",
            }
        out.append({
            "path": action.path,
            "target": action.target,
            "budget": int(action.budget),
            "stage": stage,
            "bind_previous_scope": bind_previous,
        })
    out.sort(key=lambda item: (
        int(item["stage"]),
        str(item["path"]),
        str(item["target"]),
    ))
    return {
        "version": 1,
        "execution": "staged",
        "actions": out[:5],
    }


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
