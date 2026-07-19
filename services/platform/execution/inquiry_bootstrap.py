"""Startup state preparation for adaptive inquiry retrieval."""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
    primary_retrieve,
)
from services.reasoning.retrieval.config import CONFIG as RETRIEVAL_CONFIG
from services.reasoning.sage.company_profile import (
    CompanyLearningProfile,
    load_company_learning_profile,
)

from .action_cache import seed_action_cache_from_baseline
from .config import InquiryConfig
from .question_generation import generate_hypotheses, initial_unknowns
from .reflective_rules import (
    ReflectiveRetrievalRule,
    load_reflective_retrieval_rules,
)
from .retrieval_learning import load_question_policy_stats, load_sage_route_utilities
from .retrieval_actions import SemanticRetrievalSession
from .result_composition import _add_result_to_reservoir, _merge_results
from .routing import (
    adaptive_baseline_top_n,
    cold_weak_noop_gate,
    route_for_trigger,
    signal_class_for_trigger,
)
from .runtime_metrics import append_stage_timing
from .sage_reader_execution import _build_sage_reader
from .types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    QuestionAnswer,
    QuestionPolicySignal,
    RetrievalAction,
    SignalRoute,
)


@dataclass(slots=True)
class _InquiryBootstrapState:
    cfg: InquiryConfig
    route: SignalRoute
    session_id: UUID
    candidate_top_n: int
    effective_top_n: int
    signal_class: str
    weak_signal: bool
    cold_weak_noop_gate: dict[str, Any]
    baseline_top_n: int
    stage_timing_notes: list[dict[str, Any]]
    baseline: RetrievalResult
    hypotheses: tuple[Hypothesis, ...]
    evidence_by_key: dict[tuple[str, str], EvidenceCard]
    all_questions: list[InquiryQuestion]
    all_actions: list[RetrievalAction]
    action_cache: dict[tuple[Any, ...], PathwayResult]
    baseline_action_cache_notes: Any
    action_timing_notes: list[dict[str, Any]]
    answers: list[QuestionAnswer]
    retrieval_results: list[RetrievalResult]
    unknowns: set[str]
    question_planning_notes: list[dict[str, Any]]
    reconstruction_notes: list[dict[str, Any]]
    question_policy: dict[str, QuestionPolicySignal]
    reflective_rules: tuple[ReflectiveRetrievalRule, ...]
    sage_reader_notes: dict[str, Any]
    sage_reader_runtime: Any | None
    sage_reader_substrate: Any | None
    max_rounds: int
    semantic_session: SemanticRetrievalSession | None = None
    sage_route_utilities: tuple[Any, ...] = ()
    company_learning_profile: CompanyLearningProfile | None = None


async def _prepare_sage_reader_substrate(
    *,
    sage_reader_runtime: Any | None,
    cfg: InquiryConfig,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    baseline: RetrievalResult,
    candidate_top_n: int,
    stage_timing_notes: list[dict[str, Any]],
    sage_reader_notes: dict[str, Any],
) -> Any | None:
    if (
        sage_reader_runtime is None
        or not cfg.sage_reader_shared_substrate_enabled
        or not hasattr(sage_reader_runtime, "prepare_substrate")
    ):
        return None

    stage_started = time.perf_counter()
    try:
        substrate = await sage_reader_runtime.prepare_substrate(
            conn=conn,
            tenant_id=trigger.tenant_id,
            trigger=trigger,
            baseline_models=tuple(baseline.models[:candidate_top_n]),
        )
        sage_reader_notes["substrate"] = {
            "prepared": True,
            "model_count": int(getattr(substrate, "model_count", 0)),
            "counters": dict(sorted(getattr(substrate, "counters", {}).items())),
            "timings_ms": dict(getattr(substrate, "timings_ms", {}) or {}),
        }
    except Exception as exc:  # noqa: BLE001
        substrate = None
        sage_reader_notes["substrate"] = {
            "prepared": False,
            "error": type(exc).__name__,
        }
    append_stage_timing(
        stage_timing_notes,
        "sage_substrate_prepare",
        stage_started,
        prepared=substrate is not None,
        models=int(getattr(substrate, "model_count", 0))
        if substrate is not None
        else 0,
    )
    return substrate


async def _bootstrap_inquiry_run(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    read_pool: asyncpg.Pool | None,
    route: SignalRoute | None,
    mode: Literal["deep", "fast"],
    top_n: int,
    config: InquiryConfig | None,
) -> _InquiryBootstrapState:
    cfg = config or InquiryConfig.from_env()
    resolved_route = route or (
        "FAST_PATH" if mode == "fast" else route_for_trigger(trigger)
    )
    session_id = uuid7()
    candidate_top_n = min(top_n, max(1, int(cfg.candidate_model_limit)))
    effective_top_n = min(candidate_top_n, max(1, int(cfg.result_model_limit)))
    signal_class = signal_class_for_trigger(trigger)
    weak_signal = signal_class == "weak"
    noop_gate = cold_weak_noop_gate(trigger, signal_class)
    baseline_top_n = adaptive_baseline_top_n(candidate_top_n, signal_class)
    stage_timing_notes: list[dict[str, Any]] = []

    sage_route_utilities: tuple[Any, ...] = ()
    question_policy: dict[str, QuestionPolicySignal] = {}
    company_learning_profile: CompanyLearningProfile | None = None
    if cfg.learned_policy_enabled:
        stage_started = time.perf_counter()
        sage_route_utilities = await load_sage_route_utilities(conn, trigger)
        append_stage_timing(
            stage_timing_notes,
            "sage_route_utility_load",
            stage_started,
            utilities=len(sage_route_utilities),
        )

        stage_started = time.perf_counter()
        question_policy = await load_question_policy_stats(
            conn,
            tenant_id=trigger.tenant_id,
            signal_type=trigger.kind,
        )
        append_stage_timing(
            stage_timing_notes,
            "question_policy_load",
            stage_started,
            policies=len(question_policy),
        )

        stage_started = time.perf_counter()
        company_learning_profile = await load_company_learning_profile(
            conn,
            tenant_id=trigger.tenant_id,
            route_utilities=sage_route_utilities,
            question_policy_stats=question_policy.values(),
        )
        append_stage_timing(
            stage_timing_notes,
            "company_learning_profile_build",
            stage_started,
            priors=len(company_learning_profile.priors),
            samples=company_learning_profile.sample_count,
            confidence=company_learning_profile.confidence,
        )

    stage_started = time.perf_counter()
    if noop_gate["used"]:
        baseline = _merge_results(
            trigger,
            [],
            top_n=0,
            note_prefix="cold_weak_noop",
            config=cfg,
        )
        append_stage_timing(
            stage_timing_notes,
            "primary_retrieve",
            stage_started,
            skipped=True,
            reason=str(noop_gate["reason"]),
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
            config=(
                RETRIEVAL_CONFIG
                if cfg.learned_policy_enabled
                else replace(
                    RETRIEVAL_CONFIG,
                    sage_retrieval_policy_enabled=False,
                )
            ),
            sage_route_utilities=sage_route_utilities,
            company_profile=company_learning_profile,
        )
        append_stage_timing(
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
    hypotheses = tuple(generate_hypotheses(trigger, baseline))
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
    baseline_action_cache_notes = seed_action_cache_from_baseline(
        action_cache,
        baseline,
        trigger,
        cfg,
    )
    action_timing_notes: list[dict[str, Any]] = []
    answers: list[QuestionAnswer] = []
    retrieval_results = [baseline]
    unknowns: set[str] = set(initial_unknowns(trigger, baseline))
    question_planning_notes: list[dict[str, Any]] = []
    reconstruction_notes: list[dict[str, Any]] = []
    append_stage_timing(
        stage_timing_notes,
        "baseline_reservoir_seed",
        stage_started,
        hypotheses=len(hypotheses),
        evidence=len(evidence_by_key),
    )

    stage_started = time.perf_counter()
    reflective_rules = await load_reflective_retrieval_rules(
        conn,
        trigger,
        enabled=cfg.reflective_rules_enabled,
        limit=cfg.reflective_rule_limit,
        match_threshold=cfg.reflective_rule_match_threshold,
    )
    append_stage_timing(
        stage_timing_notes,
        "reflective_rule_load",
        stage_started,
        rules=len(reflective_rules),
        shadow_only=bool(cfg.reflective_rules_shadow_only),
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
        "company_learning_profile": (
            company_learning_profile.to_policy_notes(max_priors=12)
            if company_learning_profile is not None
            else {"enabled": False, "reason": "stage1_company_memory"}
        ),
    }
    sage_reader_runtime: Any | None = None
    max_rounds = (
        0
        if (
            noop_gate["used"]
            or mode == "fast"
            or resolved_route in {"FAST_PATH", "HUMAN_VALIDATION_PATH"}
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

    sage_reader_substrate = await _prepare_sage_reader_substrate(
        sage_reader_runtime=sage_reader_runtime,
        cfg=cfg,
        conn=conn,
        trigger=trigger,
        baseline=baseline,
        candidate_top_n=candidate_top_n,
        stage_timing_notes=stage_timing_notes,
        sage_reader_notes=sage_reader_notes,
    )

    return _InquiryBootstrapState(
        cfg=cfg,
        route=resolved_route,
        session_id=session_id,
        candidate_top_n=candidate_top_n,
        effective_top_n=effective_top_n,
        signal_class=signal_class,
        weak_signal=weak_signal,
        cold_weak_noop_gate=noop_gate,
        baseline_top_n=baseline_top_n,
        stage_timing_notes=stage_timing_notes,
        baseline=baseline,
        hypotheses=hypotheses,
        evidence_by_key=evidence_by_key,
        all_questions=all_questions,
        all_actions=all_actions,
        action_cache=action_cache,
        baseline_action_cache_notes=baseline_action_cache_notes,
        action_timing_notes=action_timing_notes,
        answers=answers,
        retrieval_results=retrieval_results,
        unknowns=unknowns,
        question_planning_notes=question_planning_notes,
        reconstruction_notes=reconstruction_notes,
        question_policy=question_policy,
        reflective_rules=reflective_rules,
        sage_reader_notes=sage_reader_notes,
        sage_reader_runtime=sage_reader_runtime,
        sage_reader_substrate=sage_reader_substrate,
        max_rounds=max_rounds,
        semantic_session=SemanticRetrievalSession(),
        sage_route_utilities=sage_route_utilities,
        company_learning_profile=company_learning_profile,
    )


__all__ = [
    "_InquiryBootstrapState",
    "_bootstrap_inquiry_run",
    "_prepare_sage_reader_substrate",
]
