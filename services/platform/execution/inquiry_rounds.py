"""Question-round execution for adaptive inquiry retrieval."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Literal

import asyncpg

from lib.llm.provider import LLMProvider
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.sage.retrieval_policy import adapt_inquiry_actions

from .action_execution import (
    ActionExecutionSession,
    _ActionExecutionRecord,
    _QuestionRetrievalPlan,
    _execute_question_retrieval_actions,
)
from .answer_evaluation import (
    answer_question,
    resolved_unknowns_for_answer,
    sufficiency_gate,
)
from .config import InquiryConfig
from .inquiry_bootstrap import _InquiryBootstrapState
from .question_planning import candidate_questions_for_round
from .question_policy import apply_question_policy, select_questions
from .reconstruction_state import (
    apply_reconstruction_to_actions,
    build_reconstruction_state,
    evidence_state_for_reader,
    planner_reconstruction_payload,
    reader_reconstruction_payload,
    reconstruction_gate_decision,
    reconstruction_state_for_purpose,
    reconstruction_state_note,
    serialized_payload_size,
)
from .retrieval_learning import load_retrieval_motifs_for_questions
from .retrieval_plan import compile_retrieval_plan
from .result_composition import _add_result_to_reservoir, _merge_results
from .result_composition import _result_from_pathway
from .runtime_metrics import append_stage_timing
from .sage_reader_execution import _execute_sage_reader_actions_for_round
from .sage_reader_notes import (
    record_sage_reader_notes,
    sage_reader_action_gate,
    sage_reader_total_ms,
)
from .types import (
    InquiryQuestion,
    InquiryStopStatus,
    ReconstructionState,
    RetrievalAction,
)

_BROAD_DISCOVERY_ACTION_PATHS = frozenset({"semantic", "temporal", "pattern"})


@dataclass(frozen=True, slots=True)
class _InquiryRoundStatus:
    stop_status: InquiryStopStatus
    stop_reason: str


def _initial_round_status() -> _InquiryRoundStatus:
    return _InquiryRoundStatus(
        stop_status="insufficient_continue",
        stop_reason="inquiry has not run",
    )


def _question_policy_notes(state: _InquiryBootstrapState) -> dict[str, Any]:
    return {
        primitive: {
            "utility_score": round(signal.utility_score, 4),
            "attempts": signal.attempts,
            "successes": signal.successes,
        }
        for primitive, signal in sorted(state.question_policy.items())
    }


def _strong_context_question_budget(
    state: _InquiryBootstrapState,
    *,
    round_index: int,
) -> tuple[int | None, dict[str, Any]]:
    evidence = list(state.evidence_by_key.values())
    evidence_count = len(evidence)
    model_evidence_count = sum(1 for card in evidence if card.source_type == "model")
    action_anchor_count = sum(
        1
        for card in evidence
        if card.source_type in {"commitment", "goal", "decision", "resource"}
    )
    model_count = len(state.baseline.models)
    cfg = state.cfg
    features = {
        "enabled": bool(cfg.adaptive_question_budget_enabled),
        "round_index": round_index,
        "evidence_count": evidence_count,
        "model_evidence_count": model_evidence_count,
        "baseline_model_count": model_count,
        "action_anchor_count": action_anchor_count,
        "unknown_count": len(state.unknowns),
        "min_evidence": int(cfg.adaptive_strong_context_min_evidence),
        "min_models": int(cfg.adaptive_strong_context_min_models),
        "question_limit": int(cfg.adaptive_strong_context_question_limit),
    }
    if not cfg.adaptive_question_budget_enabled:
        features["applied"] = False
        features["reason"] = "disabled"
        return None, features
    if round_index != 1:
        features["applied"] = False
        features["reason"] = "only_first_round"
        return None, features
    if state.weak_signal:
        features["applied"] = False
        features["reason"] = "weak_signal_uses_existing_budget"
        return None, features
    if evidence_count < cfg.adaptive_strong_context_min_evidence:
        features["applied"] = False
        features["reason"] = "insufficient_evidence"
        return None, features
    if model_count < cfg.adaptive_strong_context_min_models:
        features["applied"] = False
        features["reason"] = "insufficient_baseline_models"
        return None, features
    if model_evidence_count < cfg.adaptive_strong_context_min_models:
        features["applied"] = False
        features["reason"] = "insufficient_model_evidence"
        return None, features
    if (
        action_anchor_count <= 0
        and evidence_count < cfg.adaptive_strong_context_min_evidence * 2
    ):
        features["applied"] = False
        features["reason"] = "no_action_anchor_or_extra_evidence"
        return None, features
    features["applied"] = True
    features["reason"] = "primary_context_already_rich"
    return max(1, int(cfg.adaptive_strong_context_question_limit)), features


async def _select_questions_for_round(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    round_index: int,
    reconstruction_state: ReconstructionState | None,
) -> list[InquiryQuestion]:
    stage_started = time.perf_counter()
    candidate_questions, planning_note = await candidate_questions_for_round(
        trigger,
        state.baseline,
        state.hypotheses,
        state.evidence_by_key,
        state.unknowns,
        llm_provider=llm_provider,
        config=state.cfg,
        round_index=round_index,
        reflective_rules=state.reflective_rules,
        reconstruction_state=reconstruction_state,
    )
    candidate_questions = apply_question_policy(
        candidate_questions,
        question_policy=state.question_policy,
    )
    if state.question_policy:
        planning_note["policy_stats_applied"] = _question_policy_notes(state)
    strong_context_limit, budget_note = _strong_context_question_budget(
        state,
        round_index=round_index,
    )
    planning_note["adaptive_question_budget"] = budget_note
    questions_per_round = (
        min(state.cfg.questions_per_round, strong_context_limit)
        if strong_context_limit is not None
        else state.cfg.questions_per_round
    )
    state.question_planning_notes.append(planning_note)
    selected = select_questions(
        candidate_questions,
        questions_per_round=(
            min(questions_per_round, 2) if state.weak_signal else questions_per_round
        ),
        round_index=round_index,
        already_asked={q.primitive for q in state.all_questions},
    )
    append_stage_timing(
        state.stage_timing_notes,
        "question_planning",
        stage_started,
        round_index=round_index,
        candidates=len(candidate_questions),
        selected=len(selected),
        mode=planning_note.get("mode"),
    )
    return selected


def _skip_note_for_gated_action(
    question: InquiryQuestion,
    action: RetrievalAction,
    skip_reason: str,
) -> dict[str, Any]:
    return {
        "question_id": question.question_id,
        "path": action.path,
        "target": action.target,
        "elapsed_ms": 0,
        "cache_hit": False,
        "returned": False,
        "skipped": True,
        "skip_reason": skip_reason,
    }


def _split_gated_actions(
    question: InquiryQuestion,
    actions: list[RetrievalAction],
    *,
    action_gate_scope: Literal["all", "broad"] | None,
    action_gate_reason: str | None,
) -> tuple[list[RetrievalAction], list[dict[str, Any]]]:
    actions_to_run: list[RetrievalAction] = []
    skipped_timing_notes: list[dict[str, Any]] = []
    for action in actions:
        skip_reason: str | None = None
        if action_gate_scope == "all":
            skip_reason = action_gate_reason or "sage_reader_abstained"
        elif action_gate_scope == "broad" and _is_broad_discovery_action(action):
            skip_reason = action_gate_reason or "sage_reader_focused_route"
        if skip_reason is not None:
            skipped_timing_notes.append(
                _skip_note_for_gated_action(question, action, skip_reason)
            )
            continue
        actions_to_run.append(action)
    return actions_to_run, skipped_timing_notes


def _is_broad_discovery_action(action: RetrievalAction) -> bool:
    if action.path == "temporal":
        return str(action.filters.get("_temporal_lane") or "legacy") != "nearby"
    return action.path in _BROAD_DISCOVERY_ACTION_PATHS


def _build_question_read_plan(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    question: InquiryQuestion,
    sage_result: RetrievalResult | None,
    learned_motif: Any | None,
    reconstruction_state: ReconstructionState | None,
) -> _QuestionRetrievalPlan:
    policy_signal = state.question_policy.get(question.primitive)
    actions = compile_retrieval_plan(
        question,
        trigger,
        state.cfg,
        policy_signal=policy_signal,
        learned_motif=learned_motif,
        reflective_rules=state.reflective_rules,
        apply_reflective_rules=not state.cfg.reflective_rules_shadow_only,
    )
    if state.cfg.sage_retrieval_policy_enabled:
        actions, policy_notes = adapt_inquiry_actions(
            question_primitive=question.primitive,
            actions=actions,
            signal_type=trigger.kind,
            subkind=trigger.subkind,
            route_utilities=state.sage_route_utilities,
            shadow=state.cfg.sage_retrieval_policy_shadow_mode,
            semantic_budget_floor=state.cfg.sage_retrieval_policy_semantic_budget_floor,
        )
        state.sage_reader_notes.setdefault("retrieval_policy_actions", {})[
            question.question_id
        ] = policy_notes
    action_gate_scope: Literal["all", "broad"] | None = None
    action_gate_reason: str | None = None
    if sage_result is not None:
        action_gate_scope, action_gate_reason = sage_reader_action_gate(
            sage_result,
            gate_broad_actions=state.cfg.sage_reader_gate_broad_actions,
        )

    actions_to_run, skipped_timing_notes = _split_gated_actions(
        question,
        actions,
        action_gate_scope=action_gate_scope,
        action_gate_reason=action_gate_reason,
    )
    actions_to_run = apply_reconstruction_to_actions(
        actions_to_run,
        state=reconstruction_state,
    )
    return _QuestionRetrievalPlan(
        question=question,
        sage_result=sage_result,
        action_gate_scope=action_gate_scope,
        action_gate_reason=action_gate_reason,
        actions_to_run=actions_to_run,
        skipped_timing_notes=skipped_timing_notes,
        learned_motif=learned_motif,
    )


def _build_question_read_plans(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    selected: list[InquiryQuestion],
    sage_results_by_qid: dict[str, RetrievalResult],
    learned_motifs: dict[str, Any],
    reconstruction_state: ReconstructionState | None,
) -> list[_QuestionRetrievalPlan]:
    question_read_plans: list[_QuestionRetrievalPlan] = []
    state.all_questions.extend(selected)
    for question in selected:
        question_read_plans.append(
            _build_question_read_plan(
                state,
                trigger=trigger,
                question=question,
                sage_result=sage_results_by_qid.get(question.question_id),
                learned_motif=learned_motifs.get(question.question_id),
                reconstruction_state=reconstruction_state,
            )
        )
    return question_read_plans


def _sage_reader_batch_uses_read_pool(
    state: _InquiryBootstrapState,
    *,
    selected: list[InquiryQuestion],
    read_pool: asyncpg.Pool | None,
) -> bool:
    return (
        bool(state.cfg.sage_reader_enabled)
        and state.sage_reader_runtime is not None
        and bool(state.cfg.sage_reader_parallel_enabled)
        and read_pool is not None
        and len(selected) > 1
        and int(state.cfg.sage_reader_parallelism) > 1
    )


def _single_question_read_prep_uses_read_pool(
    state: _InquiryBootstrapState,
    *,
    selected: list[InquiryQuestion],
    read_pool: asyncpg.Pool | None,
) -> bool:
    return (
        bool(state.cfg.read_prep_parallel_enabled)
        and bool(state.cfg.sage_reader_enabled)
        and state.sage_reader_runtime is not None
        and read_pool is not None
        and len(selected) == 1
    )


def _round_action_pipeline_enabled(
    state: _InquiryBootstrapState,
    *,
    selected: list[InquiryQuestion],
    read_pool: asyncpg.Pool | None,
) -> bool:
    return (
        bool(state.cfg.round_action_pipeline_enabled)
        and bool(state.cfg.read_prep_parallel_enabled)
        and bool(state.cfg.question_action_parallel_enabled)
        and int(state.cfg.question_action_parallelism) > 1
        and _sage_reader_batch_uses_read_pool(
            state,
            selected=selected,
            read_pool=read_pool,
        )
    )


def _append_sage_result(
    state: _InquiryBootstrapState,
    plan: _QuestionRetrievalPlan,
    action_results: list[RetrievalResult],
) -> None:
    if plan.sage_result is None:
        return
    question = plan.question
    state.action_timing_notes.append(
        {
            "question_id": question.question_id,
            "path": "sage_reader",
            "target": "synthesis_reader",
            "elapsed_ms": sage_reader_total_ms(plan.sage_result),
            "cache_hit": False,
            "returned": True,
            "models": len(plan.sage_result.models),
            "observations": len(plan.sage_result.observations),
            "resources": len(plan.sage_result.resources),
            "source_pathway": "SAGE",
        }
    )
    action_results.append(plan.sage_result)
    _add_result_to_reservoir(
        state.evidence_by_key,
        plan.sage_result,
        path="sage_reader",
        question_id=question.question_id,
        hypotheses=state.hypotheses,
        score_hint=max(0.0, question.score),
    )
    record_sage_reader_notes(state.sage_reader_notes, question, plan.sage_result)


def _append_action_record_results(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    question: InquiryQuestion,
    records: list[_ActionExecutionRecord],
    action_results: list[RetrievalResult],
) -> None:
    for record in records:
        state.action_timing_notes.append(record.timing_note)
        if record.path_result is None:
            continue
        result = _result_from_pathway(trigger, record.path_result, record.action)
        action_results.append(result)
        _add_result_to_reservoir(
            state.evidence_by_key,
            result,
            path=record.action.path,
            question_id=question.question_id,
            hypotheses=state.hypotheses,
            score_hint=max(0.0, question.score),
        )


def _merge_question_results(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    question: InquiryQuestion,
    action_results: list[RetrievalResult],
) -> None:
    if not action_results:
        return
    stage_started = time.perf_counter()
    merged_for_question = _merge_results(
        trigger,
        action_results,
        top_n=state.candidate_top_n,
        note_prefix=f"question_{question.question_id}",
    )
    append_stage_timing(
        state.stage_timing_notes,
        "question_result_merge",
        stage_started,
        question_id=question.question_id,
        models=len(merged_for_question.models),
        observations=len(merged_for_question.observations),
    )
    state.retrieval_results.append(merged_for_question)


def _answer_question_and_update_status(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    question: InquiryQuestion,
    round_index: int,
) -> _InquiryRoundStatus:
    stage_started = time.perf_counter()
    answer = answer_question(
        question,
        state.evidence_by_key,
        trigger_occurred_at=trigger.seed_occurred_at,
        stale_after_days=state.cfg.temporal_window_days,
    )
    append_stage_timing(
        state.stage_timing_notes,
        "question_answer",
        stage_started,
        question_id=question.question_id,
        evidence=len(state.evidence_by_key),
    )
    state.answers.append(answer)
    state.unknowns.difference_update(resolved_unknowns_for_answer(question, answer))
    state.unknowns.update(answer.new_uncertainties)
    verdict = sufficiency_gate(
        state.route,
        state.hypotheses,
        list(state.evidence_by_key.values()),
        state.answers,
        round_index=round_index,
        max_rounds=state.max_rounds,
        unknowns=state.unknowns,
    )
    return _InquiryRoundStatus(verdict.status, verdict.reason)


def _apply_question_read_plan_results(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    plan: _QuestionRetrievalPlan,
    action_records_by_qid: dict[str, list[_ActionExecutionRecord]],
    round_index: int,
) -> _InquiryRoundStatus:
    question = plan.question
    action_results: list[RetrievalResult] = []
    _append_sage_result(state, plan, action_results)
    state.action_timing_notes.extend(plan.skipped_timing_notes)
    state.all_actions.extend(plan.actions_to_run)
    _append_action_record_results(
        state,
        trigger=trigger,
        question=question,
        records=action_records_by_qid.get(question.question_id, []),
        action_results=action_results,
    )
    _merge_question_results(
        state,
        trigger=trigger,
        question=question,
        action_results=action_results,
    )
    return _answer_question_and_update_status(
        state,
        trigger=trigger,
        question=question,
        round_index=round_index,
    )


async def _load_retrieval_motifs_with_read_pool(
    conn: asyncpg.Connection,
    read_pool: asyncpg.Pool | None,
    trigger: TriggerContext,
    selected: list[InquiryQuestion],
    cfg: InquiryConfig,
) -> dict[str, Any]:
    if read_pool is None:
        return await load_retrieval_motifs_for_questions(
            conn,
            trigger,
            selected,
            cfg,
        )
    async with read_pool.acquire() as motif_conn:
        return await load_retrieval_motifs_for_questions(
            motif_conn,
            trigger,
            selected,
            cfg,
        )


async def _execute_question_read_pipeline(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    read_pool: asyncpg.Pool,
    selected: list[InquiryQuestion],
    sage_kwargs: dict[str, Any],
    reconstruction_state: ReconstructionState | None,
    round_index: int,
) -> tuple[
    list[_QuestionRetrievalPlan],
    dict[str, list[_ActionExecutionRecord]],
    dict[str, Any],
]:
    prep_started = time.perf_counter()
    motifs_task = asyncio.create_task(
        _load_retrieval_motifs_with_read_pool(
            conn,
            None,
            trigger,
            selected,
            state.cfg,
        )
    )
    action_session = ActionExecutionSession(state.cfg.question_action_parallelism)
    sage_results_by_qid: dict[str, RetrievalResult] = {}
    plans_by_qid: dict[str, _QuestionRetrievalPlan] = {}
    action_records_by_qid: dict[str, list[_ActionExecutionRecord]] = {}
    action_tasks_by_qid: dict[str, asyncio.Task[None]] = {}

    async def run_actions_for_question(
        question: InquiryQuestion,
        sage_result: RetrievalResult | None,
    ) -> None:
        learned_motifs = await motifs_task
        plan = _build_question_read_plan(
            state,
            trigger=trigger,
            question=question,
            sage_result=sage_result,
            learned_motif=learned_motifs.get(question.question_id),
            reconstruction_state=reconstruction_state,
        )
        plans_by_qid[question.question_id] = plan
        records = await _execute_question_retrieval_actions(
            [plan],
            trigger,
            conn,
            embedder,
            state.cfg,
            state.action_cache,
            read_pool=read_pool,
            semantic_session=state.semantic_session,
            execution_session=action_session,
        )
        action_records_by_qid.update(records)

    def schedule_question_actions(
        question: InquiryQuestion,
        sage_result: RetrievalResult | None,
    ) -> None:
        if sage_result is not None:
            sage_results_by_qid[question.question_id] = sage_result
        if question.question_id in action_tasks_by_qid:
            return
        action_tasks_by_qid[question.question_id] = asyncio.create_task(
            run_actions_for_question(question, sage_result)
        )

    async def on_question_result(
        question: InquiryQuestion,
        sage_result: RetrievalResult | None,
    ) -> None:
        schedule_question_actions(question, sage_result)

    try:
        sage_results, sage_batch_note = await _execute_sage_reader_actions_for_round(
            selected,
            trigger,
            conn,
            state.cfg,
            **sage_kwargs,
            on_question_result=on_question_result,
        )
        sage_results_by_qid.update(sage_results)
        for question in selected:
            schedule_question_actions(
                question,
                sage_results_by_qid.get(question.question_id),
            )
        learned_motifs = await motifs_task
        if action_tasks_by_qid:
            await asyncio.gather(
                *(action_tasks_by_qid[question.question_id] for question in selected)
            )
    except BaseException:
        motifs_task.cancel()
        for task in action_tasks_by_qid.values():
            task.cancel()
        raise

    state.all_questions.extend(selected)
    question_read_plans = [plans_by_qid[question.question_id] for question in selected]
    append_stage_timing(
        state.stage_timing_notes,
        "round_read_action_pipeline",
        prep_started,
        round_index=round_index,
        selected=len(selected),
        sage_returned=len(sage_results_by_qid),
        motifs=len(learned_motifs),
        action_plans=len(question_read_plans),
        action_records=sum(len(records) for records in action_records_by_qid.values()),
    )
    return (
        question_read_plans,
        action_records_by_qid,
        {**sage_batch_note, "pipelined_actions": True},
    )


async def _execute_inquiry_round(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    llm_provider: LLMProvider | None,
    read_pool: asyncpg.Pool | None,
    round_index: int,
) -> _InquiryRoundStatus:
    stage_started = time.perf_counter()
    reconstruction_state = build_reconstruction_state(
        trigger=trigger,
        hypotheses=state.hypotheses,
        evidence=list(state.evidence_by_key.values()),
        answers=state.answers,
        unknowns=state.unknowns,
        round_index=round_index,
    )
    planner_reconstruction_state = reconstruction_state_for_purpose(
        reconstruction_state,
        trigger=trigger,
        purpose="planner",
    )
    reader_reconstruction_state = reconstruction_state_for_purpose(
        reconstruction_state,
        trigger=trigger,
        purpose="reader",
    )
    action_reconstruction_state = reconstruction_state_for_purpose(
        reconstruction_state,
        trigger=trigger,
        purpose="actions",
    )
    gate_decision = reconstruction_gate_decision(reconstruction_state, trigger=trigger)
    state.reconstruction_notes.append(
        reconstruction_state_note(reconstruction_state, trigger=trigger)
    )
    append_stage_timing(
        state.stage_timing_notes,
        "reconstruction_state",
        stage_started,
        round_index=round_index,
        active_cues=len(reconstruction_state.active_cues),
        active_tags=len(reconstruction_state.active_tags),
        unresolved_slots=len(reconstruction_state.unresolved_slots),
        known_models=len(reconstruction_state.known_model_ids),
        known_observations=len(reconstruction_state.known_observation_ids),
        planner_enabled=bool(planner_reconstruction_state is not None),
        reader_enabled=bool(reader_reconstruction_state is not None),
        actions_enabled=bool(action_reconstruction_state is not None),
        planner_payload_chars=serialized_payload_size(
            planner_reconstruction_payload(planner_reconstruction_state)
        ),
        reader_payload_chars=serialized_payload_size(
            reader_reconstruction_payload(reader_reconstruction_state)
        ),
        action_cues=int(gate_decision.get("actions", {}).get("cue_count") or 0),
        gate_reasons={
            purpose: data.get("reason")
            for purpose, data in gate_decision.items()
            if isinstance(data, dict) and "reason" in data
        },
    )
    selected = await _select_questions_for_round(
        state,
        trigger=trigger,
        llm_provider=llm_provider,
        round_index=round_index,
        reconstruction_state=planner_reconstruction_state,
    )
    if not selected:
        return _InquiryRoundStatus(
            "insufficient_defer",
            "no high-value unanswered questions remained",
        )

    sage_kwargs = {
        "reader": state.sage_reader_runtime,
        "substrate": state.sage_reader_substrate,
        "hypotheses": state.hypotheses,
        "read_pool": read_pool,
        "evidence_state": evidence_state_for_reader(reader_reconstruction_state),
    }
    pipelined_actions = False
    if _round_action_pipeline_enabled(
        state,
        selected=selected,
        read_pool=read_pool,
    ):
        assert read_pool is not None
        (
            question_read_plans,
            action_records_by_qid,
            sage_batch_note,
        ) = await _execute_question_read_pipeline(
            state,
            trigger=trigger,
            conn=conn,
            embedder=embedder,
            read_pool=read_pool,
            selected=selected,
            sage_kwargs=sage_kwargs,
            reconstruction_state=action_reconstruction_state,
            round_index=round_index,
        )
        pipelined_actions = True
    elif state.cfg.read_prep_parallel_enabled and _sage_reader_batch_uses_read_pool(
        state,
        selected=selected,
        read_pool=read_pool,
    ):
        prep_started = time.perf_counter()
        async with asyncio.TaskGroup() as task_group:
            sage_task = task_group.create_task(
                _execute_sage_reader_actions_for_round(
                    selected,
                    trigger,
                    conn,
                    state.cfg,
                    **sage_kwargs,
                )
            )
            motifs_task = task_group.create_task(
                _load_retrieval_motifs_with_read_pool(
                    conn,
                    None,
                    trigger,
                    selected,
                    state.cfg,
                )
            )
        sage_results_by_qid, sage_batch_note = sage_task.result()
        learned_motifs = motifs_task.result()
        append_stage_timing(
            state.stage_timing_notes,
            "round_read_prep_parallel",
            prep_started,
            round_index=round_index,
            selected=len(selected),
            sage_returned=len(sage_results_by_qid),
            motifs=len(learned_motifs),
        )
    elif _single_question_read_prep_uses_read_pool(
        state,
        selected=selected,
        read_pool=read_pool,
    ):
        prep_started = time.perf_counter()
        async with asyncio.TaskGroup() as task_group:
            sage_task = task_group.create_task(
                _execute_sage_reader_actions_for_round(
                    selected,
                    trigger,
                    conn,
                    state.cfg,
                    **sage_kwargs,
                )
            )
            motifs_task = task_group.create_task(
                _load_retrieval_motifs_with_read_pool(
                    conn,
                    read_pool,
                    trigger,
                    selected,
                    state.cfg,
                )
            )
        sage_results_by_qid, sage_batch_note = sage_task.result()
        learned_motifs = motifs_task.result()
        append_stage_timing(
            state.stage_timing_notes,
            "round_read_prep_parallel",
            prep_started,
            round_index=round_index,
            selected=len(selected),
            sage_returned=len(sage_results_by_qid),
            motifs=len(learned_motifs),
            motif_read_pool=True,
        )
    else:
        (
            sage_results_by_qid,
            sage_batch_note,
        ) = await _execute_sage_reader_actions_for_round(
            selected,
            trigger,
            conn,
            state.cfg,
            **sage_kwargs,
        )
        learned_motifs = await load_retrieval_motifs_for_questions(
            conn,
            trigger,
            selected,
            state.cfg,
        )
    if not pipelined_actions:
        question_read_plans = _build_question_read_plans(
            state,
            trigger=trigger,
            selected=selected,
            sage_results_by_qid=sage_results_by_qid,
            learned_motifs=learned_motifs,
            reconstruction_state=action_reconstruction_state,
        )
        action_records_by_qid = await _execute_question_retrieval_actions(
            question_read_plans,
            trigger,
            conn,
            embedder,
            state.cfg,
            state.action_cache,
            read_pool=read_pool,
            semantic_session=state.semantic_session,
        )
    state.sage_reader_notes.setdefault("batches", []).append(
        {
            **sage_batch_note,
            "round_index": round_index,
        }
    )
    for plan in question_read_plans:
        status = _apply_question_read_plan_results(
            state,
            trigger=trigger,
            plan=plan,
            action_records_by_qid=action_records_by_qid,
            round_index=round_index,
        )
        if status.stop_status == "sufficient_for_reasoning":
            return status

    verdict = sufficiency_gate(
        state.route,
        state.hypotheses,
        list(state.evidence_by_key.values()),
        state.answers,
        round_index=round_index,
        max_rounds=state.max_rounds,
        unknowns=state.unknowns,
    )
    return _InquiryRoundStatus(verdict.status, verdict.reason)


async def _execute_inquiry_rounds(
    state: _InquiryBootstrapState,
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    llm_provider: LLMProvider | None,
    read_pool: asyncpg.Pool | None,
) -> _InquiryRoundStatus:
    status = _initial_round_status()
    for round_index in range(1, state.max_rounds + 1):
        status = await _execute_inquiry_round(
            state,
            trigger=trigger,
            conn=conn,
            embedder=embedder,
            llm_provider=llm_provider,
            read_pool=read_pool,
            round_index=round_index,
        )
        if status.stop_status != "insufficient_continue":
            break
    return status


__all__ = [
    "_BROAD_DISCOVERY_ACTION_PATHS",
    "_InquiryRoundStatus",
    "_build_question_read_plans",
    "_execute_inquiry_round",
    "_execute_inquiry_rounds",
    "_is_broad_discovery_action",
]
