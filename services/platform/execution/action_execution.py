"""Retrieval action execution and scheduling for inquiry questions."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Literal

import asyncpg

from lib.shared.errors import ValidationError
from services.reasoning.retrieval.pathways import (
    PathwayResult,
    RetrievalPathwayError,
    pathway_a_structural,
    pathway_c_temporal,
    pathway_d_pattern,
    pathway_g_model_edges,
)
from services.reasoning.retrieval.primary import TriggerContext

from .action_cache import (
    action_seed_entities as _action_seed_entities,
    action_seed_model_ids as _action_seed_model_ids,
    bind_action_to_previous_results as _bind_action_to_previous_results,
    retrieval_action_cache_key as _retrieval_action_cache_key,
)
from .config import InquiryConfig
from .retrieval_actions import (
    cap_pathway_models as _cap_pathway_models,
    execute_focused_index_action as _execute_focused_index_action,
    execute_semantic_hybrid_action as _execute_semantic_hybrid_action,
)
from .types import InquiryQuestion, LearnedRetrievalMotif, RetrievalAction


@dataclass(slots=True)
class _QuestionRetrievalPlan:
    question: InquiryQuestion
    sage_result: Any | None = None
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
                timedelta(
                    days=int(
                        action.filters.get("window_days") or cfg.temporal_window_days
                    )
                ),
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
            action_slots.append(
                (
                    plan.question.question_id,
                    action,
                    _retrieval_action_cache_key(action, trigger, cfg),
                )
            )
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
            records_by_qid.setdefault(qid, []).append(
                _ActionExecutionRecord(
                    action=action,
                    path_result=cached,
                    timing_note=_action_timing_note(
                        action, cached, elapsed_ms=0, cache_hit=True
                    ),
                )
            )
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

    task_results = await asyncio.gather(
        *(
            run_one(cache_key, qid, action)
            for cache_key, (qid, action) in first_slot_by_key.items()
        )
    )
    for cache_key, qid, action, path_result, elapsed_ms in task_results:
        if path_result is not None:
            action_cache[cache_key] = path_result
        records_by_qid.setdefault(qid, []).append(
            _ActionExecutionRecord(
                action=action,
                path_result=path_result,
                timing_note=_action_timing_note(
                    action,
                    path_result,
                    elapsed_ms=elapsed_ms,
                    cache_hit=False,
                ),
            )
        )

    for qid, action, cache_key in duplicate_slots:
        path_result = action_cache.get(cache_key)
        records_by_qid.setdefault(qid, []).append(
            _ActionExecutionRecord(
                action=action,
                path_result=path_result,
                timing_note=_action_timing_note(
                    action,
                    path_result,
                    elapsed_ms=0,
                    cache_hit=True,
                ),
            )
        )
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
        records_by_qid.setdefault(qid, []).append(
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
        note.update(
            {
                "models": len(path_result.models),
                "observations": len(path_result.observations),
                "resources": len(path_result.resources),
                "source_pathway": path_result.source_pathway,
            }
        )
    if action.filters.get("_motif_id"):
        note["motif_id"] = action.filters.get("_motif_id")
        note["motif_stage"] = action.filters.get("_motif_stage")
        note["motif_match_score"] = action.filters.get("_motif_match_score")
        note["motif_utility_score"] = action.filters.get("_motif_utility_score")
        if action.filters.get("_bound_scope"):
            note["bound_scope"] = action.filters.get("_bound_scope")
    if action.filters.get("_reflective_rule_ids"):
        note["reflective_rule_ids"] = action.filters.get("_reflective_rule_ids")
        note["reflective_rule_match_score"] = action.filters.get(
            "_reflective_rule_match_score"
        )
    return note


__all__ = [
    "_ActionExecutionRecord",
    "_QuestionRetrievalPlan",
    "_action_timing_note",
    "_execute_action",
    "_execute_question_retrieval_actions",
    "_execute_question_retrieval_actions_serial",
    "_execute_question_retrieval_actions_staged",
]
