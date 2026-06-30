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
from services.reasoning.retrieval.read_fanout import ReadFanoutBudget

from .action_cache import (
    action_seed_entities as _action_seed_entities,
    action_seed_model_ids as _action_seed_model_ids,
    bind_action_to_previous_results as _bind_action_to_previous_results,
    retrieval_action_cache_key as _retrieval_action_cache_key,
)
from .config import InquiryConfig
from .retrieval_admission import decide_action_admission
from .retrieval_actions import (
    SemanticRetrievalSession,
    cap_pathway_models as _cap_pathway_models,
    execute_focused_index_action as _execute_focused_index_action,
    execute_semantic_hybrid_action as _execute_semantic_hybrid_action,
    execute_semantic_terms_action as _execute_semantic_terms_action,
)
from .types import InquiryQuestion, LearnedRetrievalMotif, RetrievalAction

_ActionTimingKind = Literal["owner_work", "cache_hit", "in_flight_wait"]


@dataclass(slots=True)
class _QuestionRetrievalPlan:
    question: InquiryQuestion
    sage_result: Any | None = None
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
class _ActionRunResult:
    path_result: PathwayResult | None
    elapsed_ms: int
    cache_hit: bool
    timing_kind: _ActionTimingKind


@dataclass(slots=True)
class ActionExecutionSession:
    """Round-scoped action fanout and in-flight cache coordination."""

    parallelism: int
    read_fanout_budget: ReadFanoutBudget | None = None
    semaphore: asyncio.Semaphore = field(init=False)
    cache_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    in_flight: dict[tuple[Any, ...], asyncio.Task[PathwayResult | None]] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(max(1, int(self.parallelism)))

    def ensure_read_budget(self, read_pool: asyncpg.Pool) -> ReadFanoutBudget:
        if self.read_fanout_budget is None:
            self.read_fanout_budget = ReadFanoutBudget.from_pool(read_pool)
        return self.read_fanout_budget


async def _execute_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    read_pool: asyncpg.Pool | None = None,
    semantic_session: SemanticRetrievalSession | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
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
                read_fanout_budget=read_fanout_budget,
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
                read_pool=read_pool,
                read_fanout_budget=read_fanout_budget,
            )
        if action.path == "semantic_terms":
            return await _execute_semantic_terms_action(
                action,
                trigger,
                conn,
                cfg,
                model_limit=capped_budget(action.budget),
                read_pool=read_pool,
                read_fanout_budget=read_fanout_budget,
            )
        if action.path == "semantic":
            return await _execute_semantic_hybrid_action(
                action,
                trigger,
                conn,
                embedder,
                cfg,
                model_limit=capped_budget(action.budget),
                semantic_session=semantic_session,
                read_pool=read_pool,
                read_fanout_budget=read_fanout_budget,
            )
        if action.path == "temporal":
            if trigger.seed_occurred_at is None:
                return None
            window_days = int(
                action.filters.get("window_days") or cfg.temporal_window_days
            )
            result = await pathway_c_temporal(
                trigger.seed_occurred_at,
                timedelta(days=window_days),
                trigger.tenant_id,
                conn,
                scope_actors=trigger.scope_actors,
                scope_entities=_action_seed_entities(action, trigger),
                max_observations=capped_observation_budget(action.budget),
                max_models=capped_budget(action.budget),
                include_entity_mentions=bool(
                    action.filters.get("_temporal_include_entity_mentions", True)
                ),
                scope_filter_strategy=str(
                    action.filters.get("_temporal_scope_filter_strategy")
                    or "indexed_or"
                ),
            )
            result.notes["temporal_action"] = {
                "lane": str(action.filters.get("_temporal_lane") or "legacy"),
                "window_days": window_days,
                "broad_fallback": bool(
                    action.filters.get("_temporal_broad_fallback_after_nearby")
                ),
                "scope_filter_strategy": result.notes.get(
                    "temporal_scope_filter_strategy"
                ),
                "include_entity_mentions": result.notes.get("include_entity_mentions"),
            }
            return result
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


async def _execute_action_with_optional_semantic_session(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    read_pool: asyncpg.Pool | None = None,
    semantic_session: SemanticRetrievalSession | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> PathwayResult | None:
    kwargs: dict[str, Any] = {}
    if read_pool is not None:
        kwargs["read_pool"] = read_pool
    if semantic_session is not None:
        kwargs["semantic_session"] = semantic_session
    if read_fanout_budget is not None:
        kwargs["read_fanout_budget"] = read_fanout_budget
    return await _execute_action(
        action,
        trigger,
        conn,
        embedder,
        cfg,
        **kwargs,
    )


async def _run_uncached_parallel_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    read_pool: asyncpg.Pool,
    semantic_session: SemanticRetrievalSession | None,
    execution_session: ActionExecutionSession,
) -> PathwayResult | None:
    async with execution_session.semaphore:
        read_fanout_budget = execution_session.ensure_read_budget(read_pool)
        if action.path in {"semantic", "semantic_terms"}:
            return await _execute_action_with_optional_semantic_session(
                action,
                trigger,
                conn,
                embedder,
                cfg,
                read_pool=read_pool,
                semantic_session=semantic_session,
                read_fanout_budget=read_fanout_budget,
            )
        async with read_fanout_budget.connection() as action_conn:
            return await _execute_action_with_optional_semantic_session(
                action,
                trigger,
                action_conn,
                embedder,
                cfg,
                read_pool=read_pool,
                semantic_session=semantic_session,
                read_fanout_budget=read_fanout_budget,
            )


async def _run_cached_parallel_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool,
    semantic_session: SemanticRetrievalSession | None,
    execution_session: ActionExecutionSession,
) -> _ActionRunResult:
    cache_key = _retrieval_action_cache_key(action, trigger, cfg)
    async with execution_session.cache_lock:
        cached = action_cache.get(cache_key)
        if cached is not None:
            return _ActionRunResult(cached, 0, True, "cache_hit")
        task = execution_session.in_flight.get(cache_key)
        owner = task is None
        if task is None:
            task = asyncio.create_task(
                _run_uncached_parallel_action(
                    action,
                    trigger,
                    conn,
                    embedder,
                    cfg,
                    read_pool=read_pool,
                    semantic_session=semantic_session,
                    execution_session=execution_session,
                )
            )
            execution_session.in_flight[cache_key] = task

    started = time.perf_counter()
    try:
        path_result = await task
    except BaseException:
        if owner:
            async with execution_session.cache_lock:
                execution_session.in_flight.pop(cache_key, None)
        raise
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if owner:
        async with execution_session.cache_lock:
            if path_result is not None:
                action_cache[cache_key] = path_result
            execution_session.in_flight.pop(cache_key, None)
        return _ActionRunResult(path_result, elapsed_ms, False, "owner_work")
    return _ActionRunResult(path_result, elapsed_ms, True, "in_flight_wait")


async def _execute_question_retrieval_actions(
    plans: list[_QuestionRetrievalPlan],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool | None,
    semantic_session: SemanticRetrievalSession | None = None,
    execution_session: ActionExecutionSession | None = None,
) -> dict[str, list[_ActionExecutionRecord]]:
    if not plans:
        return {}
    if any(
        _action_stage(action) is not None
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
            semantic_session=semantic_session,
            execution_session=execution_session,
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
            semantic_session=semantic_session,
        )

    if execution_session is not None:
        records_by_qid: dict[str, list[_ActionExecutionRecord]] = {
            plan.question.question_id: [] for plan in plans
        }
        slot_results = await asyncio.gather(
            *(
                _run_cached_parallel_action(
                    action,
                    trigger,
                    conn,
                    embedder,
                    cfg,
                    action_cache,
                    read_pool=read_pool,
                    semantic_session=semantic_session,
                    execution_session=execution_session,
                )
                for _qid, action, _cache_key in action_slots
            )
        )
        for (qid, action, _cache_key), action_result in zip(
            action_slots,
            slot_results,
            strict=True,
        ):
            records_by_qid.setdefault(qid, []).append(
                _ActionExecutionRecord(
                    action=action,
                    path_result=action_result.path_result,
                    timing_note=_action_timing_note(
                        action,
                        action_result.path_result,
                        elapsed_ms=action_result.elapsed_ms,
                        cache_hit=action_result.cache_hit,
                        timing_kind=action_result.timing_kind,
                    ),
                )
            )
        return records_by_qid

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
                        action,
                        cached,
                        elapsed_ms=0,
                        cache_hit=True,
                        timing_kind="cache_hit",
                    ),
                )
            )
            continue
        if cache_key in first_slot_by_key:
            duplicate_slots.append((qid, action, cache_key))
            continue
        first_slot_by_key[cache_key] = (qid, action)

    local_session = ActionExecutionSession(
        max(1, int(cfg.question_action_parallelism)),
        read_fanout_budget=ReadFanoutBudget.from_pool(read_pool),
    )

    async def run_one(cache_key: tuple[Any, ...], qid: str, action: RetrievalAction):
        async with local_session.semaphore:
            started = time.perf_counter()
            if action.path in {"semantic", "semantic_terms"}:
                path_result = await _execute_action_with_optional_semantic_session(
                    action,
                    trigger,
                    conn,
                    embedder,
                    cfg,
                    read_pool=read_pool,
                    semantic_session=semantic_session,
                    read_fanout_budget=local_session.read_fanout_budget,
                )
            else:
                assert local_session.read_fanout_budget is not None
                async with local_session.read_fanout_budget.connection() as action_conn:
                    path_result = await _execute_action_with_optional_semantic_session(
                        action,
                        trigger,
                        action_conn,
                        embedder,
                        cfg,
                        read_pool=read_pool,
                        semantic_session=semantic_session,
                        read_fanout_budget=local_session.read_fanout_budget,
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
                    timing_kind="owner_work",
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
                    timing_kind="cache_hit",
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
    semantic_session: SemanticRetrievalSession | None = None,
    execution_session: ActionExecutionSession | None = None,
) -> dict[str, list[_ActionExecutionRecord]]:
    if (
        not cfg.question_action_parallel_enabled
        or read_pool is None
        or int(cfg.question_action_parallelism) <= 1
    ):
        return await _execute_question_retrieval_actions_staged_serial(
            plans,
            trigger,
            conn,
            embedder,
            cfg,
            action_cache,
            read_pool=read_pool,
            semantic_session=semantic_session,
        )

    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {
        plan.question.question_id: [] for plan in plans
    }
    local_session = execution_session or ActionExecutionSession(
        max(1, int(cfg.question_action_parallelism))
    )

    async def run_uncached_action(action: RetrievalAction) -> PathwayResult | None:
        return await _run_uncached_parallel_action(
            action,
            trigger,
            conn,
            embedder,
            cfg,
            read_pool=read_pool,
            semantic_session=semantic_session,
            execution_session=local_session,
        )

    async def run_cached_action(
        action: RetrievalAction,
    ) -> _ActionRunResult:
        cache_key = _retrieval_action_cache_key(action, trigger, cfg)
        async with local_session.cache_lock:
            cached = action_cache.get(cache_key)
            if cached is not None:
                return _ActionRunResult(cached, 0, True, "cache_hit")
            task = local_session.in_flight.get(cache_key)
            owner = task is None
            if task is None:
                task = asyncio.create_task(run_uncached_action(action))
                local_session.in_flight[cache_key] = task

        started = time.perf_counter()
        try:
            path_result = await task
        except BaseException:
            if owner:
                async with local_session.cache_lock:
                    local_session.in_flight.pop(cache_key, None)
            raise
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if owner:
            async with local_session.cache_lock:
                if path_result is not None:
                    action_cache[cache_key] = path_result
                local_session.in_flight.pop(cache_key, None)
            return _ActionRunResult(path_result, elapsed_ms, False, "owner_work")
        return _ActionRunResult(path_result, elapsed_ms, True, "in_flight_wait")

    async def run_plan(
        plan: _QuestionRetrievalPlan,
    ) -> tuple[str, list[_ActionExecutionRecord]]:
        records: list[_ActionExecutionRecord] = []
        prior_results: list[PathwayResult] = []
        actions_by_stage: dict[int, list[tuple[int, RetrievalAction]]] = {}
        for index, action in enumerate(plan.actions_to_run):
            try:
                stage = max(1, int(_action_stage(action) or 1))
            except (TypeError, ValueError):
                stage = 1
            actions_by_stage.setdefault(stage, []).append((index, action))

        for stage in sorted(actions_by_stage):
            bound_actions = [
                (
                    index,
                    _bind_action_to_previous_results(
                        raw_action,
                        trigger,
                        prior_results,
                    ),
                )
                for index, raw_action in actions_by_stage[stage]
            ]
            runnable_actions: list[tuple[int, RetrievalAction]] = []
            for index, action in bound_actions:
                admission = decide_action_admission(action, prior_results)
                if not admission.admitted:
                    records.append(
                        _ActionExecutionRecord(
                            action=action,
                            path_result=None,
                            timing_note=_skipped_action_timing_note(
                                action,
                                skip_reason=admission.reason,
                                admission_coverage=admission.coverage.notes(),
                            ),
                        )
                    )
                    continue
                runnable_actions.append((index, action))
            if not runnable_actions:
                continue
            stage_results = await asyncio.gather(
                *(run_cached_action(action) for _index, action in runnable_actions)
            )
            for (_index, action), action_result in zip(
                runnable_actions,
                stage_results,
                strict=True,
            ):
                if action_result.path_result is not None:
                    prior_results.append(action_result.path_result)
                records.append(
                    _ActionExecutionRecord(
                        action=action,
                        path_result=action_result.path_result,
                        timing_note=_action_timing_note(
                            action,
                            action_result.path_result,
                            elapsed_ms=action_result.elapsed_ms,
                            cache_hit=action_result.cache_hit,
                            timing_kind=action_result.timing_kind,
                        ),
                    )
                )
        return plan.question.question_id, records

    for qid, records in await asyncio.gather(*(run_plan(plan) for plan in plans)):
        records_by_qid[qid] = records
    return records_by_qid


async def _execute_question_retrieval_actions_staged_serial(
    plans: list[_QuestionRetrievalPlan],
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    action_cache: dict[tuple[Any, ...], PathwayResult],
    *,
    read_pool: asyncpg.Pool | None,
    semantic_session: SemanticRetrievalSession | None = None,
) -> dict[str, list[_ActionExecutionRecord]]:
    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {
        plan.question.question_id: [] for plan in plans
    }
    for plan in plans:
        prior_results: list[PathwayResult] = []
        actions_by_stage: dict[int, list[RetrievalAction]] = {}
        for action in plan.actions_to_run:
            try:
                stage = max(1, int(_action_stage(action) or 1))
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
                admission = decide_action_admission(action, prior_results)
                if not admission.admitted:
                    records_by_qid.setdefault(plan.question.question_id, []).append(
                        _ActionExecutionRecord(
                            action=action,
                            path_result=None,
                            timing_note=_skipped_action_timing_note(
                                action,
                                skip_reason=admission.reason,
                                admission_coverage=admission.coverage.notes(),
                            ),
                        )
                    )
                    continue
                cache_key = _retrieval_action_cache_key(action, trigger, cfg)
                path_result = action_cache.get(cache_key)
                cache_hit = path_result is not None
                started = time.perf_counter()
                if path_result is None:
                    path_result = await _execute_action_with_optional_semantic_session(
                        action,
                        trigger,
                        conn,
                        embedder,
                        cfg,
                        read_pool=read_pool,
                        semantic_session=semantic_session,
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
                            timing_kind="cache_hit" if cache_hit else "owner_work",
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
    semantic_session: SemanticRetrievalSession | None = None,
) -> dict[str, list[_ActionExecutionRecord]]:
    records_by_qid: dict[str, list[_ActionExecutionRecord]] = {}
    for qid, action, cache_key in action_slots:
        path_result = action_cache.get(cache_key)
        cache_hit = path_result is not None
        started = time.perf_counter()
        if path_result is None:
            path_result = await _execute_action_with_optional_semantic_session(
                action,
                trigger,
                conn,
                embedder,
                cfg,
                read_pool=read_pool,
                semantic_session=semantic_session,
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
                    timing_kind="cache_hit" if cache_hit else "owner_work",
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
    timing_kind: _ActionTimingKind | None = None,
) -> dict[str, Any]:
    elapsed = int(elapsed_ms)
    if timing_kind is not None:
        resolved_timing_kind = timing_kind
    elif cache_hit:
        resolved_timing_kind = "cache_hit"
    else:
        resolved_timing_kind = "owner_work"
    note: dict[str, Any] = {
        "question_id": action.question_id,
        "path": action.path,
        "target": action.target,
        "elapsed_ms": elapsed,
        "cache_hit": bool(cache_hit),
        "timing_kind": resolved_timing_kind,
        "in_flight_wait": resolved_timing_kind == "in_flight_wait",
        "work_elapsed_ms": elapsed if resolved_timing_kind == "owner_work" else 0,
        "wait_elapsed_ms": elapsed if resolved_timing_kind == "in_flight_wait" else 0,
        "returned": path_result is not None,
    }
    if path_result is not None:
        result_notes = dict(path_result.notes or {})
        source_set: set[str] = set()
        for hit in result_notes.get("top_hits") or []:
            if not isinstance(hit, dict):
                continue
            raw_sources = hit.get("sources")
            if isinstance(raw_sources, list):
                source_set.update(str(source) for source in raw_sources)
        note.update(
            {
                "models": len(path_result.models),
                "observations": len(path_result.observations),
                "resources": len(path_result.resources),
                "source_pathway": path_result.source_pathway,
            }
        )
        if source_set:
            note["source_set"] = sorted(source_set)
            note["source_count"] = len(source_set)
        scan_timeouts = result_notes.get("scan_timeouts")
        if isinstance(scan_timeouts, dict):
            note["scan_timeouts"] = {
                str(key): bool(value) for key, value in scan_timeouts.items()
            }
            note["bounded_lookup_timeout_count"] = sum(
                1 for value in note["scan_timeouts"].values() if value
            )
        semantic_timings = result_notes.get("semantic_substrate_timings_ms")
        if isinstance(semantic_timings, dict):
            note["semantic_substrate_timings_ms"] = {
                str(key): int(value)
                for key, value in semantic_timings.items()
                if isinstance(value, (int, float))
            }
        semantic_terms_action = result_notes.get("semantic_terms_action")
        if isinstance(semantic_terms_action, dict):
            note["semantic_terms_action"] = True
            if "models_returned" in result_notes:
                note["semantic_terms_models_returned"] = result_notes.get(
                    "models_returned"
                )
        temporal_timings = result_notes.get("temporal_timings_ms")
        if isinstance(temporal_timings, dict):
            note["temporal_timings_ms"] = {
                str(key): int(value)
                for key, value in temporal_timings.items()
                if isinstance(value, (int, float))
            }
        temporal_action = result_notes.get("temporal_action")
        if isinstance(temporal_action, dict):
            note["temporal_lane"] = temporal_action.get("lane")
            note["temporal_window_days"] = temporal_action.get("window_days")
            note["temporal_broad_fallback"] = bool(
                temporal_action.get("broad_fallback")
            )
            note["temporal_scope_filter_strategy"] = temporal_action.get(
                "scope_filter_strategy"
            )
            note["temporal_include_entity_mentions"] = temporal_action.get(
                "include_entity_mentions"
            )
    if action.filters.get("_motif_id"):
        note["motif_id"] = action.filters.get("_motif_id")
        note["motif_stage"] = action.filters.get("_motif_stage")
        note["motif_match_score"] = action.filters.get("_motif_match_score")
        note["motif_utility_score"] = action.filters.get("_motif_utility_score")
        if action.filters.get("_bound_scope"):
            note["bound_scope"] = action.filters.get("_bound_scope")
    if action.filters.get("_reconstruction_stage"):
        note["reconstruction_stage"] = action.filters.get("_reconstruction_stage")
        note["reconstruction_round"] = action.filters.get("_reconstruction_round")
        note["reconstruction_cue_count"] = action.filters.get(
            "_reconstruction_cue_count"
        )
        note["reconstruction_active_cues"] = action.filters.get(
            "_reconstruction_active_cues"
        )
        if action.filters.get("_bound_scope"):
            note["bound_scope"] = action.filters.get("_bound_scope")
    if action.filters.get("_sage_policy_stage"):
        note["sage_policy_stage"] = action.filters.get("_sage_policy_stage")
        note["sage_policy_mode"] = action.filters.get("_sage_policy_mode")
        note["sage_policy_reason"] = action.filters.get("_sage_policy_reason")
        if action.filters.get("_sage_route_utility_score") is not None:
            note["sage_route_utility_score"] = action.filters.get(
                "_sage_route_utility_score"
            )
            note["sage_route_utility_confidence"] = action.filters.get(
                "_sage_route_utility_confidence"
            )
            note["sage_route_utility_match"] = action.filters.get(
                "_sage_route_utility_match"
            )
            note["sage_route_utility_skip"] = bool(
                action.filters.get("_sage_route_utility_skip")
            )
    if action.filters.get("_reflective_rule_ids"):
        note["reflective_rule_ids"] = action.filters.get("_reflective_rule_ids")
        note["reflective_rule_match_score"] = action.filters.get(
            "_reflective_rule_match_score"
        )
    return note


def _skipped_action_timing_note(
    action: RetrievalAction,
    *,
    skip_reason: str,
    admission_coverage: dict[str, int] | None = None,
) -> dict[str, Any]:
    note = _action_timing_note(
        action,
        None,
        elapsed_ms=0,
        cache_hit=False,
        timing_kind="owner_work",
    )
    note.update(
        {
            "skipped": True,
            "skip_reason": skip_reason,
            "work_elapsed_ms": 0,
        }
    )
    if admission_coverage is not None:
        note["admission_coverage"] = dict(admission_coverage)
    return note


def _action_stage(action: RetrievalAction) -> int | None:
    raw = action.filters.get("_motif_stage")
    if raw is None:
        raw = action.filters.get("_reconstruction_stage")
    if raw is None:
        raw = action.filters.get("_sage_policy_stage")
    if raw is None:
        return None
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 1


__all__ = [
    "ActionExecutionSession",
    "_ActionExecutionRecord",
    "_QuestionRetrievalPlan",
    "_action_timing_note",
    "_action_stage",
    "_execute_action",
    "_execute_question_retrieval_actions",
    "_execute_question_retrieval_actions_serial",
    "_execute_question_retrieval_actions_staged",
]
