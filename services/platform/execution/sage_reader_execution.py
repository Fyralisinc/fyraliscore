"""Sage reader action execution for adaptive inquiry retrieval."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict
from typing import Any, Awaitable, Callable

import asyncpg

from lib.shared.errors import ValidationError
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .config import InquiryConfig
from .evidence_utils import jsonable as _jsonable
from .runtime_metrics import elapsed_ms as _elapsed_ms
from .types import Hypothesis, InquiryQuestion, RetrievalAction


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
            substrate_edge_seed_limit=max(
                12, min(48, int(cfg.candidate_model_limit // 3))
            ),
            substrate_edge_limit=max(32, min(96, int(cfg.candidate_model_limit // 2))),
            rerank_min_substrate_models=8,
            rerank_lexical_candidates=6,
            lexical_microquery_enabled=True,
            lexical_microquery_terms=8,
            lexical_microquery_per_term_limit=16,
        )
    )


def _stable_sage_cache_value(value: Any) -> str:
    return json.dumps(
        _jsonable(value),
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _sage_reader_round_cache_key(
    question: InquiryQuestion,
    trigger: TriggerContext,
    hypotheses: tuple[Hypothesis, ...],
    evidence_state: dict[str, Any] | None,
) -> tuple[Any, ...]:
    return (
        str(trigger.tenant_id),
        trigger.kind,
        " ".join(str(question.question or "").casefold().split()),
        str(question.primitive or "").upper(),
        tuple(sorted(str(actor) for actor in (trigger.scope_actors or []))),
        _stable_sage_cache_value(trigger.seed_entity_ids or []),
        _stable_sage_cache_value(trigger.seed_signature or {}),
        _stable_sage_cache_value([asdict(hypothesis) for hypothesis in hypotheses]),
        _stable_sage_cache_value(evidence_state or {}),
    )


def _clone_pathway_result_for_sage_cache(
    result: Any,
    *,
    question_id: str,
    question_primitive: str,
) -> Any:
    if not isinstance(result, PathwayResult):
        return result
    notes = dict(result.notes or {})
    if "question_id" in notes:
        notes["question_id"] = question_id
    if "question_primitive" in notes:
        notes["question_primitive"] = question_primitive
    return PathwayResult(
        models=list(result.models),
        observations=list(result.observations),
        acts={key: list(value) for key, value in (result.acts or {}).items()},
        resources=list(result.resources),
        source_pathway=result.source_pathway,
        notes=notes,
    )


def _sage_reader_question_id_from_result(result: RetrievalResult | None) -> str | None:
    if result is None or not isinstance(result.notes, dict):
        return None
    read_note = result.notes.get("sage_reader")
    if not isinstance(read_note, dict):
        return None
    raw = read_note.get("question_id")
    return str(raw) if raw else None


def _clone_sage_reader_result(
    result: RetrievalResult,
    *,
    question: InquiryQuestion,
    cache_hit: bool,
    cache_wait: bool = False,
    cache_source_question_id: str | None = None,
) -> RetrievalResult:
    notes = dict(result.notes or {})
    action_note = dict(notes.get("action") or {})
    if action_note:
        action_note["question_id"] = question.question_id
        action_note["query"] = question.question
        notes["action"] = action_note

    read_note = dict(notes.get("sage_reader") or {})
    source_qid = cache_source_question_id or read_note.get("question_id")
    read_note["question_id"] = question.question_id
    read_note["question_primitive"] = question.primitive
    read_note["cache_hit"] = bool(cache_hit)
    read_note["cache_wait"] = bool(cache_wait)
    if cache_wait:
        read_note["timing_kind"] = "in_flight_wait"
    elif cache_hit:
        read_note["timing_kind"] = "cache_hit"
    else:
        read_note["timing_kind"] = "owner_work"
    if cache_hit and source_qid and source_qid != question.question_id:
        read_note["cache_source_question_id"] = str(source_qid)
    activations = []
    for trace in read_note.get("activations") or []:
        if isinstance(trace, dict):
            rebound = dict(trace)
            rebound["question_id"] = question.question_id
            activations.append(rebound)
        else:
            activations.append(trace)
    if activations:
        read_note["activations"] = activations
    notes["sage_reader"] = read_note

    pathway_results = [
        _clone_pathway_result_for_sage_cache(
            pathway,
            question_id=question.question_id,
            question_primitive=question.primitive,
        )
        for pathway in result.pathway_results
    ]
    return RetrievalResult(
        trigger=result.trigger,
        observations=list(result.observations),
        models=list(result.models),
        acts={key: list(value) for key, value in (result.acts or {}).items()},
        resources=list(result.resources),
        pathway_results=pathway_results,
        notes=notes,
        model_scores=dict(result.model_scores),
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
    evidence_state: dict[str, Any] | None = None,
    on_question_result: Callable[
        [InquiryQuestion, RetrievalResult | None], Awaitable[None]
    ]
    | None = None,
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
    result_cache: dict[tuple[Any, ...], RetrievalResult | None] = {}
    in_flight: dict[tuple[Any, ...], asyncio.Task[RetrievalResult | None]] = {}
    cache_lock = asyncio.Lock()
    cache_hits = 0
    cache_waits = 0
    cache_wait_elapsed_ms = 0

    async def notify_question_result(
        question: InquiryQuestion,
        result: RetrievalResult | None,
    ) -> None:
        if on_question_result is not None:
            await on_question_result(question, result)

    async def run_uncached(
        question: InquiryQuestion,
        read_conn: asyncpg.Connection,
    ) -> RetrievalResult | None:
        return await _execute_sage_reader_action(
            question,
            trigger,
            read_conn,
            cfg,
            reader=reader,
            substrate=substrate,
            hypotheses=hypotheses,
            evidence_state=evidence_state,
        )

    async def run_uncached_from_pool(
        question: InquiryQuestion,
    ) -> RetrievalResult | None:
        if read_pool is None:
            raise RuntimeError("read_pool is required for pooled Sage read")
        async with read_pool.acquire() as read_conn:
            return await run_uncached(question, read_conn)

    async def run_cached(
        question: InquiryQuestion,
        read_conn: asyncpg.Connection | None,
    ) -> tuple[RetrievalResult | None, bool, bool]:
        nonlocal cache_hits, cache_waits, cache_wait_elapsed_ms
        cache_key = _sage_reader_round_cache_key(
            question,
            trigger,
            hypotheses,
            evidence_state,
        )
        async with cache_lock:
            if cache_key in result_cache:
                cache_hits += 1
                cached = result_cache[cache_key]
                source_qid = _sage_reader_question_id_from_result(cached)
                return (
                    (
                        _clone_sage_reader_result(
                            cached,
                            question=question,
                            cache_hit=True,
                            cache_source_question_id=source_qid,
                        )
                        if cached is not None
                        else None
                    ),
                    True,
                    False,
                )
            task = in_flight.get(cache_key)
            owner = task is None
            if task is None:
                task = asyncio.create_task(
                    run_uncached(question, read_conn)
                    if read_conn is not None
                    else run_uncached_from_pool(question)
                )
                in_flight[cache_key] = task

        try:
            wait_started = time.perf_counter()
            result = await task
        except BaseException:
            if owner:
                async with cache_lock:
                    in_flight.pop(cache_key, None)
            raise
        if owner:
            async with cache_lock:
                result_cache[cache_key] = result
                in_flight.pop(cache_key, None)
            return (
                (
                    _clone_sage_reader_result(
                        result,
                        question=question,
                        cache_hit=False,
                    )
                    if result is not None
                    else None
                ),
                False,
                False,
            )

        cache_waits += 1
        cache_wait_elapsed_ms += _elapsed_ms(wait_started)
        source_qid = _sage_reader_question_id_from_result(result)
        return (
            (
                _clone_sage_reader_result(
                    result,
                    question=question,
                    cache_hit=True,
                    cache_wait=True,
                    cache_source_question_id=source_qid,
                )
                if result is not None
                else None
            ),
            True,
            True,
        )

    if not parallel:
        for question in questions:
            result, _cache_hit, _cache_wait = await run_cached(question, conn)
            if result is not None:
                results[question.question_id] = result
            await notify_question_result(question, result)
        return results, {
            "used": True,
            "parallel": False,
            "question_count": len(questions),
            "returned": len(results),
            "cache_hits": cache_hits,
            "cache_waits": cache_waits,
            "cache_wait_elapsed_ms": cache_wait_elapsed_ms,
            "elapsed_ms": _elapsed_ms(started),
        }

    semaphore = asyncio.Semaphore(max(1, int(cfg.sage_reader_parallelism)))

    async def run_one(question: InquiryQuestion) -> tuple[str, RetrievalResult | None]:
        async with semaphore:
            result, _cache_hit, _cache_wait = await run_cached(question, None)
            return question.question_id, result

    question_by_id = {question.question_id: question for question in questions}
    tasks = [asyncio.create_task(run_one(question)) for question in questions]
    try:
        for completed in asyncio.as_completed(tasks):
            qid, result = await completed
            if result is not None:
                results[qid] = result
            question = question_by_id.get(qid)
            if question is not None:
                await notify_question_result(question, result)
    except BaseException:
        for task in tasks:
            task.cancel()
        raise
    return results, {
        "used": True,
        "parallel": True,
        "parallelism": max(1, int(cfg.sage_reader_parallelism)),
        "question_count": len(questions),
        "returned": len(results),
        "cache_hits": cache_hits,
        "cache_waits": cache_waits,
        "cache_wait_elapsed_ms": cache_wait_elapsed_ms,
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
    evidence_state: dict[str, Any] | None = None,
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
            evidence_state=evidence_state,
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
            "action": _jsonable(
                asdict(
                    RetrievalAction(
                        question.question_id,
                        "sage_reader",
                        "synthesis_reader",
                        query=question.question,
                        budget=cfg.result_model_limit,
                    )
                )
            ),
            "pathways_run": ["sage_reader"],
            "sage_reader": {
                "question_id": question.question_id,
                "question_primitive": read.question_primitive,
                "signature": read.signature,
                "selected_model_ids": [str(m.id) for m in read.models],
                "projected_evidence_count": len(read.projected_evidence),
                "activation_trace_count": len(read.activations),
                "debug": read.debug,
                "activations": [_jsonable(asdict(trace)) for trace in read.activations],
                "reconstruction_state": _jsonable(evidence_state or {}),
            },
        },
        model_scores=dict(read.model_scores),
    )


__all__ = [
    "_build_sage_reader",
    "_execute_sage_reader_action",
    "_execute_sage_reader_actions_for_round",
]
