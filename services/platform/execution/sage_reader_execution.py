"""Sage reader action execution for adaptive inquiry retrieval."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from typing import Any

import asyncpg

from lib.shared.errors import ValidationError
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
                evidence_state=evidence_state,
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
                    evidence_state=evidence_state,
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
