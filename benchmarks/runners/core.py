"""Core benchmark runner orchestration."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from benchmarks.adapters.base import BenchmarkAdapter
from benchmarks.fyralis_eval.answerer import (
    AnswerResult,
    CodexFixedAnswerer,
    FixedExtractiveAnswerer,
    LLMFixedAnswerer,
    PassthroughAnswerer,
)
from benchmarks.fyralis_eval.evaluator import EvaluationResult, evaluate_answer
from benchmarks.fyralis_eval.fyralis_db import (
    FyralisAskReader,
    FyralisDBReader,
    FyralisSageCoverageHybridReader,
    FyralisSageHybridReader,
    FyralisSagePrecisionHybridReader,
    FyralisSageReader,
    FyralisSageSemanticHybridReader,
)
from benchmarks.fyralis_eval.ingestion import InMemoryBenchmarkStore
from benchmarks.fyralis_eval.judge import CodexAnswerJudge, LLMAnswerJudge
from benchmarks.fyralis_eval.metrics import mean_metric
from benchmarks.fyralis_eval.packet_compiler import ContextPacketCompiler
from benchmarks.fyralis_eval.reader import LexicalMemoryReader
from benchmarks.fyralis_eval.reader import BM25MemoryReader


@dataclass(frozen=True)
class BenchmarkRunConfig:
    benchmark: str
    system_name: str = "lexical_fixed_answerer"
    top_k: int = 5
    evidence_k: int = 10
    random_seed: int = 0
    model_version: str = "fixed_extract_answerer_v1"
    answerer_name: str = "extractive"
    judge_name: str | None = None
    embedding_model_version: str = "none"
    packet_budget: int | None = None
    score_answers: bool = True
    progress: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "system_name": self.system_name,
            "top_k": self.top_k,
            "evidence_k": self.evidence_k,
            "random_seed": self.random_seed,
            "model_version": self.model_version,
            "answerer_name": self.answerer_name,
            "judge_name": self.judge_name,
            "embedding_model_version": self.embedding_model_version,
            "packet_budget": self.packet_budget,
            "score_answers": self.score_answers,
            "progress": self.progress,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "git_commit": _git_commit(),
            "hardware_profile": "local",
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class BenchmarkRunResult:
    config: BenchmarkRunConfig
    results: list[EvaluationResult]
    retrieval_traces: list[dict[str, Any]]
    metrics_summary: dict[str, Any]
    observations_ingested: int


def run_benchmark(
    adapter: BenchmarkAdapter,
    *,
    config: BenchmarkRunConfig | None = None,
) -> BenchmarkRunResult:
    config = config or BenchmarkRunConfig(benchmark=adapter.benchmark_name)
    adapter.load_raw()
    adapter.preprocess()

    observations = list(adapter.iter_observations())
    store = InMemoryBenchmarkStore()
    store.ingest(observations)

    reader = _build_reader(config, store, observations)
    compiler = ContextPacketCompiler()
    answerer = _build_answerer(config.answerer_name)
    judge = _build_judge(config.judge_name)

    results: list[EvaluationResult] = []
    traces: list[dict[str, Any]] = []
    try:
        queries = list(adapter.iter_queries())
        for index, query in enumerate(queries, start=1):
            evidence, latency_ms, retrieval_calls = _retrieve_with_transient_retry(
                reader,
                query,
            )
            retrieval = compiler.compile(
                query,
                evidence,
                latency_ms=latency_ms,
                retrieval_calls=retrieval_calls,
            )
            answer_result = _answer_with_failure_fallback(answerer, query, retrieval)
            result = evaluate_answer(
                benchmark=adapter.benchmark_name,
                system_name=config.system_name,
                query=query,
                gold=adapter.gold(query.query_id),
                answer=answer_result.answer,
                retrieval=retrieval,
                evidence_k=config.evidence_k,
                score_answer=config.score_answers,
            )
            result.debug["answerer"] = answer_result.metadata
            materialization = getattr(reader, "materialization", None)
            if materialization is not None:
                result.debug["fyralis_db_materialization"] = {
                    "namespace": materialization.namespace,
                    "observations": materialization.observations,
                    "tenants": materialization.tenants,
                    "embedding_model_version": materialization.embedding_model_version,
                }
            _attach_answerer_metrics(result, answer_result.metadata)
            if judge is not None:
                judge_result = _judge_with_failure_fallback(
                    judge,
                    query=query,
                    expected_answer=adapter.gold(query.query_id).answer,
                    predicted_answer=answer_result.answer,
                )
                _attach_judge_result(
                    result,
                    judge_result.correct,
                    judge_result.score,
                    judge_result.rationale,
                    judge_result.metadata,
                    benchmark=adapter.benchmark_name,
                )
            results.append(result)
            traces.append(retrieval.to_json())
            if config.progress:
                _print_progress(
                    index=index,
                    total=len(queries),
                    result=result,
                    evidence_k=config.evidence_k,
                )
    finally:
        close = getattr(reader, "close", None)
        if callable(close):
            close()

    return BenchmarkRunResult(
        config=config,
        results=results,
        retrieval_traces=traces,
        metrics_summary=summarize_results(results),
        observations_ingested=store.count_observations(),
    )


def _print_progress(
    *,
    index: int,
    total: int,
    result: EvaluationResult,
    evidence_k: int,
) -> None:
    support = result.metrics.get("longmemeval_v2_packet_answer_support")
    accuracy = result.metrics.get("longmemeval_v2_accuracy")
    latency = result.metrics.get("latency_ms")
    tokens = result.metrics.get("token_cost")
    print(
        "progress "
        f"{index}/{total} "
        f"query={result.query_id} "
        f"support@{evidence_k}={_fmt_progress_metric(support)} "
        f"answer_accuracy={_fmt_progress_metric(accuracy)} "
        f"latency_ms={_fmt_progress_metric(latency)} "
        f"tokens={_fmt_progress_metric(tokens)}",
        flush=True,
    )


def _fmt_progress_metric(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def summarize_results(results: list[EvaluationResult]) -> dict[str, Any]:
    metric_names = sorted({
        name
        for result in results
        for name in result.metrics
    })
    summary: dict[str, Any] = {
        "queries": len(results),
        "passed_exact": sum(1 for result in results if result.metrics.get("accuracy") == 1.0),
    }
    for name in metric_names:
        summary[name] = mean_metric([
            float(result.metrics[name])
            for result in results
            if result.metrics.get(name) is not None
        ])
    return summary


def _attach_answerer_metrics(
    result: EvaluationResult,
    metadata: dict[str, Any],
) -> None:
    llm = metadata.get("llm") if isinstance(metadata, dict) else None
    if not isinstance(llm, dict):
        return
    result.metrics["answerer_llm_calls"] = _num(llm.get("calls"))
    result.metrics["answerer_input_tokens"] = _num(llm.get("input_tokens"))
    result.metrics["answerer_output_tokens"] = _num(llm.get("output_tokens"))
    result.metrics["answerer_cost_usd"] = _num(llm.get("cost_usd"))


def _retrieve_with_transient_retry(reader, query, *, attempts: int = 3):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return reader.retrieve(query)
        except Exception as exc:  # pragma: no cover - exercised by integration runs.
            if not _is_transient_retrieval_error(exc) or attempt == attempts - 1:
                raise
            last_error = exc
            time.sleep(0.5 * (2 ** attempt))
    assert last_error is not None
    raise last_error


def _is_transient_retrieval_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.casefold()
    message = str(exc).casefold()
    transient_names = (
        "deadlockdetectederror",
        "serializationerror",
        "cannotconnectnowerror",
        "connectiondoesnotexisterror",
    )
    if any(item in name for item in transient_names):
        return True
    return "deadlock detected" in message or "could not serialize access" in message


def _answer_with_failure_fallback(answerer, query, retrieval) -> AnswerResult:
    try:
        return answerer.answer_result(query, retrieval)
    except Exception as exc:  # pragma: no cover - exercised by live judged runs.
        if not _is_transient_llm_error(exc):
            raise
        return AnswerResult(
            "I don't know",
            {
                "answerer": answerer.__class__.__name__,
                "error": exc.__class__.__name__,
                "error_message": str(exc)[:500],
                "fallback": "transient_answerer_failure",
            },
        )


def _judge_with_failure_fallback(
    judge,
    *,
    query,
    expected_answer,
    predicted_answer,
):
    try:
        return judge.judge(
            query=query,
            expected_answer=expected_answer,
            predicted_answer=predicted_answer,
        )
    except Exception as exc:  # pragma: no cover - exercised by live judged runs.
        if not _is_transient_llm_error(exc):
            raise
        from benchmarks.fyralis_eval.judge import JudgeResult

        return JudgeResult(
            correct=False,
            score=0.0,
            rationale=f"Judge failed with transient error: {exc.__class__.__name__}",
            metadata={
                "judge": judge.__class__.__name__,
                "error": exc.__class__.__name__,
                "error_message": str(exc)[:500],
                "fallback": "transient_judge_failure",
            },
        )


def _is_transient_llm_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.casefold()
    message = str(exc).casefold()
    return (
        "timeout" in name
        or "timed out" in message
        or "rate limit" in message
        or "too many requests" in message
    )


def _attach_judge_result(
    result: EvaluationResult,
    correct: bool,
    score: float,
    rationale: str,
    metadata: dict[str, Any],
    *,
    benchmark: str,
) -> None:
    result.metrics["judge_correctness"] = 1.0 if correct else 0.0
    result.metrics["judge_score"] = score
    if benchmark == "memtrack":
        result.metrics["memtrack_llm_judge_correctness"] = 1.0 if correct else 0.0
        result.metrics["memtrack_llm_judge_score"] = score
    llm = metadata.get("llm") if isinstance(metadata, dict) else None
    if isinstance(llm, dict):
        result.metrics["judge_llm_calls"] = _num(llm.get("calls"))
        result.metrics["judge_input_tokens"] = _num(llm.get("input_tokens"))
        result.metrics["judge_output_tokens"] = _num(llm.get("output_tokens"))
        result.metrics["judge_cost_usd"] = _num(llm.get("cost_usd"))
    result.debug["judge"] = {
        **metadata,
        "correct": correct,
        "score": score,
        "rationale": rationale,
    }


def _num(value: Any) -> float | int | None:
    if isinstance(value, (int, float)):
        return value
    return None


def _build_reader(
    config: BenchmarkRunConfig,
    store: InMemoryBenchmarkStore,
    observations: list,
):
    system_name = config.system_name
    normalized = system_name.casefold()
    if normalized in {"bm25", "bm25_session", "bm25_fixed_answerer"}:
        return BM25MemoryReader(store, top_k=config.top_k)
    if normalized in {"lexical", "lexical_fixed_answerer"}:
        return LexicalMemoryReader(store, top_k=config.top_k)
    if normalized in {
        "fyralis_ask_current",
        "fyralis_db_hash_embedding",
        "fyralis_current",
        "fyralis_db_current",
        "fyralis_sage_reader",
        "fyralis_sage_current",
        "fyralis_sage_bm25_seed",
        "fyralis_sage_hybrid",
        "fyralis_sage_precision_hybrid",
        "fyralis_sage_coverage_hybrid",
        "fyralis_sage_semantic_hybrid",
        "fyralis_sage_full_potential",
    }:
        apply_migrations = bool(config.metadata.get("apply_migrations"))
        embedding_mode = str(config.metadata.get("embedding_mode") or "")
        if not embedding_mode:
            embedding_mode = "hash" if normalized == "fyralis_db_hash_embedding" else "provider"
        reader_cls = (
            FyralisAskReader
            if normalized == "fyralis_ask_current"
            else
            FyralisSageReader
            if normalized in {
                "fyralis_sage_reader",
                "fyralis_sage_current",
                "fyralis_sage_bm25_seed",
                "fyralis_sage_hybrid",
                "fyralis_sage_precision_hybrid",
                "fyralis_sage_coverage_hybrid",
                "fyralis_sage_semantic_hybrid",
                "fyralis_sage_full_potential",
            }
            else FyralisDBReader
        )
        if normalized == "fyralis_sage_hybrid":
            reader_cls = FyralisSageHybridReader
        if normalized == "fyralis_sage_precision_hybrid":
            reader_cls = FyralisSagePrecisionHybridReader
        if normalized == "fyralis_sage_coverage_hybrid":
            reader_cls = FyralisSageCoverageHybridReader
        if normalized == "fyralis_sage_semantic_hybrid":
            reader_cls = FyralisSageSemanticHybridReader
        enrich_graph = bool(config.metadata.get("graph_enrichment"))
        if normalized == "fyralis_sage_full_potential":
            enrich_graph = True
        bm25_seed_candidates = 0
        if normalized in {
            "fyralis_sage_bm25_seed",
            "fyralis_sage_hybrid",
            "fyralis_sage_precision_hybrid",
            "fyralis_sage_coverage_hybrid",
            "fyralis_sage_semantic_hybrid",
            "fyralis_sage_full_potential",
        }:
            default_seed_candidates = (
                max(160, config.top_k * 24)
                if normalized in {
                    "fyralis_sage_coverage_hybrid",
                    "fyralis_sage_semantic_hybrid",
                }
                else max(80, config.top_k * 12)
                if normalized in {
                    "fyralis_sage_hybrid",
                    "fyralis_sage_precision_hybrid",
                    "fyralis_sage_coverage_hybrid",
                    "fyralis_sage_semantic_hybrid",
                }
                else max(50, config.top_k * 8)
                if normalized == "fyralis_sage_full_potential"
                else max(20, config.top_k * 4)
            )
            bm25_seed_candidates = int(
                config.metadata.get("bm25_seed_candidates") or default_seed_candidates
            )
        return reader_cls(
            observations,
            top_k=config.top_k,
            namespace=str(config.metadata.get("db_namespace") or config.benchmark),
            embedding_mode=embedding_mode,
            apply_migrations=apply_migrations,
            enrich_graph=enrich_graph,
            bm25_seed_candidates=bm25_seed_candidates,
        )
    raise ValueError(f"Unsupported benchmark system_name={system_name!r}")


def _build_answerer(answerer_name: str):
    normalized = answerer_name.casefold()
    if normalized in {"passthrough", "ask_passthrough", "product_passthrough"}:
        return PassthroughAnswerer()
    if normalized in {"extractive", "fixed_extractive", "fixed_extract_answerer_v1"}:
        return FixedExtractiveAnswerer()
    if normalized in {"codex", "codex_fixed", "fixed_codex"}:
        return CodexFixedAnswerer()
    if normalized in {"llm", "llm_fixed", "fixed_llm"}:
        return LLMFixedAnswerer()
    raise ValueError(f"Unsupported benchmark answerer_name={answerer_name!r}")


def _build_judge(judge_name: str | None):
    if not judge_name:
        return None
    normalized = judge_name.casefold()
    if normalized in {"llm", "llm_judge", "fixed_llm_judge"}:
        return LLMAnswerJudge()
    if normalized in {"codex", "codex_judge", "fixed_codex_judge"}:
        return CodexAnswerJudge()
    raise ValueError(f"Unsupported benchmark judge_name={judge_name!r}")


def _git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None
