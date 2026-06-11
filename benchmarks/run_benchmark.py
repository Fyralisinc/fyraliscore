"""CLI for running benchmark harness targets."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from benchmarks.adapters.hotpotqa_adapter import HotpotQAAdapter
from benchmarks.adapters.halumem_adapter import HaluMemAdapter
from benchmarks.adapters.longmemeval_adapter import LongMemEvalAdapter
from benchmarks.adapters.longmemeval_v2_adapter import LongMemEvalV2Adapter
from benchmarks.adapters.memtrack_adapter import MemTrackAdapter
from benchmarks.adapters.stress10_adapter import Stress10Adapter
from benchmarks.adapters.toy_adapter import ToyMemoryAdapter
from benchmarks.adapters.truss_adapter import TrussAdapter
from benchmarks.fyralis_eval.reporting import write_run_artifacts
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Fyralis benchmark harness targets.")
    parser.add_argument(
        "--benchmark",
        default="toy",
        choices=[
            "toy",
            "toy_memory",
            "longmemeval",
            "longmemeval_v2",
            "lme_v2",
            "hotpotqa",
            "halumem",
            "memtrack",
            "stress10",
            "truss",
            "truss_r1",
            "truss_r2",
            "truss_full",
        ],
        help="Benchmark adapter to run.",
    )
    parser.add_argument("--data", type=Path, help="Path to a public benchmark JSON file.")
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument(
        "--system",
        default=None,
        help=(
            "System variant, e.g. lexical, bm25, fyralis_current, "
            "fyralis_ask_current, fyralis_sage_reader, fyralis_sage_hybrid, "
            "fyralis_sage_precision_hybrid, fyralis_sage_coverage_hybrid, "
            "fyralis_sage_semantic_hybrid, or fyralis_sage_full_potential."
        ),
    )
    parser.add_argument(
        "--answerer",
        default=None,
        choices=["extractive", "llm", "codex", "passthrough"],
        help="Fixed answerer to use when scoring answers.",
    )
    parser.add_argument(
        "--judge-answers",
        action="store_true",
        help=(
            "Run an LLM judge over final predicted answers. This is intended "
            "for end-to-end correctness runs, not retrieval-only runs."
        ),
    )
    parser.add_argument(
        "--judge",
        default="llm",
        choices=["llm", "codex"],
        help="Judge provider path to use when --judge-answers is set.",
    )
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument(
        "--include-abstention",
        action="store_true",
        help="Include LongMemEval abstention questions. Retrieval comparisons normally skip them.",
    )
    parser.add_argument(
        "--haystack-tier",
        choices=["small", "medium"],
        default="small",
        help="LongMemEval-V2 haystack tier to use.",
    )
    parser.add_argument(
        "--score-answers",
        action="store_true",
        help="Score answer exact match/F1. Leave off for retrieval-only public benchmark runs.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print one progress line after each benchmark case completes.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("benchmarks/reports/generated/toy_memory"),
        help="Directory for run artifacts.",
    )
    parser.add_argument(
        "--apply-migrations",
        action="store_true",
        help="Apply db/migrations before materializing a Fyralis DB-backed benchmark run.",
    )
    parser.add_argument(
        "--embedding-mode",
        choices=["hash", "provider", "ollama", "openai"],
        default=None,
        help=(
            "Embedding mode for Fyralis DB-backed systems. Defaults to hash "
            "for fyralis_db_hash_embedding and provider for fyralis_current."
        ),
    )
    parser.add_argument(
        "--graph-enrichment",
        action="store_true",
        help="Add benchmark-derived temporal/shared-term graph edges for SAGE runs.",
    )
    parser.add_argument(
        "--bm25-seed-candidates",
        type=int,
        default=None,
        help="Candidate count for fyralis_sage_bm25_seed.",
    )
    parser.add_argument(
        "--truss-facts",
        type=Path,
        default=Path("benchmarks/truss_signal_derivable_facts.json"),
        help="Frozen signal-derivable fact checklist for the Truss adapter.",
    )
    args = parser.parse_args(argv)

    if args.benchmark in {"toy", "toy_memory"}:
        adapter = ToyMemoryAdapter()
        system_name = args.system or "lexical_fixed_answerer"
        score_answers = True
        answerer_name = args.answerer or "extractive"
    elif args.benchmark == "stress10":
        adapter = Stress10Adapter()
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    elif args.benchmark == "longmemeval":
        if args.data is None:
            parser.error("--data is required for --benchmark longmemeval")
        adapter = LongMemEvalAdapter(
            args.data,
            max_cases=args.max_cases,
            include_abstention=args.include_abstention,
        )
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    elif args.benchmark in {"longmemeval_v2", "lme_v2"}:
        if args.data is None:
            parser.error("--data is required for --benchmark longmemeval_v2")
        adapter = LongMemEvalV2Adapter(
            args.data,
            max_cases=args.max_cases,
            haystack_tier=args.haystack_tier,
        )
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    elif args.benchmark == "hotpotqa":
        if args.data is None:
            parser.error("--data is required for --benchmark hotpotqa")
        adapter = HotpotQAAdapter(args.data, max_cases=args.max_cases)
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    elif args.benchmark == "memtrack":
        if args.data is None:
            parser.error("--data is required for --benchmark memtrack")
        adapter = MemTrackAdapter(args.data, max_cases=args.max_cases)
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    elif args.benchmark in {"truss", "truss_r1", "truss_r2", "truss_full"}:
        data_path = args.data or Path(".")
        adapter = TrussAdapter(
            data_path,
            include_run1=args.benchmark in {"truss", "truss_r1", "truss_full"},
            include_run2=args.benchmark in {"truss", "truss_r2", "truss_full"},
            fact_filter_path=args.truss_facts,
            max_cases=args.max_cases,
            tenant_id=(
                "truss_company_replay_run2_only"
                if args.benchmark == "truss_r2"
                else "truss_company_replay"
            ),
        )
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")
    else:
        if args.data is None:
            parser.error("--data is required for --benchmark halumem")
        adapter = HaluMemAdapter(
            args.data,
            max_users=args.max_cases,
        )
        system_name = args.system or "bm25_session"
        score_answers = args.score_answers
        answerer_name = args.answerer or ("llm" if score_answers else "extractive")

    if system_name.casefold() == "fyralis_ask_current" and args.answerer is None:
        answerer_name = "passthrough"

    embedding_mode = _resolve_embedding_mode(system_name, args.embedding_mode)
    graph_enrichment = bool(args.graph_enrichment) or (
        system_name.casefold() == "fyralis_sage_full_potential"
    )
    config = BenchmarkRunConfig(
        benchmark=adapter.benchmark_name,
        system_name=system_name,
        top_k=args.top_k,
        evidence_k=args.evidence_k,
        score_answers=score_answers,
        answerer_name=answerer_name,
        embedding_model_version=_embedding_model_version(system_name, embedding_mode),
        model_version=_answerer_model_version(answerer_name),
        judge_name=(args.judge if args.judge_answers else None),
        progress=args.progress,
        metadata={
            "data_path": str(args.data) if args.data else None,
            "max_cases": args.max_cases,
            "include_abstention": args.include_abstention,
            "haystack_tier": args.haystack_tier if adapter.benchmark_name == "longmemeval_v2" else None,
            "apply_migrations": args.apply_migrations,
            "embedding_mode": embedding_mode,
            "graph_enrichment": graph_enrichment,
            "bm25_seed_candidates": args.bm25_seed_candidates,
            "db_namespace": (
                f"{adapter.benchmark_name}:{system_name}:{args.out.name}"
                if system_name.casefold().startswith("fyralis")
                else None
            ),
            "embedding_max_chars": (
                os.environ.get("BENCHMARK_EMBED_MAX_CHARS")
                if embedding_mode is not None
                else None
            ),
            "embedding_concurrency": (
                os.environ.get("BENCHMARK_EMBED_CONCURRENCY")
                if embedding_mode is not None
                else None
            ),
        },
    )
    run = run_benchmark(adapter, config=config)
    run_config = run.config.to_json()
    run_config["observations_ingested"] = run.observations_ingested
    artifacts = write_run_artifacts(
        output_dir=args.out,
        run_config=run_config,
        results=run.results,
        retrieval_traces=run.retrieval_traces,
        metrics_summary=run.metrics_summary,
    )
    print(f"Benchmark: {adapter.benchmark_name}")
    print(f"System: {config.system_name}")
    print(f"Queries: {run.metrics_summary['queries']}")
    if run.metrics_summary.get("accuracy") is not None:
        print(f"Accuracy: {run.metrics_summary.get('accuracy'):.4f}")
    if run.metrics_summary.get("memtrack_llm_judge_correctness") is not None:
        print(
            "MEMTRACK LLM Judge Correctness: "
            f"{run.metrics_summary['memtrack_llm_judge_correctness']:.4f}"
        )
    recall_key = f"evidence_recall_at_{config.evidence_k}"
    precision_key = f"evidence_precision_at_{config.evidence_k}"
    if run.metrics_summary.get(recall_key) is not None:
        print(f"Evidence Recall@{config.evidence_k}: {run.metrics_summary[recall_key]:.4f}")
    if run.metrics_summary.get(precision_key) is not None:
        print(f"Evidence Precision@{config.evidence_k}: {run.metrics_summary[precision_key]:.4f}")
    support_key = f"memtrack_answer_support_at_{config.evidence_k}"
    if run.metrics_summary.get(support_key) is not None:
        print(
            f"MEMTRACK Answer Support@{config.evidence_k}: "
            f"{run.metrics_summary[support_key]:.4f}"
        )
    print(f"Report: {artifacts.benchmark_report_md}")
    return 0


def _answerer_model_version(answerer_name: str) -> str:
    normalized = answerer_name.casefold()
    if normalized == "passthrough":
        return "product_passthrough_answer"
    if normalized == "codex":
        return "fixed_codex_answerer"
    if normalized == "llm":
        return "fixed_llm_answerer"
    return "fixed_extract_answerer_v1"


def _resolve_embedding_mode(system_name: str, explicit: str | None) -> str | None:
    normalized = system_name.casefold()
    if normalized not in {
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
        return None
    if explicit is not None:
        return explicit
    return "hash" if normalized == "fyralis_db_hash_embedding" else "provider"


def _embedding_model_version(system_name: str, embedding_mode: str | None) -> str:
    if embedding_mode is None:
        return "none"
    if embedding_mode == "hash":
        return "hashed_token_vector_v1"
    if embedding_mode == "ollama":
        return f"ollama:{os.environ.get('OLLAMA_EMBED_MODEL', 'nomic-embed-text')}"
    if embedding_mode == "openai":
        return f"openai:{os.environ.get('OPENAI_EMBED_MODEL', 'text-embedding-3-small')}"
    if embedding_mode == "provider":
        backend = os.environ.get("EMBEDDER_BACKEND")
        if backend:
            return f"provider:{backend}"
        if os.environ.get("OPENAI_API_KEY") and not os.environ.get("OLLAMA_URL"):
            return "provider:openai"
        return "provider:ollama"
    return f"{system_name}:{embedding_mode}"


if __name__ == "__main__":
    sys.exit(main())
