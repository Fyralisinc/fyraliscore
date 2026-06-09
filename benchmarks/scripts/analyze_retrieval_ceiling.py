"""Analyze retrieval misses and oracle ceilings for benchmark runs."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.adapters.base import BenchmarkObservation, BenchmarkQuery, GoldLabels
from benchmarks.adapters.halumem_adapter import HaluMemAdapter
from benchmarks.adapters.hotpotqa_adapter import HotpotQAAdapter
from benchmarks.adapters.longmemeval_adapter import LongMemEvalAdapter
from benchmarks.fyralis_eval.reader import (
    _bm25_term_score,
    _document_frequencies,
    token_counts,
)


@dataclass(frozen=True)
class RankedObservation:
    rank: int
    score: float
    observation: BenchmarkObservation


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze retrieval misses and BM25 oracle ceilings.",
    )
    parser.add_argument("--benchmark", choices=["longmemeval", "hotpotqa", "halumem"], required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--include-abstention", action="store_true")
    parser.add_argument("--evidence-k", type=int, default=5)
    parser.add_argument(
        "--oracle-k",
        type=int,
        action="append",
        default=None,
        help="Oracle K to report. Can be repeated. Defaults to 5,10,20,50,80,100.",
    )
    parser.add_argument("--examples", type=int, default=20)
    args = parser.parse_args()

    oracle_ks = sorted(set(args.oracle_k or [5, 10, 20, 50, 80, 100]))
    adapter = _build_adapter(
        benchmark=args.benchmark,
        data=args.data,
        max_cases=args.max_cases,
        include_abstention=args.include_abstention,
    )
    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())
    results = _load_jsonl(args.results)

    analysis = analyze(
        benchmark=args.benchmark,
        observations=observations,
        queries=queries,
        gold_lookup={query.query_id: adapter.gold(query.query_id) for query in queries},
        results=results,
        evidence_k=args.evidence_k,
        oracle_ks=oracle_ks,
        max_examples=args.examples,
    )
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "retrieval_ceiling_analysis.json").write_text(
        json.dumps(analysis, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (args.out / "retrieval_ceiling_analysis.md").write_text(
        render_markdown(analysis),
        encoding="utf-8",
    )
    print(f"Wrote {args.out / 'retrieval_ceiling_analysis.md'}")
    print(
        "Final recall "
        f"{analysis['final']['mean_recall_at_k']:.4f}; "
        "BM25 oracle@"
        f"{analysis['oracle']['ks'][-1]} "
        f"{analysis['oracle']['recall_by_k'][str(analysis['oracle']['ks'][-1])]:.4f}"
    )
    return 0


def analyze(
    *,
    benchmark: str,
    observations: list[BenchmarkObservation],
    queries: list[BenchmarkQuery],
    gold_lookup: dict[str, GoldLabels],
    results: list[dict[str, Any]],
    evidence_k: int,
    oracle_ks: list[int],
    max_examples: int,
) -> dict[str, Any]:
    observations_by_tenant: dict[str, list[BenchmarkObservation]] = defaultdict(list)
    observations_by_id: dict[str, BenchmarkObservation] = {}
    for observation in observations:
        observations_by_tenant[observation.tenant_id].append(observation)
        observations_by_id[observation.observation_id] = observation

    result_by_query = {str(result["query_id"]): result for result in results}
    per_query: list[dict[str, Any]] = []
    final_recalls: list[float] = []
    final_hits = 0
    zero_hit_queries = 0
    miss_examples: list[dict[str, Any]] = []
    miss_buckets: dict[str, int] = defaultdict(int)
    oracle_hits_by_k = {k: 0.0 for k in oracle_ks}
    oracle_recall_sum_by_k = {k: 0.0 for k in oracle_ks}
    bm25_rank_values: list[int] = []

    for query in queries:
        result = result_by_query.get(query.query_id)
        if result is None:
            continue
        gold = gold_lookup[query.query_id]
        gold_ids = list(gold.evidence_ids)
        retrieved_ids = list(result.get("debug", {}).get("retrieved_evidence_ids") or [])
        if not gold_ids:
            continue

        final_recall = _recall(retrieved_ids[:evidence_k], gold_ids)
        final_recalls.append(final_recall)
        if final_recall >= 1.0:
            final_hits += 1
        if final_recall <= 0.0:
            zero_hit_queries += 1

        bm25_ranked = _rank_bm25(
            query=query,
            observations=observations_by_tenant.get(query.tenant_id, []),
        )
        bm25_rank_by_id = {
            item.observation.observation_id: item.rank
            for item in bm25_ranked
        }
        bm25_score_by_id = {
            item.observation.observation_id: item.score
            for item in bm25_ranked
        }
        gold_bm25_ranks = [
            bm25_rank_by_id[gold_id]
            for gold_id in gold_ids
            if gold_id in bm25_rank_by_id
        ]
        if gold_bm25_ranks:
            bm25_rank_values.append(min(gold_bm25_ranks))

        for k in oracle_ks:
            bm25_ids_at_k = [item.observation.observation_id for item in bm25_ranked[:k]]
            oracle_recall = _recall(bm25_ids_at_k, gold_ids)
            oracle_recall_sum_by_k[k] += oracle_recall
            if oracle_recall >= 1.0:
                oracle_hits_by_k[k] += 1.0

        bucket = "hit"
        if final_recall < 1.0:
            max_oracle_k = max(oracle_ks)
            missing_gold_ids = [
                gold_id
                for gold_id in gold_ids
                if gold_id not in set(retrieved_ids[:evidence_k])
            ]
            missing_bm25_ranks = [
                bm25_rank_by_id[gold_id]
                for gold_id in missing_gold_ids
                if gold_id in bm25_rank_by_id
            ]
            if not missing_bm25_ranks:
                bucket = "missing_gold_not_in_bm25_candidates"
            elif min(missing_bm25_ranks) > max_oracle_k:
                bucket = f"missing_gold_bm25_rank_gt_{max_oracle_k}"
            elif min(missing_bm25_ranks) > evidence_k:
                bucket = "missing_gold_bm25_below_final_k"
            else:
                bucket = "missing_gold_bm25_top_k_but_not_final"
            miss_buckets[bucket] += 1
            if len(miss_examples) < max_examples:
                miss_examples.append(
                    _miss_example(
                        query=query,
                        gold=gold,
                        retrieved_ids=retrieved_ids,
                        bm25_ranked=bm25_ranked,
                        bm25_rank_by_id=bm25_rank_by_id,
                        bm25_score_by_id=bm25_score_by_id,
                        observations_by_id=observations_by_id,
                        bucket=bucket,
                    )
                )

        per_query.append({
            "query_id": query.query_id,
            "query_type": query.query_type,
            "final_recall": final_recall,
            "gold_ids": gold_ids,
            "retrieved_ids": retrieved_ids[:evidence_k],
            "gold_bm25_best_rank": min(gold_bm25_ranks) if gold_bm25_ranks else None,
            "gold_bm25_ranks": gold_bm25_ranks,
            "bucket": bucket,
        })

    query_count = len(final_recalls)
    recall_by_k = {
        str(k): oracle_recall_sum_by_k[k] / query_count if query_count else 0.0
        for k in oracle_ks
    }
    hit_rate_by_k = {
        str(k): oracle_hits_by_k[k] / query_count if query_count else 0.0
        for k in oracle_ks
    }
    return {
        "benchmark": benchmark,
        "queries": query_count,
        "evidence_k": evidence_k,
        "final": {
            "mean_recall_at_k": sum(final_recalls) / query_count if query_count else 0.0,
            "full_hit_rate_at_k": final_hits / query_count if query_count else 0.0,
            "not_full_hit_queries": query_count - final_hits,
            "zero_hit_queries": zero_hit_queries,
        },
        "oracle": {
            "type": "bm25_over_adapter_observations",
            "ks": oracle_ks,
            "recall_by_k": recall_by_k,
            "full_hit_rate_by_k": hit_rate_by_k,
        },
        "miss_buckets": dict(sorted(miss_buckets.items())),
        "gold_bm25_rank_stats": _rank_stats(bm25_rank_values),
        "miss_examples": miss_examples,
        "per_query": per_query,
    }


def _build_adapter(
    *,
    benchmark: str,
    data: Path,
    max_cases: int | None,
    include_abstention: bool,
):
    if benchmark == "longmemeval":
        return LongMemEvalAdapter(
            data,
            max_cases=max_cases,
            include_abstention=include_abstention,
        )
    if benchmark == "hotpotqa":
        return HotpotQAAdapter(data, max_cases=max_cases)
    if benchmark == "halumem":
        return HaluMemAdapter(data, max_users=max_cases)
    raise ValueError(f"unsupported benchmark={benchmark!r}")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _rank_bm25(
    *,
    query: BenchmarkQuery,
    observations: list[BenchmarkObservation],
    k1: float = 1.5,
    b: float = 0.75,
) -> list[RankedObservation]:
    doc_counts = [token_counts(observation.content) for observation in observations]
    doc_lengths = [sum(counts.values()) for counts in doc_counts]
    avg_doc_length = (sum(doc_lengths) / len(doc_lengths)) if doc_lengths else 0.0
    query_terms = token_counts(query.query_text)
    doc_freqs = _document_frequencies(doc_counts, query_terms)
    rows: list[tuple[float, BenchmarkObservation]] = []
    total_docs = len(observations)
    for observation, counts, doc_length in zip(observations, doc_counts, doc_lengths):
        score = 0.0
        for term, query_count in query_terms.items():
            term_frequency = counts.get(term, 0)
            if term_frequency <= 0:
                continue
            score += query_count * _bm25_term_score(
                term_frequency=term_frequency,
                doc_frequency=doc_freqs.get(term, 0),
                total_docs=total_docs,
                doc_length=doc_length,
                avg_doc_length=avg_doc_length,
                k1=k1,
                b=b,
            )
        if score > 0:
            rows.append((score, observation))
    rows.sort(key=lambda item: item[0], reverse=True)
    return [
        RankedObservation(rank=index, score=round(score, 6), observation=observation)
        for index, (score, observation) in enumerate(rows, start=1)
    ]


def _recall(retrieved_ids: Iterable[str], gold_ids: list[str]) -> float:
    if not gold_ids:
        return 0.0
    return len(set(retrieved_ids) & set(gold_ids)) / len(set(gold_ids))


def _rank_stats(ranks: list[int]) -> dict[str, float | int | None]:
    if not ranks:
        return {
            "count": 0,
            "min": None,
            "p50": None,
            "p90": None,
            "max": None,
            "mean": None,
        }
    ordered = sorted(ranks)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": _percentile(ordered, 0.50),
        "p90": _percentile(ordered, 0.90),
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def _percentile(ordered: list[int], q: float) -> float:
    if not ordered:
        return 0.0
    index = (len(ordered) - 1) * q
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(ordered[lower])
    weight = index - lower
    return (ordered[lower] * (1 - weight)) + (ordered[upper] * weight)


def _miss_example(
    *,
    query: BenchmarkQuery,
    gold: GoldLabels,
    retrieved_ids: list[str],
    bm25_ranked: list[RankedObservation],
    bm25_rank_by_id: dict[str, int],
    bm25_score_by_id: dict[str, float],
    observations_by_id: dict[str, BenchmarkObservation],
    bucket: str,
) -> dict[str, Any]:
    gold_ids = list(gold.evidence_ids)
    return {
        "query_id": query.query_id,
        "question": query.query_text,
        "query_type": query.query_type,
        "gold_answer": gold.answer,
        "bucket": bucket,
        "gold_ids": gold_ids,
        "retrieved_ids": retrieved_ids,
        "gold_bm25_ranks": {
            gold_id: bm25_rank_by_id.get(gold_id)
            for gold_id in gold_ids
        },
        "gold_bm25_scores": {
            gold_id: bm25_score_by_id.get(gold_id)
            for gold_id in gold_ids
        },
        "gold_metadata": {
            gold_id: observations_by_id[gold_id].metadata
            for gold_id in gold_ids
            if gold_id in observations_by_id
        },
        "gold_content_preview": {
            gold_id: _preview(observations_by_id[gold_id].content)
            for gold_id in gold_ids
            if gold_id in observations_by_id
        },
        "top_bm25": [
            {
                "rank": item.rank,
                "score": item.score,
                "observation_id": item.observation.observation_id,
                "metadata": item.observation.metadata,
            }
            for item in bm25_ranked[:10]
        ],
    }


def _preview(text: str, *, limit: int = 360) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def render_markdown(analysis: dict[str, Any]) -> str:
    oracle = analysis["oracle"]
    final = analysis["final"]
    lines = [
        "# Retrieval Ceiling Analysis",
        "",
        "## Summary",
        "",
        f"- Benchmark: {analysis['benchmark']}",
        f"- Queries: {analysis['queries']}",
        f"- Final recall@{analysis['evidence_k']}: {final['mean_recall_at_k']:.4f}",
        f"- Final full-hit rate@{analysis['evidence_k']}: {final['full_hit_rate_at_k']:.4f}",
        f"- Not-full-hit queries: {final['not_full_hit_queries']}",
        f"- Zero-hit queries: {final['zero_hit_queries']}",
        "",
        "## BM25 Oracle",
        "",
        "| K | Recall | Full-hit rate |",
        "| ---: | ---: | ---: |",
    ]
    for k in oracle["ks"]:
        lines.append(
            f"| {k} | {oracle['recall_by_k'][str(k)]:.4f} | "
            f"{oracle['full_hit_rate_by_k'][str(k)]:.4f} |"
        )
    lines.extend([
        "",
        "## Miss Buckets",
        "",
        "| Bucket | Count |",
        "| --- | ---: |",
    ])
    for bucket, count in analysis["miss_buckets"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend([
        "",
        "## Gold BM25 Rank Stats",
        "",
    ])
    stats = analysis["gold_bm25_rank_stats"]
    lines.extend([
        f"- Count: {stats['count']}",
        f"- Min: {stats['min']}",
        f"- P50: {stats['p50']}",
        f"- P90: {stats['p90']}",
        f"- Max: {stats['max']}",
        f"- Mean: {stats['mean']}",
        "",
        "## Miss Examples",
        "",
    ])
    for example in analysis["miss_examples"]:
        lines.extend([
            f"### {example['query_id']}",
            "",
            f"- Bucket: {example['bucket']}",
            f"- Question: {example['question']}",
            f"- Gold answer: {example['gold_answer']}",
            f"- Gold IDs: `{', '.join(example['gold_ids'])}`",
            f"- Retrieved IDs: `{', '.join(example['retrieved_ids'])}`",
            f"- Gold BM25 ranks: `{json.dumps(example['gold_bm25_ranks'], sort_keys=True)}`",
            "",
        ])
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
