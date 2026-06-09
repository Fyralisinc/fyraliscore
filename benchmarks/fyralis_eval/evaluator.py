"""Evaluation result model and scoring logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from benchmarks.adapters.base import BenchmarkQuery, GoldLabels
from benchmarks.fyralis_eval.metrics import (
    exact_match,
    longmemeval_v2_score,
    precision_at_k,
    recall_at_k,
    token_f1,
)
from benchmarks.fyralis_eval.reader import RetrievalOutput

try:
    from benchmarks.adapters.memtrack_adapter import answer_support_score
except Exception:  # pragma: no cover - keeps non-MEMTRACK imports resilient.
    answer_support_score = None


@dataclass(frozen=True)
class EvaluationResult:
    query_id: str
    system_name: str
    benchmark: str
    answer: str
    metrics: dict[str, float | int | None]
    debug: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "system_name": self.system_name,
            "benchmark": self.benchmark,
            "answer": self.answer,
            "metrics": self.metrics,
            "debug": self.debug,
        }


def evaluate_answer(
    *,
    benchmark: str,
    system_name: str,
    query: BenchmarkQuery,
    gold: GoldLabels,
    answer: str,
    retrieval: RetrievalOutput,
    evidence_k: int = 10,
    score_answer: bool = True,
) -> EvaluationResult:
    retrieved_ids = [
        item.observation_id
        for item in retrieval.retrieved_evidence
    ]
    em = exact_match(answer, gold.answer) if score_answer else None
    abstained = answer.strip().casefold() in {
        "i don't know",
        "unknown",
        "insufficient evidence",
    }
    lme_v2_score = (
        longmemeval_v2_score(
            answer,
            gold.answer,
            str(query.metadata.get("eval_function") or ""),
        )
        if benchmark == "longmemeval_v2" and score_answer
        else None
    )
    lme_v2_packet_answer_support = (
        longmemeval_v2_score(
            _packet_evidence_text(retrieval),
            gold.answer,
            str(query.metadata.get("eval_function") or ""),
        )
        if benchmark == "longmemeval_v2"
        else None
    )
    memtrack_support_score = (
        answer_support_score(_packet_evidence_text(retrieval), gold.answer)
        if benchmark == "memtrack" and answer_support_score is not None
        else None
    )
    memtrack_support_threshold = _float_metadata(
        gold.metadata.get("support_threshold"), default=0.75,
    )
    external_tool_required = bool(
        query.metadata.get("requires_external_tool_surface")
        or gold.metadata.get("requires_external_tool_surface")
    )
    external_tool_not_materialized = any(
        item.get("reason") == "external_tool_surface_not_materialized"
        for item in retrieval.omission_ledger
        if isinstance(item, dict)
    )
    metrics: dict[str, float | int | None] = {
        "exact_match": em,
        "f1": token_f1(answer, gold.answer) if score_answer else None,
        "accuracy": em,
        "longmemeval_v2_accuracy": lme_v2_score,
        "longmemeval_v2_packet_answer_support": lme_v2_packet_answer_support,
        f"memtrack_answer_support_at_{evidence_k}": (
            float(memtrack_support_score >= memtrack_support_threshold)
            if memtrack_support_score is not None
            else None
        ),
        f"memtrack_answer_support_score_at_{evidence_k}": memtrack_support_score,
        "memtrack_answer_observable": (
            1.0 if gold.metadata.get("gold_answer_observable") else 0.0
        ) if benchmark == "memtrack" else None,
        "memtrack_answer_single_observation_observable": (
            1.0
            if gold.metadata.get("gold_answer_single_observation_observable")
            else 0.0
        ) if benchmark == "memtrack" else None,
        "memtrack_answer_case_support_score": (
            _float_metadata(gold.metadata.get("gold_answer_case_support_score"), default=0.0)
        ) if benchmark == "memtrack" else None,
        "memtrack_external_tool_surface_required": (
            1.0 if external_tool_required else 0.0
        ) if benchmark == "memtrack" else None,
        "memtrack_external_tool_surface_missing": (
            1.0 if external_tool_required and external_tool_not_materialized else 0.0
        ) if benchmark == "memtrack" else None,
        f"evidence_recall_at_{evidence_k}": recall_at_k(
            retrieved_ids,
            gold.evidence_ids,
            k=evidence_k,
        ),
        f"evidence_precision_at_{evidence_k}": precision_at_k(
            retrieved_ids,
            gold.evidence_ids,
            k=evidence_k,
        ),
        "abstention_accuracy": (
            1.0 if abstained == gold.expected_abstain else 0.0
        ) if score_answer else None,
        "token_cost": retrieval.token_estimate,
        "latency_ms": retrieval.latency_ms,
        "retrieval_calls": retrieval.retrieval_calls,
    }
    return EvaluationResult(
        query_id=query.query_id,
        system_name=system_name,
        benchmark=benchmark,
        answer=answer,
        metrics=metrics,
        debug={
            "packet_id": retrieval.packet_id,
            "retrieved_evidence_ids": retrieved_ids,
            "omission_ledger": retrieval.omission_ledger,
        },
    )


def _float_metadata(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _packet_evidence_text(retrieval: RetrievalOutput) -> str:
    evidence = retrieval.context_packet.get("evidence", [])
    if not isinstance(evidence, list):
        return ""
    return "\n".join(
        str(item.get("content", ""))
        for item in evidence
        if isinstance(item, dict)
    )
