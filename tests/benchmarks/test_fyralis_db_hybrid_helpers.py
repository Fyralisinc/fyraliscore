from __future__ import annotations

from benchmarks.adapters.base import BenchmarkQuery
from benchmarks.fyralis_eval.fyralis_db import _with_query_chain_candidate
from benchmarks.fyralis_eval.reader import RetrievedEvidence


def test_query_chain_keeps_decision_event_for_final_solution_question():
    query = BenchmarkQuery(
        query_id="q-final",
        tenant_id="tenant-a",
        query_text=(
            "After the rollback, what happened to auto-scaling, "
            "and what was the final solution?"
        ),
        query_type="timeline",
        metadata={"benchmark": "MEMTRACK", "case_id": "case-a"},
    )
    evidence = [
        _evidence(
            "tenant-a:event:0000",
            1.0,
            0,
            "Initial monitoring project started as the trigger for auto-scaling.",
        ),
        _evidence(
            "tenant-a:event:0011",
            0.95,
            11,
            "Impact assessment: auto-scaling depended on the rolled-back monitoring metrics.",
        ),
        _evidence(
            "tenant-a:event:0014",
            0.8,
            14,
            "DECISION: Split auto-scaling into a basic rule-based version and a future advanced version.",
        ),
    ]

    chained = _with_query_chain_candidate(query, evidence)

    assert chained[0].metadata["derived_kind"] == "query_chain"
    assert "tenant-a:event:0014" in chained[0].content
    assert "roles=decision" in chained[0].content


def _evidence(
    observation_id: str,
    score: float,
    event_index: int,
    content: str,
) -> RetrievedEvidence:
    return RetrievedEvidence(
        observation_id=observation_id,
        content=content,
        score=score,
        occurred_at=f"2025-01-{event_index + 1:02d}T09:00:00+00:00",
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": "case-a",
            "observation_kind": "timeline_event",
            "event_index": event_index,
            "timestamp_raw": f"202501{event_index + 1:02d}T0900",
            "platform": "linear",
        },
    )
