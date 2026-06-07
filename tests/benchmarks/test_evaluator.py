from __future__ import annotations

from benchmarks.adapters.base import BenchmarkQuery, GoldLabels
from benchmarks.fyralis_eval.evaluator import evaluate_answer
from benchmarks.fyralis_eval.reader import RetrievalOutput


def test_memtrack_evaluator_reports_missing_external_tool_surface():
    query = BenchmarkQuery(
        query_id="q_repo",
        tenant_id="t1",
        query_text="Clone the repository and inspect the exact file.",
        metadata={
            "requires_external_tool_surface": True,
            "required_tool_surfaces": ["repository"],
        },
    )
    retrieval = RetrievalOutput(
        query_id=query.query_id,
        packet_id="packet_q_repo",
        retrieved_nodes=[],
        retrieved_evidence=[],
        context_packet={"evidence": []},
        omission_ledger=[
            {
                "reason": "external_tool_surface_not_materialized",
                "severity": "warning",
                "required_tool_surfaces": ["repository"],
            }
        ],
        token_estimate=1,
        latency_ms=0,
        retrieval_calls=1,
    )

    result = evaluate_answer(
        benchmark="memtrack",
        system_name="test",
        query=query,
        gold=GoldLabels(answer="src/app.py"),
        answer="I don't know",
        retrieval=retrieval,
    )

    assert result.metrics["memtrack_external_tool_surface_required"] == 1.0
    assert result.metrics["memtrack_external_tool_surface_missing"] == 1.0
