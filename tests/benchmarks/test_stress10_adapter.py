from __future__ import annotations

from benchmarks.adapters import BenchmarkQuery, Stress10Adapter
from benchmarks.fyralis_eval.packet_compiler import ContextPacketCompiler
from benchmarks.fyralis_eval.ingestion import InMemoryBenchmarkStore
from benchmarks.fyralis_eval.reader import BM25MemoryReader, RetrievedEvidence
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


EXPECTED_STRESS_AXES = {
    "dense_haystack",
    "temporal_update",
    "multi_hop_bridge",
    "contradiction",
    "dynamic_state_transition",
    "structured_ui_gating",
    "tenant_isolation",
    "abstention",
    "packet_needle",
}


def test_stress10_adapter_contains_ten_diverse_end_to_end_cases() -> None:
    adapter = Stress10Adapter()
    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(observations) == 13
    assert len(queries) == 10
    assert {query.metadata["stress_axis"] for query in queries} == EXPECTED_STRESS_AXES
    assert all(adapter.gold(query.query_id).answer for query in queries)
    assert all(isinstance(query, BenchmarkQuery) for query in queries)


def test_stress10_bm25_run_surfaces_retrieval_pressure_points() -> None:
    adapter = Stress10Adapter()
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=5,
            evidence_k=5,
            score_answers=False,
        ),
    )

    assert result.observations_ingested == 13
    assert result.metrics_summary["queries"] == 10
    assert result.metrics_summary["evidence_recall_at_5"] >= 0.80
    assert result.metrics_summary["latency_ms"] < 50
    assert result.metrics_summary["token_cost"] < 1800

    by_id = {item.query_id: item for item in result.results}
    assert by_id["stress_q_001_dense_haystack"].metrics["evidence_recall_at_5"] == 1.0
    assert by_id["stress_q_008_tenant_isolation"].debug["retrieved_evidence_ids"][0] == (
        "stress_obs_010_access_allowed"
    )
    assert "stress_obs_011_access_forbidden" not in by_id[
        "stress_q_008_tenant_isolation"
    ].debug["retrieved_evidence_ids"]

    assert by_id["stress_q_003_multi_hop"].metrics["evidence_recall_at_5"] == 1.0

    assert by_id["stress_q_002_temporal_update"].debug["retrieved_evidence_ids"] == [
        "stress_obs_003_temporal_current"
    ]
    assert result.metrics_summary["evidence_recall_at_5"] == 1.0
    assert result.metrics_summary["evidence_precision_at_5"] == 1.0


def test_stress10_dynamic_and_structured_packets_keep_the_right_state() -> None:
    adapter = Stress10Adapter()
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=5,
            evidence_k=5,
            score_answers=False,
        ),
    )
    traces = {trace["query_id"]: trace for trace in result.retrieval_traces}

    dynamic_packet = _packet_text(traces["stress_q_005_dynamic_ui"])
    assert "Priority" in dynamic_packet
    assert "Escalation" in dynamic_packet
    assert "Newly visible after action" in dynamic_packet

    popup_packet = _packet_text(traces["stress_q_006_structured_popup"])
    assert "Recent selections" in popup_packet
    assert "autocomplete popup title" in popup_packet
    assert "table summary row" not in popup_packet

    ordinary_packet = _packet_text(traces["stress_q_007_structured_non_leakage"])
    assert "Recent selections" not in ordinary_packet
    assert "table summary row" not in ordinary_packet

    needle_packet = _packet_text(traces["stress_q_010_packet_needle"])
    assert "ORCHID-17" in needle_packet
    assert "routine planning" not in needle_packet[:200]


def test_stress10_tenant_filtering_holds_under_direct_reader_use() -> None:
    adapter = Stress10Adapter()
    store = InMemoryBenchmarkStore()
    store.ingest(adapter.iter_observations())
    reader = BM25MemoryReader(store, top_k=10)

    query = BenchmarkQuery(
        query_id="stress_direct_other_tenant",
        tenant_id="bench_stress_other_tenant",
        query_text="What is this tenant's renewal code?",
    )
    evidence, _latency_ms, _calls = reader.retrieve(query)

    assert [item.observation_id for item in evidence] == ["stress_obs_011_access_forbidden"]
    assert "MAPLE-99" in evidence[0].content


def test_stress10_packet_compiler_clips_dense_evidence_around_query_terms() -> None:
    adapter = Stress10Adapter()
    target = next(
        observation
        for observation in adapter.iter_observations()
        if observation.observation_id == "stress_obs_001_dense_target"
    )
    compiler = ContextPacketCompiler(max_chars_per_evidence=500)
    retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="stress_direct_dense",
            tenant_id=target.tenant_id,
            query_text="Who owns Q4 reliability?",
        ),
        [
            RetrievedEvidence(
                observation_id=target.observation_id,
                content=target.content,
                score=1.0,
                occurred_at=target.occurred_at.isoformat(),
                metadata=target.metadata,
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    content = retrieval.context_packet["evidence"][0]["content"]
    assert "Priya Nair" in content
    assert len(content) <= 500


def _packet_text(trace: dict) -> str:
    evidence = trace["context_packet"]["evidence"]
    return "\n".join(item["content"] for item in evidence)
