from __future__ import annotations

import json

from benchmarks.adapters import BenchmarkObservation, BenchmarkQuery, ToyMemoryAdapter
from benchmarks.adapters.base import observed_at
from benchmarks.adapters.truss_adapter import TrussAdapter
from benchmarks.fyralis_eval.ingestion import InMemoryBenchmarkStore
from benchmarks.fyralis_eval.reporting import write_run_artifacts
from benchmarks.fyralis_eval.reader import (
    BM25MemoryReader,
    LexicalMemoryReader,
    token_counts,
    tokenize,
)
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_toy_benchmark_runs_end_to_end(tmp_path):
    adapter = ToyMemoryAdapter()
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(benchmark=adapter.benchmark_name),
    )

    assert result.observations_ingested == 3
    assert result.metrics_summary["queries"] == 3
    assert result.metrics_summary["accuracy"] == 1.0
    assert result.metrics_summary["abstention_accuracy"] == 1.0
    assert result.metrics_summary["evidence_recall_at_10"] == 1.0
    assert result.metrics_summary["evidence_precision_at_10"] == 1.0
    assert [item.answer for item in result.results] == [
        "walnuts",
        "tea",
        "I don't know",
    ]
    assert [
        item.debug["retrieved_evidence_ids"]
        for item in result.results
    ] == [
        ["toy_obs_001"],
        ["toy_obs_003"],
        [],
    ]

    artifacts = write_run_artifacts(
        output_dir=tmp_path,
        run_config=result.config.to_json(),
        results=result.results,
        retrieval_traces=result.retrieval_traces,
        metrics_summary=result.metrics_summary,
    )

    assert artifacts.results_jsonl.exists()
    assert artifacts.metrics_summary_json.exists()
    assert artifacts.run_config_json.exists()
    assert artifacts.trace_sample_jsonl.exists()
    assert artifacts.benchmark_report_md.exists()

    metrics = json.loads(artifacts.metrics_summary_json.read_text())
    assert metrics["accuracy"] == 1.0
    assert metrics["evidence_precision_at_10"] == 1.0
    assert "Fyralis Benchmark Report" in artifacts.benchmark_report_md.read_text()


def test_adapter_shapes_are_json_serializable():
    adapter = ToyMemoryAdapter()
    observation = next(adapter.iter_observations())
    query = next(adapter.iter_queries())

    assert isinstance(observation, BenchmarkObservation)
    assert isinstance(query, BenchmarkQuery)
    json.dumps(observation.to_json())
    json.dumps(query.to_json())
    json.dumps(adapter.gold(query.query_id).to_json())


def test_truss_adapter_loads_committed_fixture_and_frozen_facts():
    adapter = TrussAdapter(
        ".",
        fact_filter_path="benchmarks/truss_signal_derivable_facts.json",
    )

    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(observations) == 983
    assert len(queries) >= 10
    assert all(obs.trust_tier in {"authoritative", "reputable", "inferential", "unvetted"} for obs in observations)
    assert any(query.metadata["requires_run1_memory"] for query in queries)
    assert adapter.gold(queries[0].query_id).evidence_ids


def test_lexical_reader_filters_adjacent_stale_state_evidence():
    adapter = ToyMemoryAdapter()
    store = InMemoryBenchmarkStore()
    store.ingest(adapter.iter_observations())
    reader = LexicalMemoryReader(store, top_k=5)

    evidence, _latency_ms, _calls = reader.retrieve(next(
        query
        for query in adapter.iter_queries()
        if query.query_id == "toy_q_002"
    ))

    assert [item.observation_id for item in evidence] == ["toy_obs_003"]


def test_lexical_tokenization_normalizes_possessive_entities():
    assert "mira" in tokenize("What is Mira's current favorite drink?")
    assert token_counts("What is Mira's current favorite drink?")["mira"] == 1


def test_bm25_reader_promotes_structured_sort_state_over_boilerplate():
    store = InMemoryBenchmarkStore()
    store.ingest([
        BenchmarkObservation(
            observation_id="obs_boilerplate",
            source="unit",
            tenant_id="t1",
            occurred_at=observed_at(2026, 1, 1, 9),
            content=(
                "Before selecting the target field, the sort row instructions "
                "explain how default sorting works."
            ),
        ),
        BenchmarkObservation(
            observation_id="obs_sort_state",
            source="unit",
            tenant_id="t1",
            occurred_at=observed_at(2026, 1, 1, 10),
            content="All Assets list state with an active sort row.",
            metadata={
                "sort_fields": ["Acquisition method"],
                "structured_ui_facts": [
                    (
                        "editable form fields: Order results by the following "
                        "fields. Acquisition method"
                    )
                ],
            },
        ),
    ])
    reader = BM25MemoryReader(store, top_k=2)

    evidence, _latency_ms, _calls = reader.retrieve(BenchmarkQuery(
        query_id="q_sort",
        tenant_id="t1",
        query_text=(
            "Before selecting the target field, what default sort field is "
            "initially shown in the sort row?"
        ),
    ))

    assert evidence[0].observation_id == "obs_sort_state"


def test_bm25_reader_diversifies_compare_queries_across_named_options():
    store = InMemoryBenchmarkStore()
    store.ingest([
        _comparison_observation("obs_dev_a", "Developer Laptop (Mac)", repetitions=8),
        _comparison_observation("obs_dev_b", "Developer Laptop (Mac)", repetitions=7),
        _comparison_observation("obs_sales", "Sales Laptop", repetitions=5),
        _comparison_observation("obs_standard", "Standard Laptop", repetitions=2),
    ])
    reader = BM25MemoryReader(store, top_k=3)

    evidence, _latency_ms, _calls = reader.retrieve(BenchmarkQuery(
        query_id="q_compare",
        tenant_id="t1",
        query_text=(
            "Comparing the order pages for the `Standard Laptop`, "
            "`Developer Laptop (Mac)`, and `Sales Laptop`, which page has "
            "the largest number of optional software checkbox choices?"
        ),
    ))

    retrieved_text = "\n".join(item.content for item in evidence)
    assert "Standard Laptop" in retrieved_text
    assert "Developer Laptop (Mac)" in retrieved_text
    assert "Sales Laptop" in retrieved_text


def _comparison_observation(
    observation_id: str,
    product_name: str,
    *,
    repetitions: int,
) -> BenchmarkObservation:
    return BenchmarkObservation(
        observation_id=observation_id,
        source="unit",
        tenant_id="t1",
        occurred_at=observed_at(2026, 1, 2, 9),
        content=(
            f"{product_name} order page optional software checkbox choices. "
            * repetitions
        ),
        metadata={
            "structured_ui_facts": [
                "checkbox choice group visible: count=3; choices=A; B; C"
            ]
        },
    )
