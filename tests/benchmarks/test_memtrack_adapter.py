from __future__ import annotations

import json

import yaml

from benchmarks.adapters.base import BenchmarkObservation, BenchmarkQuery, observed_at
from benchmarks.adapters.memtrack_adapter import MemTrackAdapter, answer_support_score
from benchmarks.fyralis_eval.fyralis_db import (
    _benchmark_temporal_relation,
    _pack_query_evidence,
    _with_query_chain_candidate,
)
from benchmarks.fyralis_eval.reader import RetrievedEvidence
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_memtrack_adapter_loads_public_archive_shape_without_answer_leak(tmp_path):
    root = tmp_path / "Memtrak"
    config_dir = root / "test_configs"
    history_dir = root / "test_event_histories"
    config_dir.mkdir(parents=True)
    history_dir.mkdir(parents=True)

    config = {
        "repository": {"name": "demo_repo", "url": "https://example.invalid/demo"},
        "linear": {
            "teams": [{"name": "engineering", "members": ["alice", "bob"]}],
        },
            "benchmark": {
                "event_history": "test_event_histories/event_history_demo.json",
                "questions": [
                    "Clone the repository and examine the source: which cache file caused the renewal risk?",
                ],
                "expected_answers": [
                    "2-hour cache TTL broke real-time inventory visibility",
                ],
        },
    }
    (config_dir / "config_demo.yaml").write_text(
        yaml.safe_dump(config),
        encoding="utf-8",
    )
    events = [
        {
            "timestamp": "20241202T0900",
            "platform": "linear",
            "generation_type": "manual",
            "generation_meta_data": {
                "title": "Performance work",
                "description": "Dashboard load times are slow.",
                "team": "engineering",
                "lead": "alice",
            },
        },
        {
            "timestamp": "20241203T1030",
            "platform": "slack",
            "generation_type": "manual",
            "generation_meta_data": {
                "sender": "bob",
                "channel": "engineering",
                "message": (
                    "Postmortem found the 2-hour cache TTL broke real-time "
                    "inventory visibility during flash sales."
                ),
            },
        },
    ]
    (history_dir / "event_history_demo.json").write_text(
        json.dumps(events),
        encoding="utf-8",
    )

    adapter = MemTrackAdapter(root)
    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())
    gold = adapter.gold("demo:q1")

    assert len(observations) == 3
    assert len(queries) == 1
    assert queries[0].gold_answer == "2-hour cache TTL broke real-time inventory visibility"
    assert queries[0].metadata["gold_answer_observable"] is True
    assert queries[0].metadata["requires_external_tool_surface"] is True
    assert "repository" in queries[0].metadata["required_tool_surfaces"]
    assert gold.evidence_ids == ["memtrack:demo:event:0001"]
    assert "expected_answers" not in "\n".join(observation.content for observation in observations)


def test_memtrack_answer_support_handles_short_and_long_answers():
    assert answer_support_score("Merge commit: fc3f4b67d65c575daa661ecf31cf59b4ff3cced5", "fc3f4b67d65c575daa661ecf31cf59b4ff3cced5") == 1.0
    assert answer_support_score(
        "Alice proposed mandatory real-time use case review for caching changes.",
        "mandatory real-time use case review for caching changes",
    ) >= 0.75
    assert answer_support_score("Alice discussed a different issue.", "Mark Otto") == 0.0


def test_memtrack_case_observable_allows_distributed_support(tmp_path):
    root = tmp_path / "Memtrak"
    config_dir = root / "test_configs"
    history_dir = root / "test_event_histories"
    config_dir.mkdir(parents=True)
    history_dir.mkdir(parents=True)
    (config_dir / "config_demo.yaml").write_text(
        yaml.safe_dump({
            "repository": {"name": "demo_repo"},
            "benchmark": {
                "event_history": "test_event_histories/event_history_demo.json",
                "questions": ["What was the full recovery policy?"],
                "expected_answers": [
                    "cache TTL rollback plus mandatory real-time inventory review",
                ],
            },
        }),
        encoding="utf-8",
    )
    (history_dir / "event_history_demo.json").write_text(
        json.dumps([
            {
                "timestamp": "20241202T0900",
                "platform": "slack",
                "generation_type": "manual",
                "generation_meta_data": {
                    "message": "Alice approved a cache TTL rollback.",
                },
            },
            {
                "timestamp": "20241203T0900",
                "platform": "slack",
                "generation_type": "manual",
                "generation_meta_data": {
                    "message": "Bob added mandatory real-time inventory review.",
                },
            },
        ]),
        encoding="utf-8",
    )

    adapter = MemTrackAdapter(root)
    query = next(iter(adapter.iter_queries()))
    gold = adapter.gold(query.query_id)

    assert query.metadata["gold_answer_observable"] is True
    assert query.metadata["gold_answer_single_observation_observable"] is False
    assert gold.evidence_ids == []


def test_memtrack_adapter_runs_through_benchmark_runner(tmp_path):
    root = tmp_path / "Memtrak"
    config_dir = root / "test_configs"
    history_dir = root / "test_event_histories"
    config_dir.mkdir(parents=True)
    history_dir.mkdir(parents=True)
    (config_dir / "config_demo.yaml").write_text(
        yaml.safe_dump({
            "repository": {"name": "demo_repo"},
            "benchmark": {
                "event_history": "test_event_histories/event_history_demo.json",
                "questions": ["What did Alice approve?"],
                "expected_answers": ["dark mode rollout"],
            },
        }),
        encoding="utf-8",
    )
    (history_dir / "event_history_demo.json").write_text(
        json.dumps([
            {
                "timestamp": "20241202T0900",
                "platform": "slack",
                "generation_type": "manual",
                "generation_meta_data": {
                    "sender": "alice",
                    "message": "Alice approved the dark mode rollout.",
                },
            }
        ]),
        encoding="utf-8",
    )

    adapter = MemTrackAdapter(root)
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=2,
            evidence_k=2,
            score_answers=False,
        ),
    )

    assert result.metrics_summary["queries"] == 1
    assert result.metrics_summary["memtrack_answer_support_at_2"] == 1.0


def test_query_local_chain_composition_uses_only_retrieved_sources():
    query = BenchmarkQuery(
        query_id="demo:q1",
        tenant_id="memtrack:demo",
        query_text="Why did the cache workaround trigger the customer-visible outage?",
        query_type="causal_state_tracking",
        metadata={"benchmark": "MEMTRACK", "case_id": "demo"},
    )
    evidence = [
        RetrievedEvidence(
            observation_id="memtrack:demo:event:0007",
            content="Event index: 7\nBob assumed database queries were slow and chose a cache workaround.",
            score=0.82,
            occurred_at="2024-12-02T09:00:00+00:00",
            metadata={
                "observation_kind": "timeline_event",
                "event_index": 7,
                "sender": "bob",
            },
        ),
        RetrievedEvidence(
            observation_id="memtrack:demo:event:0012",
            content="Event index: 12\nThe cache workaround caused stale inventory during flash sales.",
            score=0.91,
            occurred_at="2024-12-03T09:00:00+00:00",
            metadata={
                "observation_kind": "timeline_event",
                "event_index": 12,
                "sender": "alice",
            },
        ),
    ]

    composed = _with_query_chain_candidate(query, evidence)

    assert composed[0].metadata["derived_kind"] == "query_chain"
    assert composed[0].metadata["source_observation_ids"] == [
        "memtrack:demo:event:0007",
        "memtrack:demo:event:0012",
    ]
    assert "Bob assumed database queries" in composed[0].content
    assert "caused stale inventory" in composed[0].content


def test_role_packing_prefers_contemporaneous_mental_model_over_hindsight():
    query = BenchmarkQuery(
        query_id="demo:q2",
        tenant_id="memtrack:demo",
        query_text="During Bob's work, what was his mental model of why the API was slow?",
        query_type="causal_state_tracking",
    )
    hindsight = RetrievedEvidence(
        observation_id="memtrack:demo:event:0030",
        content="Postmortem identified the root cause as cache invalidation drift.",
        score=1.00,
        occurred_at="2024-12-10T09:00:00+00:00",
        metadata={"observation_kind": "timeline_event", "event_index": 30},
    )
    contemporaneous = RetrievedEvidence(
        observation_id="memtrack:demo:event:0005",
        content=(
            "Bob assumed the slow API responses came from inefficient database "
            "queries and decided to implement aggressive caching."
        ),
        score=0.82,
        occurred_at="2024-12-02T09:00:00+00:00",
        metadata={
            "observation_kind": "timeline_event",
            "event_index": 5,
            "sender": "bob",
        },
    )

    packed = _pack_query_evidence(query, [hindsight, contemporaneous], top_k=1)

    assert packed[0].observation_id == "memtrack:demo:event:0005"


def test_temporal_relation_edges_are_inferred_from_public_event_text_only():
    left = BenchmarkObservation(
        observation_id="memtrack:demo:event:0001",
        source="benchmark_memtrack_slack",
        tenant_id="memtrack:demo",
        occurred_at=observed_at(2024, 12, 1, 9),
        content="Alice noticed dashboard timeouts after the cache rollout.",
        metadata={
            "observation_kind": "timeline_event",
            "event_index": 1,
            "title": "Cache rollout",
            "lead": "alice",
        },
    )
    right = BenchmarkObservation(
        observation_id="memtrack:demo:event:0002",
        source="benchmark_memtrack_slack",
        tenant_id="memtrack:demo",
        occurred_at=observed_at(2024, 12, 1, 10),
        content="The failure happened because stale cache entries blocked fresh inventory.",
        metadata={
            "observation_kind": "timeline_event",
            "event_index": 2,
            "title": "Cache rollout",
            "lead": "alice",
        },
    )

    assert _benchmark_temporal_relation(left, right) == (
        "causes",
        0.74,
        "temporal_causal_marker",
    )
