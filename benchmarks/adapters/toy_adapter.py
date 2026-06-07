"""Small built-in benchmark that exercises the full harness path."""

from __future__ import annotations

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
    observed_at,
)


class ToyMemoryAdapter(BenchmarkAdapter):
    """A deterministic memory/retrieval toy dataset for CI smoke coverage."""

    benchmark_name = "toy_memory"

    def __init__(self) -> None:
        tenant = "bench_toy_case_001"
        self._observations = [
            BenchmarkObservation(
                observation_id="toy_obs_001",
                source="benchmark_toy",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 1, 3, 9),
                content="Mira told support she is allergic to walnuts.",
                entities=[{"type": "person", "name": "Mira"}],
                metadata={"ability": "information_extraction"},
            ),
            BenchmarkObservation(
                observation_id="toy_obs_002",
                source="benchmark_toy",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 1, 4, 9),
                content="Mira used to prefer coffee during onboarding.",
                entities=[{"type": "person", "name": "Mira"}],
                metadata={"ability": "knowledge_update", "stale": True},
            ),
            BenchmarkObservation(
                observation_id="toy_obs_003",
                source="benchmark_toy",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 3, 10, 9),
                content="As of March, Mira drinks tea instead of coffee.",
                entities=[{"type": "person", "name": "Mira"}],
                metadata={"ability": "knowledge_update"},
            ),
        ]
        self._queries = [
            BenchmarkQuery(
                query_id="toy_q_001",
                tenant_id=tenant,
                query_text="What allergy did Mira mention?",
                gold_answer="walnuts",
                gold_evidence_ids=["toy_obs_001"],
                metadata={"ability": "information_extraction"},
            ),
            BenchmarkQuery(
                query_id="toy_q_002",
                tenant_id=tenant,
                query_text="What is Mira's current favorite drink?",
                gold_answer="tea",
                gold_evidence_ids=["toy_obs_003"],
                metadata={"ability": "knowledge_update"},
            ),
            BenchmarkQuery(
                query_id="toy_q_003",
                tenant_id=tenant,
                query_text="What is Mira's passport number?",
                gold_answer="I don't know",
                metadata={"ability": "abstention", "expected_abstain": True},
            ),
        ]
        self._gold = {
            q.query_id: GoldLabels(
                answer=q.gold_answer,
                evidence_ids=q.gold_evidence_ids,
                expected_abstain=bool(q.metadata.get("expected_abstain")),
                metadata=q.metadata,
            )
            for q in self._queries
        }

    def iter_observations(self):
        yield from self._observations

    def iter_queries(self):
        yield from self._queries

    def gold(self, query_id: str) -> GoldLabels:
        return self._gold[query_id]

