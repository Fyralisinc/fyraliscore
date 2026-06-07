from __future__ import annotations

import json

from benchmarks.adapters.halumem_adapter import HaluMemAdapter
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_halumem_adapter_maps_memory_qa_shape(tmp_path):
    data_path = _write_halumem_fixture(tmp_path)
    adapter = HaluMemAdapter(data_path)

    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(observations) == 2
    assert len(queries) == 2
    assert queries[0].gold_evidence_ids == [
        "halumem:user-1:session:0:memory:1"
    ]
    assert queries[1].metadata["expected_abstain"] is True


def test_halumem_bm25_retrieval_smoke(tmp_path):
    data_path = _write_halumem_fixture(tmp_path)
    adapter = HaluMemAdapter(data_path)
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=1,
            evidence_k=1,
            score_answers=False,
        ),
    )

    assert result.metrics_summary["queries"] == 2
    assert result.results[0].metrics["evidence_recall_at_1"] == 1.0


def _write_halumem_fixture(tmp_path):
    path = tmp_path / "halumem.jsonl"
    record = {
        "uuid": "user-1",
        "sessions": [
            {
                "start_time": "Sep 04, 2025, 18:42:18",
                "end_time": "Sep 04, 2025, 21:12:18",
                "memory_points": [
                    {
                        "index": 1,
                        "memory_content": "Martin Mark's birth date is 1996-08-02",
                        "memory_type": "Persona Memory",
                    },
                    {
                        "index": 2,
                        "memory_content": "Martin Mark lives in Columbus",
                        "memory_type": "Persona Memory",
                    },
                ],
                "questions": [
                    {
                        "question": "What is Martin Mark's birth date?",
                        "answer": "1996-08-02",
                        "evidence": [
                            {
                                "memory_content": "Martin Mark's birth date is 1996-08-02",
                                "memory_type": "Persona Memory",
                            }
                        ],
                        "difficulty": "easy",
                        "question_type": "Basic Fact Recall",
                    },
                    {
                        "question": "What is Martin Mark's middle name?",
                        "answer": "Unknown; not provided by the user.",
                        "evidence": [],
                        "difficulty": "easy",
                        "question_type": "Memory Boundary",
                    },
                ],
            }
        ],
    }
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return path
