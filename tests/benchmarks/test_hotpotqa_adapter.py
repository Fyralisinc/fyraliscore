from __future__ import annotations

import json

from benchmarks.adapters.hotpotqa_adapter import HotpotQAAdapter
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_hotpotqa_adapter_maps_official_shape(tmp_path):
    data_path = _write_hotpotqa_fixture(tmp_path)
    adapter = HotpotQAAdapter(data_path)

    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(observations) == 3
    assert len(queries) == 1
    assert queries[0].query_id == "hp_001"
    assert queries[0].gold_evidence_ids == [
        "hotpotqa:hp_001:paragraph:0:alpha_person",
        "hotpotqa:hp_001:paragraph:1:beta_band",
    ]
    assert observations[0].metadata["supporting_sentence_ids"] == [0]
    assert adapter.gold("hp_001").bridge_node_ids == ["Alpha Person", "Beta Band"]


def test_hotpotqa_bm25_retrieval_smoke(tmp_path):
    data_path = _write_hotpotqa_fixture(tmp_path)
    adapter = HotpotQAAdapter(data_path)

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
    assert result.metrics_summary["evidence_recall_at_2"] == 1.0
    assert result.metrics_summary["evidence_precision_at_2"] == 1.0


def _write_hotpotqa_fixture(tmp_path):
    data = [
        {
            "_id": "hp_001",
            "question": "What band did Alpha Person found?",
            "answer": "Beta Band",
            "type": "bridge",
            "level": "easy",
            "supporting_facts": [["Alpha Person", 0], ["Beta Band", 0]],
            "context": [
                [
                    "Alpha Person",
                    ["Alpha Person founded Beta Band in 1999."],
                ],
                [
                    "Beta Band",
                    ["Beta Band is a Scottish musical group."],
                ],
                [
                    "Gamma City",
                    ["Gamma City is unrelated to this question."],
                ],
            ],
        }
    ]
    path = tmp_path / "hotpotqa_sample.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
