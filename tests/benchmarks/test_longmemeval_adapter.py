from __future__ import annotations

import json

from benchmarks.adapters.longmemeval_adapter import LongMemEvalAdapter
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_longmemeval_adapter_maps_public_shape(tmp_path):
    data_path = _write_longmemeval_fixture(tmp_path)
    adapter = LongMemEvalAdapter(data_path, include_abstention=True)

    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(observations) == 3
    assert len(queries) == 2
    assert queries[0].query_id == "lme_001"
    assert queries[0].gold_evidence_ids == ["longmemeval:lme_001:session:s2"]
    assert "user: Sam said the launch code is blue" in observations[1].content
    assert "[has_answer]" not in observations[1].content
    assert observations[1].metadata["has_answer_turn"] is True

    gold = adapter.gold("lme_001")
    assert gold.answer == "blue"
    assert gold.evidence_ids == ["longmemeval:lme_001:session:s2"]


def test_longmemeval_can_skip_abstention_cases(tmp_path):
    data_path = _write_longmemeval_fixture(tmp_path)
    adapter = LongMemEvalAdapter(data_path, include_abstention=False)

    queries = list(adapter.iter_queries())

    assert [query.query_id for query in queries] == ["lme_001"]


def test_longmemeval_bm25_retrieval_smoke(tmp_path):
    data_path = _write_longmemeval_fixture(tmp_path)
    adapter = LongMemEvalAdapter(data_path, include_abstention=False)

    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=1,
            score_answers=False,
        ),
    )

    assert result.metrics_summary["queries"] == 1
    assert result.metrics_summary["evidence_recall_at_10"] == 1.0
    assert result.metrics_summary["evidence_precision_at_10"] == 1.0
    assert result.metrics_summary["accuracy"] is None
    assert result.results[0].debug["retrieved_evidence_ids"] == [
        "longmemeval:lme_001:session:s2"
    ]


def _write_longmemeval_fixture(tmp_path):
    data = [
        {
            "question_id": "lme_001",
            "question_type": "single-session-user",
            "question": "What color launch code did Sam mention?",
            "answer": "blue",
            "question_date": "2026-01-03",
            "haystack_session_ids": ["s1", "s2"],
            "haystack_dates": ["2026-01-01", "2026-01-02"],
            "haystack_sessions": [
                [
                    {"role": "user", "content": "Sam discussed lunch plans."},
                    {"role": "assistant", "content": "Noted."},
                ],
                [
                    {
                        "role": "user",
                        "content": "Sam said the launch code is blue.",
                        "has_answer": True,
                    },
                    {"role": "assistant", "content": "I will remember that."},
                ],
            ],
            "answer_session_ids": ["s2"],
        },
        {
            "question_id": "lme_002_abs",
            "question_type": "single-session-user",
            "question": "What passport number did Sam mention?",
            "answer": "I don't know",
            "question_date": "2026-01-03",
            "haystack_session_ids": ["s3"],
            "haystack_dates": ["2026-01-03"],
            "haystack_sessions": [
                [{"role": "user", "content": "Sam did not discuss passports."}]
            ],
            "answer_session_ids": [],
        },
    ]
    path = tmp_path / "longmemeval_sample.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
