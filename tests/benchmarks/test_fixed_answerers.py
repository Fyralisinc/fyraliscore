from __future__ import annotations

import json
from dataclasses import replace

from benchmarks.adapters.toy_adapter import ToyMemoryAdapter
from benchmarks.fyralis_eval.answerer import (
    LLMFixedAnswerer,
    PassthroughAnswerer,
    _answer_user_prompt,
)
from benchmarks.fyralis_eval.reader import RetrievalOutput, RetrievedEvidence
from benchmarks.fyralis_eval.judge import JudgeResult, LLMAnswerJudge
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark
from lib.llm.provider import LLMConfig, LLMProvider


class _StubProvider(LLMProvider):
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self._record_usage(10, 3)
        if "allergy" in user.casefold():
            answer = "walnuts"
        elif "favorite drink" in user.casefold():
            answer = "tea"
        else:
            answer = "I don't know"
        return json.dumps({
            "answer": answer,
            "confidence": 0.9,
            "supporting_evidence_ids": [],
        })


class _JudgeStubProvider(LLMProvider):
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self._record_usage(12, 4)
        correct = "walnuts" in user.casefold()
        return json.dumps({
            "correct": correct,
            "score": 1.0 if correct else 0.0,
            "rationale": "matches expected answer" if correct else "does not match",
        })


class _OverconfidentRepoProvider(LLMProvider):
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self._record_usage(10, 3)
        return json.dumps({
            "answer": "src/app.py",
            "confidence": 0.95,
            "supporting_evidence_ids": ["obs_breadcrumb"],
            "fulfilled_requirements": ["specificity"],
            "missing_requirements": [],
        })


def test_llm_fixed_answerer_with_stub_provider():
    provider = _StubProvider(
        LLMConfig(provider="openai", api_key="test", model="stub-model")
    )
    answerer = LLMFixedAnswerer(provider=provider)
    adapter = ToyMemoryAdapter()
    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            answerer_name="extractive",
            score_answers=True,
        ),
    )
    query = next(adapter.iter_queries())
    retrieval = result.retrieval_traces[0]
    # Use the concrete retrieval object from the normal path by rerunning via
    # the public answerer on the first result's packet shape.
    output = RetrievalOutput(
        query_id=query.query_id,
        packet_id=retrieval["packet_id"],
        retrieved_nodes=[],
        retrieved_evidence=[
            RetrievedEvidence(
                observation_id=item["observation_id"],
                content=item["content"],
                score=item["score"],
                occurred_at=item["occurred_at"],
                metadata=item["metadata"],
            )
            for item in retrieval["retrieved_evidence"]
        ],
        context_packet=retrieval["context_packet"],
        omission_ledger=[],
        token_estimate=retrieval["token_estimate"],
        latency_ms=retrieval["latency_ms"],
        retrieval_calls=retrieval["retrieval_calls"],
    )
    answer = answerer.answer_result(query, output)

    assert answer.answer == "walnuts"
    assert answer.metadata["llm"]["calls"] == 1


def test_passthrough_answerer_returns_product_answer_metadata():
    query = next(ToyMemoryAdapter().iter_queries())
    retrieval = RetrievalOutput(
        query_id=query.query_id,
        packet_id="packet_ask",
        retrieved_nodes=[],
        retrieved_evidence=[
            RetrievedEvidence(
                observation_id="obs_ask_1",
                content="Surfaced Ask evidence, not a generated answer.",
                score=1.0,
                occurred_at="",
                metadata={
                    "passthrough_answer": {
                        "answer": "Incident Mobile",
                        "confidence": 0.82,
                        "mode": "direct_synthesis_read",
                    }
                },
            )
        ],
        context_packet={"evidence": []},
        omission_ledger=[],
        token_estimate=1,
        latency_ms=0,
        retrieval_calls=1,
    )

    answer = PassthroughAnswerer().answer_result(query, retrieval)

    assert answer.answer == "Incident Mobile"
    assert answer.metadata["answerer"] == "passthrough"
    assert answer.metadata["confidence"] == 0.82


def test_llm_answer_judge_with_stub_provider():
    provider = _JudgeStubProvider(
        LLMConfig(provider="openai", api_key="test", model="stub-judge")
    )
    judge = LLMAnswerJudge(provider=provider)
    query = next(ToyMemoryAdapter().iter_queries())

    result = judge.judge(
        query=query,
        expected_answer="walnuts",
        predicted_answer="walnuts",
    )

    assert result.correct is True
    assert result.score == 1.0
    assert result.metadata["llm"]["calls"] == 1


def test_llm_answer_prompt_surfaces_structure_and_checklist():
    query = next(ToyMemoryAdapter().iter_queries())
    query = replace(
        query,
        query_text=(
            "After inspecting the repository code, what exact file and "
            "line number caused the issue?"
        ),
    )
    retrieval = RetrievalOutput(
        query_id=query.query_id,
        packet_id="packet_test",
        retrieved_nodes=[],
        retrieved_evidence=[],
        context_packet={
            "evidence": [
                {
                    "observation_id": "obs_1",
                    "content": "The source analysis found src/app.py line 42.",
                    "metadata": {
                        "event_index": 7,
                        "timestamp_raw": "20250101T0900",
                        "platform": "linear",
                        "lead": "alice",
                        "status": "done",
                        "title": "Repository investigation",
                    },
                }
            ]
        },
        omission_ledger=[],
        token_estimate=1,
        latency_ms=0,
        retrieval_calls=1,
    )

    prompt = _answer_user_prompt(query, retrieval, max_evidence_chars=500)

    assert "Structured fields: event_index=7" in prompt
    assert "Answer requirements:" in prompt
    assert "Apply the temporal constraint exactly." in prompt
    assert "external-tool questions" in prompt


def test_llm_answerer_forces_abstention_when_tool_surface_missing():
    provider = _OverconfidentRepoProvider(
        LLMConfig(provider="openai", api_key="test", model="stub-model")
    )
    answerer = LLMFixedAnswerer(provider=provider)
    query = replace(
        next(ToyMemoryAdapter().iter_queries()),
        query_text="Clone the repository and inspect the exact file.",
        metadata={
            "requires_external_tool_surface": True,
            "required_tool_surfaces": ["repository"],
        },
    )
    retrieval = RetrievalOutput(
        query_id=query.query_id,
        packet_id="packet_repo",
        retrieved_nodes=[],
        retrieved_evidence=[],
        context_packet={
            "evidence": [
                {
                    "observation_id": "obs_breadcrumb",
                    "content": "Someone said the repository needs inspection.",
                    "metadata": {"observation_kind": "timeline_event"},
                }
            ],
            "answer_requirements": [
                {
                    "kind": "external_tool_surface",
                    "description": "Requires a materialized repository result.",
                }
            ],
            "sufficiency": {
                "required_roles": [],
                "covered_roles": [],
                "missing_roles": [],
                "has_finality_evidence": True,
                "has_external_tool_result": False,
            },
        },
        omission_ledger=[
            {
                "reason": "external_tool_surface_not_materialized",
                "severity": "warning",
            }
        ],
        token_estimate=1,
        latency_ms=0,
        retrieval_calls=1,
    )

    answer = answerer.answer_result(query, retrieval)

    assert answer.answer == "I don't know"
    assert answer.metadata["forced_abstention"] == "external_tool_surface_not_materialized"


def test_benchmark_runner_attaches_judge_metrics(monkeypatch):
    class _FakeJudge:
        def judge(self, *, query, expected_answer, predicted_answer):
            correct = query.query_id == "toy_q_001"
            return JudgeResult(
                correct=correct,
                score=1.0 if correct else 0.0,
                rationale="scripted",
                metadata={
                    "judge": "fake",
                    "llm": {
                        "calls": 1,
                        "input_tokens": 2,
                        "output_tokens": 1,
                        "cost_usd": 0.0,
                    },
                },
            )

    monkeypatch.setattr("benchmarks.runners.core._build_judge", lambda name: _FakeJudge())
    adapter = ToyMemoryAdapter()

    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            answerer_name="extractive",
            judge_name="llm",
            score_answers=True,
        ),
    )

    assert result.metrics_summary["judge_correctness"] == 1 / 3
    assert result.metrics_summary["judge_llm_calls"] == 1.0
    assert result.results[0].debug["judge"]["rationale"] == "scripted"
