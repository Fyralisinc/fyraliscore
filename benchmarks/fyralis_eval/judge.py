"""Optional LLM answer judges for end-to-end benchmark correctness."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field

from benchmarks.adapters.base import BenchmarkQuery
from lib.llm.provider import LLMProvider, LLMUsageAggregator, build_provider


@dataclass(frozen=True)
class JudgeResult:
    correct: bool
    score: float
    rationale: str
    metadata: dict[str, Any] = field(default_factory=dict)


class _JudgeOutput(BaseModel):
    correct: bool = Field(
        description="Whether the prediction should receive full benchmark credit."
    )
    score: float = Field(
        ge=0.0,
        le=1.0,
        description="1.0 for correct, 0.0 for incorrect; use partial only if explicitly warranted.",
    )
    rationale: str = Field(
        min_length=1,
        max_length=800,
        description="Brief reason for the judgment.",
    )


class LLMAnswerJudge:
    """Conservative final-answer judge.

    MEMTRACK's public configs request an LLM judge in addition to exact
    matching. This judge intentionally sees only the question, expected
    answer, and predicted answer. It does not see hidden labels, retrieved
    evidence, or benchmark internals beyond the public expected answer.
    """

    def __init__(
        self,
        *,
        provider: LLMProvider | None = None,
        name: str = "llm",
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> None:
        self.provider = provider or build_provider()
        self.name = name
        self.temperature = temperature
        self.max_tokens = max_tokens

    def judge(
        self,
        *,
        query: BenchmarkQuery,
        expected_answer: str | None,
        predicted_answer: str,
    ) -> JudgeResult:
        return _run_coro_sync(
            self.judge_async(
                query=query,
                expected_answer=expected_answer,
                predicted_answer=predicted_answer,
            )
        )

    async def judge_async(
        self,
        *,
        query: BenchmarkQuery,
        expected_answer: str | None,
        predicted_answer: str,
    ) -> JudgeResult:
        if expected_answer is None:
            return JudgeResult(
                correct=False,
                score=0.0,
                rationale="No expected answer was provided.",
                metadata={"judge": self.name, "skipped": True},
            )
        usage = LLMUsageAggregator()
        self.provider.set_usage_aggregator(usage)
        try:
            output = await self.provider.structured(
                system=_JUDGE_SYSTEM_PROMPT,
                user=_judge_user_prompt(
                    query=query,
                    expected_answer=expected_answer,
                    predicted_answer=predicted_answer,
                ),
                schema=_JudgeOutput,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
        finally:
            self.provider.set_usage_aggregator(None)
        score = 1.0 if output.correct else 0.0
        # Keep the binary correctness faithful even if a provider returns a
        # fuzzy score. The fuzzy value is still useful in debug metadata.
        return JudgeResult(
            correct=output.correct,
            score=score,
            rationale=output.rationale,
            metadata={
                "judge": self.name,
                "provider": self.provider.config.provider,
                "model": self.provider.config.model,
                "raw_score": output.score,
                "llm": {
                    "calls": usage.call_count,
                    "input_tokens": usage.total_input_tokens,
                    "output_tokens": usage.total_output_tokens,
                    "cost_usd": usage.total_cost_usd,
                },
            },
        )


class CodexAnswerJudge(LLMAnswerJudge):
    """Answer judge pinned to the local Codex provider/auth path."""

    def __init__(self, *, temperature: float = 0.0, max_tokens: int = 512) -> None:
        super().__init__(
            provider=_build_codex_provider(),
            name="codex",
            temperature=temperature,
            max_tokens=max_tokens,
        )


_JUDGE_SYSTEM_PROMPT = """You are an impartial benchmark answer judge.

Judge whether the predicted answer should receive credit for the expected
answer. Be strict about requested entity types, numbers, dates, code symbols,
file names, commit hashes, and required output formats.

Allow harmless wording differences when the prediction fully expresses the same
answer. Do not award credit for merely related context, partial evidence, or an
answer that omits a required component.

Return JSON only.
"""


def _judge_user_prompt(
    *,
    query: BenchmarkQuery,
    expected_answer: str,
    predicted_answer: str,
) -> str:
    return "\n\n".join([
        f"Question:\n{query.query_text}",
        f"Expected answer:\n{expected_answer}",
        f"Predicted answer:\n{predicted_answer}",
        "Return whether the predicted answer is correct for this benchmark question.",
    ])


def _build_codex_provider() -> LLMProvider:
    previous = os.environ.get("LLM_PROVIDER")
    os.environ["LLM_PROVIDER"] = "codex"
    try:
        return build_provider()
    finally:
        if previous is None:
            os.environ.pop("LLM_PROVIDER", None)
        else:
            os.environ["LLM_PROVIDER"] = previous


def _run_coro_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError(
        "LLMAnswerJudge.judge cannot be called from an active event loop; "
        "use judge_async instead."
    )


__all__ = [
    "CodexAnswerJudge",
    "JudgeResult",
    "LLMAnswerJudge",
]
