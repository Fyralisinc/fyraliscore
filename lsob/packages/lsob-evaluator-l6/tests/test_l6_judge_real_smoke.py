"""Smoke test against the real Anthropic API. Skipped unless explicitly opted in.

Enable with `LSOB_RUN_REAL_JUDGE=1` and a valid `ANTHROPIC_API_KEY`. CI never
runs this path. The test exists so maintainers can sanity-check the real
client against a single trivial pair before large calibration runs.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from lsob_contracts import ClaimOp, DiffOp, Trigger

from lsob_evaluator_l6.llm_judge import (
    AnthropicJudge,
    JudgeConfig,
    LLMJudge,
)


_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _diff(name: str) -> DiffOp:
    return DiffOp(
        diff_id=name,
        produced_at=_NOW,
        claim_ops=[
            ClaimOp(
                claim_id=f"c-{name}",
                proposition=f"p-{name}",
                proposition_kind="observation",
                asserted_confidence=0.5,
                entities=["commitment:A"],
            )
        ],
    )


@pytest.mark.skipif(
    os.environ.get("LSOB_RUN_REAL_JUDGE") != "1",
    reason="LSOB_RUN_REAL_JUDGE != 1 (gated to keep CI cost-free)",
)
def test_real_anthropic_judge_returns_structured_result():
    assert os.environ.get("ANTHROPIC_API_KEY"), (
        "ANTHROPIC_API_KEY is required for the real smoke test"
    )
    config = JudgeConfig()
    inner = AnthropicJudge(config=config)
    judge = LLMJudge(judge_client=inner, config=config)

    trig = Trigger(trigger_id="smoke", kind="test", payload={}, timestamp=_NOW)
    result = asyncio.run(judge.compare(trig, _diff("ref"), _diff("sut")))

    assert result.winner in {"reference", "sut", "tie"}
    assert len(result.raw_votes) == 3
    assert result.ordering in {"ref_first", "sut_first"}
    assert len(result.prompt_hash) == 64
    assert result.cost.n_calls == 3
    assert result.cost.input_tokens > 0
    assert result.cost.output_tokens > 0
    assert result.model.startswith("anthropic-judge:") or result.model == config.model
