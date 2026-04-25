"""Cost tracking aggregates across many mock judgments."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from lsob_contracts import ClaimOp, DiffOp, Trigger

from lsob_evaluator_l6.llm_judge import (
    JudgeConfig,
    JudgeRunCost,
    LLMJudge,
    MockJudge,
)

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _diff(n: int) -> DiffOp:
    return DiffOp(
        diff_id=f"d-{n:04d}",
        produced_at=_NOW,
        claim_ops=[
            ClaimOp(
                claim_id=f"c-{n:04d}",
                proposition=f"p-{n:04d}",
                proposition_kind="k",
                asserted_confidence=0.5,
                entities=["commitment:A"],
            )
        ],
    )


def _trigger(n: int) -> Trigger:
    return Trigger(
        trigger_id=f"t-{n:04d}", kind="test", payload={}, timestamp=_NOW
    )


def test_cost_accumulates_across_500_judgments():
    # Fixed token counts -> exact totals are easy to assert.
    input_per_call = 125
    output_per_call = 40
    n_pairs = 500
    # Sonnet-4.6 default pricing: $3 / Mtok input, $15 / Mtok output.
    config = JudgeConfig(
        input_price_per_mtok=3.0, output_price_per_mtok=15.0
    )
    judge = LLMJudge(
        judge_client=MockJudge(tokens_per_call=(input_per_call, output_per_call)),
        config=config,
    )

    async def _run() -> JudgeRunCost:
        for i in range(n_pairs):
            await judge.compare(_trigger(i), _diff(2 * i), _diff(2 * i + 1))
        return judge.cost

    cost = asyncio.run(_run())

    # 3 judgments per comparison -> 1500 total calls.
    expected_calls = 3 * n_pairs
    expected_input = expected_calls * input_per_call
    expected_output = expected_calls * output_per_call
    expected_usd = (
        expected_input * 3.0 / 1_000_000.0
        + expected_output * 15.0 / 1_000_000.0
    )
    assert cost.n_calls == expected_calls
    assert cost.input_tokens == expected_input
    assert cost.output_tokens == expected_output
    assert cost.estimated_usd == pytest.approx(expected_usd, rel=1e-9)


def test_cost_to_dict_is_json_safe():
    c = JudgeRunCost(input_tokens=10, output_tokens=5, estimated_usd=0.000075, n_calls=1)
    d = c.to_dict()
    assert d["input_tokens"] == 10
    assert d["output_tokens"] == 5
    assert d["n_calls"] == 1
    assert isinstance(d["estimated_usd"], float)


def test_cost_add_is_pure():
    a = JudgeRunCost(input_tokens=1, output_tokens=2, estimated_usd=0.1, n_calls=1)
    b = JudgeRunCost(input_tokens=3, output_tokens=4, estimated_usd=0.2, n_calls=1)
    c = a.add(b)
    assert (c.input_tokens, c.output_tokens, c.n_calls) == (4, 6, 2)
    assert c.estimated_usd == pytest.approx(0.3)
    # a, b unchanged.
    assert (a.input_tokens, a.output_tokens) == (1, 2)
    assert (b.input_tokens, b.output_tokens) == (3, 4)


def test_run_manifest_accepts_judge_cost():
    from datetime import datetime, timezone

    from lsob_contracts import AblationConfig, JudgeCost, RunManifest

    manifest = RunManifest(
        run_id="r",
        company="acme",
        months_simulated=1,
        baseline="b",
        ablation=AblationConfig(),
        seed=0,
        git_sha="deadbeef",
        started_at=datetime.now(timezone.utc),
        corpus_uri="mem://",
        layers=[6],
        judge_cost=JudgeCost(
            input_tokens=1000,
            output_tokens=200,
            estimated_usd=0.0063,
            n_calls=10,
        ),
    )
    assert manifest.judge_cost is not None
    assert manifest.judge_cost.n_calls == 10
    # Round-trip through JSON (pydantic) works.
    restored = RunManifest.model_validate_json(manifest.model_dump_json())
    assert restored.judge_cost == manifest.judge_cost
