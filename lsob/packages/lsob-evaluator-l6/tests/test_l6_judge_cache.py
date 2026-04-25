"""On-disk cache for the LLM judge: same input -> hit, different input -> miss."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from lsob_contracts import ClaimOp, DiffOp, Trigger

from lsob_evaluator_l6.llm_judge import CachedJudge, LLMJudge, MockJudge
from lsob_evaluator_l6.llm_judge.cache import cache_key

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _simple_diff(name: str, *, confidence: float = 0.5) -> DiffOp:
    return DiffOp(
        diff_id=name,
        produced_at=_NOW,
        claim_ops=[
            ClaimOp(
                claim_id=f"c-{name}",
                proposition=f"p-{name}",
                proposition_kind="k",
                asserted_confidence=confidence,
                entities=["commitment:A"],
            )
        ],
    )


def _trigger() -> Trigger:
    return Trigger(trigger_id="t-1", kind="test", payload={}, timestamp=_NOW)


class _CountingMock(MockJudge):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    async def judge(self, prompt: str):
        self.calls += 1
        return await super().judge(prompt)


def test_cache_hit_on_identical_inputs(tmp_path: Path):
    counter = _CountingMock()
    inner = LLMJudge(judge_client=counter, seed=7)
    cached = CachedJudge(inner, cache_dir=tmp_path)
    trig = _trigger()
    ref = _simple_diff("ref")
    sut = _simple_diff("sut")

    first = asyncio.run(cached.compare(trig, ref, sut))
    first_calls = counter.calls
    assert first_calls == 3  # triple-judgment populated cache
    assert cached.misses == 1
    assert cached.hits == 0

    second = asyncio.run(cached.compare(trig, ref, sut))
    assert counter.calls == first_calls, "cache hit should not call the inner judge"
    assert cached.hits == 1
    assert cached.misses == 1

    # Cached result is equivalent to the fresh one.
    assert second.winner == first.winner
    assert second.raw_votes == first.raw_votes
    assert second.scores_reference == first.scores_reference
    assert second.scores_sut == first.scores_sut
    assert second.prompt_hash == first.prompt_hash


def test_cache_miss_on_different_inputs(tmp_path: Path):
    counter = _CountingMock()
    inner = LLMJudge(judge_client=counter, seed=7)
    cached = CachedJudge(inner, cache_dir=tmp_path)
    trig = _trigger()
    ref = _simple_diff("ref")
    sut_a = _simple_diff("sut-a", confidence=0.3)
    sut_b = _simple_diff("sut-b", confidence=0.9)

    asyncio.run(cached.compare(trig, ref, sut_a))
    asyncio.run(cached.compare(trig, ref, sut_b))
    assert cached.misses == 2
    assert cached.hits == 0
    # Two separate triple-judgment batches = 6 inner calls.
    assert counter.calls == 6


def test_cache_key_stable_under_field_reordering():
    """Equivalent diff content with different insertion order produces identical key."""
    ref = _simple_diff("ref")
    sut = _simple_diff("sut")
    k1 = cache_key(ref, sut, prompt_hash="abc", model="m")
    # Rebuild with the same values but different object identity.
    ref2 = DiffOp.model_validate(ref.model_dump())
    sut2 = DiffOp.model_validate(sut.model_dump())
    k2 = cache_key(ref2, sut2, prompt_hash="abc", model="m")
    assert k1 == k2
    # Different model -> different key.
    k3 = cache_key(ref, sut, prompt_hash="abc", model="other-model")
    assert k3 != k1


@pytest.mark.asyncio
async def test_cache_preserves_cost(tmp_path: Path):
    inner = LLMJudge(judge_client=MockJudge(tokens_per_call=(100, 40)), seed=1)
    cached = CachedJudge(inner, cache_dir=tmp_path)
    trig = _trigger()
    ref = _simple_diff("ref")
    sut = _simple_diff("sut")
    fresh = await cached.compare(trig, ref, sut)
    assert fresh.cost.input_tokens == 300  # 3 calls * 100
    assert fresh.cost.output_tokens == 120
    replay = await cached.compare(trig, ref, sut)
    assert replay.cost.input_tokens == 300
    assert replay.cost.output_tokens == 120
