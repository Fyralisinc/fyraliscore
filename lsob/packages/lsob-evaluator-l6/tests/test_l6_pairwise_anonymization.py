"""Ensure pairwise anonymisation randomises reference/SUT ordering roughly 50/50."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from lsob_contracts import ClaimOp, DiffOp, Trigger

from lsob_evaluator_l6.judge import LLMJudge

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _simple_diff(name: str) -> DiffOp:
    return DiffOp(
        diff_id=name,
        produced_at=_NOW,
        claim_ops=[
            ClaimOp(
                claim_id=f"c-{name}",
                proposition=f"p-{name}",
                proposition_kind="k",
                asserted_confidence=0.5,
                entities=["commitment:A"],
            )
        ],
    )


def test_pairwise_ordering_is_approximately_uniform():
    trigger = Trigger(
        trigger_id="t-1",
        kind="test",
        payload={},
        timestamp=_NOW,
    )
    reference = _simple_diff("ref")
    sut = _simple_diff("sut")

    async def _run_trials() -> list[str]:
        judge = LLMJudge(seed=1234)
        orderings: list[str] = []
        for _ in range(100):
            outcome = await judge.compare(trigger, reference, sut)
            orderings.append(outcome.ordering)
        return orderings

    orderings = asyncio.run(_run_trials())
    ref_first = orderings.count("ref_first")
    sut_first = orderings.count("sut_first")
    assert ref_first + sut_first == 100
    # ±10% tolerance around 50/50.
    assert 40 <= ref_first <= 60, (ref_first, sut_first)
    assert 40 <= sut_first <= 60, (ref_first, sut_first)
