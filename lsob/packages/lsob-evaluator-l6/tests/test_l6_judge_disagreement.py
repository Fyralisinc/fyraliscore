"""Triple-judgment flags `low_confidence` when three votes fully disagree."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lsob_contracts import ClaimOp, DiffOp, Trigger

from lsob_evaluator_l6.llm_judge import JudgeConfig, LLMJudge

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _trigger() -> Trigger:
    return Trigger(trigger_id="t-dis", kind="test", payload={}, timestamp=_NOW)


def _diff(name: str) -> DiffOp:
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


class _RotatingJudge:
    """Returns A, B, tie on successive calls so the triple-vote disagrees."""

    name = "rotating"

    def __init__(self, sequence: list[str]) -> None:
        self._seq = sequence
        self._i = 0

    async def judge(self, prompt: str) -> dict[str, Any]:  # noqa: ARG002
        winner = self._seq[self._i % len(self._seq)]
        self._i += 1
        return {
            "scores_a": {"scope": 3, "reasoning": 3, "completeness": 3, "fabrication": 3},
            "scores_b": {"scope": 3, "reasoning": 3, "completeness": 3, "fabrication": 3},
            "winner": winner,
            "rationale": f"rotating-{winner}",
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }


class _MajorityJudge:
    """Returns A, A, B -> clear 2-1 majority for A."""

    name = "majority"

    def __init__(self) -> None:
        self._seq = ["A", "A", "B"]
        self._i = 0

    async def judge(self, prompt: str) -> dict[str, Any]:  # noqa: ARG002
        winner = self._seq[self._i % len(self._seq)]
        self._i += 1
        return {
            "scores_a": {"scope": 4, "reasoning": 4, "completeness": 4, "fabrication": 4},
            "scores_b": {"scope": 2, "reasoning": 2, "completeness": 2, "fabrication": 2},
            "winner": winner,
            "rationale": "majority",
            "usage": {"input_tokens": 20, "output_tokens": 10},
        }


def test_disagreement_flags_low_confidence(tmp_path: Path):
    queue = tmp_path / "review" / "queue.jsonl"
    config = JudgeConfig(human_review_queue_path=queue)
    judge = LLMJudge(
        judge_client=_RotatingJudge(["A", "B", "tie"]),
        seed=0,
        config=config,
    )

    result = asyncio.run(judge.compare(_trigger(), _diff("ref"), _diff("sut")))
    assert result.low_confidence is True
    assert len(set(result.raw_votes)) == 3

    # The review queue has exactly one line for this comparison.
    assert queue.exists()
    lines = queue.read_text().strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["trigger_id"] == "t-dis"
    assert record["raw_votes"] == result.raw_votes
    assert record["reference_diff_id"] == "ref"
    assert record["sut_diff_id"] == "sut"
    assert record["prompt_hash"] == result.prompt_hash


def test_majority_vote_not_flagged(tmp_path: Path):
    queue = tmp_path / "review.jsonl"
    config = JudgeConfig(human_review_queue_path=queue)
    judge = LLMJudge(judge_client=_MajorityJudge(), seed=1, config=config)
    result = asyncio.run(judge.compare(_trigger(), _diff("ref"), _diff("sut")))
    assert result.low_confidence is False
    # A 2-1 majority for A: resolves to whichever side A mapped to.
    assert result.winner in {"reference", "sut"}
    # No review queue entry was written.
    assert not queue.exists()


def test_disagreement_queue_is_append_only(tmp_path: Path):
    queue = tmp_path / "queue.jsonl"
    config = JudgeConfig(human_review_queue_path=queue)
    judge = LLMJudge(
        judge_client=_RotatingJudge(["A", "B", "tie"]),
        seed=42,
        config=config,
    )
    asyncio.run(judge.compare(_trigger(), _diff("ref"), _diff("sut")))
    asyncio.run(judge.compare(_trigger(), _diff("ref2"), _diff("sut2")))
    lines = queue.read_text().strip().splitlines()
    assert len(lines) == 2
