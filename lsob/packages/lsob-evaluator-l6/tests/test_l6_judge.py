"""Tests for the MockJudge and prompt hashing."""

from __future__ import annotations

import asyncio

from lsob_evaluator_l6.judge import (
    LLMJudge,
    MockJudge,
    load_prompt_template,
    prompt_hash,
)


def test_mock_judge_deterministic():
    mj = MockJudge()
    prompt = "hello world"
    out1 = asyncio.run(mj.judge(prompt))
    out2 = asyncio.run(mj.judge(prompt))
    assert out1 == out2
    assert out1["winner"] in {"A", "B", "tie"}
    assert set(out1["scores_a"].keys()) == {
        "scope",
        "reasoning",
        "completeness",
        "fabrication",
    }


def test_mock_judge_different_prompts_can_differ():
    mj = MockJudge()
    # Use prompts chosen so their SHA-256 digests give different score sums.
    r_a = asyncio.run(mj.judge("prompt-a"))
    r_b = asyncio.run(mj.judge("prompt-xyz"))
    assert r_a["scores_a"] != r_b["scores_a"] or r_a["scores_b"] != r_b["scores_b"]


def test_prompt_hash_stable_across_calls():
    h1 = prompt_hash()
    h2 = prompt_hash()
    assert h1 == h2
    assert len(h1) == 64


def test_prompt_hash_matches_file_content():
    template = load_prompt_template()
    assert prompt_hash(template) == prompt_hash()


def test_llm_judge_prompt_hash_exposed():
    j = LLMJudge()
    assert j.prompt_hash == prompt_hash()
