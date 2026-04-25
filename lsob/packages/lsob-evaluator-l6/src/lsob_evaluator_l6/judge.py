"""Backwards-compatibility shim for the pre-Phase-2.3 judge module.

The real implementation now lives under `lsob_evaluator_l6.llm_judge.*`.
Everything here simply re-exports the public names so existing imports
(`from lsob_evaluator_l6.judge import LLMJudge, MockJudge`) keep working.
"""

from __future__ import annotations

from lsob_evaluator_l6.llm_judge.cache import CachedJudge
from lsob_evaluator_l6.llm_judge.client import (
    ANTHROPIC_MODEL_ID,
    AnthropicJudge,
    JudgeClient,
    JudgeConfig,
    JudgeResult,
    JudgeRunCost,
    LLMJudge,
    MockJudge,
    PairwiseOutcome,
    parse_judge_json,
)
from lsob_evaluator_l6.llm_judge.prompt_loader import (
    load_prompt_template,
    prompt_hash,
)

__all__ = [
    "ANTHROPIC_MODEL_ID",
    "AnthropicJudge",
    "CachedJudge",
    "JudgeClient",
    "JudgeConfig",
    "JudgeResult",
    "JudgeRunCost",
    "LLMJudge",
    "MockJudge",
    "PairwiseOutcome",
    "load_prompt_template",
    "parse_judge_json",
    "prompt_hash",
]
