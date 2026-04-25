"""Production-grade LLM judge infrastructure for LSOB Layer 6 (Phase 2.3).

This subpackage contains the machinery for running pairwise reference-vs-SUT
judgements using either a mock or a real Anthropic-backed client. It is the
supported surface going forward. The top-level `lsob_evaluator_l6.judge`
module re-exports the same names for backwards compatibility.
"""

from __future__ import annotations

from lsob_evaluator_l6.llm_judge.cache import CachedJudge
from lsob_evaluator_l6.llm_judge.calibration import (
    CalibrationReport,
    cohens_kappa,
    load_calibration_fixtures,
    run_calibration,
)
from lsob_evaluator_l6.llm_judge.client import (
    ANTHROPIC_MODEL_ID,
    AnthropicJudge,
    JudgeConfig,
    JudgeResult,
    JudgeRunCost,
    LLMJudge,
    MockJudge,
    PairwiseOutcome,
)
from lsob_evaluator_l6.llm_judge.prompt_loader import (
    load_prompt_template,
    prompt_hash,
)
from lsob_evaluator_l6.llm_judge.rate_limit import TokenBucket

__all__ = [
    "ANTHROPIC_MODEL_ID",
    "AnthropicJudge",
    "CachedJudge",
    "CalibrationReport",
    "JudgeConfig",
    "JudgeResult",
    "JudgeRunCost",
    "LLMJudge",
    "MockJudge",
    "PairwiseOutcome",
    "TokenBucket",
    "cohens_kappa",
    "load_calibration_fixtures",
    "load_prompt_template",
    "prompt_hash",
    "run_calibration",
]
