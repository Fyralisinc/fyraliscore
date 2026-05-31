"""Opt-in 100-signal real-LLM model-layer E2E.

This test wraps scripts/run_100_signal_real_llm_e2e.py so the expensive
signal -> ingestion -> retrieval -> Think -> model mutation probe can be run
from pytest when we want CI-style pass/fail semantics.
"""
from __future__ import annotations

import os

import pytest

from tests.real_llm.infrastructure.real_llm_runner import real_llm_test


RUN_100_SIGNAL_MODEL_E2E = os.environ.get("RUN_100_SIGNAL_MODEL_E2E") == "1"


@pytest.mark.asyncio
@pytest.mark.skipif(
    not RUN_100_SIGNAL_MODEL_E2E,
    reason="set RUN_100_SIGNAL_MODEL_E2E=1 to run the 100-signal live E2E",
)
@real_llm_test(
    attempts=1,
    pass_threshold=1,
    timeout_seconds=int(os.environ.get("RUN_100_SIGNAL_MODEL_E2E_TIMEOUT", "9000")),
    tags=["signal-to-model", "retrieval", "e2e"],
)
async def test_100_signal_real_llm_signal_to_model_e2e() -> None:
    from scripts.run_100_signal_real_llm_e2e import ProbeConfig, run_probe

    signals = int(os.environ.get("RUN_100_SIGNAL_MODEL_E2E_SIGNALS", "100"))
    min_effect_cases = int(
        os.environ.get("RUN_100_SIGNAL_MODEL_E2E_MIN_EFFECT_CASES", "70")
    )
    summary = await run_probe(
        ProbeConfig(
            signals=signals,
            think_timeout=int(
                os.environ.get("RUN_100_SIGNAL_MODEL_E2E_THINK_TIMEOUT", "7200")
            ),
            post_commit_timeout=int(
                os.environ.get(
                    "RUN_100_SIGNAL_MODEL_E2E_POST_COMMIT_TIMEOUT",
                    "900",
                )
            ),
            min_model_effect_cases=min_effect_cases,
            max_median_retrieved_models=int(
                os.environ.get(
                    "RUN_100_SIGNAL_MODEL_E2E_MAX_MEDIAN_RETRIEVED_MODELS",
                    "60",
                )
            ),
            run_id=os.environ.get("RUN_100_SIGNAL_MODEL_E2E_RUN_ID"),
        )
    )

    case_summary = summary["case_summary"]
    assert case_summary["cases_total"] == signals
    assert case_summary["trigger_completed_cases"] == signals
    assert case_summary["failed_case_count"] == 0
    assert case_summary["model_effect_cases"] >= min_effect_cases
    assert case_summary["retrieval_efficiency"]["passes"] is True
