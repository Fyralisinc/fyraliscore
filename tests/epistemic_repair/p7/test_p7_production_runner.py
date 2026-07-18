from __future__ import annotations

from uuid import uuid4

import pytest

from lib.evaluation.epistemic_repair.p7_production_runner import (
    P7_ARMS,
    _run_id,
    assess_provider_identity_receipts,
    seal_execution_stream,
)
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.shared.errors import InvariantViolation


def test_p7_arm_set_is_preregistered_and_complete() -> None:
    assert P7_ARMS == (
        "adaptive",
        "frozen",
        "observation_only",
        "memory_hidden",
        "corrupted",
    )


def test_sealed_execution_stream_exposes_no_gold_or_semantic_oracle() -> None:
    stream = seal_execution_stream(build_p6_population())
    assert not hasattr(stream, "gold")
    assert not hasattr(stream, "thesis_by_storyline")
    assert len(stream.batches) == 12


def test_run_id_requires_successful_durable_production_run() -> None:
    run_id = uuid4()
    assert _run_id({"run": {"id": str(run_id), "status": "success"}}) == run_id
    with pytest.raises(InvariantViolation, match="successful durable Think run"):
        _run_id({"run": {"id": str(run_id), "status": "failed"}})
    with pytest.raises(InvariantViolation, match="successful durable Think run"):
        _run_id({"run": None})


def test_receipt_guard_rejects_injected_spark_subprovider() -> None:
    logical = [{
        "logical_call_id": "main",
        "provider": "codex",
        "model": "gpt-5.4",
        "purpose": "think",
        "physical_attempt_count": 1,
    }, {
        "logical_call_id": "planner",
        "provider": "codex",
        "model": "gpt-5.3-codex-spark",
        "purpose": "inquiry_question_planning",
        "physical_attempt_count": 1,
    }]
    attempts = [{
        "physical_attempt_id": "a-main",
        "logical_call_id": "main",
        "provider": "codex",
        "model": "gpt-5.4",
        "purpose": "think",
    }, {
        "physical_attempt_id": "a-spark",
        "logical_call_id": "planner",
        "provider": "codex",
        "model": "gpt-5.3-codex-spark",
        "purpose": "inquiry_question_planning",
    }]
    result = assess_provider_identity_receipts(
        logical_receipts=logical,
        attempt_receipts=attempts,
        required_provider="codex",
        required_model="gpt-5.4",
    )
    assert not result["valid"]
    assert result["identity_mismatches"] == [{
        "receipt_kind": "logical",
        "receipt_id": "planner",
        "provider": "codex",
        "model": "gpt-5.3-codex-spark",
        "purpose": "inquiry_question_planning",
    }, {
        "receipt_kind": "physical_attempt",
        "receipt_id": "a-spark",
        "provider": "codex",
        "model": "gpt-5.3-codex-spark",
        "purpose": "inquiry_question_planning",
    }]


def test_receipt_guard_fails_when_identity_or_attempt_receipt_missing() -> None:
    result = assess_provider_identity_receipts(
        logical_receipts=[{
            "logical_call_id": "x",
            "provider": "codex",
            "model": "",
            "purpose": "think",
            "physical_attempt_count": 1,
        }],
        attempt_receipts=[],
        required_provider="codex",
        required_model="gpt-5.4",
    )
    assert not result["valid"]
    assert result["missing_identity_receipts"] == ["logical:x"]


@pytest.mark.parametrize("usage_exactness", ("estimated", "unavailable"))
def test_receipt_guard_rejects_nonreported_codex_usage(
    usage_exactness: str,
) -> None:
    result = assess_provider_identity_receipts(
        logical_receipts=[{
            "logical_call_id": "x", "provider": "codex", "model": "gpt-5.4",
            "purpose": "think", "physical_attempt_count": 1,
        }],
        attempt_receipts=[{
            "physical_attempt_id": "a", "logical_call_id": "x",
            "provider": "codex", "model": "gpt-5.4", "purpose": "think",
            "usage_exactness": usage_exactness,
        }],
        required_provider="codex", required_model="gpt-5.4",
    )
    assert not result["valid"]
    assert result["nonreported_usage_attempts"] == ["a"]


def test_receipt_guard_accepts_provider_reported_usage() -> None:
    result = assess_provider_identity_receipts(
        logical_receipts=[{
            "logical_call_id": "x", "provider": "codex", "model": "gpt-5.4",
            "purpose": "think", "physical_attempt_count": 1,
        }],
        attempt_receipts=[{
            "physical_attempt_id": "a", "logical_call_id": "x",
            "provider": "codex", "model": "gpt-5.4", "purpose": "think",
            "usage_exactness": "reported",
        }],
        required_provider="codex", required_model="gpt-5.4",
    )
    assert result["valid"]
