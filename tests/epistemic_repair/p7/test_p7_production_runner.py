from __future__ import annotations

from uuid import uuid4

import pytest

from services.evaluation.epistemic_repair.p7_production_runner import (
    P7_ARMS,
    P7_ATTEMPT_TIMEOUT_S,
    P7_BATCH_DEADLINE_S,
    P7_MAX_ATTEMPTS,
    _arm_tenant_id,
    _canonical_counts_unchanged_after_bootstrap,
    _run_id,
    _validate_deadlines,
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


def test_timeout_then_success_has_two_exact_nonduplicate_attempt_receipts() -> None:
    logical = [{
        "logical_call_id": "logical-1", "provider": "codex", "model": "gpt-5.4",
        "purpose": "think", "physical_attempt_count": 2,
    }]
    attempts = [{
        "physical_attempt_id": "attempt-timeout", "logical_call_id": "logical-1",
        "provider": "codex", "model": "gpt-5.4", "purpose": "think",
        "outcome": "timeout", "input_tokens": 10, "output_tokens": 1,
        "usage_exactness": "reported",
    }, {
        "physical_attempt_id": "attempt-success", "logical_call_id": "logical-1",
        "provider": "codex", "model": "gpt-5.4", "purpose": "think",
        "outcome": "success", "input_tokens": 12, "output_tokens": 3,
        "usage_exactness": "reported",
    }]
    result = assess_provider_identity_receipts(
        logical_receipts=logical, attempt_receipts=attempts,
        required_provider="codex", required_model="gpt-5.4",
    )
    assert result["valid"]
    assert result["physical_attempt_count"] == P7_MAX_ATTEMPTS
    assert result["input_tokens"] == 22
    assert result["output_tokens"] == 4
    assert len({row["physical_attempt_id"] for row in attempts}) == 2


def test_retry_envelope_cannot_be_shadowed_by_attempt_timeout() -> None:
    _validate_deadlines(
        attempt_timeout_s=P7_ATTEMPT_TIMEOUT_S,
        batch_deadline_s=P7_BATCH_DEADLINE_S,
    )
    with pytest.raises(ValueError, match="must exceed the two-attempt"):
        _validate_deadlines(attempt_timeout_s=300.0, batch_deadline_s=600.0)


def test_world_arm_membership_is_stable_and_isolated() -> None:
    digest = "population-digest"
    execution_id = uuid4()
    first = {
        arm: _arm_tenant_id(
            execution_id=execution_id, world_id="world-1", arm=arm,
            population_digest=digest,
        )
        for arm in P7_ARMS
    }
    second = {
        arm: _arm_tenant_id(
            execution_id=execution_id, world_id="world-1", arm=arm,
            population_digest=digest,
        )
        for arm in P7_ARMS
    }
    assert first == second
    assert len(set(first.values())) == len(P7_ARMS)


def test_restart_from_zero_gets_new_isolated_tenant_membership() -> None:
    first_execution, second_execution = uuid4(), uuid4()
    kwargs = {
        "world_id": "world-1", "arm": P7_ARMS[0],
        "population_digest": "population-digest",
    }
    first = _arm_tenant_id(execution_id=first_execution, **kwargs)
    assert first == _arm_tenant_id(execution_id=first_execution, **kwargs)
    assert first != _arm_tenant_id(execution_id=second_execution, **kwargs)


def test_post_bootstrap_mutation_guard_compares_canonical_versions() -> None:
    def waves(final_models: int) -> list[dict[str, object]]:
        return [{
            "batch_number": batch,
            "stage_snapshot": {"write_counts": {
                "canonical_model_versions": models,
                "canonical_relation_versions": 2,
            }},
        } for batch, models in ((3, 4), (12, final_models))]

    assert _canonical_counts_unchanged_after_bootstrap(waves(4))
    assert not _canonical_counts_unchanged_after_bootstrap(waves(5))
    assert not _canonical_counts_unchanged_after_bootstrap(waves(4)[:1])


def test_receipt_guard_rejects_duplicate_or_over_budget_attempts() -> None:
    logical = [{
        "logical_call_id": "logical-1", "provider": "codex", "model": "gpt-5.4",
        "purpose": "think", "physical_attempt_count": 3,
    }]
    attempt = {
        "physical_attempt_id": "duplicate", "logical_call_id": "logical-1",
        "provider": "codex", "model": "gpt-5.4", "purpose": "think",
        "usage_exactness": "reported",
    }
    result = assess_provider_identity_receipts(
        logical_receipts=logical, attempt_receipts=[attempt, attempt, attempt],
        required_provider="codex", required_model="gpt-5.4",
    )
    assert not result["valid"]
    assert result["duplicate_attempt_ids"] == ["duplicate"]
    assert result["over_budget_logical_calls"] == ["logical-1"]
