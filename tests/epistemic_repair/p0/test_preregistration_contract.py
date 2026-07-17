from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from lib.evaluation.epistemic_repair.preregistration import (
    ArtifactBinding,
    ExecutionBudget,
    PreregistrationManifest,
    PreregistrationReceipt,
    create_preregistration_receipt,
    verify_preregistration_receipt,
)
from lib.evaluation.repository_provenance import RepositoryProvenance


def _binding(name: str, character: str) -> ArtifactBinding:
    return ArtifactBinding(
        artifact_id=name,
        schema_version=f"{name}-v1",
        sha256=character * 64,
    )


def _manifest() -> PreregistrationManifest:
    return PreregistrationManifest(
        run_id="epistemic-repair-p0-contract-test",
        phase_id="P0-F",
        scenario=_binding("scenario", "1"),
        gold=_binding("gold", "2"),
        evaluation_policy=_binding("policy", "3"),
        runtime_sources=(_binding("runtime-source", "4"),),
        provider_configuration=_binding("provider-config", "5"),
        repository=RepositoryProvenance(
            head_commit="a" * 40,
            worktree_state="clean",
            worktree_digest="b" * 64,
        ),
        execution_budget=ExecutionBudget(
            allowed_logical_calls=2,
            allowed_physical_attempts=3,
            maximum_operation_seconds=240,
            maximum_prompt_tokens=10_000,
            maximum_completion_tokens=2_000,
        ),
        random_seeds=(17, 23),
        required_hard_gates=("HG-01", "HG-13", "HG-15"),
        proof_boundaries=("deterministic contract evidence only",),
    )


def test_receipt_round_trips_with_stable_digests() -> None:
    receipt = create_preregistration_receipt(
        _manifest(),
        sealed_at=datetime(2026, 7, 17, 12, tzinfo=UTC),
    )

    reopened = verify_preregistration_receipt(receipt.model_dump(mode="json"))

    assert reopened == receipt
    assert reopened.manifest_sha256 == receipt.manifest.manifest_sha256
    assert reopened.receipt_sha256 == receipt.receipt_sha256


def test_receipt_rejects_manifest_tampering() -> None:
    receipt = create_preregistration_receipt(
        _manifest(),
        sealed_at=datetime(2026, 7, 17, 12, tzinfo=UTC),
    )
    payload = receipt.model_dump(mode="json")
    payload["manifest"]["run_id"] = "tampered"

    with pytest.raises(ValidationError, match="manifest digest mismatch"):
        PreregistrationReceipt.model_validate(payload)


def test_manifest_rejects_duplicate_artifact_identity() -> None:
    payload = _manifest().model_dump(mode="json")
    payload["runtime_sources"][0]["artifact_id"] = "scenario"

    with pytest.raises(ValidationError, match="artifact IDs must be unique"):
        PreregistrationManifest.model_validate(payload)


def test_manifest_rejects_exhausted_execution_allowance() -> None:
    payload = _manifest().model_dump(mode="json")
    payload["prior_execution_count"] = payload["allowed_execution_count"]

    with pytest.raises(ValidationError, match="allowance is exhausted"):
        PreregistrationManifest.model_validate(payload)


def test_budget_counts_failed_attempts_inside_the_operation() -> None:
    with pytest.raises(
        ValidationError,
        match="physical-attempt budget cannot be smaller",
    ):
        ExecutionBudget(
            allowed_logical_calls=3,
            allowed_physical_attempts=2,
            maximum_operation_seconds=240,
        )
