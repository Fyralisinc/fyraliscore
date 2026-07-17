"""Immutable preregistration manifests for coherent company-learning proof."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.repository_provenance import RepositoryProvenance


_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class _FrozenContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class ArtifactBinding(_FrozenContract):
    """One immutable semantic input to an evaluation run."""

    artifact_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)


class ExecutionBudget(_FrozenContract):
    """The whole-operation budget, including failed physical attempts."""

    allowed_logical_calls: int = Field(ge=0)
    allowed_physical_attempts: int = Field(ge=0)
    maximum_operation_seconds: float = Field(gt=0)
    maximum_prompt_tokens: int | None = Field(default=None, ge=0)
    maximum_completion_tokens: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def attempts_cover_calls(self) -> Self:
        if self.allowed_physical_attempts < self.allowed_logical_calls:
            raise ValueError(
                "physical-attempt budget cannot be smaller than logical-call budget"
            )
        return self


class PreregistrationManifest(_FrozenContract):
    """All inputs that must be frozen before an evaluation can create evidence."""

    schema_version: Literal["company-learning-preregistration-manifest-v1"] = (
        "company-learning-preregistration-manifest-v1"
    )
    run_id: str = Field(min_length=1)
    phase_id: str = Field(pattern=r"^P[0-9]+(?:-[A-Z0-9]+)?$")
    scenario: ArtifactBinding
    gold: ArtifactBinding
    evaluation_policy: ArtifactBinding
    runtime_sources: tuple[ArtifactBinding, ...] = Field(min_length=1)
    provider_configuration: ArtifactBinding
    repository: RepositoryProvenance
    execution_budget: ExecutionBudget
    random_seeds: tuple[int, ...] = Field(min_length=1)
    required_hard_gates: tuple[str, ...] = Field(min_length=1)
    proof_boundaries: tuple[str, ...] = Field(min_length=1)
    allowed_execution_count: int = Field(default=1, ge=1)
    prior_execution_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def manifest_is_unambiguous(self) -> Self:
        bindings = (
            self.scenario,
            self.gold,
            self.evaluation_policy,
            self.provider_configuration,
            *self.runtime_sources,
        )
        artifact_ids = [item.artifact_id for item in bindings]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("artifact IDs must be unique within a manifest")
        if len(self.random_seeds) != len(set(self.random_seeds)):
            raise ValueError("random seeds must be unique")
        if len(self.required_hard_gates) != len(set(self.required_hard_gates)):
            raise ValueError("hard-gate IDs must be unique")
        if self.prior_execution_count >= self.allowed_execution_count:
            raise ValueError("preregistered execution allowance is exhausted")
        return self

    @property
    def manifest_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PreregistrationReceipt(_FrozenContract):
    """A self-verifying seal over a complete preregistration manifest."""

    schema_version: Literal["company-learning-preregistration-receipt-v1"] = (
        "company-learning-preregistration-receipt-v1"
    )
    status: Literal["sealed_before_execution"] = "sealed_before_execution"
    sealed_at: datetime
    manifest: PreregistrationManifest
    manifest_sha256: str = Field(pattern=_SHA256_PATTERN)

    @field_validator("sealed_at")
    @classmethod
    def timestamp_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("sealed_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def digest_matches_manifest(self) -> Self:
        if self.manifest_sha256 != self.manifest.manifest_sha256:
            raise ValueError("preregistration manifest digest mismatch")
        return self

    @property
    def receipt_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def create_preregistration_receipt(
    manifest: PreregistrationManifest,
    *,
    sealed_at: datetime,
) -> PreregistrationReceipt:
    """Seal a validated manifest before any evaluation execution begins."""

    return PreregistrationReceipt(
        sealed_at=sealed_at,
        manifest=manifest,
        manifest_sha256=manifest.manifest_sha256,
    )


def verify_preregistration_receipt(
    receipt: PreregistrationReceipt | dict[str, object],
) -> PreregistrationReceipt:
    """Reparse and verify a persisted receipt without trusting caller state."""

    payload = (
        receipt.model_dump(mode="json")
        if isinstance(receipt, PreregistrationReceipt)
        else receipt
    )
    return PreregistrationReceipt.model_validate(payload)
