"""Sanitized BYOC agent artifact verification evidence contract."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_agent_apply_plan import (
    ByocAgentApplyPlan,
    validate_apply_plan_contract,
)
from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_contract import ByocDataPlaneManifest


ArtifactVerificationStoredScope = Literal["sanitized_agent_metadata_only"]

_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_PLAN_ID_RE = re.compile(r"^ap_[a-f0-9]{16}$")
_VERIFICATION_ID_RE = re.compile(r"^av_[a-f0-9]{16}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RAW_MARKERS = (
    "://",
    "bearer ",
    "secret",
    "signature",
    "sigstore",
    "bundle_ref",
    "payload",
    "prompt",
    "embedding",
    " raw_",
    " pii",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAgentArtifactDigestEvidence(_StrictModel):
    role: str
    kind: str
    digest: str
    local_digest_checked: bool = False

    @field_validator("role", "kind")
    @classmethod
    def _bounded_code(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("artifact evidence fields must be bounded codes")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("artifact evidence digest must look like sha256:<64-hex>")
        return value


class ByocAgentArtifactVerificationEvidence(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.artifact_verification_evidence.v1"]
    status: Literal["pass"] = "pass"
    verification_id: str
    plan_id: str
    deployment_id: str
    customer_id: str
    current_revision: str
    desired_revision: str
    bundle_artifact_revision: str
    artifact_count: int = Field(ge=0)
    digest_pinned_artifact_count: int = Field(ge=0)
    local_digest_checked_count: int = Field(ge=0)
    required_artifact_roles: tuple[str, ...]
    artifacts: tuple[ByocAgentArtifactDigestEvidence, ...]
    stored_scope: ArtifactVerificationStoredScope = "sanitized_agent_metadata_only"

    @field_validator("verification_id")
    @classmethod
    def _verification_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _VERIFICATION_ID_RE.match(value):
            raise ValueError("verification_id must look like av_<digest>")
        return value

    @field_validator("plan_id")
    @classmethod
    def _plan_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _PLAN_ID_RE.match(value):
            raise ValueError("plan_id must look like ap_<digest>")
        return value

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value

    @field_validator(
        "current_revision",
        "desired_revision",
        "bundle_artifact_revision",
    )
    @classmethod
    def _bounded_revision(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("artifact revision fields must be bounded identifiers")
        return value

    @field_validator("required_artifact_roles")
    @classmethod
    def _required_roles_must_be_bounded(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if not value:
            raise ValueError("required_artifact_roles must not be empty")
        for role in value:
            if not _SAFE_CODE_RE.match(role):
                raise ValueError("required artifact roles must be bounded codes")
        return value


@dataclass(frozen=True, slots=True)
class ByocAgentArtifactVerificationViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def build_artifact_verification_evidence(
    plan: ByocAgentApplyPlan,
    bundle: ByocBootstrapBundleManifest,
    manifest: ByocDataPlaneManifest,
    *,
    verify_local_files: bool = False,
    repo_root: Path | None = None,
) -> ByocAgentArtifactVerificationEvidence:
    artifacts = tuple(
        ByocAgentArtifactDigestEvidence(
            role=artifact.role,
            kind=artifact.kind,
            digest=artifact.digest,
            local_digest_checked=bool(verify_local_files and artifact.local_path),
        )
        for artifact in sorted(bundle.artifacts, key=lambda item: item.role)
    )
    return ByocAgentArtifactVerificationEvidence(
        schema_version="fyralis.byoc.agent.artifact_verification_evidence.v1",
        status="pass",
        verification_id=_verification_id(plan, bundle),
        plan_id=plan.plan_id,
        deployment_id=plan.deployment_id,
        customer_id=plan.customer_id,
        current_revision=plan.current_revision,
        desired_revision=plan.desired_revision,
        bundle_artifact_revision=bundle.artifact_revision,
        artifact_count=len(artifacts),
        digest_pinned_artifact_count=sum(
            1 for artifact in bundle.artifacts if artifact.digest
        ),
        local_digest_checked_count=sum(
            1 for artifact in bundle.artifacts if verify_local_files and artifact.local_path
        ),
        required_artifact_roles=tuple(artifact.role for artifact in artifacts),
        artifacts=artifacts,
        stored_scope="sanitized_agent_metadata_only",
    )


def validate_artifact_verification_contract(
    evidence: ByocAgentArtifactVerificationEvidence,
    *,
    plan: ByocAgentApplyPlan,
    bundle: ByocBootstrapBundleManifest,
    manifest: ByocDataPlaneManifest,
    verify_local_files: bool = False,
    repo_root: Path | None = None,
) -> list[ByocAgentArtifactVerificationViolation]:
    violations: list[ByocAgentArtifactVerificationViolation] = []
    for plan_violation in validate_apply_plan_contract(plan):
        violations.append(
            _violation(
                f"plan.{plan_violation.path}",
                plan_violation.code,
                plan_violation.message,
            )
        )
    for bundle_violation in validate_bootstrap_bundle_contract(
        bundle,
        verify_local_files=verify_local_files,
        repo_root=repo_root,
    ):
        violations.append(
            _violation(
                f"bundle.{bundle_violation.path}",
                bundle_violation.code,
                bundle_violation.message,
            )
        )
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
    ):
        if getattr(bundle, field) != getattr(manifest, field):
            violations.append(
                _violation(
                    f"bundle.{field}",
                    "manifest_identity_mismatch",
                    f"bootstrap bundle {field} does not match data-plane manifest",
                )
            )
    if bundle.artifact_revision != plan.desired_revision:
        violations.append(
            _violation(
                "bundle.artifact_revision",
                "desired_revision_mismatch",
                "bootstrap bundle artifact revision must match apply-plan desired revision",
            )
        )
    if evidence.plan_id != plan.plan_id:
        violations.append(
            _violation("plan_id", "plan_id_mismatch", "evidence plan_id mismatch")
        )
    if evidence.desired_revision != plan.desired_revision:
        violations.append(
            _violation(
                "desired_revision",
                "desired_revision_mismatch",
                "evidence desired revision mismatch",
            )
        )
    if evidence.bundle_artifact_revision != bundle.artifact_revision:
        violations.append(
            _violation(
                "bundle_artifact_revision",
                "bundle_revision_mismatch",
                "evidence bundle revision mismatch",
            )
        )
    if evidence.artifact_count != len(bundle.artifacts):
        violations.append(
            _violation(
                "artifact_count",
                "artifact_count_mismatch",
                "artifact evidence count must match bundle artifacts",
            )
        )
    if evidence.digest_pinned_artifact_count != evidence.artifact_count:
        violations.append(
            _violation(
                "digest_pinned_artifact_count",
                "digest_pin_count_mismatch",
                "all artifact evidence entries must be digest pinned",
            )
        )
    if evidence.local_digest_checked_count != sum(
        1 for artifact in bundle.artifacts if verify_local_files and artifact.local_path
    ):
        violations.append(
            _violation(
                "local_digest_checked_count",
                "local_digest_count_mismatch",
                "local digest check count must match local_path artifacts",
            )
        )
    serialized = json.dumps(evidence.model_dump(mode="json"), sort_keys=True).lower()
    for marker in _RAW_MARKERS:
        if marker in serialized:
            violations.append(
                _violation(
                    "evidence",
                    "raw_material_marker",
                    "artifact evidence must not contain refs, URLs, signatures, or raw payloads",
                )
            )
            break
    return violations


def _verification_id(
    plan: ByocAgentApplyPlan,
    bundle: ByocBootstrapBundleManifest,
) -> str:
    material = (
        f"{plan.plan_id}:"
        f"{plan.desired_revision}:"
        f"{bundle.artifact_revision}:"
        f"{','.join(sorted(artifact.digest for artifact in bundle.artifacts))}"
    )
    return "av_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocAgentArtifactVerificationViolation:
    return ByocAgentArtifactVerificationViolation(
        path=path,
        code=code,
        message=message,
    )


__all__ = [
    "ByocAgentArtifactDigestEvidence",
    "ByocAgentArtifactVerificationEvidence",
    "ByocAgentArtifactVerificationViolation",
    "build_artifact_verification_evidence",
    "validate_artifact_verification_contract",
]
