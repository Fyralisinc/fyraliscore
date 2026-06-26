"""BYOC bootstrap bundle metadata and integrity contracts."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    DeploymentEnvironment,
)
from services.platform.runtime.byoc_permissions import ByocPermissionsManifest


ArtifactKind = Literal[
    "container_image",
    "helm_chart",
    "terraform_module",
    "cloudformation_template",
    "sbom",
    "provenance",
]
ArtifactRole = Literal[
    "gateway_image",
    "worker_image",
    "data_plane_agent_image",
    "helm_chart",
    "iam_bootstrap_template",
    "terraform_module",
    "source_sbom",
    "image_sbom",
    "provenance_attestation",
]
SignatureProvider = Literal["sigstore-keyless"]
ReleaseChannel = Literal["staging", "production"]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{7,40}$")
_REQUIRED_ARTIFACT_ROLES = frozenset(
    {
        "gateway_image",
        "worker_image",
        "data_plane_agent_image",
        "helm_chart",
        "iam_bootstrap_template",
        "source_sbom",
        "image_sbom",
    }
)
_IMMUTABLE_REF_KINDS = frozenset(
    {
        "container_image",
        "helm_chart",
        "terraform_module",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocBootstrapSignature(_StrictModel):
    provider: SignatureProvider = "sigstore-keyless"
    bundle_ref: str
    certificate_identity: str
    oidc_issuer: str = "https://token.actions.githubusercontent.com"
    transparency_log_included: Literal[True] = True

    @field_validator("bundle_ref", "certificate_identity", "oidc_issuer")
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("signature fields must not be empty")
        return value


class ByocBootstrapArtifact(_StrictModel):
    role: ArtifactRole
    kind: ArtifactKind
    ref: str
    digest: str
    local_path: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    signature: ByocBootstrapSignature

    @field_validator("ref")
    @classmethod
    def _ref_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("artifact ref must not be empty")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("artifact digest must look like sha256:<64-hex>")
        if value == "sha256:" + ("0" * 64):
            raise ValueError("artifact digest must not be all zeros")
        return value

    @field_validator("local_path")
    @classmethod
    def _local_path_must_be_relative(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("local_path must not be empty")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("local_path must be repository-relative")
        return value


class ByocBootstrapBundleManifest(_StrictModel):
    schema_version: Literal["fyralis.byoc.bootstrap_bundle.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    release_channel: ReleaseChannel
    source_commit: str
    created_at: datetime
    signing_certificate_identity: str
    signing_oidc_issuer: str = "https://token.actions.githubusercontent.com"
    requires_cosign_min_version: str = "2.4.0"
    artifacts: tuple[ByocBootstrapArtifact, ...]

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
        "region",
        "artifact_revision",
        "signing_certificate_identity",
        "signing_oidc_issuer",
        "requires_cosign_min_version",
    )
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("bundle fields must not be empty")
        return value

    @field_validator("source_commit")
    @classmethod
    def _source_commit_must_be_hex(cls, value: str) -> str:
        value = value.strip().lower()
        if not _COMMIT_RE.match(value):
            raise ValueError("source_commit must be a 7-40 character git SHA")
        return value

    @field_validator("artifacts")
    @classmethod
    def _artifacts_must_be_present(
        cls,
        value: tuple[ByocBootstrapArtifact, ...],
    ) -> tuple[ByocBootstrapArtifact, ...]:
        if not value:
            raise ValueError("artifacts must not be empty")
        return value


@dataclass(frozen=True, slots=True)
class ByocBootstrapBundleViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def validate_bootstrap_bundle_contract(
    bundle: ByocBootstrapBundleManifest,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    permissions_manifest: ByocPermissionsManifest | None = None,
    verify_local_files: bool = False,
    repo_root: Path | None = None,
) -> list[ByocBootstrapBundleViolation]:
    violations: list[ByocBootstrapBundleViolation] = []
    if dataplane_manifest is not None:
        violations.extend(_compare_dataplane_manifest(bundle, dataplane_manifest))
    if permissions_manifest is not None:
        violations.extend(_compare_permissions_manifest(bundle, permissions_manifest))

    artifact_roles = [artifact.role for artifact in bundle.artifacts]
    duplicates = sorted(
        {role for role in artifact_roles if artifact_roles.count(role) > 1}
    )
    for role in duplicates:
        violations.append(
            _violation(
                "artifacts",
                "duplicate_artifact_role",
                f"{role!r} is listed more than once",
            )
        )

    missing_roles = _REQUIRED_ARTIFACT_ROLES - set(artifact_roles)
    for role in sorted(missing_roles):
        violations.append(
            _violation(
                "artifacts",
                "missing_required_artifact",
                f"{role!r} is required in the BYOC bootstrap bundle",
            )
        )

    for artifact in bundle.artifacts:
        violations.extend(_validate_artifact_contract(artifact, bundle))
        if verify_local_files:
            violations.extend(
                _validate_local_artifact_digest(
                    artifact,
                    repo_root=repo_root or Path.cwd(),
                )
            )

    return violations


def byoc_bootstrap_bundle_json_schema() -> dict[str, Any]:
    return ByocBootstrapBundleManifest.model_json_schema()


def load_byoc_bootstrap_bundle(path: Path) -> ByocBootstrapBundleManifest:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC bootstrap bundle must be a JSON/YAML object")
    return ByocBootstrapBundleManifest.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _validate_artifact_contract(
    artifact: ByocBootstrapArtifact,
    bundle: ByocBootstrapBundleManifest,
) -> list[ByocBootstrapBundleViolation]:
    violations: list[ByocBootstrapBundleViolation] = []
    if artifact.kind in _IMMUTABLE_REF_KINDS:
        if "@sha256:" not in artifact.ref:
            violations.append(
                _violation(
                    f"artifacts.{artifact.role}.ref",
                    "mutable_artifact_ref",
                    "OCI artifacts must be pinned by digest",
                )
            )
        elif not artifact.ref.endswith(artifact.digest):
            violations.append(
                _violation(
                    f"artifacts.{artifact.role}.digest",
                    "artifact_ref_digest_mismatch",
                    "artifact ref digest must match the declared digest",
                )
            )
        if ":latest" in artifact.ref:
            violations.append(
                _violation(
                    f"artifacts.{artifact.role}.ref",
                    "latest_tag_forbidden",
                    "BYOC artifacts must not use mutable latest tags",
                )
            )
    if artifact.kind == "cloudformation_template" and artifact.local_path is None:
        violations.append(
            _violation(
                f"artifacts.{artifact.role}.local_path",
                "local_template_path_required",
                "checked-in IAM templates must provide local_path for hash checks",
            )
        )
    if _ref_contains_credentials(artifact.ref):
        violations.append(
            _violation(
                f"artifacts.{artifact.role}.ref",
                "artifact_ref_credentials_forbidden",
                "artifact refs must not contain credentials",
            )
        )
    if artifact.signature.certificate_identity != bundle.signing_certificate_identity:
        violations.append(
            _violation(
                f"artifacts.{artifact.role}.signature.certificate_identity",
                "signature_identity_mismatch",
                "artifact signature identity must match bundle signing identity",
            )
        )
    if artifact.signature.oidc_issuer != bundle.signing_oidc_issuer:
        violations.append(
            _violation(
                f"artifacts.{artifact.role}.signature.oidc_issuer",
                "signature_issuer_mismatch",
                "artifact signature issuer must match bundle signing issuer",
            )
        )
    if not artifact.signature.bundle_ref.endswith(".sigstore"):
        violations.append(
            _violation(
                f"artifacts.{artifact.role}.signature.bundle_ref",
                "sigstore_bundle_ref_required",
                "artifact signatures must reference a Sigstore bundle",
            )
        )
    return violations


def _validate_local_artifact_digest(
    artifact: ByocBootstrapArtifact,
    *,
    repo_root: Path,
) -> list[ByocBootstrapBundleViolation]:
    if artifact.local_path is None:
        return []
    path = repo_root / artifact.local_path
    if not path.exists() or not path.is_file():
        return [
            _violation(
                f"artifacts.{artifact.role}.local_path",
                "local_artifact_missing",
                f"{artifact.local_path!r} does not exist",
            )
        ]
    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != artifact.digest:
        return [
            _violation(
                f"artifacts.{artifact.role}.digest",
                "local_artifact_digest_mismatch",
                "local artifact digest does not match the bundle manifest",
            )
        ]
    return []


def _compare_dataplane_manifest(
    bundle: ByocBootstrapBundleManifest,
    dataplane_manifest: ByocDataPlaneManifest,
) -> list[ByocBootstrapBundleViolation]:
    violations: list[ByocBootstrapBundleViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(bundle, field) != getattr(dataplane_manifest, field):
            violations.append(
                _violation(
                    field,
                    "dataplane_manifest_mismatch",
                    f"bootstrap bundle {field} does not match data-plane manifest",
                )
            )
    return violations


def _compare_permissions_manifest(
    bundle: ByocBootstrapBundleManifest,
    permissions_manifest: ByocPermissionsManifest,
) -> list[ByocBootstrapBundleViolation]:
    violations: list[ByocBootstrapBundleViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(bundle, field) != getattr(permissions_manifest, field):
            violations.append(
                _violation(
                    field,
                    "permissions_manifest_mismatch",
                    f"bootstrap bundle {field} does not match permissions manifest",
                )
            )
    return violations


def _ref_contains_credentials(ref: str) -> bool:
    parsed = urlparse(ref)
    return bool(parsed.username or parsed.password)


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocBootstrapBundleViolation:
    return ByocBootstrapBundleViolation(path=path, code=code, message=message)


def _load_mapping(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError(
            "YAML manifests require PyYAML; use JSON or install the dev extras"
        ) from exc
    return yaml.safe_load(raw)


__all__ = [
    "ByocBootstrapArtifact",
    "ByocBootstrapBundleManifest",
    "ByocBootstrapBundleViolation",
    "ByocBootstrapSignature",
    "byoc_bootstrap_bundle_json_schema",
    "load_byoc_bootstrap_bundle",
    "render_validation_errors",
    "validate_bootstrap_bundle_contract",
]
