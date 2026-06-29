"""Sanitized BYOC customer handoff evidence package contract."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from pydantic import model_validator

from services.platform.runtime.byoc_aws_iac_package import ByocAwsIacPackage
from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
)
from services.platform.runtime.byoc_bootstrap_plan import ByocBootstrapPlanManifest
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    DeploymentEnvironment,
)
from services.platform.runtime.byoc_evidence_ledger import (
    ByocDeploymentEvidenceLedger,
    ByocEvidenceEnvelope,
    ByocEvidencePrivacyContract,
    load_byoc_evidence_ledger,
    load_evidence_envelope,
    validate_evidence_ledger_contract,
)
from services.platform.runtime.byoc_permissions import ByocPermissionsManifest


EvidencePackageArtifactKind = Literal[
    "dataplane_manifest",
    "permissions_manifest",
    "aws_iac_package",
    "bootstrap_bundle",
    "bootstrap_plan",
    "evidence_ledger",
]
EvidencePackageScope = Literal["sanitized_customer_handoff_metadata_only"]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_REF_RE = re.compile(r"^(?:generated:[A-Za-z0-9_.:-]+|[A-Za-z0-9_./+=,-]+)$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REQUIRED_ARTIFACT_KINDS: frozenset[EvidencePackageArtifactKind] = frozenset(
    {
        "dataplane_manifest",
        "permissions_manifest",
        "aws_iac_package",
        "bootstrap_bundle",
        "bootstrap_plan",
        "evidence_ledger",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocEvidencePackageArtifact(_StrictModel):
    kind: EvidencePackageArtifactKind
    ref: str
    digest: str

    @field_validator("ref")
    @classmethod
    def _ref_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_REF_RE.match(value):
            raise ValueError("package artifact ref must be bounded metadata")
        if "://" in value:
            raise ValueError("package artifact ref must not be a URL")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("package artifact ref must not escape the repository")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("package artifact digest must look like sha256:<64-hex>")
        return value


class ByocEvidencePackageEnvelopeSummary(_StrictModel):
    envelope_digest: str
    report_kind: Literal["post_deploy_validation"] = "post_deploy_validation"
    report_digest: str
    agent_id: str
    signing_key_ref: str
    signature_key_ref: str
    signature_algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    issued_at: datetime
    expires_at: datetime
    signature_verified: Literal[True] = True

    @field_validator("envelope_digest", "report_digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("envelope summary digest must look like sha256:<64-hex>")
        return value

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("signing_key_ref", "signature_key_ref")
    @classmethod
    def _key_ref_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("envelope key refs must not be empty")
        return value

    @model_validator(mode="after")
    def _expiry_must_follow_issue_time(
        self,
    ) -> "ByocEvidencePackageEnvelopeSummary":
        if self.expires_at <= self.issued_at:
            raise ValueError("expires_at must be after issued_at")
        return self


class ByocEvidencePackage(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_package.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    generated_at: datetime
    generated_by: Literal["fyralis-core"] = "fyralis-core"
    export_scope: EvidencePackageScope = "sanitized_customer_handoff_metadata_only"
    privacy: ByocEvidencePrivacyContract
    source_artifacts: tuple[ByocEvidencePackageArtifact, ...]
    ledger: ByocDeploymentEvidenceLedger
    live_report_envelope: ByocEvidencePackageEnvelopeSummary | None = None

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

    @field_validator("region", "artifact_revision")
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("package identity fields must not be empty")
        return value

    @field_validator("source_artifacts")
    @classmethod
    def _source_artifacts_must_be_complete(
        cls,
        value: tuple[ByocEvidencePackageArtifact, ...],
    ) -> tuple[ByocEvidencePackageArtifact, ...]:
        kinds = [artifact.kind for artifact in value]
        duplicates = sorted({kind for kind in kinds if kinds.count(kind) > 1})
        if duplicates:
            raise ValueError("package source artifacts must not be duplicated")
        missing = _REQUIRED_ARTIFACT_KINDS - set(kinds)
        if missing:
            raise ValueError("package source artifacts are incomplete")
        return value

    @model_validator(mode="after")
    def _package_must_match_ledger(self) -> "ByocEvidencePackage":
        for field in (
            "deployment_id",
            "customer_id",
            "environment",
            "cloud_provider",
            "region",
            "artifact_revision",
        ):
            if getattr(self, field) != getattr(self.ledger, field):
                raise ValueError(f"package {field} must match evidence ledger")
        if self.privacy != self.ledger.privacy:
            raise ValueError("package privacy flags must match evidence ledger")

        post_deploy = _post_deploy_evidence(self.ledger)
        signed_report = post_deploy.source.type == "signed_post_deploy_report_file"
        if signed_report:
            if post_deploy.signature_verified is not True:
                raise ValueError("signed report evidence must be signature verified")
            if post_deploy.envelope_digest is None:
                raise ValueError("signed report evidence must include envelope digest")
            if self.live_report_envelope is None:
                raise ValueError("signed report package requires envelope metadata")
            if self.live_report_envelope.envelope_digest != post_deploy.envelope_digest:
                raise ValueError("envelope metadata digest must match ledger evidence")
        elif self.live_report_envelope is not None:
            raise ValueError("unsigned report package must not include envelope metadata")
        return self


@dataclass(frozen=True, slots=True)
class ByocEvidencePackageViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def generate_evidence_package(
    *,
    ledger: ByocDeploymentEvidenceLedger,
    dataplane_manifest: ByocDataPlaneManifest,
    permissions_manifest: ByocPermissionsManifest,
    bootstrap_bundle: ByocBootstrapBundleManifest,
    plan: ByocBootstrapPlanManifest,
    ledger_path: Path,
    dataplane_manifest_path: Path,
    permissions_manifest_path: Path,
    aws_iac_package_path: Path,
    bootstrap_bundle_path: Path,
    plan_path: Path,
    post_deploy_envelope_path: Path | None = None,
    generated_at: datetime | None = None,
    repo_root: Path | None = None,
) -> ByocEvidencePackage:
    repo_root = (repo_root or Path.cwd()).resolve()
    envelope = (
        load_evidence_envelope(post_deploy_envelope_path)
        if post_deploy_envelope_path is not None
        else None
    )
    post_deploy = _post_deploy_evidence(ledger)
    if post_deploy.source.type == "signed_post_deploy_report_file" and envelope is None:
        raise ValueError(
            "signed evidence ledger requires --post-deploy-envelope for packaging"
        )
    if post_deploy.source.type != "signed_post_deploy_report_file" and envelope:
        raise ValueError("unsigned evidence ledger must not include an envelope")
    envelope_summary = (
        _envelope_summary(
            envelope,
            envelope_path=post_deploy_envelope_path,
            ledger=ledger,
            dataplane_manifest=dataplane_manifest,
        )
        if envelope is not None and post_deploy_envelope_path is not None
        else None
    )
    package = ByocEvidencePackage(
        schema_version="fyralis.byoc.evidence_package.v1",
        deployment_id=ledger.deployment_id,
        customer_id=ledger.customer_id,
        environment=ledger.environment,
        cloud_provider=ledger.cloud_provider,
        region=ledger.region,
        artifact_revision=ledger.artifact_revision,
        generated_at=generated_at or ledger.generated_at or datetime.now(UTC),
        generated_by="fyralis-core",
        export_scope="sanitized_customer_handoff_metadata_only",
        privacy=ledger.privacy,
        source_artifacts=(
            _artifact(
                "dataplane_manifest",
                dataplane_manifest_path,
                repo_root=repo_root,
                external_ref="generated:external_dataplane_manifest",
            ),
            _artifact(
                "permissions_manifest",
                permissions_manifest_path,
                repo_root=repo_root,
                external_ref="generated:external_permissions_manifest",
            ),
            _artifact(
                "aws_iac_package",
                aws_iac_package_path,
                repo_root=repo_root,
                external_ref="generated:external_aws_iac_package",
            ),
            _artifact(
                "bootstrap_bundle",
                bootstrap_bundle_path,
                repo_root=repo_root,
                external_ref="generated:external_bootstrap_bundle",
            ),
            _artifact(
                "bootstrap_plan",
                plan_path,
                repo_root=repo_root,
                external_ref="generated:external_bootstrap_plan",
            ),
            _artifact(
                "evidence_ledger",
                ledger_path,
                repo_root=repo_root,
                external_ref="generated:external_evidence_ledger",
            ),
        ),
        ledger=ledger,
        live_report_envelope=envelope_summary,
    )
    violations = validate_evidence_package_contract(
        package,
        dataplane_manifest=dataplane_manifest,
        permissions_manifest=permissions_manifest,
        bootstrap_bundle=bootstrap_bundle,
        plan=plan,
        source_digests=package_source_digests(
            dataplane_manifest_path=dataplane_manifest_path,
            permissions_manifest_path=permissions_manifest_path,
            aws_iac_package_path=aws_iac_package_path,
            bootstrap_bundle_path=bootstrap_bundle_path,
            plan_path=plan_path,
            ledger_path=ledger_path,
            repo_root=repo_root,
        ),
    )
    if violations:
        rendered = "; ".join(violation.render() for violation in violations)
        raise ValueError(f"evidence package contract failed: {rendered}")
    return package


def validate_evidence_package_contract(
    package: ByocEvidencePackage,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    permissions_manifest: ByocPermissionsManifest | None = None,
    aws_iac_package: ByocAwsIacPackage | None = None,
    bootstrap_bundle: ByocBootstrapBundleManifest | None = None,
    plan: ByocBootstrapPlanManifest | None = None,
    source_digests: Mapping[EvidencePackageArtifactKind, str] | None = None,
) -> list[ByocEvidencePackageViolation]:
    violations: list[ByocEvidencePackageViolation] = []
    for violation in validate_evidence_ledger_contract(
        package.ledger,
        dataplane_manifest=dataplane_manifest,
        plan=plan,
    ):
        violations.append(
            _violation(f"ledger.{violation.path}", violation.code, violation.message)
        )
    for source, name in (
        (dataplane_manifest, "dataplane_manifest"),
        (permissions_manifest, "permissions_manifest"),
        (aws_iac_package, "aws_iac_package"),
        (bootstrap_bundle, "bootstrap_bundle"),
        (plan, "bootstrap_plan"),
    ):
        if source is not None:
            violations.extend(_compare_identity(package, source, name))

    artifacts = {artifact.kind: artifact for artifact in package.source_artifacts}
    for kind in sorted(_REQUIRED_ARTIFACT_KINDS):
        if kind not in artifacts:
            violations.append(
                _violation(
                    f"source_artifacts.{kind}",
                    "missing_source_artifact",
                    f"{kind!r} source artifact is required",
                )
            )

    if source_digests:
        for kind, digest in source_digests.items():
            artifact = artifacts.get(kind)
            if artifact and artifact.digest != digest:
                violations.append(
                    _violation(
                        f"source_artifacts.{kind}.digest",
                        "source_artifact_digest_mismatch",
                        f"{kind!r} source artifact digest does not match file",
                    )
                )
    return violations


def byoc_evidence_package_json_schema() -> dict[str, Any]:
    return ByocEvidencePackage.model_json_schema()


def load_byoc_evidence_package(path: Path) -> ByocEvidencePackage:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC evidence package must be a JSON/YAML object")
    return ByocEvidencePackage.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def package_source_digests(
    *,
    dataplane_manifest_path: Path,
    permissions_manifest_path: Path,
    aws_iac_package_path: Path,
    bootstrap_bundle_path: Path,
    plan_path: Path,
    ledger_path: Path,
    repo_root: Path | None = None,
) -> dict[EvidencePackageArtifactKind, str]:
    repo_root = (repo_root or Path.cwd()).resolve()
    return {
        "dataplane_manifest": _file_digest(
            _resolve_repo_path(dataplane_manifest_path, repo_root=repo_root)
        ),
        "permissions_manifest": _file_digest(
            _resolve_repo_path(permissions_manifest_path, repo_root=repo_root)
        ),
        "aws_iac_package": _file_digest(
            _resolve_repo_path(aws_iac_package_path, repo_root=repo_root)
        ),
        "bootstrap_bundle": _file_digest(
            _resolve_repo_path(bootstrap_bundle_path, repo_root=repo_root)
        ),
        "bootstrap_plan": _file_digest(_resolve_repo_path(plan_path, repo_root=repo_root)),
        "evidence_ledger": _file_digest(
            _resolve_repo_path(ledger_path, repo_root=repo_root)
        ),
    }


def _artifact(
    kind: EvidencePackageArtifactKind,
    path: Path,
    *,
    repo_root: Path,
    external_ref: str | None = None,
) -> ByocEvidencePackageArtifact:
    resolved = _resolve_repo_path(path, repo_root=repo_root)
    return ByocEvidencePackageArtifact(
        kind=kind,
        ref=_source_ref(path, repo_root=repo_root, external_ref=external_ref),
        digest=_file_digest(resolved),
    )


def _envelope_summary(
    envelope: ByocEvidenceEnvelope,
    *,
    envelope_path: Path,
    ledger: ByocDeploymentEvidenceLedger,
    dataplane_manifest: ByocDataPlaneManifest,
) -> ByocEvidencePackageEnvelopeSummary:
    violations = _compare_envelope_identity(envelope, ledger)
    post_deploy = _post_deploy_evidence(ledger)
    envelope_digest = _file_digest(envelope_path)
    if envelope_digest != post_deploy.envelope_digest:
        violations.append(
            _violation(
                "live_report_envelope.envelope_digest",
                "envelope_digest_mismatch",
                "envelope digest does not match signed ledger evidence",
            )
        )
    if envelope.signing_key_ref != dataplane_manifest.secrets.agent_client_certificate_secret_ref:
        violations.append(
            _violation(
                "live_report_envelope.signing_key_ref",
                "signing_key_ref_mismatch",
                "envelope signing key ref does not match data-plane manifest",
            )
        )
    if envelope.signature.key_ref != envelope.signing_key_ref:
        violations.append(
            _violation(
                "live_report_envelope.signature_key_ref",
                "signature_key_ref_mismatch",
                "signature key ref must match signing key ref",
            )
        )
    if violations:
        rendered = "; ".join(violation.render() for violation in violations)
        raise ValueError(f"evidence package envelope verification failed: {rendered}")
    return ByocEvidencePackageEnvelopeSummary(
        envelope_digest=envelope_digest,
        report_kind=envelope.report_kind,
        report_digest=envelope.report_digest,
        agent_id=envelope.agent_id,
        signing_key_ref=envelope.signing_key_ref,
        signature_key_ref=envelope.signature.key_ref,
        signature_algorithm=envelope.signature.algorithm,
        issued_at=envelope.issued_at,
        expires_at=envelope.expires_at,
        signature_verified=True,
    )


def _post_deploy_evidence(ledger: ByocDeploymentEvidenceLedger):
    for entry in ledger.evidence:
        if entry.kind == "post_deploy_validation":
            return entry
    raise ValueError("evidence ledger is missing post-deploy validation evidence")


def _compare_envelope_identity(
    envelope: ByocEvidenceEnvelope,
    ledger: ByocDeploymentEvidenceLedger,
) -> list[ByocEvidencePackageViolation]:
    violations: list[ByocEvidencePackageViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "artifact_revision",
        "cloud_provider",
        "region",
    ):
        if getattr(envelope, field) != getattr(ledger, field):
            violations.append(
                _violation(
                    f"live_report_envelope.{field}",
                    "envelope_ledger_mismatch",
                    f"evidence envelope {field} does not match ledger",
                )
            )
    return violations


def _compare_identity(
    package: ByocEvidencePackage,
    source: Any,
    name: str,
) -> list[ByocEvidencePackageViolation]:
    violations: list[ByocEvidencePackageViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(package, field) != getattr(source, field):
            violations.append(
                _violation(
                    field,
                    f"{name}_mismatch",
                    f"evidence package {field} does not match {name}",
                )
            )
    return violations


def _source_ref(
    path: Path,
    *,
    repo_root: Path,
    external_ref: str | None = None,
) -> str:
    resolved = _resolve_repo_path(path, repo_root=repo_root)
    try:
        rel = Path(resolved.resolve().relative_to(repo_root).as_posix())
    except ValueError:
        if external_ref is not None:
            return external_ref
        return path.as_posix()
    return rel.as_posix()


def _resolve_repo_path(path: Path, *, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocEvidencePackageViolation:
    return ByocEvidencePackageViolation(path=path, code=code, message=message)


def _load_mapping(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError(
            "YAML evidence packages require PyYAML; use JSON or install dev extras"
        ) from exc
    return yaml.safe_load(raw)


__all__ = [
    "ByocEvidencePackage",
    "ByocEvidencePackageArtifact",
    "ByocEvidencePackageEnvelopeSummary",
    "ByocEvidencePackageViolation",
    "byoc_evidence_package_json_schema",
    "generate_evidence_package",
    "load_byoc_evidence_package",
    "package_source_digests",
    "render_validation_errors",
    "validate_evidence_package_contract",
]
