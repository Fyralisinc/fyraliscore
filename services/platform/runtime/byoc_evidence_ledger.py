"""Sanitized BYOC deployment evidence ledger contract."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
)
from services.platform.runtime.byoc_bootstrap_plan import (
    ByocBootstrapPlanManifest,
    PlanSourcePaths,
    generate_bootstrap_plan,
    validate_bootstrap_plan_contract,
)
from services.platform.runtime.byoc_bootstrap_runner import (
    ByocBootstrapRunnerInputs,
    ByocBootstrapRunnerReport,
    run_byoc_bootstrap_runner,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    DeploymentEnvironment,
)
from services.platform.runtime.byoc_permissions import ByocPermissionsManifest
from services.platform.runtime.byoc_validation import (
    ByocValidationInputs,
    ByocValidationReport,
    run_byoc_post_deploy_validation,
)


EvidenceKind = Literal[
    "bootstrap_plan",
    "bootstrap_runner",
    "post_deploy_validation",
]
EvidenceStatus = Literal["pass", "fail", "skipped"]
EvidenceSourceType = Literal[
    "file",
    "local_runner",
    "offline_validator",
    "post_deploy_report_file",
    "signed_post_deploy_report_file",
]
EvidenceReportKind = Literal["post_deploy_validation"]
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_REF_RE = re.compile(r"^(?:generated:[A-Za-z0-9_.:-]+|[A-Za-z0-9_./+=,-]+)$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_IMPORTED_CHECK_NAME_FRAGMENTS = frozenset(
    {
        "body",
        "credential",
        "email",
        "payload",
        "prompt",
        "secret",
        "token",
        "url",
    }
)
_REQUIRED_EVIDENCE_KINDS: frozenset[EvidenceKind] = frozenset(
    {"bootstrap_plan", "bootstrap_runner", "post_deploy_validation"}
)
_BLOCKING_STATUSES = {"fail"}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _ImportedModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class ByocImportedValidationCheck(_ImportedModel):
    name: str
    status: EvidenceStatus
    required: bool

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        lowered = value.lower()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("imported validation check name must be bounded")
        if any(
            fragment in lowered
            for fragment in _FORBIDDEN_IMPORTED_CHECK_NAME_FRAGMENTS
        ):
            raise ValueError("imported validation check name must not describe data")
        return value


class ByocImportedValidationReport(_ImportedModel):
    status: EvidenceStatus
    required_checks_passed: bool
    checks: tuple[ByocImportedValidationCheck, ...]

    @field_validator("checks")
    @classmethod
    def _checks_must_be_present(
        cls,
        value: tuple[ByocImportedValidationCheck, ...],
    ) -> tuple[ByocImportedValidationCheck, ...]:
        if not value:
            raise ValueError("imported validation report must include checks")
        return value

    @model_validator(mode="after")
    def _status_must_match_required_checks(self) -> "ByocImportedValidationReport":
        if self.required_checks_passed and self.status == "fail":
            raise ValueError("passing required checks cannot have fail status")
        if not self.required_checks_passed and self.status == "pass":
            raise ValueError("failed required checks cannot have pass status")
        return self


class ByocEvidenceEnvelopeSignature(_StrictModel):
    algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    key_ref: str
    value: str

    @field_validator("key_ref")
    @classmethod
    def _key_ref_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("key_ref must not be empty")
        return value

    @field_validator("value")
    @classmethod
    def _signature_must_be_sha256_hex(cls, value: str) -> str:
        value = value.strip().lower()
        if len(value) != 64:
            raise ValueError("signature value must be a SHA-256 hex digest")
        try:
            int(value, 16)
        except ValueError as exc:
            raise ValueError("signature value must be hex encoded") from exc
        return value


class ByocEvidenceEnvelopePayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_envelope.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    artifact_revision: str
    cloud_provider: CloudProvider
    region: str
    report_kind: EvidenceReportKind = "post_deploy_validation"
    report_digest: str
    issued_at: datetime
    expires_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    signing_key_ref: str

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

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("artifact_revision", "region", "nonce", "signing_key_ref")
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("envelope fields must not be empty")
        return value

    @field_validator("report_digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("report_digest must look like sha256:<64-hex>")
        return value

    @model_validator(mode="after")
    def _expiry_must_follow_issue_time(self) -> "ByocEvidenceEnvelopePayload":
        if self.expires_at <= self.issued_at:
            raise ValueError("expires_at must be after issued_at")
        return self


class ByocEvidenceEnvelope(ByocEvidenceEnvelopePayload):
    signature: ByocEvidenceEnvelopeSignature


class ByocEvidencePrivacyContract(_StrictModel):
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    credentials_included: Literal[False] = False
    artifact_refs_included: Literal[False] = False
    command_output_included: Literal[False] = False
    report_details_included: Literal[False] = False


class ByocEvidenceSource(_StrictModel):
    type: EvidenceSourceType
    ref: str
    digest: str | None = None

    @field_validator("ref")
    @classmethod
    def _ref_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_REF_RE.match(value):
            raise ValueError("evidence source ref must be bounded metadata")
        if "://" in value:
            raise ValueError("evidence source ref must not be a URL")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("evidence source ref must not escape the repository")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("evidence source digest must look like sha256:<64-hex>")
        return value


class ByocEvidenceCheckSummary(_StrictModel):
    total: int = Field(ge=0)
    required: int = Field(ge=0)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed_required: int = Field(ge=0)

    @model_validator(mode="after")
    def _counts_must_match_total(self) -> "ByocEvidenceCheckSummary":
        if self.passed + self.failed + self.skipped != self.total:
            raise ValueError("check status counts must sum to total")
        if self.required > self.total:
            raise ValueError("required check count must not exceed total")
        if self.failed_required > self.failed:
            raise ValueError("failed_required must not exceed failed")
        return self


class ByocEvidenceEntry(_StrictModel):
    kind: EvidenceKind
    status: EvidenceStatus
    required_checks_passed: bool
    observed_at: datetime
    source: ByocEvidenceSource
    check_summary: ByocEvidenceCheckSummary
    failed_check_codes: tuple[str, ...] = ()
    step_count: int | None = Field(default=None, ge=0)
    operation_counts: dict[str, int] = Field(default_factory=dict)
    signature_verified: bool | None = None
    envelope_digest: str | None = None

    @field_validator("failed_check_codes")
    @classmethod
    def _failed_codes_must_be_safe(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(code.strip() for code in value)
        if any(not code or not _SAFE_CODE_RE.match(code) for code in normalized):
            raise ValueError("failed check codes must be bounded identifiers")
        return normalized

    @field_validator("operation_counts")
    @classmethod
    def _operation_counts_must_be_safe(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, count in value.items():
            key = key.strip()
            if not key or not _SAFE_CODE_RE.match(key):
                raise ValueError("operation names must be bounded identifiers")
            if count < 0:
                raise ValueError("operation counts must be non-negative")
            normalized[key] = count
        return normalized

    @field_validator("envelope_digest")
    @classmethod
    def _envelope_digest_must_be_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("envelope_digest must look like sha256:<64-hex>")
        return value

    @model_validator(mode="after")
    def _status_must_match_required_checks(self) -> "ByocEvidenceEntry":
        if self.required_checks_passed and self.status == "fail":
            raise ValueError("passing required checks cannot have fail status")
        if not self.required_checks_passed and self.status == "pass":
            raise ValueError("failed required checks cannot have pass status")
        return self


class ByocDeploymentEvidenceLedger(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_ledger.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    generated_at: datetime
    generated_by: Literal["fyralis-core"] = "fyralis-core"
    export_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"
    overall_status: EvidenceStatus
    required_evidence_passed: bool
    privacy: ByocEvidencePrivacyContract
    evidence: tuple[ByocEvidenceEntry, ...]

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
            raise ValueError("ledger fields must not be empty")
        return value

    @field_validator("evidence")
    @classmethod
    def _evidence_must_be_present(
        cls,
        value: tuple[ByocEvidenceEntry, ...],
    ) -> tuple[ByocEvidenceEntry, ...]:
        if not value:
            raise ValueError("evidence must not be empty")
        return value

    @model_validator(mode="after")
    def _ledger_status_must_match_evidence(self) -> "ByocDeploymentEvidenceLedger":
        failed = any(entry.status in _BLOCKING_STATUSES for entry in self.evidence)
        if self.required_evidence_passed == failed:
            raise ValueError("required_evidence_passed must match evidence status")
        expected_status: EvidenceStatus = "fail" if failed else "pass"
        if self.overall_status != expected_status:
            raise ValueError("overall_status must match evidence status")
        return self


@dataclass(frozen=True, slots=True)
class ByocEvidenceLedgerViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def generate_evidence_ledger(
    *,
    plan: ByocBootstrapPlanManifest,
    dataplane_manifest: ByocDataPlaneManifest,
    permissions_manifest: ByocPermissionsManifest,
    bootstrap_bundle: ByocBootstrapBundleManifest,
    plan_path: Path,
    dataplane_manifest_path: Path,
    permissions_manifest_path: Path,
    bootstrap_bundle_path: Path,
    env_path: Path | None = None,
    post_deploy_report_path: Path | None = None,
    post_deploy_envelope_path: Path | None = None,
    evidence_signing_secret: str | None = None,
    envelope_verified_at: datetime | None = None,
    max_envelope_age_seconds: int = 86_400,
    generated_at: datetime | None = None,
    repo_root: Path | None = None,
) -> ByocDeploymentEvidenceLedger:
    repo_root = (repo_root or Path.cwd()).resolve()
    observed_at = generated_at or datetime.now(UTC)
    source_paths: PlanSourcePaths = {
        "dataplane": _repo_relative_path(dataplane_manifest_path, repo_root=repo_root),
        "permissions": _repo_relative_path(
            permissions_manifest_path,
            repo_root=repo_root,
        ),
        "bootstrap_bundle": _repo_relative_path(
            bootstrap_bundle_path,
            repo_root=repo_root,
        ),
    }
    plan_entry = _plan_evidence_entry(
        plan,
        dataplane_manifest=dataplane_manifest,
        permissions_manifest=permissions_manifest,
        bootstrap_bundle=bootstrap_bundle,
        plan_path=plan_path,
        source_paths=source_paths,
        observed_at=observed_at,
        repo_root=repo_root,
    )
    runner_report = run_byoc_bootstrap_runner(
        ByocBootstrapRunnerInputs(
            plan_path=plan_path,
            dataplane_manifest_path=dataplane_manifest_path,
            permissions_manifest_path=permissions_manifest_path,
            bootstrap_bundle_path=bootstrap_bundle_path,
            repo_root=repo_root,
            env_path=env_path,
        )
    )
    validation_entry = _post_deploy_validation_entry(
        dataplane_manifest_path=dataplane_manifest_path,
        env_path=env_path,
        post_deploy_report_path=post_deploy_report_path,
        post_deploy_envelope_path=post_deploy_envelope_path,
        manifest=dataplane_manifest,
        evidence_signing_secret=evidence_signing_secret,
        envelope_verified_at=envelope_verified_at or observed_at,
        max_envelope_age_seconds=max_envelope_age_seconds,
        observed_at=observed_at,
        repo_root=repo_root,
    )
    evidence = (
        plan_entry,
        _runner_evidence_entry(runner_report, observed_at=observed_at),
        validation_entry,
    )
    failed = any(entry.status in _BLOCKING_STATUSES for entry in evidence)
    return ByocDeploymentEvidenceLedger(
        schema_version="fyralis.byoc.evidence_ledger.v1",
        deployment_id=dataplane_manifest.deployment_id,
        customer_id=dataplane_manifest.customer_id,
        environment=dataplane_manifest.environment,
        cloud_provider=dataplane_manifest.cloud_provider,
        region=dataplane_manifest.region,
        artifact_revision=dataplane_manifest.artifact_revision,
        generated_at=observed_at,
        generated_by="fyralis-core",
        export_scope="sanitized_metadata_only",
        overall_status="fail" if failed else "pass",
        required_evidence_passed=not failed,
        privacy=ByocEvidencePrivacyContract(),
        evidence=evidence,
    )


def evidence_envelope_payload(
    *,
    manifest: ByocDataPlaneManifest,
    report_path: Path,
    agent_id: str,
    nonce: str,
    issued_at: datetime,
    expires_at: datetime,
) -> ByocEvidenceEnvelopePayload:
    return ByocEvidenceEnvelopePayload(
        schema_version="fyralis.byoc.evidence_envelope.v1",
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        agent_id=agent_id,
        artifact_revision=manifest.artifact_revision,
        cloud_provider=manifest.cloud_provider,
        region=manifest.region,
        report_kind="post_deploy_validation",
        report_digest=_file_digest(report_path),
        issued_at=issued_at,
        expires_at=expires_at,
        nonce=nonce,
        signing_key_ref=manifest.secrets.agent_client_certificate_secret_ref,
    )


def signed_evidence_envelope(
    payload: ByocEvidenceEnvelopePayload,
    *,
    signing_secret: str,
) -> ByocEvidenceEnvelope:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    signature = ByocEvidenceEnvelopeSignature(
        key_ref=payload.signing_key_ref,
        value=_hmac_sha256(canonical_evidence_envelope_payload(payload), signing_secret),
    )
    return ByocEvidenceEnvelope(**payload.model_dump(), signature=signature)


def canonical_evidence_envelope_payload(
    payload: ByocEvidenceEnvelopePayload,
) -> bytes:
    data = payload.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def load_evidence_envelope(path: Path) -> ByocEvidenceEnvelope:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC evidence envelope must be a JSON/YAML object")
    return ByocEvidenceEnvelope.model_validate(data)


def verify_evidence_envelope(
    envelope: ByocEvidenceEnvelope,
    *,
    report_path: Path,
    manifest: ByocDataPlaneManifest,
    signing_secret: str,
    verified_at: datetime | None = None,
    max_age_seconds: int = 86_400,
) -> list[ByocEvidenceLedgerViolation]:
    violations: list[ByocEvidenceLedgerViolation] = []
    now = verified_at or datetime.now(UTC)
    if not signing_secret:
        violations.append(
            _violation("signature", "missing_signing_secret", "signing secret is empty")
        )
        return violations

    violations.extend(_compare_envelope_identity(envelope, manifest))
    if envelope.signing_key_ref != manifest.secrets.agent_client_certificate_secret_ref:
        violations.append(
            _violation(
                "signing_key_ref",
                "signing_key_ref_mismatch",
                "envelope signing key ref does not match the data-plane manifest",
            )
        )
    if envelope.signature.key_ref != envelope.signing_key_ref:
        violations.append(
            _violation(
                "signature.key_ref",
                "signature_key_ref_mismatch",
                "signature key_ref must match signing_key_ref",
            )
        )
    if envelope.report_digest != _file_digest(report_path):
        violations.append(
            _violation(
                "report_digest",
                "report_digest_mismatch",
                "envelope report digest does not match the report file",
            )
        )
    if envelope.issued_at > now:
        violations.append(
            _violation("issued_at", "issued_in_future", "envelope is from the future")
        )
    if envelope.expires_at <= now:
        violations.append(
            _violation("expires_at", "envelope_expired", "evidence envelope expired")
        )
    if max_age_seconds > 0 and (now - envelope.issued_at).total_seconds() > max_age_seconds:
        violations.append(
            _violation("issued_at", "envelope_too_old", "evidence envelope is too old")
        )

    expected = _hmac_sha256(
        canonical_evidence_envelope_payload(_payload_from_envelope(envelope)),
        signing_secret,
    )
    if not hmac.compare_digest(expected, envelope.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )
    return violations


def _payload_from_envelope(
    envelope: ByocEvidenceEnvelope,
) -> ByocEvidenceEnvelopePayload:
    data = envelope.model_dump(exclude={"signature"})
    return ByocEvidenceEnvelopePayload.model_validate(data)


def _compare_envelope_identity(
    envelope: ByocEvidenceEnvelope,
    manifest: ByocDataPlaneManifest,
) -> list[ByocEvidenceLedgerViolation]:
    violations: list[ByocEvidenceLedgerViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "artifact_revision",
        "cloud_provider",
        "region",
    ):
        if getattr(envelope, field) != getattr(manifest, field):
            violations.append(
                _violation(
                    field,
                    "envelope_manifest_mismatch",
                    f"evidence envelope {field} does not match manifest",
                )
            )
    return violations


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def load_post_deploy_validation_report(path: Path) -> ByocImportedValidationReport:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC post-deploy validation report must be a JSON/YAML object")
    return ByocImportedValidationReport.model_validate(data)


def _post_deploy_validation_entry(
    *,
    dataplane_manifest_path: Path,
    env_path: Path | None,
    post_deploy_report_path: Path | None,
    post_deploy_envelope_path: Path | None,
    manifest: ByocDataPlaneManifest,
    evidence_signing_secret: str | None,
    envelope_verified_at: datetime,
    max_envelope_age_seconds: int,
    observed_at: datetime,
    repo_root: Path,
) -> ByocEvidenceEntry:
    if post_deploy_report_path is not None:
        envelope: ByocEvidenceEnvelope | None = None
        if post_deploy_envelope_path is not None:
            if evidence_signing_secret is None:
                raise ValueError(
                    "evidence signing secret is required with "
                    "--post-deploy-envelope"
                )
            envelope = load_evidence_envelope(post_deploy_envelope_path)
            violations = verify_evidence_envelope(
                envelope,
                report_path=post_deploy_report_path,
                manifest=manifest,
                signing_secret=evidence_signing_secret,
                verified_at=envelope_verified_at,
                max_age_seconds=max_envelope_age_seconds,
            )
            if violations:
                rendered = "; ".join(violation.render() for violation in violations)
                raise ValueError(f"evidence envelope verification failed: {rendered}")
        imported = load_post_deploy_validation_report(post_deploy_report_path)
        return _validation_evidence_entry(
            imported,
            observed_at=observed_at,
            source=ByocEvidenceSource(
                type=(
                    "signed_post_deploy_report_file"
                    if envelope is not None
                    else "post_deploy_report_file"
                ),
                ref=_evidence_source_ref(
                    post_deploy_report_path,
                    repo_root=repo_root,
                    external_ref="generated:external_post_deploy_report",
                ),
                digest=_validation_summary_digest(imported),
            ),
            signature_verified=envelope is not None,
            envelope_digest=(
                _file_digest(post_deploy_envelope_path)
                if post_deploy_envelope_path is not None
                else None
            ),
        )
    if post_deploy_envelope_path is not None:
        raise ValueError("--post-deploy-envelope requires --post-deploy-report")
    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=dataplane_manifest_path,
            env_path=env_path,
            require_live=False,
        )
    )
    return _validation_evidence_entry(
        report,
        observed_at=observed_at,
        source=ByocEvidenceSource(
            type="offline_validator",
            ref="generated:byoc_post_deploy_validation",
            digest=_validation_summary_digest(report),
        ),
        signature_verified=None,
        envelope_digest=None,
    )


def validate_evidence_ledger_contract(
    ledger: ByocDeploymentEvidenceLedger,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    plan: ByocBootstrapPlanManifest | None = None,
) -> list[ByocEvidenceLedgerViolation]:
    violations: list[ByocEvidenceLedgerViolation] = []
    if dataplane_manifest is not None:
        violations.extend(_compare_dataplane_manifest(ledger, dataplane_manifest))
    if plan is not None:
        violations.extend(_compare_plan_manifest(ledger, plan))

    kinds = [entry.kind for entry in ledger.evidence]
    duplicates = sorted({kind for kind in kinds if kinds.count(kind) > 1})
    for kind in duplicates:
        violations.append(
            _violation("evidence", "duplicate_evidence_kind", f"{kind!r} is duplicated")
        )
    missing = _REQUIRED_EVIDENCE_KINDS - set(kinds)
    for kind in sorted(missing):
        violations.append(
            _violation("evidence", "missing_required_evidence", f"{kind!r} is required")
        )
    for entry in ledger.evidence:
        if entry.source.digest is None:
            violations.append(
                _violation(
                    f"evidence.{entry.kind}.source.digest",
                    "evidence_digest_required",
                    "every evidence entry must include a sanitized digest",
                )
            )
        if entry.status == "fail" and not entry.failed_check_codes:
            violations.append(
                _violation(
                    f"evidence.{entry.kind}.failed_check_codes",
                    "failed_codes_required",
                    "failed evidence must include bounded failure codes",
                )
            )
        if entry.kind == "bootstrap_plan" and not entry.operation_counts:
            violations.append(
                _violation(
                    "evidence.bootstrap_plan.operation_counts",
                    "operation_counts_required",
                    "bootstrap plan evidence must include operation counts",
                )
            )
    return violations


def byoc_evidence_ledger_json_schema() -> dict[str, Any]:
    return ByocDeploymentEvidenceLedger.model_json_schema()


def load_byoc_evidence_ledger(path: Path) -> ByocDeploymentEvidenceLedger:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC evidence ledger must be a JSON/YAML object")
    return ByocDeploymentEvidenceLedger.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _plan_evidence_entry(
    plan: ByocBootstrapPlanManifest,
    *,
    dataplane_manifest: ByocDataPlaneManifest,
    permissions_manifest: ByocPermissionsManifest,
    bootstrap_bundle: ByocBootstrapBundleManifest,
    plan_path: Path,
    source_paths: PlanSourcePaths,
    observed_at: datetime,
    repo_root: Path,
) -> ByocEvidenceEntry:
    violations = validate_bootstrap_plan_contract(
        plan,
        dataplane_manifest=dataplane_manifest,
        permissions_manifest=permissions_manifest,
        bootstrap_bundle=bootstrap_bundle,
        source_paths=source_paths,
        repo_root=repo_root,
    )
    generated = generate_bootstrap_plan(
        dataplane_manifest=dataplane_manifest,
        permissions_manifest=permissions_manifest,
        bootstrap_bundle=bootstrap_bundle,
        source_paths=source_paths,
        generated_at=plan.generated_at,
        repo_root=repo_root,
    )
    failed_codes = tuple(sorted({violation.code for violation in violations}))
    contract_failed = bool(violations)
    drift_failed = plan.model_dump(mode="json") != generated.model_dump(mode="json")
    if drift_failed:
        failed_codes = tuple(sorted((*failed_codes, "generated_plan_drift")))
    failed = bool(failed_codes)
    failed_count = int(contract_failed) + int(drift_failed)
    passed_count = 2 - failed_count
    return ByocEvidenceEntry(
        kind="bootstrap_plan",
        status="fail" if failed else "pass",
        required_checks_passed=not failed,
        observed_at=observed_at,
        source=ByocEvidenceSource(
            type="file",
            ref=_repo_relative_path(plan_path, repo_root=repo_root).as_posix(),
            digest=_file_digest(_resolve_repo_path(plan_path, repo_root=repo_root)),
        ),
        check_summary=ByocEvidenceCheckSummary(
            total=2,
            required=2,
            passed=passed_count,
            failed=failed_count,
            skipped=0,
            failed_required=failed_count,
        ),
        failed_check_codes=failed_codes,
        step_count=len(plan.steps),
        operation_counts=dict(Counter(step.operation for step in plan.steps)),
    )


def _runner_evidence_entry(
    report: ByocBootstrapRunnerReport,
    *,
    observed_at: datetime,
) -> ByocEvidenceEntry:
    summary = _check_summary(report.checks)
    failed_codes = _failed_check_codes(report.checks)
    status: EvidenceStatus = "pass" if report.required_checks_passed else "fail"
    return ByocEvidenceEntry(
        kind="bootstrap_runner",
        status=status,
        required_checks_passed=report.required_checks_passed,
        observed_at=observed_at,
        source=ByocEvidenceSource(
            type="local_runner",
            ref="generated:byoc_bootstrap_runner",
            digest=_summary_digest(
                {
                    "checks": summary.model_dump(mode="json"),
                    "failed_check_codes": failed_codes,
                    "status": status,
                }
            ),
        ),
        check_summary=summary,
        failed_check_codes=failed_codes,
        step_count=sum(1 for check in report.checks if check.step_id),
        operation_counts=dict(
            Counter(
                str(check.operation)
                for check in report.checks
                if check.step_id and check.operation
            )
        ),
    )


def _validation_evidence_entry(
    report: ByocValidationReport | ByocImportedValidationReport,
    *,
    observed_at: datetime,
    source: ByocEvidenceSource,
    signature_verified: bool | None,
    envelope_digest: str | None,
) -> ByocEvidenceEntry:
    summary = _check_summary(report.checks)
    failed_codes = _failed_check_codes(report.checks)
    status: EvidenceStatus = "pass" if report.required_checks_passed else "fail"
    return ByocEvidenceEntry(
        kind="post_deploy_validation",
        status=status,
        required_checks_passed=report.required_checks_passed,
        observed_at=observed_at,
        source=source,
        check_summary=summary,
        failed_check_codes=failed_codes,
        signature_verified=signature_verified,
        envelope_digest=envelope_digest,
    )


def _validation_summary_digest(
    report: ByocValidationReport | ByocImportedValidationReport,
) -> str:
    summary = _check_summary(report.checks)
    failed_codes = _failed_check_codes(report.checks)
    status: EvidenceStatus = "pass" if report.required_checks_passed else "fail"
    return _summary_digest(
        {
            "checks": summary.model_dump(mode="json"),
            "failed_check_codes": failed_codes,
            "status": status,
        }
    )


def _check_summary(checks: Sequence[Any]) -> ByocEvidenceCheckSummary:
    statuses = Counter(str(check.status) for check in checks)
    return ByocEvidenceCheckSummary(
        total=len(checks),
        required=sum(1 for check in checks if bool(check.required)),
        passed=statuses["pass"],
        failed=statuses["fail"],
        skipped=statuses["skipped"],
        failed_required=sum(
            1
            for check in checks
            if bool(check.required) and str(check.status) == "fail"
        ),
    )


def _failed_check_codes(checks: Sequence[Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                _sanitize_code(str(check.name))
                for check in checks
                if str(check.status) == "fail"
            }
        )
    )


def _sanitize_code(value: str) -> str:
    value = value.strip().replace(" ", "_")
    if not _SAFE_CODE_RE.match(value):
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
        return f"unsafe_check_name:{digest}"
    return value


def _compare_dataplane_manifest(
    ledger: ByocDeploymentEvidenceLedger,
    dataplane_manifest: ByocDataPlaneManifest,
) -> list[ByocEvidenceLedgerViolation]:
    return _compare_identity(ledger, dataplane_manifest, "dataplane")


def _compare_plan_manifest(
    ledger: ByocDeploymentEvidenceLedger,
    plan: ByocBootstrapPlanManifest,
) -> list[ByocEvidenceLedgerViolation]:
    return _compare_identity(ledger, plan, "bootstrap_plan")


def _compare_identity(
    ledger: ByocDeploymentEvidenceLedger,
    source: Any,
    name: str,
) -> list[ByocEvidenceLedgerViolation]:
    violations: list[ByocEvidenceLedgerViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(ledger, field) != getattr(source, field):
            violations.append(
                _violation(
                    field,
                    f"{name}_mismatch",
                    f"evidence ledger {field} does not match {name}",
                )
            )
    return violations


def _repo_relative_path(path: Path, *, repo_root: Path) -> Path:
    resolved = _resolve_repo_path(path, repo_root=repo_root)
    try:
        return Path(resolved.resolve().relative_to(repo_root).as_posix())
    except ValueError:
        return path


def _evidence_source_ref(path: Path, *, repo_root: Path, external_ref: str) -> str:
    rel = _repo_relative_path(path, repo_root=repo_root)
    if rel.is_absolute() or ".." in rel.parts:
        return external_ref
    return rel.as_posix()


def _resolve_repo_path(path: Path, *, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _summary_digest(payload: Mapping[str, Any]) -> str:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocEvidenceLedgerViolation:
    return ByocEvidenceLedgerViolation(path=path, code=code, message=message)


def _load_mapping(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError(
            "YAML ledgers require PyYAML; use JSON or install the dev extras"
        ) from exc
    return yaml.safe_load(raw)


__all__ = [
    "ByocDeploymentEvidenceLedger",
    "ByocEvidenceCheckSummary",
    "ByocEvidenceEntry",
    "ByocEvidenceEnvelope",
    "ByocEvidenceEnvelopePayload",
    "ByocEvidenceEnvelopeSignature",
    "ByocEvidenceLedgerViolation",
    "ByocEvidencePrivacyContract",
    "ByocEvidenceSource",
    "ByocImportedValidationCheck",
    "ByocImportedValidationReport",
    "byoc_evidence_ledger_json_schema",
    "canonical_evidence_envelope_payload",
    "evidence_envelope_payload",
    "generate_evidence_ledger",
    "load_byoc_evidence_ledger",
    "load_evidence_envelope",
    "load_post_deploy_validation_report",
    "render_validation_errors",
    "signed_evidence_envelope",
    "validate_evidence_ledger_contract",
    "verify_evidence_envelope",
]
