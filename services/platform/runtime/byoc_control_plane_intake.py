"""BYOC control-plane intake contract for sanitized evidence packages."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic import model_validator

from services.platform.runtime.byoc_contract import CloudProvider
from services.platform.runtime.byoc_evidence_package import (
    ByocEvidencePackage,
    validate_evidence_package_contract,
)


_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_RECEIPT_ID_RE = re.compile(r"^evpkg_[0-9a-f]{32}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_VALUE_FRAGMENTS = (
    "://",
    "bearer ",
    "password=",
    "postgresql://",
    "secret=",
    "token=",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocControlPlaneSignature(_StrictModel):
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


class ByocEvidencePackageSubmissionPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_package_submission.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    cloud_provider: CloudProvider
    region: str
    submitted_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    package: ByocEvidencePackage

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

    @field_validator("agent_version", "artifact_revision", "region")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("submission fields must be bounded identifiers")
        return value

    @field_validator("nonce")
    @classmethod
    def _nonce_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("nonce must not be empty")
        return value

    @model_validator(mode="after")
    def _identity_must_match_package(
        self,
    ) -> "ByocEvidencePackageSubmissionPayload":
        for field in (
            "deployment_id",
            "customer_id",
            "artifact_revision",
            "cloud_provider",
            "region",
        ):
            if getattr(self, field) != getattr(self.package, field):
                raise ValueError(f"submission {field} must match evidence package")
        return self


class ByocEvidencePackageSubmissionRequest(ByocEvidencePackageSubmissionPayload):
    signature: ByocControlPlaneSignature


class ByocEvidencePackageReceipt(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_package_receipt.v1"]
    status: Literal["accepted"] = "accepted"
    receipt_id: str
    deployment_id: str
    customer_id: str
    agent_id: str
    package_digest: str
    package_generated_at: datetime
    ledger_overall_status: Literal["pass", "fail", "skipped"]
    required_evidence_passed: bool
    live_report_envelope_digest: str | None = None
    accepted_at: datetime
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"

    @field_validator("receipt_id")
    @classmethod
    def _receipt_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _RECEIPT_ID_RE.match(value):
            raise ValueError("receipt_id must look like evpkg_<32-hex>")
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

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("package_digest", "live_report_envelope_digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("receipt digest must look like sha256:<64-hex>")
        return value


class ByocEvidencePackageIntakeRecord(_StrictModel):
    receipt: ByocEvidencePackageReceipt
    submitted_at: datetime
    agent_version: str
    artifact_revision: str

    @field_validator("agent_version", "artifact_revision")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("record fields must be bounded identifiers")
        return value


@dataclass(frozen=True, slots=True)
class ByocEvidencePackageIntakeViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


class ByocEvidencePackageIntakeStore(Protocol):
    async def put(
        self,
        request: ByocEvidencePackageSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocEvidencePackageReceipt:
        ...

    async def get(self, receipt_id: str) -> ByocEvidencePackageIntakeRecord | None:
        ...


class InMemoryByocEvidencePackageIntakeStore:
    """Local sanitized intake store used until hosted persistence exists."""

    def __init__(self) -> None:
        self._records: dict[str, ByocEvidencePackageIntakeRecord] = {}

    @property
    def records(self) -> tuple[ByocEvidencePackageIntakeRecord, ...]:
        return tuple(self._records.values())

    async def put(
        self,
        request: ByocEvidencePackageSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocEvidencePackageReceipt:
        payload = _payload_from_request(request)
        receipt = evidence_package_receipt(payload, accepted_at=accepted_at)
        self._records[receipt.receipt_id] = ByocEvidencePackageIntakeRecord(
            receipt=receipt,
            submitted_at=payload.submitted_at,
            agent_version=payload.agent_version,
            artifact_revision=payload.artifact_revision,
        )
        return receipt

    async def get(self, receipt_id: str) -> ByocEvidencePackageIntakeRecord | None:
        return self._records.get(receipt_id)


def evidence_package_submission_payload(
    *,
    package: ByocEvidencePackage,
    agent_id: str,
    agent_version: str,
    nonce: str,
    submitted_at: datetime | None = None,
) -> ByocEvidencePackageSubmissionPayload:
    return ByocEvidencePackageSubmissionPayload(
        schema_version="fyralis.byoc.evidence_package_submission.v1",
        deployment_id=package.deployment_id,
        customer_id=package.customer_id,
        agent_id=agent_id,
        agent_version=agent_version,
        artifact_revision=package.artifact_revision,
        cloud_provider=package.cloud_provider,
        region=package.region,
        submitted_at=submitted_at or datetime.now(UTC),
        nonce=nonce,
        package=package,
    )


def signed_evidence_package_submission(
    payload: ByocEvidencePackageSubmissionPayload,
    *,
    signing_secret: str,
    key_ref: str,
) -> ByocEvidencePackageSubmissionRequest:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    signature = ByocControlPlaneSignature(
        key_ref=key_ref,
        value=_hmac_sha256(
            canonical_evidence_package_submission_payload(payload),
            signing_secret,
        ),
    )
    return ByocEvidencePackageSubmissionRequest(
        **payload.model_dump(),
        signature=signature,
    )


def canonical_evidence_package_submission_payload(
    payload: ByocEvidencePackageSubmissionPayload,
) -> bytes:
    data = payload.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def validate_evidence_package_submission(
    request: ByocEvidencePackageSubmissionRequest,
    *,
    signing_secret: str,
    expected_key_ref: str | None = None,
) -> list[ByocEvidencePackageIntakeViolation]:
    violations: list[ByocEvidencePackageIntakeViolation] = []
    if not signing_secret:
        violations.append(
            _violation("signature", "missing_signing_secret", "signing secret is empty")
        )
        return violations

    if expected_key_ref is not None and request.signature.key_ref != expected_key_ref:
        violations.append(
            _violation(
                "signature.key_ref",
                "signature_key_ref_mismatch",
                "signature key_ref does not match expected intake key",
            )
        )

    expected = _hmac_sha256(
        canonical_evidence_package_submission_payload(_payload_from_request(request)),
        signing_secret,
    )
    if not hmac.compare_digest(expected, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )

    for violation in validate_evidence_package_contract(request.package):
        violations.append(
            _violation(f"package.{violation.path}", violation.code, violation.message)
        )
    violations.extend(_privacy_boundary_violations(_payload_from_request(request)))
    return violations


def evidence_package_receipt(
    payload: ByocEvidencePackageSubmissionPayload,
    *,
    accepted_at: datetime | None = None,
) -> ByocEvidencePackageReceipt:
    accepted = accepted_at or datetime.now(UTC)
    package_digest = digest_evidence_package(payload.package)
    receipt_id = "evpkg_" + hashlib.sha256(
        canonical_evidence_package_submission_payload(payload)
    ).hexdigest()[:32]
    envelope_digest = (
        payload.package.live_report_envelope.envelope_digest
        if payload.package.live_report_envelope is not None
        else None
    )
    return ByocEvidencePackageReceipt(
        schema_version="fyralis.byoc.evidence_package_receipt.v1",
        status="accepted",
        receipt_id=receipt_id,
        deployment_id=payload.deployment_id,
        customer_id=payload.customer_id,
        agent_id=payload.agent_id,
        package_digest=package_digest,
        package_generated_at=payload.package.generated_at,
        ledger_overall_status=payload.package.ledger.overall_status,
        required_evidence_passed=payload.package.ledger.required_evidence_passed,
        live_report_envelope_digest=envelope_digest,
        accepted_at=accepted,
        stored_scope="sanitized_metadata_only",
    )


def digest_evidence_package(package: ByocEvidencePackage) -> str:
    data = package.model_dump(mode="json", exclude_none=True)
    rendered = json.dumps(data, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "submission_payload": ByocEvidencePackageSubmissionPayload.model_json_schema(),
        "submission_request": ByocEvidencePackageSubmissionRequest.model_json_schema(),
        "receipt": ByocEvidencePackageReceipt.model_json_schema(),
        "intake_record": ByocEvidencePackageIntakeRecord.model_json_schema(),
    }


def _payload_from_request(
    request: ByocEvidencePackageSubmissionRequest,
) -> ByocEvidencePackageSubmissionPayload:
    data = request.model_dump(exclude={"signature"})
    return ByocEvidencePackageSubmissionPayload.model_validate(data)


def _privacy_boundary_violations(
    payload: ByocEvidencePackageSubmissionPayload,
) -> list[ByocEvidencePackageIntakeViolation]:
    violations: list[ByocEvidencePackageIntakeViolation] = []
    _scan_value(payload.model_dump(mode="json"), path="<root>", violations=violations)
    return violations


def _scan_value(
    value: Any,
    *,
    path: str,
    violations: list[ByocEvidencePackageIntakeViolation],
) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _scan_value(item, path=f"{path}.{key}", violations=violations)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _scan_value(item, path=f"{path}[{index}]", violations=violations)
        return
    if not isinstance(value, str):
        return
    lowered = value.lower()
    if any(fragment in lowered for fragment in _FORBIDDEN_VALUE_FRAGMENTS):
        violations.append(
            _violation(
                path,
                "customer_data_marker_forbidden",
                "submission contains a raw data, URL, credential, or token marker",
            )
        )


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocEvidencePackageIntakeViolation:
    return ByocEvidencePackageIntakeViolation(path=path, code=code, message=message)


__all__ = [
    "ByocControlPlaneSignature",
    "ByocEvidencePackageIntakeRecord",
    "ByocEvidencePackageIntakeStore",
    "ByocEvidencePackageIntakeViolation",
    "ByocEvidencePackageReceipt",
    "ByocEvidencePackageSubmissionPayload",
    "ByocEvidencePackageSubmissionRequest",
    "InMemoryByocEvidencePackageIntakeStore",
    "canonical_evidence_package_submission_payload",
    "digest_evidence_package",
    "evidence_package_receipt",
    "evidence_package_submission_payload",
    "model_json_schema_bundle",
    "signed_evidence_package_submission",
    "validate_evidence_package_submission",
]
