"""BYOC control-plane intake contract for sanitized evidence packages."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Mapping, Protocol

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
READ_AUTH_KEY_REF_HEADER = "x-fyralis-byoc-read-key-ref"
READ_AUTH_TIMESTAMP_HEADER = "x-fyralis-byoc-read-timestamp"
READ_AUTH_NONCE_HEADER = "x-fyralis-byoc-read-nonce"
READ_AUTH_SIGNATURE_HEADER = "x-fyralis-byoc-read-signature"
READ_AUTH_MAX_CLOCK_SKEW_SECONDS = 300


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
    cloud_provider: CloudProvider
    region: str

    @field_validator("agent_version", "artifact_revision", "region")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("record fields must be bounded identifiers")
        return value


class ByocEvidencePackageReceiptQuery(_StrictModel):
    deployment_id: str | None = None
    customer_id: str | None = None
    limit: int = Field(default=50, ge=1, le=100)

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value

    @model_validator(mode="after")
    def _query_must_be_bounded(self) -> "ByocEvidencePackageReceiptQuery":
        if self.deployment_id is None and self.customer_id is None:
            raise ValueError("receipt queries must include deployment_id or customer_id")
        return self


class ByocEvidencePackageReceiptList(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_package_receipt_list.v1"]
    deployment_id: str | None = None
    customer_id: str | None = None
    limit: int
    result_count: int
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"
    items: tuple[ByocEvidencePackageIntakeRecord, ...]


class ByocEvidenceReceiptReadAuthPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.evidence_receipt_read_auth.v1"]
    method: Literal["GET"]
    path: str
    query: str = ""
    timestamp: datetime
    nonce: str = Field(min_length=16, max_length=128)

    @field_validator("path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith("/") or "://" in value:
            raise ValueError("path must be a relative request path")
        return value

    @field_validator("query")
    @classmethod
    def _query_must_not_include_url(cls, value: str) -> str:
        value = value.strip()
        if "://" in value:
            raise ValueError("query must not include URLs")
        return value

    @field_validator("nonce")
    @classmethod
    def _nonce_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("nonce must not be empty")
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

    async def list_receipts(
        self,
        query: ByocEvidencePackageReceiptQuery,
    ) -> ByocEvidencePackageReceiptList:
        ...


class InMemoryByocEvidencePackageIntakeStore:
    """Local sanitized intake store used by standalone contract tests."""

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
            cloud_provider=payload.cloud_provider,
            region=payload.region,
        )
        return receipt

    async def get(self, receipt_id: str) -> ByocEvidencePackageIntakeRecord | None:
        return self._records.get(receipt_id)

    async def list_receipts(
        self,
        query: ByocEvidencePackageReceiptQuery,
    ) -> ByocEvidencePackageReceiptList:
        records = [
            record
            for record in self._records.values()
            if _record_matches_receipt_query(record, query)
        ]
        records.sort(
            key=lambda record: (
                record.receipt.accepted_at,
                record.receipt.receipt_id,
            ),
            reverse=True,
        )
        items = tuple(records[: query.limit])
        return ByocEvidencePackageReceiptList(
            schema_version="fyralis.byoc.evidence_package_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


class PostgresByocEvidencePackageIntakeStore:
    """Postgres-backed sanitized receipt store.

    The store persists only scalar receipt metadata. It intentionally does not
    persist the evidence package body, ledger JSON, source artifacts, raw report
    JSON, or endpoint strings.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def put(
        self,
        request: ByocEvidencePackageSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocEvidencePackageReceipt:
        payload = _payload_from_request(request)
        receipt = evidence_package_receipt(payload, accepted_at=accepted_at)
        row = await self._pool.fetchrow(
            """
            INSERT INTO byoc_evidence_package_receipts (
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                package_digest,
                package_generated_at,
                ledger_overall_status,
                required_evidence_passed,
                live_report_envelope_digest,
                submitted_at,
                accepted_at,
                stored_scope
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16
            )
            ON CONFLICT (receipt_id) DO UPDATE
              SET receipt_id = byoc_evidence_package_receipts.receipt_id
            RETURNING
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                package_digest,
                package_generated_at,
                ledger_overall_status,
                required_evidence_passed,
                live_report_envelope_digest,
                submitted_at,
                accepted_at,
                stored_scope
            """,
            receipt.receipt_id,
            receipt.deployment_id,
            receipt.customer_id,
            receipt.agent_id,
            payload.agent_version,
            payload.artifact_revision,
            payload.cloud_provider,
            payload.region,
            receipt.package_digest,
            receipt.package_generated_at,
            receipt.ledger_overall_status,
            receipt.required_evidence_passed,
            receipt.live_report_envelope_digest,
            payload.submitted_at,
            receipt.accepted_at,
            receipt.stored_scope,
        )
        return _record_from_row(row).receipt

    async def get(self, receipt_id: str) -> ByocEvidencePackageIntakeRecord | None:
        row = await self._pool.fetchrow(
            """
            SELECT
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                package_digest,
                package_generated_at,
                ledger_overall_status,
                required_evidence_passed,
                live_report_envelope_digest,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_evidence_package_receipts
            WHERE receipt_id = $1
            """,
            receipt_id,
        )
        if row is None:
            return None
        return _record_from_row(row)

    async def list_receipts(
        self,
        query: ByocEvidencePackageReceiptQuery,
    ) -> ByocEvidencePackageReceiptList:
        where_clauses: list[str] = []
        args: list[Any] = []
        if query.deployment_id is not None:
            args.append(query.deployment_id)
            where_clauses.append(f"deployment_id = ${len(args)}")
        if query.customer_id is not None:
            args.append(query.customer_id)
            where_clauses.append(f"customer_id = ${len(args)}")
        args.append(query.limit)
        rows = await self._pool.fetch(
            f"""
            SELECT
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                package_digest,
                package_generated_at,
                ledger_overall_status,
                required_evidence_passed,
                live_report_envelope_digest,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_evidence_package_receipts
            WHERE {' AND '.join(where_clauses)}
            ORDER BY accepted_at DESC, receipt_id DESC
            LIMIT ${len(args)}
            """,
            *args,
        )
        items = tuple(_record_from_row(row) for row in rows)
        return ByocEvidencePackageReceiptList(
            schema_version="fyralis.byoc.evidence_package_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


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


def signed_evidence_receipt_read_headers(
    *,
    method: str,
    path: str,
    query: str = "",
    signing_secret: str,
    key_ref: str,
    nonce: str,
    timestamp: datetime | None = None,
) -> dict[str, str]:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    payload = evidence_receipt_read_auth_payload(
        method=method,
        path=path,
        query=query,
        nonce=nonce,
        timestamp=timestamp,
    )
    return {
        READ_AUTH_KEY_REF_HEADER: key_ref,
        READ_AUTH_TIMESTAMP_HEADER: payload.timestamp.isoformat(),
        READ_AUTH_NONCE_HEADER: payload.nonce,
        READ_AUTH_SIGNATURE_HEADER: _hmac_sha256(
            canonical_evidence_receipt_read_auth_payload(payload),
            signing_secret,
        ),
    }


def canonical_evidence_package_submission_payload(
    payload: ByocEvidencePackageSubmissionPayload,
) -> bytes:
    data = payload.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def evidence_receipt_read_auth_payload(
    *,
    method: str,
    path: str,
    query: str = "",
    nonce: str,
    timestamp: datetime | None = None,
) -> ByocEvidenceReceiptReadAuthPayload:
    return ByocEvidenceReceiptReadAuthPayload(
        schema_version="fyralis.byoc.evidence_receipt_read_auth.v1",
        method=method.upper(),
        path=path,
        query=query,
        timestamp=timestamp or datetime.now(UTC),
        nonce=nonce,
    )


def canonical_evidence_receipt_read_auth_payload(
    payload: ByocEvidenceReceiptReadAuthPayload,
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


def validate_evidence_receipt_read_auth_headers(
    headers: Mapping[str, str],
    *,
    method: str,
    path: str,
    query: str,
    signing_secret: str,
    expected_key_ref: str | None = None,
    now: datetime | None = None,
    max_clock_skew_seconds: int = READ_AUTH_MAX_CLOCK_SKEW_SECONDS,
) -> list[ByocEvidencePackageIntakeViolation]:
    violations: list[ByocEvidencePackageIntakeViolation] = []
    if not signing_secret:
        return [
            _violation("read_auth", "missing_signing_secret", "signing secret is empty")
        ]

    key_ref = _header_value(headers, READ_AUTH_KEY_REF_HEADER)
    timestamp_value = _header_value(headers, READ_AUTH_TIMESTAMP_HEADER)
    nonce = _header_value(headers, READ_AUTH_NONCE_HEADER)
    signature = _header_value(headers, READ_AUTH_SIGNATURE_HEADER)
    missing = [
        name
        for name, value in (
            (READ_AUTH_KEY_REF_HEADER, key_ref),
            (READ_AUTH_TIMESTAMP_HEADER, timestamp_value),
            (READ_AUTH_NONCE_HEADER, nonce),
            (READ_AUTH_SIGNATURE_HEADER, signature),
        )
        if value is None or not value.strip()
    ]
    if missing:
        return [
            _violation(
                "read_auth.headers",
                "missing_read_auth_headers",
                "signed receipt read headers are required",
            )
        ]

    assert key_ref is not None
    assert timestamp_value is not None
    assert nonce is not None
    assert signature is not None
    key_ref = key_ref.strip()
    signature = signature.strip().lower()
    if expected_key_ref is not None and key_ref != expected_key_ref:
        violations.append(
            _violation(
                "read_auth.key_ref",
                "signature_key_ref_mismatch",
                "signature key_ref does not match expected read key",
            )
        )
    if len(signature) != 64:
        violations.append(
            _violation(
                "read_auth.signature",
                "invalid_signature_shape",
                "read signature must be a SHA-256 hex digest",
            )
        )
        return violations
    try:
        int(signature, 16)
    except ValueError:
        violations.append(
            _violation(
                "read_auth.signature",
                "invalid_signature_shape",
                "read signature must be hex encoded",
            )
        )
        return violations

    try:
        timestamp = datetime.fromisoformat(timestamp_value.strip())
    except ValueError:
        return [
            _violation(
                "read_auth.timestamp",
                "invalid_timestamp",
                "read auth timestamp must be ISO-8601",
            )
        ]
    if timestamp.tzinfo is None:
        return [
            _violation(
                "read_auth.timestamp",
                "invalid_timestamp",
                "read auth timestamp must include a timezone",
            )
        ]
    current_time = now or datetime.now(UTC)
    current_time = current_time.astimezone(UTC)
    timestamp = timestamp.astimezone(UTC)
    skew_seconds = abs((current_time - timestamp).total_seconds())
    if skew_seconds > max_clock_skew_seconds:
        violations.append(
            _violation(
                "read_auth.timestamp",
                "stale_read_auth",
                "read auth timestamp is outside the allowed freshness window",
            )
        )

    try:
        payload = evidence_receipt_read_auth_payload(
            method=method,
            path=path,
            query=query,
            nonce=nonce,
            timestamp=timestamp,
        )
    except ValueError as exc:
        violations.append(
            _violation("read_auth.payload", "invalid_read_auth_payload", str(exc))
        )
        return violations
    expected = _hmac_sha256(
        canonical_evidence_receipt_read_auth_payload(payload),
        signing_secret,
    )
    if not hmac.compare_digest(expected, signature):
        violations.append(
            _violation("read_auth.signature", "invalid_signature", "invalid signature")
        )
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
        "receipt_list": ByocEvidencePackageReceiptList.model_json_schema(),
        "receipt_query": ByocEvidencePackageReceiptQuery.model_json_schema(),
        "receipt_read_auth": ByocEvidenceReceiptReadAuthPayload.model_json_schema(),
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


def _record_from_row(row: Any) -> ByocEvidencePackageIntakeRecord:
    data = dict(row)
    receipt = ByocEvidencePackageReceipt(
        schema_version="fyralis.byoc.evidence_package_receipt.v1",
        status="accepted",
        receipt_id=data["receipt_id"],
        deployment_id=data["deployment_id"],
        customer_id=data["customer_id"],
        agent_id=data["agent_id"],
        package_digest=data["package_digest"],
        package_generated_at=data["package_generated_at"],
        ledger_overall_status=data["ledger_overall_status"],
        required_evidence_passed=data["required_evidence_passed"],
        live_report_envelope_digest=data["live_report_envelope_digest"],
        accepted_at=data["accepted_at"],
        stored_scope=data["stored_scope"],
    )
    return ByocEvidencePackageIntakeRecord(
        receipt=receipt,
        submitted_at=data["submitted_at"],
        agent_version=data["agent_version"],
        artifact_revision=data["artifact_revision"],
        cloud_provider=data["cloud_provider"],
        region=data["region"],
    )


def _record_matches_receipt_query(
    record: ByocEvidencePackageIntakeRecord,
    query: ByocEvidencePackageReceiptQuery,
) -> bool:
    if (
        query.deployment_id is not None
        and record.receipt.deployment_id != query.deployment_id
    ):
        return False
    if query.customer_id is not None and record.receipt.customer_id != query.customer_id:
        return False
    return True


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name)
    if value is not None:
        return value
    return headers.get(name.lower())


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
    "ByocEvidencePackageReceiptList",
    "ByocEvidencePackageReceiptQuery",
    "ByocEvidenceReceiptReadAuthPayload",
    "ByocEvidencePackageSubmissionPayload",
    "ByocEvidencePackageSubmissionRequest",
    "InMemoryByocEvidencePackageIntakeStore",
    "PostgresByocEvidencePackageIntakeStore",
    "canonical_evidence_receipt_read_auth_payload",
    "canonical_evidence_package_submission_payload",
    "digest_evidence_package",
    "evidence_receipt_read_auth_payload",
    "evidence_package_receipt",
    "evidence_package_submission_payload",
    "model_json_schema_bundle",
    "signed_evidence_receipt_read_headers",
    "signed_evidence_package_submission",
    "validate_evidence_receipt_read_auth_headers",
    "validate_evidence_package_submission",
]
