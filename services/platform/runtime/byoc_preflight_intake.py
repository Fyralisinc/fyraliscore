"""BYOC control-plane intake contract for sanitized preflight reports."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.platform.runtime.byoc_control_plane_intake import (
    ByocControlPlaneSignature,
)
from services.platform.runtime.byoc_contract import CloudProvider
from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleReport,
    PreflightStatus,
)


_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_RECEIPT_ID_RE = re.compile(r"^pfrep_[0-9a-f]{32}$")
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


class ByocPreflightReportSubmissionPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.preflight_report_submission.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    cloud_provider: CloudProvider
    region: str
    submitted_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    preflight_report: ByocPreflightBundleReport

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
            raise ValueError("preflight submission fields must be bounded identifiers")
        return value

    @field_validator("nonce")
    @classmethod
    def _nonce_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("nonce must not be empty")
        return value

    @model_validator(mode="after")
    def _identity_must_match_report(
        self,
    ) -> "ByocPreflightReportSubmissionPayload":
        for field in (
            "deployment_id",
            "customer_id",
            "artifact_revision",
            "cloud_provider",
            "region",
        ):
            if getattr(self, field) != getattr(self.preflight_report, field):
                raise ValueError(f"submission {field} must match preflight report")
        return self


class ByocPreflightReportSubmissionRequest(ByocPreflightReportSubmissionPayload):
    signature: ByocControlPlaneSignature


class ByocPreflightReportReceipt(_StrictModel):
    schema_version: Literal["fyralis.byoc.preflight_report_receipt.v1"]
    status: Literal["accepted"] = "accepted"
    receipt_id: str
    deployment_id: str
    customer_id: str
    agent_id: str
    report_digest: str
    preflight_status: PreflightStatus
    required_sections_passed: bool
    section_count: int = Field(ge=0)
    failed_section_count: int = Field(ge=0)
    terraform_validate_executed: bool
    submitted_at: datetime
    accepted_at: datetime
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"

    @field_validator("receipt_id")
    @classmethod
    def _receipt_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _RECEIPT_ID_RE.match(value):
            raise ValueError("receipt_id must look like pfrep_<32-hex>")
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

    @field_validator("report_digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("report_digest must look like sha256:<64-hex>")
        return value


class ByocPreflightReportIntakeRecord(_StrictModel):
    receipt: ByocPreflightReportReceipt
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


class ByocPreflightReportReceiptQuery(_StrictModel):
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
    def _query_must_be_bounded(self) -> "ByocPreflightReportReceiptQuery":
        if self.deployment_id is None and self.customer_id is None:
            raise ValueError(
                "preflight receipt queries must include deployment_id or customer_id"
            )
        return self


class ByocPreflightReportReceiptList(_StrictModel):
    schema_version: Literal["fyralis.byoc.preflight_report_receipt_list.v1"]
    deployment_id: str | None = None
    customer_id: str | None = None
    limit: int
    result_count: int
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"
    items: tuple[ByocPreflightReportIntakeRecord, ...]


@dataclass(frozen=True, slots=True)
class ByocPreflightReportIntakeViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


class ByocPreflightReportIntakeStore(Protocol):
    async def put(
        self,
        request: ByocPreflightReportSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocPreflightReportReceipt:
        ...

    async def get(self, receipt_id: str) -> ByocPreflightReportIntakeRecord | None:
        ...

    async def list_receipts(
        self,
        query: ByocPreflightReportReceiptQuery,
    ) -> ByocPreflightReportReceiptList:
        ...


class InMemoryByocPreflightReportIntakeStore:
    """Local sanitized preflight receipt store used by contract tests."""

    def __init__(self) -> None:
        self._records: dict[str, ByocPreflightReportIntakeRecord] = {}

    @property
    def records(self) -> tuple[ByocPreflightReportIntakeRecord, ...]:
        return tuple(self._records.values())

    async def put(
        self,
        request: ByocPreflightReportSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocPreflightReportReceipt:
        payload = _payload_from_request(request)
        receipt = preflight_report_receipt(payload, accepted_at=accepted_at)
        self._records[receipt.receipt_id] = ByocPreflightReportIntakeRecord(
            receipt=receipt,
            agent_version=payload.agent_version,
            artifact_revision=payload.artifact_revision,
            cloud_provider=payload.cloud_provider,
            region=payload.region,
        )
        return receipt

    async def get(self, receipt_id: str) -> ByocPreflightReportIntakeRecord | None:
        return self._records.get(receipt_id)

    async def list_receipts(
        self,
        query: ByocPreflightReportReceiptQuery,
    ) -> ByocPreflightReportReceiptList:
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
        return ByocPreflightReportReceiptList(
            schema_version="fyralis.byoc.preflight_report_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


class PostgresByocPreflightReportIntakeStore:
    """Postgres-backed sanitized preflight receipt store.

    The store persists only scalar metadata. It intentionally does not persist
    preflight report bodies, child reports, section details, command output,
    URLs, logs, payloads, prompts, PII, or credential material.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def put(
        self,
        request: ByocPreflightReportSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocPreflightReportReceipt:
        payload = _payload_from_request(request)
        receipt = preflight_report_receipt(payload, accepted_at=accepted_at)
        row = await self._pool.fetchrow(
            """
            INSERT INTO byoc_preflight_report_receipts (
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                report_digest,
                preflight_status,
                required_sections_passed,
                section_count,
                failed_section_count,
                terraform_validate_executed,
                submitted_at,
                accepted_at,
                stored_scope
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            ON CONFLICT (receipt_id) DO UPDATE
              SET receipt_id = byoc_preflight_report_receipts.receipt_id
            RETURNING
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                report_digest,
                preflight_status,
                required_sections_passed,
                section_count,
                failed_section_count,
                terraform_validate_executed,
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
            receipt.report_digest,
            receipt.preflight_status,
            receipt.required_sections_passed,
            receipt.section_count,
            receipt.failed_section_count,
            receipt.terraform_validate_executed,
            receipt.submitted_at,
            receipt.accepted_at,
            receipt.stored_scope,
        )
        return _record_from_row(row).receipt

    async def get(self, receipt_id: str) -> ByocPreflightReportIntakeRecord | None:
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
                report_digest,
                preflight_status,
                required_sections_passed,
                section_count,
                failed_section_count,
                terraform_validate_executed,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_preflight_report_receipts
            WHERE receipt_id = $1
            """,
            receipt_id,
        )
        if row is None:
            return None
        return _record_from_row(row)

    async def list_receipts(
        self,
        query: ByocPreflightReportReceiptQuery,
    ) -> ByocPreflightReportReceiptList:
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
                report_digest,
                preflight_status,
                required_sections_passed,
                section_count,
                failed_section_count,
                terraform_validate_executed,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_preflight_report_receipts
            WHERE {' AND '.join(where_clauses)}
            ORDER BY accepted_at DESC, receipt_id DESC
            LIMIT ${len(args)}
            """,
            *args,
        )
        items = tuple(_record_from_row(row) for row in rows)
        return ByocPreflightReportReceiptList(
            schema_version="fyralis.byoc.preflight_report_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


def preflight_report_submission_payload(
    *,
    preflight_report: ByocPreflightBundleReport,
    agent_id: str,
    agent_version: str,
    nonce: str,
    submitted_at: datetime | None = None,
) -> ByocPreflightReportSubmissionPayload:
    return ByocPreflightReportSubmissionPayload(
        schema_version="fyralis.byoc.preflight_report_submission.v1",
        deployment_id=str(preflight_report.deployment_id),
        customer_id=str(preflight_report.customer_id),
        agent_id=agent_id,
        agent_version=agent_version,
        artifact_revision=str(preflight_report.artifact_revision),
        cloud_provider=preflight_report.cloud_provider,  # type: ignore[arg-type]
        region=str(preflight_report.region),
        submitted_at=submitted_at or datetime.now(UTC),
        nonce=nonce,
        preflight_report=preflight_report,
    )


def signed_preflight_report_submission(
    payload: ByocPreflightReportSubmissionPayload,
    *,
    signing_secret: str,
    key_ref: str,
) -> ByocPreflightReportSubmissionRequest:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    signature = ByocControlPlaneSignature(
        key_ref=key_ref,
        value=_hmac_sha256(
            canonical_preflight_report_submission_payload(payload),
            signing_secret,
        ),
    )
    return ByocPreflightReportSubmissionRequest(
        **payload.model_dump(),
        signature=signature,
    )


def canonical_preflight_report_submission_payload(
    payload: ByocPreflightReportSubmissionPayload,
) -> bytes:
    return _canonical_json(payload)


def preflight_report_receipt(
    payload: ByocPreflightReportSubmissionPayload,
    *,
    accepted_at: datetime | None = None,
) -> ByocPreflightReportReceipt:
    report_digest = digest_preflight_report(payload.preflight_report)
    receipt_id = "pfrep_" + hashlib.sha256(
        canonical_preflight_report_submission_payload(payload)
    ).hexdigest()[:32]
    failed_sections = sum(
        1 for section in payload.preflight_report.sections if section.status == "fail"
    )
    return ByocPreflightReportReceipt(
        schema_version="fyralis.byoc.preflight_report_receipt.v1",
        status="accepted",
        receipt_id=receipt_id,
        deployment_id=payload.deployment_id,
        customer_id=payload.customer_id,
        agent_id=payload.agent_id,
        report_digest=report_digest,
        preflight_status=payload.preflight_report.status,
        required_sections_passed=payload.preflight_report.required_sections_passed,
        section_count=len(payload.preflight_report.sections),
        failed_section_count=failed_sections,
        terraform_validate_executed=payload.preflight_report.terraform_validate_executed,
        submitted_at=payload.submitted_at,
        accepted_at=accepted_at or datetime.now(UTC),
        stored_scope="sanitized_metadata_only",
    )


def digest_preflight_report(report: ByocPreflightBundleReport) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(report)).hexdigest()


def validate_preflight_report_submission(
    request: ByocPreflightReportSubmissionRequest,
    *,
    signing_secret: str,
    expected_key_ref: str | None = None,
) -> list[ByocPreflightReportIntakeViolation]:
    violations: list[ByocPreflightReportIntakeViolation] = []
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
        canonical_preflight_report_submission_payload(_payload_from_request(request)),
        signing_secret,
    )
    if not hmac.compare_digest(expected, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )

    violations.extend(_report_contract_violations(request.preflight_report))
    violations.extend(_privacy_boundary_violations(_payload_from_request(request)))
    return violations


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "submission_payload": ByocPreflightReportSubmissionPayload.model_json_schema(),
        "submission_request": ByocPreflightReportSubmissionRequest.model_json_schema(),
        "receipt": ByocPreflightReportReceipt.model_json_schema(),
        "receipt_list": ByocPreflightReportReceiptList.model_json_schema(),
        "receipt_query": ByocPreflightReportReceiptQuery.model_json_schema(),
        "intake_record": ByocPreflightReportIntakeRecord.model_json_schema(),
    }


def _payload_from_request(
    request: ByocPreflightReportSubmissionRequest,
) -> ByocPreflightReportSubmissionPayload:
    data = request.model_dump(exclude={"signature"})
    return ByocPreflightReportSubmissionPayload.model_validate(data)


def _record_from_row(row: Any) -> ByocPreflightReportIntakeRecord:
    data = dict(row)
    receipt = ByocPreflightReportReceipt(
        schema_version="fyralis.byoc.preflight_report_receipt.v1",
        status="accepted",
        receipt_id=data["receipt_id"],
        deployment_id=data["deployment_id"],
        customer_id=data["customer_id"],
        agent_id=data["agent_id"],
        report_digest=data["report_digest"],
        preflight_status=data["preflight_status"],
        required_sections_passed=data["required_sections_passed"],
        section_count=data["section_count"],
        failed_section_count=data["failed_section_count"],
        terraform_validate_executed=data["terraform_validate_executed"],
        submitted_at=data["submitted_at"],
        accepted_at=data["accepted_at"],
        stored_scope=data["stored_scope"],
    )
    return ByocPreflightReportIntakeRecord(
        receipt=receipt,
        agent_version=data["agent_version"],
        artifact_revision=data["artifact_revision"],
        cloud_provider=data["cloud_provider"],
        region=data["region"],
    )


def _record_matches_receipt_query(
    record: ByocPreflightReportIntakeRecord,
    query: ByocPreflightReportReceiptQuery,
) -> bool:
    if (
        query.deployment_id is not None
        and record.receipt.deployment_id != query.deployment_id
    ):
        return False
    if query.customer_id is not None and record.receipt.customer_id != query.customer_id:
        return False
    return True


def _report_contract_violations(
    report: ByocPreflightBundleReport,
) -> list[ByocPreflightReportIntakeViolation]:
    violations: list[ByocPreflightReportIntakeViolation] = []
    if report.required_sections_passed and report.status == "fail":
        violations.append(
            _violation(
                "preflight_report.status",
                "status_required_sections_mismatch",
                "passing required sections cannot have fail status",
            )
        )
    if not report.required_sections_passed and report.status == "pass":
        violations.append(
            _violation(
                "preflight_report.status",
                "status_required_sections_mismatch",
                "failed required sections cannot have pass status",
            )
        )
    if not report.sections:
        violations.append(
            _violation(
                "preflight_report.sections",
                "sections_required",
                "preflight reports must include section summaries",
            )
        )
    return violations


def _privacy_boundary_violations(
    payload: ByocPreflightReportSubmissionPayload,
) -> list[ByocPreflightReportIntakeViolation]:
    violations: list[ByocPreflightReportIntakeViolation] = []
    _scan_value(payload.model_dump(mode="json"), path="<root>", violations=violations)
    return violations


def _scan_value(
    value: Any,
    *,
    path: str,
    violations: list[ByocPreflightReportIntakeViolation],
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
                "preflight submission contains a raw data, URL, credential, or token marker",
            )
        )


def _canonical_json(model: BaseModel) -> bytes:
    data = model.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocPreflightReportIntakeViolation:
    return ByocPreflightReportIntakeViolation(path=path, code=code, message=message)


__all__ = [
    "ByocPreflightReportIntakeRecord",
    "ByocPreflightReportIntakeStore",
    "ByocPreflightReportIntakeViolation",
    "ByocPreflightReportReceipt",
    "ByocPreflightReportReceiptList",
    "ByocPreflightReportReceiptQuery",
    "ByocPreflightReportSubmissionPayload",
    "ByocPreflightReportSubmissionRequest",
    "InMemoryByocPreflightReportIntakeStore",
    "PostgresByocPreflightReportIntakeStore",
    "canonical_preflight_report_submission_payload",
    "digest_preflight_report",
    "model_json_schema_bundle",
    "preflight_report_receipt",
    "preflight_report_submission_payload",
    "signed_preflight_report_submission",
    "validate_preflight_report_submission",
]
