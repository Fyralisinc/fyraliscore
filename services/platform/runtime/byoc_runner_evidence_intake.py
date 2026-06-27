"""BYOC control-plane intake contract for sanitized runner evidence."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerReport,
    AgentRunnerMode,
    AgentRunnerStatus,
)
from services.platform.runtime.byoc_contract import CloudProvider
from services.platform.runtime.byoc_control_plane_intake import (
    ByocControlPlaneSignature,
)


_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_RECEIPT_ID_RE = re.compile(r"^runev_[0-9a-f]{32}$")
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


class ByocRunnerEvidenceSummary(_StrictModel):
    schema_version: Literal["fyralis.byoc.runner_evidence_summary.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    cloud_provider: CloudProvider
    region: str
    control_plane_mode: AgentRunnerMode
    current_artifact_revision: str
    desired_revision: str
    rollout_action: Literal["none", "apply_revision"]
    config_epoch: int | None = Field(default=None, ge=0)
    runner_status: AgentRunnerStatus
    required_checks_passed: bool
    iterations_requested: int = Field(ge=1, le=10)
    iterations_completed: int = Field(ge=0, le=10)
    desired_state_poll_count: int = Field(ge=0)
    heartbeat_count: int = Field(ge=0)
    apply_plan_count: int = Field(ge=0)
    artifact_verification_count: int = Field(ge=0)
    apply_plan_ids: tuple[str, ...] = ()
    artifact_verification_ids: tuple[str, ...] = ()
    digest_pinned_artifact_count: int = Field(ge=0)
    local_digest_checked_count: int = Field(ge=0)
    stored_scope: Literal["sanitized_agent_metadata_only"] = (
        "sanitized_agent_metadata_only"
    )

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

    @field_validator(
        "agent_version",
        "region",
        "current_artifact_revision",
        "desired_revision",
    )
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("summary fields must be bounded identifiers")
        return value

    @field_validator("apply_plan_ids", "artifact_verification_ids")
    @classmethod
    def _ids_must_be_bounded(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        for value in values:
            if not _SAFE_CODE_RE.match(value):
                raise ValueError("runner evidence IDs must be bounded identifiers")
        return values

    @model_validator(mode="after")
    def _counts_must_match_summary(self) -> "ByocRunnerEvidenceSummary":
        if self.iterations_completed > self.iterations_requested:
            raise ValueError("iterations_completed must not exceed iterations_requested")
        if self.apply_plan_count != len(self.apply_plan_ids):
            raise ValueError("apply_plan_count must match apply_plan_ids")
        if self.artifact_verification_count != len(self.artifact_verification_ids):
            raise ValueError(
                "artifact_verification_count must match artifact_verification_ids"
            )
        if self.runner_status == "pass" and not self.required_checks_passed:
            raise ValueError("passing runner evidence must have required checks passed")
        if self.runner_status == "fail" and self.required_checks_passed:
            raise ValueError("failed runner evidence must not mark checks passed")
        return self


class ByocRunnerEvidenceSubmissionPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.runner_evidence_submission.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    submitted_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    evidence: ByocRunnerEvidenceSummary

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

    @field_validator("agent_version")
    @classmethod
    def _agent_version_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("agent_version must be a bounded identifier")
        return value

    @field_validator("nonce")
    @classmethod
    def _nonce_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("nonce must not be empty")
        return value

    @model_validator(mode="after")
    def _identity_must_match_evidence(
        self,
    ) -> "ByocRunnerEvidenceSubmissionPayload":
        for field in ("deployment_id", "customer_id", "agent_id", "agent_version"):
            if getattr(self, field) != getattr(self.evidence, field):
                raise ValueError(f"submission {field} must match runner evidence")
        return self


class ByocRunnerEvidenceSubmissionRequest(ByocRunnerEvidenceSubmissionPayload):
    signature: ByocControlPlaneSignature


class ByocRunnerEvidenceReceipt(_StrictModel):
    schema_version: Literal["fyralis.byoc.runner_evidence_receipt.v1"]
    status: Literal["accepted"] = "accepted"
    receipt_id: str
    deployment_id: str
    customer_id: str
    agent_id: str
    evidence_digest: str
    current_artifact_revision: str
    desired_revision: str
    rollout_action: Literal["none", "apply_revision"]
    runner_status: AgentRunnerStatus
    required_checks_passed: bool
    apply_plan_count: int = Field(ge=0)
    artifact_verification_count: int = Field(ge=0)
    digest_pinned_artifact_count: int = Field(ge=0)
    local_digest_checked_count: int = Field(ge=0)
    submitted_at: datetime
    accepted_at: datetime
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"

    @field_validator("receipt_id")
    @classmethod
    def _receipt_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _RECEIPT_ID_RE.match(value):
            raise ValueError("receipt_id must look like runev_<32-hex>")
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

    @field_validator("evidence_digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("evidence_digest must look like sha256:<64-hex>")
        return value

    @field_validator("current_artifact_revision", "desired_revision")
    @classmethod
    def _revisions_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("revision fields must be bounded identifiers")
        return value


class ByocRunnerEvidenceIntakeRecord(_StrictModel):
    receipt: ByocRunnerEvidenceReceipt
    agent_version: str
    cloud_provider: CloudProvider
    region: str
    control_plane_mode: AgentRunnerMode

    @field_validator("agent_version", "region")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("record fields must be bounded identifiers")
        return value


class ByocRunnerEvidenceReceiptQuery(_StrictModel):
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
    def _query_must_be_bounded(self) -> "ByocRunnerEvidenceReceiptQuery":
        if self.deployment_id is None and self.customer_id is None:
            raise ValueError(
                "runner evidence receipt queries must include deployment_id or customer_id"
            )
        return self


class ByocRunnerEvidenceReceiptList(_StrictModel):
    schema_version: Literal["fyralis.byoc.runner_evidence_receipt_list.v1"]
    deployment_id: str | None = None
    customer_id: str | None = None
    limit: int
    result_count: int
    stored_scope: Literal["sanitized_metadata_only"] = "sanitized_metadata_only"
    items: tuple[ByocRunnerEvidenceIntakeRecord, ...]


@dataclass(frozen=True, slots=True)
class ByocRunnerEvidenceIntakeViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


class ByocRunnerEvidenceIntakeStore(Protocol):
    async def put(
        self,
        request: ByocRunnerEvidenceSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocRunnerEvidenceReceipt:
        ...

    async def get(self, receipt_id: str) -> ByocRunnerEvidenceIntakeRecord | None:
        ...

    async def list_receipts(
        self,
        query: ByocRunnerEvidenceReceiptQuery,
    ) -> ByocRunnerEvidenceReceiptList:
        ...


class InMemoryByocRunnerEvidenceIntakeStore:
    """Local sanitized runner evidence store used by contract tests."""

    def __init__(self) -> None:
        self._records: dict[str, ByocRunnerEvidenceIntakeRecord] = {}

    @property
    def records(self) -> tuple[ByocRunnerEvidenceIntakeRecord, ...]:
        return tuple(self._records.values())

    async def put(
        self,
        request: ByocRunnerEvidenceSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocRunnerEvidenceReceipt:
        payload = _payload_from_request(request)
        receipt = runner_evidence_receipt(payload, accepted_at=accepted_at)
        self._records[receipt.receipt_id] = ByocRunnerEvidenceIntakeRecord(
            receipt=receipt,
            agent_version=payload.agent_version,
            cloud_provider=payload.evidence.cloud_provider,
            region=payload.evidence.region,
            control_plane_mode=payload.evidence.control_plane_mode,
        )
        return receipt

    async def get(self, receipt_id: str) -> ByocRunnerEvidenceIntakeRecord | None:
        return self._records.get(receipt_id)

    async def list_receipts(
        self,
        query: ByocRunnerEvidenceReceiptQuery,
    ) -> ByocRunnerEvidenceReceiptList:
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
        return ByocRunnerEvidenceReceiptList(
            schema_version="fyralis.byoc.runner_evidence_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


class PostgresByocRunnerEvidenceIntakeStore:
    """Postgres-backed sanitized runner evidence receipt store.

    The store persists only scalar metadata. It intentionally does not persist
    runner checks, apply-plan details, artifact inventories, raw reports, URLs,
    logs, request bodies, or credential material.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def put(
        self,
        request: ByocRunnerEvidenceSubmissionRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocRunnerEvidenceReceipt:
        payload = _payload_from_request(request)
        receipt = runner_evidence_receipt(payload, accepted_at=accepted_at)
        row = await self._pool.fetchrow(
            """
            INSERT INTO byoc_runner_evidence_receipts (
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                cloud_provider,
                region,
                control_plane_mode,
                evidence_digest,
                current_artifact_revision,
                desired_revision,
                rollout_action,
                runner_status,
                required_checks_passed,
                apply_plan_count,
                artifact_verification_count,
                digest_pinned_artifact_count,
                local_digest_checked_count,
                submitted_at,
                accepted_at,
                stored_scope
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21
            )
            ON CONFLICT (receipt_id) DO UPDATE
              SET receipt_id = byoc_runner_evidence_receipts.receipt_id
            RETURNING
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                cloud_provider,
                region,
                control_plane_mode,
                evidence_digest,
                current_artifact_revision,
                desired_revision,
                rollout_action,
                runner_status,
                required_checks_passed,
                apply_plan_count,
                artifact_verification_count,
                digest_pinned_artifact_count,
                local_digest_checked_count,
                submitted_at,
                accepted_at,
                stored_scope
            """,
            receipt.receipt_id,
            receipt.deployment_id,
            receipt.customer_id,
            receipt.agent_id,
            payload.agent_version,
            payload.evidence.cloud_provider,
            payload.evidence.region,
            payload.evidence.control_plane_mode,
            receipt.evidence_digest,
            receipt.current_artifact_revision,
            receipt.desired_revision,
            receipt.rollout_action,
            receipt.runner_status,
            receipt.required_checks_passed,
            receipt.apply_plan_count,
            receipt.artifact_verification_count,
            receipt.digest_pinned_artifact_count,
            receipt.local_digest_checked_count,
            receipt.submitted_at,
            receipt.accepted_at,
            receipt.stored_scope,
        )
        return _record_from_row(row).receipt

    async def get(self, receipt_id: str) -> ByocRunnerEvidenceIntakeRecord | None:
        row = await self._pool.fetchrow(
            """
            SELECT
                receipt_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                cloud_provider,
                region,
                control_plane_mode,
                evidence_digest,
                current_artifact_revision,
                desired_revision,
                rollout_action,
                runner_status,
                required_checks_passed,
                apply_plan_count,
                artifact_verification_count,
                digest_pinned_artifact_count,
                local_digest_checked_count,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_runner_evidence_receipts
            WHERE receipt_id = $1
            """,
            receipt_id,
        )
        if row is None:
            return None
        return _record_from_row(row)

    async def list_receipts(
        self,
        query: ByocRunnerEvidenceReceiptQuery,
    ) -> ByocRunnerEvidenceReceiptList:
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
                cloud_provider,
                region,
                control_plane_mode,
                evidence_digest,
                current_artifact_revision,
                desired_revision,
                rollout_action,
                runner_status,
                required_checks_passed,
                apply_plan_count,
                artifact_verification_count,
                digest_pinned_artifact_count,
                local_digest_checked_count,
                submitted_at,
                accepted_at,
                stored_scope
            FROM byoc_runner_evidence_receipts
            WHERE {' AND '.join(where_clauses)}
            ORDER BY accepted_at DESC, receipt_id DESC
            LIMIT ${len(args)}
            """,
            *args,
        )
        items = tuple(_record_from_row(row) for row in rows)
        return ByocRunnerEvidenceReceiptList(
            schema_version="fyralis.byoc.runner_evidence_receipt_list.v1",
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.limit,
            result_count=len(items),
            stored_scope="sanitized_metadata_only",
            items=items,
        )


def runner_evidence_summary_from_report(
    report: ByocAgentRunnerReport,
) -> ByocRunnerEvidenceSummary:
    identity = {
        "deployment_id": report.deployment_id,
        "customer_id": report.customer_id,
        "cloud_provider": report.cloud_provider,
        "region": report.region,
        "current_artifact_revision": report.artifact_revision,
    }
    missing = [field for field, value in identity.items() if not value]
    if missing:
        raise ValueError(
            "runner report cannot be submitted without identity fields: "
            + ", ".join(missing)
        )
    artifact_verifications = tuple(
        verification.verification_id
        for verification in report.artifact_verifications
    )
    return ByocRunnerEvidenceSummary(
        schema_version="fyralis.byoc.runner_evidence_summary.v1",
        deployment_id=str(report.deployment_id),
        customer_id=str(report.customer_id),
        agent_id=report.agent_id,
        agent_version=report.agent_version,
        cloud_provider=report.cloud_provider,  # type: ignore[arg-type]
        region=str(report.region),
        control_plane_mode=report.control_plane_mode,
        current_artifact_revision=str(report.artifact_revision),
        desired_revision=report.final_desired_revision or str(report.artifact_revision),
        rollout_action=report.final_rollout_action or "none",  # type: ignore[arg-type]
        config_epoch=report.final_config_epoch,
        runner_status=report.status,
        required_checks_passed=report.required_checks_passed,
        iterations_requested=report.iterations_requested,
        iterations_completed=report.iterations_completed,
        desired_state_poll_count=report.desired_state_poll_count,
        heartbeat_count=report.heartbeat_count,
        apply_plan_count=report.apply_plan_count,
        artifact_verification_count=report.artifact_verification_count,
        apply_plan_ids=tuple(plan.plan_id for plan in report.apply_plans),
        artifact_verification_ids=artifact_verifications,
        digest_pinned_artifact_count=sum(
            verification.digest_pinned_artifact_count
            for verification in report.artifact_verifications
        ),
        local_digest_checked_count=sum(
            verification.local_digest_checked_count
            for verification in report.artifact_verifications
        ),
        stored_scope="sanitized_agent_metadata_only",
    )


def runner_evidence_submission_payload(
    *,
    evidence: ByocRunnerEvidenceSummary,
    nonce: str,
    submitted_at: datetime | None = None,
) -> ByocRunnerEvidenceSubmissionPayload:
    return ByocRunnerEvidenceSubmissionPayload(
        schema_version="fyralis.byoc.runner_evidence_submission.v1",
        deployment_id=evidence.deployment_id,
        customer_id=evidence.customer_id,
        agent_id=evidence.agent_id,
        agent_version=evidence.agent_version,
        submitted_at=submitted_at or datetime.now(UTC),
        nonce=nonce,
        evidence=evidence,
    )


def signed_runner_evidence_submission(
    payload: ByocRunnerEvidenceSubmissionPayload,
    *,
    signing_secret: str,
    key_ref: str,
) -> ByocRunnerEvidenceSubmissionRequest:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    signature = ByocControlPlaneSignature(
        key_ref=key_ref,
        value=_hmac_sha256(
            canonical_runner_evidence_submission_payload(payload),
            signing_secret,
        ),
    )
    return ByocRunnerEvidenceSubmissionRequest(
        **payload.model_dump(),
        signature=signature,
    )


def canonical_runner_evidence_submission_payload(
    payload: ByocRunnerEvidenceSubmissionPayload,
) -> bytes:
    return _canonical_json(payload)


def runner_evidence_receipt(
    payload: ByocRunnerEvidenceSubmissionPayload,
    *,
    accepted_at: datetime | None = None,
) -> ByocRunnerEvidenceReceipt:
    evidence_digest = digest_runner_evidence_summary(payload.evidence)
    receipt_id = "runev_" + hashlib.sha256(
        canonical_runner_evidence_submission_payload(payload)
    ).hexdigest()[:32]
    return ByocRunnerEvidenceReceipt(
        schema_version="fyralis.byoc.runner_evidence_receipt.v1",
        status="accepted",
        receipt_id=receipt_id,
        deployment_id=payload.deployment_id,
        customer_id=payload.customer_id,
        agent_id=payload.agent_id,
        evidence_digest=evidence_digest,
        current_artifact_revision=payload.evidence.current_artifact_revision,
        desired_revision=payload.evidence.desired_revision,
        rollout_action=payload.evidence.rollout_action,
        runner_status=payload.evidence.runner_status,
        required_checks_passed=payload.evidence.required_checks_passed,
        apply_plan_count=payload.evidence.apply_plan_count,
        artifact_verification_count=payload.evidence.artifact_verification_count,
        digest_pinned_artifact_count=payload.evidence.digest_pinned_artifact_count,
        local_digest_checked_count=payload.evidence.local_digest_checked_count,
        submitted_at=payload.submitted_at,
        accepted_at=accepted_at or datetime.now(UTC),
        stored_scope="sanitized_metadata_only",
    )


def digest_runner_evidence_summary(summary: ByocRunnerEvidenceSummary) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(summary)).hexdigest()


def validate_runner_evidence_submission(
    request: ByocRunnerEvidenceSubmissionRequest,
    *,
    signing_secret: str,
    expected_key_ref: str | None = None,
) -> list[ByocRunnerEvidenceIntakeViolation]:
    violations: list[ByocRunnerEvidenceIntakeViolation] = []
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
        canonical_runner_evidence_submission_payload(_payload_from_request(request)),
        signing_secret,
    )
    if not hmac.compare_digest(expected, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )

    violations.extend(_summary_contract_violations(request.evidence))
    violations.extend(_privacy_boundary_violations(_payload_from_request(request)))
    return violations


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "summary": ByocRunnerEvidenceSummary.model_json_schema(),
        "submission_payload": ByocRunnerEvidenceSubmissionPayload.model_json_schema(),
        "submission_request": ByocRunnerEvidenceSubmissionRequest.model_json_schema(),
        "receipt": ByocRunnerEvidenceReceipt.model_json_schema(),
        "receipt_list": ByocRunnerEvidenceReceiptList.model_json_schema(),
        "receipt_query": ByocRunnerEvidenceReceiptQuery.model_json_schema(),
        "intake_record": ByocRunnerEvidenceIntakeRecord.model_json_schema(),
    }


def _payload_from_request(
    request: ByocRunnerEvidenceSubmissionRequest,
) -> ByocRunnerEvidenceSubmissionPayload:
    data = request.model_dump(exclude={"signature"})
    return ByocRunnerEvidenceSubmissionPayload.model_validate(data)


def _record_from_row(row: Any) -> ByocRunnerEvidenceIntakeRecord:
    data = dict(row)
    receipt = ByocRunnerEvidenceReceipt(
        schema_version="fyralis.byoc.runner_evidence_receipt.v1",
        status="accepted",
        receipt_id=data["receipt_id"],
        deployment_id=data["deployment_id"],
        customer_id=data["customer_id"],
        agent_id=data["agent_id"],
        evidence_digest=data["evidence_digest"],
        current_artifact_revision=data["current_artifact_revision"],
        desired_revision=data["desired_revision"],
        rollout_action=data["rollout_action"],
        runner_status=data["runner_status"],
        required_checks_passed=data["required_checks_passed"],
        apply_plan_count=data["apply_plan_count"],
        artifact_verification_count=data["artifact_verification_count"],
        digest_pinned_artifact_count=data["digest_pinned_artifact_count"],
        local_digest_checked_count=data["local_digest_checked_count"],
        submitted_at=data["submitted_at"],
        accepted_at=data["accepted_at"],
        stored_scope=data["stored_scope"],
    )
    return ByocRunnerEvidenceIntakeRecord(
        receipt=receipt,
        agent_version=data["agent_version"],
        cloud_provider=data["cloud_provider"],
        region=data["region"],
        control_plane_mode=data["control_plane_mode"],
    )


def _record_matches_receipt_query(
    record: ByocRunnerEvidenceIntakeRecord,
    query: ByocRunnerEvidenceReceiptQuery,
) -> bool:
    if (
        query.deployment_id is not None
        and record.receipt.deployment_id != query.deployment_id
    ):
        return False
    if query.customer_id is not None and record.receipt.customer_id != query.customer_id:
        return False
    return True


def _summary_contract_violations(
    summary: ByocRunnerEvidenceSummary,
) -> list[ByocRunnerEvidenceIntakeViolation]:
    violations: list[ByocRunnerEvidenceIntakeViolation] = []
    if summary.heartbeat_count > summary.desired_state_poll_count:
        violations.append(
            _violation(
                "evidence.heartbeat_count",
                "heartbeat_count_exceeds_polls",
                "heartbeat count must not exceed desired-state poll count",
            )
        )
    if summary.apply_plan_count > summary.desired_state_poll_count:
        violations.append(
            _violation(
                "evidence.apply_plan_count",
                "apply_plan_count_exceeds_polls",
                "apply plan count must not exceed desired-state poll count",
            )
        )
    if summary.artifact_verification_count > summary.apply_plan_count:
        violations.append(
            _violation(
                "evidence.artifact_verification_count",
                "artifact_verification_count_exceeds_plans",
                "artifact verification count must not exceed apply plan count",
            )
        )
    return violations


def _privacy_boundary_violations(
    payload: ByocRunnerEvidenceSubmissionPayload,
) -> list[ByocRunnerEvidenceIntakeViolation]:
    violations: list[ByocRunnerEvidenceIntakeViolation] = []
    _scan_value(payload.model_dump(mode="json"), path="<root>", violations=violations)
    return violations


def _scan_value(
    value: Any,
    *,
    path: str,
    violations: list[ByocRunnerEvidenceIntakeViolation],
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
                "runner evidence contains a raw data, URL, credential, or token marker",
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
) -> ByocRunnerEvidenceIntakeViolation:
    return ByocRunnerEvidenceIntakeViolation(path=path, code=code, message=message)


__all__ = [
    "ByocRunnerEvidenceIntakeRecord",
    "ByocRunnerEvidenceIntakeStore",
    "ByocRunnerEvidenceIntakeViolation",
    "ByocRunnerEvidenceReceipt",
    "ByocRunnerEvidenceReceiptList",
    "ByocRunnerEvidenceReceiptQuery",
    "ByocRunnerEvidenceSubmissionPayload",
    "ByocRunnerEvidenceSubmissionRequest",
    "ByocRunnerEvidenceSummary",
    "InMemoryByocRunnerEvidenceIntakeStore",
    "PostgresByocRunnerEvidenceIntakeStore",
    "canonical_runner_evidence_submission_payload",
    "digest_runner_evidence_summary",
    "model_json_schema_bundle",
    "runner_evidence_receipt",
    "runner_evidence_submission_payload",
    "runner_evidence_summary_from_report",
    "signed_runner_evidence_submission",
    "validate_runner_evidence_submission",
]
