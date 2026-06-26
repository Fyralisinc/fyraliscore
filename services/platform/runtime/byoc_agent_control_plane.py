"""Hosted BYOC agent enrollment and heartbeat control-plane contract."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_agent_contract import (
    ByocAgentEnrollmentPayload,
    ByocAgentEnrollmentRequest,
    ByocAgentEnrollmentResponse,
    ByocAgentHeartbeat,
    ByocAgentHeartbeatResponse,
    canonical_enrollment_payload,
)
from services.platform.runtime.byoc_contract import CloudProvider, TelemetryMode


AgentStoredScope = Literal["sanitized_agent_metadata_only"]

_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAgentEnrollmentRecord(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent_enrollment_record.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    cloud_provider: CloudProvider
    region: str
    install_token_secret_ref: str
    desired_revision: str
    heartbeat_interval_seconds: int = Field(ge=5, le=300)
    telemetry_contract: str
    enrolled_at: datetime
    stored_scope: AgentStoredScope = "sanitized_agent_metadata_only"

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
        "artifact_revision",
        "desired_revision",
        "region",
        "telemetry_contract",
    )
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("agent metadata fields must be bounded identifiers")
        return value

    @field_validator("install_token_secret_ref")
    @classmethod
    def _secret_ref_must_be_ref_like(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("install_token_secret_ref must not be empty")
        if "://" in value or "bearer " in value.lower():
            raise ValueError("install_token_secret_ref must not contain raw material")
        return value


class ByocAgentHeartbeatRecord(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent_heartbeat_record.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    sequence: int = Field(ge=0)
    validation_status: Literal["unknown", "passing", "degraded", "failing"]
    control_plane_connected: bool
    telemetry_mode: TelemetryMode
    telemetry_contract: str
    component_count: int = Field(ge=0)
    ok_component_count: int = Field(ge=0)
    degraded_component_count: int = Field(ge=0)
    failed_component_count: int = Field(ge=0)
    unknown_component_count: int = Field(ge=0)
    queued_batches: int = Field(ge=0)
    dropped_batches: int = Field(ge=0)
    sent_at: datetime
    accepted_at: datetime
    stored_scope: AgentStoredScope = "sanitized_agent_metadata_only"

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

    @field_validator("agent_version", "artifact_revision", "telemetry_contract")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("heartbeat metadata fields must be bounded identifiers")
        return value


class ByocAgentRegistrationState(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent_registration_state.v1"]
    enrollment: ByocAgentEnrollmentRecord
    latest_heartbeat: ByocAgentHeartbeatRecord | None = None
    stored_scope: AgentStoredScope = "sanitized_agent_metadata_only"


@dataclass(frozen=True, slots=True)
class ByocAgentControlPlaneViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


class ByocAgentRegistryStore(Protocol):
    async def enroll(
        self,
        request: ByocAgentEnrollmentRequest,
        *,
        enrolled_at: datetime | None = None,
        desired_revision: str | None = None,
        heartbeat_interval_seconds: int = 15,
        telemetry_contract: str = "aggregate-only-v1",
    ) -> ByocAgentEnrollmentResponse:
        ...

    async def heartbeat(
        self,
        request: ByocAgentHeartbeat,
        *,
        accepted_at: datetime | None = None,
        desired_revision: str | None = None,
        poll_after_seconds: int = 30,
    ) -> ByocAgentHeartbeatResponse | None:
        ...

    async def get(
        self,
        *,
        deployment_id: str,
        customer_id: str,
        agent_id: str,
    ) -> ByocAgentRegistrationState | None:
        ...


class InMemoryByocAgentRegistryStore:
    """Local sanitized BYOC agent registry used by contract/router tests."""

    def __init__(self) -> None:
        self._records: dict[
            tuple[str, str, str],
            ByocAgentRegistrationState,
        ] = {}

    @property
    def records(self) -> tuple[ByocAgentRegistrationState, ...]:
        return tuple(self._records.values())

    async def enroll(
        self,
        request: ByocAgentEnrollmentRequest,
        *,
        enrolled_at: datetime | None = None,
        desired_revision: str | None = None,
        heartbeat_interval_seconds: int = 15,
        telemetry_contract: str = "aggregate-only-v1",
    ) -> ByocAgentEnrollmentResponse:
        accepted = enrolled_at or datetime.now(UTC)
        desired = desired_revision or request.artifact_revision
        record = ByocAgentEnrollmentRecord(
            schema_version="fyralis.byoc.agent_enrollment_record.v1",
            deployment_id=request.deployment_id,
            customer_id=request.customer_id,
            agent_id=request.agent_id,
            agent_version=request.agent_version,
            artifact_revision=request.artifact_revision,
            cloud_provider=request.cloud_provider,
            region=request.region,
            install_token_secret_ref=request.install_token_secret_ref,
            desired_revision=desired,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            telemetry_contract=telemetry_contract,
            enrolled_at=accepted,
            stored_scope="sanitized_agent_metadata_only",
        )
        key = _agent_key(request.deployment_id, request.customer_id, request.agent_id)
        existing = self._records.get(key)
        self._records[key] = ByocAgentRegistrationState(
            schema_version="fyralis.byoc.agent_registration_state.v1",
            enrollment=record,
            latest_heartbeat=(
                existing.latest_heartbeat if existing is not None else None
            ),
            stored_scope="sanitized_agent_metadata_only",
        )
        return ByocAgentEnrollmentResponse(
            schema_version="fyralis.byoc.agent.enrollment_response.v1",
            status="accepted",
            deployment_id=request.deployment_id,
            agent_id=request.agent_id,
            desired_revision=desired,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            telemetry_contract=telemetry_contract,
            accepted_at=accepted,
        )

    async def heartbeat(
        self,
        request: ByocAgentHeartbeat,
        *,
        accepted_at: datetime | None = None,
        desired_revision: str | None = None,
        poll_after_seconds: int = 30,
    ) -> ByocAgentHeartbeatResponse | None:
        key = _agent_key(request.deployment_id, request.customer_id, request.agent_id)
        existing = self._records.get(key)
        if existing is None:
            return None
        accepted = accepted_at or datetime.now(UTC)
        heartbeat_record = heartbeat_record_from_request(
            request,
            accepted_at=accepted,
        )
        self._records[key] = ByocAgentRegistrationState(
            schema_version="fyralis.byoc.agent_registration_state.v1",
            enrollment=existing.enrollment,
            latest_heartbeat=heartbeat_record,
            stored_scope="sanitized_agent_metadata_only",
        )
        return ByocAgentHeartbeatResponse(
            schema_version="fyralis.byoc.agent.heartbeat_response.v1",
            status="accepted",
            deployment_id=request.deployment_id,
            agent_id=request.agent_id,
            desired_revision=desired_revision or existing.enrollment.desired_revision,
            poll_after_seconds=poll_after_seconds,
            accepted_at=accepted,
        )

    async def get(
        self,
        *,
        deployment_id: str,
        customer_id: str,
        agent_id: str,
    ) -> ByocAgentRegistrationState | None:
        return self._records.get(_agent_key(deployment_id, customer_id, agent_id))


class PostgresByocAgentRegistryStore:
    """Postgres-backed BYOC agent registry.

    The table stores only scalar registration metadata and latest heartbeat
    summary counts. It intentionally does not store enrollment or heartbeat
    request bodies, signature values, mTLS material, tokens, logs, payloads, or
    URLs.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def enroll(
        self,
        request: ByocAgentEnrollmentRequest,
        *,
        enrolled_at: datetime | None = None,
        desired_revision: str | None = None,
        heartbeat_interval_seconds: int = 15,
        telemetry_contract: str = "aggregate-only-v1",
    ) -> ByocAgentEnrollmentResponse:
        accepted = enrolled_at or datetime.now(UTC)
        desired = desired_revision or request.artifact_revision
        row = await self._pool.fetchrow(
            """
            INSERT INTO byoc_agent_registrations (
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                install_token_secret_ref,
                desired_revision,
                heartbeat_interval_seconds,
                telemetry_contract,
                enrolled_at,
                stored_scope,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, now()
            )
            ON CONFLICT (deployment_id, customer_id, agent_id) DO UPDATE SET
                agent_version = EXCLUDED.agent_version,
                artifact_revision = EXCLUDED.artifact_revision,
                cloud_provider = EXCLUDED.cloud_provider,
                region = EXCLUDED.region,
                install_token_secret_ref = EXCLUDED.install_token_secret_ref,
                desired_revision = EXCLUDED.desired_revision,
                heartbeat_interval_seconds = EXCLUDED.heartbeat_interval_seconds,
                telemetry_contract = EXCLUDED.telemetry_contract,
                enrolled_at = EXCLUDED.enrolled_at,
                stored_scope = EXCLUDED.stored_scope,
                updated_at = now()
            RETURNING
                deployment_id,
                agent_id,
                desired_revision,
                heartbeat_interval_seconds,
                telemetry_contract,
                enrolled_at
            """,
            request.deployment_id,
            request.customer_id,
            request.agent_id,
            request.agent_version,
            request.artifact_revision,
            request.cloud_provider,
            request.region,
            request.install_token_secret_ref,
            desired,
            heartbeat_interval_seconds,
            telemetry_contract,
            accepted,
            "sanitized_agent_metadata_only",
        )
        return ByocAgentEnrollmentResponse(
            schema_version="fyralis.byoc.agent.enrollment_response.v1",
            status="accepted",
            deployment_id=row["deployment_id"],
            agent_id=row["agent_id"],
            desired_revision=row["desired_revision"],
            heartbeat_interval_seconds=row["heartbeat_interval_seconds"],
            telemetry_contract=row["telemetry_contract"],
            accepted_at=row["enrolled_at"],
        )

    async def heartbeat(
        self,
        request: ByocAgentHeartbeat,
        *,
        accepted_at: datetime | None = None,
        desired_revision: str | None = None,
        poll_after_seconds: int = 30,
    ) -> ByocAgentHeartbeatResponse | None:
        accepted = accepted_at or datetime.now(UTC)
        record = heartbeat_record_from_request(request, accepted_at=accepted)
        row = await self._pool.fetchrow(
            """
            UPDATE byoc_agent_registrations
            SET
                agent_version = $4,
                artifact_revision = $5,
                desired_revision = COALESCE($6, desired_revision),
                latest_heartbeat_sequence = $7,
                latest_validation_status = $8,
                latest_control_plane_connected = $9,
                latest_telemetry_mode = $10,
                latest_telemetry_contract = $11,
                latest_component_count = $12,
                latest_ok_component_count = $13,
                latest_degraded_component_count = $14,
                latest_failed_component_count = $15,
                latest_unknown_component_count = $16,
                latest_queued_batches = $17,
                latest_dropped_batches = $18,
                latest_heartbeat_sent_at = $19,
                latest_heartbeat_accepted_at = $20,
                updated_at = now()
            WHERE deployment_id = $1
              AND customer_id = $2
              AND agent_id = $3
            RETURNING
                deployment_id,
                agent_id,
                desired_revision
            """,
            record.deployment_id,
            record.customer_id,
            record.agent_id,
            record.agent_version,
            record.artifact_revision,
            desired_revision,
            record.sequence,
            record.validation_status,
            record.control_plane_connected,
            record.telemetry_mode,
            record.telemetry_contract,
            record.component_count,
            record.ok_component_count,
            record.degraded_component_count,
            record.failed_component_count,
            record.unknown_component_count,
            record.queued_batches,
            record.dropped_batches,
            record.sent_at,
            record.accepted_at,
        )
        if row is None:
            return None
        return ByocAgentHeartbeatResponse(
            schema_version="fyralis.byoc.agent.heartbeat_response.v1",
            status="accepted",
            deployment_id=row["deployment_id"],
            agent_id=row["agent_id"],
            desired_revision=row["desired_revision"],
            poll_after_seconds=poll_after_seconds,
            accepted_at=accepted,
        )

    async def get(
        self,
        *,
        deployment_id: str,
        customer_id: str,
        agent_id: str,
    ) -> ByocAgentRegistrationState | None:
        row = await self._pool.fetchrow(
            """
            SELECT
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                cloud_provider,
                region,
                install_token_secret_ref,
                desired_revision,
                heartbeat_interval_seconds,
                telemetry_contract,
                enrolled_at,
                stored_scope,
                latest_heartbeat_sequence,
                latest_validation_status,
                latest_control_plane_connected,
                latest_telemetry_mode,
                latest_telemetry_contract,
                latest_component_count,
                latest_ok_component_count,
                latest_degraded_component_count,
                latest_failed_component_count,
                latest_unknown_component_count,
                latest_queued_batches,
                latest_dropped_batches,
                latest_heartbeat_sent_at,
                latest_heartbeat_accepted_at
            FROM byoc_agent_registrations
            WHERE deployment_id = $1
              AND customer_id = $2
              AND agent_id = $3
            """,
            deployment_id,
            customer_id,
            agent_id,
        )
        if row is None:
            return None
        return _state_from_row(row)


def validate_agent_enrollment_request(
    request: ByocAgentEnrollmentRequest,
    *,
    install_token: str,
    expected_install_token_secret_ref: str,
    expected_deployment_id: str | None = None,
    expected_customer_id: str | None = None,
    expected_cloud_provider: CloudProvider | None = None,
    expected_region: str | None = None,
) -> list[ByocAgentControlPlaneViolation]:
    violations: list[ByocAgentControlPlaneViolation] = []
    if not install_token:
        return [
            _violation("signature", "missing_install_token", "install token is empty")
        ]
    if request.install_token_secret_ref != expected_install_token_secret_ref:
        violations.append(
            _violation(
                "install_token_secret_ref",
                "install_token_ref_mismatch",
                "install token secret ref is not registered for this deployment",
            )
        )
    if request.signature.key_ref != request.install_token_secret_ref:
        violations.append(
            _violation(
                "signature.key_ref",
                "signature_key_ref_mismatch",
                "signature key_ref must match install_token_secret_ref",
            )
        )
    if expected_deployment_id and request.deployment_id != expected_deployment_id:
        violations.append(
            _violation("deployment_id", "deployment_mismatch", "deployment mismatch")
        )
    if expected_customer_id and request.customer_id != expected_customer_id:
        violations.append(
            _violation("customer_id", "customer_mismatch", "customer mismatch")
        )
    if expected_cloud_provider and request.cloud_provider != expected_cloud_provider:
        violations.append(
            _violation("cloud_provider", "cloud_mismatch", "cloud provider mismatch")
        )
    if expected_region and request.region != expected_region:
        violations.append(_violation("region", "region_mismatch", "region mismatch"))

    expected_signature = _hmac_sha256(
        canonical_enrollment_payload(_payload_from_request(request)),
        install_token,
    )
    if not hmac.compare_digest(expected_signature, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )
    return violations


def validate_agent_heartbeat_request(
    heartbeat: ByocAgentHeartbeat,
    *,
    expected_deployment_id: str | None = None,
    expected_customer_id: str | None = None,
    expected_telemetry_mode: TelemetryMode | None = None,
    expected_telemetry_contract: str | None = None,
) -> list[ByocAgentControlPlaneViolation]:
    violations: list[ByocAgentControlPlaneViolation] = []
    if expected_deployment_id and heartbeat.deployment_id != expected_deployment_id:
        violations.append(
            _violation("deployment_id", "deployment_mismatch", "deployment mismatch")
        )
    if expected_customer_id and heartbeat.customer_id != expected_customer_id:
        violations.append(
            _violation("customer_id", "customer_mismatch", "customer mismatch")
        )
    if expected_telemetry_mode and heartbeat.telemetry.mode != expected_telemetry_mode:
        violations.append(
            _violation("telemetry.mode", "telemetry_mode_mismatch", "mode mismatch")
        )
    if (
        expected_telemetry_contract
        and heartbeat.telemetry.contract != expected_telemetry_contract
    ):
        violations.append(
            _violation(
                "telemetry.contract",
                "telemetry_contract_mismatch",
                "telemetry contract mismatch",
            )
        )
    component_names = [component.name for component in heartbeat.components]
    duplicates = sorted(
        {name for name in component_names if component_names.count(name) > 1}
    )
    for name in duplicates:
        violations.append(
            _violation(
                "components",
                "duplicate_component",
                f"duplicate component status for {name!r}",
            )
        )
    return violations


def heartbeat_record_from_request(
    request: ByocAgentHeartbeat,
    *,
    accepted_at: datetime | None = None,
) -> ByocAgentHeartbeatRecord:
    counts = _component_status_counts(request)
    return ByocAgentHeartbeatRecord(
        schema_version="fyralis.byoc.agent_heartbeat_record.v1",
        deployment_id=request.deployment_id,
        customer_id=request.customer_id,
        agent_id=request.agent_id,
        agent_version=request.agent_version,
        artifact_revision=request.artifact_revision,
        sequence=request.sequence,
        validation_status=request.validation_status,
        control_plane_connected=request.control_plane_connected,
        telemetry_mode=request.telemetry.mode,
        telemetry_contract=request.telemetry.contract,
        component_count=len(request.components),
        ok_component_count=counts["ok"],
        degraded_component_count=counts["degraded"],
        failed_component_count=counts["failed"],
        unknown_component_count=counts["unknown"],
        queued_batches=request.telemetry.queued_batches,
        dropped_batches=request.telemetry.dropped_batches,
        sent_at=request.sent_at,
        accepted_at=accepted_at or datetime.now(UTC),
        stored_scope="sanitized_agent_metadata_only",
    )


def _payload_from_request(
    request: ByocAgentEnrollmentRequest,
) -> ByocAgentEnrollmentPayload:
    return ByocAgentEnrollmentPayload.model_validate(
        request.model_dump(exclude={"signature"})
    )


def _component_status_counts(
    request: ByocAgentHeartbeat,
) -> dict[Literal["ok", "degraded", "failed", "unknown"], int]:
    counts: dict[Literal["ok", "degraded", "failed", "unknown"], int] = {
        "ok": 0,
        "degraded": 0,
        "failed": 0,
        "unknown": 0,
    }
    for component in request.components:
        counts[component.status] += 1
    return counts


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _agent_key(
    deployment_id: str,
    customer_id: str,
    agent_id: str,
) -> tuple[str, str, str]:
    return (deployment_id, customer_id, agent_id)


def _state_from_row(row: Any) -> ByocAgentRegistrationState:
    enrollment = ByocAgentEnrollmentRecord(
        schema_version="fyralis.byoc.agent_enrollment_record.v1",
        deployment_id=row["deployment_id"],
        customer_id=row["customer_id"],
        agent_id=row["agent_id"],
        agent_version=row["agent_version"],
        artifact_revision=row["artifact_revision"],
        cloud_provider=row["cloud_provider"],
        region=row["region"],
        install_token_secret_ref=row["install_token_secret_ref"],
        desired_revision=row["desired_revision"],
        heartbeat_interval_seconds=row["heartbeat_interval_seconds"],
        telemetry_contract=row["telemetry_contract"],
        enrolled_at=row["enrolled_at"],
        stored_scope=row["stored_scope"],
    )
    latest_heartbeat = None
    if row["latest_heartbeat_sequence"] is not None:
        latest_heartbeat = ByocAgentHeartbeatRecord(
            schema_version="fyralis.byoc.agent_heartbeat_record.v1",
            deployment_id=row["deployment_id"],
            customer_id=row["customer_id"],
            agent_id=row["agent_id"],
            agent_version=row["agent_version"],
            artifact_revision=row["artifact_revision"],
            sequence=row["latest_heartbeat_sequence"],
            validation_status=row["latest_validation_status"],
            control_plane_connected=row["latest_control_plane_connected"],
            telemetry_mode=row["latest_telemetry_mode"],
            telemetry_contract=row["latest_telemetry_contract"],
            component_count=row["latest_component_count"],
            ok_component_count=row["latest_ok_component_count"],
            degraded_component_count=row["latest_degraded_component_count"],
            failed_component_count=row["latest_failed_component_count"],
            unknown_component_count=row["latest_unknown_component_count"],
            queued_batches=row["latest_queued_batches"],
            dropped_batches=row["latest_dropped_batches"],
            sent_at=row["latest_heartbeat_sent_at"],
            accepted_at=row["latest_heartbeat_accepted_at"],
            stored_scope=row["stored_scope"],
        )
    return ByocAgentRegistrationState(
        schema_version="fyralis.byoc.agent_registration_state.v1",
        enrollment=enrollment,
        latest_heartbeat=latest_heartbeat,
        stored_scope="sanitized_agent_metadata_only",
    )


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocAgentControlPlaneViolation:
    return ByocAgentControlPlaneViolation(path=path, code=code, message=message)


__all__ = [
    "ByocAgentControlPlaneViolation",
    "ByocAgentEnrollmentRecord",
    "ByocAgentHeartbeatRecord",
    "ByocAgentRegistryStore",
    "ByocAgentRegistrationState",
    "InMemoryByocAgentRegistryStore",
    "PostgresByocAgentRegistryStore",
    "heartbeat_record_from_request",
    "validate_agent_enrollment_request",
    "validate_agent_heartbeat_request",
]
