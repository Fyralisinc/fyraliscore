"""BYOC data-plane agent enrollment and heartbeat contracts."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    TelemetryMode,
)


_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,79}$")
_FORBIDDEN_TEXT_FRAGMENTS = (
    "@",
    "/",
    "\\",
    "bearer ",
    "secret",
    "token",
    "payload",
    "prompt",
    "email",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAgentSignature(_StrictModel):
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


class ByocAgentEnrollmentPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.enrollment.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    cloud_provider: CloudProvider
    region: str
    requested_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    install_token_secret_ref: str

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        if not value.startswith("dep_"):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        if not value.startswith("cus_"):
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
        "region",
        "nonce",
        "install_token_secret_ref",
    )
    @classmethod
    def _string_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be empty")
        return value


class ByocAgentEnrollmentRequest(ByocAgentEnrollmentPayload):
    signature: ByocAgentSignature


class ByocAgentEnrollmentResponse(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.enrollment_response.v1"]
    status: Literal["accepted"]
    deployment_id: str
    agent_id: str
    desired_revision: str
    heartbeat_interval_seconds: int = Field(ge=5, le=300)
    telemetry_contract: str
    accepted_at: datetime


class ByocAgentComponentStatus(_StrictModel):
    name: str
    kind: Literal[
        "gateway",
        "worker",
        "database",
        "broker",
        "object_storage",
        "redis",
        "embedding",
        "observability",
        "agent",
    ]
    status: Literal["ok", "degraded", "failed", "unknown"]
    detail_code: str | None = None
    queue_depth_band: Literal["none", "low", "medium", "high", "critical"] | None = None

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded_code(cls, value: str) -> str:
        return _safe_code(value, field_name="component name")

    @field_validator("detail_code")
    @classmethod
    def _detail_code_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _safe_code(value, field_name="detail_code")


class ByocAgentTelemetryState(_StrictModel):
    mode: TelemetryMode = "aggregate-only"
    contract: str = "aggregate-only-v1"
    raw_logs_allowed: Literal[False] = False
    raw_payloads_allowed: Literal[False] = False
    raw_prompts_allowed: Literal[False] = False
    pii_allowed: Literal[False] = False
    queued_batches: int = Field(default=0, ge=0)
    dropped_batches: int = Field(default=0, ge=0)


class ByocAgentHeartbeat(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.heartbeat.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    sent_at: datetime
    sequence: int = Field(ge=0)
    validation_status: Literal["unknown", "passing", "degraded", "failing"]
    control_plane_connected: bool
    components: tuple[ByocAgentComponentStatus, ...] = ()
    telemetry: ByocAgentTelemetryState

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        if not value.startswith("dep_"):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        if not value.startswith("cus_"):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value


class ByocAgentHeartbeatResponse(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.heartbeat_response.v1"]
    status: Literal["accepted"]
    deployment_id: str
    agent_id: str
    desired_revision: str
    poll_after_seconds: int = Field(ge=5, le=300)
    accepted_at: datetime


@dataclass(frozen=True, slots=True)
class ByocAgentContractViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def _safe_code(value: str, *, field_name: str) -> str:
    value = value.strip()
    lowered = value.lower()
    if any(fragment in lowered for fragment in _FORBIDDEN_TEXT_FRAGMENTS):
        raise ValueError(f"{field_name} must not contain customer data or secrets")
    if not _SAFE_CODE_RE.match(value):
        raise ValueError(f"{field_name} must be a bounded code")
    return value


def enrollment_payload_from_manifest(
    manifest: ByocDataPlaneManifest,
    *,
    agent_id: str,
    agent_version: str,
    nonce: str,
    requested_at: datetime | None = None,
) -> ByocAgentEnrollmentPayload:
    return ByocAgentEnrollmentPayload(
        schema_version="fyralis.byoc.agent.enrollment.v1",
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        agent_id=agent_id,
        agent_version=agent_version,
        artifact_revision=manifest.artifact_revision,
        cloud_provider=manifest.cloud_provider,
        region=manifest.region,
        requested_at=requested_at or datetime.now(UTC),
        nonce=nonce,
        install_token_secret_ref=manifest.secrets.bootstrap_token_secret_ref,
    )


def signed_enrollment_request(
    payload: ByocAgentEnrollmentPayload,
    *,
    install_token: str,
) -> ByocAgentEnrollmentRequest:
    if not install_token:
        raise ValueError("install_token must not be empty")
    signature = ByocAgentSignature(
        key_ref=payload.install_token_secret_ref,
        value=_hmac_sha256(canonical_enrollment_payload(payload), install_token),
    )
    return ByocAgentEnrollmentRequest(
        **payload.model_dump(),
        signature=signature,
    )


def canonical_enrollment_payload(payload: ByocAgentEnrollmentPayload) -> bytes:
    data = payload.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def verify_enrollment_request(
    request: ByocAgentEnrollmentRequest,
    *,
    manifest: ByocDataPlaneManifest,
    install_token: str,
) -> list[ByocAgentContractViolation]:
    violations: list[ByocAgentContractViolation] = []
    if request.deployment_id != manifest.deployment_id:
        violations.append(
            _violation("deployment_id", "deployment_mismatch", "deployment mismatch")
        )
    if request.customer_id != manifest.customer_id:
        violations.append(
            _violation("customer_id", "customer_mismatch", "customer mismatch")
        )
    if request.cloud_provider != manifest.cloud_provider or request.region != manifest.region:
        violations.append(
            _violation("cloud", "cloud_mismatch", "cloud provider or region mismatch")
        )
    if request.artifact_revision != manifest.artifact_revision:
        violations.append(
            _violation("artifact_revision", "revision_mismatch", "revision mismatch")
        )
    if request.install_token_secret_ref != manifest.secrets.bootstrap_token_secret_ref:
        violations.append(
            _violation(
                "install_token_secret_ref",
                "install_token_ref_mismatch",
                "install token secret ref does not match deployment manifest",
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
    expected_signature = _hmac_sha256(
        canonical_enrollment_payload(_payload_from_request(request)),
        install_token,
    )
    if not hmac.compare_digest(expected_signature, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )
    return violations


def _payload_from_request(
    request: ByocAgentEnrollmentRequest,
) -> ByocAgentEnrollmentPayload:
    data = request.model_dump(exclude={"signature"})
    return ByocAgentEnrollmentPayload.model_validate(data)


def validate_heartbeat_contract(
    heartbeat: ByocAgentHeartbeat,
    *,
    manifest: ByocDataPlaneManifest,
    enrolled_agent_ids: set[str] | None = None,
) -> list[ByocAgentContractViolation]:
    violations: list[ByocAgentContractViolation] = []
    if heartbeat.deployment_id != manifest.deployment_id:
        violations.append(
            _violation("deployment_id", "deployment_mismatch", "deployment mismatch")
        )
    if heartbeat.customer_id != manifest.customer_id:
        violations.append(
            _violation("customer_id", "customer_mismatch", "customer mismatch")
        )
    if enrolled_agent_ids is not None and heartbeat.agent_id not in enrolled_agent_ids:
        violations.append(
            _violation("agent_id", "agent_not_enrolled", "agent is not enrolled")
        )
    if heartbeat.telemetry.mode != manifest.telemetry.mode:
        violations.append(
            _violation("telemetry.mode", "telemetry_mode_mismatch", "mode mismatch")
        )
    if heartbeat.telemetry.contract != manifest.telemetry.contract:
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


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocAgentContractViolation:
    return ByocAgentContractViolation(path=path, code=code, message=message)


def heartbeat_from_manifest(
    manifest: ByocDataPlaneManifest,
    *,
    agent_id: str,
    agent_version: str,
    sequence: int,
    validation_status: Literal["unknown", "passing", "degraded", "failing"],
    control_plane_connected: bool,
    components: tuple[ByocAgentComponentStatus, ...] = (),
    sent_at: datetime | None = None,
) -> ByocAgentHeartbeat:
    return ByocAgentHeartbeat(
        schema_version="fyralis.byoc.agent.heartbeat.v1",
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        agent_id=agent_id,
        agent_version=agent_version,
        artifact_revision=manifest.artifact_revision,
        sent_at=sent_at or datetime.now(UTC),
        sequence=sequence,
        validation_status=validation_status,
        control_plane_connected=control_plane_connected,
        components=components,
        telemetry=ByocAgentTelemetryState(
            mode=manifest.telemetry.mode,
            contract=manifest.telemetry.contract,
            raw_logs_allowed=False,
            raw_payloads_allowed=False,
            raw_prompts_allowed=False,
            pii_allowed=False,
        ),
    )


def build_mock_control_plane_app(
    manifest: ByocDataPlaneManifest,
    *,
    install_token: str,
) -> FastAPI:
    """Build a local control-plane contract test app.

    This is not the hosted control plane. It exists so the data-plane agent and
    bootstrap tooling can prove request/response contracts without cloud
    credentials or a dashboard.
    """

    app = FastAPI(title="Fyralis BYOC Mock Control Plane")
    enrolled_agent_ids: set[str] = set()
    heartbeats: list[ByocAgentHeartbeat] = []
    app.state.enrolled_agent_ids = enrolled_agent_ids
    app.state.heartbeats = heartbeats

    @app.post("/byoc/agent/enroll")
    async def enroll(
        request: ByocAgentEnrollmentRequest,
    ) -> ByocAgentEnrollmentResponse:
        violations = verify_enrollment_request(
            request,
            manifest=manifest,
            install_token=install_token,
        )
        if violations:
            raise HTTPException(
                status_code=403,
                detail={
                    "errors": [violation.render() for violation in violations],
                },
            )
        enrolled_agent_ids.add(request.agent_id)
        return ByocAgentEnrollmentResponse(
            schema_version="fyralis.byoc.agent.enrollment_response.v1",
            status="accepted",
            deployment_id=manifest.deployment_id,
            agent_id=request.agent_id,
            desired_revision=manifest.artifact_revision,
            heartbeat_interval_seconds=(
                manifest.connectivity.heartbeat_interval_seconds
            ),
            telemetry_contract=manifest.telemetry.contract,
            accepted_at=datetime.now(UTC),
        )

    @app.post("/byoc/agent/heartbeat")
    async def heartbeat(
        request: ByocAgentHeartbeat,
    ) -> ByocAgentHeartbeatResponse:
        violations = validate_heartbeat_contract(
            request,
            manifest=manifest,
            enrolled_agent_ids=enrolled_agent_ids,
        )
        if violations:
            raise HTTPException(
                status_code=403,
                detail={
                    "errors": [violation.render() for violation in violations],
                },
            )
        heartbeats.append(request)
        return ByocAgentHeartbeatResponse(
            schema_version="fyralis.byoc.agent.heartbeat_response.v1",
            status="accepted",
            deployment_id=manifest.deployment_id,
            agent_id=request.agent_id,
            desired_revision=manifest.artifact_revision,
            poll_after_seconds=manifest.connectivity.agent_poll_interval_seconds,
            accepted_at=datetime.now(UTC),
        )

    return app


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "enrollment_payload": ByocAgentEnrollmentPayload.model_json_schema(),
        "enrollment_request": ByocAgentEnrollmentRequest.model_json_schema(),
        "enrollment_response": ByocAgentEnrollmentResponse.model_json_schema(),
        "heartbeat": ByocAgentHeartbeat.model_json_schema(),
        "heartbeat_response": ByocAgentHeartbeatResponse.model_json_schema(),
    }


__all__ = [
    "ByocAgentComponentStatus",
    "ByocAgentContractViolation",
    "ByocAgentEnrollmentPayload",
    "ByocAgentEnrollmentRequest",
    "ByocAgentEnrollmentResponse",
    "ByocAgentHeartbeat",
    "ByocAgentHeartbeatResponse",
    "ByocAgentSignature",
    "ByocAgentTelemetryState",
    "build_mock_control_plane_app",
    "canonical_enrollment_payload",
    "enrollment_payload_from_manifest",
    "heartbeat_from_manifest",
    "model_json_schema_bundle",
    "signed_enrollment_request",
    "validate_heartbeat_contract",
    "verify_enrollment_request",
]
