"""Local BYOC data-plane agent enrollment and heartbeat probe.

The long-running customer-side data-plane agent is still future work. This
module gives CI, bootstrap tooling, and operators a small executable contract:
prove enrollment with the managed install-token material, submit one bounded
heartbeat, and emit a sanitized report that can be archived without exposing
tokens, URLs, raw payloads, prompts, logs, or PII.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError

from services.platform.runtime.byoc_agent_contract import (
    ByocAgentComponentStatus,
    ByocAgentEnrollmentPayload,
    ByocAgentEnrollmentRequest,
    ByocAgentEnrollmentResponse,
    ByocAgentHeartbeat,
    ByocAgentHeartbeatResponse,
    build_mock_control_plane_app,
    enrollment_payload_from_manifest,
    heartbeat_from_manifest,
    signed_enrollment_request,
    validate_heartbeat_contract,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    load_byoc_manifest,
    render_validation_errors,
    validate_byoc_manifest_contract,
)


AgentProbeStatus = Literal["pass", "fail"]
AgentProbeMode = Literal["mock", "live"]
HeartbeatStatus = Literal["unknown", "passing", "degraded", "failing"]

_PASS: AgentProbeStatus = "pass"
_FAIL: AgentProbeStatus = "fail"
_LOCALHOSTS = {"localhost", "127.0.0.1", "::1"}


@dataclass(frozen=True, slots=True)
class ByocAgentProbeCheck:
    name: str
    status: AgentProbeStatus
    required: bool
    details: str
    metrics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ByocAgentProbeReport:
    schema_version: Literal["fyralis.byoc.agent_probe_report.v1"]
    status: AgentProbeStatus
    required_checks_passed: bool
    manifest_path: str
    control_plane_mode: AgentProbeMode
    control_plane_url_supplied: bool
    elapsed_seconds: float
    deployment_id: str | None
    customer_id: str | None
    cloud_provider: str | None
    region: str | None
    artifact_revision: str | None
    agent_id: str
    agent_version: str
    enrollment_status: AgentProbeStatus | None
    heartbeat_status: AgentProbeStatus | None
    desired_revision: str | None
    heartbeat_interval_seconds: int | None
    poll_after_seconds: int | None
    telemetry_contract: str | None
    checks: list[ByocAgentProbeCheck]

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ByocAgentProbeInputs:
    manifest_path: Path
    install_token: str
    agent_id: str = "agt_localprobe001"
    agent_version: str = "local-contract-probe"
    nonce: str = "nonce-local-agent-probe-001"
    sequence: int = 1
    validation_status: HeartbeatStatus = "passing"
    control_plane_url: str | None = None
    timeout_s: float = 5.0
    requested_at: datetime | None = None
    sent_at: datetime | None = None


async def run_byoc_agent_probe(
    inputs: ByocAgentProbeInputs,
) -> ByocAgentProbeReport:
    started = time.monotonic()
    checks: list[ByocAgentProbeCheck] = []
    mode: AgentProbeMode = "live" if inputs.control_plane_url else "mock"

    manifest, manifest_checks = _load_manifest(inputs.manifest_path)
    checks.extend(manifest_checks)
    if not inputs.install_token.strip():
        checks.append(
            _check(
                "install_token_available",
                _FAIL,
                required=True,
                details="Install token material was not available in process memory.",
            )
        )
    else:
        checks.append(
            _check(
                "install_token_available",
                _PASS,
                required=True,
                details="Install token material was available for HMAC proof.",
            )
        )

    payload: ByocAgentEnrollmentPayload | None = None
    enrollment: ByocAgentEnrollmentRequest | None = None
    heartbeat: ByocAgentHeartbeat | None = None
    if manifest is not None and _required_checks_passed(checks):
        payload, payload_checks = _build_enrollment_payload(inputs, manifest)
        checks.extend(payload_checks)
        if payload is not None:
            enrollment, enrollment_checks = _sign_enrollment(inputs, payload)
            checks.extend(enrollment_checks)
        if enrollment is not None:
            heartbeat, heartbeat_checks = _build_heartbeat(inputs, manifest)
            checks.extend(heartbeat_checks)

    enroll_response: ByocAgentEnrollmentResponse | None = None
    heartbeat_response: ByocAgentHeartbeatResponse | None = None
    if manifest is not None and enrollment is not None and heartbeat is not None:
        transport: httpx.AsyncBaseTransport | None = None
        base_url = "http://byoc-agent-probe.local"
        if mode == "mock":
            transport = httpx.ASGITransport(
                app=build_mock_control_plane_app(
                    manifest,
                    install_token=inputs.install_token,
                )
            )
            checks.append(
                _check(
                    "control_plane_endpoint",
                    _PASS,
                    required=True,
                    details="Using local mock control-plane contract endpoint.",
                )
            )
        else:
            endpoint_check = _validate_live_control_plane_url(
                inputs.control_plane_url or "",
            )
            checks.append(endpoint_check)
            base_url = (inputs.control_plane_url or "").rstrip("/")

        if checks[-1].status == _PASS:
            async with httpx.AsyncClient(
                transport=transport,
                base_url=base_url,
                timeout=inputs.timeout_s,
            ) as client:
                enroll_response, enroll_check = await _post_enrollment(
                    client,
                    enrollment,
                )
                checks.append(enroll_check)
                if enroll_response is not None:
                    heartbeat_response, heartbeat_check = await _post_heartbeat(
                        client,
                        heartbeat,
                    )
                    checks.append(heartbeat_check)

    required_checks_passed = _required_checks_passed(checks)
    status: AgentProbeStatus = _PASS if required_checks_passed else _FAIL
    identity = _manifest_identity(manifest)
    return ByocAgentProbeReport(
        schema_version="fyralis.byoc.agent_probe_report.v1",
        status=status,
        required_checks_passed=required_checks_passed,
        manifest_path=str(inputs.manifest_path),
        control_plane_mode=mode,
        control_plane_url_supplied=inputs.control_plane_url is not None,
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        agent_id=inputs.agent_id,
        agent_version=inputs.agent_version,
        enrollment_status=_PASS if enroll_response is not None else None,
        heartbeat_status=_PASS if heartbeat_response is not None else None,
        desired_revision=(
            heartbeat_response.desired_revision
            if heartbeat_response is not None
            else enroll_response.desired_revision if enroll_response is not None else None
        ),
        heartbeat_interval_seconds=(
            enroll_response.heartbeat_interval_seconds
            if enroll_response is not None
            else None
        ),
        poll_after_seconds=(
            heartbeat_response.poll_after_seconds
            if heartbeat_response is not None
            else None
        ),
        telemetry_contract=(
            enroll_response.telemetry_contract
            if enroll_response is not None
            else None
        ),
        checks=checks,
    )


def render_agent_probe_report_json(report: ByocAgentProbeReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_agent_probe_report_yaml(report: ByocAgentProbeReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _load_manifest(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocAgentProbeCheck]]:
    try:
        manifest = load_byoc_manifest(path)
    except ValidationError as exc:
        return None, [
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details="; ".join(render_validation_errors(exc)),
            )
        ]
    except Exception as exc:  # noqa: BLE001
        return None, [
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details=f"{type(exc).__name__}: could not load BYOC manifest.",
            )
        ]

    checks = [
        _check(
            "manifest_schema",
            _PASS,
            required=True,
            details="BYOC data-plane manifest schema is valid.",
        )
    ]
    violations = validate_byoc_manifest_contract(manifest)
    if violations:
        checks.append(
            _check(
                "manifest_contract",
                _FAIL,
                required=True,
                details="; ".join(violation.render() for violation in violations),
            )
        )
    else:
        checks.append(
            _check(
                "manifest_contract",
                _PASS,
                required=True,
                details="BYOC data-plane manifest preserves agent privacy guarantees.",
            )
        )
    return manifest, checks


def _build_enrollment_payload(
    inputs: ByocAgentProbeInputs,
    manifest: ByocDataPlaneManifest,
) -> tuple[ByocAgentEnrollmentPayload | None, list[ByocAgentProbeCheck]]:
    try:
        payload = enrollment_payload_from_manifest(
            manifest,
            agent_id=inputs.agent_id,
            agent_version=inputs.agent_version,
            nonce=inputs.nonce,
            requested_at=inputs.requested_at or datetime.now(UTC),
        )
    except ValidationError as exc:
        return None, [
            _check(
                "enrollment_payload_contract",
                _FAIL,
                required=True,
                details="; ".join(error["msg"] for error in exc.errors()),
            )
        ]
    return payload, [
        _check(
            "enrollment_payload_contract",
            _PASS,
            required=True,
            details="Enrollment payload uses install-token secret ref only.",
        )
    ]


def _sign_enrollment(
    inputs: ByocAgentProbeInputs,
    payload: ByocAgentEnrollmentPayload,
) -> tuple[ByocAgentEnrollmentRequest | None, list[ByocAgentProbeCheck]]:
    try:
        enrollment = signed_enrollment_request(
            payload,
            install_token=inputs.install_token,
        )
    except ValueError as exc:
        return None, [
            _check(
                "enrollment_signature_contract",
                _FAIL,
                required=True,
                details=str(exc),
            )
        ]
    return enrollment, [
        _check(
            "enrollment_signature_contract",
            _PASS,
            required=True,
            details="Enrollment request was HMAC signed without serializing token material.",
        )
    ]


def _build_heartbeat(
    inputs: ByocAgentProbeInputs,
    manifest: ByocDataPlaneManifest,
) -> tuple[ByocAgentHeartbeat | None, list[ByocAgentProbeCheck]]:
    try:
        heartbeat = heartbeat_from_manifest(
            manifest,
            agent_id=inputs.agent_id,
            agent_version=inputs.agent_version,
            sequence=inputs.sequence,
            validation_status=inputs.validation_status,
            control_plane_connected=True,
            components=(
                ByocAgentComponentStatus(
                    name="agent",
                    kind="agent",
                    status="ok",
                    detail_code="probe",
                ),
            ),
            sent_at=inputs.sent_at or datetime.now(UTC),
        )
    except ValidationError as exc:
        return None, [
            _check(
                "heartbeat_payload_contract",
                _FAIL,
                required=True,
                details="; ".join(error["msg"] for error in exc.errors()),
            )
        ]
    violations = validate_heartbeat_contract(heartbeat, manifest=manifest)
    if violations:
        return heartbeat, [
            _check(
                "heartbeat_payload_contract",
                _FAIL,
                required=True,
                details="; ".join(violation.render() for violation in violations),
            )
        ]
    return heartbeat, [
        _check(
            "heartbeat_payload_contract",
            _PASS,
            required=True,
            details="Heartbeat payload is bounded aggregate status only.",
        )
    ]


def _validate_live_control_plane_url(url: str) -> ByocAgentProbeCheck:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return _check(
            "control_plane_endpoint",
            _FAIL,
            required=True,
            details="Live control-plane URL must include scheme and host.",
        )
    if parsed.username or parsed.password:
        return _check(
            "control_plane_endpoint",
            _FAIL,
            required=True,
            details="Live control-plane URL must not contain credentials.",
        )
    if parsed.scheme != "https" and parsed.hostname not in _LOCALHOSTS:
        return _check(
            "control_plane_endpoint",
            _FAIL,
            required=True,
            details="Live control-plane URL must use https outside localhost.",
        )
    return _check(
        "control_plane_endpoint",
        _PASS,
        required=True,
        details="Live control-plane endpoint accepted for egress-only probe.",
    )


async def _post_enrollment(
    client: httpx.AsyncClient,
    enrollment: ByocAgentEnrollmentRequest,
) -> tuple[ByocAgentEnrollmentResponse | None, ByocAgentProbeCheck]:
    try:
        response = await client.post(
            "/byoc/agent/enroll",
            json=enrollment.model_dump(mode="json"),
        )
    except httpx.TimeoutException:
        return None, _check(
            "enrollment_request",
            _FAIL,
            required=True,
            details="Enrollment request timed out.",
        )
    except httpx.HTTPError:
        return None, _check(
            "enrollment_request",
            _FAIL,
            required=True,
            details="Enrollment request failed before a contract response.",
        )
    if response.status_code != 200:
        return None, _check(
            "enrollment_request",
            _FAIL,
            required=True,
            details="Enrollment request was rejected by the control plane.",
            metrics={"status_code": response.status_code},
        )
    try:
        parsed = ByocAgentEnrollmentResponse.model_validate(response.json())
    except (ValueError, ValidationError):
        return None, _check(
            "enrollment_response_contract",
            _FAIL,
            required=True,
            details="Enrollment response did not match the agent contract.",
            metrics={"status_code": response.status_code},
        )
    return parsed, _check(
        "enrollment_request",
        _PASS,
        required=True,
        details="Enrollment request accepted.",
        metrics={"status_code": response.status_code},
    )


async def _post_heartbeat(
    client: httpx.AsyncClient,
    heartbeat: ByocAgentHeartbeat,
) -> tuple[ByocAgentHeartbeatResponse | None, ByocAgentProbeCheck]:
    try:
        response = await client.post(
            "/byoc/agent/heartbeat",
            json=heartbeat.model_dump(mode="json"),
        )
    except httpx.TimeoutException:
        return None, _check(
            "heartbeat_request",
            _FAIL,
            required=True,
            details="Heartbeat request timed out.",
        )
    except httpx.HTTPError:
        return None, _check(
            "heartbeat_request",
            _FAIL,
            required=True,
            details="Heartbeat request failed before a contract response.",
        )
    if response.status_code != 200:
        return None, _check(
            "heartbeat_request",
            _FAIL,
            required=True,
            details="Heartbeat request was rejected by the control plane.",
            metrics={"status_code": response.status_code},
        )
    try:
        parsed = ByocAgentHeartbeatResponse.model_validate(response.json())
    except (ValueError, ValidationError):
        return None, _check(
            "heartbeat_response_contract",
            _FAIL,
            required=True,
            details="Heartbeat response did not match the agent contract.",
            metrics={"status_code": response.status_code},
        )
    return parsed, _check(
        "heartbeat_request",
        _PASS,
        required=True,
        details="Heartbeat request accepted.",
        metrics={"status_code": response.status_code},
    )


def _check(
    name: str,
    status: AgentProbeStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocAgentProbeCheck:
    return ByocAgentProbeCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _required_checks_passed(checks: list[ByocAgentProbeCheck]) -> bool:
    return all(check.status != _FAIL for check in checks if check.required)


def _manifest_identity(manifest: ByocDataPlaneManifest | None) -> dict[str, str]:
    if manifest is None:
        return {}
    return {
        "deployment_id": manifest.deployment_id,
        "customer_id": manifest.customer_id,
        "cloud_provider": manifest.cloud_provider,
        "region": manifest.region,
        "artifact_revision": manifest.artifact_revision,
    }


__all__ = [
    "ByocAgentProbeCheck",
    "ByocAgentProbeInputs",
    "ByocAgentProbeReport",
    "render_agent_probe_report_json",
    "render_agent_probe_report_yaml",
    "run_byoc_agent_probe",
]
