"""Bounded local BYOC data-plane agent runner skeleton.

This module is the first executable shape of the customer-side agent loop. It
does not apply revisions, rotate tokens, run mTLS enrollment, or daemonize.
Instead it proves the runtime cadence that later packaging will keep:
enroll once, poll metadata-only desired state, submit bounded heartbeat status,
and emit a sanitized report.
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
    ByocAgentEnrollmentRequest,
    ByocAgentEnrollmentResponse,
    ByocAgentHeartbeat,
    ByocAgentHeartbeatResponse,
    enrollment_payload_from_manifest,
    heartbeat_from_manifest,
    signed_enrollment_request,
    validate_heartbeat_contract,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStatePollRequest,
    ByocAgentDesiredStateResponse,
    desired_state_poll_payload,
    signed_desired_state_poll_request,
)
from services.platform.runtime.byoc_agent_local_control_plane import (
    build_local_byoc_agent_control_plane_app,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    load_byoc_manifest,
    render_validation_errors,
    validate_byoc_manifest_contract,
)


AgentRunnerStatus = Literal["pass", "fail"]
AgentRunnerMode = Literal["mock", "live"]
HeartbeatStatus = Literal["unknown", "passing", "degraded", "failing"]

_PASS: AgentRunnerStatus = "pass"
_FAIL: AgentRunnerStatus = "fail"
_LOCALHOSTS = {"localhost", "127.0.0.1", "::1"}
_MAX_ITERATIONS = 10


@dataclass(frozen=True, slots=True)
class ByocAgentRunnerCheck:
    name: str
    status: AgentRunnerStatus
    required: bool
    details: str
    metrics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ByocAgentRunnerIteration:
    index: int
    sequence: int
    desired_revision: str
    rollout_action: str
    config_epoch: int
    heartbeat_status: AgentRunnerStatus
    poll_after_seconds: int


@dataclass(frozen=True, slots=True)
class ByocAgentRunnerReport:
    schema_version: Literal["fyralis.byoc.agent_runner_report.v1"]
    status: AgentRunnerStatus
    required_checks_passed: bool
    manifest_path: str
    control_plane_mode: AgentRunnerMode
    control_plane_url_supplied: bool
    elapsed_seconds: float
    deployment_id: str | None
    customer_id: str | None
    cloud_provider: str | None
    region: str | None
    artifact_revision: str | None
    agent_id: str
    agent_version: str
    iterations_requested: int
    iterations_completed: int
    enrollment_status: AgentRunnerStatus | None
    desired_state_poll_count: int
    heartbeat_count: int
    final_desired_revision: str | None
    final_rollout_action: str | None
    final_config_epoch: int | None
    heartbeat_interval_seconds: int | None
    next_poll_after_seconds: int | None
    telemetry_contract: str | None
    checks: list[ByocAgentRunnerCheck]
    iterations: list[ByocAgentRunnerIteration]

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ByocAgentRunnerInputs:
    manifest_path: Path
    install_token: str
    agent_id: str = "agt_localrunner001"
    agent_version: str = "local-runner-skeleton"
    nonce_prefix: str = "nonce-local-agent-runner"
    starting_sequence: int = 1
    iterations: int = 1
    validation_status: HeartbeatStatus = "passing"
    control_plane_url: str | None = None
    timeout_s: float = 5.0
    requested_at: datetime | None = None
    sent_at: datetime | None = None


async def run_byoc_agent_runner(
    inputs: ByocAgentRunnerInputs,
) -> ByocAgentRunnerReport:
    started = time.monotonic()
    checks: list[ByocAgentRunnerCheck] = []
    iterations: list[ByocAgentRunnerIteration] = []
    mode: AgentRunnerMode = "live" if inputs.control_plane_url else "mock"

    manifest, manifest_checks = _load_manifest(inputs.manifest_path)
    checks.extend(manifest_checks)
    checks.extend(_input_checks(inputs))

    enrollment_response: ByocAgentEnrollmentResponse | None = None
    desired_state_response: ByocAgentDesiredStateResponse | None = None
    heartbeat_response: ByocAgentHeartbeatResponse | None = None
    desired_state_poll_count = 0
    heartbeat_count = 0

    if manifest is not None and _required_checks_passed(checks):
        enrollment, enrollment_checks = _build_enrollment(inputs, manifest)
        checks.extend(enrollment_checks)
        if enrollment is not None and _required_checks_passed(checks):
            transport: httpx.AsyncBaseTransport | None = None
            base_url = "http://byoc-agent-runner.local"
            if mode == "mock":
                transport = httpx.ASGITransport(
                    app=build_local_byoc_agent_control_plane_app(
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
                checks.append(_validate_live_control_plane_url(inputs.control_plane_url))
                base_url = (inputs.control_plane_url or "").rstrip("/")

            if checks[-1].status == _PASS:
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url=base_url,
                    timeout=inputs.timeout_s,
                ) as client:
                    enrollment_response, enrollment_check = await _post_enrollment(
                        client,
                        enrollment,
                    )
                    checks.append(enrollment_check)
                    if enrollment_response is not None:
                        (
                            desired_state_response,
                            heartbeat_response,
                            desired_state_poll_count,
                            heartbeat_count,
                        ) = await _run_iterations(
                            client,
                            inputs,
                            manifest,
                            checks,
                            iterations,
                        )

    required_checks_passed = _required_checks_passed(checks)
    status: AgentRunnerStatus = _PASS if required_checks_passed else _FAIL
    identity = _manifest_identity(manifest)
    return ByocAgentRunnerReport(
        schema_version="fyralis.byoc.agent_runner_report.v1",
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
        iterations_requested=inputs.iterations,
        iterations_completed=len(iterations),
        enrollment_status=_PASS if enrollment_response is not None else None,
        desired_state_poll_count=desired_state_poll_count,
        heartbeat_count=heartbeat_count,
        final_desired_revision=(
            desired_state_response.desired_revision
            if desired_state_response is not None
            else None
        ),
        final_rollout_action=(
            desired_state_response.rollout_action
            if desired_state_response is not None
            else None
        ),
        final_config_epoch=(
            desired_state_response.config_epoch
            if desired_state_response is not None
            else None
        ),
        heartbeat_interval_seconds=(
            enrollment_response.heartbeat_interval_seconds
            if enrollment_response is not None
            else None
        ),
        next_poll_after_seconds=(
            heartbeat_response.poll_after_seconds
            if heartbeat_response is not None
            else (
                desired_state_response.poll_after_seconds
                if desired_state_response is not None
                else None
            )
        ),
        telemetry_contract=(
            enrollment_response.telemetry_contract
            if enrollment_response is not None
            else None
        ),
        checks=checks,
        iterations=iterations,
    )


def render_agent_runner_report_json(report: ByocAgentRunnerReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_agent_runner_report_yaml(report: ByocAgentRunnerReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


async def _run_iterations(
    client: httpx.AsyncClient,
    inputs: ByocAgentRunnerInputs,
    manifest: ByocDataPlaneManifest,
    checks: list[ByocAgentRunnerCheck],
    iterations: list[ByocAgentRunnerIteration],
) -> tuple[
    ByocAgentDesiredStateResponse | None,
    ByocAgentHeartbeatResponse | None,
    int,
    int,
]:
    latest_desired: ByocAgentDesiredStateResponse | None = None
    latest_heartbeat: ByocAgentHeartbeatResponse | None = None
    desired_state_poll_count = 0
    heartbeat_count = 0
    last_seen_desired_revision: str | None = manifest.artifact_revision

    for iteration_index in range(inputs.iterations):
        poll, poll_checks = _build_desired_state_poll(
            inputs,
            manifest,
            iteration_index=iteration_index,
            last_seen_desired_revision=last_seen_desired_revision,
        )
        checks.extend(poll_checks)
        if poll is None or not _required_checks_passed(checks):
            break
        latest_desired, desired_state_check = await _post_desired_state(client, poll)
        checks.append(desired_state_check)
        if latest_desired is None:
            break
        desired_state_poll_count += 1
        last_seen_desired_revision = latest_desired.desired_revision

        sequence = inputs.starting_sequence + iteration_index
        heartbeat, heartbeat_checks = _build_heartbeat(
            inputs,
            manifest,
            sequence=sequence,
        )
        checks.extend(heartbeat_checks)
        if heartbeat is None or not _required_checks_passed(checks):
            break
        latest_heartbeat, heartbeat_check = await _post_heartbeat(client, heartbeat)
        checks.append(heartbeat_check)
        if latest_heartbeat is None:
            break
        heartbeat_count += 1
        iterations.append(
            ByocAgentRunnerIteration(
                index=iteration_index + 1,
                sequence=sequence,
                desired_revision=latest_desired.desired_revision,
                rollout_action=latest_desired.rollout_action,
                config_epoch=latest_desired.config_epoch,
                heartbeat_status=_PASS,
                poll_after_seconds=latest_heartbeat.poll_after_seconds,
            )
        )

    return (
        latest_desired,
        latest_heartbeat,
        desired_state_poll_count,
        heartbeat_count,
    )


def _load_manifest(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocAgentRunnerCheck]]:
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


def _input_checks(inputs: ByocAgentRunnerInputs) -> list[ByocAgentRunnerCheck]:
    checks: list[ByocAgentRunnerCheck] = []
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
    if inputs.iterations < 1 or inputs.iterations > _MAX_ITERATIONS:
        checks.append(
            _check(
                "iteration_bound",
                _FAIL,
                required=True,
                details=f"Iterations must be between 1 and {_MAX_ITERATIONS}.",
                metrics={"iterations": inputs.iterations},
            )
        )
    else:
        checks.append(
            _check(
                "iteration_bound",
                _PASS,
                required=True,
                details="Runner iteration count is bounded.",
                metrics={"iterations": inputs.iterations},
            )
        )
    if inputs.timeout_s <= 0:
        checks.append(
            _check(
                "timeout_bound",
                _FAIL,
                required=True,
                details="Timeout must be positive.",
            )
        )
    return checks


def _build_enrollment(
    inputs: ByocAgentRunnerInputs,
    manifest: ByocDataPlaneManifest,
) -> tuple[ByocAgentEnrollmentRequest | None, list[ByocAgentRunnerCheck]]:
    try:
        payload = enrollment_payload_from_manifest(
            manifest,
            agent_id=inputs.agent_id,
            agent_version=inputs.agent_version,
            nonce=f"{inputs.nonce_prefix}-enroll",
            requested_at=inputs.requested_at or datetime.now(UTC),
        )
        enrollment = signed_enrollment_request(
            payload,
            install_token=inputs.install_token,
        )
    except ValidationError as exc:
        return None, [
            _check(
                "enrollment_contract",
                _FAIL,
                required=True,
                details="; ".join(error["msg"] for error in exc.errors()),
            )
        ]
    except ValueError as exc:
        return None, [
            _check(
                "enrollment_contract",
                _FAIL,
                required=True,
                details=str(exc),
            )
        ]
    return enrollment, [
        _check(
            "enrollment_contract",
            _PASS,
            required=True,
            details="Enrollment request was HMAC signed without token material.",
        )
    ]


def _build_desired_state_poll(
    inputs: ByocAgentRunnerInputs,
    manifest: ByocDataPlaneManifest,
    *,
    iteration_index: int,
    last_seen_desired_revision: str | None,
) -> tuple[ByocAgentDesiredStatePollRequest | None, list[ByocAgentRunnerCheck]]:
    try:
        payload = desired_state_poll_payload(
            deployment_id=manifest.deployment_id,
            customer_id=manifest.customer_id,
            agent_id=inputs.agent_id,
            agent_version=inputs.agent_version,
            artifact_revision=manifest.artifact_revision,
            install_token_secret_ref=manifest.secrets.bootstrap_token_secret_ref,
            nonce=f"{inputs.nonce_prefix}-desired-{iteration_index + 1:03d}",
            last_seen_desired_revision=last_seen_desired_revision,
            requested_at=inputs.requested_at or datetime.now(UTC),
        )
        poll = signed_desired_state_poll_request(
            payload,
            install_token=inputs.install_token,
        )
    except ValidationError as exc:
        return None, [
            _check(
                "desired_state_poll_contract",
                _FAIL,
                required=True,
                details="; ".join(error["msg"] for error in exc.errors()),
            )
        ]
    except ValueError as exc:
        return None, [
            _check(
                "desired_state_poll_contract",
                _FAIL,
                required=True,
                details=str(exc),
            )
        ]
    return poll, [
        _check(
            "desired_state_poll_contract",
            _PASS,
            required=True,
            details="Desired-state poll was HMAC signed and metadata-only.",
            metrics={"iteration": iteration_index + 1},
        )
    ]


def _build_heartbeat(
    inputs: ByocAgentRunnerInputs,
    manifest: ByocDataPlaneManifest,
    *,
    sequence: int,
) -> tuple[ByocAgentHeartbeat | None, list[ByocAgentRunnerCheck]]:
    try:
        heartbeat = heartbeat_from_manifest(
            manifest,
            agent_id=inputs.agent_id,
            agent_version=inputs.agent_version,
            sequence=sequence,
            validation_status=inputs.validation_status,
            control_plane_connected=True,
            components=(
                ByocAgentComponentStatus(
                    name="agent",
                    kind="agent",
                    status="ok",
                    detail_code="runner_loop",
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
            metrics={"sequence": sequence},
        )
    ]


def _validate_live_control_plane_url(url: str | None) -> ByocAgentRunnerCheck:
    parsed = urlparse(url or "")
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
        details="Live control-plane endpoint accepted for egress-only runner.",
    )


async def _post_enrollment(
    client: httpx.AsyncClient,
    enrollment: ByocAgentEnrollmentRequest,
) -> tuple[ByocAgentEnrollmentResponse | None, ByocAgentRunnerCheck]:
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


async def _post_desired_state(
    client: httpx.AsyncClient,
    poll: ByocAgentDesiredStatePollRequest,
) -> tuple[ByocAgentDesiredStateResponse | None, ByocAgentRunnerCheck]:
    try:
        response = await client.post(
            "/byoc/agent/desired-state",
            json=poll.model_dump(mode="json"),
        )
    except httpx.TimeoutException:
        return None, _check(
            "desired_state_request",
            _FAIL,
            required=True,
            details="Desired-state request timed out.",
        )
    except httpx.HTTPError:
        return None, _check(
            "desired_state_request",
            _FAIL,
            required=True,
            details="Desired-state request failed before a contract response.",
        )
    if response.status_code != 200:
        return None, _check(
            "desired_state_request",
            _FAIL,
            required=True,
            details="Desired-state request was rejected by the control plane.",
            metrics={"status_code": response.status_code},
        )
    try:
        parsed = ByocAgentDesiredStateResponse.model_validate(response.json())
    except (ValueError, ValidationError):
        return None, _check(
            "desired_state_response_contract",
            _FAIL,
            required=True,
            details="Desired-state response did not match the agent contract.",
            metrics={"status_code": response.status_code},
        )
    return parsed, _check(
        "desired_state_request",
        _PASS,
        required=True,
        details="Desired-state request accepted with metadata-only intent.",
        metrics={
            "status_code": response.status_code,
            "config_epoch": parsed.config_epoch,
        },
    )


async def _post_heartbeat(
    client: httpx.AsyncClient,
    heartbeat: ByocAgentHeartbeat,
) -> tuple[ByocAgentHeartbeatResponse | None, ByocAgentRunnerCheck]:
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
    status: AgentRunnerStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocAgentRunnerCheck:
    return ByocAgentRunnerCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _required_checks_passed(checks: list[ByocAgentRunnerCheck]) -> bool:
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
    "ByocAgentRunnerCheck",
    "ByocAgentRunnerInputs",
    "ByocAgentRunnerIteration",
    "ByocAgentRunnerReport",
    "render_agent_runner_report_json",
    "render_agent_runner_report_yaml",
    "run_byoc_agent_runner",
]
