"""Local BYOC agent control-plane harness for offline agent contract proof."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException

from services.platform.runtime.byoc_agent_contract import (
    ByocAgentEnrollmentRequest,
    ByocAgentEnrollmentResponse,
    ByocAgentHeartbeat,
    ByocAgentHeartbeatResponse,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStatePollRequest,
    ByocAgentDesiredStateResponse,
    InMemoryByocAgentRegistryStore,
    validate_agent_enrollment_request,
    validate_agent_heartbeat_request,
    validate_desired_state_poll_request,
)
from services.platform.runtime.byoc_contract import ByocDataPlaneManifest


def build_local_byoc_agent_control_plane_app(
    manifest: ByocDataPlaneManifest,
    *,
    install_token: str,
    desired_revision: str | None = None,
    config_epoch: int = 0,
) -> FastAPI:
    """Build an in-process egress target for local BYOC agent checks.

    This is not a production control plane. It intentionally mirrors the
    hosted enrollment, desired-state, and heartbeat contracts while keeping
    storage in memory and metadata-only.
    """

    app = FastAPI(title="Fyralis BYOC Local Agent Control Plane")
    store = InMemoryByocAgentRegistryStore()
    resolved_desired_revision = desired_revision or manifest.artifact_revision

    @app.post("/byoc/agent/enroll")
    async def enroll(
        request: ByocAgentEnrollmentRequest,
    ) -> ByocAgentEnrollmentResponse:
        violations = validate_agent_enrollment_request(
            request,
            install_token=install_token,
            expected_install_token_secret_ref=(
                manifest.secrets.bootstrap_token_secret_ref
            ),
            expected_deployment_id=manifest.deployment_id,
            expected_customer_id=manifest.customer_id,
            expected_cloud_provider=manifest.cloud_provider,
            expected_region=manifest.region,
        )
        if violations:
            raise HTTPException(
                status_code=403,
                detail={"errors": [violation.render() for violation in violations]},
            )
        return await store.enroll(
            request,
            desired_revision=resolved_desired_revision,
            heartbeat_interval_seconds=(
                manifest.connectivity.heartbeat_interval_seconds
            ),
            telemetry_contract=manifest.telemetry.contract,
        )

    @app.post("/byoc/agent/desired-state")
    async def desired_state(
        request: ByocAgentDesiredStatePollRequest,
    ) -> ByocAgentDesiredStateResponse:
        violations = validate_desired_state_poll_request(
            request,
            install_token=install_token,
            expected_install_token_secret_ref=(
                manifest.secrets.bootstrap_token_secret_ref
            ),
            expected_deployment_id=manifest.deployment_id,
            expected_customer_id=manifest.customer_id,
        )
        if violations:
            raise HTTPException(
                status_code=403,
                detail={"errors": [violation.render() for violation in violations]},
            )
        response = await store.desired_state(
            request,
            poll_after_seconds=manifest.connectivity.agent_poll_interval_seconds,
            config_epoch=config_epoch,
        )
        if response is None:
            raise HTTPException(
                status_code=403,
                detail={"errors": ["agent_id: agent_not_enrolled"]},
            )
        return response

    @app.post("/byoc/agent/heartbeat")
    async def heartbeat(
        request: ByocAgentHeartbeat,
    ) -> ByocAgentHeartbeatResponse:
        violations = validate_agent_heartbeat_request(
            request,
            expected_deployment_id=manifest.deployment_id,
            expected_customer_id=manifest.customer_id,
            expected_telemetry_mode=manifest.telemetry.mode,
            expected_telemetry_contract=manifest.telemetry.contract,
        )
        if violations:
            raise HTTPException(
                status_code=403,
                detail={"errors": [violation.render() for violation in violations]},
            )
        response = await store.heartbeat(
            request,
            desired_revision=resolved_desired_revision,
            poll_after_seconds=manifest.connectivity.agent_poll_interval_seconds,
        )
        if response is None:
            raise HTTPException(
                status_code=403,
                detail={"errors": ["agent_id: agent_not_enrolled"]},
            )
        return response

    return app


__all__ = ["build_local_byoc_agent_control_plane_app"]
