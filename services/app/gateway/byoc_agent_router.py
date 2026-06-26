"""BYOC data-plane agent enrollment and heartbeat routes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, status

from lib.shared.errors import SecretStoreError
from services.app.gateway.byoc_agent_keys import (
    ResolvedByocAgentInstallToken,
    resolver_from_app_state,
)
from services.app.gateway.settings import GatewaySettings
from services.platform.runtime.byoc_agent_contract import (
    ByocAgentEnrollmentRequest,
    ByocAgentEnrollmentResponse,
    ByocAgentHeartbeat,
    ByocAgentHeartbeatResponse,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentRegistryStore,
    InMemoryByocAgentRegistryStore,
    PostgresByocAgentRegistryStore,
    validate_agent_enrollment_request,
    validate_agent_heartbeat_request,
)
from services.platform.runtime.byoc_contract import CloudProvider, TelemetryMode


DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15
DEFAULT_AGENT_POLL_AFTER_SECONDS = 30
DEFAULT_TELEMETRY_CONTRACT = "aggregate-only-v1"


@dataclass(frozen=True, slots=True)
class _ExpectedByocAgentIdentity:
    deployment_id: str | None
    customer_id: str | None
    cloud_provider: CloudProvider | None
    region: str | None
    telemetry_mode: TelemetryMode | None
    telemetry_contract: str
    production: bool


def build_byoc_agent_router(
    *,
    store: ByocAgentRegistryStore | None = None,
    install_token: str | None = None,
    install_token_secret_ref: str | None = None,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    poll_after_seconds: int = DEFAULT_AGENT_POLL_AFTER_SECONDS,
    telemetry_contract: str = DEFAULT_TELEMETRY_CONTRACT,
) -> APIRouter:
    router = APIRouter(
        prefix="/byoc/agent",
        tags=["byoc-agent"],
    )

    @router.post("/enroll")
    async def enroll_agent(
        request: Request,
        enrollment: ByocAgentEnrollmentRequest,
    ) -> ByocAgentEnrollmentResponse:
        resolved_token = await _resolve_install_token(
            request,
            key_ref=enrollment.signature.key_ref,
            install_token=install_token,
            install_token_secret_ref=install_token_secret_ref,
        )
        expected = _expected_identity_from_state(
            request,
            telemetry_contract=telemetry_contract,
        )
        violations = validate_agent_enrollment_request(
            enrollment,
            install_token=resolved_token.secret,
            expected_install_token_secret_ref=resolved_token.key_ref,
            expected_deployment_id=expected.deployment_id,
            expected_customer_id=expected.customer_id,
            expected_cloud_provider=expected.cloud_provider,
            expected_region=expected.region,
        )
        if violations:
            status_code = (
                status.HTTP_403_FORBIDDEN
                if any("signature" in violation.path for violation in violations)
                else status.HTTP_400_BAD_REQUEST
            )
            raise HTTPException(
                status_code=status_code,
                detail={"errors": [violation.render() for violation in violations]},
            )
        return await (store or _store_from_state(request)).enroll(
            enrollment,
            desired_revision=enrollment.artifact_revision,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            telemetry_contract=expected.telemetry_contract,
        )

    @router.post("/heartbeat")
    async def heartbeat_agent(
        request: Request,
        heartbeat: ByocAgentHeartbeat,
    ) -> ByocAgentHeartbeatResponse:
        expected = _expected_identity_from_state(
            request,
            telemetry_contract=telemetry_contract,
        )
        violations = validate_agent_heartbeat_request(
            heartbeat,
            expected_deployment_id=expected.deployment_id,
            expected_customer_id=expected.customer_id,
            expected_telemetry_mode=expected.telemetry_mode,
            expected_telemetry_contract=expected.telemetry_contract,
        )
        if violations:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [violation.render() for violation in violations]},
            )
        response = await (store or _store_from_state(request)).heartbeat(
            heartbeat,
            desired_revision=heartbeat.artifact_revision,
            poll_after_seconds=poll_after_seconds,
        )
        if response is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"errors": ["agent_id: agent_not_enrolled"]},
            )
        return response

    return router


async def _resolve_install_token(
    request: Request,
    *,
    key_ref: str,
    install_token: str | None,
    install_token_secret_ref: str | None,
) -> ResolvedByocAgentInstallToken:
    settings = getattr(request.app.state, "gateway_settings", None)
    production = bool(getattr(settings, "is_production", False))
    if install_token:
        if production:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "errors": [
                        "BYOC agent enrollment install-token is not configured"
                    ]
                },
            )
        expected_key_ref = install_token_secret_ref or key_ref
        if key_ref != expected_key_ref:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"errors": ["signature.key_ref: unknown_key_ref"]},
            )
        return ResolvedByocAgentInstallToken(
            key_ref=expected_key_ref,
            secret=install_token,
            source="static_test_secret",
            secret_ref=expected_key_ref,
        )

    resolver = resolver_from_app_state(request.app.state)
    if resolver is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "errors": ["BYOC agent enrollment install-token is not configured"]
            },
        )
    try:
        resolved = await resolver.resolve(key_ref=key_ref)
    except SecretStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "errors": [
                    "BYOC agent enrollment install-token could not be resolved",
                ]
            },
        ) from exc
    if resolved is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"errors": ["signature.key_ref: unknown_key_ref"]},
        )
    return resolved


def _expected_identity_from_state(
    request: Request,
    *,
    telemetry_contract: str,
) -> _ExpectedByocAgentIdentity:
    settings = getattr(request.app.state, "gateway_settings", None)
    if not isinstance(settings, GatewaySettings):
        return _ExpectedByocAgentIdentity(
            deployment_id=None,
            customer_id=None,
            cloud_provider=None,
            region=None,
            telemetry_mode=None,
            telemetry_contract=telemetry_contract,
            production=False,
        )
    production = settings.is_production
    missing = [
        name
        for name, value in (
            ("FYRALIS_BYOC_DEPLOYMENT_ID", settings.byoc_deployment_id),
            ("FYRALIS_BYOC_CUSTOMER_ID", settings.byoc_customer_id),
            ("FYRALIS_BYOC_CLOUD_PROVIDER", settings.byoc_cloud_provider),
            ("FYRALIS_BYOC_REGION", settings.byoc_region),
        )
        if not value
    ]
    if production and missing:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "errors": [
                    "BYOC agent identity is not configured: "
                    + ", ".join(sorted(missing))
                ]
            },
        )
    telemetry_mode: TelemetryMode | None = (
        settings.telemetry_mode
        if settings.telemetry_mode in {"aggregate-only", "disabled"}
        else None
    )
    return _ExpectedByocAgentIdentity(
        deployment_id=settings.byoc_deployment_id,
        customer_id=settings.byoc_customer_id,
        cloud_provider=_cloud_provider(settings.byoc_cloud_provider),
        region=settings.byoc_region,
        telemetry_mode=telemetry_mode,
        telemetry_contract=telemetry_contract,
        production=production,
    )


def _cloud_provider(value: str | None) -> CloudProvider | None:
    if value in {"aws", "gcp", "azure", "customer-managed-kubernetes"}:
        return value  # type: ignore[return-value]
    return None


def _store_from_state(request: Request) -> ByocAgentRegistryStore:
    existing = getattr(request.app.state, "byoc_agent_registry_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocAgentRegistryStore(pool)
        request.app.state.byoc_agent_registry_store = created
        return created
    created = InMemoryByocAgentRegistryStore()
    request.app.state.byoc_agent_registry_store = created
    return created


__all__ = [
    "DEFAULT_AGENT_POLL_AFTER_SECONDS",
    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
    "DEFAULT_TELEMETRY_CONTRACT",
    "build_byoc_agent_router",
]
