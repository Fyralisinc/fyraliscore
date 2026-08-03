from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_platform.lifecycle_controller import (
    ContinuousInstallationController,
)
from services.ingest.connector_platform.pilots import (
    NOTION_CONNECTOR_ID,
    build_pilot_composition,
)
from services.ingest.connector_runtime.authority import InstallationAuthority
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationLifecycle,
    InstallationPhase,
)
from services.ingest.source_contract.host_services import SecretValue


class _Authorities:
    def __init__(self, authority) -> None:
        self.authority = authority
        self.revoked = False

    async def load(self, installation_id):
        return self.authority

    async def grant(self, authority):
        self.authority = authority

    async def revoke(self, installation_id, *, revoked_at, reason):
        self.revoked = True


class _LifecycleRepository:
    def __init__(self, lifecycle) -> None:
        self.lifecycle = lifecycle
        self.retired = False

    async def list_due(self, *, limit=100):
        return (self.lifecycle,)

    async def save(self, lifecycle):
        self.lifecycle = lifecycle

    async def retire_credentials(self, installation_id):
        self.retired = True


def _installation(desired=DesiredInstallationState.READY):
    return InstallationLifecycle(
        installation_id=uuid4(),
        tenant_id=uuid4(),
        connector_id=NOTION_CONNECTOR_ID,
        desired=desired,
        observed=InstallationPhase.INITIALIZING,
        generation=1,
        observed_generation=0,
    )


def _authority(lifecycle):
    return InstallationAuthority(
        installation_id=lifecycle.installation_id,
        tenant_id=lifecycle.tenant_id,
        connector_id=lifecycle.connector_id,
        generation=1,
        credential_owner="oauth_callback",
        secret_slots=frozenset({"oauth_access_token"}),
        outbound_hosts=frozenset({"api.notion.com"}),
        maximum_trust_tier="attested_agent",
        granted_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_continuous_controller_observes_health_and_marks_ready() -> None:
    lifecycle = _installation()
    authorities = _Authorities(_authority(lifecycle))
    repository = _LifecycleRepository(lifecycle)

    async def read_secret(_installation, _slot):
        return SecretValue.from_text("token")

    async with httpx.AsyncClient() as client:
        controller = ContinuousInstallationController(
            build_pilot_composition().registry,
            authorities,
            repository,
            HostServicesFactory(http_client=client, secret_reader=read_secret),
        )
        assert await controller.run_once() == 1

    assert repository.lifecycle.observed is InstallationPhase.READY


@pytest.mark.asyncio
async def test_continuous_controller_reconciles_cleanup_and_revokes_authority() -> None:
    lifecycle = _installation(DesiredInstallationState.REMOVED)
    authorities = _Authorities(_authority(lifecycle))
    repository = _LifecycleRepository(lifecycle)
    async with httpx.AsyncClient() as client:
        controller = ContinuousInstallationController(
            build_pilot_composition().registry,
            authorities,
            repository,
            HostServicesFactory(http_client=client),
        )
        await controller.run_once()

    assert repository.lifecycle.observed is InstallationPhase.REMOVED
    assert repository.retired
    assert authorities.revoked


@pytest.mark.asyncio
async def test_continuous_controller_rejects_quarantined_artifact() -> None:
    lifecycle = _installation()
    authorities = _Authorities(_authority(lifecycle))
    repository = _LifecycleRepository(lifecycle)
    async with httpx.AsyncClient() as client:
        controller = ContinuousInstallationController(
            build_pilot_composition().registry,
            authorities,
            repository,
            HostServicesFactory(http_client=client),
            admitted_connector_ids=frozenset(),
        )
        await controller.run_once()

    assert repository.lifecycle.observed is InstallationPhase.FAILED
    assert repository.lifecycle.conditions[-1].reason == "ReconcileFailed"
