from uuid import uuid4

import pytest

from services.ingest.connector_platform.workflow_wiring import (
    build_workflow_connector_wiring,
)
from services.ingest.connector_runtime.policy import RouteRequest
from services.ingest.source_contract.errors import SourceUnavailableError


@pytest.mark.asyncio
async def test_workflow_router_owns_every_manifest_source() -> None:
    wiring = build_workflow_connector_wiring()
    try:
        sources = tuple(
            description.source
            for description in wiring.composition.registry.descriptions()
        )
        assert all(
            wiring.router.supports(source)
            and wiring.router.is_native(source)
            for source in sources
        )
        assert len(sources) == 26
    finally:
        await wiring.close()


@pytest.mark.asyncio
async def test_workflow_router_preserves_installation_generation_fences() -> None:
    wiring = build_workflow_connector_wiring()
    installation_id = uuid4()
    tenant_id = uuid4()
    try:
        row = {
            "id": installation_id,
            "tenant_id": tenant_id,
            "connector_id": "fyralis/slack",
            "generation": 7,
            "observed_generation": 6,
            "desired_state": "Ready",
            "observed_phase": "Degraded",
        }
        installation = wiring.router._installation("slack", row)
        lifecycle = wiring.router._lifecycle(installation, row)

        assert installation.generation == 7
        assert lifecycle.generation == 7
        assert lifecycle.observed_generation == 6
        assert lifecycle.observed.value == "Degraded"
    finally:
        await wiring.close()


@pytest.mark.asyncio
async def test_database_free_owner_fails_closed_when_signing_is_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS", "true")
    wiring = build_workflow_connector_wiring()
    try:
        with pytest.raises(SourceUnavailableError, match="quarantined"):
            wiring.composition.routing.resolve(
                RouteRequest(
                    uuid4(),
                    "fyralis/slack",
                    "slack",
                    "semantic.normalization",
                )
            )
    finally:
        await wiring.close()
