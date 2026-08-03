from __future__ import annotations

import base64
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from services.ingest.connector_platform.deployment import (
    ArtifactAdmissionController,
    ArtifactAdmissionSettings,
    _trusted_signers,
)
from services.ingest.connector_platform.pilots import build_pilot_candidates
from services.ingest.connector_runtime.artifacts import ArtifactAttestation
from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RouteRequest,
    RoutingPolicy,
)


class MemoryArtifacts:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], ArtifactAttestation] = {}
        self.quarantined: list[tuple[str, str, str]] = []

    async def load_all(self) -> dict[tuple[str, str], ArtifactAttestation]:
        return self.items

    async def quarantine(self, connector_id: str, version: str, reason: str) -> None:
        self.quarantined.append((connector_id, version, reason))


@pytest.mark.asyncio
async def test_missing_signed_artifacts_quarantine_and_override_routing() -> None:
    candidates = build_pilot_candidates()
    routing = AtomicRoutingPolicy(
        RoutingPolicy(
            connector_modes={
                candidate.manifest.connector_id: ExecutionMode.CONNECTOR
                for candidate in candidates
            }
        )
    )
    controller = ArtifactAdmissionController(
        MemoryArtifacts(),
        routing,
        candidates,
        ArtifactAdmissionSettings({}, frozenset(), True),
    )

    admission = await controller.refresh()

    assert set(admission.quarantined) == {
        candidate.manifest.connector_id for candidate in candidates
    }
    decision = routing.resolve(
        RouteRequest(
            tenant_id=uuid4(),
            connector_id="fyralis/slack",
            source="slack",
            capability="ingestion.historical_pull",
        )
    )
    assert decision.mode is ExecutionMode.LEGACY
    assert decision.matched_scope == "artifact_quarantine"


def test_trusted_signers_parser_accepts_raw_ed25519_public_keys() -> None:
    public_key = Ed25519PrivateKey.generate().public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    encoded = base64.b64encode(public_key).decode()

    assert set(_trusted_signers('{"release": "' + encoded + '"}')) == {"release"}
