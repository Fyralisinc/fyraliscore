"""Production artifact admission and connector quarantine control."""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from services.ingest.connector_runtime.artifacts import (
    ArtifactAdmission,
    ArtifactAttestation,
    ArtifactDeploymentPolicy,
    DeploymentStatus,
    connector_artifact_sha256,
)
from services.ingest.connector_runtime.definitions import ConnectorCandidate
from services.ingest.connector_runtime.policy import AtomicRoutingPolicy


REQUIRE_SIGNED_ARTIFACTS_ENV = "SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS"
TRUSTED_SIGNERS_ENV = "SOURCE_CONNECTOR_TRUSTED_SIGNERS_JSON"
ALLOWED_BUILDERS_ENV = "SOURCE_CONNECTOR_ALLOWED_BUILDERS"


class ArtifactRepository(Protocol):
    async def load_all(self) -> dict[tuple[str, str], ArtifactAttestation]: ...

    async def quarantine(
        self,
        connector_id: str,
        connector_version: str,
        reason: str,
    ) -> None: ...


def _enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _trusted_signers(value: str | None) -> dict[str, Ed25519PublicKey]:
    if not value:
        return {}
    raw = json.loads(value)
    if not isinstance(raw, dict):
        raise ValueError(f"{TRUSTED_SIGNERS_ENV} must be a JSON object")
    signers: dict[str, Ed25519PublicKey] = {}
    for key_id, encoded in raw.items():
        if not isinstance(key_id, str) or not isinstance(encoded, str):
            raise ValueError(f"{TRUSTED_SIGNERS_ENV} keys and values must be strings")
        try:
            public_bytes = base64.b64decode(encoded, validate=True)
            signers[key_id] = Ed25519PublicKey.from_public_bytes(public_bytes)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"{TRUSTED_SIGNERS_ENV} contains an invalid Ed25519 key"
            ) from exc
    return signers


@dataclass(frozen=True)
class ArtifactAdmissionSettings:
    trusted_signers: Mapping[str, Ed25519PublicKey]
    allowed_builders: frozenset[str]
    require_signed: bool

    @classmethod
    def from_env(cls) -> "ArtifactAdmissionSettings":
        production = any(
            os.environ.get(name, "").strip().lower() in {"prod", "production"}
            for name in ("COMPANY_OS_ENV", "FYRALIS_ENV")
        )
        return cls(
            trusted_signers=_trusted_signers(os.environ.get(TRUSTED_SIGNERS_ENV)),
            allowed_builders=frozenset(
                item.strip()
                for item in os.environ.get(ALLOWED_BUILDERS_ENV, "").split(",")
                if item.strip()
            ),
            require_signed=(
                production or _enabled(os.environ.get(REQUIRE_SIGNED_ARTIFACTS_ENV))
            ),
        )


class ArtifactAdmissionController:
    """Verify artifacts and make quarantine override every routing revision."""

    def __init__(
        self,
        repository: ArtifactRepository,
        routing: AtomicRoutingPolicy,
        candidates: Sequence[ConnectorCandidate],
        settings: ArtifactAdmissionSettings,
    ) -> None:
        self._repository = repository
        self._routing = routing
        self._candidates = tuple(candidates)
        self._measured_artifacts = {
            (
                candidate.manifest.connector_id,
                candidate.manifest.metadata.version,
            ): connector_artifact_sha256(candidate.manifest)
            for candidate in self._candidates
        }
        self._policy = ArtifactDeploymentPolicy(
            settings.trusted_signers,
            allowed_builders=settings.allowed_builders,
            require_signed=settings.require_signed,
        )

    async def refresh(self) -> ArtifactAdmission:
        attestations = await self._repository.load_all()
        admission = self._policy.admit(
            self._candidates,
            attestations,
            measured_artifacts=self._measured_artifacts,
        )
        self._routing.replace_quarantine(admission.quarantined)
        for candidate in self._candidates:
            connector_id = candidate.manifest.connector_id
            reason = admission.quarantined.get(connector_id)
            key = (connector_id, candidate.manifest.metadata.version)
            attestation = attestations.get(key)
            if (
                reason is not None
                and attestation is not None
                and attestation.deployment_status is not DeploymentStatus.QUARANTINED
            ):
                await self._repository.quarantine(
                    connector_id,
                    candidate.manifest.metadata.version,
                    reason,
                )
        return admission


__all__ = [
    "ALLOWED_BUILDERS_ENV",
    "ArtifactAdmissionController",
    "ArtifactAdmissionSettings",
    "REQUIRE_SIGNED_ARTIFACTS_ENV",
    "TRUSTED_SIGNERS_ENV",
]
