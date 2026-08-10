"""Immutable registry health and snapshot fingerprint models."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from services.ingest.connector_runtime.binding import RegisteredConnector
from services.ingest.connector_runtime.definitions import ConnectorDescription
from services.ingest.connector_runtime.diagnostics import RegistryDiagnostic


class RegistryStatus(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"


@dataclass(frozen=True)
class RegistryHealth:
    status: RegistryStatus
    fingerprint: str
    connector_count: int
    connectors: tuple[ConnectorDescription, ...]
    diagnostics: tuple[RegistryDiagnostic, ...]

    @property
    def healthy(self) -> bool:
        return self.status is RegistryStatus.READY


def registry_fingerprint(
    registrations: Mapping[str, RegisteredConnector],
) -> str:
    snapshot = [
        {
            "connector_id": registration.connector_id,
            "source": registration.source,
            "aliases": list(registration.manifest.metadata.aliases),
            "connector_version": registration.manifest.metadata.version,
            "contract_version": str(registration.negotiated_contract),
            "capabilities": [
                {"id": key.ref.id, "version": key.ref.version}
                for key in registration.capability_keys
            ],
            "origin": registration.origin,
            "conformance_fingerprint": registration.conformance_fingerprint,
        }
        for registration in (
            registrations[key] for key in sorted(registrations)
        )
    ]
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["RegistryHealth", "RegistryStatus", "registry_fingerprint"]
