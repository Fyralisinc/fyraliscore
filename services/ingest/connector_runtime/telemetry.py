"""Uniform metadata and telemetry around capability execution."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from services.ingest.source_contract.host_services import HostServices


@dataclass(frozen=True)
class ConnectorTelemetryFields:
    connector_id: str
    capability: str
    contract_version: str
    connector_version: str
    execution_id: UUID
    registry_fingerprint: str
    installation_id: UUID

    def attributes(self) -> tuple[tuple[str, str], ...]:
        return (
            ("connector_id", self.connector_id),
            ("capability", self.capability),
            ("contract_version", self.contract_version),
            ("connector_version", self.connector_version),
            ("execution_id", str(self.execution_id)),
            ("registry_fingerprint", self.registry_fingerprint),
            ("installation_id", str(self.installation_id)),
        )

    def fields(self) -> dict[str, str]:
        return dict(self.attributes())


class CapabilityTelemetry:
    def __init__(self, services: HostServices) -> None:
        self._services = services

    def started(self, fields: ConnectorTelemetryFields) -> float:
        attributes = fields.attributes()
        self._services.metrics.increment(
            "source_connector.capability.started", attributes=attributes
        )
        self._services.logger.info(
            "source_connector.capability.started", **fields.fields()
        )
        return time.monotonic()

    def completed(
        self,
        fields: ConnectorTelemetryFields,
        started_at: float,
        *,
        mode: str,
    ) -> None:
        attributes = fields.attributes() + (("mode", mode),)
        self._services.metrics.increment(
            "source_connector.capability.completed", attributes=attributes
        )
        self._services.metrics.observe(
            "source_connector.capability.duration_ms",
            (time.monotonic() - started_at) * 1000,
            attributes=attributes,
        )
        self._services.logger.info(
            "source_connector.capability.completed",
            **fields.fields(),
            mode=mode,
        )

    def failed(
        self,
        fields: ConnectorTelemetryFields,
        started_at: float,
        *,
        failure_code: str,
        retryable: bool,
    ) -> None:
        attributes = fields.attributes() + (
            ("failure_code", failure_code),
            ("retryable", str(retryable).lower()),
        )
        self._services.metrics.increment(
            "source_connector.capability.failed", attributes=attributes
        )
        self._services.metrics.observe(
            "source_connector.capability.duration_ms",
            (time.monotonic() - started_at) * 1000,
            attributes=attributes,
        )
        self._services.logger.error(
            "source_connector.capability.failed",
            **fields.fields(),
            failure_code=failure_code,
            retryable=retryable,
        )


__all__ = ["CapabilityTelemetry", "ConnectorTelemetryFields"]
