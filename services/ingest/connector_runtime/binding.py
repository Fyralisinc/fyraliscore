"""Installation-scoped binding and negotiated capability resolution."""

from __future__ import annotations

from dataclasses import dataclass

from services.ingest.connector_runtime.authority import scope_authority
from services.ingest.connector_runtime.definitions import ConnectorDescription
from services.ingest.source_contract.connector import (
    BindingContext,
    CapabilityKey,
    SourceConnector,
    StaticBoundConnector,
    validate_binding_identity,
)
from services.ingest.source_contract.errors import (
    BindingError,
    CapabilityMismatchError,
    ConnectorError,
)
from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
)
from services.ingest.source_contract.versioning import SemanticVersion


@dataclass(frozen=True)
class RegisteredConnector:
    manifest: ConnectorManifest
    connector: SourceConnector
    negotiated_contract: SemanticVersion
    capability_keys: tuple[CapabilityKey[object], ...]
    origin: str
    conformance_fingerprint: str | None = None

    @property
    def connector_id(self) -> str:
        return self.manifest.connector_id

    @property
    def source(self) -> str:
        return self.manifest.source

    def bind(self, context: BindingContext) -> StaticBoundConnector:
        _validate_authority(self.manifest, context)
        try:
            raw_binding = self.connector.bind(context)
        except ConnectorError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize the boundary
            raise BindingError(
                f"connector {self.connector_id} failed while binding",
                details={
                    "connector_id": self.connector_id,
                    "exception_type": type(exc).__name__,
                },
            ) from exc
        validate_binding_identity(
            manifest=self.manifest,
            context=context,
            binding=raw_binding,
        )

        configured_refs = set(
            self.manifest.configured_capability_refs(context.authority.secret_slots)
        )
        capabilities: dict[CapabilityRef, object] = {}
        for key in self.capability_keys:
            if key.ref not in configured_refs:
                continue
            try:
                implementation = raw_binding.capability(key)
            except CapabilityMismatchError as exc:
                raise BindingError(
                    f"connector {self.connector_id} returned an invalid "
                    f"implementation for {key.ref.id}/v{key.ref.version}",
                    details=exc.details,
                ) from exc
            except ConnectorError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalize the boundary
                raise BindingError(
                    f"connector {self.connector_id} failed capability resolution",
                    details={
                        "connector_id": self.connector_id,
                        "capability": key.ref.id,
                        "version": key.ref.version,
                        "exception_type": type(exc).__name__,
                    },
                ) from exc
            if implementation is None:
                raise BindingError(
                    f"connector {self.connector_id} configured but did not bind "
                    f"{key.ref.id}/v{key.ref.version}",
                    details={
                        "connector_id": self.connector_id,
                        "capability": key.ref.id,
                        "version": key.ref.version,
                    },
                )
            capabilities[key.ref] = implementation
        return StaticBoundConnector(context.installation, capabilities)

    def describe(self) -> ConnectorDescription:
        return ConnectorDescription(
            connector_id=self.connector_id,
            source=self.source,
            connector_version=self.manifest.metadata.version,
            negotiated_contract_version=str(self.negotiated_contract),
            capabilities=tuple(key.ref for key in self.capability_keys),
            origin=self.origin,
            conformance_fingerprint=self.conformance_fingerprint,
        )


def _validate_authority(
    manifest: ConnectorManifest,
    context: BindingContext,
) -> None:
    if context.installation.connector_id != manifest.connector_id:
        raise BindingError(
            "installation connector ID does not match registry definition",
            details={
                "installation_connector_id": context.installation.connector_id,
                "registry_connector_id": manifest.connector_id,
            },
        )
    scoped = scope_authority(manifest, context.authority)
    if context.authority != scoped:
        raise BindingError(
            "binding context must carry manifest-scoped least authority",
            details={
                "connector_id": manifest.connector_id,
            },
        )


__all__ = ["RegisteredConnector"]
