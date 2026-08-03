"""Fyralis Source Connector Contract.

This package contains only transport-neutral contracts. It does not discover
connectors, import source implementations, or touch runtime infrastructure.
"""

from services.ingest.source_contract.connector import (
    BindingContext,
    BoundConnector,
    CapabilityKey,
    GrantedAuthority,
    OperationContext,
    SourceConnector,
    StaticBoundConnector,
)
from services.ingest.source_contract.errors import ConnectorError
from services.ingest.source_contract.manifest import (
    CapabilityDeclaration,
    CapabilityRef,
    ConnectorManifest,
    load_connector_manifest,
    load_connector_manifests,
)
from services.ingest.source_contract.models import InstallationRef
from services.ingest.source_contract.versioning import SemanticVersion, VersionRange


__all__ = [
    "BindingContext",
    "BoundConnector",
    "CapabilityDeclaration",
    "CapabilityKey",
    "CapabilityRef",
    "ConnectorError",
    "ConnectorManifest",
    "GrantedAuthority",
    "InstallationRef",
    "OperationContext",
    "SemanticVersion",
    "SourceConnector",
    "StaticBoundConnector",
    "VersionRange",
    "load_connector_manifest",
    "load_connector_manifests",
]
