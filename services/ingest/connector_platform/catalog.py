"""Manifest-derived inventory for all Fyralis ingestion source families."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.ingest.source_contract.manifest import load_connector_manifests


@dataclass(frozen=True)
class ConnectorCatalogEntry:
    source: str
    ingress_kinds: tuple[str, ...]
    connector_id: str
    connector_version: str
    implementation: str


_MANIFEST_DIRECTORY = Path(__file__).resolve().parents[1] / "connectors" / "manifests"


def _load_catalog() -> tuple[ConnectorCatalogEntry, ...]:
    return tuple(
        ConnectorCatalogEntry(
            source=manifest.source,
            ingress_kinds=manifest.spec.ingress_kinds,
            connector_id=manifest.connector_id,
            connector_version=manifest.metadata.version,
            implementation=manifest.spec.implementation,
        )
        for manifest in load_connector_manifests(_MANIFEST_DIRECTORY)
    )


CONNECTOR_CATALOG = _load_catalog()


def catalog_by_source() -> dict[str, ConnectorCatalogEntry]:
    return {entry.source: entry for entry in CONNECTOR_CATALOG}


__all__ = ["CONNECTOR_CATALOG", "ConnectorCatalogEntry", "catalog_by_source"]
