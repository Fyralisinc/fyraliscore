"""Cross-layer validation for manifest-generated fleet wiring."""

from __future__ import annotations

from services.ingest.connector_platform.catalog import CONNECTOR_CATALOG
from services.ingest.connector_runtime.definitions import ConnectorCandidate
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.source_contract.manifest import MANIFEST_API_VERSION, Maturity
from services.ingest.source_contract.source_catalog import source_ids


def validate_native_fleet(candidates: tuple[ConnectorCandidate, ...]) -> None:
    manifests = tuple(candidate.manifest for candidate in candidates)
    manifest_sources = tuple(manifest.source for manifest in manifests)
    catalog_sources = tuple(entry.source for entry in CONNECTOR_CATALOG)
    expected = set(source_ids())
    if set(manifest_sources) != expected or set(catalog_sources) != expected:
        raise ValueError("manifest, runtime catalog, and source index have drifted")
    if len(manifest_sources) != len(set(manifest_sources)):
        raise ValueError("native fleet contains duplicate source identities")

    for candidate in candidates:
        manifest = candidate.manifest
        if manifest.api_version != MANIFEST_API_VERSION:
            raise ValueError(f"{manifest.connector_id} is not on the stable v1 API")
        if manifest.metadata.semantic_version.major != 1:
            raise ValueError(f"{manifest.connector_id} is not on connector major v1")
        if manifest.spec.maturity is not Maturity.STABLE:
            raise ValueError(f"{manifest.connector_id} is not declared stable")
        if candidate.origin != f"first-party-native:{manifest.source}":
            raise ValueError(f"{manifest.connector_id} is not a native candidate")
        if not manifest.spec.implementation.startswith("services.ingest.connectors."):
            raise ValueError(
                f"{manifest.connector_id} factory is outside the connector package"
            )
        for ingress_kind in manifest.spec.ingress_kinds:
            channel = resolve_channel(manifest.source, ingress_kind)  # type: ignore[arg-type]
            if channel is None:
                raise ValueError(
                    f"{manifest.connector_id} has no channel for {ingress_kind}"
                )
            get_handler(channel)


__all__ = ["validate_native_fleet"]
