"""Manifest-first connector factory discovery.

Manifest inspection stays side-effect free. Implementation modules are imported
only after callers deliberately request a candidate/factory for activation.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import cast

from services.ingest.connector_runtime.definitions import ConnectorCandidate
from services.ingest.source_contract.capabilities import CAPABILITY_CATALOG
from services.ingest.source_contract.connector import SourceConnector
from services.ingest.source_contract.manifest import ConnectorManifest

ConnectorFactory = Callable[[], SourceConnector]


def resolve_connector_factory(manifest: ConnectorManifest) -> ConnectorFactory:
    module_name, attribute = manifest.spec.implementation.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ValueError(
            f"connector {manifest.connector_id} implementation module "
            f"{module_name!r} could not be imported"
        ) from exc
    factory = getattr(module, attribute, None)
    if factory is None or not callable(factory):
        raise ValueError(
            f"connector {manifest.connector_id} implementation target "
            f"{manifest.spec.implementation!r} is not callable"
        )
    return cast(ConnectorFactory, factory)


def candidate_from_manifest(
    manifest: ConnectorManifest,
    *,
    origin: str,
    conformance_fingerprint: str | None = None,
) -> ConnectorCandidate:
    """Create a candidate without importing implementation code yet."""

    keys = tuple(
        CAPABILITY_CATALOG[ref]
        for ref in manifest.available_capability_refs
        if ref in CAPABILITY_CATALOG
    )

    def activate() -> SourceConnector:
        return resolve_connector_factory(manifest)()

    return ConnectorCandidate(
        manifest=manifest,
        factory=activate,
        capability_keys=keys,
        origin=origin,
        conformance_fingerprint=conformance_fingerprint,
    )


__all__ = ["candidate_from_manifest", "resolve_connector_factory"]
