from __future__ import annotations

from collections.abc import Callable, Mapping

from services.ingest.connector_runtime.registry import ConnectorCandidate
from services.ingest.source_contract.capabilities import IDENTITY_V1
from services.ingest.source_contract.connector import (
    BindingContext,
    BoundConnector,
    CapabilityKey,
    StaticBoundConnector,
)
from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
)
from services.ingest.source_contract.models import IdentityInput


class ExampleIdentity:
    def external_id(self, input: IdentityInput) -> str:
        return f"example:{input.external_installation_id}"


class ExampleConnector:
    def __init__(
        self,
        manifest: ConnectorManifest,
        *,
        capabilities: Mapping[CapabilityRef, object] | None = None,
        binding_factory: Callable[[BindingContext], BoundConnector] | None = None,
    ) -> None:
        self._manifest = manifest
        self._capabilities = dict(capabilities or {IDENTITY_V1.ref: ExampleIdentity()})
        self._binding_factory = binding_factory
        self.bind_calls = 0

    @property
    def manifest(self) -> ConnectorManifest:
        return self._manifest

    def bind(self, context: BindingContext) -> BoundConnector:
        self.bind_calls += 1
        if self._binding_factory is not None:
            return self._binding_factory(context)
        return StaticBoundConnector(context.installation, self._capabilities)


def build_example_connector() -> ExampleConnector:
    return ExampleConnector(make_manifest())


def make_manifest(
    *,
    connector_id: str = "fyralis/example",
    source: str = "example",
    aliases: tuple[str, ...] = (),
    contract: str = ">=1.0,<2.0",
    capabilities: tuple[tuple[str, int, bool], ...] = (("semantic.identity", 1, True),),
) -> ConnectorManifest:
    return ConnectorManifest.model_validate(
        {
            "apiVersion": "sources.fyralis.io/v1alpha1",
            "kind": "SourceConnector",
            "metadata": {
                "id": connector_id,
                "source": source,
                "aliases": list(aliases),
                "displayName": source.title(),
                "version": "1.0.0",
                "owner": "ingestion",
            },
            "spec": {
                "contract": contract,
                "implementation": "tests.example:create_connector",
                "maturity": "preview",
                "capabilities": [
                    {"id": item_id, "version": version, "required": required}
                    for item_id, version, required in capabilities
                ],
                "ingressKinds": [],
                "permissions": {
                    "secretSlots": ["api_token"],
                    "outboundHosts": ["api.example.com"],
                    "requestedScopes": ["events:read"],
                },
                "trust": {"maximumTier": "authoritative"},
                "runtime": {"isolation": "in_process_trusted"},
            },
        }
    )


def make_candidate(
    manifest: ConnectorManifest | None = None,
    *,
    connector: ExampleConnector | None = None,
    capability_keys: tuple[CapabilityKey[object], ...] = (IDENTITY_V1,),
    origin: str = "test-catalog",
) -> tuple[ConnectorCandidate, ExampleConnector]:
    selected_manifest = manifest or make_manifest()
    selected_connector = connector or ExampleConnector(selected_manifest)
    return (
        ConnectorCandidate(
            manifest=selected_manifest,
            factory=lambda: selected_connector,
            capability_keys=capability_keys,
            origin=origin,
        ),
        selected_connector,
    )


__all__ = [
    "ExampleConnector",
    "ExampleIdentity",
    "build_example_connector",
    "make_candidate",
    "make_manifest",
]
