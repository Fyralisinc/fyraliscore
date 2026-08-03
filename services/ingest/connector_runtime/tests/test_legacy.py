from __future__ import annotations

from services.ingest.connector_conformance.fakes import make_binding_context
from services.ingest.connector_runtime.legacy import LegacyConnectorAdapter
from services.ingest.connector_runtime.registry import ConnectorRegistryBuilder
from services.ingest.connector_runtime.tests.helpers import (
    ExampleIdentity,
    make_manifest,
)
from services.ingest.source_contract.capabilities import IDENTITY_V1
from services.ingest.source_contract.connector import BindingContext


def test_legacy_adapter_is_explicit_and_installation_scoped() -> None:
    manifest = make_manifest()
    contexts: list[BindingContext] = []

    def provider(context: BindingContext) -> ExampleIdentity:
        contexts.append(context)
        return ExampleIdentity()

    adapter = LegacyConnectorAdapter(
        manifest,
        {IDENTITY_V1.ref: provider},
    )
    candidate = adapter.candidate((IDENTITY_V1,), origin="legacy:test")

    registry = ConnectorRegistryBuilder().add(candidate).build()

    assert contexts == []
    assert registry.describe(manifest.connector_id).origin == "legacy:test"
    context = make_binding_context(manifest)
    binding = registry.resolve_for_install(context)
    assert binding.capability(IDENTITY_V1) is not None
    assert contexts == [context]
