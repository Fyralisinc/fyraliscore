from __future__ import annotations

from typing import Any

import pytest

from services.ingest.source_contract.manifest import ConnectorManifest


@pytest.fixture
def manifest_data() -> dict[str, Any]:
    return {
        "apiVersion": "sources.fyralis.io/v1alpha1",
        "kind": "SourceConnector",
        "metadata": {
            "id": "fyralis/example",
            "source": "example",
            "displayName": "Example",
            "version": "1.2.3",
            "owner": "ingestion",
        },
        "spec": {
            "contract": ">=1.0,<2.0",
            "implementation": "tests.example:create_connector",
            "maturity": "preview",
            "capabilities": [
                {
                    "id": "semantic.identity",
                    "version": 1,
                    "required": True,
                },
                {
                    "id": "semantic.normalization",
                    "version": 1,
                    "required": True,
                },
            ],
            "ingressKinds": ["webhook", "backfill"],
            "permissions": {
                "secretSlots": ["api_token"],
                "outboundHosts": ["api.example.com"],
                "requestedScopes": ["events:read"],
            },
            "trust": {"maximumTier": "authoritative"},
            "runtime": {
                "isolation": "in_process_trusted",
                "networkProfile": "example_api",
                "resourceClass": "io_standard",
            },
        },
    }


@pytest.fixture
def manifest(manifest_data: dict[str, Any]) -> ConnectorManifest:
    return ConnectorManifest.model_validate(manifest_data)
