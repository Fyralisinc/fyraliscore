from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
    IsolationMode,
)
from services.ingest.source_contract.versioning import SemanticVersion


def test_manifest_is_inspectable_and_alias_round_trips(
    manifest: ConnectorManifest,
) -> None:
    assert manifest.connector_id == "fyralis/example"
    assert manifest.source == "example"
    assert manifest.metadata.semantic_version == SemanticVersion.parse("1.2.3")
    assert manifest.spec.runtime.isolation is IsolationMode.IN_PROCESS_TRUSTED
    assert manifest.capability_refs == (
        CapabilityRef(id="semantic.identity", version=1),
        CapabilityRef(id="semantic.normalization", version=1),
    )
    dumped = manifest.model_dump(by_alias=True)
    assert dumped["apiVersion"] == "sources.fyralis.io/v1alpha1"
    assert dumped["metadata"]["displayName"] == "Example"


def test_manifest_rejects_unknown_fields(manifest_data: dict[str, Any]) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["magic"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ConnectorManifest.model_validate(data)


def test_manifest_rejects_duplicate_capabilities(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["capabilities"].append(
        {"id": "semantic.identity", "version": 1}
    )
    with pytest.raises(ValidationError, match="capability declarations must be unique"):
        ConnectorManifest.model_validate(data)


def test_manifest_rejects_non_dns_outbound_host(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["permissions"]["outboundHosts"] = [
        "https://api.example.com/v1"
    ]
    with pytest.raises(ValidationError, match="bare DNS name"):
        ConnectorManifest.model_validate(data)


def test_manifest_rejects_invalid_contract_range(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["contract"] = "^1.0"
    with pytest.raises(ValidationError, match="invalid version comparator"):
        ConnectorManifest.model_validate(data)


def test_manifest_is_frozen(manifest: ConnectorManifest) -> None:
    with pytest.raises(ValidationError, match="Instance is frozen"):
        manifest.kind = "Other"  # type: ignore[assignment]


def test_source_aliases_are_unique_and_distinct(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["metadata"]["aliases"] = ["example_legacy"]
    manifest = ConnectorManifest.model_validate(data)
    assert manifest.metadata.aliases == ("example_legacy",)

    data["metadata"]["aliases"] = ["example", "example"]
    with pytest.raises(ValidationError, match="source aliases must be unique"):
        ConnectorManifest.model_validate(data)

    data["metadata"]["aliases"] = ["example"]
    with pytest.raises(ValidationError, match="canonical source"):
        ConnectorManifest.model_validate(data)
