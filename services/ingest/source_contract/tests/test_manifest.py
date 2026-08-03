from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
    IsolationMode,
    load_connector_manifest,
    load_connector_manifests,
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
    data["spec"]["capabilities"].append({"id": "semantic.identity", "version": 1})
    with pytest.raises(ValidationError, match="capability declarations must be unique"):
        ConnectorManifest.model_validate(data)


def test_stable_manifest_declares_available_and_configured_capabilities(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["apiVersion"] = "sources.fyralis.io/v1"
    data["spec"]["permissions"]["secretSlots"] = ["access_token"]
    data["spec"]["capabilities"][0]["configuredBy"] = ["access_token"]
    manifest = ConnectorManifest.model_validate(data)

    assert manifest.available_capability_refs == manifest.capability_refs
    assert manifest.configured_capability_refs(frozenset()) == (
        manifest.capability_refs[1],
    )
    assert manifest.configured_capability_refs(frozenset({"access_token"})) == (
        manifest.capability_refs
    )


def test_configured_capability_cannot_reference_undeclared_slot(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["capabilities"][0]["configuredBy"] = ["missing"]
    with pytest.raises(ValidationError, match="undeclared secret slots"):
        ConnectorManifest.model_validate(data)


def test_manifest_rejects_non_dns_outbound_host(
    manifest_data: dict[str, Any],
) -> None:
    data = deepcopy(manifest_data)
    data["spec"]["permissions"]["outboundHosts"] = ["https://api.example.com/v1"]
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


def test_declarative_manifest_directory_load_is_deterministic(
    tmp_path, manifest_data: dict[str, Any]
) -> None:
    second = deepcopy(manifest_data)
    second["metadata"]["id"] = "fyralis/another"
    second["metadata"]["source"] = "another"
    (tmp_path / "z.json").write_text(json.dumps(manifest_data), encoding="utf-8")
    (tmp_path / "a.json").write_text(json.dumps(second), encoding="utf-8")

    manifests = load_connector_manifests(tmp_path)

    assert tuple(item.source for item in manifests) == ("another", "example")


def test_manifest_loader_rejects_non_object_json(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="must be an object"):
        load_connector_manifest(path)
