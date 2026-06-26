from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
    byoc_bootstrap_bundle_json_schema,
    load_byoc_bootstrap_bundle,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


ROOT = Path(__file__).resolve().parents[4]
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"


def _bundle_data() -> dict:
    return yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))


def test_checked_in_bootstrap_bundle_matches_byoc_contracts() -> None:
    bundle = load_byoc_bootstrap_bundle(BUNDLE)
    dataplane = load_byoc_manifest(DATAPLANE)
    permissions = load_byoc_permissions_manifest(PERMISSIONS)

    assert validate_bootstrap_bundle_contract(
        bundle,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        verify_local_files=True,
        repo_root=ROOT,
    ) == []
    assert {artifact.role for artifact in bundle.artifacts} >= {
        "gateway_image",
        "worker_image",
        "data_plane_agent_image",
        "helm_chart",
        "iam_bootstrap_template",
        "source_sbom",
        "image_sbom",
    }


def test_bootstrap_bundle_schema_is_exportable() -> None:
    schema = byoc_bootstrap_bundle_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.bootstrap_bundle.v1"
    )


def test_bootstrap_bundle_rejects_mutable_or_mismatched_refs() -> None:
    data = _bundle_data()
    data["artifacts"][0]["ref"] = "ghcr.io/fyralisinc/fyraliscore-gateway:latest"
    data["artifacts"][1]["ref"] = (
        "ghcr.io/fyralisinc/fyraliscore-worker@sha256:"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    bundle = ByocBootstrapBundleManifest.model_validate(data)

    violations = validate_bootstrap_bundle_contract(bundle)

    assert {
        violation.code for violation in violations
    } >= {"mutable_artifact_ref", "latest_tag_forbidden", "artifact_ref_digest_mismatch"}


def test_bootstrap_bundle_rejects_signature_identity_drift() -> None:
    data = _bundle_data()
    data["artifacts"][0]["signature"]["certificate_identity"] = (
        "https://github.com/example/other/.github/workflows/ci.yml@refs/heads/main"
    )
    bundle = ByocBootstrapBundleManifest.model_validate(data)

    violations = validate_bootstrap_bundle_contract(bundle)

    assert "signature_identity_mismatch" in {
        violation.code for violation in violations
    }


def test_bootstrap_bundle_rejects_manifest_identity_mismatch() -> None:
    data = _bundle_data()
    data["artifact_revision"] = "2026.06.26-2"
    bundle = ByocBootstrapBundleManifest.model_validate(data)
    dataplane = load_byoc_manifest(DATAPLANE)
    permissions = load_byoc_permissions_manifest(PERMISSIONS)

    violations = validate_bootstrap_bundle_contract(
        bundle,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
    )

    assert {
        violation.code for violation in violations
    } >= {"dataplane_manifest_mismatch", "permissions_manifest_mismatch"}


def test_bootstrap_bundle_rejects_bad_local_digest() -> None:
    data = _bundle_data()
    template = next(
        artifact
        for artifact in data["artifacts"]
        if artifact["role"] == "iam_bootstrap_template"
    )
    template["digest"] = (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    bundle = ByocBootstrapBundleManifest.model_validate(data)

    violations = validate_bootstrap_bundle_contract(
        bundle,
        verify_local_files=True,
        repo_root=ROOT,
    )

    assert "local_artifact_digest_mismatch" in {
        violation.code for violation in violations
    }


def test_bootstrap_bundle_schema_rejects_absolute_local_path() -> None:
    data = deepcopy(_bundle_data())
    template = next(
        artifact
        for artifact in data["artifacts"]
        if artifact["role"] == "iam_bootstrap_template"
    )
    template["local_path"] = "/tmp/iam.yaml"

    with pytest.raises(ValidationError):
        ByocBootstrapBundleManifest.model_validate(data)
