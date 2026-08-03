from __future__ import annotations

import json

import pytest

from services.ingest.connector_runtime.discovery import (
    candidate_from_manifest,
    resolve_connector_factory,
)
from services.ingest.connector_runtime.tests.helpers import make_manifest
from services.ingest.source_contract.manifest import load_connector_manifest


def test_manifest_load_does_not_resolve_implementation(tmp_path) -> None:
    manifest = make_manifest()
    path = tmp_path / "example.json"
    path.write_text(
        json.dumps(manifest.model_dump(mode="json", by_alias=True)),
        encoding="utf-8",
    )

    loaded = load_connector_manifest(path)

    assert loaded == manifest


def test_candidate_discovery_rejects_missing_factory() -> None:
    manifest = make_manifest().model_copy(
        update={
            "spec": make_manifest().spec.model_copy(
                update={
                    "implementation": (
                        "services.ingest.connector_runtime.tests.helpers:missing"
                    )
                }
            )
        }
    )

    with pytest.raises(ValueError, match="is not callable"):
        resolve_connector_factory(manifest)


def test_candidate_is_derived_without_importing_implementation(
    monkeypatch,
) -> None:
    manifest = make_manifest().model_copy(
        update={
            "spec": make_manifest().spec.model_copy(
                update={
                    "implementation": (
                        "services.ingest.connector_runtime.tests.helpers:"
                        "build_example_connector"
                    )
                }
            )
        }
    )

    def unexpected_import(_module):
        raise AssertionError("candidate construction imported implementation code")

    monkeypatch.setattr(
        "services.ingest.connector_runtime.discovery.importlib.import_module",
        unexpected_import,
    )
    candidate = candidate_from_manifest(manifest, origin="test")

    assert tuple(key.ref for key in candidate.capability_keys) == (
        manifest.capability_refs[0],
    )
