from __future__ import annotations

from uuid import uuid4

from services.ingest.connector_platform.catalog import CONNECTOR_CATALOG
from services.ingest.connector_platform.pilots import (
    build_pilot_composition,
    build_runtime_candidates,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.source_contract.capabilities import NORMALIZATION_V1
from services.ingest.source_contract.source_catalog import source_ids


def test_catalog_is_complete_and_matches_generated_source_index() -> None:
    sources = tuple(entry.source for entry in CONNECTOR_CATALOG)

    assert len(sources) == len(set(sources)) == 26
    assert set(sources) == set(source_ids())
    assert all(entry.connector_version == "1.0.0" for entry in CONNECTOR_CATALOG)


def test_all_catalog_entries_are_native_and_bootstrap_native() -> None:
    candidates = build_runtime_candidates()
    composition = build_pilot_composition()

    assert len(candidates) == 26
    assert all(candidate.conformance_fingerprint for candidate in candidates)
    assert all(
        candidate.origin.startswith("first-party-native:") for candidate in candidates
    )
    github = composition.registry.for_source("github")
    assert github.origin == "first-party-native:github"
    decision = composition.routing.resolve(
        RouteRequest(
            tenant_id=uuid4(),
            connector_id=github.connector_id,
            source="github",
            capability=NORMALIZATION_V1.ref.id,
        )
    )
    assert decision.mode is ExecutionMode.CONNECTOR
