from __future__ import annotations

from typing import get_args
from uuid import uuid4

from services.ingest.connector_platform.catalog import (
    CONNECTOR_CATALOG,
    MigrationState,
    build_compatibility_candidates,
)
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.ingestion.raw_tier.envelope import SourceLiteral
from services.ingest.source_contract.capabilities import NORMALIZATION_V1


def test_catalog_is_complete_and_matches_raw_envelope_source_literal() -> None:
    sources = tuple(entry.source for entry in CONNECTOR_CATALOG)

    assert len(sources) == len(set(sources)) == 26
    assert set(sources) == set(get_args(SourceLiteral))
    assert {
        entry.source
        for entry in CONNECTOR_CATALOG
        if entry.migration_state is MigrationState.NATIVE
    } == {"slack", "notion", "whatsapp"}


def test_non_migrated_catalog_entries_are_conformed_but_route_legacy() -> None:
    candidates = build_compatibility_candidates()
    composition = build_pilot_composition()

    assert len(candidates) == 23
    assert all(candidate.conformance_fingerprint for candidate in candidates)
    github = composition.registry.for_source("github")
    assert github.origin == "compatibility:github"
    decision = composition.routing.resolve(
        RouteRequest(
            tenant_id=uuid4(),
            connector_id=github.connector_id,
            source="github",
            capability=NORMALIZATION_V1.ref.id,
        )
    )
    assert decision.mode is ExecutionMode.LEGACY
