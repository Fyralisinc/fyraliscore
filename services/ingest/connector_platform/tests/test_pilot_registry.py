from __future__ import annotations

from uuid import uuid4

from services.ingest.connector_conformance import ConnectorConformanceSuite
from services.ingest.connector_platform.catalog import CONNECTOR_CATALOG
from services.ingest.connector_platform.pilots import (
    NOTION_CONFORMANCE_FINGERPRINT,
    NOTION_CONNECTOR_ID,
    SLACK_CONFORMANCE_FINGERPRINT,
    SLACK_CONNECTOR_ID,
    WHATSAPP_CONFORMANCE_FINGERPRINT,
    WHATSAPP_CONNECTOR_ID,
    build_pilot_candidates,
    build_pilot_composition,
    release_evidence_catalog,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    INCREMENTAL_POLL_V1,
    WEBHOOK_V1,
)


def test_fleet_composition_is_immutable_and_bootstraps_native() -> None:
    composition = build_pilot_composition()

    assert len(composition.registry.connector_ids()) == 26
    assert set(composition.registry.connector_ids()) == {
        entry.connector_id for entry in CONNECTOR_CATALOG
    }
    assert "slack" in {
        item.source for item in composition.registry.list_by_capability(WEBHOOK_V1.ref)
    }
    assert "notion" in {
        item.source
        for item in composition.registry.list_by_capability(INCREMENTAL_POLL_V1.ref)
    }
    native_backfill_sources = {
        item.source
        for item in composition.registry.list_by_capability(HISTORICAL_PULL_V1.ref)
        if item.origin.startswith("first-party-native")
    }
    assert native_backfill_sources == {
        entry.source for entry in CONNECTOR_CATALOG if "backfill" in entry.ingress_kinds
    }
    assert (
        composition.routing.resolve(
            RouteRequest(
                tenant_id=uuid4(),
                connector_id=SLACK_CONNECTOR_ID,
                source="slack",
                capability=HISTORICAL_PULL_V1.ref.id,
            )
        ).mode
        is ExecutionMode.CONNECTOR
    )


def test_pilot_registration_evidence_matches_independent_conformance() -> None:
    reports = {
        candidate.manifest.connector_id: ConnectorConformanceSuite().run(candidate)
        for candidate in build_pilot_candidates()
    }

    assert reports[SLACK_CONNECTOR_ID].passed
    catalog = release_evidence_catalog()
    assert (
        reports[SLACK_CONNECTOR_ID].fingerprint
        == catalog.require(SLACK_CONNECTOR_ID, "1.0.0").structural_fingerprint
    )
    assert (
        SLACK_CONFORMANCE_FINGERPRINT
        == catalog.require(SLACK_CONNECTOR_ID, "1.0.0").admission_fingerprint
    )
    assert reports[NOTION_CONNECTOR_ID].passed
    assert (
        reports[NOTION_CONNECTOR_ID].fingerprint
        == catalog.require(NOTION_CONNECTOR_ID, "1.0.0").structural_fingerprint
    )
    assert (
        NOTION_CONFORMANCE_FINGERPRINT
        == catalog.require(NOTION_CONNECTOR_ID, "1.0.0").admission_fingerprint
    )
    assert reports[WHATSAPP_CONNECTOR_ID].passed
    assert (
        reports[WHATSAPP_CONNECTOR_ID].fingerprint
        == catalog.require(WHATSAPP_CONNECTOR_ID, "1.0.0").structural_fingerprint
    )
    assert (
        WHATSAPP_CONFORMANCE_FINGERPRINT
        == catalog.require(WHATSAPP_CONNECTOR_ID, "1.0.0").admission_fingerprint
    )
