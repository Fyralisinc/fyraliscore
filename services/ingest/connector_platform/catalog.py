"""Canonical inventory for all Fyralis ingestion source families."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from functools import lru_cache

from services.ingest.connector_conformance import (
    ConnectorConformanceSuite,
    assert_connector_conforms,
)
from services.ingest.connector_platform.legacy_capabilities import (
    LegacyHistoricalPull,
    LegacyIncrementalPoll,
    LegacyNormalization,
    LegacyReconciliation,
)
from services.ingest.connector_runtime.legacy import LegacyConnectorAdapter
from services.ingest.connector_runtime.registry import ConnectorCandidate
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
)
from services.ingest.source_contract.manifest import ConnectorManifest


class MigrationState(StrEnum):
    NATIVE = "native"
    COMPATIBILITY = "compatibility"


@dataclass(frozen=True)
class ConnectorCatalogEntry:
    source: str
    ingress_kinds: tuple[str, ...]
    migration_state: MigrationState = MigrationState.COMPATIBILITY

    @property
    def connector_id(self) -> str:
        return f"fyralis/{self.source}"


CONNECTOR_CATALOG = (
    ConnectorCatalogEntry("slack", ("backfill", "webhook"), MigrationState.NATIVE),
    ConnectorCatalogEntry("github", ("backfill", "webhook")),
    ConnectorCatalogEntry("discord", ("backfill", "gateway", "webhook")),
    ConnectorCatalogEntry("gmail", ("backfill", "poll")),
    ConnectorCatalogEntry("notion", ("backfill", "poll"), MigrationState.NATIVE),
    ConnectorCatalogEntry("google_calendar", ("backfill", "poll")),
    ConnectorCatalogEntry("google_drive", ("backfill", "poll")),
    ConnectorCatalogEntry("jira", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("mercury", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("quickbooks", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("grafana", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("telegram", ("backfill", "gateway")),
    ConnectorCatalogEntry("brex", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("ramp", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("gusto", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("deel", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("fireflies", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("signal", ("backfill", "gateway")),
    ConnectorCatalogEntry("aws", ("backfill", "poll")),
    ConnectorCatalogEntry("miro", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("figma", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("carta", ("backfill", "poll")),
    ConnectorCatalogEntry("hibob", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("ashby", ("backfill", "poll", "webhook")),
    ConnectorCatalogEntry("linkedin", ("backfill", "poll")),
    ConnectorCatalogEntry("whatsapp", ("webhook",), MigrationState.NATIVE),
)


def catalog_by_source() -> dict[str, ConnectorCatalogEntry]:
    return {entry.source: entry for entry in CONNECTOR_CATALOG}


def _compatibility_manifest(entry: ConnectorCatalogEntry) -> ConnectorManifest:
    refs = [NORMALIZATION_V1.ref]
    if "backfill" in entry.ingress_kinds:
        refs.extend((HISTORICAL_PULL_V1.ref, RECONCILIATION_V1.ref))
    if "poll" in entry.ingress_kinds:
        refs.append(INCREMENTAL_POLL_V1.ref)
    return ConnectorManifest.model_validate(
        {
            "apiVersion": "sources.fyralis.io/v1alpha1",
            "kind": "SourceConnector",
            "metadata": {
                "id": entry.connector_id,
                "source": entry.source,
                "displayName": entry.source.replace("_", " ").title(),
                "version": "0.1.0",
                "owner": "ingestion",
            },
            "spec": {
                "contract": ">=1.0,<2.0",
                "implementation": (
                    "services.ingest.connector_platform.catalog:"
                    "build_compatibility_candidates"
                ),
                "maturity": "deprecated",
                "capabilities": [
                    {"id": ref.id, "version": ref.version, "required": True}
                    for ref in refs
                ],
                "ingressKinds": list(entry.ingress_kinds),
                "permissions": {},
                "trust": {"maximumTier": "attested_agent"},
                "runtime": {"isolation": "in_process_trusted"},
            },
        }
    )


def _compatibility_candidate(entry: ConnectorCatalogEntry) -> ConnectorCandidate:
    manifest = _compatibility_manifest(entry)
    providers = {
        NORMALIZATION_V1.ref: lambda _context: LegacyNormalization(entry.source),
    }
    keys = [NORMALIZATION_V1]
    if "backfill" in entry.ingress_kinds:
        providers[HISTORICAL_PULL_V1.ref] = (
            lambda _context: LegacyHistoricalPull(entry.source)
        )
        providers[RECONCILIATION_V1.ref] = (
            lambda _context: LegacyReconciliation(entry.source)
        )
        keys.extend((HISTORICAL_PULL_V1, RECONCILIATION_V1))
    if "poll" in entry.ingress_kinds:
        providers[INCREMENTAL_POLL_V1.ref] = (
            lambda _context: LegacyIncrementalPoll(entry.source)
        )
        keys.append(INCREMENTAL_POLL_V1)
    adapter = LegacyConnectorAdapter(manifest, providers)
    raw = adapter.candidate(tuple(keys), origin=f"compatibility:{entry.source}")
    report = ConnectorConformanceSuite().run(raw)
    assert_connector_conforms(report)
    return replace(raw, conformance_fingerprint=report.fingerprint)


@lru_cache(maxsize=1)
def build_compatibility_candidates() -> tuple[ConnectorCandidate, ...]:
    """Conform and cache the immutable compatibility portion of the catalog."""

    return tuple(
        _compatibility_candidate(entry)
        for entry in CONNECTOR_CATALOG
        if entry.migration_state is MigrationState.COMPATIBILITY
    )


__all__ = [
    "CONNECTOR_CATALOG",
    "ConnectorCatalogEntry",
    "MigrationState",
    "build_compatibility_candidates",
    "catalog_by_source",
]
