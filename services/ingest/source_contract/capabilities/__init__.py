"""Versioned capability protocols and their canonical keys."""

from __future__ import annotations

from types import MappingProxyType

from services.ingest.source_contract.capabilities.ingestion import (
    GatewayStreamCapability,
    HistoricalPullCapability,
    IncrementalPollCapability,
    PushSubscriptionCapability,
    ReconciliationCapability,
    ResourceDiscoveryCapability,
    WebhookCapability,
)
from services.ingest.source_contract.capabilities.installation import (
    ConfigurationCapability,
    OAuth2Capability,
    SecretRotationCapability,
)
from services.ingest.source_contract.capabilities.lifecycle import (
    CleanupCapability,
    HealthProbeCapability,
)
from services.ingest.source_contract.capabilities.semantic import (
    IdentityCapability,
    NormalizationCapability,
)
from services.ingest.source_contract.connector import CapabilityKey
from services.ingest.source_contract.manifest import CapabilityRef


CONFIGURATION_V1: CapabilityKey[ConfigurationCapability] = CapabilityKey(
    CapabilityRef(id="installation.configure", version=1),
    ConfigurationCapability,
)
OAUTH2_V1: CapabilityKey[OAuth2Capability] = CapabilityKey(
    CapabilityRef(id="installation.oauth2", version=1), OAuth2Capability
)
SECRET_ROTATION_V1: CapabilityKey[SecretRotationCapability] = CapabilityKey(
    CapabilityRef(id="installation.secret_rotation", version=1),
    SecretRotationCapability,
)
RESOURCE_DISCOVERY_V1: CapabilityKey[ResourceDiscoveryCapability] = CapabilityKey(
    CapabilityRef(id="resource.discovery", version=1),
    ResourceDiscoveryCapability,
)
HISTORICAL_PULL_V1: CapabilityKey[HistoricalPullCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.historical_pull", version=1),
    HistoricalPullCapability,
)
INCREMENTAL_POLL_V1: CapabilityKey[IncrementalPollCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.incremental_poll", version=1),
    IncrementalPollCapability,
)
WEBHOOK_V1: CapabilityKey[WebhookCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.webhook", version=1), WebhookCapability
)
PUSH_SUBSCRIPTION_V1: CapabilityKey[PushSubscriptionCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.push_subscription", version=1),
    PushSubscriptionCapability,
)
GATEWAY_STREAM_V1: CapabilityKey[GatewayStreamCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.gateway_stream", version=1),
    GatewayStreamCapability,
)
RECONCILIATION_V1: CapabilityKey[ReconciliationCapability] = CapabilityKey(
    CapabilityRef(id="ingestion.reconciliation", version=1),
    ReconciliationCapability,
)
IDENTITY_V1: CapabilityKey[IdentityCapability] = CapabilityKey(
    CapabilityRef(id="semantic.identity", version=1), IdentityCapability
)
NORMALIZATION_V1: CapabilityKey[NormalizationCapability] = CapabilityKey(
    CapabilityRef(id="semantic.normalization", version=1),
    NormalizationCapability,
)
HEALTH_PROBE_V1: CapabilityKey[HealthProbeCapability] = CapabilityKey(
    CapabilityRef(id="health.probe", version=1), HealthProbeCapability
)
CLEANUP_V1: CapabilityKey[CleanupCapability] = CapabilityKey(
    CapabilityRef(id="lifecycle.cleanup", version=1), CleanupCapability
)


CAPABILITY_CATALOG = MappingProxyType(
    {
        key.ref: key
        for key in (
            CONFIGURATION_V1,
            OAUTH2_V1,
            SECRET_ROTATION_V1,
            RESOURCE_DISCOVERY_V1,
            HISTORICAL_PULL_V1,
            INCREMENTAL_POLL_V1,
            WEBHOOK_V1,
            PUSH_SUBSCRIPTION_V1,
            GATEWAY_STREAM_V1,
            RECONCILIATION_V1,
            IDENTITY_V1,
            NORMALIZATION_V1,
            HEALTH_PROBE_V1,
            CLEANUP_V1,
        )
    }
)


__all__ = [
    "CAPABILITY_CATALOG",
    "CLEANUP_V1",
    "CONFIGURATION_V1",
    "GATEWAY_STREAM_V1",
    "HEALTH_PROBE_V1",
    "HISTORICAL_PULL_V1",
    "IDENTITY_V1",
    "INCREMENTAL_POLL_V1",
    "NORMALIZATION_V1",
    "OAUTH2_V1",
    "PUSH_SUBSCRIPTION_V1",
    "RECONCILIATION_V1",
    "RESOURCE_DISCOVERY_V1",
    "SECRET_ROTATION_V1",
    "WEBHOOK_V1",
]
