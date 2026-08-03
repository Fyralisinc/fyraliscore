"""Explicit first-party Slack and Notion connector catalog."""

from __future__ import annotations

from typing import Any

from services.ingest.connectors.native import (
    DirectHistoricalPull,
    DirectIncrementalPoll,
    DirectNormalization,
    DirectReconciliation,
    NativeIdentity,
    NativeSlackWebhook,
    NativeSourceConnector,
)
from services.ingest.connector_runtime.composition import (
    ConnectorRuntimeComposition,
    build_runtime_composition,
)
from services.ingest.connector_runtime.policy import RoutingPolicy
from services.ingest.connector_runtime.registry import ConnectorCandidate
from services.ingest.ingestion import idempotency
from services.ingest.ingestion.fetchers.notion import fetch_page_notion
from services.ingest.ingestion.fetchers.slack import fetch_page_slack
from services.ingest.ingestion.handlers.notion import handle_notion_object
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.ingest.ingestion.planners.notion import plan_shards_notion
from services.ingest.ingestion.planners.slack import plan_shards_slack
from services.ingest.ingestion.reconcilers.notion import reconcile_notion
from services.ingest.ingestion.reconcilers.slack import reconcile_slack
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.manifest import ConnectorManifest
from services.ingest.source_contract.models import IdentityInput


SLACK_CONNECTOR_ID = "fyralis/slack"
NOTION_CONNECTOR_ID = "fyralis/notion"


def _manifest(
    *,
    connector_id: str,
    source: str,
    display_name: str,
    capabilities: tuple[tuple[str, int], ...],
    ingress_kinds: tuple[str, ...],
    secret_slots: tuple[str, ...],
    outbound_hosts: tuple[str, ...],
    scopes: tuple[str, ...] = (),
) -> ConnectorManifest:
    return ConnectorManifest.model_validate(
        {
            "apiVersion": "sources.fyralis.io/v1alpha1",
            "kind": "SourceConnector",
            "metadata": {
                "id": connector_id,
                "source": source,
                "displayName": display_name,
                "version": "1.0.0",
                "owner": "ingestion",
            },
            "spec": {
                "contract": ">=1.0,<2.0",
                "implementation": (
                    "services.ingest.connectors.native:"
                    f"build_{source}_candidate"
                ),
                "maturity": "preview",
                "capabilities": [
                    {"id": item_id, "version": version, "required": True}
                    for item_id, version in capabilities
                ],
                "ingressKinds": list(ingress_kinds),
                "permissions": {
                    "secretSlots": list(secret_slots),
                    "outboundHosts": list(outbound_hosts),
                    "requestedScopes": list(scopes),
                },
                "trust": {"maximumTier": "attested_agent"},
                "runtime": {"isolation": "in_process_trusted"},
            },
        }
    )


SLACK_MANIFEST = _manifest(
    connector_id=SLACK_CONNECTOR_ID,
    source="slack",
    display_name="Slack",
    capabilities=(
        (HISTORICAL_PULL_V1.ref.id, 1),
        (WEBHOOK_V1.ref.id, 1),
        (RECONCILIATION_V1.ref.id, 1),
        (IDENTITY_V1.ref.id, 1),
        (NORMALIZATION_V1.ref.id, 1),
    ),
    ingress_kinds=("webhook", "backfill"),
    secret_slots=("webhook_signing_secret",),
    outbound_hosts=("slack.com",),
)


NOTION_MANIFEST = _manifest(
    connector_id=NOTION_CONNECTOR_ID,
    source="notion",
    display_name="Notion",
    capabilities=(
        (HISTORICAL_PULL_V1.ref.id, 1),
        (INCREMENTAL_POLL_V1.ref.id, 1),
        (RECONCILIATION_V1.ref.id, 1),
        (IDENTITY_V1.ref.id, 1),
        (NORMALIZATION_V1.ref.id, 1),
    ),
    ingress_kinds=("backfill", "poll"),
    secret_slots=("oauth_access_token",),
    outbound_hosts=("api.notion.com",),
)


def _payload(input: IdentityInput) -> dict[str, Any]:
    if not isinstance(input.record.payload, dict):
        raise ValueError("pilot identity requires a JSON object")
    return input.record.payload


def _slack_identity(input: IdentityInput) -> str:
    payload = _payload(input)
    raw_event = payload.get("event")
    event: dict[str, Any] = raw_event if isinstance(raw_event, dict) else payload
    channel = event.get("channel") or event.get("channel_id")
    timestamp = event.get("ts") or event.get("event_ts")
    if not isinstance(channel, str) or not isinstance(timestamp, str):
        raise ValueError("Slack identity requires channel and timestamp")
    return idempotency.slack_message(channel, timestamp)


def _notion_identity(input: IdentityInput) -> str:
    payload = _payload(input)
    object_type = payload.get("object")
    object_id = payload.get("id")
    if not isinstance(object_type, str) or not isinstance(object_id, str):
        raise ValueError("Notion identity requires object and id")
    return idempotency.notion_object(object_type, object_id)


def build_slack_candidate() -> ConnectorCandidate:
    connector = NativeSourceConnector(
        SLACK_MANIFEST,
        {
            HISTORICAL_PULL_V1.ref: lambda _context: DirectHistoricalPull(
                plan_shards_slack, fetch_page_slack
            ),
            WEBHOOK_V1.ref: lambda context: NativeSlackWebhook(context),
            RECONCILIATION_V1.ref: lambda _context: DirectReconciliation(
                reconcile_slack
            ),
            IDENTITY_V1.ref: lambda _context: NativeIdentity(_slack_identity),
            NORMALIZATION_V1.ref: lambda _context: DirectNormalization(
                handle_slack_message
            ),
        },
    )
    return connector.candidate(
        (
            HISTORICAL_PULL_V1,
            WEBHOOK_V1,
            RECONCILIATION_V1,
            IDENTITY_V1,
            NORMALIZATION_V1,
        ),
    )


def build_notion_candidate() -> ConnectorCandidate:
    connector = NativeSourceConnector(
        NOTION_MANIFEST,
        {
            HISTORICAL_PULL_V1.ref: lambda _context: DirectHistoricalPull(
                plan_shards_notion, fetch_page_notion
            ),
            INCREMENTAL_POLL_V1.ref: lambda _context: DirectIncrementalPoll(
                fetch_page_notion
            ),
            RECONCILIATION_V1.ref: lambda _context: DirectReconciliation(
                reconcile_notion
            ),
            IDENTITY_V1.ref: lambda _context: NativeIdentity(_notion_identity),
            NORMALIZATION_V1.ref: lambda _context: DirectNormalization(
                handle_notion_object
            ),
        },
    )
    return connector.candidate(
        (
            HISTORICAL_PULL_V1,
            INCREMENTAL_POLL_V1,
            RECONCILIATION_V1,
            IDENTITY_V1,
            NORMALIZATION_V1,
        ),
    )


def build_pilot_candidates() -> tuple[ConnectorCandidate, ...]:
    return (build_slack_candidate(), build_notion_candidate())


def build_pilot_composition(
    policy: RoutingPolicy | None = None,
) -> ConnectorRuntimeComposition:
    """Freeze both native definitions with the supplied routing policy."""

    return build_runtime_composition(build_pilot_candidates(), policy=policy)


__all__ = [
    "NOTION_CONNECTOR_ID",
    "NOTION_MANIFEST",
    "SLACK_CONNECTOR_ID",
    "SLACK_MANIFEST",
    "build_notion_candidate",
    "build_pilot_candidates",
    "build_pilot_composition",
    "build_slack_candidate",
]
