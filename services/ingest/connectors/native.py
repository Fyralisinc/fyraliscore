"""First-party source connector roots and factories.

Pilot capability implementations live beside their provider wire semantics and
depend only on the source contract.  Legacy adapters remain in
``connector_platform`` as explicit rollback implementations; no ambient legacy
binding is required to invoke any connector declared here.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.ingest.connectors.notion import (
    NotionIngestion,
    NotionNormalization,
    notion_external_id,
)
from services.ingest.connectors.oauth import (
    NotionOAuthCapability,
    SlackOAuthCapability,
)
from services.ingest.connectors.slack import (
    SlackNormalization,
    SlackPull,
    SlackWebhook,
    slack_external_id,
)
from services.ingest.connectors.whatsapp import (
    WhatsAppNormalization,
    WhatsAppWebhook,
    whatsapp_external_id,
)
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    OAUTH2_LIFECYCLE_V1,
    OAUTH2_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.installation import (
    OAuthRevokeRequest,
)
from services.ingest.source_contract.capabilities.lifecycle import (
    CleanupRequest,
    CleanupResult,
    HealthProbeRequest,
)
from services.ingest.source_contract.connector import (
    BindingContext,
    OperationContext,
    StaticBoundConnector,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
    load_connector_manifest,
)
from services.ingest.source_contract.models import (
    HealthCondition,
    HealthReport,
    IdentityInput,
)


CapabilityFactory = Callable[[BindingContext], object]
_MANIFEST_DIRECTORY = Path(__file__).resolve().parent / "manifests"


class NativeSourceConnector:
    def __init__(
        self,
        manifest: ConnectorManifest,
        factories: Mapping[CapabilityRef, CapabilityFactory],
    ) -> None:
        self._manifest = manifest
        self._factories = dict(factories)

    @property
    def manifest(self) -> ConnectorManifest:
        return self._manifest

    def bind(self, context: BindingContext) -> StaticBoundConnector:
        return StaticBoundConnector(
            context.installation,
            {ref: factory(context) for ref, factory in self._factories.items()},
        )


class NativeIdentity:
    def __init__(self, derive: Callable[[IdentityInput], str]) -> None:
        self._derive = derive

    def external_id(self, input: IdentityInput) -> str:
        return self._derive(input)


class CredentialHealthProbe:
    def __init__(self, binding: BindingContext, slots: tuple[str, ...]) -> None:
        self._binding = binding
        self._slots = slots

    async def probe(
        self, request: HealthProbeRequest, context: OperationContext
    ) -> HealthReport:
        conditions: list[HealthCondition] = []
        healthy = True
        for slot in self._slots:
            try:
                value = await self._binding.services.secrets.resolve(SlotId(slot))
                present = bool(value.reveal_bytes())
            except Exception:
                present = False
            healthy = healthy and present
            conditions.append(
                HealthCondition(
                    type="CredentialsValid",
                    status="true" if present else "false",
                    reason="CredentialPresent" if present else "CredentialUnavailable",
                    observed_at=datetime.now(timezone.utc),
                )
            )
        return HealthReport(healthy=healthy, conditions=tuple(conditions))


class OAuthCleanup:
    def __init__(self, oauth_lifecycle: Any) -> None:
        self._oauth_lifecycle = oauth_lifecycle

    async def cleanup(
        self, request: CleanupRequest, context: OperationContext
    ) -> CleanupResult:
        result = await self._oauth_lifecycle.revoke(
            OAuthRevokeRequest(
                operation_id=request.operation_id,
                revoke_remote=request.revoke_remote,
            ),
            context,
        )
        return CleanupResult(
            complete=result.complete or request.force,
            remote_revoked=result.remote_revoked,
            reason_code=result.reason_code,
        )


class LocalCredentialCleanup:
    async def cleanup(
        self, request: CleanupRequest, context: OperationContext
    ) -> CleanupResult:
        return CleanupResult(
            complete=True,
            remote_revoked=False,
            reason_code="host_retires_local_credentials",
        )


def _manifest(source: str) -> ConnectorManifest:
    return load_connector_manifest(_MANIFEST_DIRECTORY / f"{source}.json")


def build_slack_connector() -> NativeSourceConnector:
    """Build the native Slack connector referenced by its manifest."""

    return NativeSourceConnector(
        _manifest("slack"),
        {
            OAUTH2_V1.ref: SlackOAuthCapability,
            OAUTH2_LIFECYCLE_V1.ref: SlackOAuthCapability,
            HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(
                context, ("oauth_access_token", "webhook_signing_secret")
            ),
            CLEANUP_V1.ref: lambda context: OAuthCleanup(SlackOAuthCapability(context)),
            HISTORICAL_PULL_V1.ref: SlackPull,
            WEBHOOK_V1.ref: SlackWebhook,
            RECONCILIATION_V1.ref: SlackPull,
            IDENTITY_V1.ref: lambda _context: NativeIdentity(slack_external_id),
            NORMALIZATION_V1.ref: lambda _context: SlackNormalization(),
        },
    )


def build_notion_connector() -> NativeSourceConnector:
    """Build the native Notion connector referenced by its manifest."""

    return NativeSourceConnector(
        _manifest("notion"),
        {
            OAUTH2_V1.ref: NotionOAuthCapability,
            OAUTH2_LIFECYCLE_V1.ref: NotionOAuthCapability,
            HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(
                context, ("oauth_access_token",)
            ),
            CLEANUP_V1.ref: lambda context: OAuthCleanup(
                NotionOAuthCapability(context)
            ),
            HISTORICAL_PULL_V1.ref: NotionIngestion,
            INCREMENTAL_POLL_V1.ref: NotionIngestion,
            RECONCILIATION_V1.ref: NotionIngestion,
            IDENTITY_V1.ref: lambda _context: NativeIdentity(notion_external_id),
            NORMALIZATION_V1.ref: lambda _context: NotionNormalization(),
        },
    )


def build_whatsapp_connector() -> NativeSourceConnector:
    """Build the native WhatsApp connector referenced by its manifest."""

    return NativeSourceConnector(
        _manifest("whatsapp"),
        {
            HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(
                context, ("app_secret",)
            ),
            CLEANUP_V1.ref: lambda _context: LocalCredentialCleanup(),
            WEBHOOK_V1.ref: WhatsAppWebhook,
            IDENTITY_V1.ref: lambda _context: NativeIdentity(whatsapp_external_id),
            NORMALIZATION_V1.ref: lambda _context: WhatsAppNormalization(),
        },
    )


__all__ = [
    "CredentialHealthProbe",
    "LocalCredentialCleanup",
    "NativeIdentity",
    "NativeSourceConnector",
    "OAuthCleanup",
    "build_notion_connector",
    "build_slack_connector",
    "build_whatsapp_connector",
]
