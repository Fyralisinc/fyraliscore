"""Native connector root and direct-call capability facets.

The pull/reconciliation facets call named source functions directly. They do
not consult legacy dispatch registries; the invocation context is retained only
while the existing worker DTOs are being replaced at their outer boundaries.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from datetime import datetime, timezone

from services.ingest.connector_platform.legacy_context import require_legacy_binding
from services.ingest.connector_runtime.definitions import ConnectorCandidate
from services.ingest.ingestion.handlers import ObservationDraft as LegacyDraft
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.connector import (
    BindingContext,
    CapabilityKey,
    OperationContext,
    StaticBoundConnector,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.capabilities.installation import (
    OAuthRevokeRequest,
)
from services.ingest.source_contract.capabilities.lifecycle import (
    CleanupRequest,
    CleanupResult,
    HealthProbeRequest,
)
from services.ingest.source_contract.manifest import CapabilityRef, ConnectorManifest
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchRequest,
    FetchedPage,
    HealthCondition,
    HealthReport,
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


CapabilityFactory = Callable[[BindingContext], object]
Planner = Callable[[Any], Awaitable[list[Any]]]
Fetcher = Callable[[Any, dict[str, Any], dict[str, Any] | None], Awaitable[Any]]
Reconciler = Callable[[list[Any], Any], Awaitable[Any]]
Handler = Callable[[dict[str, Any], dict[str, str]], Awaitable[LegacyDraft]]


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

    def candidate(
        self,
        keys: tuple[CapabilityKey[Any], ...],
        *,
        conformance_fingerprint: str | None = None,
    ) -> ConnectorCandidate:
        return ConnectorCandidate(
            manifest=self.manifest,
            factory=lambda: self,
            capability_keys=keys,
            origin=f"first-party-native:{self.manifest.metadata.source}",
            conformance_fingerprint=conformance_fingerprint,
        )


def _source_record(payload: Any) -> SourceRecord:
    return SourceRecord(
        native_type=str(payload.get("object") or payload.get("type") or "record")
        if isinstance(payload, dict)
        else "record",
        payload=payload,
    )


class DirectHistoricalPull:
    def __init__(self, planner: Planner, fetcher: Fetcher) -> None:
        self._planner = planner
        self._fetcher = fetcher

    async def plan(
        self, request: PlanRequest, context: OperationContext
    ) -> PlanResult:
        binding = require_legacy_binding()
        shards = await self._planner(binding.planner_context)
        return PlanResult(
            shards=tuple(
                ShardPlan(
                    kind=shard.shard_kind,
                    identifier=dict(shard.shard_identifier),
                    priority=shard.recency_score,
                    window_start=shard.window_start,
                    window_end=shard.window_end,
                )
                for shard in shards
            )
        )

    async def fetch(
        self, request: FetchRequest, context: OperationContext
    ) -> FetchedPage:
        binding = require_legacy_binding()
        result = await self._fetcher(
            binding.install,
            dict(request.shard.identifier),
            request.cursor.payload if request.cursor is not None else None,
        )
        return FetchedPage(
            records=tuple(_source_record(item) for item in result.records),
            next_cursor=(
                CursorState(schema_version=1, payload=result.next_cursor)
                if result.next_cursor is not None and not result.end_of_data
                else None
            ),
            end_of_data=result.end_of_data,
        )


class DirectIncrementalPoll:
    def __init__(self, fetcher: Fetcher) -> None:
        self._fetcher = fetcher

    async def poll(
        self, request: PollRequest, context: OperationContext
    ) -> FetchedPage:
        binding = require_legacy_binding()
        result = await self._fetcher(
            binding.install,
            {
                "shard_kind": "notion_page_tree",
                "workspace_id": binding.external_installation_id,
            },
            request.cursor.payload if request.cursor is not None else None,
        )
        return FetchedPage(
            records=tuple(_source_record(item) for item in result.records),
            next_cursor=(
                CursorState(schema_version=1, payload=result.next_cursor)
                if result.next_cursor is not None and not result.end_of_data
                else None
            ),
            end_of_data=result.end_of_data,
        )


class DirectReconciliation:
    def __init__(self, reconciler: Reconciler) -> None:
        self._reconciler = reconciler

    async def reconcile(
        self,
        request: ReconciliationRequest,
        context: OperationContext,
    ) -> ReconciliationDecision:
        binding = require_legacy_binding()
        decision = await self._reconciler(
            list(binding.reconciliation_shards or ()),
            binding.reconciliation_run,
        )
        repairs = tuple(
            RepairShard(
                shard=ShardPlan(
                    kind=item.shard_kind,
                    identifier=dict(item.shard_identifier),
                    priority=item.recency_score,
                    window_start=item.window_start,
                    window_end=item.window_end,
                ),
                parent_shard_id=item.parent_shard_id,
            )
            for item in decision.new_shards
        )
        return ReconciliationDecision(
            has_gaps=decision.has_gaps,
            reason_code="legacy_gap" if decision.has_gaps else "clean",
            new_shards=repairs,
        )


class NativeIdentity:
    def __init__(self, derive: Callable[[IdentityInput], str]) -> None:
        self._derive = derive

    def external_id(self, input: IdentityInput) -> str:
        return self._derive(input)


def _contract_draft(draft: LegacyDraft) -> ObservationDraft:
    return ObservationDraft(
        source_channel=draft.source_channel,
        content_text=draft.content_text,
        content=dict(draft.content),
        occurred_at=draft.occurred_at,
        trust_tier=draft.trust_tier,
        kind=draft.kind,
        source_actor_ref=draft.source_actor_ref,
        external_id=draft.external_id,
        entities_hint=tuple(draft.entities_hint),
        raw_payload=draft.raw_payload,
    )


class DirectNormalization:
    def __init__(self, handler: Handler) -> None:
        self._handler = handler

    async def normalize(
        self,
        request: NormalizationInput,
        context: OperationContext,
    ) -> tuple[ObservationDraft, ...]:
        if not isinstance(request.record.payload, dict):
            raise ValueError("native normalization requires a JSON object")
        draft = await self._handler(
            dict(request.record.payload),
            {str(k): str(v) for k, v in request.ingress_metadata.items()},
        )
        return (_contract_draft(draft),)


class NativeSlackWebhook:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def verify_and_decode(
        self,
        request: BoundedWebhookRequest,
        context: OperationContext,
    ) -> VerifiedWebhookResult:
        from services.ingest.ingestion.handlers.slack import verify_slack_signature

        secret = await self._binding.services.secrets.resolve(
            SlotId("webhook_signing_secret")
        )
        headers = {key.lower(): value for key, value in request.headers.items()}
        timestamp = headers.get("x-slack-request-timestamp", "")
        signature = headers.get("x-slack-signature", "")
        verify_slack_signature(
            request.body,
            timestamp,
            signature,
            secret.reveal_text(),
            now=request.received_at.timestamp(),
        )
        payload = json.loads(request.body)
        if payload.get("type") == "url_verification":
            return VerifiedWebhookResult(events=(), response_status_hint=200)
        external_id = str(
            payload.get("team_id")
            or payload.get("team", {}).get("id")
            or "unknown"
        )
        event = (
            payload.get("event")
            if isinstance(payload.get("event"), dict)
            else payload
        )
        return VerifiedWebhookResult(
            events=(
                VerifiedWebhookEvent(
                    external_installation_id=external_id,
                    native_event_type=str(event.get("type") or "event"),
                    record=_source_record(payload),
                ),
            )
        )


class NativeWhatsAppWebhook:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def verify_and_decode(
        self,
        request: BoundedWebhookRequest,
        context: OperationContext,
    ) -> VerifiedWebhookResult:
        from services.ingest.integrations.whatsapp.signature import verify_signature

        secret = await self._binding.services.secrets.resolve(SlotId("app_secret"))
        headers = {key.lower(): value for key, value in request.headers.items()}
        if not verify_signature(
            secret.reveal_text(),
            request.body,
            headers.get("x-hub-signature-256"),
        ):
            from services.ingest.source_contract.errors import (
                AuthenticationRejectedError,
            )

            raise AuthenticationRejectedError("Meta webhook signature is invalid")
        payload = json.loads(request.body)
        events: list[VerifiedWebhookEvent] = []
        for entry in payload.get("entry") or []:
            if not isinstance(entry, dict):
                continue
            for change in entry.get("changes") or []:
                if not isinstance(change, dict):
                    continue
                value = change.get("value")
                if not isinstance(value, dict):
                    continue
                metadata = value.get("metadata") or {}
                phone_number_id = metadata.get("phone_number_id")
                if not isinstance(phone_number_id, str) or not phone_number_id:
                    continue
                contacts = value.get("contacts") or []
                for message in value.get("messages") or []:
                    if isinstance(message, dict):
                        events.append(
                            VerifiedWebhookEvent(
                                external_installation_id=phone_number_id,
                                native_event_type="message",
                                record=_source_record(
                                    {
                                        "message": message,
                                        "metadata": metadata,
                                        "contacts": contacts,
                                    }
                                ),
                            )
                        )
                for status in value.get("statuses") or []:
                    if isinstance(status, dict):
                        events.append(
                            VerifiedWebhookEvent(
                                external_installation_id=phone_number_id,
                                native_event_type="status",
                                record=_source_record(
                                    {"status": status, "metadata": metadata}
                                ),
                            )
                        )
        return VerifiedWebhookResult(events=tuple(events))


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


__all__ = [
    "DirectHistoricalPull",
    "DirectIncrementalPoll",
    "DirectNormalization",
    "DirectReconciliation",
    "CredentialHealthProbe",
    "NativeIdentity",
    "NativeSlackWebhook",
    "NativeWhatsAppWebhook",
    "NativeSourceConnector",
    "OAuthCleanup",
    "LocalCredentialCleanup",
]
