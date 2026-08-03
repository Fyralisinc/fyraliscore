"""Capability facets that delegate to today's dispatch maps and handlers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from services.ingest.connector_platform.legacy_context import (
    LegacyBindingPayload,
    require_legacy_binding,
)
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.planners import PLANNER_DISPATCH
from services.ingest.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingest.source_contract.capabilities.ingestion import (
    GatewayBatch,
    GatewayOpenRequest,
    GatewayReceiveRequest,
    GatewaySession,
)
from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.errors import (
    InvalidConfigurationError,
    PayloadRejectedError,
)
from services.ingest.source_contract.host_services import SecretValue
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchRequest,
    FetchedPage,
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


IdentityFunction = Callable[[IdentityInput], str]


def _cursor(payload: dict[str, Any] | None) -> CursorState | None:
    if payload is None:
        return None
    return CursorState(schema_version=1, payload=payload)


def _shard_plan(shard: Any) -> ShardPlan:
    return ShardPlan(
        kind=shard.shard_kind,
        identifier=dict(shard.shard_identifier),
        priority=shard.recency_score,
        window_start=shard.window_start,
        window_end=shard.window_end,
    )


def _source_record(record: dict[str, Any]) -> SourceRecord:
    return SourceRecord(
        native_type=str(record.get("object") or record.get("type") or "record"),
        payload=record,
    )


def _contract_draft(draft: Any) -> ObservationDraft:
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


class LegacyHistoricalPull:
    def __init__(
        self, source: str, binding: LegacyBindingPayload | None = None
    ) -> None:
        self._source = source
        self._binding = binding

    def _payload(self) -> LegacyBindingPayload:
        return self._binding or require_legacy_binding()

    async def plan(
        self, request: PlanRequest, context: OperationContext
    ) -> PlanResult:
        del request, context
        binding = self._payload()
        if binding.planner_context is None:
            raise InvalidConfigurationError("legacy planner context is unavailable")
        shards = await PLANNER_DISPATCH[self._source](
            binding.planner_context
        )
        return PlanResult(shards=tuple(_shard_plan(shard) for shard in shards))

    async def fetch(
        self, request: FetchRequest, context: OperationContext
    ) -> FetchedPage:
        del context
        result = await FETCHER_DISPATCH[self._source](
            self._payload().install,
            request.shard.identifier,
            request.cursor.payload if request.cursor is not None else None,
        )
        return FetchedPage(
            records=tuple(_source_record(record) for record in result.records),
            next_cursor=None if result.end_of_data else _cursor(result.next_cursor),
            end_of_data=result.end_of_data,
        )


class LegacyIncrementalPoll:
    def __init__(
        self, source: str, binding: LegacyBindingPayload | None = None
    ) -> None:
        self._source = source
        self._binding = binding

    async def poll(
        self, request: PollRequest, context: OperationContext
    ) -> FetchedPage:
        del context
        binding = self._binding or require_legacy_binding()
        shard_identifier = binding.poll_shard_identifier
        if shard_identifier is None:
            raise InvalidConfigurationError(
                "legacy poll requires an installation-scoped shard identifier"
            )
        result = await FETCHER_DISPATCH[self._source](
            binding.install,
            shard_identifier,
            request.cursor.payload if request.cursor is not None else None,
        )
        return FetchedPage(
            records=tuple(_source_record(record) for record in result.records),
            next_cursor=None if result.end_of_data else _cursor(result.next_cursor),
            end_of_data=result.end_of_data,
        )


class LegacyReconciliation:
    def __init__(
        self, source: str, binding: LegacyBindingPayload | None = None
    ) -> None:
        self._source = source
        self._binding = binding

    async def reconcile(
        self, request: ReconciliationRequest, context: OperationContext
    ) -> ReconciliationDecision:
        del request, context
        binding = self._binding or require_legacy_binding()
        if (
            binding.reconciliation_shards is None
            or binding.reconciliation_run is None
        ):
            raise InvalidConfigurationError(
                "legacy reconciliation rows are unavailable"
            )
        decision = await RECONCILER_DISPATCH[self._source](
            binding.reconciliation_shards,
            binding.reconciliation_run,
        )
        repairs = tuple(
            RepairShard(
                shard=_shard_plan(item.shard),
                parent_shard_id=item.parent_shard_id,
            )
            for item in decision.new_shards
        )
        return ReconciliationDecision(
            has_gaps=decision.has_gaps,
            reason_code="gaps_detected" if decision.has_gaps else "clean",
            message=decision.message,
            new_shards=repairs,
        )


class LegacyIdentity:
    def __init__(self, identity: IdentityFunction) -> None:
        self._identity = identity

    def external_id(self, input: IdentityInput) -> str:
        return self._identity(input)


class LegacyNormalization:
    def __init__(self, source: str) -> None:
        self._source = source

    async def normalize(
        self, input: NormalizationInput, context: OperationContext
    ) -> tuple[ObservationDraft, ...]:
        del context
        channel = resolve_channel(  # type: ignore[arg-type]
            self._source, input.ingress_kind
        )
        if channel is None:
            raise PayloadRejectedError(
                "legacy channel mapping has no route",
                details={
                    "source": self._source,
                    "ingress_kind": input.ingress_kind,
                },
            )
        if not isinstance(input.record.payload, dict):
            raise PayloadRejectedError("legacy handler requires a JSON object")
        headers = input.ingress_metadata.get("headers") or {}
        draft = await get_handler(channel)(input.record.payload, dict(headers))
        return (_contract_draft(draft),)


class LegacySlackWebhook:
    """Delegate Slack HMAC semantics to the existing verifier function."""

    def __init__(self, binding: LegacyBindingPayload) -> None:
        self._binding = binding

    async def verify_and_decode(
        self, request: BoundedWebhookRequest, context: OperationContext
    ) -> VerifiedWebhookResult:
        from services.ingest.ingestion.handlers.slack import verify_slack_signature

        secret: SecretValue = await context.services.secrets.resolve(
            SlotId("webhook_signing_secret")
        )
        headers = {key.lower(): value for key, value in request.headers.items()}
        verify_slack_signature(
            request.body,
            headers.get("x-slack-request-timestamp", ""),
            headers.get("x-slack-signature", ""),
            secret.reveal_text(),
            now=request.received_at.timestamp(),
        )
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadRejectedError("Slack webhook body is not JSON") from exc
        if not isinstance(payload, dict):
            raise PayloadRejectedError("Slack webhook body is not an object")
        raw_event = payload.get("event")
        event: dict[str, Any] = raw_event if isinstance(raw_event, dict) else payload
        native_event_type = str(event.get("type") or payload.get("type") or "event")
        external_installation_id = str(
            payload.get("team_id") or self._binding.external_installation_id
        )
        signed_at = datetime.fromtimestamp(
            int(headers["x-slack-request-timestamp"]), tz=timezone.utc
        )
        return VerifiedWebhookResult(
            events=(
                VerifiedWebhookEvent(
                    external_installation_id=external_installation_id,
                    native_event_type=native_event_type,
                    record=_source_record(payload),
                    signed_at=signed_at,
                    verification_evidence={"scheme": "slack-v0-hmac-sha256"},
                ),
            )
        )


class LegacyGatewayStream:
    """Adapter shape for existing queue/session-backed gateway drivers."""

    def __init__(self, binding: LegacyBindingPayload) -> None:
        if binding.gateway_driver is None:
            raise InvalidConfigurationError("legacy gateway driver is unavailable")
        self._driver = binding.gateway_driver

    async def open(
        self, request: GatewayOpenRequest, context: OperationContext
    ) -> GatewaySession:
        del context
        return await self._driver.open(request)

    async def receive(
        self, request: GatewayReceiveRequest, context: OperationContext
    ) -> GatewayBatch:
        del context
        return await self._driver.receive(request)

    async def close(
        self, session: GatewaySession, context: OperationContext
    ) -> None:
        del context
        await self._driver.close(session)


__all__ = [
    "IdentityFunction",
    "LegacyGatewayStream",
    "LegacyHistoricalPull",
    "LegacyIdentity",
    "LegacyIncrementalPoll",
    "LegacyNormalization",
    "LegacyReconciliation",
    "LegacySlackWebhook",
]
