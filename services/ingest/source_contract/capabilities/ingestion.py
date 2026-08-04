"""Pull, poll, push, stream, discovery, and reconciliation capabilities."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import Field

from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    ContractModel,
    CursorState,
    FetchRequest,
    FetchedPage,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    ResourceDescriptor,
    SourceRecord,
    VerifiedWebhookResult,
)


class DiscoveryRequest(ContractModel):
    cursor: CursorState | None = None
    resource_kinds: tuple[str, ...] = ()


class DiscoveryResult(ContractModel):
    resources: tuple[ResourceDescriptor, ...]
    next_cursor: CursorState | None = None
    end_of_data: bool = True


class SubscriptionRequest(ContractModel):
    callback_url: str
    endpoint_id: str
    event_types: tuple[str, ...]
    verification_token: str | None = None


class SubscriptionState(ContractModel):
    external_subscription_id: str = Field(min_length=1)
    expires_at_epoch_seconds: int | None = None
    state: CursorState | None = None


class GatewayOpenRequest(ContractModel):
    resume_state: CursorState | None = None


class GatewaySession(ContractModel):
    session_id: str = Field(min_length=1)
    resume_state: CursorState | None = None


class GatewayReceiveRequest(ContractModel):
    session: GatewaySession
    max_records: int = Field(default=100, ge=1)


class GatewayBatch(ContractModel):
    records: tuple[SourceRecord, ...]
    resume_state: CursorState | None = None
    session_closed: bool = False


@runtime_checkable
class ResourceDiscoveryCapability(Protocol):
    async def discover(
        self,
        request: DiscoveryRequest,
        context: OperationContext,
    ) -> DiscoveryResult: ...


@runtime_checkable
class HistoricalPullCapability(Protocol):
    async def plan(
        self,
        request: PlanRequest,
        context: OperationContext,
    ) -> PlanResult: ...

    async def fetch(
        self,
        request: FetchRequest,
        context: OperationContext,
    ) -> FetchedPage: ...


@runtime_checkable
class IncrementalPollCapability(Protocol):
    async def poll(
        self,
        request: PollRequest,
        context: OperationContext,
    ) -> FetchedPage: ...


@runtime_checkable
class WebhookCapability(Protocol):
    async def verify_and_decode(
        self,
        request: BoundedWebhookRequest,
        context: OperationContext,
    ) -> VerifiedWebhookResult: ...


@runtime_checkable
class PushSubscriptionCapability(Protocol):
    async def ensure(
        self,
        request: SubscriptionRequest,
        context: OperationContext,
    ) -> SubscriptionState: ...

    async def renew(
        self,
        subscription: SubscriptionState,
        context: OperationContext,
    ) -> SubscriptionState: ...

    async def revoke(
        self,
        subscription: SubscriptionState,
        context: OperationContext,
    ) -> None: ...


@runtime_checkable
class GatewayStreamCapability(Protocol):
    async def open(
        self,
        request: GatewayOpenRequest,
        context: OperationContext,
    ) -> GatewaySession: ...

    async def receive(
        self,
        request: GatewayReceiveRequest,
        context: OperationContext,
    ) -> GatewayBatch: ...

    async def close(
        self,
        session: GatewaySession,
        context: OperationContext,
    ) -> None: ...


@runtime_checkable
class ReconciliationCapability(Protocol):
    async def reconcile(
        self,
        request: ReconciliationRequest,
        context: OperationContext,
    ) -> ReconciliationDecision: ...


__all__ = [
    "DiscoveryRequest",
    "DiscoveryResult",
    "GatewayBatch",
    "GatewayOpenRequest",
    "GatewayReceiveRequest",
    "GatewaySession",
    "GatewayStreamCapability",
    "HistoricalPullCapability",
    "IncrementalPollCapability",
    "PushSubscriptionCapability",
    "ReconciliationCapability",
    "ResourceDiscoveryCapability",
    "SubscriptionRequest",
    "SubscriptionState",
    "WebhookCapability",
]
