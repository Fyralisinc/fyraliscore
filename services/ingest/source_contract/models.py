"""Transport-neutral DTOs shared by source capability interfaces."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.ingest.source_contract.identity import ConnectorId


SourceOperation = Literal["create", "update", "delete", "retract", "snapshot"]


class ContractModel(BaseModel):
    """Base for strict immutable contract values.

    Nested native payload dictionaries are treated as immutable by contract;
    the Pydantic ``frozen`` setting protects the DTO boundary itself.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class InstallationRef(ContractModel):
    id: UUID
    tenant_id: UUID
    connector_id: ConnectorId
    generation: int = Field(ge=1)


class ResourceDescriptor(ContractModel):
    resource_id: str = Field(min_length=1)
    resource_kind: str = Field(min_length=1)
    display_name: str | None = None
    parent_resource_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ShardPlan(ContractModel):
    kind: str = Field(min_length=1)
    identifier: dict[str, Any]
    priority: float = 1.0
    window_start: datetime | None = None
    window_end: datetime | None = None

    @field_validator("window_end")
    @classmethod
    def validate_window(cls, value: datetime | None, info: Any) -> datetime | None:
        start = info.data.get("window_start")
        if value is not None and start is not None and value < start:
            raise ValueError("window_end must not precede window_start")
        return value


class CursorState(ContractModel):
    schema_version: int = Field(ge=1)
    payload: dict[str, Any]


class SourceObjectRef(ContractModel):
    """Stable object identity plus one immutable source revision.

    ``object_id`` remains stable while ``revision_id`` changes.  Connectors
    must never put a mutable object identifier in ``revision_id`` without a
    version component; when a provider exposes no revision token the raw
    content hash is used by the writer as the deterministic fallback.
    """

    object_type: str = Field(min_length=1)
    object_id: str = Field(min_length=1)
    revision_id: str | None = None
    operation: SourceOperation = "snapshot"
    source_recorded_at: datetime | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    supersedes_revision_id: str | None = None
    parent_object_type: str | None = None
    parent_object_id: str | None = None
    container_object_type: str | None = None
    container_object_id: str | None = None
    thread_id: str | None = None

    @model_validator(mode="after")
    def validate_valid_window(self) -> "SourceObjectRef":
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_to < self.valid_from
        ):
            raise ValueError("valid_to must not precede valid_from")
        return self


class SourceRecord(ContractModel):
    native_type: str = Field(min_length=1)
    payload: bytes | dict[str, Any]
    identity_hints: dict[str, str] = Field(default_factory=dict)
    occurred_at: datetime | None = None
    source_object: SourceObjectRef | None = None


class PlanRequest(ContractModel):
    selected_resources: tuple[str, ...] = ()
    mode: Literal["initial", "resync", "repair"] = "initial"
    window_start: datetime | None = None
    window_end: datetime | None = None


class PlanResult(ContractModel):
    shards: tuple[ShardPlan, ...]


class FetchRequest(ContractModel):
    shard: ShardPlan
    cursor: CursorState | None = None
    page_size_hint: int | None = Field(default=None, ge=1)


class FetchedPage(ContractModel):
    records: tuple[SourceRecord, ...] = ()
    next_cursor: CursorState | None = None
    # Durable high-water/checkpoint for this page. Unlike next_cursor, this is
    # retained on the terminal page and is never interpreted as continuation.
    checkpoint: CursorState | None = None
    end_of_data: bool = False

    @model_validator(mode="after")
    def validate_terminal_cursor(self) -> "FetchedPage":
        if self.end_of_data and self.next_cursor is not None:
            raise ValueError("terminal pages cannot provide a next_cursor")
        return self


class PollRequest(ContractModel):
    cursor: CursorState | None = None
    selected_resources: tuple[str, ...] = ()
    overlap_seconds: int = Field(default=0, ge=0)
    page_size_hint: int | None = Field(default=None, ge=1)


class BoundedWebhookRequest(ContractModel):
    body: bytes
    headers: dict[str, str]
    query: dict[str, str] = Field(default_factory=dict)
    endpoint_id: str | None = None
    received_at: datetime


class VerifiedWebhookEvent(ContractModel):
    external_installation_id: str = Field(min_length=1)
    native_event_type: str = Field(min_length=1)
    record: SourceRecord
    signed_at: datetime | None = None
    verification_evidence: dict[str, str] = Field(default_factory=dict)


class VerifiedWebhookResult(ContractModel):
    events: tuple[VerifiedWebhookEvent, ...]
    response_status_hint: int = Field(default=202, ge=200, le=299)


class NormalizationInput(ContractModel):
    record: SourceRecord
    ingress_kind: Literal["webhook", "gateway", "pubsub", "backfill", "poll"]
    ingress_metadata: dict[str, Any] = Field(default_factory=dict)
    raw_object_key: str | None = None
    content_hash: str | None = None


class ObservationDraft(ContractModel):
    source_channel: str = Field(min_length=1)
    content_text: str
    content: dict[str, Any]
    occurred_at: datetime
    trust_tier: str
    kind: str
    source_actor_ref: str | None = None
    external_id: str | None = None
    entities_hint: tuple[dict[str, Any], ...] = ()
    raw_payload: dict[str, Any] | None = None
    source_object: SourceObjectRef | None = None


class IdentityInput(ContractModel):
    record: SourceRecord
    external_installation_id: str
    ingress_kind: Literal["webhook", "gateway", "pubsub", "backfill", "poll"]


class ShardSummary(ContractModel):
    shard_id: UUID
    shard: ShardPlan
    state: str
    cursor: CursorState | None = None
    record_count: int = Field(default=0, ge=0)
    first_occurred_at: datetime | None = None
    last_occurred_at: datetime | None = None


class ReconciliationRequest(ContractModel):
    run_id: UUID
    shards: tuple[ShardSummary, ...]
    pass_number: int = Field(ge=1)


class RepairShard(ContractModel):
    shard: ShardPlan
    parent_shard_id: UUID


class ReconciliationDecision(ContractModel):
    has_gaps: bool
    reason_code: str = "clean"
    message: str = ""
    new_shards: tuple[RepairShard, ...] = ()

    @field_validator("new_shards")
    @classmethod
    def validate_repairs(
        cls, value: tuple[RepairShard, ...], info: Any
    ) -> tuple[RepairShard, ...]:
        if value and info.data.get("has_gaps") is False:
            raise ValueError("clean reconciliation cannot propose repair shards")
        return value


class HealthCondition(ContractModel):
    type: str = Field(min_length=1)
    status: Literal["true", "false", "unknown"]
    reason: str = Field(min_length=1)
    message: str = ""
    observed_at: datetime


class HealthReport(ContractModel):
    healthy: bool
    conditions: tuple[HealthCondition, ...] = ()


class VersionedState(ContractModel):
    kind: str = Field(min_length=1)
    schema_version: int = Field(ge=1)
    producing_connector_version: str
    revision: int = Field(ge=0)
    payload: dict[str, Any]


class PublicationReceipt(ContractModel):
    receipt_id: UUID
    raw_object_key: str
    content_hash: str
    acknowledged_at: datetime


__all__ = [
    "BoundedWebhookRequest",
    "ContractModel",
    "CursorState",
    "FetchRequest",
    "FetchedPage",
    "HealthCondition",
    "HealthReport",
    "IdentityInput",
    "InstallationRef",
    "NormalizationInput",
    "ObservationDraft",
    "PlanRequest",
    "PlanResult",
    "PollRequest",
    "PublicationReceipt",
    "ReconciliationDecision",
    "ReconciliationRequest",
    "RepairShard",
    "ResourceDescriptor",
    "ShardPlan",
    "ShardSummary",
    "SourceObjectRef",
    "SourceOperation",
    "SourceRecord",
    "VerifiedWebhookEvent",
    "VerifiedWebhookResult",
    "VersionedState",
]
