"""Connector-owned workflow boundary models.

These models keep the durable ingestion workflows independent from the retired
per-source planner, fetcher, reconciler, and handler registries.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.types import ObservationKind, TrustTierValue


@dataclass(frozen=True)
class Shard:
    shard_kind: str
    shard_identifier: dict[str, Any]
    recency_score: float = 1.0
    window_start: dt.datetime | None = None
    window_end: dt.datetime | None = None


@dataclass(frozen=True)
class PlannerContext:
    tenant_id: UUID
    install: asyncpg.Record
    conn: asyncpg.Connection
    source_client: Any | None = None


@dataclass(frozen=True)
class FetchResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: dict[str, Any] | None = None
    end_of_data: bool = False


class ResharedShard(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    shard: Shard
    parent_shard_id: UUID


class ReconciliationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    has_gaps: bool
    message: str = ""
    new_shards: list[ResharedShard] = Field(default_factory=list)


@dataclass
class ObservationDraft:
    source_channel: str
    content_text: str
    content: dict[str, Any]
    occurred_at: dt.datetime
    trust_tier: TrustTierValue
    kind: ObservationKind = "signal"
    source_actor_ref: str | None = None
    external_id: str | None = None
    entities_hint: list[dict[str, Any]] = field(default_factory=list)
    unresolved_phrases: list[str] = field(default_factory=list)
    raw_payload: dict[str, Any] | None = None


__all__ = [
    "FetchResult",
    "ObservationDraft",
    "PlannerContext",
    "ReconciliationDecision",
    "ResharedShard",
    "Shard",
]
