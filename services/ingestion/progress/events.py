"""services/ingestion/progress/events.py
   — Pydantic event models for the `onboarding.progress` Kafka topic.

Per ingestion LLD §6 (Bridge contract). The exact event shapes
Bridge consumes; producer-side validation lives in these models.

============================================================
VERSIONING POLICY
============================================================
Additive fields only within v1. Breaking changes require a new
`event_kind` (e.g. `source.onboarding.complete_v2`) so consumers can
subscribe to specific versions without coordinated upgrades. This is
the same versioning shape the LLD §6 narrative documents.

============================================================
DEDUP CONTRACT
============================================================
Consumer-side dedup key (LLD §6): `(event_kind, tenant_id,
source_if_applicable, shard_id_if_applicable)`. Bridge consumers
dedup on this tuple. Producer side reasons about dedup through the
N1 cursor-data ordering invariant — `feels_onboarded` etc. are
emitted from within `advance_cursor_atomic_with_kafka_publish` or
behind a guarded UPDATE (LLD §2.6: `UPDATE onboarding_runs SET
feels_onboarded_at = now() WHERE id = $1 AND feels_onboarded_at IS
NULL` only publishes if the UPDATE affected 1 row).
"""
from __future__ import annotations

import datetime as dt
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


Source = Literal["slack", "github", "discord", "gmail"]


class ProgressEventBase(BaseModel):
    """Envelope for all `onboarding.progress` events.

    Subclasses override `event_kind` with a `Literal[...]` so the
    Pydantic model class is uniquely identified by the value.
    Concrete events are exhaustively enumerated below; new kinds
    require a new subclass (deliberate: a free-form `event_kind` would
    let producers ship events Bridge doesn't know how to consume).
    """

    model_config = ConfigDict(extra="forbid")

    event_version: Literal[1] = 1
    event_kind: str
    tenant_id: UUID
    emitted_at: dt.datetime = Field(
        default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc),
    )


class TenantOnboardingStarted(ProgressEventBase):
    event_kind: Literal["tenant.onboarding.started"] = "tenant.onboarding.started"
    started_at: dt.datetime
    sources: list[Source]
    eta_minutes: int  # planner estimate; non-binding


class SourceOnboardingStarted(ProgressEventBase):
    event_kind: Literal["source.onboarding.started"] = "source.onboarding.started"
    source: Source
    started_at: dt.datetime
    planned_shard_count: int


class SourceOnboardingFeelsOnboarded(ProgressEventBase):
    """Per Phase 2.1 Q C3: fires when the last 7 days are queryable
    for this source. Content-based (gap below reconciliation
    threshold), NOT time-based.
    """

    event_kind: Literal["source.onboarding.feels_onboarded"] = (
        "source.onboarding.feels_onboarded"
    )
    source: Source
    observations_count: int
    recency_window_days: int = 7


class ShardFetched(ProgressEventBase):
    event_kind: Literal["shard.fetched"] = "shard.fetched"
    source: Source
    shard_id: UUID
    observation_count: int
    fetched_in_seconds: float


CoverageConfidence = Literal[
    "exact",
    "gap_reshared",
    "sparse_sampled_ok",
    "sparse_sampled_gaps_found",
    "partial",
]


class SourceOnboardingComplete(ProgressEventBase):
    event_kind: Literal["source.onboarding.complete"] = (
        "source.onboarding.complete"
    )
    source: Source
    total_observations: int
    total_seconds: float
    gaps_resolved: int
    coverage_confidence: CoverageConfidence


class TenantOnboardingComplete(ProgressEventBase):
    event_kind: Literal["tenant.onboarding.complete"] = (
        "tenant.onboarding.complete"
    )
    total_observations: int
    completed_at: dt.datetime
    sources: list[Source]


class TenantOnboardingBehindSchedule(ProgressEventBase):
    """Ops-only signal. Fires 15 min after `tenant.onboarding.started`
    if no source has emitted `feels_onboarded`. Do NOT route to
    user-facing UI; this is for ops alerting only.
    """

    event_kind: Literal["tenant.onboarding.behind_schedule"] = (
        "tenant.onboarding.behind_schedule"
    )
    sources_pending: list[str]
    shard_progress: dict[str, dict[str, int]]  # {source: {done, total, in_progress}}


# Union of all concrete event types. Producers accept this as the
# `event` argument; mypy enforces that callers pass a known kind.
ProgressEvent = (
    TenantOnboardingStarted
    | SourceOnboardingStarted
    | SourceOnboardingFeelsOnboarded
    | ShardFetched
    | SourceOnboardingComplete
    | TenantOnboardingComplete
    | TenantOnboardingBehindSchedule
)


__all__ = [
    "CoverageConfidence",
    "ProgressEvent",
    "ProgressEventBase",
    "ShardFetched",
    "Source",
    "SourceOnboardingComplete",
    "SourceOnboardingFeelsOnboarded",
    "SourceOnboardingStarted",
    "TenantOnboardingBehindSchedule",
    "TenantOnboardingComplete",
    "TenantOnboardingStarted",
]
