"""Leased work boundary for InterventionEpisode manifest projection.

Canonical stage writers remain authoritative.  This repository discovers their
immutable canonical events, revalidates the exact source object/version, and
owns only the mutable delivery state needed to ask ``EpisodeCoordinator`` to
link that already-committed object into its episode.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.agency import EpisodeStageFate, InterventionEpisode
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


class InterventionManifestWorkStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_SCHEDULED = "retry_scheduled"
    APPLIED = "applied"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True, slots=True)
class InterventionManifestWorkItem:
    id: UUID
    tenant_id: UUID
    source_event_id: UUID
    episode_id: UUID
    stage: str
    object_ref: str
    writer_id: str
    source_object_type: str
    source_object_id: UUID
    source_object_version: int
    intervention_spec_digest: str | None
    status: InterventionManifestWorkStatus
    attempt_count: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    lease_expires_at: datetime | None
    applied_episode_version: int | None
    last_failure_class: str | None
    last_failure_reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class InterventionManifestWorkContext:
    work_item: InterventionManifestWorkItem
    episode: InterventionEpisode
    current_episode_version: int
    source_event_created_at: datetime


@dataclass(frozen=True, slots=True)
class _SourceKind:
    stage: str
    writer_id: str
    object_type: str


@dataclass(frozen=True, slots=True)
class _ResolvedSource:
    episode_id: UUID
    intervention_spec_digest: str | None


_SOURCE_KINDS: dict[tuple[str, str], _SourceKind] = {
    ("ProposalAppender", "consequential_proposal"): _SourceKind(
        "proposal", "ProposalAppender", "consequential_proposal"
    ),
    ("PredictionWriter", "prediction"): _SourceKind(
        "prediction", "PredictionWriter", "prediction"
    ),
    ("AuthorizationApplier", "authorization_decision"): _SourceKind(
        "authorization", "AuthorizationApplier", "authorization_decision"
    ),
    ("AgencyStateApplier", "workflow_run"): _SourceKind(
        "workflow", "AgencyStateApplier", "workflow_run"
    ),
    ("AgencyStateApplier", "task"): _SourceKind(
        "task", "AgencyStateApplier", "task"
    ),
    ("WorkLedgerApplier", "work_obligation"): _SourceKind(
        "work", "WorkLedgerApplier", "work_obligation"
    ),
    ("ExecutionLedgerApplier", "external_effect_attempt"): _SourceKind(
        "effect", "ExecutionLedgerApplier", "external_effect_attempt"
    ),
    ("OutcomeRecorder", "outcome"): _SourceKind(
        "outcome", "OutcomeRecorder", "outcome"
    ),
    ("SettlementApplier", "settlement"): _SourceKind(
        "settlement", "SettlementApplier", "settlement"
    ),
    ("AttributionApplier", "attribution"): _SourceKind(
        "attribution", "AttributionApplier", "attribution"
    ),
}


def _work_item(row: asyncpg.Record) -> InterventionManifestWorkItem:
    return InterventionManifestWorkItem(
        id=row["id"],
        tenant_id=row["tenant_id"],
        source_event_id=row["source_event_id"],
        episode_id=row["episode_id"],
        stage=str(row["stage"]),
        object_ref=str(row["object_ref"]),
        writer_id=str(row["writer_id"]),
        source_object_type=str(row["source_object_type"]),
        source_object_id=row["source_object_id"],
        source_object_version=int(row["source_object_version"]),
        intervention_spec_digest=row["intervention_spec_digest"],
        status=InterventionManifestWorkStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        lease_expires_at=row["lease_expires_at"],
        applied_episode_version=(
            int(row["applied_episode_version"])
            if row["applied_episode_version"] is not None
            else None
        ),
        last_failure_class=row["last_failure_class"],
        last_failure_reason=row["last_failure_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


class InterventionManifestWorkRepo:
    """Discover, lease and terminalize episode-manifest projection work."""

    async def discover_ready_work(
        self,
        conn: asyncpg.Connection,
        *,
        now: datetime,
        limit: int,
        tenant_id: UUID | None = None,
    ) -> int:
        """Discover version-one stage events not yet represented as work.

        Later lifecycle events for workflow, task, work, effect and proposal do
        not create new manifest work because the manifest links object identity,
        not every source aggregate version.
        """

        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await conn.fetch(
            """
            SELECT event.id
            FROM agency_canonical_events event
            WHERE event.object_version = 1
              AND ($1::uuid IS NULL OR event.tenant_id = $1)
              AND (
                (event.writer_id='ProposalAppender'
                  AND event.object_type='consequential_proposal')
                OR (event.writer_id='PredictionWriter'
                  AND event.object_type='prediction')
                OR (event.writer_id='AuthorizationApplier'
                  AND event.object_type='authorization_decision')
                OR (event.writer_id='AgencyStateApplier'
                  AND event.object_type IN ('workflow_run', 'task'))
                OR (event.writer_id='WorkLedgerApplier'
                  AND event.object_type='work_obligation'
                  AND EXISTS (
                    SELECT 1
                    FROM work_obligation_versions version
                    JOIN work_obligation_specs spec
                      ON spec.tenant_id=version.tenant_id
                     AND spec.obligation_id=version.obligation_id
                    JOIN agency_task_heads task
                      ON task.tenant_id=spec.tenant_id
                     AND task.task_id=spec.target_object_id
                    WHERE version.tenant_id=event.tenant_id
                      AND version.obligation_id=event.object_id
                      AND version.aggregate_version=event.object_version
                      AND version.command_result_id=event.command_result_id
                      AND spec.target_object_type='task'
                  ))
                OR (event.writer_id='ExecutionLedgerApplier'
                  AND event.object_type='external_effect_attempt')
                OR (event.writer_id='OutcomeRecorder'
                  AND event.object_type='outcome')
                OR (event.writer_id='SettlementApplier'
                  AND event.object_type='settlement')
                OR (event.writer_id='AttributionApplier'
                  AND event.object_type='attribution')
              )
              AND NOT EXISTS (
                SELECT 1
                FROM intervention_episode_manifest_work_items work
                WHERE work.tenant_id=event.tenant_id
                  AND work.source_event_id=event.id
              )
            ORDER BY event.created_at, event.id
            FOR UPDATE OF event SKIP LOCKED
            LIMIT $2
            """,
            tenant_id,
            limit,
        )
        discovered = 0
        for row in rows:
            item = await self.discover_from_event(
                conn,
                source_event_id=row["id"],
                now=now,
            )
            if item is not None and item.source_event_id == row["id"]:
                discovered += 1
        return discovered

    async def discover_from_event(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
        now: datetime,
    ) -> InterventionManifestWorkItem | None:
        """Idempotently create manifest work from one canonical stage event."""

        event = await self._load_event_and_result(
            conn,
            source_event_id=source_event_id,
        )
        source_kind = _SOURCE_KINDS.get(
            (str(event["writer_id"]), str(event["object_type"]))
        )
        if source_kind is None:
            return None
        resolved = await self._resolve_source(conn, event=event)
        if resolved is None:
            # A WorkObligation can be unrelated to an intervention episode.
            if source_kind.stage == "work":
                return None
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_SOURCE_MISSING",
                "canonical stage event does not resolve to its exact source object",
                source_event_id=str(source_event_id),
                object_type=source_kind.object_type,
                object_id=str(event["object_id"]),
                object_version=int(event["object_version"]),
            )
        event_digest = event["intervention_spec_digest"]
        if (
            event_digest is not None
            and resolved.intervention_spec_digest is not None
            and event_digest != resolved.intervention_spec_digest
        ):
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_SPEC_DRIFT",
                "canonical event and exact source object disagree on InterventionSpec",
                source_event_id=str(source_event_id),
            )
        spec_digest = resolved.intervention_spec_digest or event_digest
        object_ref = f"{source_kind.stage}:{event['object_id']}"
        inserted = await conn.fetchrow(
            """
            INSERT INTO intervention_episode_manifest_work_items (
              id, tenant_id, source_event_id, episode_id, stage, object_ref,
              writer_id, source_object_type, source_object_id,
              source_object_version, intervention_spec_digest, status,
              available_at, created_at, updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',$12,$12,$12
            )
            ON CONFLICT DO NOTHING
            RETURNING *
            """,
            uuid7(),
            event["tenant_id"],
            source_event_id,
            resolved.episode_id,
            source_kind.stage,
            object_ref,
            source_kind.writer_id,
            source_kind.object_type,
            event["object_id"],
            int(event["object_version"]),
            spec_digest,
            now,
        )
        if inserted is not None:
            return _work_item(inserted)
        existing = await conn.fetchrow(
            """
            SELECT *
            FROM intervention_episode_manifest_work_items
            WHERE tenant_id=$1
              AND (
                source_event_id=$2
                OR (episode_id=$3 AND stage=$4)
              )
            FOR KEY SHARE
            """,
            event["tenant_id"],
            source_event_id,
            resolved.episode_id,
            source_kind.stage,
        )
        if existing is None:
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_DISCOVERY_RACE",
                "manifest work conflict disappeared during idempotent discovery",
                source_event_id=str(source_event_id),
            )
        item = _work_item(existing)
        if (
            item.episode_id != resolved.episode_id
            or item.stage != source_kind.stage
            or item.object_ref != object_ref
            or item.writer_id != source_kind.writer_id
            or item.source_object_type != source_kind.object_type
            or item.source_object_id != event["object_id"]
            or item.intervention_spec_digest != spec_digest
        ):
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_STAGE_CONFLICT",
                "one episode stage was discovered from a different canonical object",
                episode_id=str(resolved.episode_id),
                stage=source_kind.stage,
                existing_object_ref=item.object_ref,
                proposed_object_ref=object_ref,
            )
        return item

    async def claim_ready_work(
        self,
        conn: asyncpg.Connection,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[InterventionManifestWorkItem, ...]:
        """Claim ready work and recover abandoned expired leases atomically."""

        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await conn.fetch(
            """
            WITH candidates AS (
              SELECT work.id
              FROM intervention_episode_manifest_work_items work
              WHERE (
                (work.status IN ('pending', 'retry_scheduled')
                  AND work.available_at <= $2)
                OR (work.status='processing' AND work.lease_expires_at <= $2)
              )
              ORDER BY
                CASE WHEN work.status='processing' THEN work.lease_expires_at
                     ELSE work.available_at END,
                work.created_at,
                work.id
              FOR UPDATE OF work SKIP LOCKED
              LIMIT $4
            )
            UPDATE intervention_episode_manifest_work_items work
            SET status='processing',
                attempt_count=work.attempt_count + 1,
                claimed_by=$1,
                claim_token=gen_random_uuid(),
                lease_expires_at=$2 + $3::interval,
                updated_at=$2
            FROM candidates
            WHERE work.id=candidates.id
            RETURNING work.*
            """,
            worker_id,
            now,
            lease_duration,
            limit,
        )
        return tuple(_work_item(row) for row in rows)

    async def load_claimed_context(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
    ) -> InterventionManifestWorkContext:
        """Load current episode state after revalidating the exact source event."""

        row = await conn.fetchrow(
            """
            SELECT *
            FROM intervention_episode_manifest_work_items
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            FOR UPDATE
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
        )
        if row is None:
            self._raise_stale_claim(work_item_id)
        item = _work_item(row)
        event = await self._load_event_and_result(
            conn,
            source_event_id=item.source_event_id,
        )
        source_kind = _SOURCE_KINDS.get(
            (str(event["writer_id"]), str(event["object_type"]))
        )
        resolved = await self._resolve_source(conn, event=event)
        expected = (
            source_kind is not None
            and resolved is not None
            and source_kind.stage == item.stage
            and source_kind.writer_id == item.writer_id
            and source_kind.object_type == item.source_object_type
            and event["tenant_id"] == item.tenant_id
            and event["object_id"] == item.source_object_id
            and int(event["object_version"]) == item.source_object_version
            and resolved.episode_id == item.episode_id
            and f"{source_kind.stage}:{event['object_id']}" == item.object_ref
            and (
                item.intervention_spec_digest
                == (
                    resolved.intervention_spec_digest
                    or event["intervention_spec_digest"]
                )
            )
        )
        if not expected:
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_SOURCE_DRIFT",
                "claimed manifest work no longer matches its exact canonical source",
                work_item_id=str(work_item_id),
                source_event_id=str(item.source_event_id),
            )
        episode_row = await conn.fetchrow(
            """
            SELECT head.current_version, version.episode
            FROM intervention_episode_heads head
            JOIN intervention_episode_versions version
              ON version.tenant_id=head.tenant_id
             AND version.episode_id=head.episode_id
             AND version.aggregate_version=head.current_version
            WHERE head.tenant_id=$1 AND head.episode_id=$2
            FOR UPDATE OF head
            """,
            item.tenant_id,
            item.episode_id,
        )
        if episode_row is None:
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_EPISODE_MISSING",
                "claimed work episode has no current canonical manifest version",
                work_item_id=str(work_item_id),
                episode_id=str(item.episode_id),
            )
        episode = InterventionEpisode.model_validate(_json(episode_row["episode"]))
        return InterventionManifestWorkContext(
            work_item=item,
            episode=episode,
            current_episode_version=int(episode_row["current_version"]),
            source_event_created_at=event["created_at"],
        )

    async def mark_applied(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        applied_episode_version: int,
        now: datetime,
    ) -> InterventionManifestWorkItem:
        """Terminalize only after an exact stage link exists in that episode version."""

        if applied_episode_version < 1:
            raise ValueError("applied_episode_version must be positive")
        item_row = await self._load_live_claim(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        item = _work_item(item_row)
        episode_payload = await conn.fetchval(
            """
            SELECT version.episode
            FROM intervention_episode_versions version
            JOIN intervention_episode_heads head
              ON head.tenant_id=version.tenant_id
             AND head.episode_id=version.episode_id
            WHERE version.tenant_id=$1 AND version.episode_id=$2
              AND version.aggregate_version=$3
              AND head.current_version >= version.aggregate_version
            FOR KEY SHARE OF version, head
            """,
            tenant_id,
            item.episode_id,
            applied_episode_version,
        )
        if episode_payload is None:
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_APPLIED_VERSION_MISSING",
                "applied episode version does not exist on the current episode lineage",
                work_item_id=str(work_item_id),
                applied_episode_version=applied_episode_version,
            )
        episode = InterventionEpisode.model_validate(_json(episode_payload))
        link = next(
            (candidate for candidate in episode.stage_links if candidate.stage == item.stage),
            None,
        )
        if (
            link is None
            or link.fate is not EpisodeStageFate.PRESENT
            or link.object_ref != item.object_ref
            or link.writer_id != item.writer_id
            or (
                item.intervention_spec_digest is not None
                and episode.intervention_spec_digest
                != item.intervention_spec_digest
            )
        ):
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_LINK_NOT_APPLIED",
                "episode version does not contain the exact discovered stage link",
                work_item_id=str(work_item_id),
                episode_id=str(item.episode_id),
                stage=item.stage,
            )
        updated = await conn.fetchrow(
            """
            UPDATE intervention_episode_manifest_work_items
            SET status='applied', applied_episode_version=$6,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=NULL, last_failure_reason=NULL, updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            applied_episode_version,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def schedule_retry(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
        next_attempt_at: datetime,
        failure_class: str,
        failure_reason: str,
    ) -> InterventionManifestWorkItem:
        """Release a live fenced claim into a durable retry schedule."""

        if next_attempt_at <= now:
            raise ValueError("next_attempt_at must be after now")
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE intervention_episode_manifest_work_items
            SET status='retry_scheduled', available_at=$6,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=$7, last_failure_reason=$8, updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            next_attempt_at,
            failure_class,
            failure_reason,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def fail_work_terminally(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
        failure_class: str,
        failure_reason: str,
    ) -> InterventionManifestWorkItem:
        """Record an explicit terminal projection failure under the live fence."""

        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE intervention_episode_manifest_work_items
            SET status='failed_terminal',
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=$6, last_failure_reason=$7, updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            failure_class,
            failure_reason,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def _load_event_and_result(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
    ) -> asyncpg.Record:
        row = await conn.fetchrow(
            """
            SELECT event.*,
                   result.tenant_id AS result_tenant_id,
                   result.writer_id AS result_writer_id,
                   result.object_type AS result_object_type,
                   result.object_id AS result_object_id,
                   result.object_version AS result_object_version
            FROM agency_canonical_events event
            JOIN agency_command_results result
              ON result.id=event.command_result_id
            WHERE event.id=$1
            """,
            source_event_id,
        )
        if row is None:
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_SOURCE_EVENT_MISSING",
                "manifest work requires an existing canonical source event",
                source_event_id=str(source_event_id),
            )
        if (
            row["result_tenant_id"] != row["tenant_id"]
            or row["result_writer_id"] != row["writer_id"]
            or row["result_object_type"] != row["object_type"]
            or row["result_object_id"] != row["object_id"]
            or int(row["result_object_version"]) != int(row["object_version"])
        ):
            raise InvariantViolation(
                "INTERVENTION_MANIFEST_EVENT_RESULT_DRIFT",
                "canonical event no longer matches its exact command result",
                source_event_id=str(source_event_id),
            )
        return row

    async def _resolve_source(
        self,
        conn: asyncpg.Connection,
        *,
        event: asyncpg.Record,
    ) -> _ResolvedSource | None:
        object_type = str(event["object_type"])
        if object_type == "consequential_proposal":
            row = await conn.fetchrow(
                """
                SELECT proposal.episode_id,
                       proposal.intervention_spec_digest
                FROM consequential_proposals proposal
                WHERE proposal.tenant_id=$1 AND proposal.id=$2
                  AND (
                    ($3=1 AND proposal.command_result_id=$4)
                    OR EXISTS (
                      SELECT 1
                      FROM consequential_proposal_reviews review
                      WHERE review.tenant_id=proposal.tenant_id
                        AND review.proposal_id=proposal.id
                        AND review.to_fate_version=$3
                        AND review.command_result_id=$4
                    )
                  )
                """,
                event["tenant_id"],
                event["object_id"],
                int(event["object_version"]),
                event["command_result_id"],
            )
        elif object_type == "prediction":
            row = await self._immutable_source_row(
                conn,
                table="consequential_predictions",
                event=event,
            )
        elif object_type == "authorization_decision":
            row = await self._immutable_source_row(
                conn,
                table="consequential_authorization_decisions",
                event=event,
            )
        elif object_type == "workflow_run":
            row = await conn.fetchrow(
                """
                SELECT head.episode_id, head.intervention_spec_digest
                FROM agency_workflow_run_versions version
                JOIN agency_workflow_run_heads head
                  ON head.tenant_id=version.tenant_id
                 AND head.workflow_run_id=version.workflow_run_id
                WHERE version.tenant_id=$1 AND version.workflow_run_id=$2
                  AND version.aggregate_version=$3
                  AND version.command_result_id=$4
                """,
                event["tenant_id"],
                event["object_id"],
                int(event["object_version"]),
                event["command_result_id"],
            )
        elif object_type == "task":
            row = await conn.fetchrow(
                """
                SELECT head.episode_id, head.intervention_spec_digest
                FROM agency_task_versions version
                JOIN agency_task_heads head
                  ON head.tenant_id=version.tenant_id
                 AND head.task_id=version.task_id
                WHERE version.tenant_id=$1 AND version.task_id=$2
                  AND version.aggregate_version=$3
                  AND version.command_result_id=$4
                """,
                event["tenant_id"],
                event["object_id"],
                int(event["object_version"]),
                event["command_result_id"],
            )
        elif object_type == "work_obligation":
            row = await conn.fetchrow(
                """
                SELECT task.episode_id, task.intervention_spec_digest
                FROM work_obligation_versions version
                JOIN work_obligation_specs spec
                  ON spec.tenant_id=version.tenant_id
                 AND spec.obligation_id=version.obligation_id
                JOIN agency_task_heads task
                  ON task.tenant_id=spec.tenant_id
                 AND task.task_id=spec.target_object_id
                WHERE version.tenant_id=$1 AND version.obligation_id=$2
                  AND version.aggregate_version=$3
                  AND version.command_result_id=$4
                  AND spec.target_object_type='task'
                """,
                event["tenant_id"],
                event["object_id"],
                int(event["object_version"]),
                event["command_result_id"],
            )
        elif object_type == "external_effect_attempt":
            row = await conn.fetchrow(
                """
                SELECT head.episode_id, head.intervention_spec_digest
                FROM external_effect_attempt_versions version
                JOIN external_effect_attempt_heads head
                  ON head.tenant_id=version.tenant_id
                 AND head.effect_attempt_id=version.effect_attempt_id
                WHERE version.tenant_id=$1 AND version.effect_attempt_id=$2
                  AND version.aggregate_version=$3
                  AND version.command_result_id=$4
                """,
                event["tenant_id"],
                event["object_id"],
                int(event["object_version"]),
                event["command_result_id"],
            )
        elif object_type == "outcome":
            row = await self._immutable_source_row(
                conn,
                table="consequential_outcomes",
                event=event,
                has_spec_digest=False,
            )
        elif object_type == "settlement":
            row = await self._immutable_source_row(
                conn,
                table="consequential_settlements",
                event=event,
                has_spec_digest=False,
            )
        elif object_type == "attribution":
            row = await self._immutable_source_row(
                conn,
                table="consequential_attributions",
                event=event,
                has_spec_digest=False,
            )
        else:
            return None
        if row is None:
            return None
        return _ResolvedSource(
            episode_id=row["episode_id"],
            intervention_spec_digest=row["intervention_spec_digest"],
        )

    @staticmethod
    async def _immutable_source_row(
        conn: asyncpg.Connection,
        *,
        table: str,
        event: asyncpg.Record,
        has_spec_digest: bool = True,
    ) -> asyncpg.Record | None:
        if table not in {
            "consequential_predictions",
            "consequential_authorization_decisions",
            "consequential_outcomes",
            "consequential_settlements",
            "consequential_attributions",
        }:
            raise ValueError("unsupported immutable source table")
        digest_column = (
            "intervention_spec_digest" if has_spec_digest else "NULL::text"
        )
        return await conn.fetchrow(
            f"""
            SELECT episode_id, {digest_column} AS intervention_spec_digest
            FROM {table}
            WHERE tenant_id=$1 AND id=$2 AND command_result_id=$3
              AND $4=1
            """,
            event["tenant_id"],
            event["object_id"],
            event["command_result_id"],
            int(event["object_version"]),
        )

    async def _load_live_claim(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
    ) -> asyncpg.Record:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM intervention_episode_manifest_work_items
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            FOR UPDATE
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
        )
        if row is None:
            self._raise_stale_claim(work_item_id)
        return row

    @staticmethod
    def _validate_failure(failure_class: str, failure_reason: str) -> None:
        if not failure_class.strip() or not failure_reason.strip():
            raise ValueError("failure class and reason must be non-empty")

    @staticmethod
    def _raise_stale_claim(work_item_id: UUID) -> None:
        raise InvariantViolation(
            "INTERVENTION_MANIFEST_STALE_CLAIM",
            "manifest work transition requires the current live fence token",
            work_item_id=str(work_item_id),
        )

    def _require_claim_transition(
        self,
        row: asyncpg.Record | None,
        work_item_id: UUID,
    ) -> InterventionManifestWorkItem:
        if row is None:
            self._raise_stale_claim(work_item_id)
        return _work_item(row)


__all__ = [
    "InterventionManifestWorkContext",
    "InterventionManifestWorkItem",
    "InterventionManifestWorkRepo",
    "InterventionManifestWorkStatus",
]
