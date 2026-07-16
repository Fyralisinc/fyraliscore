"""Persistence for authenticated source-native identity mappings."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.contracts.kernel import BitemporalInterval
from lib.contracts.perception import SourceIdentityBinding
from lib.shared.ids import uuid7


@dataclass(frozen=True)
class ResolvedSourceIdentityBinding:
    """One visible binding plus the typed canonical candidate it authorizes."""

    binding: SourceIdentityBinding
    canonical_ref: dict[str, Any]
    attachment_authority_ref: str
    source_surface: str


@dataclass(frozen=True)
class SourceIdentityBindingLifecycleResult:
    """One applied or idempotently replayed binding lifecycle command."""

    operation_kind: Literal["close", "revoke", "supersede"]
    operation_ref: str
    binding_lineage_id: str
    prior_binding_version: int
    result_bindings: tuple[SourceIdentityBinding, ...]
    applied: bool
    effective_at: datetime
    transaction_at: datetime


class SourceIdentityBindingRepo:
    """Append and resolve source-envelope identity authority."""

    def __init__(self, pool: asyncpg.Pool | None) -> None:
        self._pool = pool

    async def bind(
        self,
        *,
        tenant_id: UUID,
        source_system: str,
        source_native_identifier: str,
        source_identity_authority_ref: str,
        canonical_ref: dict[str, Any],
        evidence_refs: tuple[str, ...],
        valid_from: datetime,
        transaction_from: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBinding:
        """Create the current binding; identical retries return the first row."""

        transaction_from = transaction_from or datetime.now(timezone.utc)
        normalized_ref = _canonical_ref(canonical_ref)
        binding_id = uuid7()

        async def write(target: asyncpg.Connection) -> SourceIdentityBinding:
            await target.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                (
                    f"source-binding-key:{tenant_id}:{source_system}:"
                    f"{source_native_identifier}"
                ),
            )
            overlapping = await target.fetch(
                """
                SELECT *
                FROM source_identity_bindings
                WHERE tenant_id=$1
                  AND source_system=$2
                  AND source_native_identifier=$3
                  AND transaction_to IS NULL
                  AND (
                    valid_to IS NULL
                    OR $4::timestamptz < valid_to
                  )
                ORDER BY valid_from, binding_version
                LIMIT 2
                """,
                tenant_id,
                source_system,
                source_native_identifier,
                valid_from,
            )
            if overlapping:
                existing = overlapping[0]
                identical_current = (
                    len(overlapping) == 1
                    and existing["valid_to"] is None
                    and dict(existing["canonical_referent"]) == normalized_ref
                    and existing["source_identity_authority_ref"]
                    == source_identity_authority_ref
                    and tuple(existing["evidence_refs"]) == evidence_refs
                    and existing["valid_from"] == valid_from
                )
                if identical_current:
                    return _binding_from_row(existing)
                raise ValueError(
                    "source-native identifier already has a binding whose "
                    "valid-time interval overlaps the requested binding"
                )
            row = await target.fetchrow(
                """
                INSERT INTO source_identity_bindings (
                    id, tenant_id, lineage_id, binding_version, source_system,
                    source_native_identifier, source_identity_authority_ref,
                    canonical_referent, valid_from, transaction_from,
                    evidence_refs, lifecycle_operation_kind,
                    lifecycle_operation_ref
                ) VALUES (
                    $1, $2, $1, 1, $3, $4, $5, $6::jsonb, $7, $8, $9,
                    'bind', $10
                )
                ON CONFLICT (
                    tenant_id, source_system, source_native_identifier
                ) WHERE valid_to IS NULL AND transaction_to IS NULL
                DO NOTHING
                RETURNING *
                """,
                binding_id,
                tenant_id,
                source_system,
                source_native_identifier,
                source_identity_authority_ref,
                normalized_ref,
                valid_from,
                transaction_from,
                list(evidence_refs),
                f"bind:{binding_id}:1",
            )
            if row is None:
                row = await target.fetchrow(
                    """
                    SELECT * FROM source_identity_bindings
                    WHERE tenant_id=$1
                      AND source_system=$2
                      AND source_native_identifier=$3
                      AND valid_to IS NULL
                      AND transaction_to IS NULL
                    """,
                    tenant_id,
                    source_system,
                    source_native_identifier,
                )
                if row is None:
                    raise RuntimeError("source identity binding conflict vanished")
                if (
                    dict(row["canonical_referent"]) != normalized_ref
                    or row["source_identity_authority_ref"]
                    != source_identity_authority_ref
                    or tuple(row["evidence_refs"]) != evidence_refs
                    or row["valid_from"] != valid_from
                ):
                    raise ValueError(
                        "source-native identifier already has a different "
                        "current binding"
                    )
            return _binding_from_row(row)

        if conn is not None:
            return await write(conn)
        if self._pool is None:
            raise ValueError("source identity binding write requires a connection")
        async with self._pool.acquire() as owned, owned.transaction():
            return await write(owned)

    async def close(
        self,
        *,
        tenant_id: UUID,
        binding_lineage_id: UUID | str,
        expected_binding_version: int,
        effective_at: datetime,
        operation_ref: str,
        reason: str,
        evidence_refs: tuple[str, ...],
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBindingLifecycleResult:
        """Close one current lineage without creating a successor."""

        return await self._run_lifecycle_operation(
            tenant_id=tenant_id,
            binding_lineage_id=UUID(str(binding_lineage_id)),
            expected_binding_version=expected_binding_version,
            effective_at=effective_at,
            operation_kind="close",
            operation_ref=operation_ref,
            reason=reason,
            evidence_refs=evidence_refs,
            new_canonical_ref=None,
            new_source_identity_authority_ref=None,
            new_evidence_refs=None,
            conn=conn,
        )

    async def revoke(
        self,
        *,
        tenant_id: UUID,
        binding_lineage_id: UUID | str,
        expected_binding_version: int,
        effective_at: datetime,
        operation_ref: str,
        reason: str,
        evidence_refs: tuple[str, ...],
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBindingLifecycleResult:
        """Revoke one current lineage at an explicit valid-time boundary."""

        return await self._run_lifecycle_operation(
            tenant_id=tenant_id,
            binding_lineage_id=UUID(str(binding_lineage_id)),
            expected_binding_version=expected_binding_version,
            effective_at=effective_at,
            operation_kind="revoke",
            operation_ref=operation_ref,
            reason=reason,
            evidence_refs=evidence_refs,
            new_canonical_ref=None,
            new_source_identity_authority_ref=None,
            new_evidence_refs=None,
            conn=conn,
        )

    async def supersede(
        self,
        *,
        tenant_id: UUID,
        binding_lineage_id: UUID | str,
        expected_binding_version: int,
        effective_at: datetime,
        operation_ref: str,
        reason: str,
        evidence_refs: tuple[str, ...],
        new_canonical_ref: dict[str, Any],
        new_source_identity_authority_ref: str,
        new_evidence_refs: tuple[str, ...],
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBindingLifecycleResult:
        """Replace one current binding while preserving its valid-time history."""

        return await self._run_lifecycle_operation(
            tenant_id=tenant_id,
            binding_lineage_id=UUID(str(binding_lineage_id)),
            expected_binding_version=expected_binding_version,
            effective_at=effective_at,
            operation_kind="supersede",
            operation_ref=operation_ref,
            reason=reason,
            evidence_refs=evidence_refs,
            new_canonical_ref=_canonical_ref(new_canonical_ref),
            new_source_identity_authority_ref=(
                new_source_identity_authority_ref.strip()
            ),
            new_evidence_refs=new_evidence_refs,
            conn=conn,
        )

    async def _run_lifecycle_operation(
        self,
        *,
        tenant_id: UUID,
        binding_lineage_id: UUID,
        expected_binding_version: int,
        effective_at: datetime,
        operation_kind: Literal["close", "revoke", "supersede"],
        operation_ref: str,
        reason: str,
        evidence_refs: tuple[str, ...],
        new_canonical_ref: dict[str, Any] | None,
        new_source_identity_authority_ref: str | None,
        new_evidence_refs: tuple[str, ...] | None,
        conn: asyncpg.Connection | None,
    ) -> SourceIdentityBindingLifecycleResult:
        operation_ref = operation_ref.strip()
        reason = reason.strip()
        if effective_at.tzinfo is None or effective_at.utcoffset() is None:
            raise ValueError("effective_at must be timezone-aware")
        if expected_binding_version < 1:
            raise ValueError("expected_binding_version must be positive")
        if not operation_ref or not reason or not evidence_refs:
            raise ValueError(
                "lifecycle operation requires ref, reason and evidence"
            )
        if operation_kind == "supersede" and (
            new_canonical_ref is None
            or not new_source_identity_authority_ref
            or not new_evidence_refs
        ):
            raise ValueError("supersede requires a complete successor binding")

        fingerprint = _lifecycle_request_fingerprint(
            operation_kind=operation_kind,
            binding_lineage_id=binding_lineage_id,
            expected_binding_version=expected_binding_version,
            effective_at=effective_at,
            reason=reason,
            evidence_refs=evidence_refs,
            new_canonical_ref=new_canonical_ref,
            new_source_identity_authority_ref=(
                new_source_identity_authority_ref
            ),
            new_evidence_refs=new_evidence_refs,
        )

        async def write(
            target: asyncpg.Connection,
        ) -> SourceIdentityBindingLifecycleResult:
            await target.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"source-binding:{tenant_id}:{binding_lineage_id}",
            )
            replay = await target.fetchrow(
                """
                SELECT *
                FROM source_identity_binding_operations
                WHERE tenant_id=$1 AND operation_ref=$2
                """,
                tenant_id,
                operation_ref,
            )
            if replay is not None:
                if replay["request_fingerprint"] != fingerprint:
                    raise ValueError(
                        "operation_ref already used for a different request"
                    )
                return await _lifecycle_result_from_operation(
                    target,
                    replay,
                    applied=False,
                )

            head = await target.fetchrow(
                """
                SELECT *
                FROM source_identity_bindings
                WHERE tenant_id=$1
                  AND lineage_id=$2
                  AND valid_to IS NULL
                  AND transaction_to IS NULL
                FOR UPDATE
                """,
                tenant_id,
                binding_lineage_id,
            )
            if head is None:
                raise ValueError("binding lineage has no current binding")
            if int(head["binding_version"]) != expected_binding_version:
                raise ValueError(
                    "stale binding version: expected current version "
                    f"{head['binding_version']}"
                )
            if effective_at <= head["valid_from"]:
                raise ValueError(
                    "effective_at must follow the current valid_from"
                )

            transaction_at = await target.fetchval(
                "SELECT transaction_timestamp()"
            )
            closed = await target.fetchrow(
                """
                UPDATE source_identity_bindings
                SET transaction_to=$4
                WHERE tenant_id=$1
                  AND id=$2
                  AND binding_version=$3
                  AND transaction_to IS NULL
                RETURNING *
                """,
                tenant_id,
                head["id"],
                expected_binding_version,
                transaction_at,
            )
            if closed is None:
                raise ValueError("binding lineage changed concurrently")

            closure_version = expected_binding_version + 1
            closure_id = uuid7()
            closure_kind = (
                "supersede_closure"
                if operation_kind == "supersede"
                else operation_kind
            )
            closure = await target.fetchrow(
                """
                INSERT INTO source_identity_bindings (
                    id, tenant_id, lineage_id, binding_version,
                    source_system, source_native_identifier,
                    source_identity_authority_ref, canonical_referent,
                    valid_from, valid_to, transaction_from, evidence_refs,
                    predecessor_binding_id, predecessor_binding_version,
                    lifecycle_operation_kind, lifecycle_operation_ref
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb,
                    $9, $10, $11, $12, $13, $14, $15, $16
                )
                RETURNING *
                """,
                closure_id,
                tenant_id,
                binding_lineage_id,
                closure_version,
                head["source_system"],
                head["source_native_identifier"],
                head["source_identity_authority_ref"],
                dict(head["canonical_referent"]),
                head["valid_from"],
                effective_at,
                transaction_at,
                list(head["evidence_refs"]),
                head["id"],
                expected_binding_version,
                closure_kind,
                operation_ref,
            )
            result_rows = [closure]

            if operation_kind == "supersede":
                successor_id = uuid7()
                successor = await target.fetchrow(
                    """
                    INSERT INTO source_identity_bindings (
                        id, tenant_id, lineage_id, binding_version,
                        source_system, source_native_identifier,
                        source_identity_authority_ref, canonical_referent,
                        valid_from, transaction_from, evidence_refs,
                        predecessor_binding_id,
                        predecessor_binding_version,
                        lifecycle_operation_kind, lifecycle_operation_ref
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8::jsonb,
                        $9, $10, $11, $12, $13,
                        'supersede_successor', $14
                    )
                    RETURNING *
                    """,
                    successor_id,
                    tenant_id,
                    binding_lineage_id,
                    closure_version + 1,
                    head["source_system"],
                    head["source_native_identifier"],
                    new_source_identity_authority_ref,
                    new_canonical_ref,
                    effective_at,
                    transaction_at,
                    list(new_evidence_refs or ()),
                    closure_id,
                    closure_version,
                    operation_ref,
                )
                result_rows.append(successor)

            result_refs = [
                {
                    "binding_id": str(row["id"]),
                    "binding_version": int(row["binding_version"]),
                }
                for row in result_rows
            ]
            operation = await target.fetchrow(
                """
                INSERT INTO source_identity_binding_operations (
                    tenant_id, operation_ref, operation_kind,
                    binding_lineage_id, expected_binding_version,
                    request_fingerprint, effective_at, transaction_at,
                    reason, evidence_refs, result_binding_refs
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11::jsonb
                )
                RETURNING *
                """,
                tenant_id,
                operation_ref,
                operation_kind,
                binding_lineage_id,
                expected_binding_version,
                fingerprint,
                effective_at,
                transaction_at,
                reason,
                list(evidence_refs),
                json.dumps(result_refs),
            )
            return await _lifecycle_result_from_operation(
                target,
                operation,
                applied=True,
                prefetched_rows=result_rows,
            )

        if conn is not None:
            return await write(conn)
        if self._pool is None:
            raise ValueError("source identity lifecycle requires a connection")
        async with self._pool.acquire() as owned, owned.transaction():
            return await write(owned)

    async def resolve_observation_source(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        phrase: str,
        valid_at: datetime,
        known_at: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> ResolvedSourceIdentityBinding | None:
        """Resolve only an authenticated Observation envelope.

        The observation must have an explicit ingestion-authorized binding
        attachment. Identifiers merely appearing in ``external_id``,
        ``content_text`` or ``entities_mentioned`` never enter this lookup.
        """

        known_at = known_at or datetime.now(timezone.utc)
        normalized_phrase = _normalize_surface(phrase)
        if not normalized_phrase:
            return None

        async def read(
            target: asyncpg.Connection,
        ) -> ResolvedSourceIdentityBinding | None:
            rows = await target.fetch(
                """
                SELECT binding.*, attachment.attachment_authority_ref,
                       attachment.source_surface
                FROM source_identity_bindings binding
                JOIN observation_source_identity_bindings attachment
                  ON attachment.tenant_id=binding.tenant_id
                 AND attachment.binding_id=binding.id
                 AND attachment.binding_version=binding.binding_version
                JOIN observations observation
                  ON observation.tenant_id=attachment.tenant_id
                 AND observation.id=attachment.observation_id
                 AND observation.occurred_at=
                   attachment.observation_occurred_at
                WHERE binding.tenant_id=$1
                  AND attachment.observation_id=$2
                  AND attachment.normalized_source_surface=$3
                  AND split_part(
                    observation.source_channel, ':', 1
                  )=binding.source_system
                  AND binding.valid_from <= $4
                  AND (
                    binding.valid_to IS NULL OR $4 < binding.valid_to
                  )
                  AND binding.transaction_from <= $5
                  AND (
                    binding.transaction_to IS NULL
                    OR $5 < binding.transaction_to
                  )
                  AND attachment.attached_at <= $5
                  AND CASE
                    WHEN binding.canonical_referent ->> 'type' = 'actor'
                    THEN EXISTS (
                      SELECT 1 FROM actors target
                      WHERE target.tenant_id=binding.tenant_id
                        AND target.id::text =
                          binding.canonical_referent ->> 'id'
                        AND target.status='active'
                    )
                    WHEN binding.canonical_referent ->> 'type'
                         IN ('resource', 'customer')
                    THEN EXISTS (
                      SELECT 1 FROM resources target
                      WHERE target.tenant_id=binding.tenant_id
                        AND target.id::text =
                          binding.canonical_referent ->> 'id'
                        AND (
                          target.archived_at IS NULL
                          OR target.archived_at > $4
                        )
                        AND (
                          binding.canonical_referent ->> 'type' <> 'customer'
                          OR target.metadata ->> 'semantic_kind' = 'customer'
                        )
                    )
                    ELSE TRUE
                  END
                ORDER BY binding.binding_version DESC,
                         binding.transaction_from DESC
                LIMIT 2
                """,
                tenant_id,
                observation_id,
                normalized_phrase,
                valid_at,
                known_at,
            )
            if len(rows) != 1:
                return None
            row = rows[0]
            return ResolvedSourceIdentityBinding(
                binding=_binding_from_row(row),
                canonical_ref=_canonical_ref(dict(row["canonical_referent"])),
                attachment_authority_ref=row[
                    "attachment_authority_ref"
                ],
                source_surface=row["source_surface"],
            )

        if conn is not None:
            return await read(conn)
        if self._pool is None:
            raise ValueError("source identity binding read requires a connection")
        async with self._pool.acquire() as owned:
            return await read(owned)

    async def find_visible_binding(
        self,
        *,
        tenant_id: UUID,
        source_system: str,
        source_native_identifier: str,
        valid_at: datetime,
        known_at: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBinding | None:
        """Find one pre-existing governed binding without creating authority."""

        if (
            not source_system
            or not source_native_identifier.startswith(f"{source_system}:")
        ):
            return None
        known_at = known_at or datetime.now(timezone.utc)

        async def read(
            target: asyncpg.Connection,
        ) -> SourceIdentityBinding | None:
            rows = await target.fetch(
                """
                SELECT binding.*
                FROM source_identity_bindings binding
                WHERE binding.tenant_id=$1
                  AND binding.source_system=$2
                  AND binding.source_native_identifier=$3
                  AND binding.valid_from <= $4
                  AND (
                    binding.valid_to IS NULL OR $4 < binding.valid_to
                  )
                  AND binding.transaction_from <= $5
                  AND (
                    binding.transaction_to IS NULL
                    OR $5 < binding.transaction_to
                  )
                  AND CASE
                    WHEN binding.canonical_referent ->> 'type' = 'actor'
                    THEN EXISTS (
                      SELECT 1 FROM actors target
                      WHERE target.tenant_id=binding.tenant_id
                        AND target.id::text =
                          binding.canonical_referent ->> 'id'
                        AND target.status='active'
                    )
                    WHEN binding.canonical_referent ->> 'type'
                         IN ('resource', 'customer')
                    THEN EXISTS (
                      SELECT 1 FROM resources target
                      WHERE target.tenant_id=binding.tenant_id
                        AND target.id::text =
                          binding.canonical_referent ->> 'id'
                        AND (
                          target.archived_at IS NULL
                          OR target.archived_at > $4
                        )
                        AND (
                          binding.canonical_referent ->> 'type' <> 'customer'
                          OR target.metadata ->> 'semantic_kind' = 'customer'
                        )
                    )
                    ELSE TRUE
                  END
                ORDER BY binding.binding_version DESC,
                         binding.transaction_from DESC
                LIMIT 2
                """,
                tenant_id,
                source_system,
                source_native_identifier,
                valid_at,
                known_at,
            )
            if len(rows) != 1:
                return None
            return _binding_from_row(rows[0])

        if conn is not None:
            return await read(conn)
        if self._pool is None:
            raise ValueError("source identity binding read requires a connection")
        async with self._pool.acquire() as owned:
            return await read(owned)

    async def find_as_of_binding(
        self,
        *,
        tenant_id: UUID,
        source_system: str,
        source_native_identifier: str,
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBinding | None:
        """Read the binding visible at one valid-time and knowledge cutoff."""

        return await self.find_visible_binding(
            tenant_id=tenant_id,
            source_system=source_system,
            source_native_identifier=source_native_identifier,
            valid_at=valid_at,
            known_at=known_at,
            conn=conn,
        )

    async def find_current_binding(
        self,
        *,
        tenant_id: UUID,
        source_system: str,
        source_native_identifier: str,
        at: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> SourceIdentityBinding | None:
        """Read the binding valid and known at the current operational time."""

        at = at or datetime.now(timezone.utc)
        return await self.find_visible_binding(
            tenant_id=tenant_id,
            source_system=source_system,
            source_native_identifier=source_native_identifier,
            valid_at=at,
            known_at=at,
            conn=conn,
        )

    async def list_bindings_for_canonical_ref(
        self,
        *,
        tenant_id: UUID,
        canonical_referent_type: str,
        canonical_referent_id: str,
        canonical_referent_version: int,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> tuple[SourceIdentityBinding, ...]:
        """List bindings whose exact canonical ref is visible at one cutoff.

        Omitting both cutoffs performs a current read. Supplying cutoffs performs
        a bitemporal as-of read. Target lifecycle is deliberately not consulted,
        so repair can still discover bindings after a referent is retired.
        """

        canonical_referent_type = canonical_referent_type.strip()
        canonical_referent_id = canonical_referent_id.strip()
        if not canonical_referent_type or not canonical_referent_id:
            raise ValueError("canonical referent type and id are required")
        if canonical_referent_version < 1:
            raise ValueError("canonical referent version must be positive")
        if (valid_at is None) != (known_at is None):
            raise ValueError(
                "valid_at and known_at must both be provided for an as-of read"
            )
        if valid_at is None:
            valid_at = known_at = datetime.now(timezone.utc)
        assert known_at is not None
        for field_name, value in (
            ("valid_at", valid_at),
            ("known_at", known_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")

        async def read(
            target: asyncpg.Connection,
        ) -> tuple[SourceIdentityBinding, ...]:
            rows = await target.fetch(
                """
                SELECT binding.*
                FROM source_identity_bindings binding
                WHERE binding.tenant_id=$1
                  AND binding.canonical_referent ->> 'type'=$2
                  AND binding.canonical_referent ->> 'id'=$3
                  AND COALESCE(
                    (binding.canonical_referent ->> 'version')::integer,
                    1
                  )=$4
                  AND binding.valid_from <= $5
                  AND (
                    binding.valid_to IS NULL OR $5 < binding.valid_to
                  )
                  AND binding.transaction_from <= $6
                  AND (
                    binding.transaction_to IS NULL
                    OR $6 < binding.transaction_to
                  )
                ORDER BY
                  binding.source_system,
                  binding.source_native_identifier,
                  binding.lineage_id,
                  binding.binding_version,
                  binding.transaction_from,
                  binding.id
                """,
                tenant_id,
                canonical_referent_type,
                canonical_referent_id,
                canonical_referent_version,
                valid_at,
                known_at,
            )
            return tuple(_binding_from_row(row) for row in rows)

        if conn is not None:
            return await read(conn)
        if self._pool is None:
            raise ValueError("source identity binding read requires a connection")
        async with self._pool.acquire() as owned:
            return await read(owned)

    async def attach_to_observation(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        binding: SourceIdentityBinding,
        source_surface: str,
        attachment_authority_ref: str,
        conn: asyncpg.Connection | None = None,
    ) -> None:
        """Attach ingestion-authenticated source identity to one Observation."""

        if binding.tenant_id != tenant_id:
            raise ValueError("source binding tenant does not match observation tenant")
        binding_lineage_id = UUID(
            binding.binding_lineage_id or binding.binding_id
        )
        normalized_surface = _normalize_surface(source_surface)
        if not normalized_surface:
            raise ValueError("source identity attachment surface is empty")

        async def write(target: asyncpg.Connection) -> None:
            observation = await target.fetchrow(
                """
                SELECT split_part(source_channel, ':', 1) AS source_system,
                       occurred_at
                FROM observations
                WHERE tenant_id=$1 AND id=$2
                """,
                tenant_id,
                observation_id,
            )
            if observation is None:
                raise ValueError("source identity attachment observation is missing")
            if observation["source_system"] != binding.source_system:
                raise ValueError(
                    "source identity binding system does not match observation source"
                )
            inserted = await target.fetchrow(
                """
                INSERT INTO observation_source_identity_bindings (
                    tenant_id, observation_id, observation_occurred_at,
                    binding_id, binding_version, binding_lineage_id,
                    source_surface,
                    normalized_source_surface, attachment_authority_ref
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (
                    tenant_id, observation_id, observation_occurred_at,
                    binding_lineage_id, normalized_source_surface
                ) DO NOTHING
                RETURNING binding_id
                """,
                tenant_id,
                observation_id,
                observation["occurred_at"],
                UUID(binding.binding_id),
                binding.binding_version,
                binding_lineage_id,
                source_surface,
                normalized_surface,
                attachment_authority_ref,
            )
            if inserted is not None:
                return
            existing = await target.fetchrow(
                """
                SELECT binding_id, binding_version, source_surface,
                       attachment_authority_ref
                FROM observation_source_identity_bindings
                WHERE tenant_id=$1
                  AND observation_id=$2
                  AND observation_occurred_at=$3
                  AND binding_lineage_id=$4
                  AND normalized_source_surface=$5
                """,
                tenant_id,
                observation_id,
                observation["occurred_at"],
                binding_lineage_id,
                normalized_surface,
            )
            if existing is None:
                raise RuntimeError(
                    "source identity attachment conflict vanished"
                )
            if (
                existing["binding_id"] != UUID(binding.binding_id)
                or int(existing["binding_version"])
                != binding.binding_version
                or existing["source_surface"] != source_surface
                or existing["attachment_authority_ref"]
                != attachment_authority_ref
            ):
                raise ValueError(
                    "observation source identity lineage is already attached "
                    "at a different binding version or authority"
                )

        if conn is not None:
            await write(conn)
            return
        if self._pool is None:
            raise ValueError("source identity attachment requires a connection")
        async with self._pool.acquire() as owned, owned.transaction():
            await write(owned)


def _canonical_ref(value: dict[str, Any]) -> dict[str, Any]:
    entity_type = str(value.get("type") or "").strip()
    entity_id = str(value.get("id") or "").strip()
    version = int(value.get("version", 1))
    if not entity_type or not entity_id or version < 1:
        raise ValueError("canonical_ref requires type, id and positive version")
    return {"type": entity_type, "id": entity_id, "version": version}


def _normalize_surface(value: str) -> str:
    """Match entity-grounding exact-surface casefold/whitespace semantics."""

    return " ".join(value.casefold().split())


def _lifecycle_request_fingerprint(
    *,
    operation_kind: str,
    binding_lineage_id: UUID,
    expected_binding_version: int,
    effective_at: datetime,
    reason: str,
    evidence_refs: tuple[str, ...],
    new_canonical_ref: dict[str, Any] | None,
    new_source_identity_authority_ref: str | None,
    new_evidence_refs: tuple[str, ...] | None,
) -> str:
    payload = {
        "operation_kind": operation_kind,
        "binding_lineage_id": str(binding_lineage_id),
        "expected_binding_version": expected_binding_version,
        "effective_at": effective_at.isoformat(),
        "reason": reason,
        "evidence_refs": list(evidence_refs),
        "new_canonical_ref": new_canonical_ref,
        "new_source_identity_authority_ref": (
            new_source_identity_authority_ref
        ),
        "new_evidence_refs": list(new_evidence_refs or ()),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


async def _lifecycle_result_from_operation(
    conn: asyncpg.Connection,
    operation: asyncpg.Record,
    *,
    applied: bool,
    prefetched_rows: list[asyncpg.Record] | None = None,
) -> SourceIdentityBindingLifecycleResult:
    refs = operation["result_binding_refs"]
    if isinstance(refs, str):
        refs = json.loads(refs)
    rows = prefetched_rows or []
    if not rows:
        for ref in refs:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM source_identity_bindings
                WHERE tenant_id=$1
                  AND id=$2
                  AND binding_version=$3
                """,
                operation["tenant_id"],
                UUID(str(ref["binding_id"])),
                int(ref["binding_version"]),
            )
            if row is None:
                raise RuntimeError(
                    "binding lifecycle operation result is missing"
                )
            rows.append(row)
    return SourceIdentityBindingLifecycleResult(
        operation_kind=operation["operation_kind"],
        operation_ref=operation["operation_ref"],
        binding_lineage_id=str(operation["binding_lineage_id"]),
        prior_binding_version=int(operation["expected_binding_version"]),
        result_bindings=tuple(_binding_from_row(row) for row in rows),
        applied=applied,
        effective_at=operation["effective_at"],
        transaction_at=operation["transaction_at"],
    )


def _binding_from_row(row: asyncpg.Record) -> SourceIdentityBinding:
    canonical_ref = _canonical_ref(dict(row["canonical_referent"]))
    return SourceIdentityBinding(
        binding_id=str(row["id"]),
        binding_version=int(row["binding_version"]),
        binding_lineage_id=str(row["lineage_id"]),
        tenant_id=row["tenant_id"],
        source_system=row["source_system"],
        source_native_identifier=row["source_native_identifier"],
        source_identity_authority_ref=row[
            "source_identity_authority_ref"
        ],
        canonical_referent_type=canonical_ref["type"],
        canonical_referent_id=canonical_ref["id"],
        canonical_referent_version=canonical_ref["version"],
        temporal_scope=BitemporalInterval(
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            transaction_from=row["transaction_from"],
            transaction_to=row["transaction_to"],
        ),
        evidence_refs=tuple(row["evidence_refs"]),
    )


__all__ = [
    "ResolvedSourceIdentityBinding",
    "SourceIdentityBindingLifecycleResult",
    "SourceIdentityBindingRepo",
]
