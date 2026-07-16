"""Persistence for authenticated source-native identity mappings."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
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
            row = await target.fetchrow(
                """
                INSERT INTO source_identity_bindings (
                    id, tenant_id, binding_version, source_system,
                    source_native_identifier, source_identity_authority_ref,
                    canonical_referent, valid_from, transaction_from,
                    evidence_refs
                ) VALUES (
                    $1, $2, 1, $3, $4, $5, $6::jsonb, $7, $8, $9
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
                        AND target.archived_at IS NULL
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
            await target.execute(
                """
                INSERT INTO observation_source_identity_bindings (
                    tenant_id, observation_id, observation_occurred_at,
                    binding_id, binding_version, source_surface,
                    normalized_source_surface, attachment_authority_ref
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT DO NOTHING
                """,
                tenant_id,
                observation_id,
                observation["occurred_at"],
                UUID(binding.binding_id),
                binding.binding_version,
                source_surface,
                normalized_surface,
                attachment_authority_ref,
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


def _binding_from_row(row: asyncpg.Record) -> SourceIdentityBinding:
    canonical_ref = _canonical_ref(dict(row["canonical_referent"]))
    return SourceIdentityBinding(
        binding_id=str(row["id"]),
        binding_version=int(row["binding_version"]),
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
    "SourceIdentityBindingRepo",
]
