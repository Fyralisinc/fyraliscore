"""
services/actors/repo.py — Actors + actor_identity_mappings repository.

Schema refs (SCHEMA-LOCK.md):
  - S5.1 `actors` table
  - S5.2 `actor_identity_mappings` table
  - S5.3 indexes + enum values

Public API (per BUILD-PLAN §2 Prompt 1-B, with Q5 + Q6 resolutions applied):

  - create_actor(email, display_name, type, tenant_id, nexus_attested=False)
        type is Literal["human_internal", "human_external", "ai_agent"]
        (Q5 resolution: three-value enum, NOT 'human'|'agent'.)

  - add_identity_mapping(actor_id, source_channel, source_actor_ref,
                         confidence=1.0)
        Column names per S5.2 (Q6: NOT source_system / external_id).

  - resolve_by_source_actor_ref(ref: "<channel>:<external_ref>")
        Splits on the first ':' and looks up the mapping. Returns the
        canonical actor_id or None.

  - list_active_actors(tenant_id) -> list[ActorRow]
        Actors with status == 'active'.

  - deactivate(actor_id, reason) -> ActorRow
        Soft delete: sets status='inactive' and records the reason in
        metadata.deactivation_reason. Invariant: fails with
        InvariantViolation if the actor owns any commitment whose
        state is not in ('proposed', 'doneverified', 'closed').

No ORM. Plain asyncpg + the shared typed helpers from lib.shared.db.
UUID v7 from lib.shared.ids for every new actor row.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ActorIdentityMappingRow, ActorRow, ActorType


# Mirrors lib.shared.types.ActorType; kept explicit so the repo can
# validate the parameter before hitting the DB (clearer error message
# than a CHECK-constraint violation, and lets tests assert on
# ValidationError without waiting for asyncpg).
_LEGAL_ACTOR_TYPES: frozenset[str] = frozenset(
    ("human_internal", "human_external", "ai_agent")
)

# Commitment states that count as "active" for the deactivation
# invariant per BUILD-PLAN §2 Prompt 1-B. Anything not in this set
# (proposed, doneverified, closed) is still-in-flight and blocks
# deactivation.
_COMMITMENT_TERMINAL_OR_PROPOSED = ("proposed", "doneverified", "closed")


class ActorRepo:
    """Thin stateless repository bound to a connection pool."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -----------------------------------------------------------------
    # create_actor
    # -----------------------------------------------------------------
    async def create_actor(
        self,
        *,
        email: str | None,
        display_name: str,
        type: ActorType,
        tenant_id: UUID,
        nexus_attested: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> ActorRow:
        """
        Insert a new actor row and return it.

        Validates `type` locally so a wrong enum value raises
        ValidationError (deterministic, before touching the DB) rather
        than bubbling up a constraint error. `nexus_attested` is
        recorded into metadata for downstream access-control.
        """
        if type not in _LEGAL_ACTOR_TYPES:
            raise ValidationError(
                f"invalid actor type {type!r}; "
                f"must be one of {sorted(_LEGAL_ACTOR_TYPES)}",
                field="type",
                value=type,
            )
        if not display_name or not display_name.strip():
            raise ValidationError(
                "display_name must be non-empty",
                field="display_name",
            )

        actor_id = uuid7()
        md: dict[str, Any] = dict(metadata or {})
        # Record Nexus attestation flag in metadata so downstream
        # access-control / trust decisions have a place to look. Real
        # attestation payloads land here in Phase 4.
        md["nexus_attested"] = bool(nexus_attested)

        row = await self._pool.fetchrow(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, email,
                status, metadata, specification_id,
                created_at, last_seen_at
            ) VALUES (
                $1, $2, $3, $4, $5,
                'active', $6::jsonb, NULL,
                now(), NULL
            )
            RETURNING
                id, tenant_id, type, display_name, email, status,
                metadata, specification_id, created_at, last_seen_at
            """,
            actor_id,
            tenant_id,
            type,
            display_name,
            email,
            _jsonb(md),
        )
        assert row is not None  # INSERT ... RETURNING always yields a row
        return _hydrate_actor(row)

    # -----------------------------------------------------------------
    # add_identity_mapping
    # -----------------------------------------------------------------
    async def add_identity_mapping(
        self,
        *,
        actor_id: UUID,
        source_channel: str,
        source_actor_ref: str,
        confidence: float = 1.0,
    ) -> ActorIdentityMappingRow:
        """
        Insert a (source_channel, source_actor_ref) -> actor_id mapping.

        The (source_channel, source_actor_ref) pair is the table's
        primary key (S5.2), so a duplicate raises a clear
        ValidationError rather than the raw UniqueViolation.
        """
        if not source_channel or ":" in source_channel and source_channel.startswith(":"):
            raise ValidationError(
                "source_channel must be non-empty",
                field="source_channel",
            )
        if not source_actor_ref:
            raise ValidationError(
                "source_actor_ref must be non-empty",
                field="source_actor_ref",
            )
        if not (0.0 <= confidence <= 1.0):
            raise ValidationError(
                f"confidence must be in [0,1]; got {confidence}",
                field="confidence",
                value=confidence,
            )

        try:
            row = await self._pool.fetchrow(
                """
                INSERT INTO actor_identity_mappings (
                    actor_id, source_channel, source_actor_ref,
                    confidence, created_at
                ) VALUES ($1, $2, $3, $4, now())
                RETURNING actor_id, source_channel, source_actor_ref,
                          confidence, created_at
                """,
                actor_id,
                source_channel,
                source_actor_ref,
                confidence,
            )
        except asyncpg.exceptions.UniqueViolationError as e:
            raise ValidationError(
                f"identity mapping already exists for "
                f"{source_channel}:{source_actor_ref}",
                source_channel=source_channel,
                source_actor_ref=source_actor_ref,
                conflict=str(e),
            ) from e
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            raise ValidationError(
                f"actor_id {actor_id} does not exist",
                actor_id=str(actor_id),
                conflict=str(e),
            ) from e

        assert row is not None
        return ActorIdentityMappingRow.model_validate(dict(row))

    # -----------------------------------------------------------------
    # resolve_by_source_actor_ref
    # -----------------------------------------------------------------
    async def resolve_by_source_actor_ref(self, ref: str) -> UUID | None:
        """
        Resolve a `<channel>:<external_ref>` string to an actor_id.

        Format per BUILD-PLAN 1-B: "slack:U01ALICE" → actor_id.
        The channel is everything before the first colon; the ref is
        everything after. This mirrors §14's `source_channel` format
        and is tolerant of refs that contain additional colons.
        Returns None when no mapping exists.
        """
        if not ref or ":" not in ref:
            raise ValidationError(
                f"ref must be '<channel>:<external_ref>'; got {ref!r}",
                field="ref",
                value=ref,
            )
        source_channel, _, source_actor_ref = ref.partition(":")
        if not source_channel or not source_actor_ref:
            raise ValidationError(
                f"ref must have non-empty channel and ref; got {ref!r}",
                field="ref",
                value=ref,
            )
        val = await self._pool.fetchval(
            """
            SELECT actor_id FROM actor_identity_mappings
            WHERE source_channel = $1 AND source_actor_ref = $2
            """,
            source_channel,
            source_actor_ref,
        )
        return val  # asyncpg returns UUID or None

    # -----------------------------------------------------------------
    # list_active_actors
    # -----------------------------------------------------------------
    async def list_active_actors(self, tenant_id: UUID) -> list[ActorRow]:
        """
        Return every actor in `tenant_id` whose status == 'active'.

        Uses the `actors_type_idx (tenant_id, type, status)` index for
        the tenant+status filter.
        """
        rows = await self._pool.fetch(
            """
            SELECT id, tenant_id, type, display_name, email, status,
                   metadata, specification_id, created_at, last_seen_at
            FROM actors
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY created_at ASC, id ASC
            """,
            tenant_id,
        )
        return [_hydrate_actor(r) for r in rows]

    # -----------------------------------------------------------------
    # deactivate
    # -----------------------------------------------------------------
    async def deactivate(self, actor_id: UUID, reason: str) -> ActorRow:
        """
        Soft-delete: set status='inactive', write the reason into
        metadata.deactivation_reason, touch last_seen_at.

        Invariant (BUILD-PLAN 1-B): if the actor owns any commitment
        whose state is NOT in ('proposed', 'doneverified', 'closed'),
        raise InvariantViolation with the blocking commitment ids in
        the context dict. Check + update happen in the same transaction
        so the invariant cannot race a concurrent commitment transition.
        """
        if not reason or not reason.strip():
            raise ValidationError(
                "deactivation reason must be non-empty",
                field="reason",
            )

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Lock the actor row so a concurrent deactivation
                # serialises through here.
                existing = await conn.fetchrow(
                    """
                    SELECT id, status FROM actors
                    WHERE id = $1 FOR UPDATE
                    """,
                    actor_id,
                )
                if existing is None:
                    raise ValidationError(
                        f"actor {actor_id} not found",
                        actor_id=str(actor_id),
                    )

                # Invariant: no active commitments. BUILD-PLAN 1-B
                # dictates the exact SQL (excluded states). The LIMIT
                # 5 caps the context dict while still showing more than
                # one offender so debugging is easy.
                blockers = await conn.fetch(
                    """
                    SELECT id, state, title FROM commitments
                    WHERE owner_id = $1
                      AND state NOT IN ('proposed', 'doneverified', 'closed')
                    ORDER BY id
                    LIMIT 5
                    """,
                    actor_id,
                )
                if blockers:
                    raise InvariantViolation(
                        invariant="actor_deactivation_no_active_commitments",
                        message=(
                            f"cannot deactivate actor {actor_id}: "
                            f"{len(blockers)} active commitment(s) still owned"
                        ),
                        actor_id=str(actor_id),
                        active_commitments=[
                            {
                                "id": str(b["id"]),
                                "state": b["state"],
                                "title": b["title"],
                            }
                            for b in blockers
                        ],
                    )

                row = await conn.fetchrow(
                    """
                    UPDATE actors
                    SET status = 'inactive',
                        last_seen_at = now(),
                        metadata = COALESCE(metadata, '{}'::jsonb)
                                   || jsonb_build_object(
                                        'deactivation_reason', $2::text,
                                        'deactivated_at', now()::text
                                      )
                    WHERE id = $1
                    RETURNING id, tenant_id, type, display_name, email,
                              status, metadata, specification_id,
                              created_at, last_seen_at
                    """,
                    actor_id,
                    reason,
                )
                assert row is not None
                return _hydrate_actor(row)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _hydrate_actor(row: asyncpg.Record) -> ActorRow:
    """
    Convert an asyncpg Record to ActorRow. asyncpg does not install a
    JSONB codec by default, so `metadata` arrives as a string. Parse
    it once here so Pydantic sees a dict.
    """
    import json

    d = dict(row)
    md = d.get("metadata")
    if isinstance(md, str):
        d["metadata"] = json.loads(md)
    return ActorRow.model_validate(d)


def _jsonb(value: Any) -> str:
    """
    asyncpg serialises dict/list parameters as JSON automatically only
    when the parameter is typed as json/jsonb by the query. Our INSERTs
    use the `::jsonb` cast, so we must pass a JSON string — not a dict
    — to avoid asyncpg trying to encode the dict with its default codec
    (which will fail for unregistered types). We stringify here so the
    repo doesn't have to register codecs on every connection.
    """
    import json

    return json.dumps(value)


__all__ = ["ActorRepo"]
