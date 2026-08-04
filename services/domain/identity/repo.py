"""Auditable and reversible identity decisions.

The current actor/alias tables are projections. This ledger is the authority
for candidate, negative, accepted, rejected, merge, and split history.
"""

from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .models import IdentityAssertionCreate, IdentityAssertionRow


_COLUMNS = (
    "id", "tenant_id", "source_identity_key", "source_identity_ref",
    "candidate_entity_ref", "assertion_kind", "status", "confidence",
    "evidence_id", "decision_provenance", "valid_from", "valid_to",
    "version", "supersedes_assertion_id", "created_at", "decided_at",
)
_SELECT = ", ".join(_COLUMNS)


def _object(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _hydrate(row: asyncpg.Record) -> IdentityAssertionRow:
    value = dict(row)
    for key in ("source_identity_ref", "candidate_entity_ref", "decision_provenance"):
        value[key] = _object(value[key])
    return IdentityAssertionRow.model_validate(value)


class IdentityAssertionRepository:
    async def propose(
        self,
        value: IdentityAssertionCreate,
        *,
        conn: asyncpg.Connection,
    ) -> IdentityAssertionRow:
        async with conn.transaction():
            return await self._propose_in_transaction(value, conn=conn)

    async def _propose_in_transaction(
        self,
        value: IdentityAssertionCreate,
        *,
        conn: asyncpg.Connection,
    ) -> IdentityAssertionRow:
        if not value.source_identity_ref or not value.candidate_entity_ref:
            raise ValidationError("identity refs must be non-empty objects")
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"{value.tenant_id}:{value.source_identity_key}",
        )
        duplicate = await conn.fetchrow(
            f"""
            SELECT {_SELECT} FROM identity_assertions
             WHERE tenant_id = $1 AND source_identity_key = $2
               AND candidate_entity_ref = $3::jsonb
               AND assertion_kind = $4
               AND status IN ('proposed', 'accepted')
             ORDER BY version DESC LIMIT 1
            """,
            value.tenant_id,
            value.source_identity_key,
            json.dumps(value.candidate_entity_ref, sort_keys=True),
            value.assertion_kind,
        )
        if duplicate is not None:
            return _hydrate(duplicate)
        version = await conn.fetchval(
            """
            SELECT COALESCE(max(version), 0) + 1
              FROM identity_assertions
             WHERE tenant_id = $1 AND source_identity_key = $2
            """,
            value.tenant_id,
            value.source_identity_key,
        )
        row = await conn.fetchrow(
            f"""
            INSERT INTO identity_assertions (
              id, tenant_id, source_identity_key, source_identity_ref,
              candidate_entity_ref, assertion_kind, status, confidence,
              evidence_id, decision_provenance, valid_from, version
            ) VALUES (
              $1, $2, $3, $4::jsonb, $5::jsonb, $6, 'proposed', $7,
              $8, $9::jsonb, COALESCE($10, now()), $11
            ) RETURNING {_SELECT}
            """,
            uuid7(), value.tenant_id, value.source_identity_key,
            json.dumps(value.source_identity_ref, sort_keys=True),
            json.dumps(value.candidate_entity_ref, sort_keys=True),
            value.assertion_kind, value.confidence, value.evidence_id,
            json.dumps(value.decision_provenance, sort_keys=True),
            value.valid_from, version,
        )
        assert row is not None
        return _hydrate(row)

    async def _project_actor_mapping(
        self,
        assertion: IdentityAssertionRow,
        *,
        conn: asyncpg.Connection,
    ) -> None:
        source = assertion.source_identity_ref
        candidate = assertion.candidate_entity_ref
        if source.get("kind") != "source_actor" or candidate.get("type") != "actor":
            return
        try:
            actor_id = UUID(str(candidate["id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError("accepted actor identity has an invalid actor ref") from exc
        actor_tenant = await conn.fetchval(
            "SELECT tenant_id FROM actors WHERE id = $1", actor_id
        )
        if actor_tenant != assertion.tenant_id:
            raise ValidationError("accepted actor identity crosses tenant boundary")

        source_channel = str(source.get("source_channel") or "")
        source_actor_ref = str(source.get("source_actor_ref") or "")
        installation_scope = str(source.get("installation_scope") or "")
        if not source_channel or not source_actor_ref or not installation_scope:
            raise ValidationError("accepted source actor identity is incomplete")
        installation_raw = source.get("connector_installation_id")
        installation_id = UUID(str(installation_raw)) if installation_raw else None
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings (
              actor_id, tenant_id, connector_installation_id,
              installation_scope, source_channel, source_actor_ref,
              confidence, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            ON CONFLICT (
              tenant_id, installation_scope, source_channel, source_actor_ref
            ) DO UPDATE SET actor_id = EXCLUDED.actor_id,
                            confidence = EXCLUDED.confidence,
                            connector_installation_id = EXCLUDED.connector_installation_id
            """,
            actor_id,
            assertion.tenant_id,
            installation_id,
            installation_scope,
            source_channel,
            source_actor_ref,
            assertion.confidence,
        )

    async def decide(
        self,
        assertion_id: UUID,
        *,
        tenant_id: UUID,
        decision: Literal["accepted", "rejected"],
        provenance: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> IdentityAssertionRow:
        async with conn.transaction():
            return await self._decide_in_transaction(
                assertion_id,
                tenant_id=tenant_id,
                decision=decision,
                provenance=provenance,
                conn=conn,
            )

    async def _decide_in_transaction(
        self,
        assertion_id: UUID,
        *,
        tenant_id: UUID,
        decision: Literal["accepted", "rejected"],
        provenance: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> IdentityAssertionRow:
        current = await conn.fetchrow(
            f"SELECT {_SELECT} FROM identity_assertions "
            "WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            assertion_id,
            tenant_id,
        )
        if current is None:
            raise ValidationError("identity assertion not found")
        assertion = _hydrate(current)
        if assertion.status != "proposed":
            raise ValidationError(
                "only proposed identity assertions can be decided",
                status=assertion.status,
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"{tenant_id}:{assertion.source_identity_key}",
        )
        supersedes_id = None
        if decision == "accepted" and assertion.assertion_kind == "same_as":
            supersedes_id = await conn.fetchval(
                """
                SELECT id FROM identity_assertions
                 WHERE tenant_id = $1 AND source_identity_key = $2
                   AND status = 'accepted' AND id <> $3
                 ORDER BY version DESC LIMIT 1
                """,
                tenant_id,
                assertion.source_identity_key,
                assertion_id,
            )
            await conn.execute(
                """
                UPDATE identity_assertions
                   SET status = 'superseded', valid_to = now()
                 WHERE tenant_id = $1 AND source_identity_key = $2
                   AND status = 'accepted' AND id <> $3
                """,
                tenant_id,
                assertion.source_identity_key,
                assertion_id,
            )
        row = await conn.fetchrow(
            f"""
            UPDATE identity_assertions
               SET status = $3, decided_at = now(),
                   supersedes_assertion_id = $4,
                   decision_provenance = decision_provenance || $5::jsonb
             WHERE id = $1 AND tenant_id = $2
            RETURNING {_SELECT}
            """,
            assertion_id,
            tenant_id,
            decision,
            supersedes_id,
            json.dumps(provenance, sort_keys=True),
        )
        assert row is not None
        decided = _hydrate(row)
        if decision == "accepted" and assertion.assertion_kind == "same_as":
            await self._project_actor_mapping(decided, conn=conn)
        return decided

    async def current_candidates(
        self,
        *,
        tenant_id: UUID,
        source_identity_key: str,
        conn: asyncpg.Connection,
    ) -> list[IdentityAssertionRow]:
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT} FROM identity_assertions
             WHERE tenant_id = $1 AND source_identity_key = $2
               AND status IN ('proposed', 'accepted')
             ORDER BY status, confidence DESC, version DESC
            """,
            tenant_id,
            source_identity_key,
        )
        return [_hydrate(row) for row in rows]

    async def record_cluster_event(
        self,
        *,
        tenant_id: UUID,
        event_kind: Literal["merge", "split", "relabel"],
        before_refs: list[dict[str, Any]],
        after_refs: list[dict[str, Any]],
        evidence_ids: list[UUID],
        provenance: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> UUID:
        if not before_refs or not after_refs:
            raise ValidationError("cluster changes require before and after refs")
        event_id = uuid7()
        await conn.execute(
            """
            INSERT INTO identity_cluster_events (
              id, tenant_id, event_kind, before_refs, after_refs,
              evidence_ids, provenance
            ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7::jsonb)
            """,
            event_id, tenant_id, event_kind,
            json.dumps(before_refs, sort_keys=True),
            json.dumps(after_refs, sort_keys=True),
            evidence_ids,
            json.dumps(provenance, sort_keys=True),
        )
        return event_id

    async def list_dependents(
        self,
        assertion_ids: list[UUID],
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT identity_assertion_id, dependent_kind, dependent_id,
                   registered_at
              FROM identity_dependents
             WHERE tenant_id = $1
               AND identity_assertion_id = ANY($2::uuid[])
             ORDER BY dependent_kind, dependent_id
            """,
            tenant_id,
            assertion_ids,
        )
        return [dict(row) for row in rows]

    async def register_dependent(
        self,
        assertion_id: UUID,
        *,
        tenant_id: UUID,
        dependent_kind: Literal["observation", "claim", "topic", "episode_membership"],
        dependent_id: UUID,
        conn: asyncpg.Connection,
    ) -> None:
        inserted = await conn.execute(
            """
            INSERT INTO identity_dependents (
              tenant_id, identity_assertion_id, dependent_kind, dependent_id
            )
            SELECT $1, id, $3, $4 FROM identity_assertions
             WHERE id = $2 AND tenant_id = $1
            ON CONFLICT DO NOTHING
            """,
            tenant_id,
            assertion_id,
            dependent_kind,
            dependent_id,
        )
        if inserted == "INSERT 0 0":
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                  SELECT 1 FROM identity_dependents
                   WHERE tenant_id = $1 AND identity_assertion_id = $2
                     AND dependent_kind = $3 AND dependent_id = $4
                )
                """,
                tenant_id,
                assertion_id,
                dependent_kind,
                dependent_id,
            )
            if not exists:
                raise ValidationError("identity assertion not found for dependent")
