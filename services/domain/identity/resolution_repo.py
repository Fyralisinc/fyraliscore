"""Candidate retrieval and persistence for the resolution engine."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import normalize_phrase

from .foundation import EntityMentionRow
from .resolution import (
    CandidateSeed,
    IdentityConstraintCreate,
    IdentityConstraintValue,
    IdentityResolutionSnapshot,
    RankedCandidate,
)


class CandidateProvider(Protocol):
    async def candidates_for(
        self,
        mention: EntityMentionRow,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[CandidateSeed]: ...


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


class PostgresCandidateProvider:
    """High-recall retrieval; ranking and acceptance remain pure policy."""

    async def candidates_for(
        self,
        mention: EntityMentionRow,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[CandidateSeed]:
        seeds: list[CandidateSeed] = []
        if mention.source_reference_id is not None:
            row = await conn.fetchrow(
                """
                SELECT id, reference_kind, attributes, installation_scope, source
                  FROM identity_source_references
                 WHERE id = $1 AND tenant_id = $2 AND status = 'active'
                """,
                mention.source_reference_id,
                tenant_id,
            )
            if row is not None:
                attributes = _json(row["attributes"])
                mapped_actor = await self._mapped_actor(
                    tenant_id=tenant_id,
                    source=str(row["source"]),
                    installation_scope=str(row["installation_scope"]),
                    attributes=attributes,
                    conn=conn,
                )
                if mapped_actor is not None:
                    seeds.append(
                        CandidateSeed(
                            candidate_ref={"type": "actor", "id": str(mapped_actor)},
                            retrieval_method="accepted_principal_mapping",
                            features={"accepted_mapping": 1.0, "type_compatibility": 1.0},
                            expected_type="person",
                        )
                    )
                if mapped_actor is None:
                    expected_by_kind = {
                        "principal": "person",
                        "artifact": "document",
                        "work_record": "work_item",
                        "scheduled_event": "meeting",
                    }
                    seeds.append(
                        CandidateSeed(
                            candidate_ref={
                                "type": "source_reference",
                                "id": str(row["id"]),
                                "reference_kind": str(row["reference_kind"]),
                            },
                            retrieval_method="deterministic_source_ref",
                            features={
                                "direct_source_identity": 1.0,
                                "type_compatibility": 1.0,
                            },
                            expected_type=expected_by_kind.get(
                                str(row["reference_kind"])
                            ),
                        )
                    )

        provided = mention.context.get("provided_candidate_ref")
        if isinstance(provided, dict) and provided:
            seeds.append(
                CandidateSeed(
                    candidate_ref=provided,
                    retrieval_method="structured_hint",
                    features={"provided_reference": 1.0, "type_compatibility": 1.0},
                    expected_type=(mention.expected_types[0] if mention.expected_types else None),
                )
            )

        seeds.extend(await self._alias_candidates(mention, tenant_id=tenant_id, conn=conn))
        if "person" in mention.expected_types:
            seeds.extend(await self._actor_candidates(mention, tenant_id=tenant_id, conn=conn))
        return seeds

    async def _mapped_actor(
        self,
        *,
        tenant_id: UUID,
        source: str,
        installation_scope: str,
        attributes: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> UUID | None:
        source_actor_ref = attributes.get("source_actor_ref")
        source_channel = attributes.get("source_channel") or source
        if not source_actor_ref:
            return None
        return await conn.fetchval(
            """
            SELECT actor_id FROM actor_identity_mappings
             WHERE tenant_id = $1 AND installation_scope = $2
               AND source_channel = $3 AND source_actor_ref = $4
            """,
            tenant_id,
            installation_scope,
            str(source_channel),
            str(source_actor_ref),
        )

    async def _alias_candidates(
        self,
        mention: EntityMentionRow,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[CandidateSeed]:
        normalized = normalize_phrase(mention.text)
        if not normalized:
            return []
        rows = await conn.fetch(
            """
            SELECT resolved_entity_ref, confidence,
                   regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = $2 AS exact,
                   similarity(alias_text, $3) AS name_similarity
              FROM entity_aliases
             WHERE tenant_id = $1
               AND (
                 regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = $2
                 OR similarity(alias_text, $3) >= 0.25
               )
             ORDER BY exact DESC, name_similarity DESC, confidence DESC
             LIMIT 20
            """,
            tenant_id,
            normalized,
            mention.text,
        )
        return [
            CandidateSeed(
                candidate_ref=_json(row["resolved_entity_ref"]),
                retrieval_method="exact_alias" if row["exact"] else "fuzzy_alias",
                features={
                    "exact_alias": 1.0 if row["exact"] else 0.0,
                    "alias_confidence": float(row["confidence"]),
                    "name_similarity": float(row["name_similarity"]),
                    "type_compatibility": 1.0,
                },
                expected_type=(mention.expected_types[0] if mention.expected_types else None),
            )
            for row in rows
        ]

    async def _actor_candidates(
        self,
        mention: EntityMentionRow,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[CandidateSeed]:
        rows = await conn.fetch(
            """
            SELECT id, display_name, email,
                   GREATEST(
                     similarity(display_name, $2),
                     CASE WHEN lower(COALESCE(email, '')) = lower($2) THEN 1.0 ELSE 0.0 END
                   ) AS name_similarity
              FROM actors
             WHERE tenant_id = $1 AND status = 'active'
               AND (
                 similarity(display_name, $2) >= 0.25
                 OR lower(COALESCE(email, '')) = lower($2)
               )
             ORDER BY name_similarity DESC, id
             LIMIT 10
            """,
            tenant_id,
            mention.text,
        )
        return [
            CandidateSeed(
                candidate_ref={"type": "actor", "id": str(row["id"])},
                retrieval_method="actor_name",
                features={
                    "name_similarity": float(row["name_similarity"]),
                    "type_compatibility": 1.0,
                },
                expected_type="person",
            )
            for row in rows
        ]


class IdentityResolutionRepository:
    async def active_constraints(
        self, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> list[IdentityConstraintValue]:
        rows = await conn.fetch(
            """
            SELECT id, constraint_kind, left_ref, right_ref, authority,
                   valid_from, valid_to
              FROM identity_constraints
             WHERE tenant_id = $1 AND status = 'active'
             ORDER BY created_at, id
            """,
            tenant_id,
        )
        return [
            IdentityConstraintValue(
                id=row["id"],
                kind=row["constraint_kind"],
                left_ref=_json(row["left_ref"]),
                right_ref=_json(row["right_ref"]),
                authority=row["authority"],
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
            )
            for row in rows
        ]

    async def record_constraint(
        self, value: IdentityConstraintCreate, *, conn: asyncpg.Connection
    ) -> IdentityConstraintValue:
        row = await conn.fetchrow(
            """
            INSERT INTO identity_constraints (
              id, tenant_id, constraint_kind, left_ref, right_ref, authority,
              evidence_id, provenance, valid_from, valid_to
            ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb, $9, $10)
            ON CONFLICT (tenant_id, constraint_kind, left_ref, right_ref, valid_from)
            DO UPDATE SET id = identity_constraints.id
            RETURNING id, constraint_kind, left_ref, right_ref, authority,
                      valid_from, valid_to
            """,
            uuid7(),
            value.tenant_id,
            value.kind,
            json.dumps(value.left_ref, sort_keys=True),
            json.dumps(value.right_ref, sort_keys=True),
            value.authority,
            value.evidence_id,
            json.dumps(value.provenance, sort_keys=True),
            value.valid_from,
            value.valid_to,
        )
        assert row is not None
        return IdentityConstraintValue(
            id=row["id"],
            kind=row["constraint_kind"],
            left_ref=_json(row["left_ref"]),
            right_ref=_json(row["right_ref"]),
            authority=row["authority"],
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
        )

    async def record_candidates(
        self,
        *,
        tenant_id: UUID,
        resolver_run_id: UUID,
        mention_id: UUID,
        candidates: list[RankedCandidate],
        conn: asyncpg.Connection,
    ) -> None:
        for candidate in candidates:
            await conn.execute(
                """
                INSERT INTO identity_resolution_candidates (
                  id, tenant_id, resolver_run_id, mention_id, candidate_key,
                  candidate_ref, retrieval_methods, features, score, rank,
                  constraint_outcome, reasons
                ) VALUES (
                  $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb,
                  $9, $10, $11, $12::jsonb
                )
                ON CONFLICT (tenant_id, resolver_run_id, mention_id, candidate_key)
                DO NOTHING
                """,
                uuid7(), tenant_id, resolver_run_id, mention_id,
                candidate.candidate_key,
                json.dumps(candidate.candidate_ref, sort_keys=True),
                json.dumps(candidate.retrieval_methods),
                json.dumps(candidate.features, sort_keys=True),
                candidate.score, candidate.rank, candidate.constraint_outcome,
                json.dumps(candidate.reasons),
            )

    async def persist_snapshot(
        self,
        snapshot: IdentityResolutionSnapshot,
        *,
        assertion_ids: list[UUID],
        conn: asyncpg.Connection,
    ) -> IdentityResolutionSnapshot:
        existing = await conn.fetchrow(
            """
            SELECT manifest, snapshot_hash
              FROM identity_resolution_snapshots
             WHERE tenant_id = $1 AND resolver_run_id = $2
            """,
            snapshot.tenant_id,
            snapshot.resolver_run_id,
        )
        if existing is not None:
            if existing["snapshot_hash"] != snapshot.snapshot_hash:
                raise ValidationError("resolver run already sealed a different snapshot")
            manifest = _json(existing["manifest"])
            return IdentityResolutionSnapshot.model_validate(
                {**manifest, "snapshot_hash": existing["snapshot_hash"]}
            )

        counts = {
            outcome: sum(item.outcome == outcome for item in snapshot.items)
            for outcome in ("resolved", "probable", "ambiguous", "unresolved")
        }
        manifest = snapshot.model_dump(mode="json", exclude={"snapshot_hash"})
        await conn.execute(
            """
            INSERT INTO identity_resolution_snapshots (
              id, tenant_id, resolver_run_id, input_kind, observation_id,
              observation_occurred_at, requester_actor_id, resolution_status,
              mention_count, resolved_count, probable_count, ambiguous_count,
              unresolved_count, assertion_ids, manifest, snapshot_hash,
              access_policy_hash, resolver_name, resolver_version, policy_version,
              created_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
              $13, $14, $15::jsonb, $16, $17, $18, $19, $20, $21
            )
            """,
            snapshot.id, snapshot.tenant_id, snapshot.resolver_run_id,
            snapshot.input_kind, snapshot.observation_id,
            snapshot.observation_occurred_at, snapshot.requester_actor_id,
            snapshot.resolution_status, len(snapshot.items), counts["resolved"],
            counts["probable"], counts["ambiguous"], counts["unresolved"],
            assertion_ids, json.dumps(manifest, sort_keys=True),
            snapshot.snapshot_hash, snapshot.access_policy_hash,
            snapshot.resolver_name, snapshot.resolver_version,
            snapshot.policy_version, snapshot.created_at,
        )
        for item in snapshot.items:
            await conn.execute(
                """
                INSERT INTO identity_resolution_snapshot_items (
                  tenant_id, snapshot_id, mention_id, outcome, selected_ref,
                  confidence, assertion_id, alternatives, reasons
                ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::jsonb, $9::jsonb)
                """,
                snapshot.tenant_id, snapshot.id, item.mention_id, item.outcome,
                json.dumps(item.selected_ref, sort_keys=True)
                if item.selected_ref is not None else None,
                item.confidence, item.assertion_id,
                json.dumps(item.alternatives, sort_keys=True),
                json.dumps(item.reasons),
            )
        return snapshot

    async def latest_snapshot(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        conn: asyncpg.Connection,
    ) -> IdentityResolutionSnapshot | None:
        row = await conn.fetchrow(
            """
            SELECT manifest, snapshot_hash
              FROM identity_resolution_snapshots
             WHERE tenant_id = $1 AND observation_id = $2
             ORDER BY created_at DESC, id DESC LIMIT 1
            """,
            tenant_id,
            observation_id,
        )
        if row is None:
            return None
        return IdentityResolutionSnapshot.model_validate(
            {**_json(row["manifest"]), "snapshot_hash": row["snapshot_hash"]}
        )

    async def snapshot_for_run(
        self,
        *,
        tenant_id: UUID,
        resolver_run_id: UUID,
        conn: asyncpg.Connection,
    ) -> IdentityResolutionSnapshot | None:
        row = await conn.fetchrow(
            """
            SELECT manifest, snapshot_hash
              FROM identity_resolution_snapshots
             WHERE tenant_id = $1 AND resolver_run_id = $2
            """,
            tenant_id,
            resolver_run_id,
        )
        if row is None:
            return None
        return IdentityResolutionSnapshot.model_validate(
            {**_json(row["manifest"]), "snapshot_hash": row["snapshot_hash"]}
        )


__all__ = [
    "CandidateProvider",
    "IdentityResolutionRepository",
    "PostgresCandidateProvider",
]
