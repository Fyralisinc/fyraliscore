"""Persistence for the Edge Intelligence Kernel."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .compiler import confidence_from_pair_evidence
from .types import (
    ModelPairEvidence,
    PairEvidenceObservation,
    PairDirectionVote,
    RelationClaim,
    RelationEdgeProjection,
    RelationEvidence,
    RelationFrame,
    RelationParticipant,
    canonical_model_pair,
    normalize_primitive,
)


_RELATION_SELECT = """
  id, tenant_id, source_observation_id, think_run_id,
  source_model_id, target_model_id, subject_ref, object_ref, predicate,
  edge_kind_hint, direction, scope_entities, temporal_bounds, evidence_text,
  confidence, extraction_method, status, metadata, created_at, updated_at
"""

_PAIR_SELECT = """
  id, tenant_id, model_a_id, model_b_id, primitive,
  co_retrieved_count, co_used_valid_diff_count, explicit_relation_count,
  think_edge_op_count, t4_accept_count, t4_reject_count, no_edge_count,
  positive_outcome_count, negative_outcome_count,
  direction_votes, edge_kind_votes, confidence_score,
  last_seen_at, metadata, created_at, updated_at
"""

_CLAIM_SELECT = """
  id, tenant_id, source_observation_id, think_run_id,
  source_model_id, target_model_id, subject_ref, object_ref, predicate,
  edge_kind, direction, endpoint_binding_status, write_policy, status,
  confidence, weight, binding_confidence, evidence_event_ids, evidence_model_ids,
  evidence_text, explanation, accepted_edge_ids, temporal_bounds, metadata,
  created_at, updated_at, decided_at
"""

_FRAME_SELECT = """
  id, tenant_id, source_observation_id, think_run_id, relation_kind,
  status, participant_binding_status, write_policy, confidence,
  evidence_event_ids, evidence_model_ids, evidence_text, explanation,
  temporal_bounds, metadata, created_at, updated_at, decided_at
"""

_PARTICIPANT_SELECT = """
  id, relation_id, tenant_id, model_id, role, binding_confidence,
  cardinality_group, metadata, created_at, updated_at
"""

_PROJECTION_SELECT = """
  id, relation_id, tenant_id, edge_id, projection_rule, source_role,
  target_role, source_model_id, target_model_id, edge_kind, status,
  metadata, created_at, updated_at
"""


class EdgeIntelligenceRepo:
    """Write/read relation evidence and model-pair evidence aggregates."""

    async def insert_relation_evidence(
        self,
        conn: asyncpg.Connection,
        evidence: RelationEvidence,
    ) -> dict[str, Any]:
        _validate_relation_evidence(evidence)
        row_id = evidence.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO relation_evidence (
              id, tenant_id, source_observation_id, think_run_id,
              source_model_id, target_model_id, subject_ref, object_ref,
              predicate, edge_kind_hint, direction, scope_entities,
              temporal_bounds, evidence_text, confidence, extraction_method,
              status, metadata
            )
            VALUES (
              $1, $2, $3, $4,
              $5, $6, $7::jsonb, $8::jsonb,
              $9, $10, $11, $12::jsonb,
              $13::jsonb, $14, $15, $16,
              $17, $18::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
              subject_ref = EXCLUDED.subject_ref,
              object_ref = EXCLUDED.object_ref,
              predicate = EXCLUDED.predicate,
              edge_kind_hint = EXCLUDED.edge_kind_hint,
              direction = EXCLUDED.direction,
              scope_entities = EXCLUDED.scope_entities,
              temporal_bounds = EXCLUDED.temporal_bounds,
              evidence_text = EXCLUDED.evidence_text,
              confidence = EXCLUDED.confidence,
              extraction_method = EXCLUDED.extraction_method,
              status = EXCLUDED.status,
              metadata = EXCLUDED.metadata,
              updated_at = now()
            RETURNING {_RELATION_SELECT}
            """,
            row_id,
            evidence.tenant_id,
            evidence.source_observation_id,
            evidence.think_run_id,
            evidence.source_model_id,
            evidence.target_model_id,
            _jsonb(evidence.subject_ref),
            _jsonb(evidence.object_ref),
            evidence.predicate.strip(),
            _clean_optional_text(evidence.edge_kind_hint),
            evidence.direction,
            _jsonb(evidence.scope_entities),
            _jsonb(evidence.temporal_bounds),
            _clean_optional_text(evidence.evidence_text),
            _clamp01(evidence.confidence),
            evidence.extraction_method.strip(),
            evidence.status,
            _jsonb(evidence.metadata),
        )
        if row is None:
            raise ValidationError("relation evidence insert returned no row")
        return _row_to_relation_dict(row)

    async def insert_relation_claim(
        self,
        conn: asyncpg.Connection,
        claim: RelationClaim,
    ) -> dict[str, Any]:
        """Persist one first-class relation write-plan."""
        _validate_relation_claim(claim)
        row_id = claim.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO relation_claims (
              id, tenant_id, source_observation_id, think_run_id,
              source_model_id, target_model_id, subject_ref, object_ref,
              predicate, edge_kind, direction, endpoint_binding_status,
              write_policy, status, confidence, weight, binding_confidence,
              evidence_event_ids, evidence_model_ids, evidence_text,
              explanation, accepted_edge_ids, temporal_bounds, metadata
            )
            VALUES (
              $1, $2, $3, $4,
              $5, $6, $7::jsonb, $8::jsonb,
              $9, $10, $11, $12,
              $13, $14, $15, $16,
              $17, $18::uuid[], $19::uuid[], $20,
              $21, $22::uuid[], $23::jsonb, $24::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
              source_observation_id = EXCLUDED.source_observation_id,
              think_run_id = EXCLUDED.think_run_id,
              source_model_id = EXCLUDED.source_model_id,
              target_model_id = EXCLUDED.target_model_id,
              subject_ref = EXCLUDED.subject_ref,
              object_ref = EXCLUDED.object_ref,
              predicate = EXCLUDED.predicate,
              edge_kind = EXCLUDED.edge_kind,
              direction = EXCLUDED.direction,
              endpoint_binding_status = EXCLUDED.endpoint_binding_status,
              write_policy = EXCLUDED.write_policy,
              status = EXCLUDED.status,
              confidence = EXCLUDED.confidence,
              weight = EXCLUDED.weight,
              binding_confidence = EXCLUDED.binding_confidence,
              evidence_event_ids = EXCLUDED.evidence_event_ids,
              evidence_model_ids = EXCLUDED.evidence_model_ids,
              evidence_text = EXCLUDED.evidence_text,
              explanation = EXCLUDED.explanation,
              accepted_edge_ids = EXCLUDED.accepted_edge_ids,
              temporal_bounds = EXCLUDED.temporal_bounds,
              metadata = EXCLUDED.metadata,
              updated_at = now(),
              decided_at = CASE
                WHEN EXCLUDED.status IN ('accepted', 'rejected', 'retired')
                THEN COALESCE(relation_claims.decided_at, now())
                ELSE relation_claims.decided_at
              END
            RETURNING {_CLAIM_SELECT}
            """,
            row_id,
            claim.tenant_id,
            claim.source_observation_id,
            claim.think_run_id,
            claim.source_model_id,
            claim.target_model_id,
            _jsonb(claim.subject_ref),
            _jsonb(claim.object_ref),
            claim.predicate.strip(),
            claim.edge_kind.strip(),
            claim.direction,
            claim.endpoint_binding_status,
            claim.write_policy,
            claim.status,
            _clamp01(claim.confidence),
            _clamp01(claim.weight) if claim.weight is not None else None,
            _clamp01(claim.binding_confidence),
            list(claim.evidence_event_ids),
            list(claim.evidence_model_ids),
            _clean_optional_text(claim.evidence_text),
            _clean_optional_text(claim.explanation),
            list(claim.accepted_edge_ids),
            _jsonb(claim.temporal_bounds),
            _jsonb(claim.metadata),
        )
        if row is None:
            raise ValidationError("relation claim insert returned no row")
        return _row_to_claim_dict(row)

    async def mark_relation_claim_decided(
        self,
        conn: asyncpg.Connection,
        *,
        claim_id: UUID,
        tenant_id: UUID,
        status: str,
        accepted_edge_ids: list[UUID] | tuple[UUID, ...] = (),
        decision_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if status not in {
            "accepted",
            "candidate",
            "needs_review",
            "rejected",
            "retired",
        }:
            raise ValidationError("invalid relation claim status", status=status)
        metadata_patch = (
            {"latest_adjudication": decision_metadata}
            if decision_metadata is not None
            else {}
        )
        row = await conn.fetchrow(
            f"""
            UPDATE relation_claims
            SET status = $3,
                accepted_edge_ids = $4::uuid[],
                metadata = metadata || $5::jsonb,
                decided_at = CASE
                  WHEN $3 IN ('accepted', 'rejected', 'retired') THEN now()
                  ELSE decided_at
                END,
                updated_at = now()
            WHERE id = $1
              AND tenant_id = $2
            RETURNING {_CLAIM_SELECT}
            """,
            claim_id,
            tenant_id,
            status,
            list(accepted_edge_ids),
            _jsonb(metadata_patch),
        )
        return _row_to_claim_dict(row) if row is not None else None

    async def insert_relation_frame(
        self,
        conn: asyncpg.Connection,
        frame: RelationFrame,
        *,
        participants: list[RelationParticipant] | tuple[RelationParticipant, ...],
    ) -> dict[str, Any]:
        """Persist one N-ary relation frame and its role-bound participants."""
        _validate_relation_frame(frame, participants)
        row_id = frame.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO relation_instances (
              id, tenant_id, source_observation_id, think_run_id,
              relation_kind, status, participant_binding_status, write_policy,
              confidence, evidence_event_ids, evidence_model_ids, evidence_text,
              explanation, temporal_bounds, metadata
            )
            VALUES (
              $1, $2, $3, $4,
              $5, $6, $7, $8,
              $9, $10::uuid[], $11::uuid[], $12,
              $13, $14::jsonb, $15::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
              source_observation_id = EXCLUDED.source_observation_id,
              think_run_id = EXCLUDED.think_run_id,
              relation_kind = EXCLUDED.relation_kind,
              status = EXCLUDED.status,
              participant_binding_status = EXCLUDED.participant_binding_status,
              write_policy = EXCLUDED.write_policy,
              confidence = EXCLUDED.confidence,
              evidence_event_ids = EXCLUDED.evidence_event_ids,
              evidence_model_ids = EXCLUDED.evidence_model_ids,
              evidence_text = EXCLUDED.evidence_text,
              explanation = EXCLUDED.explanation,
              temporal_bounds = EXCLUDED.temporal_bounds,
              metadata = EXCLUDED.metadata,
              updated_at = now(),
              decided_at = CASE
                WHEN EXCLUDED.status IN ('accepted', 'rejected', 'retired')
                THEN COALESCE(relation_instances.decided_at, now())
                ELSE relation_instances.decided_at
              END
            RETURNING {_FRAME_SELECT}
            """,
            row_id,
            frame.tenant_id,
            frame.source_observation_id,
            frame.think_run_id,
            frame.relation_kind.strip(),
            frame.status,
            frame.participant_binding_status,
            frame.write_policy,
            _clamp01(frame.confidence),
            list(frame.evidence_event_ids),
            list(frame.evidence_model_ids),
            _clean_optional_text(frame.evidence_text),
            _clean_optional_text(frame.explanation),
            _jsonb(frame.temporal_bounds),
            _jsonb(frame.metadata),
        )
        if row is None:
            raise ValidationError("relation frame insert returned no row")

        await conn.execute(
            "DELETE FROM relation_participants WHERE relation_id = $1",
            row_id,
        )
        for participant in participants:
            await self.insert_relation_participant(
                conn,
                participant,
                relation_id=row_id,
                tenant_id=frame.tenant_id,
            )
        return await self.get_relation_frame(
            conn,
            tenant_id=frame.tenant_id,
            relation_id=row_id,
        )

    async def insert_relation_participant(
        self,
        conn: asyncpg.Connection,
        participant: RelationParticipant,
        *,
        relation_id: UUID,
        tenant_id: UUID,
    ) -> dict[str, Any]:
        _validate_relation_participant(participant)
        row = await conn.fetchrow(
            f"""
            INSERT INTO relation_participants (
              id, relation_id, tenant_id, model_id, role,
              binding_confidence, cardinality_group, metadata
            )
            VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8::jsonb
            )
            ON CONFLICT ON CONSTRAINT relation_participants_unique
            DO UPDATE SET
              binding_confidence = EXCLUDED.binding_confidence,
              cardinality_group = EXCLUDED.cardinality_group,
              metadata = EXCLUDED.metadata,
              updated_at = now()
            RETURNING {_PARTICIPANT_SELECT}
            """,
            participant.id or uuid7(),
            relation_id,
            tenant_id,
            participant.model_id,
            participant.role.strip(),
            _clamp01(participant.binding_confidence),
            _clean_optional_text(participant.cardinality_group),
            _jsonb(participant.metadata),
        )
        if row is None:
            raise ValidationError("relation participant insert returned no row")
        return _row_to_participant_dict(row)

    async def get_relation_frame(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        relation_id: UUID,
    ) -> dict[str, Any]:
        row = await conn.fetchrow(
            f"""
            SELECT {_FRAME_SELECT}
            FROM relation_instances
            WHERE tenant_id = $1
              AND id = $2
            """,
            tenant_id,
            relation_id,
        )
        if row is None:
            raise ValidationError("relation frame not found", relation_id=str(relation_id))
        frame = _row_to_frame_dict(row)
        frame["participants"] = await self.list_relation_participants(
            conn,
            tenant_id=tenant_id,
            relation_id=relation_id,
        )
        frame["edge_projections"] = await self.list_relation_edge_projections(
            conn,
            tenant_id=tenant_id,
            relation_id=relation_id,
        )
        return frame

    async def list_relation_participants(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        relation_id: UUID,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            f"""
            SELECT {_PARTICIPANT_SELECT}
            FROM relation_participants
            WHERE tenant_id = $1
              AND relation_id = $2
            ORDER BY role ASC, created_at ASC, id ASC
            """,
            tenant_id,
            relation_id,
        )
        return [_row_to_participant_dict(row) for row in rows]

    async def mark_relation_frame_decided(
        self,
        conn: asyncpg.Connection,
        *,
        relation_id: UUID,
        tenant_id: UUID,
        status: str,
        decision_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if status not in {
            "accepted",
            "candidate",
            "needs_review",
            "disputed",
            "rejected",
            "retired",
        }:
            raise ValidationError("invalid relation frame status", status=status)
        metadata_patch = (
            {"latest_adjudication": decision_metadata}
            if decision_metadata is not None
            else {}
        )
        row = await conn.fetchrow(
            f"""
            UPDATE relation_instances
            SET status = $3,
                metadata = metadata || $4::jsonb,
                decided_at = CASE
                  WHEN $3 IN ('accepted', 'rejected', 'retired') THEN now()
                  ELSE decided_at
                END,
                updated_at = now()
            WHERE id = $1
              AND tenant_id = $2
            RETURNING {_FRAME_SELECT}
            """,
            relation_id,
            tenant_id,
            status,
            _jsonb(metadata_patch),
        )
        return _row_to_frame_dict(row) if row is not None else None

    async def insert_relation_edge_projection(
        self,
        conn: asyncpg.Connection,
        projection: RelationEdgeProjection,
    ) -> dict[str, Any]:
        _validate_relation_edge_projection(projection)
        row = await conn.fetchrow(
            f"""
            INSERT INTO relation_edge_projections (
              id, relation_id, tenant_id, edge_id, projection_rule,
              source_role, target_role, source_model_id, target_model_id,
              edge_kind, status, metadata
            )
            VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9,
              $10, $11, $12::jsonb
            )
            ON CONFLICT ON CONSTRAINT relation_edge_projections_unique
            DO UPDATE SET
              edge_id = EXCLUDED.edge_id,
              status = EXCLUDED.status,
              metadata = relation_edge_projections.metadata || EXCLUDED.metadata,
              updated_at = now()
            RETURNING {_PROJECTION_SELECT}
            """,
            projection.id or uuid7(),
            projection.relation_id,
            projection.tenant_id,
            projection.edge_id,
            projection.projection_rule.strip(),
            projection.source_role.strip(),
            projection.target_role.strip(),
            projection.source_model_id,
            projection.target_model_id,
            projection.edge_kind.strip(),
            projection.status,
            _jsonb(projection.metadata),
        )
        if row is None:
            raise ValidationError("relation edge projection insert returned no row")
        return _row_to_projection_dict(row)

    async def list_relation_edge_projections(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        relation_id: UUID,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            f"""
            SELECT {_PROJECTION_SELECT}
            FROM relation_edge_projections
            WHERE tenant_id = $1
              AND relation_id = $2
            ORDER BY projection_rule ASC, created_at ASC, id ASC
            """,
            tenant_id,
            relation_id,
        )
        return [_row_to_projection_dict(row) for row in rows]

    async def retire_pairwise_relation_frames(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        source_model_id: UUID,
        target_model_id: UUID,
        relation_kind: str,
        reason: str,
        exclude_relation_id: UUID | None = None,
    ) -> tuple[UUID, ...]:
        """Retire canonical pairwise truth and all of its edge projections."""
        rows = await conn.fetch(
            """
            UPDATE relation_instances relation
            SET status = 'retired',
                metadata = relation.metadata || jsonb_build_object(
                  'retirement_reason', $5::text
                ),
                decided_at = COALESCE(relation.decided_at, now()),
                updated_at = now()
            WHERE relation.tenant_id = $1
              AND relation.relation_kind = $4
              AND relation.status IN ('active', 'accepted', 'disputed')
              AND ($6::uuid IS NULL OR relation.id <> $6)
              AND EXISTS (
                SELECT 1 FROM relation_participants source_participant
                WHERE source_participant.tenant_id = $1
                  AND source_participant.relation_id = relation.id
                  AND source_participant.model_id = $2
                  AND source_participant.role = 'source'
              )
              AND EXISTS (
                SELECT 1 FROM relation_participants target_participant
                WHERE target_participant.tenant_id = $1
                  AND target_participant.relation_id = relation.id
                  AND target_participant.model_id = $3
                  AND target_participant.role = 'target'
              )
            RETURNING relation.id
            """,
            tenant_id,
            source_model_id,
            target_model_id,
            relation_kind.strip(),
            reason,
            exclude_relation_id,
        )
        relation_ids = tuple(row["id"] for row in rows)
        if relation_ids:
            await conn.execute(
                """
                UPDATE relation_edge_projections
                SET status = 'retired',
                    metadata = metadata || jsonb_build_object(
                      'retirement_reason', $3::text
                    ),
                    updated_at = now()
                WHERE tenant_id = $1
                  AND relation_id = ANY($2::uuid[])
                  AND status = 'active'
                """,
                tenant_id,
                list(relation_ids),
                reason,
            )
        return relation_ids

    async def retire_relation_frames_for_projection_edges(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        edge_ids: Iterable[UUID],
        reason: str,
    ) -> tuple[UUID, ...]:
        """Retire canonical truth whose compatibility projection was superseded."""
        normalized_edge_ids = tuple(dict.fromkeys(edge_ids))
        if not normalized_edge_ids:
            return ()
        rows = await conn.fetch(
            """
            UPDATE relation_instances relation
            SET status = 'retired',
                metadata = relation.metadata || jsonb_build_object(
                  'retirement_reason', $3::text
                ),
                decided_at = COALESCE(relation.decided_at, now()),
                updated_at = now()
            WHERE relation.tenant_id = $1
              AND relation.status IN ('active', 'accepted', 'disputed')
              AND EXISTS (
                SELECT 1 FROM relation_edge_projections projection
                WHERE projection.tenant_id = $1
                  AND projection.relation_id = relation.id
                  AND projection.edge_id = ANY($2::uuid[])
                  AND projection.status = 'active'
              )
            RETURNING relation.id
            """,
            tenant_id,
            list(normalized_edge_ids),
            reason,
        )
        relation_ids = tuple(row["id"] for row in rows)
        if relation_ids:
            await conn.execute(
                """
                UPDATE relation_edge_projections
                SET status = 'retired',
                    metadata = metadata || jsonb_build_object(
                      'retirement_reason', $3::text
                    ),
                    updated_at = now()
                WHERE tenant_id = $1
                  AND relation_id = ANY($2::uuid[])
                  AND status = 'active'
                """,
                tenant_id,
                list(relation_ids),
                reason,
            )
        return relation_ids

    async def record_pair_observation(
        self,
        conn: asyncpg.Connection,
        observation: PairEvidenceObservation,
    ) -> ModelPairEvidence:
        _validate_pair_observation(observation)
        model_a_id, model_b_id = canonical_model_pair(
            observation.left_model_id,
            observation.right_model_id,
        )
        primitive = normalize_primitive(observation.primitive)
        direction_vote = _direction_vote(
            observation,
            model_a_id=model_a_id,
            model_b_id=model_b_id,
        )
        edge_kind_vote = _clean_optional_text(observation.edge_kind_hint)

        inserted = await conn.fetchrow(
            f"""
            INSERT INTO model_pair_evidence (
              id, tenant_id, model_a_id, model_b_id, primitive,
              co_retrieved_count, co_used_valid_diff_count,
              explicit_relation_count, think_edge_op_count,
              t4_accept_count, t4_reject_count, no_edge_count,
              positive_outcome_count, negative_outcome_count,
              direction_votes, edge_kind_votes, confidence_score,
              metadata
            )
            VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9,
              $10, $11, $12,
              $13, $14,
              $15::jsonb, $16::jsonb, 0.0,
              $17::jsonb
            )
            ON CONFLICT ON CONSTRAINT model_pair_evidence_unique DO NOTHING
            RETURNING {_PAIR_SELECT}
            """,
            uuid7(),
            observation.tenant_id,
            model_a_id,
            model_b_id,
            primitive,
            observation.co_retrieved_delta,
            observation.co_used_valid_diff_delta,
            observation.explicit_relation_delta,
            observation.think_edge_op_delta,
            observation.t4_accept_delta,
            observation.t4_reject_delta,
            observation.no_edge_delta,
            observation.positive_outcome_delta,
            observation.negative_outcome_delta,
            _jsonb(_vote_dict(direction_vote)),
            _jsonb(_vote_dict(edge_kind_vote)),
            _jsonb(observation.metadata),
        )
        if inserted is not None:
            aggregate = _row_to_pair_evidence(inserted)
            return await self._update_pair_confidence(conn, aggregate)

        current = await conn.fetchrow(
            f"""
            SELECT {_PAIR_SELECT}
            FROM model_pair_evidence
            WHERE tenant_id = $1
              AND model_a_id = $2
              AND model_b_id = $3
              AND primitive = $4
            FOR UPDATE
            """,
            observation.tenant_id,
            model_a_id,
            model_b_id,
            primitive,
        )
        if current is None:
            raise ValidationError("model pair evidence upsert lost conflict row")
        merged_direction_votes = _merge_votes(
            _json_obj(current["direction_votes"]),
            _vote_dict(direction_vote),
        )
        merged_edge_kind_votes = _merge_votes(
            _json_obj(current["edge_kind_votes"]),
            _vote_dict(edge_kind_vote),
        )
        merged_metadata = {
            **_json_obj(current["metadata"]),
            **observation.metadata,
        }
        updated = await conn.fetchrow(
            f"""
            UPDATE model_pair_evidence
            SET co_retrieved_count = co_retrieved_count + $5,
                co_used_valid_diff_count = co_used_valid_diff_count + $6,
                explicit_relation_count = explicit_relation_count + $7,
                think_edge_op_count = think_edge_op_count + $8,
                t4_accept_count = t4_accept_count + $9,
                t4_reject_count = t4_reject_count + $10,
                no_edge_count = no_edge_count + $11,
                positive_outcome_count = positive_outcome_count + $12,
                negative_outcome_count = negative_outcome_count + $13,
                direction_votes = $14::jsonb,
                edge_kind_votes = $15::jsonb,
                metadata = $16::jsonb,
                last_seen_at = now(),
                updated_at = now()
            WHERE tenant_id = $1
              AND model_a_id = $2
              AND model_b_id = $3
              AND primitive = $4
            RETURNING {_PAIR_SELECT}
            """,
            observation.tenant_id,
            model_a_id,
            model_b_id,
            primitive,
            observation.co_retrieved_delta,
            observation.co_used_valid_diff_delta,
            observation.explicit_relation_delta,
            observation.think_edge_op_delta,
            observation.t4_accept_delta,
            observation.t4_reject_delta,
            observation.no_edge_delta,
            observation.positive_outcome_delta,
            observation.negative_outcome_delta,
            _jsonb(merged_direction_votes),
            _jsonb(merged_edge_kind_votes),
            _jsonb(merged_metadata),
        )
        if updated is None:
            raise ValidationError("model pair evidence update returned no row")
        aggregate = _row_to_pair_evidence(updated)
        return await self._update_pair_confidence(conn, aggregate)

    async def list_promotable_pair_evidence(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        min_confidence: float = 0.62,
        limit: int = 50,
        model_ids: Iterable[UUID] | None = None,
    ) -> list[ModelPairEvidence]:
        scoped_model_ids = _unique_uuid_list(model_ids)
        model_filter = ""
        args: list[Any] = [
            tenant_id,
            _clamp01(min_confidence),
            max(1, int(limit)),
        ]
        if scoped_model_ids:
            args.append(scoped_model_ids)
            model_filter = (
                f"AND (model_a_id = ANY(${len(args)}::uuid[]) "
                f"OR model_b_id = ANY(${len(args)}::uuid[]))"
            )
        rows = await conn.fetch(
            f"""
            SELECT {_PAIR_SELECT}
            FROM model_pair_evidence
            WHERE tenant_id = $1
              AND confidence_score >= $2
              {model_filter}
            ORDER BY confidence_score DESC, last_seen_at DESC
            LIMIT $3
            """,
            *args,
        )
        return [_row_to_pair_evidence(row) for row in rows]

    async def metrics(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID | None = None,
    ) -> "EdgeIntelligenceMetrics":
        clauses: list[str] = []
        args: list[Any] = []
        if tenant_id is not None:
            args.append(tenant_id)
            clauses.append(f"tenant_id = ${len(args)}")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        relation_summary = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE source_model_id IS NOT NULL
                               AND target_model_id IS NOT NULL)::int AS endpoint_bound,
              COUNT(*) FILTER (WHERE status = 'active')::int AS active
            FROM relation_evidence
            {where}
            """,
            *args,
        )
        claim_summary = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE endpoint_binding_status = 'bound')::int
                AS bound,
              COUNT(*) FILTER (WHERE status = 'accepted')::int AS accepted,
              COUNT(*) FILTER (WHERE status IN ('active', 'candidate', 'needs_review'))
                AS open
            FROM relation_claims
            {where}
            """,
            *args,
        )
        frame_summary = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE participant_binding_status = 'bound')::int
                AS bound,
              COUNT(*) FILTER (WHERE status = 'accepted')::int AS accepted,
              COUNT(*) FILTER (WHERE status IN ('active', 'candidate', 'needs_review'))
                AS open
            FROM relation_instances
            {where}
            """,
            *args,
        )
        projection_summary = await conn.fetchrow(
            f"""
            SELECT COUNT(*)::int AS total
            FROM relation_edge_projections
            {where}
            """,
            *args,
        )
        pair_summary = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE confidence_score >= 0.60)::int AS promotable,
              COALESCE(AVG(confidence_score), 0.0)::float AS avg_confidence
            FROM model_pair_evidence
            {where}
            """,
            *args,
        )
        frame_kind_rows = await conn.fetch(
            f"""
            SELECT relation_kind, COUNT(*)::int AS n
            FROM relation_instances
            {where}
            GROUP BY relation_kind
            """,
            *args,
        )
        kind_rows = await conn.fetch(
            f"""
            SELECT edge_kind_hint, COUNT(*)::int AS n
            FROM relation_evidence
            {where}
            GROUP BY edge_kind_hint
            """,
            *args,
        )
        claim_kind_rows = await conn.fetch(
            f"""
            SELECT edge_kind, COUNT(*)::int AS n
            FROM relation_claims
            {where}
            GROUP BY edge_kind
            """,
            *args,
        )
        primitive_rows = await conn.fetch(
            f"""
            SELECT primitive, COUNT(*)::int AS n
            FROM model_pair_evidence
            {where}
            GROUP BY primitive
            """,
            *args,
        )
        return EdgeIntelligenceMetrics(
            relation_evidence_total=int(relation_summary["total"] or 0),
            relation_evidence_endpoint_bound=int(
                relation_summary["endpoint_bound"] or 0
            ),
            relation_evidence_active=int(relation_summary["active"] or 0),
            relation_claims_total=int(claim_summary["total"] or 0),
            relation_claims_bound=int(claim_summary["bound"] or 0),
            relation_claims_accepted=int(claim_summary["accepted"] or 0),
            relation_claims_open=int(claim_summary["open"] or 0),
            relation_frames_total=int(frame_summary["total"] or 0),
            relation_frames_bound=int(frame_summary["bound"] or 0),
            relation_frames_accepted=int(frame_summary["accepted"] or 0),
            relation_frames_open=int(frame_summary["open"] or 0),
            relation_edge_projections_total=int(projection_summary["total"] or 0),
            pair_evidence_total=int(pair_summary["total"] or 0),
            pair_evidence_promotable=int(pair_summary["promotable"] or 0),
            pair_evidence_avg_confidence=float(
                pair_summary["avg_confidence"] or 0.0
            ),
            relation_evidence_by_edge_kind={
                str(row["edge_kind_hint"] or "unknown"): int(row["n"])
                for row in kind_rows
            },
            relation_claims_by_edge_kind={
                str(row["edge_kind"] or "unknown"): int(row["n"])
                for row in claim_kind_rows
            },
            relation_frames_by_kind={
                str(row["relation_kind"] or "unknown"): int(row["n"])
                for row in frame_kind_rows
            },
            pair_evidence_by_primitive={
                str(row["primitive"] or "UNKNOWN"): int(row["n"])
                for row in primitive_rows
            },
        )

    async def _update_pair_confidence(
        self,
        conn: asyncpg.Connection,
        aggregate: ModelPairEvidence,
    ) -> ModelPairEvidence:
        confidence = confidence_from_pair_evidence(aggregate)
        if abs(confidence - aggregate.confidence_score) < 0.000001:
            return aggregate
        row = await conn.fetchrow(
            f"""
            UPDATE model_pair_evidence
            SET confidence_score = $5,
                updated_at = now()
            WHERE tenant_id = $1
              AND model_a_id = $2
              AND model_b_id = $3
              AND primitive = $4
            RETURNING {_PAIR_SELECT}
            """,
            aggregate.tenant_id,
            aggregate.model_a_id,
            aggregate.model_b_id,
            aggregate.primitive,
            confidence,
        )
        if row is None:
            raise ValidationError("model pair confidence update returned no row")
        return _row_to_pair_evidence(row)


def _validate_relation_evidence(evidence: RelationEvidence) -> None:
    if not evidence.predicate or not evidence.predicate.strip():
        raise ValidationError("relation evidence requires predicate")
    if not evidence.extraction_method or not evidence.extraction_method.strip():
        raise ValidationError("relation evidence requires extraction_method")
    if (
        evidence.source_model_id is not None
        and evidence.source_model_id == evidence.target_model_id
    ):
        raise ValidationError("relation evidence cannot use same source/target model")
    if evidence.direction not in {
        "source_to_target",
        "target_to_source",
        "symmetric",
        "unknown",
    }:
        raise ValidationError("invalid relation evidence direction")
    if evidence.status not in {"active", "consumed", "rejected", "superseded"}:
        raise ValidationError("invalid relation evidence status")


def _validate_relation_claim(claim: RelationClaim) -> None:
    if not claim.predicate or not claim.predicate.strip():
        raise ValidationError("relation claim requires predicate")
    if not claim.edge_kind or not claim.edge_kind.strip():
        raise ValidationError("relation claim requires edge_kind")
    if (
        claim.source_model_id is not None
        and claim.source_model_id == claim.target_model_id
    ):
        raise ValidationError("relation claim cannot use same source/target model")
    if claim.direction not in {
        "source_to_target",
        "target_to_source",
        "symmetric",
        "unknown",
    }:
        raise ValidationError("invalid relation claim direction")
    if claim.endpoint_binding_status not in {
        "bound",
        "partially_bound",
        "unbound",
        "ambiguous",
    }:
        raise ValidationError("invalid relation claim endpoint binding status")
    if claim.write_policy not in {
        "accepted_edge",
        "candidate",
        "needs_review",
        "no_edge",
    }:
        raise ValidationError("invalid relation claim write_policy")
    if claim.status not in {
        "active",
        "accepted",
        "candidate",
        "needs_review",
        "rejected",
        "retired",
    }:
        raise ValidationError("invalid relation claim status")
    if claim.weight is not None and not (0.0 <= float(claim.weight) <= 1.0):
        raise ValidationError("relation claim weight must be in [0, 1]")
    if claim.write_policy == "accepted_edge":
        if claim.source_model_id is None or claim.target_model_id is None:
            raise ValidationError("accepted relation claim requires bound endpoints")
        if claim.endpoint_binding_status != "bound":
            raise ValidationError("accepted relation claim requires bound status")


def _validate_relation_frame(
    frame: RelationFrame,
    participants: list[RelationParticipant] | tuple[RelationParticipant, ...],
) -> None:
    if not frame.relation_kind or not frame.relation_kind.strip():
        raise ValidationError("relation frame requires relation_kind")
    if frame.status not in {
        "active",
        "candidate",
        "accepted",
        "needs_review",
        "disputed",
        "rejected",
        "retired",
    }:
        raise ValidationError("invalid relation frame status")
    if frame.participant_binding_status not in {
        "bound",
        "partially_bound",
        "unbound",
        "ambiguous",
    }:
        raise ValidationError("invalid relation frame participant binding status")
    if frame.write_policy not in {
        "project_edges",
        "candidate",
        "needs_review",
        "no_projection",
    }:
        raise ValidationError("invalid relation frame write_policy")
    if len(participants) < 2:
        raise ValidationError("relation frame requires at least two participants")
    if len(participants) > 12:
        raise ValidationError("relation frame cannot exceed 12 participants")
    seen: set[tuple[UUID, str]] = set()
    model_ids: set[UUID] = set()
    for participant in participants:
        _validate_relation_participant(participant)
        key = (participant.model_id, participant.role.strip())
        if key in seen:
            raise ValidationError("duplicate relation participant role/model binding")
        seen.add(key)
        model_ids.add(participant.model_id)
    if len(model_ids) < 2:
        raise ValidationError("relation frame requires at least two distinct models")
    if frame.write_policy == "project_edges":
        if frame.status != "accepted":
            raise ValidationError("projected relation frame must be accepted")
        if frame.participant_binding_status != "bound":
            raise ValidationError("projected relation frame requires bound participants")


def _validate_relation_participant(participant: RelationParticipant) -> None:
    if not participant.role or not participant.role.strip():
        raise ValidationError("relation participant requires role")
    if not (0.0 <= float(participant.binding_confidence) <= 1.0):
        raise ValidationError("relation participant binding_confidence must be in [0, 1]")


def _validate_relation_edge_projection(projection: RelationEdgeProjection) -> None:
    if projection.source_model_id == projection.target_model_id:
        raise ValidationError("relation edge projection cannot self-edge")
    for field_name in (
        "projection_rule",
        "source_role",
        "target_role",
        "edge_kind",
    ):
        if not str(getattr(projection, field_name) or "").strip():
            raise ValidationError(
                f"relation edge projection requires {field_name}",
            )
    if projection.status not in {"active", "retired", "failed"}:
        raise ValidationError("invalid relation edge projection status")


def _validate_pair_observation(observation: PairEvidenceObservation) -> None:
    canonical_model_pair(observation.left_model_id, observation.right_model_id)
    deltas = (
        observation.co_retrieved_delta,
        observation.co_used_valid_diff_delta,
        observation.explicit_relation_delta,
        observation.think_edge_op_delta,
        observation.t4_accept_delta,
        observation.t4_reject_delta,
        observation.no_edge_delta,
        observation.positive_outcome_delta,
        observation.negative_outcome_delta,
    )
    if any(delta < 0 for delta in deltas):
        raise ValidationError("pair evidence deltas must be non-negative")
    if sum(deltas) <= 0 and not observation.edge_kind_hint:
        raise ValidationError("pair evidence observation has no signal")
    if (
        observation.directed_source_model_id is None
    ) != (observation.directed_target_model_id is None):
        raise ValidationError("direction evidence requires both endpoints")


def _direction_vote(
    observation: PairEvidenceObservation,
    *,
    model_a_id: UUID,
    model_b_id: UUID,
) -> PairDirectionVote | None:
    src = observation.directed_source_model_id
    tgt = observation.directed_target_model_id
    if src is None or tgt is None:
        return None
    if src == model_a_id and tgt == model_b_id:
        return "a_to_b"
    if src == model_b_id and tgt == model_a_id:
        return "b_to_a"
    return "unknown"


def _vote_dict(value: str | None) -> dict[str, int]:
    return {value: 1} if value else {}


def _merge_votes(left: dict[str, Any], right: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {
        str(key): int(value)
        for key, value in left.items()
        if key is not None and _is_intlike(value)
    }
    for key, value in right.items():
        merged[key] = merged.get(key, 0) + int(value)
    return {key: value for key, value in sorted(merged.items()) if value > 0}


def _is_intlike(value: Any) -> bool:
    try:
        int(value)
    except (TypeError, ValueError):
        return False
    return True


def _row_to_relation_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {key: row[key] for key in row.keys()}
    for key in ("subject_ref", "object_ref", "temporal_bounds", "metadata"):
        out[key] = _json_obj(out.get(key))
    out["scope_entities"] = _json_list(out.get("scope_entities"))
    return out


def _row_to_claim_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {key: row[key] for key in row.keys()}
    for key in ("subject_ref", "object_ref", "temporal_bounds", "metadata"):
        out[key] = _json_obj(out.get(key))
    out["evidence_event_ids"] = list(out.get("evidence_event_ids") or [])
    out["evidence_model_ids"] = list(out.get("evidence_model_ids") or [])
    out["accepted_edge_ids"] = list(out.get("accepted_edge_ids") or [])
    return out


def _row_to_frame_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {key: row[key] for key in row.keys()}
    for key in ("temporal_bounds", "metadata"):
        out[key] = _json_obj(out.get(key))
    out["evidence_event_ids"] = list(out.get("evidence_event_ids") or [])
    out["evidence_model_ids"] = list(out.get("evidence_model_ids") or [])
    return out


def _row_to_participant_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {key: row[key] for key in row.keys()}
    out["metadata"] = _json_obj(out.get("metadata"))
    return out


def _row_to_projection_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {key: row[key] for key in row.keys()}
    out["metadata"] = _json_obj(out.get("metadata"))
    return out


def _row_to_pair_evidence(row: asyncpg.Record) -> ModelPairEvidence:
    return ModelPairEvidence(
        id=row["id"],
        tenant_id=row["tenant_id"],
        model_a_id=row["model_a_id"],
        model_b_id=row["model_b_id"],
        primitive=row["primitive"],
        co_retrieved_count=int(row["co_retrieved_count"]),
        co_used_valid_diff_count=int(row["co_used_valid_diff_count"]),
        explicit_relation_count=int(row["explicit_relation_count"]),
        think_edge_op_count=int(row["think_edge_op_count"]),
        t4_accept_count=int(row["t4_accept_count"]),
        t4_reject_count=int(row["t4_reject_count"]),
        no_edge_count=int(row["no_edge_count"]),
        positive_outcome_count=int(row["positive_outcome_count"]),
        negative_outcome_count=int(row["negative_outcome_count"]),
        direction_votes=_json_int_dict(row["direction_votes"]),
        edge_kind_votes=_json_int_dict(row["edge_kind_votes"]),
        confidence_score=float(row["confidence_score"]),
        last_seen_at=row["last_seen_at"],
        metadata=_json_obj(row["metadata"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _json_int_dict(value: Any) -> dict[str, int]:
    return {
        str(key): int(raw)
        for key, raw in _json_obj(value).items()
        if key is not None and _is_intlike(raw)
    }


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _unique_uuid_list(values: Iterable[UUID] | None) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values or ():
        if not isinstance(value, UUID) or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


@dataclass(frozen=True)
class EdgeIntelligenceMetrics:
    relation_evidence_total: int = 0
    relation_evidence_endpoint_bound: int = 0
    relation_evidence_active: int = 0
    relation_claims_total: int = 0
    relation_claims_bound: int = 0
    relation_claims_accepted: int = 0
    relation_claims_open: int = 0
    relation_frames_total: int = 0
    relation_frames_bound: int = 0
    relation_frames_accepted: int = 0
    relation_frames_open: int = 0
    relation_edge_projections_total: int = 0
    pair_evidence_total: int = 0
    pair_evidence_promotable: int = 0
    pair_evidence_avg_confidence: float = 0.0
    relation_evidence_by_edge_kind: dict[str, int] = field(default_factory=dict)
    relation_claims_by_edge_kind: dict[str, int] = field(default_factory=dict)
    relation_frames_by_kind: dict[str, int] = field(default_factory=dict)
    pair_evidence_by_primitive: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "relation_evidence_total": self.relation_evidence_total,
            "relation_evidence_endpoint_bound": (
                self.relation_evidence_endpoint_bound
            ),
            "relation_evidence_active": self.relation_evidence_active,
            "relation_claims_total": self.relation_claims_total,
            "relation_claims_bound": self.relation_claims_bound,
            "relation_claims_accepted": self.relation_claims_accepted,
            "relation_claims_open": self.relation_claims_open,
            "relation_frames_total": self.relation_frames_total,
            "relation_frames_bound": self.relation_frames_bound,
            "relation_frames_accepted": self.relation_frames_accepted,
            "relation_frames_open": self.relation_frames_open,
            "relation_edge_projections_total": (
                self.relation_edge_projections_total
            ),
            "pair_evidence_total": self.pair_evidence_total,
            "pair_evidence_promotable": self.pair_evidence_promotable,
            "pair_evidence_avg_confidence": self.pair_evidence_avg_confidence,
            "relation_evidence_by_edge_kind": dict(
                self.relation_evidence_by_edge_kind
            ),
            "relation_claims_by_edge_kind": dict(
                self.relation_claims_by_edge_kind
            ),
            "relation_frames_by_kind": dict(self.relation_frames_by_kind),
            "pair_evidence_by_primitive": dict(self.pair_evidence_by_primitive),
        }
