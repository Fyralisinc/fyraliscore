"""Asyncpg persistence adapter for admitted business relations."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid5

from .contracts import (
    DirectionAssertion,
    RelationDisposition,
    RelationEvidence,
    RelationKind,
    RelationLifecycle,
    RelationParticipant,
    RelationVersion,
)
from .service import (
    AdmitRelationCommand,
    RelationCommandReceipt,
    RelationHead,
)


class AsyncpgRelationKernelStorage:
    """Store relation-kernel state in a transaction owned by the caller."""

    async def find_receipt(
        self, *, tx: Any, tenant_id: UUID, idempotency_key: str
    ) -> RelationCommandReceipt | None:
        row = await tx.fetchrow(
            """
            SELECT command_id, tenant_id, idempotency_key, request_digest,
                   outcome, result_relation_version_id, rejection_code, recorded_at
            FROM truth_command_receipts
            WHERE tenant_id=$1 AND idempotency_key=$2
              AND command_kind IN ('admit_relation','transition_relation')
            """,
            tenant_id,
            idempotency_key,
        )
        if row is None:
            return None
        code = row["rejection_code"]
        disposition = (
            RelationDisposition.ACCEPTED
            if row["result_relation_version_id"] is not None
            else RelationDisposition.NEEDS_REVIEW
            if code and code.startswith("RELATION_KIND_UNKNOWN")
            else RelationDisposition.REJECTED
        )
        return RelationCommandReceipt(
            command_id=row["command_id"], tenant_id=row["tenant_id"],
            idempotency_key=row["idempotency_key"], request_digest=row["request_digest"],
            outcome=row["outcome"], disposition=disposition,
            relation_version_id=row["result_relation_version_id"],
            rejection_code=code, recorded_at=row["recorded_at"],
        )

    async def record_candidate_decision(
        self, *, tx: Any, command: AdmitRelationCommand,
        disposition: RelationDisposition, reason_codes: tuple[str, ...],
        version: RelationVersion | None,
    ) -> None:
        candidate = command.candidate
        await tx.execute(
            """
            INSERT INTO relation_truth_admission_decisions (
              decision_id, tenant_id, candidate_relation_id, candidate_digest,
              disposition, reason_codes, decided_by, decided_at,
              admitted_relation_version_id
            ) VALUES ($1,$2,$3,$4,$5,$6,'relation_truth_kernel',$7,$8)
            """,
            command.admission_decision_id, candidate.tenant_id,
            candidate.candidate_relation_id, candidate.candidate_digest,
            disposition.value, list(reason_codes), command.issued_at,
            version.relation_version_id if version else None,
        )
        if version is not None:
            await self._insert_version_rows(tx=tx, version=version)

    async def insert_initial_head(self, *, tx: Any, head: RelationHead) -> None:
        await tx.execute(
            """
            INSERT INTO relation_truth_heads (
              tenant_id, relation_id, relation_version_id, version,
              semantic_digest, lifecycle, advanced_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
            head.tenant_id, head.relation_id, head.relation_version_id,
            head.version, head.semantic_digest, head.lifecycle.value, head.advanced_at,
        )

    async def lock_head(
        self, *, tx: Any, tenant_id: UUID, relation_id: UUID
    ) -> RelationHead | None:
        row = await tx.fetchrow(
            """
            SELECT tenant_id, relation_id, relation_version_id, version,
                   semantic_digest, lifecycle, advanced_at
            FROM relation_truth_heads
            WHERE tenant_id=$1 AND relation_id=$2 FOR UPDATE
            """,
            tenant_id, relation_id,
        )
        return self._head(row) if row else None

    async def load_version(
        self, *, tx: Any, tenant_id: UUID, relation_version_id: UUID
    ) -> RelationVersion:
        row = await tx.fetchrow(
            """
            SELECT relation_version_id, relation_id, tenant_id, version,
                   admission_decision_id, relation_kind, lifecycle, rationale,
                   supersedes_relation_version_id, created_at, semantic_digest
            FROM relation_truth_versions
            WHERE tenant_id=$1 AND relation_version_id=$2
            """,
            tenant_id, relation_version_id,
        )
        if row is None:
            raise LookupError(f"relation version {relation_version_id} does not exist")
        participant_rows = await tx.fetch(
            """
            SELECT model_id, model_version_id, role, ordinal
            FROM relation_truth_participants
            WHERE tenant_id=$1 AND relation_version_id=$2
            ORDER BY ordinal, role
            """,
            tenant_id, relation_version_id,
        )
        evidence_rows = await tx.fetch(
            """
            SELECT evidence_reference_id, model_version_id, evidence_digest,
                   polarity, weight
            FROM relation_truth_evidence
            WHERE tenant_id=$1 AND relation_version_id=$2
            ORDER BY evidence_reference_id
            """,
            tenant_id, relation_version_id,
        )
        participants = tuple(RelationParticipant(**dict(item)) for item in participant_rows)
        evidence = tuple(RelationEvidence(**dict(item)) for item in evidence_rows)
        kind = RelationKind(row["relation_kind"])
        by_role = {item.role: item for item in participants}
        roles = {RelationKind.CAUSAL_INFLUENCE: ("cause", "effect"), RelationKind.DEPENDENCY_CONSTRAINT: ("dependent", "prerequisite"), RelationKind.ENABLEMENT: ("enabler", "enabled"), RelationKind.PREDICTIVE_INDICATOR: ("indicator", "outcome")}[kind]
        return RelationVersion(
            relation_version_id=row["relation_version_id"], relation_id=row["relation_id"],
            tenant_id=row["tenant_id"], version=int(row["version"]),
            admission_decision_id=row["admission_decision_id"], kind=kind,
            lifecycle=RelationLifecycle(row["lifecycle"]), participants=participants,
            rationale=row["rationale"], assertion=DirectionAssertion(
                kind=kind, source_model_version_id=by_role[roles[0]].model_version_id,
                target_model_version_id=by_role[roles[1]].model_version_id, polarity=1,
            ), evidence=evidence,
            supersedes_relation_version_id=row["supersedes_relation_version_id"],
            created_at=row["created_at"], semantic_digest=row["semantic_digest"],
        )

    async def validate_active_participants(
        self, *, tx: Any, tenant_id: UUID,
        participants: tuple[RelationParticipant, ...],
    ) -> tuple[str, ...]:
        rows = await tx.fetch(
            """
            SELECT id, truth_version_id
            FROM accepted_current_models
            WHERE tenant_id=$1 AND truth_version_id = ANY($2::uuid[])
            """,
            tenant_id, [item.model_version_id for item in participants],
        )
        active = {(row["id"], row["truth_version_id"]) for row in rows}
        return tuple(
            f"{item.role}:{item.model_id}:{item.model_version_id}"
            for item in participants
            if (item.model_id, item.model_version_id) not in active
        )

    async def insert_version(self, *, tx: Any, version: RelationVersion) -> None:
        await self._insert_version_rows(tx=tx, version=version)

    async def _insert_version_rows(self, *, tx: Any, version: RelationVersion) -> None:
        await tx.execute(
            """
            INSERT INTO relation_truth_versions (
              relation_version_id, tenant_id, relation_id, version,
              admission_decision_id, relation_kind, lifecycle, rationale,
              semantic_digest, supersedes_relation_version_id, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
            version.relation_version_id, version.tenant_id, version.relation_id,
            version.version, version.admission_decision_id, version.kind.value,
            version.lifecycle.value, version.rationale, version.semantic_digest,
            version.supersedes_relation_version_id, version.created_at,
        )
        await tx.executemany(
            """
            INSERT INTO relation_truth_participants (
              participant_id, tenant_id, relation_version_id, model_id,
              model_version_id, role, ordinal, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """,
            [(uuid5(version.relation_version_id, f"participant:{p.role}:{p.ordinal}"),
              version.tenant_id, version.relation_version_id, p.model_id,
              p.model_version_id, p.role, p.ordinal, version.created_at)
             for p in version.participants],
        )
        await tx.executemany(
            """
            INSERT INTO relation_truth_evidence (
              relation_evidence_id, tenant_id, relation_version_id,
              evidence_reference_id, model_version_id, polarity, weight,
              evidence_digest, created_at
            ) SELECT $1,$2,$3,
                     CASE WHEN $4::text = 'unused' AND ($7::uuid IS NULL OR EXISTS (
                       SELECT 1 FROM relation_truth_admission_decisions
                        WHERE tenant_id=$2 AND admitted_relation_version_id=$7
                     )) THEN 'admit_relation' ELSE 'transition_relation' END,
                     $5,$6,$7,$8,$9
            """,
            [(uuid5(version.relation_version_id, f"evidence:{e.evidence_reference_id}"),
              version.tenant_id, version.relation_version_id,
              e.evidence_reference_id, e.model_version_id, e.polarity,
              e.weight, e.evidence_digest, version.created_at)
             for e in version.evidence],
        )

    async def compare_and_swap_head(
        self, *, tx: Any, expected: RelationHead, successor: RelationHead
    ) -> bool:
        result = await tx.execute(
            """
            UPDATE relation_truth_heads
            SET relation_version_id=$3, version=$4, semantic_digest=$5,
                lifecycle=$6, advanced_at=$7
            WHERE tenant_id=$1 AND relation_id=$2
              AND relation_version_id=$8 AND version=$9
              AND semantic_digest=$10 AND lifecycle=$11 AND advanced_at=$12
            """,
            successor.tenant_id, successor.relation_id,
            successor.relation_version_id, successor.version,
            successor.semantic_digest, successor.lifecycle.value,
            successor.advanced_at, expected.relation_version_id,
            expected.version, expected.semantic_digest, expected.lifecycle.value,
            expected.advanced_at,
        )
        return result == "UPDATE 1"

    async def insert_receipt(
        self, *, tx: Any, receipt: RelationCommandReceipt
    ) -> None:
        is_admission = receipt.relation_version_id is None or await tx.fetchval(
            """
            SELECT EXISTS (
              SELECT 1 FROM relation_truth_admission_decisions
              WHERE tenant_id=$1 AND admitted_relation_version_id=$2
            )
            """,
            receipt.tenant_id,
            receipt.relation_version_id,
        )
        await tx.execute(
            """
            INSERT INTO truth_command_receipts (
              command_id, tenant_id, idempotency_key, command_kind,
              request_digest, outcome, result_relation_version_id,
              rejection_code, recorded_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            receipt.command_id, receipt.tenant_id, receipt.idempotency_key,
            "admit_relation" if is_admission else "transition_relation",
            receipt.request_digest, receipt.outcome, receipt.relation_version_id,
            receipt.rejection_code, receipt.recorded_at,
        )

    async def dispute_for_invalidated_evidence(
        self, *, tx: Any, tenant_id: UUID, invalidated_model_version_id: UUID,
        cause_code: str, occurred_at: Any,
    ) -> tuple[UUID, ...]:
        rows = await tx.fetch(
            """
            WITH affected AS (
              SELECT h.*, v.admission_decision_id, v.relation_kind, v.rationale
              FROM relation_truth_heads h
              JOIN relation_truth_versions v USING (tenant_id, relation_version_id)
              WHERE h.tenant_id=$1 AND h.lifecycle='active'
                AND EXISTS (
                  SELECT 1 FROM relation_truth_evidence e
                  WHERE e.tenant_id=h.tenant_id
                    AND e.relation_version_id=h.relation_version_id
                    AND e.model_version_id=$2
                )
              FOR UPDATE OF h
            ), inserted_versions AS (
              INSERT INTO relation_truth_versions (
                relation_version_id, tenant_id, relation_id, version,
                admission_decision_id, relation_kind, lifecycle, rationale,
                semantic_digest, supersedes_relation_version_id, created_at
              ) SELECT gen_random_uuid(), tenant_id, relation_id, version+1,
                       admission_decision_id, relation_kind, 'disputed', rationale,
                       semantic_digest, relation_version_id, $4
                FROM affected
              RETURNING *
            ), copied_participants AS (
              INSERT INTO relation_truth_participants (
                participant_id, tenant_id, relation_version_id, model_id,
                model_version_id, role, ordinal, created_at
              ) SELECT gen_random_uuid(), p.tenant_id, n.relation_version_id,
                       p.model_id, p.model_version_id, p.role, p.ordinal, $4
                FROM inserted_versions n
                JOIN relation_truth_participants p
                  ON p.tenant_id=n.tenant_id
                 AND p.relation_version_id=n.supersedes_relation_version_id
            ), copied_evidence AS (
              INSERT INTO relation_truth_evidence (
                relation_evidence_id, tenant_id, relation_version_id,
                evidence_reference_id, model_version_id, polarity, weight,
                evidence_digest, created_at
              ) SELECT gen_random_uuid(), e.tenant_id, n.relation_version_id,
                       e.evidence_reference_id, e.model_version_id, e.polarity,
                       e.weight, e.evidence_digest, $4
                FROM inserted_versions n
                JOIN relation_truth_evidence e
                  ON e.tenant_id=n.tenant_id
                 AND e.relation_version_id=n.supersedes_relation_version_id
            ), advanced AS (
              UPDATE relation_truth_heads h
                 SET relation_version_id=n.relation_version_id,
                     version=n.version, lifecycle='disputed', advanced_at=$4
                FROM inserted_versions n
               WHERE h.tenant_id=n.tenant_id AND h.relation_id=n.relation_id
                 AND h.relation_version_id=n.supersedes_relation_version_id
              RETURNING n.supersedes_relation_version_id AS affected_id
            ), obligations AS (
              INSERT INTO truth_repair_obligations (
                obligation_id, tenant_id, invalidated_model_version_id,
                affected_kind, affected_id, cause_code, created_at
              ) SELECT gen_random_uuid(), $1, $2, 'relation_version',
                       affected_id, $3, $4 FROM advanced
              ON CONFLICT (tenant_id, invalidated_model_version_id,
                           affected_kind, affected_id, cause_code) DO NOTHING
              RETURNING affected_id
            )
            SELECT affected_id FROM obligations
            UNION
            SELECT affected_id FROM truth_repair_obligations
             WHERE tenant_id=$1 AND invalidated_model_version_id=$2
               AND affected_kind='relation_version' AND cause_code=$3
            ORDER BY affected_id
            """,
            tenant_id, invalidated_model_version_id, cause_code, occurred_at,
        )
        return tuple(row["affected_id"] for row in rows)

    @staticmethod
    def _head(row: Any) -> RelationHead:
        return RelationHead(
            tenant_id=row["tenant_id"], relation_id=row["relation_id"],
            relation_version_id=row["relation_version_id"], version=int(row["version"]),
            semantic_digest=row["semantic_digest"],
            lifecycle=RelationLifecycle(row["lifecycle"]), advanced_at=row["advanced_at"],
        )


__all__ = ["AsyncpgRelationKernelStorage"]
