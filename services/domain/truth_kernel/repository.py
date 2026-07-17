"""Asyncpg persistence adapter for the P2 Model truth kernel."""

from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID, uuid5

from lib.contracts.truth_admission import (
    AdmitModelCommand,
    ModelHead,
    ModelTruthLifecycle,
    ModelTruthTransition,
    ModelVersion,
)
from services.domain.truth_kernel.service import TruthCommandReceipt


MODEL_HEAD_CAS_SQL_TEMPLATE = """
            UPDATE {head_relation}
            SET version_id=$3, version=$4, semantic_digest=$5,
                lifecycle=$6, advanced_at=$7
            WHERE tenant_id=$1 AND model_id=$2 AND version_id=$8
              AND version=$9 AND semantic_digest=$10 AND lifecycle=$11
            """


def render_model_head_cas_sql(head_relation: str = "model_truth_heads") -> str:
    """Render the production CAS statement for a trusted relation name."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?", head_relation):
        raise ValueError("invalid CAS head relation")
    return MODEL_HEAD_CAS_SQL_TEMPLATE.format(head_relation=head_relation)


class AsyncpgTruthKernelStorage:
    """Persist one command through the transaction supplied by the caller."""

    async def find_receipt(
        self, *, tx: Any, tenant_id: UUID, idempotency_key: str
    ) -> TruthCommandReceipt | None:
        row = await tx.fetchrow(
            """
            SELECT r.command_id, r.tenant_id, r.idempotency_key,
                   r.command_kind, r.request_digest, r.outcome, r.recorded_at,
                   v.model_id, v.version_id, v.version,
                   v.semantic_digest, v.lifecycle
            FROM truth_command_receipts r
            JOIN model_truth_versions v
              ON v.tenant_id = r.tenant_id
             AND v.version_id = r.result_model_version_id
            WHERE r.tenant_id = $1 AND r.idempotency_key = $2
            """,
            tenant_id,
            idempotency_key,
        )
        if row is None:
            return None
        return TruthCommandReceipt(
            command_id=row["command_id"],
            tenant_id=row["tenant_id"],
            idempotency_key=row["idempotency_key"],
            request_digest=row["request_digest"],
            operation=("admit" if row["command_kind"] == "admit_model" else "advance"),
            model_id=row["model_id"],
            version_id=row["version_id"],
            version=int(row["version"]),
            semantic_digest=row["semantic_digest"],
            lifecycle=ModelTruthLifecycle(row["lifecycle"]),
            applied_at=row["recorded_at"],
            outcome=(
                "absorbed_duplicate"
                if row["outcome"] == "absorbed_duplicate"
                else "applied"
            ),
        )

    async def lock_semantic_admission(
        self, *, tx: Any, tenant_id: UUID, semantic_digest: str
    ) -> None:
        await tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"truth-semantic-admission:{tenant_id}:{semantic_digest}",
        )

    async def find_active_semantic_head(
        self, *, tx: Any, tenant_id: UUID, semantic_digest: str
    ) -> ModelHead | None:
        row = await tx.fetchrow(
            """
            SELECT tenant_id, model_id, version_id, version,
                   semantic_digest, lifecycle, advanced_at
            FROM model_truth_heads
            WHERE tenant_id=$1 AND semantic_digest=$2 AND lifecycle='active'
            ORDER BY advanced_at, version_id
            LIMIT 1
            FOR UPDATE
            """,
            tenant_id,
            semantic_digest,
        )
        return self._head(row) if row else None

    async def insert_admission_bundle(
        self, *, tx: Any, command: AdmitModelCommand
    ) -> None:
        candidate, decision, version = (
            command.candidate,
            command.decision,
            command.version,
        )
        await tx.execute(
            """
            INSERT INTO truth_candidates (
              candidate_id, candidate_version, tenant_id, kind, review_state,
              natural_text, proposition, candidate_digest, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
            """,
            candidate.candidate_id,
            candidate.candidate_version,
            candidate.tenant_id,
            candidate.kind.value,
            candidate.review_state.value,
            candidate.natural,
            json.dumps(candidate.proposition),
            candidate.candidate_digest,
            candidate.created_at,
        )
        await tx.execute(
            """
            INSERT INTO truth_admission_decisions (
              decision_id, tenant_id, candidate_id, candidate_version,
              candidate_digest, disposition, reason_codes, decided_by,
              decided_at, admitted_model_id, admitted_version_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
            decision.decision_id,
            decision.tenant_id,
            decision.candidate_id,
            decision.candidate_version,
            decision.candidate_digest,
            decision.disposition.value,
            list(decision.reason_codes),
            decision.decided_by,
            decision.decided_at,
            decision.admitted_model_id,
            decision.admitted_version_id,
        )
        await self._insert_version_rows(tx=tx, version=version, supersedes=None)
        await self._insert_legacy_read_projection(tx=tx, version=version)

    async def _insert_legacy_read_projection(
        self, *, tx: Any, version: ModelVersion
    ) -> None:
        """Materialize the temporary full-ModelRow compatibility payload.

        Canonical membership and lifecycle come exclusively from truth heads;
        this row supplies legacy retrieval fields until their derived sidecars
        are rebuilt directly from immutable truth versions.
        """

        observations = [
            item for item in version.evidence if item.kind.value == "observation"
        ]
        born_from_event_id = (
            _uuid_or_reference(observations[0].evidence_id, observations[0].reference_id)
            if observations
            else version.evidence[0].reference_id
        )
        scope_actors = [
            binding.subject_id
            for binding in version.scope
            if binding.subject_kind.value == "person"
            or binding.role.value == "actor"
        ]
        scope_entities = [
            {
                "type": binding.subject_kind.value,
                "id": str(binding.subject_id),
                "role": binding.role.value,
            }
            for binding in version.scope
        ]
        compatibility_proposition = dict(version.proposition)
        compatibility_proposition.setdefault("kind", "belief")
        # The old ModelRow surface exposed the source canonical reference, not
        # the kernel's UUID-normalized scope subject. Preserve that read shape
        # when the admitted proposition carries grounding continuity. This is
        # projection logic only; canonical scope remains the typed binding.
        continuity = compatibility_proposition.get("grounding_continuity")
        selected = (
            continuity.get("selected_referent")
            if isinstance(continuity, dict)
            else None
        )
        if isinstance(selected, dict) and selected.get("referent_id"):
            referent_id = str(selected["referent_id"])
            scope_entities = [
                {
                    "type": referent_id.partition(":")[0] or "other",
                    "id": referent_id,
                    "version": int(selected.get("referent_version", 1)),
                }
            ]
        await tx.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, activation,
              evidential_weight, status, created_at
            ) VALUES (
              $1,$2,$3,$4::jsonb,$5,
              array_fill(0.0::real, ARRAY[768])::vector,
              $6,$7::jsonb,'{}'::jsonb,0.5,0.5,1.0,0.5,'active',$8
            )
            ON CONFLICT (id) DO NOTHING
            """,
            version.model_id,
            version.tenant_id,
            born_from_event_id,
            json.dumps(compatibility_proposition),
            version.natural,
            scope_actors,
            json.dumps(scope_entities),
            version.created_at,
        )

    async def lock_head(
        self, *, tx: Any, tenant_id: UUID, model_id: UUID
    ) -> ModelHead | None:
        row = await tx.fetchrow(
            """
            SELECT tenant_id, model_id, version_id, version,
                   semantic_digest, lifecycle, advanced_at
            FROM model_truth_heads
            WHERE tenant_id = $1 AND model_id = $2
            FOR UPDATE
            """,
            tenant_id,
            model_id,
        )
        return self._head(row) if row else None

    async def insert_version(
        self, *, tx: Any, version: ModelVersion, prior_head: ModelHead
    ) -> None:
        await self._insert_version_rows(
            tx=tx, version=version, supersedes=prior_head.version_id
        )

    async def _insert_version_rows(
        self, *, tx: Any, version: ModelVersion, supersedes: UUID | None
    ) -> None:
        await tx.execute(
            """
            INSERT INTO model_truth_versions (
              version_id, tenant_id, model_id, version, admission_decision_id,
              source_candidate_id, source_candidate_version, natural_text,
              proposition, lifecycle, semantic_digest, supersedes_version_id,
              created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13)
            """,
            version.version_id,
            version.tenant_id,
            version.model_id,
            version.version,
            version.admission_decision_id,
            version.source_candidate_id,
            version.source_candidate_version,
            version.natural,
            json.dumps(version.proposition),
            version.lifecycle.value,
            version.semantic_digest,
            supersedes,
            version.created_at,
        )
        for item in version.evidence:
            coordinate, authority = item.coordinate, item.authority
            await tx.execute(
                """
                INSERT INTO model_truth_evidence_references (
                  reference_id, tenant_id, model_version_id, evidence_kind,
                  evidence_id, evidence_version, evidence_digest, evidence_role,
                  source_system, source_object_id, source_revision, field_path,
                  span_start, span_end, time_range_start, time_range_end,
                  authority_ref, policy_version, authority_epoch,
                  authority_decided_at, authority_expires_at, occurred_at,
                  recorded_at, cutoff_at, reference_digest
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                          $15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
                """,
                item.reference_id,
                version.tenant_id,
                version.version_id,
                item.kind.value,
                item.evidence_id,
                item.evidence_version,
                item.evidence_digest,
                item.role.value,
                coordinate.source_system,
                coordinate.source_object_id,
                coordinate.source_revision,
                coordinate.field_path,
                coordinate.span_start,
                coordinate.span_end,
                coordinate.time_range_start,
                coordinate.time_range_end,
                authority.authority_ref,
                authority.policy_version,
                authority.authority_epoch,
                authority.decided_at,
                authority.expires_at,
                item.occurred_at,
                item.recorded_at,
                item.cutoff_at,
                item.reference_digest,
            )
        for binding in version.scope:
            binding_id = uuid5(
                version.version_id,
                f"{binding.subject_id}:{binding.role.value}",
            )
            await tx.execute(
                """
                INSERT INTO model_truth_scope_bindings (
                  binding_id, tenant_id, model_version_id, subject_id,
                  subject_kind, scope_role, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                binding_id,
                version.tenant_id,
                version.version_id,
                binding.subject_id,
                binding.subject_kind.value,
                binding.role.value,
                version.created_at,
            )
            await tx.executemany(
                """
                INSERT INTO model_truth_scope_evidence (
                  tenant_id, model_version_id, binding_id, evidence_reference_id
                ) VALUES ($1,$2,$3,$4)
                """,
                [
                    (version.tenant_id, version.version_id, binding_id, reference_id)
                    for reference_id in binding.claim_local_evidence_refs
                ],
            )

    async def insert_initial_head(self, *, tx: Any, head: ModelHead) -> None:
        await tx.execute(
            """
            INSERT INTO model_truth_heads (
              tenant_id, model_id, version_id, version, semantic_digest,
              lifecycle, advanced_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
            head.tenant_id,
            head.model_id,
            head.version_id,
            head.version,
            head.semantic_digest,
            head.lifecycle.value,
            head.advanced_at,
        )

    async def compare_and_swap_head(
        self, *, tx: Any, expected: ModelHead, successor: ModelHead
    ) -> bool:
        result = await tx.execute(
            render_model_head_cas_sql(),
            successor.tenant_id,
            successor.model_id,
            successor.version_id,
            successor.version,
            successor.semantic_digest,
            successor.lifecycle.value,
            successor.advanced_at,
            expected.version_id,
            expected.version,
            expected.semantic_digest,
            expected.lifecycle.value,
        )
        return result == "UPDATE 1"

    async def append_event(
        self,
        *,
        tx: Any,
        operation: str,
        prior_head: ModelHead | None,
        successor: ModelHead,
        command_id: UUID,
        request_digest: str,
        transition: ModelTruthTransition | None,
        reason_codes: tuple[str, ...],
    ) -> None:
        del request_digest
        if prior_head is None:  # Admission is already represented by its decision.
            return
        if transition is None:
            raise ValueError("lifecycle event requires an explicit transition")
        await tx.execute(
            """
            INSERT INTO model_truth_lifecycle_events (
              lifecycle_event_id, tenant_id, model_id, command_id,
              from_version_id, to_version_id, transition, reason_codes,
              occurred_at
            ) VALUES (gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8)
            """,
            successor.tenant_id,
            successor.model_id,
            command_id,
            prior_head.version_id,
            successor.version_id,
            transition.value,
            list(reason_codes),
            successor.advanced_at,
        )

    async def insert_receipt(
        self, *, tx: Any, receipt: TruthCommandReceipt
    ) -> None:
        await tx.execute(
            """
            INSERT INTO truth_command_receipts (
              command_id, tenant_id, idempotency_key, command_kind,
              request_digest, outcome, result_model_version_id, recorded_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """,
            receipt.command_id,
            receipt.tenant_id,
            receipt.idempotency_key,
            "admit_model" if receipt.operation == "admit" else "transition_model",
            receipt.request_digest,
            receipt.outcome,
            receipt.version_id,
            receipt.applied_at,
        )

    async def insert_semantic_absorption(
        self,
        *,
        tx: Any,
        command: AdmitModelCommand,
        receipt: TruthCommandReceipt,
    ) -> None:
        if receipt.outcome != "absorbed_duplicate":
            raise ValueError("semantic absorption requires an absorbed receipt")
        await tx.execute(
            """
            INSERT INTO truth_semantic_absorptions (
              command_id, tenant_id, request_digest, semantic_digest,
              attempted_candidate_id, attempted_model_id,
              attempted_version_id, absorbed_into_version_id,
              attempted_command, recorded_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
            """,
            command.command_id,
            command.tenant_id,
            command.request_digest,
            command.version.semantic_digest,
            command.candidate.candidate_id,
            command.version.model_id,
            command.version.version_id,
            receipt.version_id,
            json.dumps(command.model_dump(mode="json")),
            receipt.applied_at,
        )

    @staticmethod
    def _head(row: Any) -> ModelHead:
        return ModelHead(
            tenant_id=row["tenant_id"],
            model_id=row["model_id"],
            version_id=row["version_id"],
            version=int(row["version"]),
            semantic_digest=row["semantic_digest"],
            lifecycle=ModelTruthLifecycle(row["lifecycle"]),
            advanced_at=row["advanced_at"],
        )


__all__ = [
    "AsyncpgTruthKernelStorage", "MODEL_HEAD_CAS_SQL_TEMPLATE",
    "render_model_head_cas_sql",
]


def _uuid_or_reference(value: str, fallback: UUID) -> UUID:
    try:
        return UUID(value)
    except (TypeError, ValueError):
        return fallback
