"""Transactional ConcernApplier and attention-governance binding registry.

The caller owns the database transaction.  One apply atomically commits the
Concern snapshot, total-reducer transition, command result, canonical event and
outbox.  Identity correction writes predecessor and deterministic successor in
the same transaction; there is no saga or in-place dedupe-key edit.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.contracts.agency import (
    AttentionGovernanceBinding,
    ConcernCriterionState,
    ConcernEvaluationCommand,
    ConcernIdentityCorrectionCommand,
    ConcernSnapshot,
    ConcernState,
    ConcernTransition,
    EffectiveAttentionGovernanceEnvelope,
    compose_attention_governance_bindings,
    reduce_concern_state,
)
from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7


@dataclass(frozen=True)
class ConcernApplyResult:
    command_result_id: UUID
    concern_id: UUID
    aggregate_version: int
    state: ConcernState
    duplicate: bool = False


@dataclass(frozen=True)
class ConcernIdentityCorrectionResult:
    command_result_id: UUID
    predecessor_concern_id: UUID
    predecessor_version: int
    successor_concern_id: UUID
    successor_version: int
    duplicate: bool = False


class AttentionGovernanceBindingRegistry:
    """Append immutable binding versions used by Criteria and Concern."""

    async def register(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        binding: AttentionGovernanceBinding,
        registered_by_ref: str,
    ) -> tuple[AttentionGovernanceBinding, bool]:
        if not registered_by_ref.strip():
            raise ValidationError("attention binding registrar is required")
        payload = binding.model_dump(mode="json")
        inserted = await conn.fetchrow(
            """
            INSERT INTO attention_governance_bindings (
                tenant_id, binding_id, binding_version, binding_ref,
                attention_source_ref, attention_source_kind, binding_digest,
                binding, valid_from, valid_until, registered_by_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
            )
            ON CONFLICT (tenant_id, binding_ref) DO NOTHING
            RETURNING binding
            """,
            tenant_id,
            binding.binding_id,
            binding.binding_version,
            binding.binding_ref,
            binding.attention_source_ref,
            binding.attention_source_kind.value,
            binding.binding_digest,
            json.dumps(payload),
            binding.valid_from,
            binding.valid_until,
            registered_by_ref,
        )
        if inserted is not None:
            return binding, True
        existing = await conn.fetchrow(
            """
            SELECT binding, binding_digest
            FROM attention_governance_bindings
            WHERE tenant_id = $1 AND binding_ref = $2
            """,
            tenant_id,
            binding.binding_ref,
        )
        if existing is None:
            raise InvariantViolation(
                "ATTENTION_BINDING_IDEMPOTENCY",
                "binding conflict disappeared during idempotency check",
            )
        parsed = AttentionGovernanceBinding.model_validate(_json(existing["binding"]))
        if parsed != binding or existing["binding_digest"] != binding.binding_digest:
            raise InvariantViolation(
                "ATTENTION_BINDING_IMMUTABILITY",
                "one attention binding reference was reused for different content",
                binding_ref=binding.binding_ref,
            )
        return parsed, False


class ConcernApplier:
    """Only writer for scoped-gap identity and Concern lifecycle."""

    async def get_snapshot(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        concern_id: UUID,
        for_update: bool = False,
    ) -> ConcernSnapshot | None:
        lock = " FOR UPDATE" if for_update else ""
        row = await conn.fetchrow(
            f"""
            SELECT v.snapshot
            FROM concern_heads h
            JOIN concern_versions v
              ON v.tenant_id = h.tenant_id
             AND v.concern_id = h.concern_id
             AND v.aggregate_version = h.current_version
            WHERE h.tenant_id = $1 AND h.concern_id = $2
            {lock}
            """,
            tenant_id,
            concern_id,
        )
        return ConcernSnapshot.model_validate(_json(row["snapshot"])) if row else None

    async def apply_evaluation(
        self,
        *,
        conn: asyncpg.Connection,
        command: ConcernEvaluationCommand,
        now: datetime | None = None,
    ) -> ConcernApplyResult:
        now = now or datetime.now(timezone.utc)
        self._validate_live_command(command, now=now)
        prior_result = await self._prior_result(
            conn=conn,
            tenant_id=command.tenant_id,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior_result is not None:
            result = _json(prior_result["result"])
            return ConcernApplyResult(
                command_result_id=prior_result["id"],
                concern_id=UUID(result["concern_id"]),
                aggregate_version=int(result["aggregate_version"]),
                state=ConcernState(result["state"]),
                duplicate=True,
            )

        head = await conn.fetchrow(
            """
            SELECT * FROM concern_heads
            WHERE tenant_id = $1 AND concern_id = $2
            FOR UPDATE
            """,
            command.tenant_id,
            command.concern_id,
        )
        collision = await conn.fetchrow(
            """
            SELECT concern_id FROM concern_heads
            WHERE tenant_id = $1 AND dedupe_key = $2 AND concern_id <> $3
            FOR UPDATE
            """,
            command.tenant_id,
            command.identity.dedupe_key,
            command.concern_id,
        )
        if collision is not None:
            raise InvariantViolation(
                "CONCERN_SCOPED_GAP_UNIQUENESS",
                "scoped gap already belongs to a different Concern aggregate",
                existing_concern_id=str(collision["concern_id"]),
            )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "CONCERN_CAS",
                "Concern expected version does not match current head",
                expected_version=command.expected_version,
                current_version=current_version,
            )
        prior_snapshot = (
            await self.get_snapshot(
                conn=conn,
                tenant_id=command.tenant_id,
                concern_id=command.concern_id,
            )
            if head
            else None
        )
        criteria = self._merge_criteria(
            prior_snapshot.criteria if prior_snapshot else (), command.criteria
        )
        bindings, envelope = await self._validate_bindings(
            conn=conn,
            tenant_id=command.tenant_id,
            criteria=criteria,
            changed_criteria=command.criteria,
            at=now,
            prior_concern_id=command.concern_id if prior_snapshot else None,
            authorized_capability_refs=command.consumption_authority.authority_basis_refs,
        )
        del bindings
        next_version = current_version + 1
        if prior_snapshot is None:
            state = ConcernState.CANDIDATE
            contributor_refs = frozenset(item.attention_source_ref for item in criteria)
            origin = command.originating_attention_source_ref
        else:
            state = reduce_concern_state(
                criteria=criteria,
                at=now,
                gap_identity_valid=command.gap_identity_valid,
                validity_deadline=command.validity_deadline,
            )
            contributor_refs = prior_snapshot.contributing_attention_source_refs | frozenset(
                item.attention_source_ref for item in criteria
            )
            origin = prior_snapshot.originating_attention_source_ref
        snapshot = ConcernSnapshot(
            concern_id=command.concern_id,
            aggregate_version=next_version,
            identity=command.identity,
            declared_dedupe_key=command.identity.dedupe_key,
            originating_attention_source_ref=origin,
            contributing_attention_source_refs=contributor_refs,
            criteria=criteria,
            current_state_estimate=command.current_state_estimate,
            materiality=command.materiality,
            uncertainty=command.uncertainty,
            consequence=command.consequence,
            urgency=command.urgency,
            actionability=command.actionability,
            evidence_cutoff=command.evidence_cutoff,
            validity_deadline=command.validity_deadline,
            next_review_at=command.next_review_at,
            gap_identity_valid=command.gap_identity_valid,
            state=state,
            transition_cause=command.transition_cause,
        )
        transition = ConcernTransition(
            concern_id=command.concern_id,
            from_version=current_version,
            to_version=next_version,
            from_state=prior_snapshot.state if prior_snapshot else None,
            to_state=state,
            cause=command.transition_cause,
            transitioned_at=now,
        )
        result_id = uuid7()
        result_payload = {
            "concern_id": str(command.concern_id),
            "aggregate_version": next_version,
            "state": state.value,
            "snapshot_digest": canonical_sha256(snapshot.model_dump(mode="json")),
            "effective_binding_digest": envelope.envelope_digest,
        }
        await self._insert_command_result(
            conn=conn,
            result_id=result_id,
            tenant_id=command.tenant_id,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
            command_kind="evaluate",
            command_payload=command.model_dump(mode="json"),
            processing_authority_fingerprint=command.processing_authority.fingerprint,
            consumption_authority_fingerprint=command.consumption_authority.fingerprint,
            writer_scope_id=command.writer_scope_epoch.scope_id,
            writer_epoch=command.writer_scope_epoch.epoch,
            aggregate_versions=[
                {"concern_id": str(command.concern_id), "version": next_version}
            ],
            result_payload=result_payload,
        )
        await self._commit_snapshot(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=snapshot,
            envelope=envelope,
            transition=transition,
            result_id=result_id,
            expected_version=current_version,
            predecessor_concern_id=head["predecessor_concern_id"] if head else None,
            successor_concern_id=head["successor_concern_id"] if head else None,
        )
        await self._record_event_and_outbox(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=snapshot,
            result_id=result_id,
            semantic_transition=f"concern.{state.value}",
            now=now,
        )
        return ConcernApplyResult(
            command_result_id=result_id,
            concern_id=command.concern_id,
            aggregate_version=next_version,
            state=state,
        )

    async def correct_identity(
        self,
        *,
        conn: asyncpg.Connection,
        command: ConcernIdentityCorrectionCommand,
        now: datetime | None = None,
    ) -> ConcernIdentityCorrectionResult:
        now = now or datetime.now(timezone.utc)
        self._validate_live_command(command.successor, now=now)
        prior_result = await self._prior_result(
            conn=conn,
            tenant_id=command.tenant_id,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior_result is not None:
            result = _json(prior_result["result"])
            return ConcernIdentityCorrectionResult(
                command_result_id=prior_result["id"],
                predecessor_concern_id=UUID(result["predecessor_concern_id"]),
                predecessor_version=int(result["predecessor_version"]),
                successor_concern_id=UUID(result["successor_concern_id"]),
                successor_version=int(result["successor_version"]),
                duplicate=True,
            )
        rows = await conn.fetch(
            """
            SELECT * FROM concern_heads
            WHERE tenant_id = $1 AND concern_id = ANY($2::uuid[])
            ORDER BY concern_id
            FOR UPDATE
            """,
            command.tenant_id,
            sorted(
                (command.predecessor_concern_id, command.successor.concern_id),
                key=str,
            ),
        )
        heads = {row["concern_id"]: row for row in rows}
        predecessor_head = heads.get(command.predecessor_concern_id)
        if predecessor_head is None:
            raise ValidationError(
                "predecessor Concern not found",
                concern_id=str(command.predecessor_concern_id),
            )
        if int(predecessor_head["current_version"]) != command.expected_predecessor_version:
            raise InvariantViolation(
                "CONCERN_CORRECTION_CAS",
                "predecessor expected version does not match current head",
            )
        if predecessor_head["current_state"] == ConcernState.INVALIDATED.value:
            raise InvariantViolation(
                "CONCERN_CORRECTION_TERMINAL",
                "an invalidated Concern cannot acquire a different successor",
            )
        if command.successor.concern_id in heads:
            raise InvariantViolation(
                "CONCERN_CORRECTION_SUCCESSOR_COLLISION",
                "corrected scoped gap already has a Concern aggregate",
            )
        predecessor = await self.get_snapshot(
            conn=conn,
            tenant_id=command.tenant_id,
            concern_id=command.predecessor_concern_id,
        )
        if predecessor is None:
            raise InvariantViolation(
                "CONCERN_CORRECTION_HEAD",
                "predecessor head has no current snapshot",
            )
        predecessor_envelope_row = await conn.fetchrow(
            """
            SELECT effective_binding_envelope
            FROM concern_versions
            WHERE tenant_id = $1 AND concern_id = $2 AND aggregate_version = $3
            """,
            command.tenant_id,
            predecessor.concern_id,
            predecessor.aggregate_version,
        )
        predecessor_envelope = EffectiveAttentionGovernanceEnvelope.model_validate(
            _json(predecessor_envelope_row["effective_binding_envelope"])
        )
        _, successor_envelope = await self._validate_bindings(
            conn=conn,
            tenant_id=command.tenant_id,
            criteria=command.successor.criteria,
            changed_criteria=command.successor.criteria,
            at=now,
            prior_concern_id=None,
            authorized_capability_refs=(
                command.successor.consumption_authority.authority_basis_refs
            ),
        )
        predecessor_version = predecessor.aggregate_version + 1
        invalidated = ConcernSnapshot.model_validate(
            predecessor.model_dump(mode="python")
            | {
                "aggregate_version": predecessor_version,
                "gap_identity_valid": False,
                "state": ConcernState.INVALIDATED,
                "transition_cause": command.correction_reason,
            }
        )
        successor = ConcernSnapshot(
            concern_id=command.successor.concern_id,
            aggregate_version=1,
            identity=command.successor.identity,
            declared_dedupe_key=command.successor.identity.dedupe_key,
            originating_attention_source_ref=(
                command.successor.originating_attention_source_ref
            ),
            contributing_attention_source_refs=frozenset(
                item.attention_source_ref for item in command.successor.criteria
            ),
            criteria=tuple(
                sorted(command.successor.criteria, key=lambda item: item.criterion_ref)
            ),
            current_state_estimate=command.successor.current_state_estimate,
            materiality=command.successor.materiality,
            uncertainty=command.successor.uncertainty,
            consequence=command.successor.consequence,
            urgency=command.successor.urgency,
            actionability=command.successor.actionability,
            evidence_cutoff=command.successor.evidence_cutoff,
            validity_deadline=command.successor.validity_deadline,
            next_review_at=command.successor.next_review_at,
            gap_identity_valid=True,
            state=ConcernState.CANDIDATE,
            transition_cause=command.correction_reason,
        )
        predecessor_transition = ConcernTransition(
            concern_id=predecessor.concern_id,
            from_version=predecessor.aggregate_version,
            to_version=predecessor_version,
            from_state=predecessor.state,
            to_state=ConcernState.INVALIDATED,
            cause=command.correction_reason,
            transitioned_at=now,
        )
        successor_transition = ConcernTransition(
            concern_id=successor.concern_id,
            from_version=0,
            to_version=1,
            from_state=None,
            to_state=ConcernState.CANDIDATE,
            cause=command.correction_reason,
            transitioned_at=now,
        )
        result_id = uuid7()
        result_payload = {
            "predecessor_concern_id": str(predecessor.concern_id),
            "predecessor_version": predecessor_version,
            "successor_concern_id": str(successor.concern_id),
            "successor_version": 1,
        }
        successor_command = command.successor
        await self._insert_command_result(
            conn=conn,
            result_id=result_id,
            tenant_id=command.tenant_id,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
            command_kind="correct_identity",
            command_payload=command.model_dump(mode="json"),
            processing_authority_fingerprint=(
                successor_command.processing_authority.fingerprint
            ),
            consumption_authority_fingerprint=(
                successor_command.consumption_authority.fingerprint
            ),
            writer_scope_id=successor_command.writer_scope_epoch.scope_id,
            writer_epoch=successor_command.writer_scope_epoch.epoch,
            aggregate_versions=[
                {"concern_id": str(predecessor.concern_id), "version": predecessor_version},
                {"concern_id": str(successor.concern_id), "version": 1},
            ],
            result_payload=result_payload,
        )
        await self._commit_snapshot(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=invalidated,
            envelope=predecessor_envelope,
            transition=predecessor_transition,
            result_id=result_id,
            expected_version=predecessor.aggregate_version,
            predecessor_concern_id=predecessor_head["predecessor_concern_id"],
            successor_concern_id=successor.concern_id,
        )
        await self._commit_snapshot(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=successor,
            envelope=successor_envelope,
            transition=successor_transition,
            result_id=result_id,
            expected_version=0,
            predecessor_concern_id=predecessor.concern_id,
            successor_concern_id=None,
        )
        await conn.execute(
            """
            INSERT INTO concern_identity_corrections (
                id, tenant_id, predecessor_concern_id, predecessor_version,
                successor_concern_id, successor_version, correction_epoch,
                request_digest, correction_reason, command_result_id
            ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9)
            """,
            uuid7(),
            command.tenant_id,
            predecessor.concern_id,
            predecessor_version,
            successor.concern_id,
            command.correction_epoch,
            command.request_digest,
            command.correction_reason,
            result_id,
        )
        await self._record_event_and_outbox(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=invalidated,
            result_id=result_id,
            semantic_transition="concern.identity_invalidated",
            now=now,
        )
        await self._record_event_and_outbox(
            conn=conn,
            tenant_id=command.tenant_id,
            snapshot=successor,
            result_id=result_id,
            semantic_transition="concern.identity_successor_created",
            now=now,
        )
        return ConcernIdentityCorrectionResult(
            command_result_id=result_id,
            predecessor_concern_id=predecessor.concern_id,
            predecessor_version=predecessor_version,
            successor_concern_id=successor.concern_id,
            successor_version=1,
        )

    @staticmethod
    def _validate_live_command(command: ConcernEvaluationCommand, *, now: datetime) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValidationError("Concern commit time must be timezone-aware")
        if not command.issued_at <= now < command.expires_at:
            raise ValidationError("Concern command is not live at commit time")
        if not command.processing_authority.is_live(now):
            raise ValidationError("Concern processing authority is not live at commit time")
        if not command.consumption_authority.is_live(now):
            raise ValidationError("Concern consumption authority is not live at commit time")

    async def _prior_result(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        idempotency_key: str,
        request_digest: str,
    ) -> asyncpg.Record | None:
        row = await conn.fetchrow(
            """
            SELECT * FROM concern_command_results
            WHERE tenant_id = $1 AND semantic_idempotency_key = $2
            FOR UPDATE
            """,
            tenant_id,
            idempotency_key,
        )
        if row is None:
            return None
        if row["request_digest"] != request_digest:
            raise InvariantViolation(
                "CONCERN_COMMAND_IDEMPOTENCY",
                "one Concern idempotency key was reused for a different request",
            )
        if row["status"] not in {"applied", "duplicate"}:
            raise InvariantViolation(
                "CONCERN_COMMAND_PRIOR_FATE",
                "prior Concern command did not produce a reusable applied result",
            )
        return row

    @staticmethod
    def _merge_criteria(
        prior: Iterable[ConcernCriterionState],
        changed: Iterable[ConcernCriterionState],
    ) -> tuple[ConcernCriterionState, ...]:
        merged = {item.criterion_ref: item for item in prior}
        merged.update({item.criterion_ref: item for item in changed})
        return tuple(merged[key] for key in sorted(merged))

    async def _validate_bindings(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        criteria: tuple[ConcernCriterionState, ...],
        changed_criteria: tuple[ConcernCriterionState, ...],
        at: datetime,
        prior_concern_id: UUID | None,
        authorized_capability_refs: frozenset[str],
    ) -> tuple[dict[str, AttentionGovernanceBinding], EffectiveAttentionGovernanceEnvelope]:
        refs = sorted({item.attention_binding_ref for item in criteria})
        rows = await conn.fetch(
            """
            SELECT binding_ref, binding
            FROM attention_governance_bindings
            WHERE tenant_id = $1 AND binding_ref = ANY($2::text[])
            """,
            tenant_id,
            refs,
        )
        bindings = {
            row["binding_ref"]: AttentionGovernanceBinding.model_validate(
                _json(row["binding"])
            )
            for row in rows
        }
        missing = sorted(set(refs) - set(bindings))
        if missing:
            raise ValidationError(
                "Concern references missing attention governance bindings",
                missing_binding_refs=missing,
            )
        changed_refs = {item.criterion_ref for item in changed_criteria}
        for criterion in criteria:
            binding = bindings[criterion.attention_binding_ref]
            if binding.attention_source_ref != criterion.attention_source_ref:
                raise InvariantViolation(
                    "CONCERN_BINDING_SOURCE",
                    "criterion source does not match its attention binding",
                    criterion_ref=criterion.criterion_ref,
                )
            if criterion.applicable and not binding.is_live(at):
                raise ValidationError(
                    "applicable Concern criterion uses an inactive attention binding",
                    criterion_ref=criterion.criterion_ref,
                )
            if criterion.criterion_ref in changed_refs and criterion.disposition is not None:
                required = binding.disposition_capability_refs.get(criterion.disposition)
                if required is None or required != criterion.disposition_capability_ref:
                    raise InvariantViolation(
                        "CONCERN_DISPOSITION_CAPABILITY",
                        "criterion disposition lacks its source-specific capability",
                        criterion_ref=criterion.criterion_ref,
                    )
                if required not in authorized_capability_refs:
                    raise InvariantViolation(
                        "CONCERN_DISPOSITION_AUTHORITY",
                        "committing principal lacks the source-specific disposition capability",
                        criterion_ref=criterion.criterion_ref,
                    )
                if criterion.disposition_expires_at is None or criterion.disposition_expires_at <= at:
                    raise ValidationError(
                        "new Concern disposition is already expired",
                        criterion_ref=criterion.criterion_ref,
                    )
        active_bindings = {
            criterion.attention_binding_ref: bindings[criterion.attention_binding_ref]
            for criterion in criteria
            if criterion.applicable
        }
        if active_bindings:
            envelope = compose_attention_governance_bindings(
                tuple(active_bindings.values()), at=at
            )
        elif prior_concern_id is not None:
            row = await conn.fetchrow(
                """
                SELECT v.effective_binding_envelope
                FROM concern_heads h
                JOIN concern_versions v
                  ON v.tenant_id = h.tenant_id
                 AND v.concern_id = h.concern_id
                 AND v.aggregate_version = h.current_version
                WHERE h.tenant_id = $1 AND h.concern_id = $2
                """,
                tenant_id,
                prior_concern_id,
            )
            envelope = EffectiveAttentionGovernanceEnvelope.model_validate(
                _json(row["effective_binding_envelope"])
            )
        else:
            raise ValidationError("new Concern requires an applicable attention source")
        return bindings, envelope

    @staticmethod
    async def _insert_command_result(
        *,
        conn: asyncpg.Connection,
        result_id: UUID,
        tenant_id: UUID,
        command_id: UUID,
        idempotency_key: str,
        request_digest: str,
        command_kind: str,
        command_payload: dict[str, Any],
        processing_authority_fingerprint: str,
        consumption_authority_fingerprint: str,
        writer_scope_id: str,
        writer_epoch: int,
        aggregate_versions: list[dict[str, Any]],
        result_payload: dict[str, Any],
    ) -> None:
        await conn.execute(
            """
            INSERT INTO concern_command_results (
                id, tenant_id, command_id, semantic_idempotency_key,
                request_digest, command_kind, status, command,
                processing_authority_fingerprint,
                consumption_authority_fingerprint, writer_scope_id,
                writer_epoch, aggregate_versions, result
            ) VALUES (
                $1, $2, $3, $4, $5, $6, 'applied', $7::jsonb,
                $8, $9, $10, $11, $12::jsonb, $13::jsonb
            )
            """,
            result_id,
            tenant_id,
            command_id,
            idempotency_key,
            request_digest,
            command_kind,
            json.dumps(command_payload),
            processing_authority_fingerprint,
            consumption_authority_fingerprint,
            writer_scope_id,
            writer_epoch,
            json.dumps(aggregate_versions),
            json.dumps(result_payload),
        )

    async def _commit_snapshot(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        snapshot: ConcernSnapshot,
        envelope: EffectiveAttentionGovernanceEnvelope,
        transition: ConcernTransition,
        result_id: UUID,
        expected_version: int,
        predecessor_concern_id: UUID | None,
        successor_concern_id: UUID | None,
    ) -> None:
        snapshot_payload = snapshot.model_dump(mode="json")
        snapshot_digest = canonical_sha256(snapshot_payload)
        if expected_version == 0:
            await conn.execute(
                """
                INSERT INTO concern_heads (
                    tenant_id, concern_id, dedupe_key, current_version,
                    current_state, current_snapshot_digest,
                    effective_binding_digest, predecessor_concern_id,
                    successor_concern_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                tenant_id,
                snapshot.concern_id,
                snapshot.declared_dedupe_key,
                snapshot.aggregate_version,
                snapshot.state.value,
                snapshot_digest,
                envelope.envelope_digest,
                predecessor_concern_id,
                successor_concern_id,
            )
        else:
            status = await conn.execute(
                """
                UPDATE concern_heads
                SET current_version = $4, current_state = $5,
                    current_snapshot_digest = $6,
                    effective_binding_digest = $7,
                    predecessor_concern_id = $8,
                    successor_concern_id = $9,
                    updated_at = now()
                WHERE tenant_id = $1 AND concern_id = $2
                  AND current_version = $3
                """,
                tenant_id,
                snapshot.concern_id,
                expected_version,
                snapshot.aggregate_version,
                snapshot.state.value,
                snapshot_digest,
                envelope.envelope_digest,
                predecessor_concern_id,
                successor_concern_id,
            )
            if status != "UPDATE 1":
                raise InvariantViolation(
                    "CONCERN_CAS",
                    "Concern head changed during atomic apply",
                )
        await conn.execute(
            """
            INSERT INTO concern_versions (
                id, tenant_id, concern_id, aggregate_version, state,
                snapshot_digest, snapshot, effective_binding_digest,
                effective_binding_envelope, command_result_id, evidence_cutoff
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10, $11
            )
            """,
            uuid7(),
            tenant_id,
            snapshot.concern_id,
            snapshot.aggregate_version,
            snapshot.state.value,
            snapshot_digest,
            json.dumps(snapshot_payload),
            envelope.envelope_digest,
            json.dumps(envelope.model_dump(mode="json")),
            result_id,
            snapshot.evidence_cutoff,
        )
        await conn.execute(
            """
            INSERT INTO concern_transitions (
                id, tenant_id, concern_id, from_version, to_version,
                from_state, to_state, cause, transition, transitioned_at,
                command_result_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11
            )
            """,
            uuid7(),
            tenant_id,
            snapshot.concern_id,
            transition.from_version,
            transition.to_version,
            transition.from_state.value if transition.from_state else None,
            transition.to_state.value,
            transition.cause,
            json.dumps(transition.model_dump(mode="json")),
            transition.transitioned_at,
            result_id,
        )

    @staticmethod
    async def _record_event_and_outbox(
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        snapshot: ConcernSnapshot,
        result_id: UUID,
        semantic_transition: str,
        now: datetime,
    ) -> None:
        event_id = uuid7()
        event_payload = {
            "concern_id": str(snapshot.concern_id),
            "aggregate_version": snapshot.aggregate_version,
            "state": snapshot.state.value,
            "snapshot_digest": canonical_sha256(snapshot.model_dump(mode="json")),
        }
        await conn.execute(
            """
            INSERT INTO concern_canonical_events (
                id, tenant_id, command_result_id, concern_id,
                aggregate_version, semantic_transition, event_payload,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            """,
            event_id,
            tenant_id,
            result_id,
            snapshot.concern_id,
            snapshot.aggregate_version,
            semantic_transition,
            json.dumps(event_payload),
            now,
        )
        payload = event_payload | {"semantic_transition": semantic_transition}
        await conn.execute(
            """
            INSERT INTO concern_outbox_records (
                id, tenant_id, event_id, destination_operation,
                payload_hash, payload, deadline, attempt_budget
            ) VALUES ($1, $2, $3, 'concern_state_changed', $4, $5::jsonb, $6, 12)
            """,
            uuid7(),
            tenant_id,
            event_id,
            canonical_sha256(payload),
            json.dumps(payload),
            now + timedelta(hours=24),
        )


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
