"""WorkLedger-owned failure and cross-owner terminalization protocol."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import asyncpg

from lib.contracts.failure import (
    FailureRecord,
    FailureRecordCommand,
    FailureState,
    OwnerTerminalizationRequestCommand,
    OwnerTerminalizationResolutionCommand,
    failure_transition_allowed,
)
from lib.contracts.execution import (
    WorkObligationState,
    work_obligation_transition_allowed,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.agency_protocol import (
    AgencyCommitResult,
    AgencyProtocolIds,
    ensure_live_context,
    insert_protocol_event_and_outbox,
    insert_protocol_result,
    prior_protocol_result,
)


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _dump(value) -> str:
    return json.dumps(value.model_dump(mode="json"))


def _revalidate(command):
    return command.__class__.model_validate(command.model_dump(mode="json"))


class WorkFailureLedgerApplier:
    """Package port of the logical ``WorkLedgerApplier`` writer."""

    async def apply_failure(
        self,
        *,
        conn: asyncpg.Connection,
        command: FailureRecordCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior is not None:
            return prior
        record = command.record
        head = await conn.fetchrow(
            """
            SELECT * FROM failure_record_heads
            WHERE tenant_id=$1 AND failure_id=$2 FOR UPDATE
            """,
            record.tenant_id,
            record.failure_id,
        )
        current = await self._current_record(conn=conn, head=head) if head else None
        work, work_spec = await self._work(
            conn=conn,
            tenant_id=record.tenant_id,
            obligation_id=record.work_obligation_id,
        )
        redrive_parent_resolution = bool(
            current is not None
            and current.state is FailureState.REDRIVE_IN_PROGRESS
            and record.state is FailureState.RESOLVED
        )
        self._validate_work_binding(
            record=record,
            work=work,
            work_spec=work_spec,
            allow_superseded_redrive_parent=redrive_parent_resolution,
        )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "FAILURE_RECORD_CAS",
                "failure expected version does not match current head",
            )
        if record.state is FailureState.OWNER_TERMINALIZATION_PENDING:
            raise InvariantViolation(
                "FAILURE_OWNER_HANDSHAKE_REQUIRED",
                "owner-terminalization state requires the exact handshake command",
            )
        create = head is None
        current_state = None
        if create:
            await self._validate_new_lineage(
                conn=conn,
                record=record,
                work=work,
                work_spec=work_spec,
            )
        else:
            assert current is not None
            current_state = current.state
            if current_state is FailureState.OWNER_TERMINALIZATION_PENDING:
                raise InvariantViolation(
                    "FAILURE_OWNER_RESULT_REQUIRED",
                    "pending owner terminalization requires an exact owner result",
                )
            self._validate_identity(current=current, successor=record)
            if redrive_parent_resolution:
                await self._validate_redrive_parent_resolution(
                    conn=conn,
                    current=current,
                    successor=record,
                )
            if record.state in {FailureState.EXHAUSTED, FailureState.ESCALATED} and (
                record.semantic_owner_writer_id != "WorkLedgerApplier"
            ):
                raise InvariantViolation(
                    "FAILURE_FOREIGN_OWNER_TERMINAL_FATE",
                    "foreign semantic fate must close through owner terminalization",
                )
        if not failure_transition_allowed(current_state, record.state):
            raise InvariantViolation(
                "FAILURE_RECORD_TRANSITION",
                "illegal failure lifecycle transition",
                current_state=str(current_state),
                target_state=record.state,
            )
        return await self._commit_failure(
            conn=conn,
            command=command,
            next_version=current_version + 1,
            create=create,
            transition_kind="register" if create else "transition",
        )

    async def request_owner_terminalization(
        self,
        *,
        conn: asyncpg.Connection,
        command: OwnerTerminalizationRequestCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior is not None:
            return prior
        request = command.request
        work_head, _ = await self._work(
            conn=conn,
            tenant_id=request.tenant_id,
            obligation_id=request.work_obligation_id,
        )
        failure_head = await self._failure_head(
            conn=conn,
            tenant_id=request.tenant_id,
            failure_id=request.failure_id,
        )
        if (
            int(failure_head["current_version"]) != command.expected_failure_version
            or int(work_head["current_version"]) != command.expected_work_version
            or int(failure_head["generation"]) != request.failure_generation
            or int(work_head["generation"]) != request.work_obligation_generation
            or failure_head["current_state"] != request.from_failure_state
            or work_head["current_state"] != request.from_work_state
        ):
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_CAS",
                "owner-terminalization request does not match failure/work heads",
            )
        current = await self._current_record(conn=conn, head=failure_head)
        if (
            current.semantic_owner_writer_id != request.semantic_owner_writer_id
            or current.target_object_type != request.target_object_type
            or current.target_object_id != request.target_object_id
            or request.semantic_owner_writer_id == "WorkLedgerApplier"
        ):
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_TARGET",
                "request does not bind the exact foreign semantic owner/object",
            )
        if not failure_transition_allowed(
            current.state, FailureState.OWNER_TERMINALIZATION_PENDING
        ):
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_FAILURE_TRANSITION",
                "failure state cannot enter owner terminalization",
            )
        existing = await conn.fetchval(
            """
            SELECT request_id FROM owner_terminalization_requests
            WHERE tenant_id=$1 AND failure_id=$2
            """,
            request.tenant_id,
            request.failure_id,
        )
        if existing is not None:
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_REQUEST_EXISTS",
                "failure already has an owner-terminalization request",
            )
        next_failure_version = int(failure_head["current_version"]) + 1
        next_work_version = int(work_head["current_version"]) + 1
        successor = FailureRecord.model_validate(
            {
                **current.model_dump(mode="json"),
                "state": FailureState.OWNER_TERMINALIZATION_PENDING,
                "next_action": "await exact semantic-owner CommandResult",
                "owner_terminalization_request_id": request.request_id,
                "reason": request.terminal_reason,
                "updated_at": request.requested_at,
            }
        )
        ids = AgencyProtocolIds.new()
        result = {
            "failure_id": str(request.failure_id),
            "failure_version": next_failure_version,
            "failure_state": FailureState.OWNER_TERMINALIZATION_PENDING,
            "obligation_id": str(request.work_obligation_id),
            "obligation_version": next_work_version,
            "work_state": "owner_terminalization_pending",
            "owner_terminalization_request_id": str(request.request_id),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="request_owner_terminalization",
            command=command,
            request_digest=command.request_digest,
            object_type="failure_record",
            object_id=request.failure_id,
            object_version=next_failure_version,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO owner_terminalization_requests (
              request_id, tenant_id, request_digest, failure_id,
              failure_generation, failure_version, work_obligation_id,
              work_obligation_generation, work_obligation_version,
              semantic_owner_writer_id, target_object_type, target_object_id,
              request, command_result_id, requested_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15
            )
            """,
            request.request_id,
            request.tenant_id,
            request.request_digest,
            request.failure_id,
            request.failure_generation,
            next_failure_version,
            request.work_obligation_id,
            request.work_obligation_generation,
            next_work_version,
            request.semantic_owner_writer_id,
            request.target_object_type,
            request.target_object_id,
            _dump(request),
            ids.command_result_id,
            request.requested_at,
        )
        await self._update_failure(
            conn=conn,
            record=successor,
            next_version=next_failure_version,
            transition_kind="owner_terminalization_requested",
            command_result_id=ids.command_result_id,
        )
        await self._update_work(
            conn=conn,
            tenant_id=request.tenant_id,
            obligation_id=request.work_obligation_id,
            next_version=next_work_version,
            state="owner_terminalization_pending",
            payload=request,
            transition_kind="owner_terminalization_requested",
            command_result_id=ids.command_result_id,
            at=request.requested_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="failure_record",
            object_id=request.failure_id,
            object_version=next_failure_version,
            semantic_transition="owner_terminalization_pending",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="owner_terminalization_requested",
        )

    async def resolve_owner_terminalization(
        self,
        *,
        conn: asyncpg.Connection,
        command: OwnerTerminalizationResolutionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior is not None:
            return prior
        resolution = command.resolution
        request_row = await conn.fetchrow(
            """
            SELECT * FROM owner_terminalization_requests
            WHERE tenant_id=$1 AND request_id=$2
            """,
            resolution.tenant_id,
            resolution.request_id,
        )
        if request_row is None:
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_REQUEST_MISSING",
                "resolution references an unknown request",
            )
        request = _json(request_row["request"])
        work_head, _ = await self._work(
            conn=conn,
            tenant_id=resolution.tenant_id,
            obligation_id=resolution.work_obligation_id,
        )
        failure_head = await self._failure_head(
            conn=conn,
            tenant_id=resolution.tenant_id,
            failure_id=resolution.failure_id,
        )
        if (
            int(failure_head["current_version"]) != command.expected_failure_version
            or int(work_head["current_version"]) != command.expected_work_version
            or failure_head["current_state"]
            != FailureState.OWNER_TERMINALIZATION_PENDING
            or work_head["current_state"] != "owner_terminalization_pending"
            or failure_head["current_owner_terminalization_request_id"]
            != resolution.request_id
            or resolution.failure_generation != int(failure_head["generation"])
            or resolution.work_obligation_generation != int(work_head["generation"])
        ):
            raise InvariantViolation(
                "OWNER_TERMINALIZATION_RESOLUTION_CAS",
                "resolution does not match pending failure/work heads",
            )
        owner_result = await conn.fetchrow(
            """
            SELECT writer_id, object_type, object_id, object_version, result
            FROM agency_command_results
            WHERE tenant_id=$1 AND id=$2
            """,
            resolution.tenant_id,
            resolution.owner_command_result_id,
        )
        if owner_result is None:
            raise InvariantViolation(
                "OWNER_COMMAND_RESULT_MISSING",
                "owner terminalization requires an exact committed CommandResult",
            )
        result_payload = _json(owner_result["result"])
        result_state = str(
            result_payload.get("state") or result_payload.get("current_fate") or ""
        )
        exact = (
            owner_result["writer_id"] == request["semantic_owner_writer_id"]
            == resolution.observed_owner_writer_id
            and owner_result["object_type"] == request["target_object_type"]
            == resolution.observed_owner_object_type
            and owner_result["object_id"] == resolution.observed_owner_object_id
            and str(owner_result["object_id"]) == request["target_object_id"]
            and int(owner_result["object_version"])
            == resolution.observed_owner_object_version
            and result_state == resolution.observed_owner_terminal_state
            and result_state in request["acceptable_owner_terminal_states"]
        )
        if not exact:
            raise InvariantViolation(
                "OWNER_COMMAND_RESULT_MISMATCH",
                "owner result does not bind the requested writer/object/fate",
            )
        if not failure_transition_allowed(
            FailureState.OWNER_TERMINALIZATION_PENDING,
            resolution.to_failure_state,
        ):
            raise InvariantViolation(
                "OWNER_FAILURE_RESOLUTION_TRANSITION",
                "owner result cannot produce requested failure fate",
            )
        if not work_obligation_transition_allowed(
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
            resolution.to_work_state,
        ):
            raise InvariantViolation(
                "OWNER_WORK_RESOLUTION_TRANSITION",
                "owner result cannot produce requested work fate",
            )
        current = await self._current_record(conn=conn, head=failure_head)
        successor = FailureRecord.model_validate(
            {
                **current.model_dump(mode="json"),
                "state": resolution.to_failure_state,
                "next_action": "owner terminal result consumed",
                "owner_terminalization_request_id": None,
                "remediation_evidence_refs": tuple(
                    sorted(
                        {
                            *current.remediation_evidence_refs,
                            f"agency-command-result:{resolution.owner_command_result_id}",
                        }
                    )
                ),
                "reason": resolution.reason,
                "updated_at": resolution.resolved_at,
            }
        )
        next_failure_version = int(failure_head["current_version"]) + 1
        next_work_version = int(work_head["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "failure_id": str(resolution.failure_id),
            "failure_version": next_failure_version,
            "failure_state": resolution.to_failure_state,
            "obligation_id": str(resolution.work_obligation_id),
            "obligation_version": next_work_version,
            "work_state": resolution.to_work_state,
            "owner_command_result_id": str(resolution.owner_command_result_id),
            "owner_terminalization_resolution_id": str(resolution.resolution_id),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="resolve_owner_terminalization",
            command=command,
            request_digest=command.request_digest,
            object_type="failure_record",
            object_id=resolution.failure_id,
            object_version=next_failure_version,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO owner_terminalization_resolutions (
              resolution_id, tenant_id, resolution_digest, request_id,
              failure_id, failure_generation, failure_version,
              work_obligation_id, work_obligation_generation,
              work_obligation_version, owner_command_result_id, resolution,
              command_result_id, resolved_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14
            )
            """,
            resolution.resolution_id,
            resolution.tenant_id,
            resolution.resolution_digest,
            resolution.request_id,
            resolution.failure_id,
            resolution.failure_generation,
            next_failure_version,
            resolution.work_obligation_id,
            resolution.work_obligation_generation,
            next_work_version,
            resolution.owner_command_result_id,
            _dump(resolution),
            ids.command_result_id,
            resolution.resolved_at,
        )
        await self._update_failure(
            conn=conn,
            record=successor,
            next_version=next_failure_version,
            transition_kind="owner_terminalization_resolved",
            command_result_id=ids.command_result_id,
        )
        await self._update_work(
            conn=conn,
            tenant_id=resolution.tenant_id,
            obligation_id=resolution.work_obligation_id,
            next_version=next_work_version,
            state=resolution.to_work_state,
            payload=resolution,
            transition_kind="owner_terminalization_resolved",
            command_result_id=ids.command_result_id,
            at=resolution.resolved_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="failure_record",
            object_id=resolution.failure_id,
            object_version=next_failure_version,
            semantic_transition=resolution.to_failure_state,
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="owner_terminalization_resolved",
        )

    async def _commit_failure(
        self,
        *,
        conn,
        command,
        next_version,
        create,
        transition_kind,
    ):
        record = command.record
        ids = AgencyProtocolIds.new()
        result = {
            "failure_id": str(record.failure_id),
            "failure_version": next_version,
            "failure_state": record.state,
            "record_digest": record.record_digest,
            "obligation_id": str(record.work_obligation_id),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="apply_failure_record",
            command=command,
            request_digest=command.request_digest,
            object_type="failure_record",
            object_id=record.failure_id,
            object_version=next_version,
            result=result,
        )
        if create:
            await conn.execute(
                """
                INSERT INTO failure_record_specs (
                  tenant_id, failure_id, lineage_id, generation,
                  parent_failure_id, work_obligation_id,
                  work_obligation_generation, causal_operation,
                  owner_writer_id, semantic_owner_writer_id,
                  target_object_type, target_object_id,
                  original_semantic_idempotency_key, maximum_attempts,
                  deadline, initial_record_digest, initial_record,
                  command_result_id, created_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                  $16,$17::jsonb,$18,$19
                )
                """,
                record.tenant_id,
                record.failure_id,
                record.lineage_id,
                record.generation,
                record.parent_failure_id,
                record.work_obligation_id,
                record.work_obligation_generation,
                record.causal_operation,
                record.owner_writer_id,
                record.semantic_owner_writer_id,
                record.target_object_type,
                record.target_object_id,
                record.original_semantic_idempotency_key,
                record.maximum_attempts,
                record.deadline,
                record.record_digest,
                _dump(record),
                ids.command_result_id,
                record.created_at,
            )
            if record.generation == 1:
                await conn.execute(
                    """
                    INSERT INTO failure_record_lineage_heads (
                      tenant_id, lineage_id, current_failure_id,
                      current_generation, updated_at
                    ) VALUES ($1,$2,$3,$4,$5)
                    """,
                    record.tenant_id,
                    record.lineage_id,
                    record.failure_id,
                    record.generation,
                    record.updated_at,
                )
            else:
                await conn.execute(
                    """
                    UPDATE failure_record_lineage_heads
                    SET current_failure_id=$3, current_generation=$4, updated_at=$5
                    WHERE tenant_id=$1 AND lineage_id=$2
                    """,
                    record.tenant_id,
                    record.lineage_id,
                    record.failure_id,
                    record.generation,
                    record.updated_at,
                )
            await conn.execute(
                """
                INSERT INTO failure_record_heads (
                  tenant_id, failure_id, lineage_id, generation,
                  work_obligation_id, work_obligation_generation,
                  current_version, current_state, current_record_digest,
                  current_owner_terminalization_request_id, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                """,
                record.tenant_id,
                record.failure_id,
                record.lineage_id,
                record.generation,
                record.work_obligation_id,
                record.work_obligation_generation,
                next_version,
                record.state,
                record.record_digest,
                record.owner_terminalization_request_id,
                record.updated_at,
            )
        else:
            await conn.execute(
                """
                UPDATE failure_record_heads
                SET current_version=$3, current_state=$4,
                    current_record_digest=$5,
                    current_owner_terminalization_request_id=$6,
                    updated_at=$7
                WHERE tenant_id=$1 AND failure_id=$2
                """,
                record.tenant_id,
                record.failure_id,
                next_version,
                record.state,
                record.record_digest,
                record.owner_terminalization_request_id,
                record.updated_at,
            )
        await conn.execute(
            """
            INSERT INTO failure_record_versions (
              id, tenant_id, failure_id, aggregate_version, state,
              record_digest, record, transition_kind, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
            """,
            uuid7(),
            record.tenant_id,
            record.failure_id,
            next_version,
            record.state,
            record.record_digest,
            _dump(record),
            transition_kind,
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="failure_record",
            object_id=record.failure_id,
            object_version=next_version,
            semantic_transition=record.state,
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="failure_record_transition_committed",
        )

    async def _update_failure(
        self,
        *,
        conn,
        record,
        next_version,
        transition_kind,
        command_result_id,
    ):
        await conn.execute(
            """
            UPDATE failure_record_heads
            SET current_version=$3, current_state=$4,
                current_record_digest=$5,
                current_owner_terminalization_request_id=$6,
                updated_at=$7
            WHERE tenant_id=$1 AND failure_id=$2
            """,
            record.tenant_id,
            record.failure_id,
            next_version,
            record.state,
            record.record_digest,
            record.owner_terminalization_request_id,
            record.updated_at,
        )
        await conn.execute(
            """
            INSERT INTO failure_record_versions (
              id, tenant_id, failure_id, aggregate_version, state,
              record_digest, record, transition_kind, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
            """,
            uuid7(),
            record.tenant_id,
            record.failure_id,
            next_version,
            record.state,
            record.record_digest,
            _dump(record),
            transition_kind,
            command_result_id,
        )

    async def _update_work(
        self,
        *,
        conn,
        tenant_id,
        obligation_id,
        next_version,
        state,
        payload,
        transition_kind,
        command_result_id,
        at,
    ):
        await conn.execute(
            """
            UPDATE work_obligation_heads
            SET current_version=$3, current_state=$4,
                current_lease_token_id=NULL, updated_at=$5
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            tenant_id,
            obligation_id,
            next_version,
            state,
            at,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
            """,
            uuid7(),
            tenant_id,
            obligation_id,
            next_version,
            state,
            transition_kind,
            _dump(payload),
            command_result_id,
        )

    async def _validate_new_lineage(
        self, *, conn, record, work, work_spec
    ) -> None:
        lineage = await conn.fetchrow(
            """
            SELECT * FROM failure_record_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2 FOR UPDATE
            """,
            record.tenant_id,
            record.lineage_id,
        )
        if record.generation == 1:
            if lineage is not None:
                raise InvariantViolation(
                    "FAILURE_LINEAGE_EXISTS",
                    "first failure generation cannot replace an existing lineage",
                )
            return
        if (
            lineage is None
            or lineage["current_failure_id"] != record.parent_failure_id
            or int(lineage["current_generation"]) + 1 != record.generation
        ):
            raise InvariantViolation(
                "FAILURE_SUCCESSOR_LINEAGE_CAS",
                "failure successor does not extend the exact current head",
            )
        parent_head = await self._failure_head(
            conn=conn,
            tenant_id=record.tenant_id,
            failure_id=record.parent_failure_id,
        )
        if parent_head["current_state"] != FailureState.REDRIVE_IN_PROGRESS:
            raise InvariantViolation(
                "FAILURE_REDRIVE_NOT_IN_PROGRESS",
                "failure successor requires the exact in-progress redrive parent",
            )
        parent = await self._current_record(conn=conn, head=parent_head)
        identity_fields = (
            "tenant_id",
            "lineage_id",
            "causal_operation",
            "owner_writer_id",
            "semantic_owner_writer_id",
            "target_object_type",
            "target_object_id",
        )
        if any(getattr(parent, name) != getattr(record, name) for name in identity_fields):
            raise InvariantViolation(
                "FAILURE_REDRIVE_IDENTITY_DRIFT",
                "failure successor changed semantic operation or ownership",
            )
        if (
            record.original_semantic_idempotency_key
            == parent.original_semantic_idempotency_key
            or record.created_at <= parent.updated_at
        ):
            raise InvariantViolation(
                "FAILURE_REDRIVE_KEY_OR_TIME",
                "failure redrive requires a new semantic key and later generation time",
            )
        parent_work = await conn.fetchrow(
            """
            SELECT h.current_state, h.generation, s.lineage_id
            FROM work_obligation_heads h
            JOIN work_obligation_specs s
              ON s.tenant_id=h.tenant_id AND s.obligation_id=h.obligation_id
            WHERE h.tenant_id=$1 AND h.obligation_id=$2
            FOR SHARE OF h
            """,
            record.tenant_id,
            parent.work_obligation_id,
        )
        work_lineage = await conn.fetchrow(
            """
            SELECT current_obligation_id, current_generation
            FROM work_obligation_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2 FOR SHARE
            """,
            record.tenant_id,
            work_spec["lineage_id"],
        )
        if (
            parent_work is None
            or parent_work["current_state"]
            != WorkObligationState.SUPERSEDED_BY_NEW_GENERATION
            or int(parent_work["generation"]) + 1
            != record.work_obligation_generation
            or work_spec["parent_obligation_id"] != parent.work_obligation_id
            or work_spec["lineage_id"] != parent_work["lineage_id"]
            or work_lineage is None
            or work_lineage["current_obligation_id"] != record.work_obligation_id
            or int(work_lineage["current_generation"])
            != record.work_obligation_generation
            or int(work["generation"]) != record.work_obligation_generation
        ):
            raise InvariantViolation(
                "FAILURE_REDRIVE_WORK_LINEAGE",
                "failure successor does not bind the exact authorized Work successor",
            )

    async def _validate_redrive_parent_resolution(
        self, *, conn, current: FailureRecord, successor: FailureRecord
    ) -> None:
        lineage = await conn.fetchrow(
            """
            SELECT current_failure_id, current_generation
            FROM failure_record_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2 FOR SHARE
            """,
            current.tenant_id,
            current.lineage_id,
        )
        child = None
        if lineage is not None and lineage["current_failure_id"] != current.failure_id:
            child = await conn.fetchrow(
                """
                SELECT current_state, work_obligation_id
                FROM failure_record_heads
                WHERE tenant_id=$1 AND failure_id=$2 FOR SHARE
                """,
                current.tenant_id,
                lineage["current_failure_id"],
            )
        child_work_state = None
        if child is not None:
            child_work_state = await conn.fetchval(
                """
                SELECT current_state FROM work_obligation_heads
                WHERE tenant_id=$1 AND obligation_id=$2 FOR SHARE
                """,
                current.tenant_id,
                child["work_obligation_id"],
            )
        terminal_failure_states = {
            FailureState.TERMINAL_REJECTED,
            FailureState.RESOLVED,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
        child_failure_ref = (
            f"failure:{lineage['current_failure_id']}" if lineage is not None else ""
        )
        if (
            lineage is None
            or int(lineage["current_generation"]) <= current.generation
            or child is None
            or child["current_state"] not in terminal_failure_states
            or child_work_state
            not in {
                WorkObligationState.COMPLETED,
                WorkObligationState.NO_OP,
                WorkObligationState.SUPPRESSED,
                WorkObligationState.REJECTED,
                WorkObligationState.CANCELLED,
                WorkObligationState.EXPIRED,
                WorkObligationState.EXHAUSTED,
                WorkObligationState.ESCALATED,
            }
            or child_failure_ref not in successor.remediation_evidence_refs
        ):
            raise InvariantViolation(
                "FAILURE_REDRIVE_RESULT_INCOMPLETE",
                "redrive parent requires an exact terminal child Failure and Work result",
            )

    async def _failure_head(self, *, conn, tenant_id, failure_id):
        row = await conn.fetchrow(
            """
            SELECT * FROM failure_record_heads
            WHERE tenant_id=$1 AND failure_id=$2 FOR UPDATE
            """,
            tenant_id,
            failure_id,
        )
        if row is None:
            raise InvariantViolation(
                "FAILURE_RECORD_NOT_FOUND", "failure record does not exist"
            )
        return row

    async def _current_record(self, *, conn, head) -> FailureRecord:
        raw = await conn.fetchval(
            """
            SELECT record FROM failure_record_versions
            WHERE tenant_id=$1 AND failure_id=$2 AND aggregate_version=$3
            """,
            head["tenant_id"],
            head["failure_id"],
            head["current_version"],
        )
        return FailureRecord.model_validate(_json(raw))

    async def _work(self, *, conn, tenant_id, obligation_id):
        row = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2 FOR UPDATE
            """,
            tenant_id,
            obligation_id,
        )
        spec = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_specs
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            tenant_id,
            obligation_id,
        )
        if row is None or spec is None:
            raise InvariantViolation(
                "FAILURE_WORK_NOT_FOUND", "failure requires exact WorkObligation"
            )
        return row, spec

    def _validate_work_binding(
        self,
        *,
        record,
        work,
        work_spec,
        allow_superseded_redrive_parent: bool = False,
    ) -> None:
        if (
            int(work["generation"]) != record.work_obligation_generation
            or work_spec["target_object_type"] != record.target_object_type
            or work_spec["target_object_id"] != record.target_object_id
            or work_spec["owner_writer_id"] != record.semantic_owner_writer_id
            or record.maximum_attempts > int(work_spec["maximum_attempts"])
            or record.deadline > work_spec["deadline"]
            or (
                work["current_state"] in {
                "completed",
                "no_op",
                "suppressed",
                "rejected",
                "cancelled",
                "expired",
                "superseded_by_new_generation",
                "exhausted",
                "escalated",
                }
                and not (
                    allow_superseded_redrive_parent
                    and work["current_state"] == "superseded_by_new_generation"
                )
            )
        ):
            raise InvariantViolation(
                "FAILURE_WORK_BINDING",
                "failure does not bind exact nonterminal work/semantic owner",
            )

    def _validate_identity(self, *, current, successor) -> None:
        fields = (
            "failure_id",
            "lineage_id",
            "tenant_id",
            "generation",
            "parent_failure_id",
            "work_obligation_id",
            "work_obligation_generation",
            "causal_operation",
            "owner_writer_id",
            "semantic_owner_writer_id",
            "target_object_type",
            "target_object_id",
            "original_semantic_idempotency_key",
            "maximum_attempts",
            "deadline",
            "created_at",
        )
        if any(getattr(current, name) != getattr(successor, name) for name in fields):
            raise InvariantViolation(
                "FAILURE_IDENTITY_MUTATION",
                "failure transition changed immutable identity/owner/budget",
            )
        if successor.updated_at <= current.updated_at:
            raise InvariantViolation(
                "FAILURE_TIME_REGRESSION", "failure successor time must advance"
            )

    async def _prior(self, *, conn, command):
        return await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="WorkLedgerApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )


__all__ = ["WorkFailureLedgerApplier"]
