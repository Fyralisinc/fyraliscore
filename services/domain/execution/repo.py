"""Named appliers for workflow state, runtime work, and external effects."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.agency import (
    AuthorizationDecision,
    AuthorizationDisposition,
    InterventionSpec,
)
from lib.contracts.execution import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    EffectReservationCommand,
    EffectTransitionCommand,
    ExecutionReceipt,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseGrantCommand,
    LeaseHeartbeatCommand,
    LeaseResolutionCommand,
    LeaseState,
    LeaseTakeoverCommand,
    TaskCommand,
    TaskSnapshot,
    TaskState,
    WorkDecisionCommand,
    WorkObligation,
    WorkObligationRegistrationCommand,
    WorkObligationState,
    WorkStateTransitionCommand,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkflowRunState,
    external_effect_transition_allowed,
    lease_transition_allowed,
    task_transition_allowed,
    work_obligation_transition_allowed,
    workflow_run_transition_allowed,
)
from lib.contracts.failure import EffectUncertainty, FailureRecord, FailureState
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
    """Do not let unvalidated ``model_copy(update=...)`` cross a writer boundary."""

    return command.__class__.model_validate(command.model_dump(mode="json"))


async def _authorization(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    decision_id: UUID,
) -> AuthorizationDecision:
    value = await conn.fetchval(
        """
        SELECT decision
        FROM consequential_authorization_decisions
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        decision_id,
    )
    if value is None:
        raise InvariantViolation(
            "EXECUTION_AUTHORIZATION_MISSING",
            "workflow/effect references an unknown authorization decision",
            authorization_decision_id=str(decision_id),
        )
    return AuthorizationDecision.model_validate(_json(value))


async def _require_live_authorization(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    decision_id: UUID,
    intervention_spec_digest: str,
    operation: str | None,
    target_refs: tuple[str, ...] = (),
    at: datetime,
    require_live: bool = True,
) -> AuthorizationDecision:
    decision = await _authorization(
        conn,
        tenant_id=tenant_id,
        decision_id=decision_id,
    )
    if decision.disposition is not AuthorizationDisposition.AUTHORIZED:
        raise InvariantViolation(
            "EXECUTION_NOT_AUTHORIZED",
            "rejected authorization cannot instantiate or dispatch work",
        )
    if decision.intervention_spec_digest != intervention_spec_digest:
        raise InvariantViolation(
            "EXECUTION_SPEC_AUTHORIZATION_MISMATCH",
            "authorization does not bind the exact InterventionSpec",
        )
    if require_live and (
        at >= decision.expires_at or not decision.authority.is_live(at)
    ):
        raise InvariantViolation(
            "EXECUTION_AUTHORIZATION_EXPIRED",
            "authorization is not live at the consequential transition",
        )
    if operation and operation not in decision.exact_operations:
        raise InvariantViolation(
            "EXECUTION_OPERATION_NOT_AUTHORIZED",
            "operation is outside the exact authorization scope",
            operation=operation,
        )
    if target_refs and not set(target_refs) <= set(decision.exact_target_refs):
        raise InvariantViolation(
            "EXECUTION_TARGET_NOT_AUTHORIZED",
            "grounded effect targets exceed exact authorization scope",
        )
    return decision


async def _intervention_spec(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    digest: str,
) -> InterventionSpec:
    value = await conn.fetchval(
        """
        SELECT spec
        FROM consequential_intervention_specs
        WHERE tenant_id = $1 AND spec_digest = $2
        """,
        tenant_id,
        digest,
    )
    if value is None:
        raise InvariantViolation(
            "EXECUTION_SPEC_MISSING",
            "workflow/effect references an unknown InterventionSpec digest",
        )
    spec = InterventionSpec.model_validate(_json(value))
    if spec.spec_digest != digest:
        raise InvariantViolation(
            "EXECUTION_SPEC_DIGEST_DRIFT",
            "stored InterventionSpec no longer matches its canonical digest",
        )
    return spec


class AgencyStateApplier:
    """Own business WorkflowRun and Task state, never runtime lease/effect truth."""

    async def apply_workflow_run(
        self,
        *,
        conn: asyncpg.Connection,
        command: WorkflowRunCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="AgencyStateApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        snapshot = command.snapshot
        await _intervention_spec(
            conn,
            tenant_id=snapshot.tenant_id,
            digest=snapshot.intervention_spec_digest,
        )
        head = await conn.fetchrow(
            """
            SELECT * FROM agency_workflow_run_heads
            WHERE tenant_id = $1 AND workflow_run_id = $2
            FOR UPDATE
            """,
            snapshot.tenant_id,
            snapshot.workflow_run_id,
        )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "WORKFLOW_RUN_CAS",
                "workflow expected version does not match current head",
                expected_version=command.expected_version,
                current_version=current_version,
            )
        await _require_live_authorization(
            conn,
            tenant_id=snapshot.tenant_id,
            decision_id=snapshot.authorization_decision_id,
            intervention_spec_digest=snapshot.intervention_spec_digest,
            operation=None,
            at=now,
            require_live=head is None or snapshot.state is WorkflowRunState.ACTIVE,
        )
        current_state = None
        if head:
            current_state = head["current_state"]
            prior_snapshot = WorkflowRunSnapshot.model_validate(
                _json(
                    await conn.fetchval(
                        """
                        SELECT snapshot FROM agency_workflow_run_versions
                        WHERE tenant_id = $1 AND workflow_run_id = $2
                          AND aggregate_version = $3
                        """,
                        snapshot.tenant_id,
                        snapshot.workflow_run_id,
                        current_version,
                    )
                )
            )
            invariant_fields = (
                "tenant_id",
                "workflow_run_id",
                "episode_id",
                "intervention_spec_digest",
                "workflow_spec_version_ref",
                "authorization_decision_id",
                "authorization_decision_version",
                "created_at",
            )
            if any(
                getattr(prior_snapshot, name) != getattr(snapshot, name)
                for name in invariant_fields
            ):
                raise InvariantViolation(
                    "WORKFLOW_RUN_IDENTITY_MUTATION",
                    "workflow successor changed immutable identity/spec/authority",
                )
        if not workflow_run_transition_allowed(current_state, snapshot.state):
            raise InvariantViolation(
                "WORKFLOW_RUN_TRANSITION",
                "illegal workflow lifecycle transition",
                current_state=str(current_state),
                target_state=snapshot.state,
            )
        if snapshot.state.value == "completed":
            await self._validate_required_tasks(conn=conn, snapshot=snapshot)
        return await self._commit_workflow(
            conn=conn,
            command=command,
            next_version=current_version + 1,
            create=head is None,
        )

    async def _validate_required_tasks(
        self,
        *,
        conn: asyncpg.Connection,
        snapshot: WorkflowRunSnapshot,
    ) -> None:
        if not snapshot.required_task_ids:
            return
        rows = await conn.fetch(
            """
            SELECT task_id, current_state
            FROM agency_task_heads
            WHERE tenant_id = $1 AND workflow_run_id = $2
              AND task_id = ANY($3::uuid[])
            """,
            snapshot.tenant_id,
            snapshot.workflow_run_id,
            list(snapshot.required_task_ids),
        )
        states = {row["task_id"]: row["current_state"] for row in rows}
        if set(states) != set(snapshot.required_task_ids) or any(
            state != "completed" for state in states.values()
        ):
            raise InvariantViolation(
                "WORKFLOW_RUN_TASKS_INCOMPLETE",
                "workflow completion requires every declared required task completed",
            )

    async def _commit_workflow(
        self,
        *,
        conn: asyncpg.Connection,
        command: WorkflowRunCommand,
        next_version: int,
        create: bool,
    ) -> AgencyCommitResult:
        snapshot = command.snapshot
        ids = AgencyProtocolIds.new()
        result = {
            "workflow_run_id": str(snapshot.workflow_run_id),
            "workflow_run_version": next_version,
            "state": snapshot.state,
            "snapshot_digest": snapshot.snapshot_digest,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="AgencyStateApplier",
            command_kind="apply_workflow_run",
            command=command,
            request_digest=command.request_digest,
            object_type="workflow_run",
            object_id=snapshot.workflow_run_id,
            object_version=next_version,
            result=result,
        )
        if create:
            await conn.execute(
                """
                INSERT INTO agency_workflow_run_heads (
                  tenant_id, workflow_run_id, episode_id,
                  intervention_spec_digest, current_version, current_state,
                  current_snapshot_digest, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                snapshot.tenant_id,
                snapshot.workflow_run_id,
                snapshot.episode_id,
                snapshot.intervention_spec_digest,
                next_version,
                snapshot.state,
                snapshot.snapshot_digest,
                snapshot.updated_at,
            )
        else:
            await conn.execute(
                """
                UPDATE agency_workflow_run_heads
                SET current_version=$3, current_state=$4,
                    current_snapshot_digest=$5, updated_at=$6
                WHERE tenant_id=$1 AND workflow_run_id=$2
                """,
                snapshot.tenant_id,
                snapshot.workflow_run_id,
                next_version,
                snapshot.state,
                snapshot.snapshot_digest,
                snapshot.updated_at,
            )
        await conn.execute(
            """
            INSERT INTO agency_workflow_run_versions (
              id, tenant_id, workflow_run_id, aggregate_version, state,
              snapshot_digest, snapshot, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
            """,
            uuid7(),
            snapshot.tenant_id,
            snapshot.workflow_run_id,
            next_version,
            snapshot.state,
            snapshot.snapshot_digest,
            _dump(snapshot),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="AgencyStateApplier",
            object_type="workflow_run",
            object_id=snapshot.workflow_run_id,
            object_version=next_version,
            semantic_transition=snapshot.state,
            event_payload=result,
            intervention_spec_digest=snapshot.intervention_spec_digest,
            destination_operation="workflow_run_transition_committed",
        )

    async def apply_task(
        self,
        *,
        conn: asyncpg.Connection,
        command: TaskCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="AgencyStateApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        snapshot = command.snapshot
        run = await conn.fetchrow(
            """
            SELECT * FROM agency_workflow_run_heads
            WHERE tenant_id=$1 AND workflow_run_id=$2
            """,
            snapshot.tenant_id,
            snapshot.workflow_run_id,
        )
        if run is None or run["episode_id"] != snapshot.episode_id or (
            run["intervention_spec_digest"] != snapshot.intervention_spec_digest
        ):
            raise InvariantViolation(
                "TASK_WORKFLOW_MISMATCH",
                "task does not belong to the exact workflow episode/spec",
            )
        head = await conn.fetchrow(
            """
            SELECT * FROM agency_task_heads
            WHERE tenant_id=$1 AND task_id=$2 FOR UPDATE
            """,
            snapshot.tenant_id,
            snapshot.task_id,
        )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "TASK_CAS",
                "task expected version does not match current head",
            )
        await _require_live_authorization(
            conn,
            tenant_id=snapshot.tenant_id,
            decision_id=snapshot.authorization_decision_id,
            intervention_spec_digest=snapshot.intervention_spec_digest,
            operation=None,
            at=now,
            require_live=head is None
            or snapshot.state in {TaskState.READY, TaskState.IN_PROGRESS},
        )
        current_state = None
        if head:
            current_state = head["current_state"]
            prior_snapshot = TaskSnapshot.model_validate(
                _json(
                    await conn.fetchval(
                        """
                        SELECT snapshot FROM agency_task_versions
                        WHERE tenant_id=$1 AND task_id=$2 AND aggregate_version=$3
                        """,
                        snapshot.tenant_id,
                        snapshot.task_id,
                        current_version,
                    )
                )
            )
            invariant_fields = (
                "tenant_id",
                "task_id",
                "workflow_run_id",
                "episode_id",
                "intervention_spec_digest",
                "task_kind",
                "authorization_decision_id",
                "authorization_decision_version",
                "external_effect_required",
                "created_at",
            )
            if any(
                getattr(prior_snapshot, name) != getattr(snapshot, name)
                for name in invariant_fields
            ):
                raise InvariantViolation(
                    "TASK_IDENTITY_MUTATION",
                    "task successor changed immutable identity/spec/authority",
                )
        if not task_transition_allowed(current_state, snapshot.state):
            raise InvariantViolation(
                "TASK_TRANSITION",
                "illegal task lifecycle transition",
                current_state=str(current_state),
                target_state=snapshot.state,
            )
        if snapshot.state is TaskState.COMPLETED and snapshot.external_effect_required:
            await self._validate_effect_receipt(conn=conn, snapshot=snapshot)
        return await self._commit_task(
            conn=conn,
            command=command,
            next_version=current_version + 1,
            create=head is None,
        )

    async def _validate_effect_receipt(
        self,
        *,
        conn: asyncpg.Connection,
        snapshot: TaskSnapshot,
    ) -> None:
        row = await conn.fetchrow(
            """
            SELECT r.effect_state, r.effect_attempt_id, e.task_id,
                   e.intervention_spec_digest
            FROM execution_receipts r
            JOIN external_effect_attempt_heads e
              ON e.tenant_id=r.tenant_id
             AND e.effect_attempt_id=r.effect_attempt_id
            WHERE r.tenant_id=$1 AND r.receipt_id=$2
            """,
            snapshot.tenant_id,
            snapshot.execution_receipt_id,
        )
        if (
            row is None
            or row["effect_state"] != ExternalEffectState.SUCCEEDED
            or row["effect_attempt_id"] != snapshot.effect_attempt_id
            or row["task_id"] != snapshot.task_id
            or row["intervention_spec_digest"] != snapshot.intervention_spec_digest
        ):
            raise InvariantViolation(
                "TASK_EFFECT_RECEIPT_INVALID",
                "external-effect task completion requires exact succeeded receipt",
            )

    async def _commit_task(
        self,
        *,
        conn: asyncpg.Connection,
        command: TaskCommand,
        next_version: int,
        create: bool,
    ) -> AgencyCommitResult:
        snapshot = command.snapshot
        ids = AgencyProtocolIds.new()
        result = {
            "task_id": str(snapshot.task_id),
            "task_version": next_version,
            "state": snapshot.state,
            "snapshot_digest": snapshot.snapshot_digest,
            "execution_receipt_id": (
                str(snapshot.execution_receipt_id)
                if snapshot.execution_receipt_id
                else None
            ),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="AgencyStateApplier",
            command_kind="apply_task",
            command=command,
            request_digest=command.request_digest,
            object_type="task",
            object_id=snapshot.task_id,
            object_version=next_version,
            result=result,
        )
        if create:
            await conn.execute(
                """
                INSERT INTO agency_task_heads (
                  tenant_id, task_id, workflow_run_id, episode_id,
                  intervention_spec_digest, current_version, current_state,
                  current_snapshot_digest, external_effect_required,
                  current_effect_attempt_id, current_execution_receipt_id, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                """,
                snapshot.tenant_id,
                snapshot.task_id,
                snapshot.workflow_run_id,
                snapshot.episode_id,
                snapshot.intervention_spec_digest,
                next_version,
                snapshot.state,
                snapshot.snapshot_digest,
                snapshot.external_effect_required,
                snapshot.effect_attempt_id,
                snapshot.execution_receipt_id,
                snapshot.updated_at,
            )
        else:
            await conn.execute(
                """
                UPDATE agency_task_heads
                SET current_version=$3, current_state=$4,
                    current_snapshot_digest=$5, current_effect_attempt_id=$6,
                    current_execution_receipt_id=$7, updated_at=$8
                WHERE tenant_id=$1 AND task_id=$2
                """,
                snapshot.tenant_id,
                snapshot.task_id,
                next_version,
                snapshot.state,
                snapshot.snapshot_digest,
                snapshot.effect_attempt_id,
                snapshot.execution_receipt_id,
                snapshot.updated_at,
            )
        await conn.execute(
            """
            INSERT INTO agency_task_versions (
              id, tenant_id, task_id, aggregate_version, state,
              snapshot_digest, snapshot, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
            """,
            uuid7(),
            snapshot.tenant_id,
            snapshot.task_id,
            next_version,
            snapshot.state,
            snapshot.snapshot_digest,
            _dump(snapshot),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="AgencyStateApplier",
            object_type="task",
            object_id=snapshot.task_id,
            object_version=next_version,
            semantic_transition=snapshot.state,
            event_payload=result,
            intervention_spec_digest=snapshot.intervention_spec_digest,
            destination_operation="task_transition_committed",
        )


class WorkLedgerApplier:
    """Own obligations, decisions, lease fences, retries, and redrive lineage."""

    async def register(
        self,
        *,
        conn: asyncpg.Connection,
        command: WorkObligationRegistrationCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="WorkLedgerApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        work = command.obligation
        lineage = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2 FOR UPDATE
            """,
            work.tenant_id,
            work.lineage_id,
        )
        parent_head = None
        redrive_failure_head = None
        redrive_failure = None
        if work.generation == 1:
            if lineage is not None:
                raise InvariantViolation(
                    "WORK_LINEAGE_EXISTS",
                    "first work generation cannot replace an existing lineage",
                )
        else:
            if (
                lineage is None
                or lineage["current_obligation_id"] != work.parent_obligation_id
                or int(lineage["current_generation"]) + 1 != work.generation
            ):
                raise InvariantViolation(
                    "WORK_SUCCESSOR_LINEAGE_CAS",
                    "work successor does not extend the exact current lineage head",
                )
            parent_head = await conn.fetchrow(
                """
                SELECT h.*, s.obligation AS parent_obligation
                FROM work_obligation_heads h
                JOIN work_obligation_specs s
                  ON s.tenant_id=h.tenant_id AND s.obligation_id=h.obligation_id
                WHERE h.tenant_id=$1 AND h.obligation_id=$2
                FOR UPDATE OF h
                """,
                work.tenant_id,
                work.parent_obligation_id,
            )
            if parent_head is None or parent_head["current_state"] != (
                WorkObligationState.REDRIVE_AUTHORIZED
            ):
                raise InvariantViolation(
                    "WORK_REDRIVE_NOT_AUTHORIZED",
                    "successor generation requires exact redrive-authorized parent",
                )
            parent = WorkObligation.model_validate(
                _json(parent_head["parent_obligation"])
            )
            redrive_identity_fields = (
                "tenant_id",
                "lineage_id",
                "semantic_dedupe_key",
                "target_object_type",
                "target_object_id",
                "owner_writer_id",
                "purpose",
                "effect_possible",
            )
            if any(
                getattr(parent, name) != getattr(work, name)
                for name in redrive_identity_fields
            ):
                raise InvariantViolation(
                    "WORK_REDRIVE_IDENTITY_DRIFT",
                    "successor generation changed its semantic work identity",
                )
            failure_rows = await conn.fetch(
                """
                SELECT h.*, v.record
                FROM failure_record_heads h
                JOIN failure_record_versions v
                  ON v.tenant_id=h.tenant_id AND v.failure_id=h.failure_id
                 AND v.aggregate_version=h.current_version
                WHERE h.tenant_id=$1 AND h.work_obligation_id=$2
                  AND h.work_obligation_generation=$3
                  AND h.current_state='redrive_authorized'
                FOR UPDATE OF h
                """,
                work.tenant_id,
                work.parent_obligation_id,
                parent.generation,
            )
            if len(failure_rows) != 1:
                raise InvariantViolation(
                    "WORK_REDRIVE_FAILURE_AUTHORIZATION",
                    "work successor requires exactly one redrive-authorized failure",
                )
            redrive_failure_head = failure_rows[0]
            redrive_failure = FailureRecord.model_validate(
                _json(redrive_failure_head["record"])
            )
            if (
                redrive_failure.semantic_owner_writer_id != work.owner_writer_id
                or redrive_failure.target_object_type != work.target_object_type
                or redrive_failure.target_object_id != work.target_object_id
                or redrive_failure.work_obligation_id != work.parent_obligation_id
                or redrive_failure.effect_uncertainty
                not in {EffectUncertainty.NONE, EffectUncertainty.KNOWN_NO_EFFECT}
            ):
                raise InvariantViolation(
                    "WORK_REDRIVE_FAILURE_MISMATCH",
                    "redrive-authorized failure does not bind the exact safe work identity",
                )
        ids = AgencyProtocolIds.new()
        result = {
            "obligation_id": str(work.obligation_id),
            "lineage_id": str(work.lineage_id),
            "generation": work.generation,
            "obligation_version": 1,
            "state": WorkObligationState.REGISTERED,
            "obligation_digest": work.obligation_digest,
            "superseded_parent_id": (
                str(work.parent_obligation_id) if work.parent_obligation_id else None
            ),
            "redrive_failure_id": (
                str(redrive_failure.failure_id) if redrive_failure else None
            ),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="register_work_obligation",
            command=command,
            request_digest=command.request_digest,
            object_type="work_obligation",
            object_id=work.obligation_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_specs (
              obligation_id, tenant_id, lineage_id, generation,
              parent_obligation_id, semantic_dedupe_key, obligation_digest,
              target_object_type, target_object_id, owner_writer_id, purpose,
              risk_tier, maximum_attempts, deadline, effect_possible,
              obligation, command_result_id, registered_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,$18
            )
            """,
            work.obligation_id,
            work.tenant_id,
            work.lineage_id,
            work.generation,
            work.parent_obligation_id,
            work.semantic_dedupe_key,
            work.obligation_digest,
            work.target_object_type,
            work.target_object_id,
            work.owner_writer_id,
            work.purpose,
            work.risk_tier,
            work.maximum_attempts,
            work.deadline,
            work.effect_possible,
            _dump(work),
            ids.command_result_id,
            work.registered_at,
        )
        if lineage is None:
            await conn.execute(
                """
                INSERT INTO work_obligation_lineage_heads (
                  tenant_id, lineage_id, current_obligation_id,
                  current_generation, updated_at
                ) VALUES ($1,$2,$3,$4,$5)
                """,
                work.tenant_id,
                work.lineage_id,
                work.obligation_id,
                work.generation,
                now,
            )
        else:
            parent_version = int(parent_head["current_version"]) + 1
            await conn.execute(
                """
                UPDATE work_obligation_heads
                SET current_version=$3,
                    current_state='superseded_by_new_generation', updated_at=$4
                WHERE tenant_id=$1 AND obligation_id=$2
                """,
                work.tenant_id,
                work.parent_obligation_id,
                parent_version,
                now,
            )
            await conn.execute(
                """
                INSERT INTO work_obligation_versions (
                  id, tenant_id, obligation_id, aggregate_version, state,
                  transition_kind, transition_payload, command_result_id
                ) VALUES ($1,$2,$3,$4,'superseded_by_new_generation',
                          'successor_registered',$5::jsonb,$6)
                """,
                uuid7(),
                work.tenant_id,
                work.parent_obligation_id,
                parent_version,
                json.dumps(result),
                ids.command_result_id,
            )
            assert redrive_failure_head is not None
            assert redrive_failure is not None
            failure_version = int(redrive_failure_head["current_version"]) + 1
            failure_progress = FailureRecord.model_validate(
                {
                    **redrive_failure.model_dump(mode="json"),
                    "state": FailureState.REDRIVE_IN_PROGRESS,
                    "next_action": (
                        f"observe Work successor {work.obligation_id} "
                        f"generation {work.generation}"
                    ),
                    "remediation_evidence_refs": tuple(
                        sorted(
                            {
                                *redrive_failure.remediation_evidence_refs,
                                f"work:{work.obligation_id}",
                            }
                        )
                    ),
                    "reason": "authorized Work successor entered execution",
                    "updated_at": work.registered_at,
                }
            )
            await conn.execute(
                """
                UPDATE failure_record_heads
                SET current_version=$3, current_state=$4,
                    current_record_digest=$5, updated_at=$6
                WHERE tenant_id=$1 AND failure_id=$2
                """,
                work.tenant_id,
                redrive_failure.failure_id,
                failure_version,
                failure_progress.state,
                failure_progress.record_digest,
                work.registered_at,
            )
            await conn.execute(
                """
                INSERT INTO failure_record_versions (
                  id, tenant_id, failure_id, aggregate_version, state,
                  record_digest, record, transition_kind, command_result_id
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,
                          'work_redrive_successor_registered',$8)
                """,
                uuid7(),
                work.tenant_id,
                redrive_failure.failure_id,
                failure_version,
                failure_progress.state,
                failure_progress.record_digest,
                _dump(failure_progress),
                ids.command_result_id,
            )
            await conn.execute(
                """
                UPDATE work_obligation_lineage_heads
                SET current_obligation_id=$3, current_generation=$4, updated_at=$5
                WHERE tenant_id=$1 AND lineage_id=$2
                """,
                work.tenant_id,
                work.lineage_id,
                work.obligation_id,
                work.generation,
                now,
            )
        await conn.execute(
            """
            INSERT INTO work_obligation_heads (
              tenant_id, obligation_id, lineage_id, generation,
              current_version, current_state, updated_at
            ) VALUES ($1,$2,$3,$4,1,'registered',$5)
            """,
            work.tenant_id,
            work.obligation_id,
            work.lineage_id,
            work.generation,
            now,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,1,'registered','register',$4::jsonb,$5)
            """,
            uuid7(),
            work.tenant_id,
            work.obligation_id,
            _dump(work),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="work_obligation",
            object_id=work.obligation_id,
            object_version=1,
            semantic_transition="registered",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_obligation_registered",
        )

    async def decide(
        self,
        *,
        conn: asyncpg.Connection,
        command: WorkDecisionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        decision = command.decision
        head, spec = await self._locked_work(
            conn=conn,
            tenant_id=decision.tenant_id,
            obligation_id=decision.obligation_id,
        )
        self._require_work_version(
            head=head,
            expected_version=command.expected_version,
            expected_generation=decision.obligation_generation,
            expected_state=decision.from_state,
        )
        work = _json(spec["obligation"])
        minimum_rank = int(str(work["minimum_processing_class"])[1:])
        maximum_rank = int(str(work["maximum_processing_class"])[1:])
        if not minimum_rank <= decision.selected_processing_class.rank <= maximum_rank:
            raise InvariantViolation(
                "WORK_PROCESSING_CLASS_OUTSIDE_ENVELOPE",
                "selected processing class is outside the obligation envelope",
            )
        if not work_obligation_transition_allowed(
            decision.from_state, decision.to_state
        ) or decision.to_state is WorkObligationState.LEASED:
            raise InvariantViolation(
                "WORK_DECISION_TRANSITION",
                "illegal scheduler decision over work state",
            )
        if decision.next_eligible_at and decision.next_eligible_at >= spec["deadline"]:
            raise InvariantViolation(
                "WORK_DEFER_PAST_DEADLINE",
                "deferred wake time must precede the work deadline",
            )
        return await self._commit_work_transition(
            conn=conn,
            context=command.context,
            command=command,
            request_digest=command.request_digest,
            head=head,
            from_state=decision.from_state,
            to_state=decision.to_state,
            transition_kind="decision",
            transition_payload=decision,
            next_eligible_at=decision.next_eligible_at,
            wake_predicate=decision.wake_predicate,
            extra_insert=("decision", decision),
        )

    async def transition(
        self,
        *,
        conn: asyncpg.Connection,
        command: WorkStateTransitionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        transition = command.transition
        head, spec = await self._locked_work(
            conn=conn,
            tenant_id=transition.tenant_id,
            obligation_id=transition.obligation_id,
        )
        self._require_work_version(
            head=head,
            expected_version=command.expected_version,
            expected_generation=transition.obligation_generation,
            expected_state=transition.from_state,
        )
        if not work_obligation_transition_allowed(
            transition.from_state, transition.to_state
        ):
            raise InvariantViolation(
                "WORK_STATE_TRANSITION",
                "illegal work obligation lifecycle transition",
            )
        if transition.to_state in {
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        } and spec["owner_writer_id"] != "WorkLedgerApplier":
            if transition.from_state is not (
                WorkObligationState.OWNER_TERMINALIZATION_PENDING
            ) or not transition.owner_terminal_result_ref:
                raise InvariantViolation(
                    "WORK_OWNER_TERMINALIZATION_BYPASS",
                    "work cannot declare another semantic owner's terminal fate",
                )
        return await self._commit_work_transition(
            conn=conn,
            context=command.context,
            command=command,
            request_digest=command.request_digest,
            head=head,
            from_state=transition.from_state,
            to_state=transition.to_state,
            transition_kind="state_transition",
            transition_payload=transition,
            next_eligible_at=transition.next_eligible_at,
            wake_predicate=transition.wake_predicate,
        )

    async def grant_lease(
        self,
        *,
        conn: asyncpg.Connection,
        command: LeaseGrantCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        lease = command.lease
        head, spec = await self._locked_work(
            conn=conn,
            tenant_id=lease.tenant_id,
            obligation_id=lease.obligation_id,
        )
        self._require_work_version(
            head=head,
            expected_version=command.expected_obligation_version,
            expected_generation=lease.obligation_generation,
            expected_state=WorkObligationState.ELIGIBLE,
        )
        expected_fence = int(head["current_fence"]) + 1
        expected_attempt = int(head["attempt_count"]) + 1
        if lease.fence != expected_fence or lease.attempt != expected_attempt:
            raise InvariantViolation(
                "WORK_LEASE_FENCE",
                "lease fence and attempt must advance monotonically by one",
            )
        if lease.attempt > int(spec["maximum_attempts"]):
            raise InvariantViolation(
                "WORK_ATTEMPT_BUDGET",
                "lease would exceed the obligation attempt budget",
            )
        if lease.expires_at > spec["deadline"]:
            raise InvariantViolation(
                "WORK_LEASE_DEADLINE",
                "lease cannot outlive the work deadline",
            )
        if lease.effect_possible != spec["effect_possible"]:
            raise InvariantViolation(
                "WORK_LEASE_EFFECT_CLASS",
                "lease cannot weaken the obligation's effect-possible class",
            )
        next_version = int(head["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "obligation_id": str(lease.obligation_id),
            "obligation_version": next_version,
            "state": WorkObligationState.LEASED,
            "lease_token_id": str(lease.lease_token_id),
            "lease_version": 1,
            "fence": lease.fence,
            "attempt": lease.attempt,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="grant_work_lease",
            command=command,
            request_digest=command.request_digest,
            object_type="work_obligation",
            object_id=lease.obligation_id,
            object_version=next_version,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_heads (
              tenant_id, lease_token_id, obligation_id, obligation_generation,
              current_version, current_state, fence, attempt, owner_ref,
              heartbeat_deadline, expires_at, effect_possible, updated_at
            ) VALUES ($1,$2,$3,$4,1,'active',$5,$6,$7,$8,$9,$10,$11)
            """,
            lease.tenant_id,
            lease.lease_token_id,
            lease.obligation_id,
            lease.obligation_generation,
            lease.fence,
            lease.attempt,
            lease.owner_ref,
            lease.heartbeat_deadline,
            lease.expires_at,
            lease.effect_possible,
            lease.granted_at,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_versions (
              id, tenant_id, lease_token_id, aggregate_version, state,
              lease_payload, command_result_id
            ) VALUES ($1,$2,$3,1,'active',$4::jsonb,$5)
            """,
            uuid7(),
            lease.tenant_id,
            lease.lease_token_id,
            _dump(lease),
            ids.command_result_id,
        )
        await conn.execute(
            """
            UPDATE work_obligation_heads
            SET current_version=$3, current_state='leased',
                current_lease_token_id=$4, current_fence=$5,
                attempt_count=$6, next_eligible_at=NULL, wake_predicate=NULL,
                updated_at=$7
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            lease.tenant_id,
            lease.obligation_id,
            next_version,
            lease.lease_token_id,
            lease.fence,
            lease.attempt,
            lease.granted_at,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,'leased','lease_granted',$5::jsonb,$6)
            """,
            uuid7(),
            lease.tenant_id,
            lease.obligation_id,
            next_version,
            _dump(lease),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="work_obligation",
            object_id=lease.obligation_id,
            object_version=next_version,
            semantic_transition="leased",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_lease_granted",
        )

    async def resolve_lease(
        self,
        *,
        conn: asyncpg.Connection,
        command: LeaseResolutionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        resolution = command.resolution
        head, spec = await self._locked_work(
            conn=conn,
            tenant_id=resolution.tenant_id,
            obligation_id=resolution.obligation_id,
        )
        self._require_work_version(
            head=head,
            expected_version=command.expected_obligation_version,
            expected_generation=resolution.obligation_generation,
            expected_state=WorkObligationState.LEASED,
        )
        lease = await conn.fetchrow(
            """
            SELECT * FROM work_lease_token_heads
            WHERE tenant_id=$1 AND lease_token_id=$2 FOR UPDATE
            """,
            resolution.tenant_id,
            resolution.lease_token_id,
        )
        if (
            lease is None
            or int(lease["current_version"]) != command.expected_lease_version
            or lease["current_state"] != LeaseState.ACTIVE
            or int(lease["fence"]) != resolution.fence
            or head["current_lease_token_id"] != resolution.lease_token_id
            or int(head["current_fence"]) != resolution.fence
        ):
            raise InvariantViolation(
                "WORK_STALE_LEASE",
                "lease resolution does not hold the exact active fence",
            )
        if not lease_transition_allowed(
            LeaseState.ACTIVE, resolution.to_lease_state
        ) or not work_obligation_transition_allowed(
            WorkObligationState.LEASED, resolution.to_work_state
        ):
            raise InvariantViolation(
                "WORK_LEASE_TRANSITION",
                "illegal coordinated lease/work transition",
            )
        if now >= lease["expires_at"] and resolution.to_work_state in {
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.RETRY_WAIT,
            WorkObligationState.QUARANTINED,
        }:
            raise InvariantViolation(
                "WORK_EXPIRED_LEASE_COMMIT",
                "expired lease cannot commit results or schedule retry",
            )
        if (
            spec["effect_possible"]
            and resolution.to_work_state is WorkObligationState.RETRY_WAIT
            and not resolution.result_evidence_refs
        ):
            raise InvariantViolation(
                "WORK_EFFECT_RETRY_UNPROVEN",
                "effect-capable work requires evidence of no dispatch before retry",
            )
        if (
            not spec["effect_possible"]
            and spec["target_object_type"] == "repair_obligation"
            and resolution.to_work_state is WorkObligationState.COMPLETED
        ):
            await self._validate_repair_owner_result(
                conn=conn,
                spec=spec,
                resolution=resolution,
            )
        if spec["effect_possible"]:
            await self._validate_effect_resolution(
                conn=conn,
                resolution=resolution,
            )
        next_work_version = int(head["current_version"]) + 1
        next_lease_version = int(lease["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "obligation_id": str(resolution.obligation_id),
            "obligation_version": next_work_version,
            "work_state": resolution.to_work_state,
            "lease_token_id": str(resolution.lease_token_id),
            "lease_version": next_lease_version,
            "lease_state": resolution.to_lease_state,
            "fence": resolution.fence,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="resolve_work_lease",
            command=command,
            request_digest=command.request_digest,
            object_type="work_obligation",
            object_id=resolution.obligation_id,
            object_version=next_work_version,
            result=result,
        )
        await conn.execute(
            """
            UPDATE work_lease_token_heads
            SET current_version=$3, current_state=$4, updated_at=$5
            WHERE tenant_id=$1 AND lease_token_id=$2
            """,
            resolution.tenant_id,
            resolution.lease_token_id,
            next_lease_version,
            resolution.to_lease_state,
            resolution.resolved_at,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_versions (
              id, tenant_id, lease_token_id, aggregate_version, state,
              lease_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
            """,
            uuid7(),
            resolution.tenant_id,
            resolution.lease_token_id,
            next_lease_version,
            resolution.to_lease_state,
            _dump(resolution),
            ids.command_result_id,
        )
        await conn.execute(
            """
            UPDATE work_obligation_heads
            SET current_version=$3, current_state=$4,
                current_lease_token_id=NULL, next_eligible_at=$5,
                updated_at=$6
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            resolution.tenant_id,
            resolution.obligation_id,
            next_work_version,
            resolution.to_work_state,
            resolution.next_eligible_at,
            resolution.resolved_at,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,'lease_resolved',$6::jsonb,$7)
            """,
            uuid7(),
            resolution.tenant_id,
            resolution.obligation_id,
            next_work_version,
            resolution.to_work_state,
            _dump(resolution),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="work_obligation",
            object_id=resolution.obligation_id,
            object_version=next_work_version,
            semantic_transition=resolution.to_work_state,
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_lease_resolved",
        )

    async def heartbeat_lease(
        self,
        *,
        conn: asyncpg.Connection,
        command: LeaseHeartbeatCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        heartbeat = command.heartbeat
        lease = await conn.fetchrow(
            """
            SELECT l.*, w.current_lease_token_id, w.current_fence,
                   w.current_state AS work_state, w.generation AS work_generation
            FROM work_lease_token_heads l
            JOIN work_obligation_heads w
              ON w.tenant_id=l.tenant_id AND w.obligation_id=l.obligation_id
            WHERE l.tenant_id=$1 AND l.lease_token_id=$2
            FOR UPDATE OF l, w
            """,
            heartbeat.tenant_id,
            heartbeat.lease_token_id,
        )
        if (
            lease is None
            or int(lease["current_version"]) != command.expected_lease_version
            or lease["current_state"] != LeaseState.ACTIVE
            or lease["obligation_id"] != heartbeat.obligation_id
            or int(lease["obligation_generation"])
            != heartbeat.obligation_generation
            or int(lease["fence"]) != heartbeat.fence
            or lease["owner_ref"] != heartbeat.owner_ref
            or lease["heartbeat_deadline"]
            != heartbeat.expected_heartbeat_deadline
            or lease["expires_at"] != heartbeat.lease_expires_at
            or lease["work_state"] != WorkObligationState.LEASED
            or int(lease["work_generation"]) != heartbeat.obligation_generation
            or lease["current_lease_token_id"] != heartbeat.lease_token_id
            or int(lease["current_fence"]) != heartbeat.fence
        ):
            raise InvariantViolation(
                "WORK_STALE_HEARTBEAT",
                "heartbeat does not hold the exact active work/lease fence",
            )
        next_version = int(lease["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "lease_token_id": str(heartbeat.lease_token_id),
            "lease_version": next_version,
            "state": LeaseState.ACTIVE,
            "fence": heartbeat.fence,
            "heartbeat_deadline": heartbeat.extended_heartbeat_deadline.isoformat(),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="heartbeat_work_lease",
            command=command,
            request_digest=command.request_digest,
            object_type="work_lease_token",
            object_id=heartbeat.lease_token_id,
            object_version=next_version,
            result=result,
        )
        await conn.execute(
            """
            UPDATE work_lease_token_heads
            SET current_version=$3, heartbeat_deadline=$4, updated_at=$5
            WHERE tenant_id=$1 AND lease_token_id=$2
            """,
            heartbeat.tenant_id,
            heartbeat.lease_token_id,
            next_version,
            heartbeat.extended_heartbeat_deadline,
            heartbeat.heartbeat_at,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_versions (
              id, tenant_id, lease_token_id, aggregate_version, state,
              lease_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,'active',$5::jsonb,$6)
            """,
            uuid7(),
            heartbeat.tenant_id,
            heartbeat.lease_token_id,
            next_version,
            _dump(heartbeat),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="work_lease_token",
            object_id=heartbeat.lease_token_id,
            object_version=next_version,
            semantic_transition="heartbeat_extended",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_lease_heartbeat_extended",
        )

    async def take_over_lease(
        self,
        *,
        conn: asyncpg.Connection,
        command: LeaseTakeoverCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        takeover = command.takeover
        successor = takeover.successor
        head, spec = await self._locked_work(
            conn=conn,
            tenant_id=takeover.tenant_id,
            obligation_id=takeover.obligation_id,
        )
        self._require_work_version(
            head=head,
            expected_version=command.expected_obligation_version,
            expected_generation=takeover.obligation_generation,
            expected_state=WorkObligationState.LEASED,
        )
        predecessor = await conn.fetchrow(
            """
            SELECT * FROM work_lease_token_heads
            WHERE tenant_id=$1 AND lease_token_id=$2 FOR UPDATE
            """,
            takeover.tenant_id,
            takeover.predecessor_lease_token_id,
        )
        if (
            predecessor is None
            or int(predecessor["current_version"])
            != command.expected_predecessor_lease_version
            or predecessor["current_state"] != LeaseState.ACTIVE
            or predecessor["obligation_id"] != takeover.obligation_id
            or int(predecessor["obligation_generation"])
            != takeover.obligation_generation
            or int(predecessor["fence"]) != takeover.predecessor_fence
            or int(predecessor["attempt"]) != takeover.predecessor_attempt
            or predecessor["owner_ref"] != takeover.predecessor_owner_ref
            or predecessor["heartbeat_deadline"]
            != takeover.predecessor_heartbeat_deadline
            or head["current_lease_token_id"]
            != takeover.predecessor_lease_token_id
            or int(head["current_fence"]) != takeover.predecessor_fence
            or int(head["attempt_count"]) != takeover.predecessor_attempt
        ):
            raise InvariantViolation(
                "WORK_TAKEOVER_STALE_PREDECESSOR",
                "takeover does not bind the exact missed-heartbeat lease fence",
            )
        if successor.attempt > int(spec["maximum_attempts"]):
            raise InvariantViolation(
                "WORK_ATTEMPT_BUDGET",
                "takeover successor exceeds the work attempt budget",
            )
        if successor.expires_at > spec["deadline"]:
            raise InvariantViolation(
                "WORK_LEASE_DEADLINE",
                "takeover successor cannot outlive the work deadline",
            )
        if successor.effect_possible != bool(spec["effect_possible"]):
            raise InvariantViolation(
                "WORK_LEASE_EFFECT_CLASS",
                "takeover cannot weaken the work effect-possible class",
            )
        effect_states = await conn.fetch(
            """
            SELECT effect_attempt_id, current_version, current_state
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND lease_token_id=$2 AND lease_fence=$3
            """,
            takeover.tenant_id,
            takeover.predecessor_lease_token_id,
            takeover.predecessor_fence,
        )
        unsafe = {
            row["current_state"] for row in effect_states
        } - {
            ExternalEffectState.RESERVED,
            ExternalEffectState.CANCELLED,
            ExternalEffectState.EXPIRED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
        }
        if unsafe:
            raise InvariantViolation(
                "WORK_TAKEOVER_EFFECT_UNCERTAIN",
                "takeover is fenced until predecessor effects prove no effect",
                states=sorted(str(state) for state in unsafe),
            )
        if successor.effect_possible:
            expected_no_effect_refs = await self._takeover_no_effect_refs(
                conn=conn,
                tenant_id=takeover.tenant_id,
                lease_token_id=takeover.predecessor_lease_token_id,
                lease_fence=takeover.predecessor_fence,
                effect_rows=effect_states,
            )
            if set(takeover.no_effect_evidence_refs) != expected_no_effect_refs:
                raise InvariantViolation(
                    "WORK_TAKEOVER_NO_EFFECT_EVIDENCE_MISMATCH",
                    "effect-capable takeover lacks exact predecessor ledger evidence",
                )
        next_work_version = int(head["current_version"]) + 1
        next_predecessor_version = int(predecessor["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "obligation_id": str(takeover.obligation_id),
            "obligation_version": next_work_version,
            "state": WorkObligationState.LEASED,
            "predecessor_lease_token_id": str(takeover.predecessor_lease_token_id),
            "predecessor_lease_version": next_predecessor_version,
            "predecessor_state": LeaseState.SUPERSEDED_BY_NEW_LEASE,
            "successor_lease_token_id": str(successor.lease_token_id),
            "successor_lease_version": 1,
            "successor_fence": successor.fence,
            "successor_attempt": successor.attempt,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            command_kind="take_over_work_lease",
            command=command,
            request_digest=command.request_digest,
            object_type="work_obligation",
            object_id=takeover.obligation_id,
            object_version=next_work_version,
            result=result,
        )
        await conn.execute(
            """
            UPDATE work_lease_token_heads
            SET current_version=$3, current_state='superseded_by_new_lease',
                updated_at=$4
            WHERE tenant_id=$1 AND lease_token_id=$2
            """,
            takeover.tenant_id,
            takeover.predecessor_lease_token_id,
            next_predecessor_version,
            takeover.taken_over_at,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_versions (
              id, tenant_id, lease_token_id, aggregate_version, state,
              lease_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,'superseded_by_new_lease',$5::jsonb,$6)
            """,
            uuid7(),
            takeover.tenant_id,
            takeover.predecessor_lease_token_id,
            next_predecessor_version,
            _dump(takeover),
            ids.command_result_id,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_heads (
              tenant_id, lease_token_id, obligation_id, obligation_generation,
              current_version, current_state, fence, attempt, owner_ref,
              heartbeat_deadline, expires_at, effect_possible, updated_at
            ) VALUES ($1,$2,$3,$4,1,'active',$5,$6,$7,$8,$9,$10,$11)
            """,
            successor.tenant_id,
            successor.lease_token_id,
            successor.obligation_id,
            successor.obligation_generation,
            successor.fence,
            successor.attempt,
            successor.owner_ref,
            successor.heartbeat_deadline,
            successor.expires_at,
            successor.effect_possible,
            successor.granted_at,
        )
        await conn.execute(
            """
            INSERT INTO work_lease_token_versions (
              id, tenant_id, lease_token_id, aggregate_version, state,
              lease_payload, command_result_id
            ) VALUES ($1,$2,$3,1,'active',$4::jsonb,$5)
            """,
            uuid7(),
            successor.tenant_id,
            successor.lease_token_id,
            _dump(successor),
            ids.command_result_id,
        )
        await conn.execute(
            """
            UPDATE work_obligation_heads
            SET current_version=$3, current_lease_token_id=$4,
                current_fence=$5, attempt_count=$6, updated_at=$7
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            takeover.tenant_id,
            takeover.obligation_id,
            next_work_version,
            successor.lease_token_id,
            successor.fence,
            successor.attempt,
            takeover.taken_over_at,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,'leased','lease_taken_over',$5::jsonb,$6)
            """,
            uuid7(),
            takeover.tenant_id,
            takeover.obligation_id,
            next_work_version,
            _dump(takeover),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="WorkLedgerApplier",
            object_type="work_obligation",
            object_id=takeover.obligation_id,
            object_version=next_work_version,
            semantic_transition="lease_taken_over",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_lease_taken_over",
        )

    async def _validate_effect_resolution(self, *, conn, resolution) -> None:
        rows = await conn.fetch(
            """
            SELECT effect_attempt_id, current_state
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND lease_token_id=$2 AND lease_fence=$3
            """,
            resolution.tenant_id,
            resolution.lease_token_id,
            resolution.fence,
        )
        if not rows:
            expected_ref = (
                f"effect-ledger:no-attempt:{resolution.lease_token_id}:"
                f"fence:{resolution.fence}"
            )
            if set(resolution.result_evidence_refs) != {expected_ref}:
                raise InvariantViolation(
                    "WORK_EFFECT_NO_ATTEMPT_EVIDENCE_REQUIRED",
                    "effect-capable work without an attempt requires exact ledger evidence",
                )
            if resolution.to_work_state is WorkObligationState.COMPLETED:
                raise InvariantViolation(
                    "WORK_EFFECT_COMPLETION_WITHOUT_ATTEMPT",
                    "effect-capable work cannot complete without a succeeded attempt",
                )
            return
        states = {row["current_state"] for row in rows}
        unsafe_or_unknown = states - {
            ExternalEffectState.RESERVED,
            ExternalEffectState.CANCELLED,
            ExternalEffectState.EXPIRED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.FAILED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.TERMINAL_PARTIAL,
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        }
        if unsafe_or_unknown and resolution.to_work_state is not (
            WorkObligationState.RECONCILIATION_REQUIRED
        ):
            raise InvariantViolation(
                "WORK_EFFECT_RECONCILIATION_BYPASS",
                "dispatched effect is not in a safe terminal state",
                effect_states=sorted(str(state) for state in states),
            )
        receipt_states = await conn.fetch(
            """
            SELECT effect_state
            FROM execution_receipts
            WHERE tenant_id=$1
              AND effect_attempt_id = ANY($2::uuid[])
              AND receipt_id::text = ANY($3::text[])
            """,
            resolution.tenant_id,
            [row["effect_attempt_id"] for row in rows],
            list(resolution.result_evidence_refs),
        )
        evidenced_states = {row["effect_state"] for row in receipt_states}
        if ExternalEffectState.SUCCEEDED in states:
            if (
                resolution.to_work_state is not WorkObligationState.COMPLETED
                or ExternalEffectState.SUCCEEDED not in evidenced_states
            ):
                raise InvariantViolation(
                    "WORK_EFFECT_SUCCESS_RECEIPT_REQUIRED",
                    "succeeded effect requires exact receipt-backed work completion",
                )
        if ExternalEffectState.COMPENSATED in states and (
            resolution.to_work_state
            not in {WorkObligationState.CANCELLED, WorkObligationState.NO_OP}
            or ExternalEffectState.COMPENSATED not in evidenced_states
        ):
            raise InvariantViolation(
                "WORK_EFFECT_COMPENSATION_RECEIPT_REQUIRED",
                "compensated effect requires exact receipt-backed non-success closure",
            )
        residual_states = states & {
            ExternalEffectState.TERMINAL_PARTIAL,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        }
        if residual_states and (
            resolution.to_work_state
            not in {
                WorkObligationState.QUARANTINED,
                WorkObligationState.RECONCILIATION_REQUIRED,
                WorkObligationState.CANCELLED,
            }
            or not residual_states <= evidenced_states
        ):
            raise InvariantViolation(
                "WORK_EFFECT_RESIDUAL_RECEIPT_REQUIRED",
                "partial or failed compensation requires exact residual receipts",
            )
        known_no_effect = states <= {
            ExternalEffectState.RESERVED,
            ExternalEffectState.CANCELLED,
            ExternalEffectState.EXPIRED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.FAILED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
        }
        if (
            known_no_effect
            and states
            & {
                ExternalEffectState.REJECTED,
                ExternalEffectState.FAILED,
                ExternalEffectState.RECONCILED_NO_EFFECT,
            }
            and not evidenced_states
        ):
            raise InvariantViolation(
                "WORK_EFFECT_NO_EFFECT_RECEIPT_REQUIRED",
                "retry after provider failure/no-effect requires exact receipt evidence",
            )

    async def _takeover_no_effect_refs(
        self,
        *,
        conn,
        tenant_id,
        lease_token_id,
        lease_fence,
        effect_rows,
    ) -> set[str]:
        if not effect_rows:
            return {
                f"effect-ledger:no-attempt:{lease_token_id}:fence:{lease_fence}"
            }
        refs: set[str] = set()
        for row in effect_rows:
            state = ExternalEffectState(str(row["current_state"]))
            if state is ExternalEffectState.RESERVED:
                refs.add(
                    f"external-effect-attempt:{row['effect_attempt_id']}:"
                    f"state:reserved:version:{int(row['current_version'])}"
                )
                continue
            receipt_id = await conn.fetchval(
                """
                SELECT receipt_id FROM execution_receipts
                WHERE tenant_id=$1 AND effect_attempt_id=$2
                  AND effect_version=$3 AND effect_state=$4
                """,
                tenant_id,
                row["effect_attempt_id"],
                int(row["current_version"]),
                state,
            )
            if receipt_id is None:
                raise InvariantViolation(
                    "WORK_TAKEOVER_NO_EFFECT_RECEIPT_MISSING",
                    "terminal predecessor effect lacks its exact current receipt",
                )
            refs.add(f"execution-receipt:{receipt_id}")
        return refs

    async def _validate_repair_owner_result(self, *, conn, spec, resolution) -> None:
        prefix = "agency-command-result:"
        result_ids: list[UUID] = []
        for ref in resolution.result_evidence_refs:
            if ref.startswith(prefix):
                try:
                    result_ids.append(UUID(ref.removeprefix(prefix)))
                except ValueError:
                    continue
        if len(result_ids) != 1:
            raise InvariantViolation(
                "WORK_REPAIR_OWNER_RESULT_REQUIRED",
                "repair child completion requires one exact RepairLedger CommandResult",
            )
        row = await conn.fetchrow(
            """
            SELECT writer_id, command_kind, object_type, object_id, result
            FROM agency_command_results
            WHERE tenant_id=$1 AND id=$2
            """,
            resolution.tenant_id,
            result_ids[0],
        )
        result = _json(row["result"]) if row is not None else {}
        if (
            row is None
            or row["writer_id"] != "RepairLedgerApplier"
            or row["command_kind"] != "apply_repair_receipt"
            or row["object_type"] != spec["target_object_type"]
            or row["object_id"] != spec["target_object_id"]
            or result.get("repair_state")
            not in {
                "repaired",
                "no_op",
                "adjudicated_residue",
                "exhausted",
                "escalated",
            }
        ):
            raise InvariantViolation(
                "WORK_REPAIR_OWNER_RESULT_MISMATCH",
                "repair child evidence is not the exact terminal receipt result",
            )

    async def _prior(self, *, conn, command):
        return await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="WorkLedgerApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )

    async def _locked_work(self, *, conn, tenant_id, obligation_id):
        head = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2 FOR UPDATE
            """,
            tenant_id,
            obligation_id,
        )
        if head is None:
            raise InvariantViolation("WORK_NOT_FOUND", "work obligation does not exist")
        spec = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_specs
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            tenant_id,
            obligation_id,
        )
        return head, spec

    def _require_work_version(
        self,
        *,
        head,
        expected_version,
        expected_generation,
        expected_state,
    ):
        if (
            int(head["current_version"]) != expected_version
            or int(head["generation"]) != expected_generation
            or head["current_state"] != expected_state
        ):
            raise InvariantViolation(
                "WORK_OBLIGATION_CAS",
                "work version, generation, or state does not match",
            )

    async def _commit_work_transition(
        self,
        *,
        conn,
        context,
        command,
        request_digest,
        head,
        from_state,
        to_state,
        transition_kind,
        transition_payload,
        next_eligible_at,
        wake_predicate,
        extra_insert=None,
    ):
        next_version = int(head["current_version"]) + 1
        ids = AgencyProtocolIds.new()
        result = {
            "obligation_id": str(head["obligation_id"]),
            "obligation_version": next_version,
            "from_state": from_state,
            "state": to_state,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="WorkLedgerApplier",
            command_kind=f"work_{transition_kind}",
            command=command,
            request_digest=request_digest,
            object_type="work_obligation",
            object_id=head["obligation_id"],
            object_version=next_version,
            result=result,
        )
        if extra_insert:
            _, decision = extra_insert
            await conn.execute(
                """
                INSERT INTO work_decisions (
                  decision_id, tenant_id, obligation_id, obligation_generation,
                  obligation_version, decision_digest, selected_processing_class,
                  from_state, to_state, decision, command_result_id, decided_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)
                """,
                decision.decision_id,
                decision.tenant_id,
                decision.obligation_id,
                decision.obligation_generation,
                next_version,
                decision.decision_digest,
                decision.selected_processing_class,
                decision.from_state,
                decision.to_state,
                _dump(decision),
                ids.command_result_id,
                decision.decided_at,
            )
        await conn.execute(
            """
            UPDATE work_obligation_heads
            SET current_version=$3, current_state=$4, next_eligible_at=$5,
                wake_predicate=$6, updated_at=$7
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            context.tenant_id,
            head["obligation_id"],
            next_version,
            to_state,
            next_eligible_at,
            wake_predicate,
            context.issued_at,
        )
        await conn.execute(
            """
            INSERT INTO work_obligation_versions (
              id, tenant_id, obligation_id, aggregate_version, state,
              transition_kind, transition_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
            """,
            uuid7(),
            context.tenant_id,
            head["obligation_id"],
            next_version,
            to_state,
            transition_kind,
            _dump(transition_payload),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="WorkLedgerApplier",
            object_type="work_obligation",
            object_id=head["obligation_id"],
            object_version=next_version,
            semantic_transition=to_state,
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="work_obligation_transition_committed",
        )


class ExecutionLedgerApplier:
    """Own adapter guarantees, effect reservations, observations, and receipts."""

    async def register_capabilities(
        self,
        *,
        conn: asyncpg.Connection,
        command: AdapterCapabilityRegistrationCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        capabilities = command.capabilities
        head = await conn.fetchrow(
            """
            SELECT * FROM action_adapter_capability_heads
            WHERE tenant_id=$1 AND capability_id=$2 FOR UPDATE
            """,
            capabilities.tenant_id,
            capabilities.capability_id,
        )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "ADAPTER_CAPABILITY_CAS",
                "adapter capability expected version does not match head",
            )
        if head:
            prior_value = ActionAdapterCapabilities.model_validate(
                _json(
                    await conn.fetchval(
                        """
                        SELECT capabilities FROM action_adapter_capability_versions
                        WHERE tenant_id=$1 AND capability_id=$2
                          AND aggregate_version=$3
                        """,
                        capabilities.tenant_id,
                        capabilities.capability_id,
                        current_version,
                    )
                )
            )
            if (
                prior_value.adapter_name != capabilities.adapter_name
                or prior_value.provider_name != capabilities.provider_name
            ):
                raise InvariantViolation(
                    "ADAPTER_CAPABILITY_IDENTITY_MUTATION",
                    "capability successor changed adapter/provider identity",
                )
        next_version = current_version + 1
        ids = AgencyProtocolIds.new()
        result = {
            "capability_id": str(capabilities.capability_id),
            "capability_aggregate_version": next_version,
            "capability_version": capabilities.capability_version,
            "capability_digest": capabilities.capability_digest,
            "autonomous_repeat_safe": capabilities.autonomous_repeat_safe,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            command_kind="register_action_adapter_capabilities",
            command=command,
            request_digest=command.request_digest,
            object_type="action_adapter_capabilities",
            object_id=capabilities.capability_id,
            object_version=next_version,
            result=result,
        )
        if head is None:
            await conn.execute(
                """
                INSERT INTO action_adapter_capability_heads (
                  tenant_id, capability_id, current_version,
                  current_capability_version, current_capability_digest,
                  expires_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                capabilities.tenant_id,
                capabilities.capability_id,
                next_version,
                capabilities.capability_version,
                capabilities.capability_digest,
                capabilities.expires_at,
                now,
            )
        else:
            await conn.execute(
                """
                UPDATE action_adapter_capability_heads
                SET current_version=$3, current_capability_version=$4,
                    current_capability_digest=$5, expires_at=$6, updated_at=$7
                WHERE tenant_id=$1 AND capability_id=$2
                """,
                capabilities.tenant_id,
                capabilities.capability_id,
                next_version,
                capabilities.capability_version,
                capabilities.capability_digest,
                capabilities.expires_at,
                now,
            )
        await conn.execute(
            """
            INSERT INTO action_adapter_capability_versions (
              id, tenant_id, capability_id, aggregate_version,
              capability_version, capability_digest, adapter_name, provider_name,
              verified_at, expires_at, capabilities, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)
            """,
            uuid7(),
            capabilities.tenant_id,
            capabilities.capability_id,
            next_version,
            capabilities.capability_version,
            capabilities.capability_digest,
            capabilities.adapter_name,
            capabilities.provider_name,
            capabilities.verified_at,
            capabilities.expires_at,
            _dump(capabilities),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            object_type="action_adapter_capabilities",
            object_id=capabilities.capability_id,
            object_version=next_version,
            semantic_transition="registered",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="action_adapter_capability_registered",
        )

    async def reserve(
        self,
        *,
        conn: asyncpg.Connection,
        command: EffectReservationCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        attempt = command.attempt
        spec = await _intervention_spec(
            conn,
            tenant_id=attempt.tenant_id,
            digest=attempt.intervention_spec_digest,
        )
        if spec.operation != attempt.operation:
            raise InvariantViolation(
                "EFFECT_OPERATION_SPEC_MISMATCH",
                "effect operation differs from exact InterventionSpec",
            )
        decision = await _require_live_authorization(
            conn,
            tenant_id=attempt.tenant_id,
            decision_id=attempt.authorization_decision_id,
            intervention_spec_digest=attempt.intervention_spec_digest,
            operation=attempt.operation,
            target_refs=attempt.target_grounding_refs,
            at=now,
        )
        capabilities = await self._capabilities(
            conn=conn,
            attempt=attempt,
            at=now,
        )
        if (
            spec.action_adapter_version != attempt.capability_version
            or spec.action_adapter_capability_digest != attempt.capability_digest
        ):
            raise InvariantViolation(
                "EFFECT_ADAPTER_SPEC_MISMATCH",
                "effect does not use the adapter version/digest frozen in the spec",
            )
        if not capabilities.autonomous_repeat_safe and not (
            attempt.duplicate_or_unknown_risk_authorization_ref
        ):
            raise InvariantViolation(
                "EFFECT_UNSAFE_PROVIDER_SEMANTICS",
                "provider has neither idempotency nor reconciliation guarantee",
            )
        if (
            capabilities.idempotency_retention_until
            and capabilities.idempotency_retention_until < attempt.dispatch_deadline
        ):
            raise InvariantViolation(
                "EFFECT_IDEMPOTENCY_RETENTION_SHORT",
                "provider key retention does not cover the dispatch window",
            )
        await self._validate_task_work_and_lease(conn=conn, attempt=attempt, at=now)
        if attempt.compensates_effect_attempt_id is not None:
            await self._validate_compensation_reservation(
                conn=conn,
                attempt=attempt,
            )
        lineage = await conn.fetchrow(
            """
            SELECT * FROM external_effect_attempt_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2 FOR UPDATE
            """,
            attempt.tenant_id,
            attempt.lineage_id,
        )
        if attempt.generation == 1:
            if lineage is not None:
                raise InvariantViolation(
                    "EFFECT_LINEAGE_EXISTS",
                    "first effect generation cannot replace existing lineage",
                )
        else:
            if (
                lineage is None
                or lineage["current_effect_attempt_id"] != attempt.prior_attempt_id
                or int(lineage["current_generation"]) + 1 != attempt.generation
            ):
                raise InvariantViolation(
                    "EFFECT_SUCCESSOR_CAS",
                    "effect retry does not extend the exact current lineage head",
                )
            prior_head = await conn.fetchrow(
                """
                SELECT * FROM external_effect_attempt_heads
                WHERE tenant_id=$1 AND effect_attempt_id=$2 FOR UPDATE
                """,
                attempt.tenant_id,
                attempt.prior_attempt_id,
            )
            if prior_head["current_state"] not in {
                ExternalEffectState.REJECTED,
                ExternalEffectState.FAILED,
                ExternalEffectState.RECONCILED_NO_EFFECT,
            }:
                raise InvariantViolation(
                    "EFFECT_RETRY_UNSAFE",
                    "new attempt requires a terminal known-no-effect/failed predecessor",
                )
            if prior_head["canonical_request_hash"] != attempt.canonical_request_hash:
                raise InvariantViolation(
                    "EFFECT_RETRY_REQUEST_DRIFT",
                    "retry generation changed the canonical request identity",
                )
        attempt_count = await conn.fetchval(
            """
            SELECT count(*) FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND lineage_id=$2
            """,
            attempt.tenant_id,
            attempt.lineage_id,
        )
        if int(attempt_count) >= decision.attempt_budget:
            raise InvariantViolation(
                "EFFECT_AUTHORIZATION_ATTEMPT_BUDGET",
                "effect attempt exceeds authorization budget",
            )
        ids = AgencyProtocolIds.new()
        result = {
            "effect_attempt_id": str(attempt.effect_attempt_id),
            "lineage_id": str(attempt.lineage_id),
            "generation": attempt.generation,
            "effect_version": 1,
            "state": ExternalEffectState.RESERVED,
            "attempt_digest": attempt.attempt_digest,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            command_kind="reserve_external_effect",
            command=command,
            request_digest=command.request_digest,
            object_type="external_effect_attempt",
            object_id=attempt.effect_attempt_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO external_effect_provider_keys (
              tenant_id, capability_id, provider_idempotency_key, lineage_id,
              canonical_request_hash, registered_at
            ) VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (tenant_id, capability_id, provider_idempotency_key)
            DO NOTHING
            """,
            attempt.tenant_id,
            attempt.capability_id,
            attempt.provider_idempotency_key,
            attempt.lineage_id,
            attempt.canonical_request_hash,
            now,
        )
        key_row = await conn.fetchrow(
            """
            SELECT lineage_id, canonical_request_hash
            FROM external_effect_provider_keys
            WHERE tenant_id=$1 AND capability_id=$2 AND provider_idempotency_key=$3
            """,
            attempt.tenant_id,
            attempt.capability_id,
            attempt.provider_idempotency_key,
        )
        if (
            key_row["lineage_id"] != attempt.lineage_id
            or key_row["canonical_request_hash"] != attempt.canonical_request_hash
        ):
            raise InvariantViolation(
                "EFFECT_PROVIDER_KEY_REUSE",
                "provider idempotency key was reused for a different effect identity",
            )
        if lineage is None:
            await conn.execute(
                """
                INSERT INTO external_effect_attempt_lineage_heads (
                  tenant_id, lineage_id, current_effect_attempt_id,
                  current_generation, updated_at
                ) VALUES ($1,$2,$3,$4,$5)
                """,
                attempt.tenant_id,
                attempt.lineage_id,
                attempt.effect_attempt_id,
                attempt.generation,
                now,
            )
        else:
            await conn.execute(
                """
                UPDATE external_effect_attempt_lineage_heads
                SET current_effect_attempt_id=$3, current_generation=$4, updated_at=$5
                WHERE tenant_id=$1 AND lineage_id=$2
                """,
                attempt.tenant_id,
                attempt.lineage_id,
                attempt.effect_attempt_id,
                attempt.generation,
                now,
            )
        await conn.execute(
            """
            INSERT INTO external_effect_attempt_heads (
              tenant_id, effect_attempt_id, lineage_id, generation,
              prior_attempt_id, episode_id, task_id, intervention_spec_digest,
              authorization_decision_id, capability_id, capability_version,
              capability_digest, operation, canonical_request_hash,
              provider_idempotency_key, work_obligation_id,
              work_obligation_generation, lease_token_id, lease_fence,
              dispatch_deadline, current_version, current_state,
              current_attempt_digest, reserved_at, updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
              $17,$18,$19,$20,1,'reserved',$21,$22,$23
            )
            """,
            attempt.tenant_id,
            attempt.effect_attempt_id,
            attempt.lineage_id,
            attempt.generation,
            attempt.prior_attempt_id,
            attempt.episode_id,
            attempt.task_id,
            attempt.intervention_spec_digest,
            attempt.authorization_decision_id,
            attempt.capability_id,
            attempt.capability_version,
            attempt.capability_digest,
            attempt.operation,
            attempt.canonical_request_hash,
            attempt.provider_idempotency_key,
            attempt.work_obligation_id,
            attempt.work_obligation_generation,
            attempt.lease_token_id,
            attempt.lease_fence,
            attempt.dispatch_deadline,
            attempt.attempt_digest,
            attempt.reserved_at,
            now,
        )
        await conn.execute(
            """
            INSERT INTO external_effect_attempt_versions (
              id, tenant_id, effect_attempt_id, aggregate_version, state,
              transition_kind, attempt_payload, command_result_id
            ) VALUES ($1,$2,$3,1,'reserved','reserve',$4::jsonb,$5)
            """,
            uuid7(),
            attempt.tenant_id,
            attempt.effect_attempt_id,
            _dump(attempt),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            object_type="external_effect_attempt",
            object_id=attempt.effect_attempt_id,
            object_version=1,
            semantic_transition="reserved",
            event_payload=result,
            intervention_spec_digest=attempt.intervention_spec_digest,
            destination_operation="external_effect_reserved",
        )

    async def transition(
        self,
        *,
        conn: asyncpg.Connection,
        command: EffectTransitionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = _revalidate(command)
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await self._prior(conn=conn, command=command)
        if prior:
            return prior
        observation = command.observation
        head = await conn.fetchrow(
            """
            SELECT * FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND effect_attempt_id=$2 FOR UPDATE
            """,
            observation.tenant_id,
            observation.effect_attempt_id,
        )
        if head is None:
            raise InvariantViolation(
                "EFFECT_ATTEMPT_NOT_FOUND", "external effect attempt does not exist"
            )
        if (
            int(head["current_version"]) != command.expected_version
            or head["current_state"] != observation.from_state
        ):
            raise InvariantViolation(
                "EFFECT_ATTEMPT_CAS",
                "effect expected version/state does not match current head",
            )
        if not external_effect_transition_allowed(
            observation.from_state, observation.to_state
        ):
            raise InvariantViolation(
                "EFFECT_TRANSITION",
                "illegal external effect lifecycle transition",
            )
        if observation.to_state is ExternalEffectState.DISPATCH_INTENT_RECORDED:
            if now >= head["dispatch_deadline"]:
                raise InvariantViolation(
                    "EFFECT_DISPATCH_DEADLINE",
                    "dispatch intent cannot be recorded after its deadline",
                )
            await _require_live_authorization(
                conn,
                tenant_id=observation.tenant_id,
                decision_id=head["authorization_decision_id"],
                intervention_spec_digest=head["intervention_spec_digest"],
                operation=head["operation"],
                at=now,
            )
            await self._validate_live_effect_fence(conn=conn, head=head, at=now)
            await self._capabilities_from_head(conn=conn, head=head, at=now)
        if observation.to_state is ExternalEffectState.COMPENSATION_PROPOSED:
            await self._validate_compensation_proposal(
                conn=conn,
                head=head,
                observation=observation,
            )
        elif observation.to_state in {
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        }:
            await self._validate_compensation_proposal_terminal_fate(
                conn=conn,
                head=head,
                observation=observation,
            )
        elif observation.to_state is ExternalEffectState.COMPENSATION_AUTHORIZED:
            await self._validate_compensation_authorization(
                conn=conn,
                head=head,
                observation=observation,
                at=now,
            )
        elif observation.to_state is ExternalEffectState.COMPENSATION_ATTEMPT_LINKED:
            await self._validate_compensation_link(
                conn=conn,
                head=head,
                observation=observation,
            )
        elif observation.to_state in {
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_UNKNOWN,
        }:
            await self._validate_compensation_outcome(
                conn=conn,
                head=head,
                observation=observation,
            )
        next_version = int(head["current_version"]) + 1
        receipt = self._receipt(head=head, observation=observation, version=next_version)
        ids = AgencyProtocolIds.new()
        result = {
            "effect_attempt_id": str(observation.effect_attempt_id),
            "effect_version": next_version,
            "from_state": observation.from_state,
            "state": observation.to_state,
            "execution_receipt_id": str(receipt.receipt_id),
            "execution_receipt_digest": receipt.receipt_digest,
            "compensation_spec_digest": (
                observation.compensation_intervention_spec_digest
                or head["current_compensation_spec_digest"]
            ),
            "compensation_authorization_decision_id": str(
                observation.compensation_authorization_decision_id
                or head["current_compensation_authorization_decision_id"]
                or ""
            )
            or None,
            "compensation_attempt_id": str(
                observation.compensation_attempt_id
                or head["current_compensation_attempt_id"]
                or ""
            )
            or None,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            command_kind="transition_external_effect",
            command=command,
            request_digest=command.request_digest,
            object_type="external_effect_attempt",
            object_id=observation.effect_attempt_id,
            object_version=next_version,
            result=result,
        )
        await conn.execute(
            """
            UPDATE external_effect_attempt_heads
            SET current_version=$3, current_state=$4, updated_at=$5,
                current_compensation_spec_digest=COALESCE($6, current_compensation_spec_digest),
                current_compensation_authorization_decision_id=COALESCE(
                  $7, current_compensation_authorization_decision_id
                ),
                current_compensation_attempt_id=COALESCE(
                  $8, current_compensation_attempt_id
                )
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            """,
            observation.tenant_id,
            observation.effect_attempt_id,
            next_version,
            observation.to_state,
            now,
            observation.compensation_intervention_spec_digest,
            observation.compensation_authorization_decision_id,
            observation.compensation_attempt_id,
        )
        await conn.execute(
            """
            INSERT INTO external_effect_attempt_versions (
              id, tenant_id, effect_attempt_id, aggregate_version, state,
              transition_kind, attempt_payload, command_result_id
            ) VALUES ($1,$2,$3,$4,$5,'observation',$6::jsonb,$7)
            """,
            uuid7(),
            observation.tenant_id,
            observation.effect_attempt_id,
            next_version,
            observation.to_state,
            _dump(observation),
            ids.command_result_id,
        )
        await conn.execute(
            """
            INSERT INTO execution_receipts (
              receipt_id, tenant_id, effect_attempt_id, effect_version,
              effect_state, receipt_digest, requested, provider_accepted,
              externally_observed, partial, reconciled, compensated,
              receipt, command_result_id, observed_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15
            )
            """,
            receipt.receipt_id,
            receipt.tenant_id,
            receipt.effect_attempt_id,
            receipt.effect_version,
            receipt.effect_state,
            receipt.receipt_digest,
            receipt.requested,
            receipt.provider_accepted,
            receipt.externally_observed,
            receipt.partial,
            receipt.reconciled,
            receipt.compensated,
            _dump(receipt),
            ids.command_result_id,
            receipt.observed_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id="ExecutionLedgerApplier",
            object_type="external_effect_attempt",
            object_id=observation.effect_attempt_id,
            object_version=next_version,
            semantic_transition=observation.to_state,
            event_payload=result,
            intervention_spec_digest=head["intervention_spec_digest"],
            destination_operation="external_effect_transition_committed",
        )

    async def _validate_compensation_proposal(
        self,
        *,
        conn,
        head,
        observation,
    ) -> None:
        original_spec = await _intervention_spec(
            conn,
            tenant_id=observation.tenant_id,
            digest=head["intervention_spec_digest"],
        )
        compensation_spec = await _intervention_spec(
            conn,
            tenant_id=observation.tenant_id,
            digest=observation.compensation_intervention_spec_digest,
        )
        proposal_fate = await conn.fetchval(
            """
            SELECT p.current_fate
            FROM consequential_intervention_specs s
            JOIN consequential_proposals p
              ON p.tenant_id=s.tenant_id
             AND p.id=s.registered_by_proposal_id
             AND p.proposal_version=s.registered_by_proposal_version
            WHERE s.tenant_id=$1 AND s.spec_digest=$2
            """,
            observation.tenant_id,
            compensation_spec.spec_digest,
        )
        capabilities = await self._capabilities_from_head(
            conn=conn,
            head=head,
            at=head["reserved_at"],
        )
        required_parent_ref = f"external-effect-attempt:{head['effect_attempt_id']}"
        if (
            compensation_spec.spec_digest == original_spec.spec_digest
            or required_parent_ref not in compensation_spec.grounding_dependency_refs
            or not original_spec.reversible
            or not original_spec.compensation_declaration
            or not capabilities.compensation_supported
            or proposal_fate != "open"
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_PROPOSAL_INVALID",
                "compensation requires a separate linked spec and supported reversal",
            )

    async def _validate_compensation_authorization(
        self,
        *,
        conn,
        head,
        observation,
        at,
    ) -> None:
        if (
            head["current_compensation_spec_digest"]
            != observation.compensation_intervention_spec_digest
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_SPEC_DRIFT",
                "compensation authorization changed the proposed spec",
            )
        spec = await _intervention_spec(
            conn,
            tenant_id=observation.tenant_id,
            digest=observation.compensation_intervention_spec_digest,
        )
        target_ref = (
            f"referent:{spec.target_referent.referent_id}:"
            f"v{spec.target_referent.referent_version}"
        )
        await _require_live_authorization(
            conn,
            tenant_id=observation.tenant_id,
            decision_id=observation.compensation_authorization_decision_id,
            intervention_spec_digest=spec.spec_digest,
            operation=spec.operation,
            target_refs=(target_ref,),
            at=at,
        )

    async def _validate_compensation_proposal_terminal_fate(
        self,
        *,
        conn,
        head,
        observation,
    ) -> None:
        proposal_fate = {
            ExternalEffectState.COMPENSATION_REJECTED: "rejected",
            ExternalEffectState.COMPENSATION_EXPIRED: "expired",
        }[observation.to_state]
        review = await conn.fetchrow(
            """
            SELECT p.current_fate, r.command_result_id
            FROM consequential_intervention_specs s
            JOIN consequential_proposals p
              ON p.tenant_id=s.tenant_id
             AND p.id=s.registered_by_proposal_id
             AND p.proposal_version=s.registered_by_proposal_version
            LEFT JOIN consequential_proposal_reviews r
              ON r.tenant_id=p.tenant_id
             AND r.proposal_id=p.id
             AND r.proposal_version=p.proposal_version
             AND r.to_fate_version=p.current_fate_version
            WHERE s.tenant_id=$1 AND s.spec_digest=$2
            """,
            observation.tenant_id,
            head["current_compensation_spec_digest"],
        )
        expected_ref = (
            f"agency-command-result:{review['command_result_id']}"
            if review is not None and review["command_result_id"] is not None
            else None
        )
        if (
            review is None
            or review["current_fate"] != proposal_fate
            or expected_ref not in observation.external_state_evidence_refs
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_PROPOSAL_FATE_UNBOUND",
                "compensation terminal fate requires the exact current proposal review",
            )

    async def _validate_compensation_reservation(self, *, conn, attempt) -> None:
        original = await conn.fetchrow(
            """
            SELECT current_state, current_compensation_spec_digest,
                   current_compensation_authorization_decision_id
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            FOR UPDATE
            """,
            attempt.tenant_id,
            attempt.compensates_effect_attempt_id,
        )
        if (
            original is None
            or original["current_state"]
            != ExternalEffectState.COMPENSATION_AUTHORIZED
            or original["current_compensation_spec_digest"]
            != attempt.intervention_spec_digest
            or original["current_compensation_authorization_decision_id"]
            != attempt.authorization_decision_id
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_RESERVATION_UNBOUND",
                "compensation attempt does not bind the authorized original effect",
            )

    async def _validate_compensation_link(
        self,
        *,
        conn,
        head,
        observation,
    ) -> None:
        value = await conn.fetchval(
            """
            SELECT v.attempt_payload
            FROM external_effect_attempt_versions v
            WHERE v.tenant_id=$1 AND v.effect_attempt_id=$2
              AND v.aggregate_version=1
            """,
            observation.tenant_id,
            observation.compensation_attempt_id,
        )
        linked = (
            ExternalEffectAttempt.model_validate(_json(value))
            if value is not None
            else None
        )
        if (
            linked is None
            or linked.effect_attempt_id == head["effect_attempt_id"]
            or linked.compensates_effect_attempt_id != head["effect_attempt_id"]
            or linked.intervention_spec_digest
            != head["current_compensation_spec_digest"]
            or linked.authorization_decision_id
            != head["current_compensation_authorization_decision_id"]
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_NOT_SEPARATE",
                "compensation must be the exact separately authorized attempt",
            )

    async def _validate_compensation_outcome(
        self,
        *,
        conn,
        head,
        observation,
    ) -> None:
        linked = await conn.fetchrow(
            """
            SELECT current_version, current_state
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            """,
            observation.tenant_id,
            head["current_compensation_attempt_id"],
        )
        allowed = {
            ExternalEffectState.COMPENSATED: {ExternalEffectState.SUCCEEDED},
            ExternalEffectState.COMPENSATION_FAILED: {
                ExternalEffectState.FAILED,
                ExternalEffectState.REJECTED,
                ExternalEffectState.TERMINAL_PARTIAL,
                ExternalEffectState.COMPENSATION_FAILED,
            },
            ExternalEffectState.COMPENSATION_UNKNOWN: {
                ExternalEffectState.ACKNOWLEDGED,
                ExternalEffectState.UNKNOWN,
                ExternalEffectState.RECONCILING,
                ExternalEffectState.PARTIALLY_EXECUTED,
                ExternalEffectState.COMPENSATION_UNKNOWN,
                ExternalEffectState.COMPENSATION_RECONCILING,
            },
        }
        receipt_id = None
        if linked is not None:
            receipt_id = await conn.fetchval(
                """
                SELECT receipt_id FROM execution_receipts
                WHERE tenant_id=$1 AND effect_attempt_id=$2
                  AND effect_version=$3 AND effect_state=$4
                """,
                observation.tenant_id,
                head["current_compensation_attempt_id"],
                int(linked["current_version"]),
                linked["current_state"],
            )
        if (
            linked is None
            or linked["current_state"] not in allowed[observation.to_state]
            or receipt_id is None
            or f"execution-receipt:{receipt_id}"
            not in observation.external_state_evidence_refs
        ):
            raise InvariantViolation(
                "EFFECT_COMPENSATION_OUTCOME_UNBOUND",
                "compensation fate lacks the exact linked attempt receipt",
            )

    async def _prior(self, *, conn, command):
        return await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id="ExecutionLedgerApplier",
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )

    async def _capabilities(self, *, conn, attempt, at):
        value = await conn.fetchval(
            """
            SELECT capabilities FROM action_adapter_capability_versions
            WHERE tenant_id=$1 AND capability_id=$2
              AND capability_version=$3 AND capability_digest=$4
            """,
            attempt.tenant_id,
            attempt.capability_id,
            attempt.capability_version,
            attempt.capability_digest,
        )
        if value is None:
            raise InvariantViolation(
                "EFFECT_CAPABILITY_MISSING",
                "effect references unknown adapter capability version/digest",
            )
        capabilities = ActionAdapterCapabilities.model_validate(_json(value))
        if at >= capabilities.expires_at or attempt.operation not in (
            capabilities.permitted_operations
        ):
            raise InvariantViolation(
                "EFFECT_CAPABILITY_NOT_LIVE",
                "adapter capability is expired or excludes the operation",
            )
        return capabilities

    async def _capabilities_from_head(self, *, conn, head, at):
        attempt = SimpleNamespace(
            tenant_id=head["tenant_id"],
            capability_id=head["capability_id"],
            capability_version=head["capability_version"],
            capability_digest=head["capability_digest"],
            operation=head["operation"],
        )
        return await self._capabilities(conn=conn, attempt=attempt, at=at)

    async def _validate_task_work_and_lease(self, *, conn, attempt, at):
        task = await conn.fetchrow(
            """
            SELECT * FROM agency_task_heads
            WHERE tenant_id=$1 AND task_id=$2
            """,
            attempt.tenant_id,
            attempt.task_id,
        )
        if (
            task is None
            or task["episode_id"] != attempt.episode_id
            or task["intervention_spec_digest"] != attempt.intervention_spec_digest
            or task["current_state"] != TaskState.IN_PROGRESS
            or not task["external_effect_required"]
        ):
            raise InvariantViolation(
                "EFFECT_TASK_NOT_EXECUTABLE",
                "effect requires exact in-progress external-effect task",
            )
        await self._validate_live_effect_fence(
            conn=conn,
            head={
                "tenant_id": attempt.tenant_id,
                "work_obligation_id": attempt.work_obligation_id,
                "work_obligation_generation": attempt.work_obligation_generation,
                "lease_token_id": attempt.lease_token_id,
                "lease_fence": attempt.lease_fence,
            },
            at=at,
        )

    async def _validate_live_effect_fence(self, *, conn, head, at):
        work = await conn.fetchrow(
            """
            SELECT * FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2 FOR SHARE
            """,
            head["tenant_id"],
            head["work_obligation_id"],
        )
        lease = await conn.fetchrow(
            """
            SELECT * FROM work_lease_token_heads
            WHERE tenant_id=$1 AND lease_token_id=$2 FOR SHARE
            """,
            head["tenant_id"],
            head["lease_token_id"],
        )
        if (
            work is None
            or lease is None
            or int(work["generation"]) != int(head["work_obligation_generation"])
            or work["current_state"] != WorkObligationState.LEASED
            or work["current_lease_token_id"] != head["lease_token_id"]
            or int(work["current_fence"]) != int(head["lease_fence"])
            or lease["current_state"] != LeaseState.ACTIVE
            or int(lease["fence"]) != int(head["lease_fence"])
            or at >= lease["expires_at"]
        ):
            raise InvariantViolation(
                "EFFECT_STALE_LEASE_FENCE",
                "effect reservation/dispatch requires exact live work lease fence",
            )

    def _receipt(self, *, head, observation, version):
        state = observation.to_state
        accepted_true = {
            ExternalEffectState.ACKNOWLEDGED,
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.PARTIALLY_EXECUTED,
            ExternalEffectState.TERMINAL_PARTIAL,
            ExternalEffectState.COMPENSATION_PROPOSED,
            ExternalEffectState.COMPENSATION_AUTHORIZED,
            ExternalEffectState.COMPENSATION_ATTEMPT_LINKED,
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_UNKNOWN,
            ExternalEffectState.COMPENSATION_RECONCILING,
        }
        accepted_false = {
            ExternalEffectState.REJECTED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
        }
        provider_accepted = (
            True
            if state in accepted_true
            else False
            if state in accepted_false
            else None
        )
        return ExecutionReceipt(
            receipt_id=observation.receipt_id,
            tenant_id=observation.tenant_id,
            effect_attempt_id=observation.effect_attempt_id,
            effect_version=version,
            effect_state=state,
            canonical_request_hash=head["canonical_request_hash"],
            provider_idempotency_key=head["provider_idempotency_key"],
            provider_accepted=provider_accepted,
            externally_observed=bool(observation.external_state_evidence_refs),
            partial=state
            in {
                ExternalEffectState.PARTIALLY_EXECUTED,
                ExternalEffectState.TERMINAL_PARTIAL,
            },
            reconciled=observation.from_state
            in {
                ExternalEffectState.RECONCILING,
                ExternalEffectState.COMPENSATION_RECONCILING,
            }
            or state is ExternalEffectState.RECONCILED_NO_EFFECT,
            compensated=state is ExternalEffectState.COMPENSATED,
            provider_observation_refs=observation.provider_observation_refs,
            external_state_evidence_refs=observation.external_state_evidence_refs,
            observed_at=observation.observed_at,
        )


__all__ = ["AgencyStateApplier", "ExecutionLedgerApplier", "WorkLedgerApplier"]
