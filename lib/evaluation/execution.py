"""Independent evaluation of workflow, work, lease, and external-effect truth.

The evaluator reads committed state and replays the legal reducers.  It never
calls the runtime writers and never treats a successful command result as proof
that the state it describes is coherent.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.agency import (
    AuthorizationDecision,
    AuthorizationDisposition,
    InterventionSpec,
)
from lib.contracts.execution import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    EffectObservation,
    EffectReservationCommand,
    EffectTransitionCommand,
    ExecutionReceipt,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseGrantCommand,
    LeaseHeartbeat,
    LeaseHeartbeatCommand,
    LeaseResolution,
    LeaseResolutionCommand,
    LeaseState,
    LeaseTakeover,
    LeaseTakeoverCommand,
    LeaseToken,
    TaskCommand,
    TaskSnapshot,
    TaskState,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkDecision,
    WorkDecisionCommand,
    WorkObligation,
    WorkObligationRegistrationCommand,
    WorkObligationState,
    WorkStateTransition,
    WorkStateTransitionCommand,
    external_effect_transition_allowed,
    lease_transition_allowed,
    task_transition_allowed,
    work_obligation_transition_allowed,
    workflow_run_transition_allowed,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.failure import (
    FailureRecord,
    FailureRecordCommand,
    FailureState,
    OwnerTerminalizationRequest,
    OwnerTerminalizationRequestCommand,
    OwnerTerminalizationResolution,
    OwnerTerminalizationResolutionCommand,
    failure_transition_allowed,
)
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantRunEvidence,
    MetricObservation,
)


class _ExecutionEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ExecutionEvaluationScope(_ExecutionEvaluationModel):
    tenant_id: UUID
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @field_validator("start", "end")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def interval_is_forward(self) -> Self:
        if self.end <= self.start:
            raise ValueError("execution evaluation end must follow start")
        return self


class ExecutionEvaluationState(_ExecutionEvaluationModel):
    scope: ExecutionEvaluationScope
    workflow_run_count: int = Field(ge=0)
    workflow_state_counts: dict[str, int]
    legal_workflow_run_count: int = Field(ge=0)
    workflow_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    task_count: int = Field(ge=0)
    task_state_counts: dict[str, int]
    legal_task_count: int = Field(ge=0)
    task_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    completed_external_task_count: int = Field(ge=0)
    receipt_backed_external_task_count: int = Field(ge=0)
    external_task_receipt_rate: float | None = Field(default=None, ge=0, le=1)
    work_obligation_count: int = Field(ge=0)
    work_fate_counts: dict[str, int]
    legal_work_obligation_count: int = Field(ge=0)
    work_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    valid_work_lineage_count: int = Field(ge=0)
    work_lineage_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    work_redrive_generation_count: int = Field(ge=0)
    authorized_work_redrive_generation_count: int = Field(ge=0)
    work_redrive_authorization_rate: float | None = Field(default=None, ge=0, le=1)
    work_decision_count: int = Field(ge=0)
    envelope_conformant_decision_count: int = Field(ge=0)
    work_decision_envelope_rate: float | None = Field(default=None, ge=0, le=1)
    lease_count: int = Field(ge=0)
    lease_fate_counts: dict[str, int]
    valid_lease_count: int = Field(ge=0)
    lease_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    lease_heartbeat_count: int = Field(ge=0)
    missed_heartbeat_takeover_count: int = Field(ge=0)
    safe_missed_heartbeat_takeover_count: int = Field(ge=0)
    takeover_safety_rate: float | None = Field(default=None, ge=0, le=1)
    failure_record_count: int = Field(ge=0)
    failure_fate_counts: dict[str, int]
    legal_failure_record_count: int = Field(ge=0)
    failure_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    failure_redrive_generation_count: int = Field(ge=0)
    authorized_failure_redrive_generation_count: int = Field(ge=0)
    failure_redrive_authorization_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    closed_failure_redrive_generation_count: int = Field(ge=0)
    failure_redrive_closure_rate: float | None = Field(default=None, ge=0, le=1)
    owner_terminalization_request_count: int = Field(ge=0)
    valid_owner_terminalization_count: int = Field(ge=0)
    resolved_owner_terminalization_count: int = Field(ge=0)
    owner_terminalization_closure_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    owner_terminalization_writer_counts: dict[str, int] = Field(
        default_factory=dict
    )
    resolved_owner_terminalization_writer_counts: dict[str, int] = Field(
        default_factory=dict
    )
    owner_terminalization_writer_closure_rates: dict[str, float] = Field(
        default_factory=dict
    )
    effect_attempt_count: int = Field(ge=0)
    effect_fate_counts: dict[str, int]
    legal_effect_attempt_count: int = Field(ge=0)
    effect_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    exact_effect_continuity_count: int = Field(ge=0)
    effect_continuity_rate: float | None = Field(default=None, ge=0, le=1)
    retry_attempt_count: int = Field(ge=0)
    safe_retry_attempt_count: int = Field(ge=0)
    retry_safety_rate: float | None = Field(default=None, ge=0, le=1)
    compensation_episode_count: int = Field(ge=0)
    valid_compensation_episode_count: int = Field(ge=0)
    compensation_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    terminal_compensation_episode_count: int = Field(ge=0)
    closed_compensation_episode_count: int = Field(ge=0)
    compensation_closure_rate: float | None = Field(default=None, ge=0, le=1)
    receipt_required_transition_count: int = Field(ge=0)
    valid_execution_receipt_count: int = Field(ge=0)
    receipt_closure_rate: float | None = Field(default=None, ge=0, le=1)
    unresolved_effect_count: int = Field(ge=0)
    mean_effect_resolution_seconds: float | None = Field(default=None, ge=0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float | None = Field(default=None, ge=0, le=1)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float | None = Field(default=None, ge=0, le=1)
    command_event_coverage: float | None = Field(default=None, ge=0, le=1)
    command_outbox_coverage: float | None = Field(default=None, ge=0, le=1)
    incident_counts: dict[str, int]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


_IMMUTABLE_EXECUTION_TABLES = frozenset(
    {
        "action_adapter_capability_versions",
        "agency_workflow_run_versions",
        "agency_task_versions",
        "work_obligation_specs",
        "work_obligation_versions",
        "work_decisions",
        "work_lease_token_versions",
        "external_effect_provider_keys",
        "external_effect_attempt_versions",
        "execution_receipts",
        "failure_record_specs",
        "failure_record_versions",
        "owner_terminalization_requests",
        "owner_terminalization_resolutions",
    }
)

_KNOWN_RETRY_PREDECESSOR_STATES = frozenset(
    {
        ExternalEffectState.REJECTED,
        ExternalEffectState.FAILED,
        ExternalEffectState.RECONCILED_NO_EFFECT,
    }
)


async def evaluate_execution_state(
    conn: asyncpg.Connection,
    *,
    scope: ExecutionEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> ExecutionEvaluationState:
    """Read the complete histories of execution aggregates touched in scope."""

    workflows = await conn.fetch(
        """
        SELECT v.*, h.episode_id AS head_episode_id,
               h.intervention_spec_digest AS head_spec_digest,
               h.current_version AS head_version,
               h.current_state AS head_state,
               h.current_snapshot_digest AS head_snapshot_digest
        FROM agency_workflow_run_versions v
        JOIN agency_workflow_run_heads h
          ON h.tenant_id=v.tenant_id AND h.workflow_run_id=v.workflow_run_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.workflow_run_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    tasks = await conn.fetch(
        """
        SELECT v.*, h.workflow_run_id AS head_workflow_run_id,
               h.episode_id AS head_episode_id,
               h.intervention_spec_digest AS head_spec_digest,
               h.current_version AS head_version,
               h.current_state AS head_state,
               h.current_snapshot_digest AS head_snapshot_digest,
               h.external_effect_required AS head_external_effect_required,
               h.current_effect_attempt_id, h.current_execution_receipt_id
        FROM agency_task_versions v
        JOIN agency_task_heads h
          ON h.tenant_id=v.tenant_id AND h.task_id=v.task_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.task_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    works = await conn.fetch(
        """
        SELECT v.*, s.obligation, s.obligation_digest,
               s.lineage_id AS spec_lineage_id, s.generation AS spec_generation,
               s.parent_obligation_id, s.maximum_attempts, s.effect_possible,
               s.target_object_type, s.target_object_id,
               s.owner_writer_id, s.purpose,
               h.lineage_id AS head_lineage_id, h.generation AS head_generation,
               h.current_version AS head_version, h.current_state AS head_state,
               h.current_lease_token_id, h.current_fence, h.attempt_count,
               l.current_obligation_id AS lineage_current_obligation_id,
               l.current_generation AS lineage_current_generation,
               EXISTS (
                 SELECT 1 FROM agency_command_results rr
                 WHERE rr.tenant_id=v.tenant_id
                   AND rr.writer_id='RepairLedgerApplier'
                   AND rr.command_kind='apply_repair_receipt'
                   AND rr.object_type='repair_obligation'
                   AND rr.object_id=s.target_object_id
                   AND rr.result->>'repair_state' IN (
                     'repaired','no_op','adjudicated_residue','exhausted','escalated'
                   )
                   AND ('agency-command-result:' || rr.id::text) IN (
                     SELECT jsonb_array_elements_text(
                       COALESCE(v.transition_payload->'result_evidence_refs','[]'::jsonb)
                     )
                   )
               ) AS exact_repair_owner_result
        FROM work_obligation_versions v
        JOIN work_obligation_heads h
          ON h.tenant_id=v.tenant_id AND h.obligation_id=v.obligation_id
        JOIN work_obligation_specs s
          ON s.tenant_id=h.tenant_id AND s.obligation_id=h.obligation_id
        JOIN work_obligation_lineage_heads l
          ON l.tenant_id=h.tenant_id AND l.lineage_id=h.lineage_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.obligation_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    decisions = await conn.fetch(
        """
        SELECT d.*, s.obligation
        FROM work_decisions d
        JOIN work_obligation_specs s
          ON s.tenant_id=d.tenant_id AND s.obligation_id=d.obligation_id
        WHERE d.tenant_id=$1 AND d.decided_at >= $2 AND d.decided_at < $3
        ORDER BY d.decided_at, d.decision_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    leases = await conn.fetch(
        """
        SELECT v.*, h.obligation_id AS head_obligation_id,
               h.obligation_generation AS head_obligation_generation,
               h.current_version AS head_version, h.current_state AS head_state,
               h.fence AS head_fence, h.attempt AS head_attempt,
               h.owner_ref AS head_owner_ref,
               h.heartbeat_deadline AS head_heartbeat_deadline,
               h.expires_at AS head_expires_at,
               h.effect_possible AS head_effect_possible,
               s.maximum_attempts, s.deadline AS work_deadline,
               s.effect_possible AS work_effect_possible
        FROM work_lease_token_versions v
        JOIN work_lease_token_heads h
          ON h.tenant_id=v.tenant_id AND h.lease_token_id=v.lease_token_id
        JOIN work_obligation_specs s
          ON s.tenant_id=h.tenant_id AND s.obligation_id=h.obligation_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.lease_token_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    effects = await conn.fetch(
        """
        SELECT v.*, h.lineage_id AS head_lineage_id,
               h.generation AS head_generation, h.prior_attempt_id,
               h.episode_id AS head_episode_id, h.task_id AS head_task_id,
               h.intervention_spec_digest AS head_spec_digest,
               h.authorization_decision_id AS head_authorization_id,
               h.capability_id AS head_capability_id,
               h.capability_version AS head_capability_version,
               h.capability_digest AS head_capability_digest,
               h.operation AS head_operation,
               h.canonical_request_hash AS head_request_hash,
               h.provider_idempotency_key AS head_provider_key,
               h.work_obligation_id AS head_work_obligation_id,
               h.work_obligation_generation AS head_work_generation,
               h.lease_token_id AS head_lease_token_id,
               h.lease_fence AS head_lease_fence,
               h.current_version AS head_version, h.current_state AS head_state,
               h.current_attempt_digest AS head_attempt_digest,
               h.reserved_at AS head_reserved_at,
               h.current_compensation_spec_digest,
               h.current_compensation_authorization_decision_id,
               h.current_compensation_attempt_id,
               el.current_effect_attempt_id AS lineage_current_attempt_id,
               el.current_generation AS lineage_current_generation,
               s.spec, s.spec_digest AS stored_spec_digest,
               a.decision AS authorization_decision,
               c.capabilities, c.capability_digest AS stored_capability_digest,
               cs.spec AS compensation_spec,
               cp.current_fate AS compensation_proposal_fate,
               cpr.command_result_id AS compensation_proposal_fate_command_result_id,
               ca.decision AS compensation_authorization_decision,
               t.workflow_run_id AS task_workflow_run_id,
               t.episode_id AS task_episode_id,
               t.intervention_spec_digest AS task_spec_digest,
               t.external_effect_required AS task_effect_required,
               w.generation AS work_generation,
               w.current_state AS work_state,
               w.current_fence AS work_fence,
               l.obligation_id AS lease_obligation_id,
               l.obligation_generation AS lease_work_generation,
               l.fence AS lease_fence, l.attempt AS lease_attempt,
               pk.lineage_id AS provider_key_lineage_id,
               pk.canonical_request_hash AS provider_key_request_hash
        FROM external_effect_attempt_versions v
        JOIN external_effect_attempt_heads h
          ON h.tenant_id=v.tenant_id AND h.effect_attempt_id=v.effect_attempt_id
        JOIN external_effect_attempt_lineage_heads el
          ON el.tenant_id=h.tenant_id AND el.lineage_id=h.lineage_id
        LEFT JOIN consequential_intervention_specs s
          ON s.tenant_id=h.tenant_id AND s.spec_digest=h.intervention_spec_digest
        LEFT JOIN consequential_authorization_decisions a
          ON a.tenant_id=h.tenant_id AND a.id=h.authorization_decision_id
        LEFT JOIN action_adapter_capability_versions c
          ON c.tenant_id=h.tenant_id AND c.capability_id=h.capability_id
         AND c.capability_version=h.capability_version
         AND c.capability_digest=h.capability_digest
        LEFT JOIN consequential_intervention_specs cs
          ON cs.tenant_id=h.tenant_id
         AND cs.spec_digest=h.current_compensation_spec_digest
        LEFT JOIN consequential_proposals cp
          ON cp.tenant_id=cs.tenant_id
         AND cp.id=cs.registered_by_proposal_id
         AND cp.proposal_version=cs.registered_by_proposal_version
        LEFT JOIN consequential_proposal_reviews cpr
          ON cpr.tenant_id=cp.tenant_id
         AND cpr.proposal_id=cp.id
         AND cpr.proposal_version=cp.proposal_version
         AND cpr.to_fate_version=cp.current_fate_version
        LEFT JOIN consequential_authorization_decisions ca
          ON ca.tenant_id=h.tenant_id
         AND ca.id=h.current_compensation_authorization_decision_id
        LEFT JOIN agency_task_heads t
          ON t.tenant_id=h.tenant_id AND t.task_id=h.task_id
        LEFT JOIN work_obligation_heads w
          ON w.tenant_id=h.tenant_id AND w.obligation_id=h.work_obligation_id
        LEFT JOIN work_lease_token_heads l
          ON l.tenant_id=h.tenant_id AND l.lease_token_id=h.lease_token_id
        LEFT JOIN external_effect_provider_keys pk
          ON pk.tenant_id=h.tenant_id AND pk.capability_id=h.capability_id
         AND pk.provider_idempotency_key=h.provider_idempotency_key
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.effect_attempt_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    receipts = await conn.fetch(
        """
        SELECT r.*
        FROM execution_receipts r
        JOIN external_effect_attempt_heads h
          ON h.tenant_id=r.tenant_id AND h.effect_attempt_id=r.effect_attempt_id
        WHERE r.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY r.effect_attempt_id, r.effect_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    failures = await conn.fetch(
        """
        SELECT v.*, h.lineage_id AS head_lineage_id,
               h.generation AS head_generation,
               h.work_obligation_id AS head_work_obligation_id,
               h.work_obligation_generation AS head_work_generation,
               h.current_version AS head_version,
               h.current_state AS head_state,
               h.current_record_digest AS head_record_digest,
               h.current_owner_terminalization_request_id,
               l.current_failure_id AS lineage_current_failure_id,
               l.current_generation AS lineage_current_generation
        FROM failure_record_versions v
        JOIN failure_record_heads h
          ON h.tenant_id=v.tenant_id AND h.failure_id=v.failure_id
        JOIN failure_record_lineage_heads l
          ON l.tenant_id=h.tenant_id AND l.lineage_id=h.lineage_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.failure_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    owner_terminalizations = await conn.fetch(
        """
        SELECT q.*,
               z.resolution_id, z.resolution_digest, z.resolution,
               z.owner_command_result_id, z.resolved_at,
               r.writer_id AS owner_result_writer_id,
               r.object_type AS owner_result_object_type,
               r.object_id AS owner_result_object_id,
               r.object_version AS owner_result_object_version,
               r.result AS owner_result,
               f.current_state AS failure_current_state,
               f.current_owner_terminalization_request_id,
               w.current_state AS work_current_state
        FROM owner_terminalization_requests q
        LEFT JOIN owner_terminalization_resolutions z
          ON z.tenant_id=q.tenant_id AND z.request_id=q.request_id
        LEFT JOIN agency_command_results r
          ON r.tenant_id=z.tenant_id AND r.id=z.owner_command_result_id
        JOIN failure_record_heads f
          ON f.tenant_id=q.tenant_id AND f.failure_id=q.failure_id
        JOIN work_obligation_heads w
          ON w.tenant_id=q.tenant_id AND w.obligation_id=q.work_obligation_id
        WHERE q.tenant_id=$1 AND q.requested_at >= $2 AND q.requested_at < $3
        ORDER BY q.requested_at, q.request_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    commands = await conn.fetch(
        """
        WITH component_results AS (
          SELECT command_result_id FROM agency_workflow_run_versions v
            JOIN agency_workflow_run_heads h USING (tenant_id, workflow_run_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT command_result_id FROM agency_task_versions v
            JOIN agency_task_heads h USING (tenant_id, task_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT command_result_id FROM work_obligation_versions v
            JOIN work_obligation_heads h USING (tenant_id, obligation_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT command_result_id FROM work_lease_token_versions v
            JOIN work_lease_token_heads h USING (tenant_id, lease_token_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT command_result_id FROM external_effect_attempt_versions v
            JOIN external_effect_attempt_heads h USING (tenant_id, effect_attempt_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT v.command_result_id FROM failure_record_versions v
            JOIN failure_record_heads h USING (tenant_id, failure_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION SELECT v.command_result_id
            FROM action_adapter_capability_versions v
            JOIN action_adapter_capability_heads h
              USING (tenant_id, capability_id)
            WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        )
        SELECT r.*,
               (SELECT count(*) FROM agency_canonical_events e
                 WHERE e.command_result_id=r.id) AS event_count,
               (SELECT count(*) FROM agency_canonical_events e
                 JOIN agency_outbox_records o ON o.event_id=e.id
                 WHERE e.command_result_id=r.id) AS outbox_count
        FROM agency_command_results r
        JOIN component_results cr ON cr.command_result_id=r.id
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    guarded_tables = await conn.fetch(
        """
        SELECT DISTINCT c.relname AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND NOT t.tgisinternal
          AND t.tgname LIKE 'reject_%_mutation'
        """
    )
    return analyze_execution_rows(
        scope=scope,
        workflows=workflows,
        tasks=tasks,
        works=works,
        decisions=decisions,
        leases=leases,
        effects=effects,
        receipts=receipts,
        failures=failures,
        owner_terminalizations=owner_terminalizations,
        commands=commands,
        guarded_tables={row["table_name"] for row in guarded_tables},
        artifact_refs=artifact_refs,
    )


def analyze_execution_rows(
    *,
    scope: ExecutionEvaluationScope,
    workflows: Sequence[Mapping[str, Any]],
    tasks: Sequence[Mapping[str, Any]],
    works: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    leases: Sequence[Mapping[str, Any]],
    effects: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    failures: Sequence[Mapping[str, Any]],
    owner_terminalizations: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    guarded_tables: set[str],
    artifact_refs: tuple[str, ...],
) -> ExecutionEvaluationState:
    incidents: Counter[str] = Counter()
    workflow_groups = _group(workflows, "workflow_run_id")
    task_groups = _group(tasks, "task_id")
    work_groups = _group(works, "obligation_id")
    lease_groups = _group(leases, "lease_token_id")
    effect_groups = _group(effects, "effect_attempt_id")
    failure_groups = _group(failures, "failure_id")

    valid_workflows = sum(
        _valid_workflow_history(rows, incidents) for rows in workflow_groups.values()
    )

    receipt_models: dict[tuple[UUID, int], ExecutionReceipt] = {}
    valid_receipt_rows: set[tuple[UUID, int]] = set()
    for row in receipts:
        key = (row["effect_attempt_id"], int(row["effect_version"]))
        try:
            receipt = ExecutionReceipt.model_validate(_json(row["receipt"]))
            valid = (
                receipt.receipt_id == row["receipt_id"]
                and receipt.effect_attempt_id == key[0]
                and receipt.effect_version == key[1]
                and receipt.effect_state == row["effect_state"]
                and receipt.receipt_digest == row["receipt_digest"]
            )
        except (KeyError, TypeError, ValueError):
            valid = False
            receipt = None
        if valid and receipt is not None and key not in receipt_models:
            receipt_models[key] = receipt
            valid_receipt_rows.add(key)
        else:
            incidents["invalid_execution_receipt"] += 1

    effect_attempt_models: dict[UUID, ExternalEffectAttempt] = {}
    effect_terminal_states: dict[UUID, ExternalEffectState] = {}
    effect_current_versions: dict[UUID, int] = {}
    valid_effect_histories = 0
    exact_effect_continuity = 0
    retry_attempts: list[ExternalEffectAttempt] = []
    receipt_required = 0
    valid_receipts = 0
    effect_resolution_seconds: list[float] = []
    for effect_id, rows in effect_groups.items():
        history_valid, attempt, required, receipt_valid_count = _valid_effect_history(
            rows=rows,
            receipt_models=receipt_models,
            incidents=incidents,
        )
        valid_effect_histories += int(history_valid)
        receipt_required += required
        valid_receipts += receipt_valid_count
        if attempt is not None:
            effect_attempt_models[effect_id] = attempt
            current_state = ExternalEffectState(str(rows[-1]["head_state"]))
            effect_terminal_states[effect_id] = current_state
            effect_current_versions[effect_id] = int(rows[-1]["head_version"])
            continuity = _effect_continuity_valid(rows[0], attempt, incidents)
            exact_effect_continuity += int(continuity)
            if attempt.generation > 1:
                retry_attempts.append(attempt)
            if current_state.terminal:
                last_observed = _last_observed_at(rows)
                if last_observed is not None:
                    effect_resolution_seconds.append(
                        max(0.0, (last_observed - attempt.reserved_at).total_seconds())
                    )

    retry_count = len(retry_attempts)
    safe_retries = sum(
        effect_terminal_states.get(attempt.prior_attempt_id)
        in _KNOWN_RETRY_PREDECESSOR_STATES
        for attempt in retry_attempts
    )
    incidents["unsafe_effect_retry"] += retry_count - safe_retries
    if not incidents["unsafe_effect_retry"]:
        del incidents["unsafe_effect_retry"]

    compensation_results = tuple(
        _valid_compensation_episode(
            rows=rows,
            effect_attempts=effect_attempt_models,
            effect_terminal_states=effect_terminal_states,
            effect_current_versions=effect_current_versions,
            receipt_models=receipt_models,
            incidents=incidents,
        )
        for rows in effect_groups.values()
        if any(
            str(row["state"]) == ExternalEffectState.COMPENSATION_PROPOSED
            for row in rows
        )
    )
    valid_compensation_episodes = sum(result[0] for result in compensation_results)
    terminal_compensation_episodes = sum(result[1] for result in compensation_results)
    closed_compensation_episodes = sum(result[2] for result in compensation_results)

    valid_tasks = 0
    completed_external_tasks = 0
    receipt_backed_external_tasks = 0
    for rows in task_groups.values():
        valid, completed_external, receipt_backed = _valid_task_history(
            rows,
            receipt_models=receipt_models,
            effect_attempts=effect_attempt_models,
            incidents=incidents,
        )
        valid_tasks += int(valid)
        completed_external_tasks += int(completed_external)
        receipt_backed_external_tasks += int(receipt_backed)

    valid_work = sum(
        _valid_work_history(rows, incidents) for rows in work_groups.values()
    )
    valid_lineages = sum(
        _valid_work_lineage(rows[0], incidents) for rows in work_groups.values()
    )
    work_models: dict[UUID, WorkObligation] = {}
    for obligation_id, rows in work_groups.items():
        try:
            work_models[obligation_id] = WorkObligation.model_validate(
                _json(rows[0]["obligation"])
            )
        except (KeyError, TypeError, ValueError):
            continue
    redrive_work = tuple(
        work for work in work_models.values() if work.generation > 1
    )
    authorized_work_redrives = sum(
        _valid_work_redrive_generation(
            child=work,
            work_models=work_models,
            work_groups=work_groups,
            incidents=incidents,
        )
        for work in redrive_work
    )
    conformant_decisions = sum(
        _valid_work_decision(row, incidents) for row in decisions
    )
    valid_leases = sum(
        _valid_lease_history(rows, incidents) for rows in lease_groups.values()
    )
    lease_models: dict[UUID, LeaseToken] = {}
    heartbeat_count = 0
    takeovers: list[tuple[LeaseTakeover, datetime]] = []
    for lease_id, rows in lease_groups.items():
        try:
            lease_models[lease_id] = LeaseToken.model_validate(
                _json(rows[0]["lease_payload"])
            )
        except (KeyError, TypeError, ValueError):
            continue
        for row in rows[1:]:
            state = str(row.get("state"))
            if state == LeaseState.ACTIVE:
                heartbeat_count += 1
            elif state == LeaseState.SUPERSEDED_BY_NEW_LEASE:
                try:
                    takeovers.append(
                        (
                            LeaseTakeover.model_validate(_json(row["lease_payload"])),
                            row["transaction_at"],
                        )
                    )
                except (KeyError, TypeError, ValueError):
                    incidents["unsafe_missed_heartbeat_takeover"] += 1
    safe_takeovers = sum(
        _valid_missed_heartbeat_takeover(
            takeover=takeover,
            takeover_transaction_at=takeover_transaction_at,
            lease_models=lease_models,
            effect_attempts=effect_attempt_models,
            effect_histories=effect_groups,
            receipt_models=receipt_models,
            incidents=incidents,
        )
        for takeover, takeover_transaction_at in takeovers
    )
    valid_failures = sum(
        _valid_failure_history(rows, incidents) for rows in failure_groups.values()
    )
    failure_initial_models: dict[UUID, FailureRecord] = {}
    failure_current_models: dict[UUID, FailureRecord] = {}
    for failure_id, rows in failure_groups.items():
        try:
            failure_initial_models[failure_id] = FailureRecord.model_validate(
                _json(rows[0]["record"])
            )
            failure_current_models[failure_id] = FailureRecord.model_validate(
                _json(rows[-1]["record"])
            )
        except (KeyError, TypeError, ValueError):
            continue
    failure_redrives = tuple(
        failure
        for failure in failure_initial_models.values()
        if failure.generation > 1
    )
    failure_redrive_results = tuple(
        _valid_failure_redrive_generation(
            child=child,
            failure_initial_models=failure_initial_models,
            failure_current_models=failure_current_models,
            failure_groups=failure_groups,
            work_models=work_models,
            work_groups=work_groups,
            incidents=incidents,
        )
        for child in failure_redrives
    )
    authorized_failure_redrives = sum(result[0] for result in failure_redrive_results)
    closed_failure_redrives = sum(result[1] for result in failure_redrive_results)
    owner_terminalization_validity = tuple(
        _valid_owner_terminalization(row, incidents)
        for row in owner_terminalizations
    )
    valid_owner_terminalizations = sum(owner_terminalization_validity)
    resolved_owner_terminalizations = sum(
        int(valid and row.get("resolution_id") is not None)
        for valid, row in zip(
            owner_terminalization_validity,
            owner_terminalizations,
            strict=True,
        )
    )
    owner_terminalization_writer_counts = Counter(
        str(row["semantic_owner_writer_id"]) for row in owner_terminalizations
    )
    resolved_owner_terminalization_writer_counts = Counter(
        str(row["semantic_owner_writer_id"])
        for valid, row in zip(
            owner_terminalization_validity,
            owner_terminalizations,
            strict=True,
        )
        if valid and row.get("resolution_id") is not None
    )
    owner_terminalization_writer_closure_rates = {
        writer_id: (
            resolved_owner_terminalization_writer_counts[writer_id] / count
        )
        for writer_id, count in owner_terminalization_writer_counts.items()
    }

    reconstructable = 0
    event_covered = 0
    outbox_covered = 0
    for row in commands:
        reconstructable += int(_command_reconstructable(row))
        if not _command_reconstructable(row):
            incidents["unreconstructable_execution_command"] += 1
        event_count = int(row.get("event_count") or 0)
        outbox_count = int(row.get("outbox_count") or 0)
        event_covered += int(event_count == 1)
        outbox_covered += int(outbox_count == 1)
        if event_count != 1:
            incidents["execution_command_without_exact_event"] += 1
        if outbox_count != 1:
            incidents["execution_command_without_exact_outbox"] += 1

    guarded = len(_IMMUTABLE_EXECUTION_TABLES & guarded_tables)
    missing_guards = len(_IMMUTABLE_EXECUTION_TABLES - guarded_tables)
    if missing_guards:
        incidents["immutable_execution_table_unguarded"] += missing_guards

    workflow_states = Counter(
        str(rows[-1]["head_state"]) for rows in workflow_groups.values()
    )
    task_states = Counter(str(rows[-1]["head_state"]) for rows in task_groups.values())
    work_states = Counter(str(rows[-1]["head_state"]) for rows in work_groups.values())
    lease_states = Counter(str(rows[-1]["head_state"]) for rows in lease_groups.values())
    failure_states = Counter(
        str(rows[-1]["head_state"]) for rows in failure_groups.values()
    )
    effect_states = Counter(str(rows[-1]["head_state"]) for rows in effect_groups.values())
    unresolved = sum(
        count
        for state, count in effect_states.items()
        if not ExternalEffectState(state).terminal
    )

    return ExecutionEvaluationState(
        scope=scope,
        workflow_run_count=len(workflow_groups),
        workflow_state_counts=dict(workflow_states),
        legal_workflow_run_count=valid_workflows,
        workflow_history_integrity_rate=_ratio(valid_workflows, len(workflow_groups)),
        task_count=len(task_groups),
        task_state_counts=dict(task_states),
        legal_task_count=valid_tasks,
        task_history_integrity_rate=_ratio(valid_tasks, len(task_groups)),
        completed_external_task_count=completed_external_tasks,
        receipt_backed_external_task_count=receipt_backed_external_tasks,
        external_task_receipt_rate=_ratio(
            receipt_backed_external_tasks, completed_external_tasks
        ),
        work_obligation_count=len(work_groups),
        work_fate_counts=dict(work_states),
        legal_work_obligation_count=valid_work,
        work_history_integrity_rate=_ratio(valid_work, len(work_groups)),
        valid_work_lineage_count=valid_lineages,
        work_lineage_integrity_rate=_ratio(valid_lineages, len(work_groups)),
        work_redrive_generation_count=len(redrive_work),
        authorized_work_redrive_generation_count=authorized_work_redrives,
        work_redrive_authorization_rate=_ratio(
            authorized_work_redrives, len(redrive_work)
        ),
        work_decision_count=len(decisions),
        envelope_conformant_decision_count=conformant_decisions,
        work_decision_envelope_rate=_ratio(conformant_decisions, len(decisions)),
        lease_count=len(lease_groups),
        lease_fate_counts=dict(lease_states),
        valid_lease_count=valid_leases,
        lease_integrity_rate=_ratio(valid_leases, len(lease_groups)),
        lease_heartbeat_count=heartbeat_count,
        missed_heartbeat_takeover_count=len(takeovers),
        safe_missed_heartbeat_takeover_count=safe_takeovers,
        takeover_safety_rate=_ratio(safe_takeovers, len(takeovers)),
        failure_record_count=len(failure_groups),
        failure_fate_counts=dict(failure_states),
        legal_failure_record_count=valid_failures,
        failure_history_integrity_rate=_ratio(valid_failures, len(failure_groups)),
        failure_redrive_generation_count=len(failure_redrives),
        authorized_failure_redrive_generation_count=authorized_failure_redrives,
        failure_redrive_authorization_rate=_ratio(
            authorized_failure_redrives, len(failure_redrives)
        ),
        closed_failure_redrive_generation_count=closed_failure_redrives,
        failure_redrive_closure_rate=_ratio(
            closed_failure_redrives, len(failure_redrives)
        ),
        owner_terminalization_request_count=len(owner_terminalizations),
        valid_owner_terminalization_count=valid_owner_terminalizations,
        resolved_owner_terminalization_count=resolved_owner_terminalizations,
        owner_terminalization_closure_rate=_ratio(
            resolved_owner_terminalizations, len(owner_terminalizations)
        ),
        owner_terminalization_writer_counts=dict(
            owner_terminalization_writer_counts
        ),
        resolved_owner_terminalization_writer_counts=dict(
            resolved_owner_terminalization_writer_counts
        ),
        owner_terminalization_writer_closure_rates=(
            owner_terminalization_writer_closure_rates
        ),
        effect_attempt_count=len(effect_groups),
        effect_fate_counts=dict(effect_states),
        legal_effect_attempt_count=valid_effect_histories,
        effect_history_integrity_rate=_ratio(
            valid_effect_histories, len(effect_groups)
        ),
        exact_effect_continuity_count=exact_effect_continuity,
        effect_continuity_rate=_ratio(exact_effect_continuity, len(effect_groups)),
        retry_attempt_count=retry_count,
        safe_retry_attempt_count=safe_retries,
        retry_safety_rate=_ratio(safe_retries, retry_count),
        compensation_episode_count=len(compensation_results),
        valid_compensation_episode_count=valid_compensation_episodes,
        compensation_integrity_rate=_ratio(
            valid_compensation_episodes, len(compensation_results)
        ),
        terminal_compensation_episode_count=terminal_compensation_episodes,
        closed_compensation_episode_count=closed_compensation_episodes,
        compensation_closure_rate=_ratio(
            closed_compensation_episodes, terminal_compensation_episodes
        ),
        receipt_required_transition_count=receipt_required,
        valid_execution_receipt_count=valid_receipts,
        receipt_closure_rate=_ratio(valid_receipts, receipt_required),
        unresolved_effect_count=unresolved,
        mean_effect_resolution_seconds=(
            mean(effect_resolution_seconds) if effect_resolution_seconds else None
        ),
        immutable_table_count=len(_IMMUTABLE_EXECUTION_TABLES),
        guarded_immutable_table_count=guarded,
        immutable_storage_guard_rate=_ratio(
            guarded, len(_IMMUTABLE_EXECUTION_TABLES)
        ),
        command_count=len(commands),
        reconstructable_command_count=reconstructable,
        command_reconstructability_rate=_ratio(reconstructable, len(commands)),
        command_event_coverage=_ratio(event_covered, len(commands)),
        command_outbox_coverage=_ratio(outbox_covered, len(commands)),
        incident_counts=dict(sorted(incidents.items())),
        uncertainty=(
            "This E3 evaluator proves committed component mechanics, not a real provider call or provider-contract truth.",
            "Provider observations are source-linked but their external validity requires an independent simulator oracle or live-provider audit.",
            "Exact owner-terminalization handshakes are exercised for AgencyStateApplier Task and ProposalAppender consequential-proposal fates, with per-writer closure rates; pure-computation and effect-capable missed-heartbeat takeovers with no-attempt, reserved-version, and terminal-receipt evidence, one atomically coordinated Work/Failure redrive, one known-no-effect external-effect retry, separately proposed and authorized compensation episodes resolved through unknown/reconciling to success, failure, or terminal partial state, exact rejected/expired proposal fates, and rejection of non-reversible nested compensation are also exercised. Process-crash/reorder recovery, semantic-owner types beyond those two, and positively authorized nested compensation remain unproven.",
            "RepairLedger closure and live legacy-producer/consumer cutover are outside this component population.",
            "No causal, customer-outcome, economic-operability, or long-horizon calibration claim follows from this execution proof.",
        ),
        artifact_refs=artifact_refs,
    )


def _valid_workflow_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> bool:
    valid = True
    prior: WorkflowRunSnapshot | None = None
    for position, row in enumerate(rows, start=1):
        try:
            snapshot = WorkflowRunSnapshot.model_validate(_json(row["snapshot"]))
            row_valid = (
                int(row["aggregate_version"]) == position
                and snapshot.workflow_run_id == row["workflow_run_id"]
                and snapshot.state == row["state"]
                and snapshot.snapshot_digest == row["snapshot_digest"]
                and workflow_run_transition_allowed(
                    prior.state if prior else None, snapshot.state
                )
            )
            if prior is not None:
                row_valid = row_valid and _same_fields(
                    prior,
                    snapshot,
                    (
                        "workflow_run_id",
                        "tenant_id",
                        "episode_id",
                        "intervention_spec_digest",
                        "workflow_spec_version_ref",
                        "authorization_decision_id",
                        "authorization_decision_version",
                        "created_at",
                    ),
                )
        except (KeyError, TypeError, ValueError):
            row_valid = False
            snapshot = None
        valid = valid and row_valid
        if snapshot is not None:
            prior = snapshot
    if prior is None or not _head_snapshot_matches(rows[-1], prior):
        valid = False
    if not valid:
        incidents["invalid_workflow_history"] += 1
    return valid


def _valid_task_history(
    rows: Sequence[Mapping[str, Any]],
    *,
    receipt_models: Mapping[tuple[UUID, int], ExecutionReceipt],
    effect_attempts: Mapping[UUID, ExternalEffectAttempt],
    incidents: Counter[str],
) -> tuple[bool, bool, bool]:
    valid = True
    prior: TaskSnapshot | None = None
    completed_external = False
    receipt_backed = False
    for position, row in enumerate(rows, start=1):
        try:
            snapshot = TaskSnapshot.model_validate(_json(row["snapshot"]))
            row_valid = (
                int(row["aggregate_version"]) == position
                and snapshot.task_id == row["task_id"]
                and snapshot.state == row["state"]
                and snapshot.snapshot_digest == row["snapshot_digest"]
                and task_transition_allowed(prior.state if prior else None, snapshot.state)
            )
            if prior is not None:
                row_valid = row_valid and _same_fields(
                    prior,
                    snapshot,
                    (
                        "task_id",
                        "tenant_id",
                        "workflow_run_id",
                        "episode_id",
                        "intervention_spec_digest",
                        "task_kind",
                        "authorization_decision_id",
                        "authorization_decision_version",
                        "external_effect_required",
                        "created_at",
                    ),
                )
            if snapshot.state is TaskState.COMPLETED and snapshot.external_effect_required:
                completed_external = True
                effect = effect_attempts.get(snapshot.effect_attempt_id)
                receipt = next(
                    (
                        item
                        for (attempt_id, _), item in receipt_models.items()
                        if item.receipt_id == snapshot.execution_receipt_id
                        and attempt_id == snapshot.effect_attempt_id
                    ),
                    None,
                )
                receipt_backed = bool(
                    receipt
                    and effect
                    and receipt.effect_state is ExternalEffectState.SUCCEEDED
                    and effect.task_id == snapshot.task_id
                    and effect.intervention_spec_digest
                    == snapshot.intervention_spec_digest
                )
                if not receipt_backed:
                    incidents["external_task_without_succeeded_receipt"] += 1
                    row_valid = False
        except (KeyError, TypeError, ValueError):
            row_valid = False
            snapshot = None
        valid = valid and row_valid
        if snapshot is not None:
            prior = snapshot
    if prior is None or not _head_snapshot_matches(rows[-1], prior):
        valid = False
    if not valid:
        incidents["invalid_task_history"] += 1
    return valid, completed_external, receipt_backed


def _valid_work_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> bool:
    valid = True
    current: WorkObligationState | None = None
    work: WorkObligation | None = None
    for position, row in enumerate(rows, start=1):
        target: WorkObligationState | None = None
        kind = str(row["transition_kind"])
        try:
            payload = _json(row["transition_payload"])
            target = WorkObligationState(str(row["state"]))
            row_valid = int(row["aggregate_version"]) == position
            if kind == "register":
                work = WorkObligation.model_validate(payload)
                row_valid = row_valid and position == 1 and target is (
                    WorkObligationState.REGISTERED
                )
            elif kind == "decision":
                decision = WorkDecision.model_validate(payload)
                row_valid = row_valid and current == decision.from_state
                row_valid = row_valid and target == decision.to_state
                row_valid = row_valid and work_obligation_transition_allowed(
                    decision.from_state, decision.to_state
                )
            elif kind == "state_transition":
                transition = WorkStateTransition.model_validate(payload)
                row_valid = row_valid and current == transition.from_state
                row_valid = row_valid and target == transition.to_state
                row_valid = row_valid and work_obligation_transition_allowed(
                    transition.from_state, transition.to_state
                )
            elif kind == "lease_granted":
                lease = LeaseToken.model_validate(payload)
                row_valid = row_valid and current is WorkObligationState.ELIGIBLE
                row_valid = row_valid and target is WorkObligationState.LEASED
                row_valid = row_valid and work is not None
                if work is not None:
                    row_valid = row_valid and (
                        lease.obligation_id == work.obligation_id
                        and lease.obligation_generation == work.generation
                        and lease.attempt <= work.maximum_attempts
                        and lease.expires_at <= work.deadline
                        and lease.effect_possible == work.effect_possible
                    )
            elif kind == "lease_resolved":
                resolution = LeaseResolution.model_validate(payload)
                row_valid = row_valid and current is WorkObligationState.LEASED
                row_valid = row_valid and target == resolution.to_work_state
                row_valid = row_valid and work_obligation_transition_allowed(
                    WorkObligationState.LEASED, target
                )
                if (
                    work is not None
                    and work.target_object_type == "repair_obligation"
                    and target is WorkObligationState.COMPLETED
                ):
                    owner_result_valid = bool(row["exact_repair_owner_result"])
                    row_valid = row_valid and owner_result_valid
                    if not owner_result_valid:
                        incidents[
                            "repair_child_without_exact_owner_result"
                        ] += 1
            elif kind == "lease_taken_over":
                takeover = LeaseTakeover.model_validate(payload)
                row_valid = row_valid and current is WorkObligationState.LEASED
                row_valid = row_valid and target is WorkObligationState.LEASED
                row_valid = row_valid and work is not None
                if work is not None:
                    row_valid = row_valid and (
                        takeover.obligation_id == work.obligation_id
                        and takeover.obligation_generation == work.generation
                        and takeover.successor.attempt <= work.maximum_attempts
                        and takeover.successor.expires_at <= work.deadline
                        and takeover.successor.effect_possible == work.effect_possible
                    )
            elif kind == "successor_registered":
                row_valid = row_valid and current is WorkObligationState.REDRIVE_AUTHORIZED
                row_valid = row_valid and target is (
                    WorkObligationState.SUPERSEDED_BY_NEW_GENERATION
                )
            elif kind == "owner_terminalization_requested":
                request = OwnerTerminalizationRequest.model_validate(payload)
                row_valid = row_valid and current == request.from_work_state
                row_valid = row_valid and target is (
                    WorkObligationState.OWNER_TERMINALIZATION_PENDING
                )
                row_valid = row_valid and work_obligation_transition_allowed(
                    request.from_work_state, target
                )
            elif kind == "owner_terminalization_resolved":
                resolution = OwnerTerminalizationResolution.model_validate(payload)
                row_valid = row_valid and current is (
                    WorkObligationState.OWNER_TERMINALIZATION_PENDING
                )
                row_valid = row_valid and target == resolution.to_work_state
                row_valid = row_valid and work_obligation_transition_allowed(
                    WorkObligationState.OWNER_TERMINALIZATION_PENDING,
                    target,
                )
            else:
                row_valid = False
        except (KeyError, TypeError, ValueError):
            row_valid = False
        valid = valid and row_valid
        if target is not None:
            current = target
    first = rows[0]
    last = rows[-1]
    try:
        stored = WorkObligation.model_validate(_json(first["obligation"]))
        valid = valid and stored.obligation_digest == first["obligation_digest"]
        valid = valid and int(last["head_version"]) == int(last["aggregate_version"])
        valid = valid and str(last["head_state"]) == str(last["state"])
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_work_obligation_history"] += 1
    return valid


def _valid_work_lineage(row: Mapping[str, Any], incidents: Counter[str]) -> bool:
    try:
        generation = int(row["spec_generation"])
        current_generation = int(row["lineage_current_generation"])
        valid = (
            row["spec_lineage_id"] == row["head_lineage_id"]
            and generation == int(row["head_generation"])
            and current_generation >= generation
        )
        if current_generation == generation:
            valid = valid and row["lineage_current_obligation_id"] == row["obligation_id"]
        valid = valid and ((generation == 1) == (row["parent_obligation_id"] is None))
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_work_lineage"] += 1
    return valid


def _valid_work_redrive_generation(
    *,
    child: WorkObligation,
    work_models: Mapping[UUID, WorkObligation],
    work_groups: Mapping[UUID, Sequence[Mapping[str, Any]]],
    incidents: Counter[str],
) -> bool:
    parent = work_models.get(child.parent_obligation_id)
    parent_rows = work_groups.get(child.parent_obligation_id, ())
    valid = parent is not None and len(parent_rows) >= 2
    if parent is not None:
        valid = valid and (
            child.tenant_id == parent.tenant_id
            and child.lineage_id == parent.lineage_id
            and child.generation == parent.generation + 1
            and child.semantic_dedupe_key == parent.semantic_dedupe_key
            and child.target_object_type == parent.target_object_type
            and child.target_object_id == parent.target_object_id
            and child.owner_writer_id == parent.owner_writer_id
            and child.purpose == parent.purpose
            and child.effect_possible == parent.effect_possible
        )
    if len(parent_rows) >= 2:
        try:
            prior_state = WorkObligationState(str(parent_rows[-2]["state"]))
            final_state = WorkObligationState(str(parent_rows[-1]["state"]))
            payload = _json(parent_rows[-1]["transition_payload"])
            valid = valid and (
                prior_state is WorkObligationState.REDRIVE_AUTHORIZED
                and final_state is WorkObligationState.SUPERSEDED_BY_NEW_GENERATION
                and parent_rows[-1]["transition_kind"] == "successor_registered"
                and payload.get("obligation_id") == str(child.obligation_id)
                and payload.get("lineage_id") == str(child.lineage_id)
                and int(payload.get("generation") or 0) == child.generation
                and payload.get("superseded_parent_id")
                == str(child.parent_obligation_id)
            )
        except (KeyError, TypeError, ValueError):
            valid = False
    if not valid:
        incidents["unauthorized_or_drifted_work_redrive"] += 1
    return valid


def _valid_missed_heartbeat_takeover(
    *,
    takeover: LeaseTakeover,
    takeover_transaction_at: datetime,
    lease_models: Mapping[UUID, LeaseToken],
    effect_attempts: Mapping[UUID, ExternalEffectAttempt],
    effect_histories: Mapping[UUID, Sequence[Mapping[str, Any]]],
    receipt_models: Mapping[tuple[UUID, int], ExecutionReceipt],
    incidents: Counter[str],
) -> bool:
    successor = lease_models.get(takeover.successor.lease_token_id)
    safe_effect_states = {
        ExternalEffectState.RESERVED,
        ExternalEffectState.CANCELLED,
        ExternalEffectState.EXPIRED,
        ExternalEffectState.REJECTED,
        ExternalEffectState.FAILED,
        ExternalEffectState.RECONCILED_NO_EFFECT,
    }
    predecessor_effects = tuple(
        (effect_id, _effect_state_at_transaction(effect_histories.get(effect_id, ()), takeover_transaction_at))
        for effect_id, attempt in effect_attempts.items()
        if attempt.lease_token_id == takeover.predecessor_lease_token_id
        and attempt.lease_fence == takeover.predecessor_fence
    )
    valid = successor == takeover.successor
    valid = valid and all(
        state_and_version is not None and state_and_version[0] in safe_effect_states
        for _, state_and_version in predecessor_effects
    )
    if takeover.successor.effect_possible:
        expected_refs: set[str] = set()
        if not predecessor_effects:
            expected_refs.add(
                f"effect-ledger:no-attempt:{takeover.predecessor_lease_token_id}:"
                f"fence:{takeover.predecessor_fence}"
            )
        for effect_id, state_and_version in predecessor_effects:
            if state_and_version is None:
                continue
            state, version = state_and_version
            if state is ExternalEffectState.RESERVED:
                expected_refs.add(
                    f"external-effect-attempt:{effect_id}:state:reserved:"
                    f"version:{version}"
                )
            else:
                receipt = receipt_models.get((effect_id, version))
                if receipt is not None and receipt.effect_state is state:
                    expected_refs.add(f"execution-receipt:{receipt.receipt_id}")
        valid = valid and set(takeover.no_effect_evidence_refs) == expected_refs
    if not valid:
        incidents["unsafe_missed_heartbeat_takeover"] += 1
    return valid


def _effect_state_at_transaction(
    rows: Sequence[Mapping[str, Any]],
    transaction_at: datetime,
) -> tuple[ExternalEffectState, int] | None:
    """Return the effect state/version committed when the takeover was applied."""

    latest: tuple[ExternalEffectState, int] | None = None
    for row in rows:
        committed_at = row.get("transaction_at")
        if committed_at is None or committed_at > transaction_at:
            continue
        try:
            latest = (
                ExternalEffectState(str(row["state"])),
                int(row["aggregate_version"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
    return latest


def _valid_work_decision(row: Mapping[str, Any], incidents: Counter[str]) -> bool:
    try:
        decision = WorkDecision.model_validate(_json(row["decision"]))
        work = WorkObligation.model_validate(_json(row["obligation"]))
        valid = (
            decision.decision_id == row["decision_id"]
            and decision.decision_digest == row["decision_digest"]
            and decision.obligation_id == work.obligation_id
            and decision.obligation_generation == work.generation
            and work.minimum_processing_class.rank
            <= decision.selected_processing_class.rank
            <= work.maximum_processing_class.rank
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["processing_class_outside_work_envelope"] += 1
    return valid


def _valid_lease_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> bool:
    valid = True
    lease: LeaseToken | None = None
    current: LeaseState | None = None
    current_heartbeat_deadline: datetime | None = None
    for position, row in enumerate(rows, start=1):
        try:
            target = LeaseState(str(row["state"]))
            if position == 1:
                lease = LeaseToken.model_validate(_json(row["lease_payload"]))
                row_valid = target is LeaseState.ACTIVE
                current_heartbeat_deadline = lease.heartbeat_deadline
            else:
                payload = _json(row["lease_payload"])
                if target is LeaseState.ACTIVE:
                    heartbeat = LeaseHeartbeat.model_validate(payload)
                    row_valid = current is LeaseState.ACTIVE and lease is not None
                    if lease is not None:
                        row_valid = row_valid and (
                            heartbeat.lease_token_id == lease.lease_token_id
                            and heartbeat.obligation_id == lease.obligation_id
                            and heartbeat.fence == lease.fence
                            and heartbeat.owner_ref == lease.owner_ref
                            and heartbeat.expected_heartbeat_deadline
                            == current_heartbeat_deadline
                            and heartbeat.lease_expires_at == lease.expires_at
                        )
                    current_heartbeat_deadline = heartbeat.extended_heartbeat_deadline
                elif target is LeaseState.SUPERSEDED_BY_NEW_LEASE:
                    takeover = LeaseTakeover.model_validate(payload)
                    row_valid = current is not None and lease_transition_allowed(
                        current, target
                    )
                    if lease is not None:
                        row_valid = row_valid and (
                            takeover.predecessor_lease_token_id
                            == lease.lease_token_id
                            and takeover.predecessor_fence == lease.fence
                            and takeover.predecessor_attempt == lease.attempt
                            and takeover.predecessor_owner_ref == lease.owner_ref
                            and takeover.predecessor_heartbeat_deadline
                            == current_heartbeat_deadline
                        )
                else:
                    resolution = LeaseResolution.model_validate(payload)
                    row_valid = current is not None and lease_transition_allowed(
                        current, target
                    )
                    row_valid = row_valid and resolution.to_lease_state == target
                    if lease is not None:
                        row_valid = row_valid and (
                            resolution.lease_token_id == lease.lease_token_id
                            and resolution.fence == lease.fence
                        )
            row_valid = row_valid and int(row["aggregate_version"]) == position
        except (KeyError, TypeError, ValueError):
            row_valid = False
            target = None
        valid = valid and row_valid
        if target is not None:
            current = target
    last = rows[-1]
    if lease is None:
        valid = False
    else:
        valid = valid and (
            lease.fence == int(last["head_fence"])
            and lease.attempt == int(last["head_attempt"])
            and lease.attempt <= int(last["maximum_attempts"])
            and lease.expires_at <= last["work_deadline"]
            and lease.effect_possible == bool(last["work_effect_possible"])
            and lease.owner_ref == last["head_owner_ref"]
            and lease.expires_at == last["head_expires_at"]
            and current_heartbeat_deadline == last["head_heartbeat_deadline"]
            and int(last["head_version"]) == int(last["aggregate_version"])
            and str(last["head_state"]) == str(last["state"])
        )
    if not valid:
        incidents["invalid_lease_history_or_fence"] += 1
    return valid


def _valid_failure_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> bool:
    valid = True
    prior: FailureRecord | None = None
    identity_fields = (
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
    for position, row in enumerate(rows, start=1):
        try:
            record = FailureRecord.model_validate(_json(row["record"]))
            row_valid = (
                int(row["aggregate_version"]) == position
                and record.failure_id == row["failure_id"]
                and record.state == row["state"]
                and record.record_digest == row["record_digest"]
                and failure_transition_allowed(
                    prior.state if prior else None,
                    record.state,
                )
            )
            if prior is not None:
                row_valid = row_valid and _same_fields(
                    prior, record, identity_fields
                )
                row_valid = row_valid and record.updated_at > prior.updated_at
        except (KeyError, TypeError, ValueError):
            row_valid = False
            record = None
        valid = valid and row_valid
        if record is not None:
            prior = record
    last = rows[-1]
    if prior is None:
        valid = False
    else:
        valid = valid and (
            int(last["head_version"]) == int(last["aggregate_version"])
            and str(last["head_state"]) == str(prior.state)
            and last["head_record_digest"] == prior.record_digest
            and last["head_lineage_id"] == prior.lineage_id
            and int(last["head_generation"]) == prior.generation
            and last["head_work_obligation_id"] == prior.work_obligation_id
            and int(last["head_work_generation"])
            == prior.work_obligation_generation
            and int(last["lineage_current_generation"]) >= prior.generation
            and (
                int(last["lineage_current_generation"]) != prior.generation
                or last["lineage_current_failure_id"] == prior.failure_id
            )
            and last["current_owner_terminalization_request_id"]
            == prior.owner_terminalization_request_id
        )
    if not valid:
        incidents["invalid_failure_record_history"] += 1
    return valid


def _valid_failure_redrive_generation(
    *,
    child: FailureRecord,
    failure_initial_models: Mapping[UUID, FailureRecord],
    failure_current_models: Mapping[UUID, FailureRecord],
    failure_groups: Mapping[UUID, Sequence[Mapping[str, Any]]],
    work_models: Mapping[UUID, WorkObligation],
    work_groups: Mapping[UUID, Sequence[Mapping[str, Any]]],
    incidents: Counter[str],
) -> tuple[bool, bool]:
    parent_initial = failure_initial_models.get(child.parent_failure_id)
    parent_current = failure_current_models.get(child.parent_failure_id)
    child_current = failure_current_models.get(child.failure_id)
    parent_rows = failure_groups.get(child.parent_failure_id, ())
    child_work = work_models.get(child.work_obligation_id)
    parent_work = (
        work_models.get(parent_initial.work_obligation_id)
        if parent_initial is not None
        else None
    )
    progress_record: FailureRecord | None = None
    progress_transition_valid = False
    for index, row in enumerate(parent_rows):
        if index == 0:
            continue
        try:
            record = FailureRecord.model_validate(_json(row["record"]))
            prior = FailureRecord.model_validate(_json(parent_rows[index - 1]["record"]))
            if record.state is FailureState.REDRIVE_IN_PROGRESS:
                progress_record = record
                progress_transition_valid = (
                    prior.state is FailureState.REDRIVE_AUTHORIZED
                    and row["transition_kind"]
                    == "work_redrive_successor_registered"
                )
                break
        except (KeyError, TypeError, ValueError):
            continue
    authorized = all(
        value is not None
        for value in (
            parent_initial,
            parent_current,
            child_current,
            child_work,
            parent_work,
            progress_record,
        )
    )
    if (
        parent_initial is not None
        and child_work is not None
        and parent_work is not None
        and progress_record is not None
    ):
        identity_fields = (
            "tenant_id",
            "lineage_id",
            "causal_operation",
            "owner_writer_id",
            "semantic_owner_writer_id",
            "target_object_type",
            "target_object_id",
        )
        authorized = authorized and (
            all(
                getattr(child, name) == getattr(parent_initial, name)
                for name in identity_fields
            )
            and child.generation == parent_initial.generation + 1
            and child.original_semantic_idempotency_key
            != parent_initial.original_semantic_idempotency_key
            and child.created_at > progress_record.updated_at
            and child_work.parent_obligation_id == parent_work.obligation_id
            and child_work.generation == parent_work.generation + 1
            and child.work_obligation_id == child_work.obligation_id
            and child.work_obligation_generation == child_work.generation
            and f"work:{child_work.obligation_id}"
            in progress_record.remediation_evidence_refs
            and progress_transition_valid
        )
    else:
        authorized = False
    if not authorized:
        incidents["unauthorized_or_drifted_failure_redrive"] += 1

    child_work_terminal = False
    child_work_rows = work_groups.get(child.work_obligation_id, ())
    if child_work_rows:
        try:
            child_work_terminal = WorkObligationState(
                str(child_work_rows[-1]["state"])
            ).terminal
        except (KeyError, ValueError):
            pass
    closed = bool(
        authorized
        and parent_current is not None
        and child_current is not None
        and parent_current.state is FailureState.RESOLVED
        and child_current.state.terminal
        and child_work_terminal
        and f"failure:{child.failure_id}"
        in parent_current.remediation_evidence_refs
    )
    return authorized, closed


def _valid_owner_terminalization(
    row: Mapping[str, Any], incidents: Counter[str]
) -> bool:
    try:
        request = OwnerTerminalizationRequest.model_validate(_json(row["request"]))
        valid = (
            request.request_id == row["request_id"]
            and request.request_digest == row["request_digest"]
            and request.failure_id == row["failure_id"]
            and request.failure_generation == int(row["failure_generation"])
            and request.work_obligation_id == row["work_obligation_id"]
            and request.work_obligation_generation
            == int(row["work_obligation_generation"])
            and request.semantic_owner_writer_id
            == row["semantic_owner_writer_id"]
            and request.target_object_type == row["target_object_type"]
            and request.target_object_id == row["target_object_id"]
        )
        if row.get("resolution_id") is None:
            valid = valid and (
                row["failure_current_state"]
                == FailureState.OWNER_TERMINALIZATION_PENDING
                and row["work_current_state"]
                == WorkObligationState.OWNER_TERMINALIZATION_PENDING
                and row["current_owner_terminalization_request_id"]
                == request.request_id
            )
        else:
            resolution = OwnerTerminalizationResolution.model_validate(
                _json(row["resolution"])
            )
            owner_result = _json(row["owner_result"])
            owner_state = str(
                owner_result.get("state")
                or owner_result.get("current_fate")
                or ""
            )
            valid = valid and (
                resolution.resolution_id == row["resolution_id"]
                and resolution.resolution_digest == row["resolution_digest"]
                and resolution.request_id == request.request_id
                and resolution.failure_id == request.failure_id
                and resolution.work_obligation_id == request.work_obligation_id
                and resolution.owner_command_result_id
                == row["owner_command_result_id"]
                and row["owner_result_writer_id"]
                == request.semantic_owner_writer_id
                == resolution.observed_owner_writer_id
                and row["owner_result_object_type"] == request.target_object_type
                == resolution.observed_owner_object_type
                and row["owner_result_object_id"] == request.target_object_id
                == resolution.observed_owner_object_id
                and int(row["owner_result_object_version"])
                == resolution.observed_owner_object_version
                and owner_state == resolution.observed_owner_terminal_state
                and owner_state in request.acceptable_owner_terminal_states
                and row["failure_current_state"] == resolution.to_failure_state
                and row["work_current_state"] == resolution.to_work_state
                and row["current_owner_terminalization_request_id"] is None
            )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_owner_terminalization_handshake"] += 1
    return valid


def _valid_effect_history(
    *,
    rows: Sequence[Mapping[str, Any]],
    receipt_models: Mapping[tuple[UUID, int], ExecutionReceipt],
    incidents: Counter[str],
) -> tuple[bool, ExternalEffectAttempt | None, int, int]:
    valid = True
    attempt: ExternalEffectAttempt | None = None
    current: ExternalEffectState | None = None
    dispatch_seen = False
    receipt_required = 0
    valid_receipts = 0
    for position, row in enumerate(rows, start=1):
        try:
            target = ExternalEffectState(str(row["state"]))
            if position == 1:
                attempt = ExternalEffectAttempt.model_validate(
                    _json(row["attempt_payload"])
                )
                row_valid = target is ExternalEffectState.RESERVED
                row_valid = row_valid and attempt.attempt_digest == row[
                    "head_attempt_digest"
                ]
            else:
                observation = EffectObservation.model_validate(
                    _json(row["attempt_payload"])
                )
                row_valid = current is not None and external_effect_transition_allowed(
                    current, target
                )
                row_valid = row_valid and (
                    observation.from_state == current
                    and observation.to_state == target
                    and observation.effect_attempt_id == row["effect_attempt_id"]
                )
                if target is ExternalEffectState.DISPATCH_INTENT_RECORDED:
                    dispatch_seen = True
                if observation.provider_observation_refs and not dispatch_seen:
                    incidents["provider_observation_before_dispatch_intent"] += 1
                    row_valid = False
                receipt_required += 1
                receipt = receipt_models.get((row["effect_attempt_id"], position))
                receipt_valid = bool(
                    receipt
                    and receipt.receipt_id == observation.receipt_id
                    and receipt.effect_state == target
                )
                valid_receipts += int(receipt_valid)
                if not receipt_valid:
                    incidents["effect_version_without_exact_receipt"] += 1
                    row_valid = False
            row_valid = row_valid and int(row["aggregate_version"]) == position
        except (KeyError, TypeError, ValueError):
            row_valid = False
            target = None
        valid = valid and row_valid
        if target is not None:
            current = target
    last = rows[-1]
    valid = valid and (
        int(last["head_version"]) == int(last["aggregate_version"])
        and str(last["head_state"]) == str(last["state"])
    )
    if not valid:
        incidents["invalid_external_effect_history"] += 1
    return valid, attempt, receipt_required, valid_receipts


def _valid_compensation_episode(
    *,
    rows: Sequence[Mapping[str, Any]],
    effect_attempts: Mapping[UUID, ExternalEffectAttempt],
    effect_terminal_states: Mapping[UUID, ExternalEffectState],
    effect_current_versions: Mapping[UUID, int],
    receipt_models: Mapping[tuple[UUID, int], ExecutionReceipt],
    incidents: Counter[str],
) -> tuple[bool, bool, bool]:
    valid = True
    terminal = False
    closed = False
    try:
        original = effect_attempts[rows[0]["effect_attempt_id"]]
        original_spec = InterventionSpec.model_validate(_json(rows[0]["spec"]))
        capabilities = ActionAdapterCapabilities.model_validate(
            _json(rows[0]["capabilities"])
        )
        compensation_spec = InterventionSpec.model_validate(
            _json(rows[0]["compensation_spec"])
        )
        proposed_rows = [
            row
            for row in rows
            if str(row["state"]) == ExternalEffectState.COMPENSATION_PROPOSED
        ]
        authorized_rows = [
            row
            for row in rows
            if str(row["state"]) == ExternalEffectState.COMPENSATION_AUTHORIZED
        ]
        linked_rows = [
            row
            for row in rows
            if str(row["state"])
            == ExternalEffectState.COMPENSATION_ATTEMPT_LINKED
        ]
        proposed = EffectObservation.model_validate(
            _json(proposed_rows[0]["attempt_payload"])
        )
        required_parent_ref = f"external-effect-attempt:{original.effect_attempt_id}"
        valid = (
            len(proposed_rows) == 1
            and proposed.compensation_intervention_spec_digest
            == compensation_spec.spec_digest
            == rows[0]["current_compensation_spec_digest"]
            and original_spec.reversible
            and bool(original_spec.compensation_declaration)
            and capabilities.compensation_supported
            and compensation_spec.spec_digest != original_spec.spec_digest
            and required_parent_ref in compensation_spec.grounding_dependency_refs
        )

        authorization: AuthorizationDecision | None = None
        linked: ExternalEffectAttempt | None = None
        link_observation: EffectObservation | None = None
        if authorized_rows:
            authorization_observation = EffectObservation.model_validate(
                _json(authorized_rows[0]["attempt_payload"])
            )
            authorization = AuthorizationDecision.model_validate(
                _json(rows[0]["compensation_authorization_decision"])
            )
            valid = valid and (
                len(authorized_rows) == 1
                and authorization_observation.compensation_intervention_spec_digest
                == compensation_spec.spec_digest
                and authorization_observation.compensation_authorization_decision_id
                == authorization.decision_id
                == rows[0]["current_compensation_authorization_decision_id"]
                and authorization_observation.compensation_authorization_ref
                == f"authorization-decision:{authorization.decision_id}"
                and authorization.disposition
                is AuthorizationDisposition.AUTHORIZED
                and authorization.intervention_spec_digest
                == compensation_spec.spec_digest
                and compensation_spec.operation in authorization.exact_operations
                and rows[0]["compensation_proposal_fate"]
                == "accepted_for_authorization"
            )
        if linked_rows:
            link_observation = EffectObservation.model_validate(
                _json(linked_rows[0]["attempt_payload"])
            )
            linked = effect_attempts.get(link_observation.compensation_attempt_id)
            valid = valid and (
                len(linked_rows) == 1
                and authorization is not None
                and linked is not None
                and link_observation.compensation_attempt_id
                == rows[0]["current_compensation_attempt_id"]
                and linked.compensates_effect_attempt_id
                == original.effect_attempt_id
                and linked.intervention_spec_digest == compensation_spec.spec_digest
                and linked.authorization_decision_id == authorization.decision_id
            )

        current_state = ExternalEffectState(str(rows[-1]["head_state"]))
        terminal = current_state in {
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        }
        if terminal and current_state in {
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
        }:
            final_observation = EffectObservation.model_validate(
                _json(rows[-1]["attempt_payload"])
            )
            linked_state = (
                effect_terminal_states.get(linked.effect_attempt_id)
                if linked is not None
                else None
            )
            linked_version = (
                effect_current_versions.get(linked.effect_attempt_id)
                if linked is not None
                else None
            )
            linked_receipt = (
                receipt_models.get((linked.effect_attempt_id, linked_version))
                if linked is not None and linked_version is not None
                else None
            )
            allowed_linked_states = (
                {ExternalEffectState.SUCCEEDED}
                if current_state is ExternalEffectState.COMPENSATED
                else {
                    ExternalEffectState.FAILED,
                    ExternalEffectState.REJECTED,
                    ExternalEffectState.TERMINAL_PARTIAL,
                    ExternalEffectState.COMPENSATION_FAILED,
                }
            )
            closed = bool(
                valid
                and linked_state in allowed_linked_states
                and linked_receipt is not None
                and linked_receipt.effect_state is linked_state
                and f"execution-receipt:{linked_receipt.receipt_id}"
                in final_observation.external_state_evidence_refs
            )
        elif terminal:
            expected_fate = (
                "rejected"
                if current_state is ExternalEffectState.COMPENSATION_REJECTED
                else "expired"
            )
            final_observation = EffectObservation.model_validate(
                _json(rows[-1]["attempt_payload"])
            )
            proposal_result_id = rows[0][
                "compensation_proposal_fate_command_result_id"
            ]
            closed = bool(
                valid
                and not authorized_rows
                and not linked_rows
                and rows[0]["compensation_proposal_fate"] == expected_fate
                and proposal_result_id is not None
                and f"agency-command-result:{proposal_result_id}"
                in final_observation.external_state_evidence_refs
            )
    except (IndexError, KeyError, TypeError, ValueError):
        valid = False
        terminal = False
        closed = False
    if not valid:
        incidents["invalid_compensation_spec_authorization_attempt_linkage"] += 1
    if terminal and not closed:
        incidents["terminal_compensation_without_exact_linked_receipt"] += 1
    return bool(valid), terminal, closed


def _effect_continuity_valid(
    row: Mapping[str, Any],
    attempt: ExternalEffectAttempt,
    incidents: Counter[str],
) -> bool:
    try:
        spec = _json(row["spec"])
        authorization = _json(row["authorization_decision"])
        capabilities = ActionAdapterCapabilities.model_validate(
            _json(row["capabilities"])
        )
        valid = (
            attempt.lineage_id == row["head_lineage_id"]
            and attempt.generation == int(row["head_generation"])
            and attempt.episode_id == row["head_episode_id"]
            and attempt.task_id == row["head_task_id"]
            and attempt.intervention_spec_digest == row["head_spec_digest"]
            and attempt.intervention_spec_digest == row["stored_spec_digest"]
            and spec["operation"] == attempt.operation
            and spec["action_adapter_version"] == attempt.capability_version
            and spec["action_adapter_capability_digest"]
            == attempt.capability_digest
            and authorization["intervention_spec_digest"]
            == attempt.intervention_spec_digest
            and attempt.operation in authorization["exact_operations"]
            and set(attempt.target_grounding_refs)
            <= set(authorization["exact_target_refs"])
            and capabilities.capability_id == attempt.capability_id
            and capabilities.capability_version == attempt.capability_version
            and capabilities.capability_digest == attempt.capability_digest
            and attempt.operation in capabilities.permitted_operations
            and attempt.task_id == row["head_task_id"]
            and row["task_episode_id"] == attempt.episode_id
            and row["task_spec_digest"] == attempt.intervention_spec_digest
            and bool(row["task_effect_required"])
            and row["work_generation"] == attempt.work_obligation_generation
            and row["lease_obligation_id"] == attempt.work_obligation_id
            and row["lease_work_generation"] == attempt.work_obligation_generation
            and int(row["lease_fence"]) == attempt.lease_fence
            and row["provider_key_lineage_id"] == attempt.lineage_id
            and row["provider_key_request_hash"] == attempt.canonical_request_hash
            and row["lineage_current_generation"] >= attempt.generation
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["effect_spec_authorization_capability_fence_discontinuity"] += 1
    return valid


def _command_reconstructable(row: Mapping[str, Any]) -> bool:
    model_by_kind = {
        "apply_workflow_run": WorkflowRunCommand,
        "apply_task": TaskCommand,
        "register_work_obligation": WorkObligationRegistrationCommand,
        "work_decision": WorkDecisionCommand,
        "work_state_transition": WorkStateTransitionCommand,
        "grant_work_lease": LeaseGrantCommand,
        "heartbeat_work_lease": LeaseHeartbeatCommand,
        "resolve_work_lease": LeaseResolutionCommand,
        "take_over_work_lease": LeaseTakeoverCommand,
        "register_action_adapter_capabilities": AdapterCapabilityRegistrationCommand,
        "reserve_external_effect": EffectReservationCommand,
        "transition_external_effect": EffectTransitionCommand,
        "apply_failure_record": FailureRecordCommand,
        "request_owner_terminalization": OwnerTerminalizationRequestCommand,
        "resolve_owner_terminalization": OwnerTerminalizationResolutionCommand,
    }
    try:
        model = model_by_kind[str(row["command_kind"])]
        command = model.model_validate(_json(row["command"]))
        return bool(
            command.request_digest == row["request_digest"]
            and command.context.processing_authority.fingerprint
            == row["processing_authority_fingerprint"]
            and command.context.writer_scope_epoch.scope_id == row["writer_scope_id"]
            and command.context.writer_scope_epoch.epoch == row["writer_epoch"]
        )
    except (KeyError, TypeError, ValueError):
        return False


def build_execution_invariant_evidence(
    state: ExecutionEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    effect_safe = min(
        state.legal_effect_attempt_count,
        state.exact_effect_continuity_count,
    ) + state.valid_compensation_episode_count + state.closed_compensation_episode_count
    effect_exposures = (
        state.effect_attempt_count
        + state.compensation_episode_count
        + state.terminal_compensation_episode_count
    )
    work_safe = (
        min(state.legal_work_obligation_count, state.valid_work_lineage_count)
        + state.valid_lease_count
        + state.envelope_conformant_decision_count
        + state.legal_failure_record_count
        + state.authorized_failure_redrive_generation_count
        + state.closed_failure_redrive_generation_count
        + state.resolved_owner_terminalization_count
        + state.authorized_work_redrive_generation_count
        + state.safe_missed_heartbeat_takeover_count
    )
    work_exposures = (
        state.work_obligation_count
        + state.lease_count
        + state.work_decision_count
        + state.failure_record_count
        + (2 * state.failure_redrive_generation_count)
        + state.owner_terminalization_request_count
        + state.work_redrive_generation_count
        + state.missed_heartbeat_takeover_count
    )
    definitions = {
        "INV-12": (
            "inv.external_effect_safety",
            effect_safe,
            effect_exposures,
            {
                "invalid_external_effect_history",
                "effect_spec_authorization_capability_fence_discontinuity",
                "unsafe_effect_retry",
                "provider_observation_before_dispatch_intent",
                "effect_version_without_exact_receipt",
                "invalid_execution_receipt",
                "invalid_compensation_spec_authorization_attempt_linkage",
                "terminal_compensation_without_exact_linked_receipt",
            },
        ),
        "INV-16": (
            "inv.execution_reconstructability",
            state.reconstructable_command_count,
            state.command_count,
            {"unreconstructable_execution_command"},
        ),
        "INV-22": (
            "inv.execution_spec_continuity",
            state.exact_effect_continuity_count,
            state.effect_attempt_count,
            {"effect_spec_authorization_capability_fence_discontinuity"},
        ),
        "INV-23": (
            "inv.work_lease_fate_integrity",
            work_safe,
            work_exposures,
            {
                "invalid_work_obligation_history",
                "invalid_work_lineage",
                "unauthorized_or_drifted_work_redrive",
                "processing_class_outside_work_envelope",
                "invalid_lease_history_or_fence",
                "unsafe_missed_heartbeat_takeover",
                "invalid_failure_record_history",
                "unauthorized_or_drifted_failure_redrive",
                "invalid_owner_terminalization_handshake",
                "repair_child_without_exact_owner_result",
            },
        ),
        "INV-29": (
            "inv.execution_atomic_transport",
            min(
                state.reconstructable_command_count,
                _rate_numerator(state.command_event_coverage, state.command_count),
                _rate_numerator(state.command_outbox_coverage, state.command_count),
                _rate_numerator(
                    state.immutable_storage_guard_rate, state.command_count
                ),
            ),
            state.command_count,
            {
                "unreconstructable_execution_command",
                "execution_command_without_exact_event",
                "execution_command_without_exact_outbox",
                "immutable_execution_table_unguarded",
            },
        ),
    }
    rows = []
    for invariant_id, (metric_id, numerator, denominator_value, names) in (
        definitions.items()
    ):
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(state.incident_counts.get(name, 0) for name in names)
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:execution",
            denominator_version="workflow-work-effect-denominator-v1",
            population_definition_version="canonical-execution-component-v1",
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "invariant": invariant_id,
                }
            ),
            source_or_oracle_population=denominator_value,
            production_accepted=denominator_value,
            eligible=denominator_value,
            attempted_or_committed=denominator_value,
            terminal_fates={"covered": min(numerator, denominator_value)},
            nonterminal_fates={
                "uncovered": max(0, denominator_value - numerator)
            },
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="workflow_work_external_effect",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "unsafe" in name or "discontinuity" in name else 4,
                summary=f"Observed {state.incident_counts[name]} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name in sorted(names)
            if state.incident_counts.get(name, 0)
        )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "object_event_and_result_ids",
                        "authority_context",
                        "proposal_spec_authority_work_lease_effect_receipt_and_episode_versions",
                        "work_obligation_and_lease_fates",
                        "external_effect_attempt_and_receipt",
                        "separate_compensation_spec_authorization_attempt_and_receipt",
                    }
                ),
                executed_scenario_ids=frozenset(
                    invariant.proof.suite_and_scenario_ids
                )
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="workflow-work-effect-runtime-v1",
                        raw_numerator=float(numerator),
                        raw_denominator=float(denominator_value),
                        point_estimate=(
                            numerator / denominator_value
                            if denominator_value
                            else None
                        ),
                        violation_count=violations,
                        severity_mass=float(violations),
                        artifact_refs=state.artifact_refs,
                    ),
                ),
                incidents=incidents,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=denominator,
                uncertainty=state.uncertainty,
                blind_spots=state.uncertainty,
                artifact_refs=state.artifact_refs,
            )
        )
    return tuple(rows)


def render_execution_markdown(state: ExecutionEvaluationState) -> str:
    lines = [
        f"# Workflow, work, and external-effect evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        _metric_line(
            "Legal workflow histories",
            state.legal_workflow_run_count,
            state.workflow_run_count,
            state.workflow_history_integrity_rate,
        ),
        _metric_line(
            "Legal task histories",
            state.legal_task_count,
            state.task_count,
            state.task_history_integrity_rate,
        ),
        _metric_line(
            "Receipt-backed external task completions",
            state.receipt_backed_external_task_count,
            state.completed_external_task_count,
            state.external_task_receipt_rate,
        ),
        _metric_line(
            "Legal work histories",
            state.legal_work_obligation_count,
            state.work_obligation_count,
            state.work_history_integrity_rate,
        ),
        _metric_line(
            "Valid work lineages",
            state.valid_work_lineage_count,
            state.work_obligation_count,
            state.work_lineage_integrity_rate,
        ),
        _metric_line(
            "Authorized work redrive generations",
            state.authorized_work_redrive_generation_count,
            state.work_redrive_generation_count,
            state.work_redrive_authorization_rate,
        ),
        _metric_line(
            "Valid lease histories",
            state.valid_lease_count,
            state.lease_count,
            state.lease_integrity_rate,
        ),
        f"- Lease heartbeats observed: {state.lease_heartbeat_count}",
        _metric_line(
            "Safe missed-heartbeat takeovers",
            state.safe_missed_heartbeat_takeover_count,
            state.missed_heartbeat_takeover_count,
            state.takeover_safety_rate,
        ),
        _metric_line(
            "Legal failure histories",
            state.legal_failure_record_count,
            state.failure_record_count,
            state.failure_history_integrity_rate,
        ),
        _metric_line(
            "Authorized failure redrive generations",
            state.authorized_failure_redrive_generation_count,
            state.failure_redrive_generation_count,
            state.failure_redrive_authorization_rate,
        ),
        _metric_line(
            "Closed failure redrive generations",
            state.closed_failure_redrive_generation_count,
            state.failure_redrive_generation_count,
            state.failure_redrive_closure_rate,
        ),
        _metric_line(
            "Resolved owner-terminalization handshakes",
            state.resolved_owner_terminalization_count,
            state.owner_terminalization_request_count,
            state.owner_terminalization_closure_rate,
        ),
        "- Owner-terminalization closure by semantic writer: "
        + (
            ", ".join(
                f"{writer_id}="
                f"{state.resolved_owner_terminalization_writer_counts.get(writer_id, 0)}"
                f"/{count} ({state.owner_terminalization_writer_closure_rates[writer_id]:.3f})"
                for writer_id, count in sorted(
                    state.owner_terminalization_writer_counts.items()
                )
            )
            if state.owner_terminalization_writer_counts
            else "not exposed"
        ),
        _metric_line(
            "Legal external-effect histories",
            state.legal_effect_attempt_count,
            state.effect_attempt_count,
            state.effect_history_integrity_rate,
        ),
        _metric_line(
            "Exact effect continuity",
            state.exact_effect_continuity_count,
            state.effect_attempt_count,
            state.effect_continuity_rate,
        ),
        _metric_line(
            "Execution receipt closure",
            state.valid_execution_receipt_count,
            state.receipt_required_transition_count,
            state.receipt_closure_rate,
        ),
        _metric_line(
            "Valid separately governed compensation episodes",
            state.valid_compensation_episode_count,
            state.compensation_episode_count,
            state.compensation_integrity_rate,
        ),
        _metric_line(
            "Closed terminal compensation episodes",
            state.closed_compensation_episode_count,
            state.terminal_compensation_episode_count,
            state.compensation_closure_rate,
        ),
        _metric_line(
            "Command reconstruction",
            state.reconstructable_command_count,
            state.command_count,
            state.command_reconstructability_rate,
        ),
        f"- Currently unresolved effect attempts: **{state.unresolved_effect_count}**",
        "",
        "## External-effect fates",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.effect_fate_counts.items())
            if state.effect_fate_counts
            else ("- no scoped effect attempts",)
        ),
        "",
        "## Failure fates",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.failure_fate_counts.items())
            if state.failure_fate_counts
            else ("- no scoped failure records",)
        ),
        "",
        "## Work fates",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.work_fate_counts.items())
            if state.work_fate_counts
            else ("- no scoped work obligations",)
        ),
        "",
        "## Incidents",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.incident_counts.items())
            if state.incident_counts
            else ("- none observed in this scope",)
        ),
        "",
        "## Proof limits",
        "",
        *(f"- {item}" for item in state.uncertainty),
        "",
    ]
    return "\n".join(lines)


def _head_snapshot_matches(
    row: Mapping[str, Any], snapshot: WorkflowRunSnapshot | TaskSnapshot
) -> bool:
    return bool(
        int(row["head_version"]) == int(row["aggregate_version"])
        and str(row["head_state"]) == str(snapshot.state)
        and row["head_snapshot_digest"] == snapshot.snapshot_digest
    )


def _same_fields(left: Any, right: Any, names: tuple[str, ...]) -> bool:
    return all(getattr(left, name) == getattr(right, name) for name in names)


def _last_observed_at(rows: Sequence[Mapping[str, Any]]) -> datetime | None:
    for row in reversed(rows[1:]):
        try:
            return EffectObservation.model_validate(
                _json(row["attempt_payload"])
            ).observed_at
        except (KeyError, TypeError, ValueError):
            continue
    return None


def _group(
    rows: Sequence[Mapping[str, Any]], key: str
) -> dict[UUID, tuple[Mapping[str, Any], ...]]:
    grouped: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return {
        item_id: tuple(sorted(items, key=lambda item: int(item["aggregate_version"])))
        for item_id, items in grouped.items()
    }


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _rate_numerator(value: float | None, denominator: int) -> int:
    return round((value or 0.0) * denominator)


def _metric_line(
    label: str, numerator: int, denominator: int, rate: float | None
) -> str:
    rendered = f"{rate:.1%}" if rate is not None else "n/a"
    return f"- {label}: **{numerator}/{denominator} ({rendered})**"


__all__ = [
    "ExecutionEvaluationScope",
    "ExecutionEvaluationState",
    "analyze_execution_rows",
    "build_execution_invariant_evidence",
    "evaluate_execution_state",
    "render_execution_markdown",
]
