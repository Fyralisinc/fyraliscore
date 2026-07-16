from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.execution import (
    ExecutionEvaluationScope,
    ExecutionEvaluationState,
    analyze_execution_rows,
    build_execution_invariant_evidence,
)
from lib.evaluation.proof import EvidenceTier


NOW = datetime(2026, 7, 16, tzinfo=timezone.utc)


def _scope() -> ExecutionEvaluationScope:
    return ExecutionEvaluationScope(
        tenant_id=uuid4(),
        start=NOW,
        end=NOW.replace(day=17),
        run_id="execution-evaluation-unit",
    )


def _state() -> ExecutionEvaluationState:
    return ExecutionEvaluationState(
        scope=_scope(),
        workflow_run_count=1,
        workflow_state_counts={"completed": 1},
        legal_workflow_run_count=1,
        workflow_history_integrity_rate=1.0,
        task_count=1,
        task_state_counts={"completed": 1},
        legal_task_count=1,
        task_history_integrity_rate=1.0,
        completed_external_task_count=1,
        receipt_backed_external_task_count=1,
        external_task_receipt_rate=1.0,
        work_obligation_count=1,
        work_fate_counts={"completed": 1},
        legal_work_obligation_count=1,
        work_history_integrity_rate=1.0,
        valid_work_lineage_count=1,
        work_lineage_integrity_rate=1.0,
        work_redrive_generation_count=1,
        authorized_work_redrive_generation_count=1,
        work_redrive_authorization_rate=1.0,
        work_decision_count=1,
        envelope_conformant_decision_count=1,
        work_decision_envelope_rate=1.0,
        lease_count=1,
        lease_fate_counts={"completed": 1},
        valid_lease_count=1,
        lease_integrity_rate=1.0,
        lease_heartbeat_count=1,
        missed_heartbeat_takeover_count=1,
        safe_missed_heartbeat_takeover_count=1,
        takeover_safety_rate=1.0,
        failure_record_count=1,
        failure_fate_counts={"resolved": 1},
        legal_failure_record_count=1,
        failure_history_integrity_rate=1.0,
        failure_redrive_generation_count=1,
        authorized_failure_redrive_generation_count=1,
        failure_redrive_authorization_rate=1.0,
        closed_failure_redrive_generation_count=1,
        failure_redrive_closure_rate=1.0,
        owner_terminalization_request_count=1,
        valid_owner_terminalization_count=1,
        resolved_owner_terminalization_count=1,
        owner_terminalization_closure_rate=1.0,
        effect_attempt_count=1,
        effect_fate_counts={"succeeded": 1},
        legal_effect_attempt_count=1,
        effect_history_integrity_rate=1.0,
        exact_effect_continuity_count=1,
        effect_continuity_rate=1.0,
        retry_attempt_count=0,
        safe_retry_attempt_count=0,
        retry_safety_rate=None,
        compensation_episode_count=0,
        valid_compensation_episode_count=0,
        compensation_integrity_rate=None,
        terminal_compensation_episode_count=0,
        closed_compensation_episode_count=0,
        compensation_closure_rate=None,
        receipt_required_transition_count=4,
        valid_execution_receipt_count=4,
        receipt_closure_rate=1.0,
        unresolved_effect_count=0,
        mean_effect_resolution_seconds=120.0,
        immutable_table_count=14,
        guarded_immutable_table_count=14,
        immutable_storage_guard_rate=1.0,
        command_count=17,
        reconstructable_command_count=17,
        command_reconstructability_rate=1.0,
        command_event_coverage=1.0,
        command_outbox_coverage=1.0,
        incident_counts={},
        uncertainty=("component proof only",),
        artifact_refs=("pytest://execution-evaluator",),
    )


def test_execution_evidence_is_e3_and_component_partitioned():
    registry = load_architecture_registry(Path("architecture/registry.yaml"))
    evidence = build_execution_invariant_evidence(
        _state(),
        registry=registry,
        executed_scenario_ids=frozenset(),
    )

    assert {row.invariant_id for row in evidence} == {
        "INV-12",
        "INV-16",
        "INV-22",
        "INV-23",
        "INV-29",
    }
    assert all(row.achieved_evidence_tier is EvidenceTier.E3 for row in evidence)
    assert all(
        row.denominator.population_partition_value
        == "workflow_work_external_effect"
        for row in evidence
    )
    assert all(not row.incidents for row in evidence)


def test_empty_population_is_not_rendered_as_perfect_and_tampering_is_localized():
    state = analyze_execution_rows(
        scope=_scope(),
        workflows=(),
        tasks=(),
        works=(),
        decisions=(),
        leases=(),
        effects=(),
        receipts=(
            {
                "effect_attempt_id": uuid4(),
                "effect_version": 2,
                "receipt": {"not": "a receipt"},
            },
        ),
        failures=(),
        owner_terminalizations=(),
        commands=(
            {
                "command_kind": "forged_execution_command",
                "command": {},
                "event_count": 0,
                "outbox_count": 2,
            },
        ),
        guarded_tables={
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
        },
        artifact_refs=("pytest://tampered",),
    )

    assert state.workflow_history_integrity_rate is None
    assert state.effect_continuity_rate is None
    assert state.retry_safety_rate is None
    assert state.compensation_integrity_rate is None
    assert state.compensation_closure_rate is None
    assert state.work_redrive_authorization_rate is None
    assert state.takeover_safety_rate is None
    assert state.failure_redrive_authorization_rate is None
    assert state.failure_redrive_closure_rate is None
    assert state.incident_counts == {
        "execution_command_without_exact_event": 1,
        "execution_command_without_exact_outbox": 1,
        "invalid_execution_receipt": 1,
        "unreconstructable_execution_command": 1,
    }
