from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from services.ingest.source_certification.scenario_execution_artifact import (
    SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION,
    SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION,
    SCENARIO_EXECUTION_ISOLATION_ACK,
    ScenarioExecutionArtifactError,
    evidence_record_sha256,
    execution_context_sha256,
    scenario_contract_sha256,
    seal_scenario_execution_artifact,
    validate_scenario_execution_artifact,
)


START = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
CHECKPOINT_IDS = {
    "pagination_resume": (
        "before_resume",
        "resume_boundary",
        "after_resume",
    ),
    "lifecycle_transition": (
        "before",
        "created",
        "updated",
        "deleted_or_absent",
    ),
    "ordering_convergence": (
        "ordered_result",
        "out_of_order_result",
    ),
    "declared_absence": ("declaration", "rejection"),
    "renewal": ("before_expiry", "renewed", "after_expiry"),
    "backlog_drain": (),
    "cursor_hold_then_resume": (),
}


def _at(seconds: float) -> str:
    return (START + timedelta(seconds=seconds)).isoformat()


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _record(value: dict[str, object]) -> dict[str, object]:
    value["evidence_sha256"] = evidence_record_sha256(value)
    return value


def _rehash_record(value: dict[str, object]) -> None:
    value["evidence_sha256"] = evidence_record_sha256(value)


def _rehash_artifact(value: dict[str, object]) -> None:
    body = dict(value)
    body.pop("artifact_sha256", None)
    rendered = (
        json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    value["artifact_sha256"] = hashlib.sha256(rendered).hexdigest()


def _infrastructure(label: str = "fault") -> dict[str, object]:
    return {
        "isolation_ack": SCENARIO_EXECUTION_ISOLATION_ACK,
        "network_scope": "loopback",
        "database_identity_sha256": _digest(f"{label}:database"),
        "kafka_cluster_identity_sha256": _digest(f"{label}:kafka"),
        "object_store_identity_sha256": _digest(f"{label}:object-store"),
        "redis_identity_sha256": _digest(f"{label}:redis"),
        "provider_lab_identity_sha256": _digest(f"{label}:provider-lab"),
    }


def _fault_topology() -> dict[str, object]:
    return {
        "tenant_ids": ["tenant-a", "tenant-b"],
        "installations": [
            {"tenant_id": "tenant-a", "installation_id": "install-a"},
            {"tenant_id": "tenant-b", "installation_id": "install-b"},
        ],
        "replicas": [
            {
                "replica_id": "replica-a",
                "process_identity_sha256": _digest("fault:replica-a"),
            },
            {
                "replica_id": "replica-b",
                "process_identity_sha256": _digest("fault:replica-b"),
            },
        ],
    }


def _operation(
    *,
    sequence: int,
    execution_id: str,
    context_sha256: str,
    tenant_id: str,
    installation_id: str,
    replica_id: str,
    phase: str,
    outcome: str,
    start: float,
    end: float,
) -> dict[str, object]:
    return _record(
        {
            "sequence": sequence,
            "execution_id": execution_id,
            "context_sha256": context_sha256,
            "operation_id": "messages.list",
            "tenant_id": tenant_id,
            "installation_id": installation_id,
            "replica_id": replica_id,
            "attempt": sequence,
            "phase": phase,
            "outcome": outcome,
            "started_at": _at(start),
            "completed_at": _at(end),
            "request_sha256": _digest(f"request:{execution_id}:{sequence}"),
            "response_sha256": (
                None
                if outcome == "shared_state_blocked"
                else _digest(f"response:{execution_id}:{sequence}")
            ),
        }
    )


def _checkpoint(
    *,
    sequence: int,
    checkpoint_id: str,
    observed_at: float,
    state: str,
    receipt: str,
) -> dict[str, object]:
    return {
        "sequence": sequence,
        "checkpoint_id": checkpoint_id,
        "observed_at": _at(observed_at),
        "state_sha256": _digest(state),
        "receipt_sha256": _digest(receipt),
    }


def _requirement_row(
    *,
    sequence: int,
    execution_id: str,
    context_sha256: str,
    requirement_id: str,
    requirement_type: str,
    operation_id: str | None,
    fault_id: str | None,
    operation_hash: str | None,
    checkpoints: list[dict[str, object]],
    backlog: dict[str, object] | None = None,
    cursor: dict[str, object] | None = None,
) -> dict[str, object]:
    return _record(
        {
            "sequence": sequence,
            "execution_id": execution_id,
            "context_sha256": context_sha256,
            "requirement_id": requirement_id,
            "requirement_type": requirement_type,
            "operation_id": operation_id,
            "fault_id": fault_id,
            "operation_evidence_sha256": operation_hash,
            "checkpoints": checkpoints,
            "backlog": backlog,
            "cursor": cursor,
        }
    )


def _fault_artifact(
    *,
    execution_id: str = "fault-execution-a",
) -> dict[str, object]:
    source_id = "slack"
    scenario_id = "provider_429_shared_cooldown"
    executable_sha256 = _digest("slack-429-executable")
    contract = {
        "schema_version": SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION,
        "source_id": source_id,
        "scenario_id": scenario_id,
        "required_operation_ids": ["messages.list"],
        "required_faults": [
            {
                "fault_id": "shared-429",
                "fault_kind": "provider_429",
                "operation_id": "messages.list",
                "observer_scope": "cross_replica",
            }
        ],
        "requirements": [
            {
                "requirement_id": "429-recovery",
                "requirement_type": "fault_recovery",
                "operation_id": "messages.list",
                "fault_id": "shared-429",
                "checkpoint_ids": [
                    "fault_injected",
                    "fault_observed",
                    "recovered",
                ],
            },
            {
                "requirement_id": "backlog-drain",
                "requirement_type": "backlog_drain",
                "operation_id": "messages.list",
                "fault_id": "shared-429",
                "checkpoint_ids": [],
            },
            {
                "requirement_id": "cursor-hold",
                "requirement_type": "cursor_hold_then_resume",
                "operation_id": "messages.list",
                "fault_id": "shared-429",
                "checkpoint_ids": [],
            },
            {
                "requirement_id": "topology",
                "requirement_type": "topology_participation",
                "operation_id": None,
                "fault_id": None,
                "checkpoint_ids": ["topology_observed"],
            },
        ],
    }
    contract_sha256 = scenario_contract_sha256(contract)
    infrastructure = _infrastructure()
    topology = _fault_topology()
    context_sha256 = execution_context_sha256(
        source_id=source_id,
        scenario_id=scenario_id,
        executable_sha256=executable_sha256,
        scenario_contract_sha256=contract_sha256,
        execution_id=execution_id,
        infrastructure=infrastructure,
        topology=topology,
    )
    operations = [
        _operation(
            sequence=1,
            execution_id=execution_id,
            context_sha256=context_sha256,
            tenant_id="tenant-a",
            installation_id="install-a",
            replica_id="replica-a",
            phase="inject",
            outcome="fault_observed",
            start=1,
            end=1.4,
        ),
        _operation(
            sequence=2,
            execution_id=execution_id,
            context_sha256=context_sha256,
            tenant_id="tenant-a",
            installation_id="install-a",
            replica_id="replica-b",
            phase="observe",
            outcome="shared_state_blocked",
            start=2,
            end=2.4,
        ),
        _operation(
            sequence=3,
            execution_id=execution_id,
            context_sha256=context_sha256,
            tenant_id="tenant-a",
            installation_id="install-a",
            replica_id="replica-b",
            phase="recover",
            outcome="succeeded",
            start=3,
            end=3.4,
        ),
        _operation(
            sequence=4,
            execution_id=execution_id,
            context_sha256=context_sha256,
            tenant_id="tenant-b",
            installation_id="install-b",
            replica_id="replica-a",
            phase="control",
            outcome="succeeded",
            start=4,
            end=4.4,
        ),
    ]
    operation_hashes = [
        str(operation["evidence_sha256"]) for operation in operations
    ]
    fault = _record(
        {
            "sequence": 1,
            "execution_id": execution_id,
            "context_sha256": context_sha256,
            "fault_id": "shared-429",
            "fault_kind": "provider_429",
            "operation_id": "messages.list",
            "injected_operation_evidence_sha256": operation_hashes[0],
            "observer_operation_evidence_sha256": operation_hashes[1],
            "recovery_operation_evidence_sha256": operation_hashes[2],
            "injected_at": _at(1.2),
            "observed_at": _at(2.2),
            "recovered_at": _at(3.2),
        }
    )
    backlog = {
        "baseline_count": 0,
        "peak_count": 5,
        "recovered_count": 0,
        "baseline_observed_at": _at(0.5),
        "peak_observed_at": _at(2.5),
        "recovered_observed_at": _at(4.5),
        "sample_sha256": _digest("fault:backlog-samples"),
    }
    cursor = {
        "before_sha256": _digest("cursor:before"),
        "held_sha256": _digest("cursor:before"),
        "after_sha256": _digest("cursor:after"),
        "before_observed_at": _at(0.5),
        "held_observed_at": _at(2.5),
        "after_observed_at": _at(4.5),
        "sample_sha256": _digest("fault:cursor-samples"),
    }
    requirements = [
        _requirement_row(
            sequence=1,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="429-recovery",
            requirement_type="fault_recovery",
            operation_id="messages.list",
            fault_id="shared-429",
            operation_hash=operation_hashes[2],
            checkpoints=[
                _checkpoint(
                    sequence=1,
                    checkpoint_id="fault_injected",
                    observed_at=1.2,
                    state="fault-active",
                    receipt=f"{execution_id}:fault-injected",
                ),
                _checkpoint(
                    sequence=2,
                    checkpoint_id="fault_observed",
                    observed_at=2.2,
                    state="fault-active",
                    receipt=f"{execution_id}:fault-observed",
                ),
                _checkpoint(
                    sequence=3,
                    checkpoint_id="recovered",
                    observed_at=3.2,
                    state="fault-recovered",
                    receipt=f"{execution_id}:fault-recovered",
                ),
            ],
        ),
        _requirement_row(
            sequence=2,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="backlog-drain",
            requirement_type="backlog_drain",
            operation_id="messages.list",
            fault_id="shared-429",
            operation_hash=operation_hashes[2],
            checkpoints=[],
            backlog=backlog,
        ),
        _requirement_row(
            sequence=3,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="cursor-hold",
            requirement_type="cursor_hold_then_resume",
            operation_id="messages.list",
            fault_id="shared-429",
            operation_hash=operation_hashes[2],
            checkpoints=[],
            cursor=cursor,
        ),
        _requirement_row(
            sequence=4,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="topology",
            requirement_type="topology_participation",
            operation_id=None,
            fault_id=None,
            operation_hash=None,
            checkpoints=[
                _checkpoint(
                    sequence=1,
                    checkpoint_id="topology_observed",
                    observed_at=4.6,
                    state="two-tenant-two-replica-topology",
                    receipt=f"{execution_id}:topology",
                )
            ],
        ),
    ]
    return seal_scenario_execution_artifact(
        {
            "schema_version": SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION,
            "source_id": source_id,
            "scenario_id": scenario_id,
            "executable_sha256": executable_sha256,
            "scenario_contract": contract,
            "scenario_contract_sha256": contract_sha256,
            "execution": {
                "execution_id": execution_id,
                "context_sha256": context_sha256,
                "started_at": _at(0),
                "completed_at": _at(10),
            },
            "infrastructure": infrastructure,
            "topology": topology,
            "operation_evidence": operations,
            "fault_ledger": [fault],
            "requirement_evidence": requirements,
        }
    )


def _non_fault_states(requirement_type: str) -> tuple[str, ...]:
    return {
        "pagination_resume": ("page-1", "cursor-1", "page-2"),
        "lifecycle_transition": (
            "absent",
            "created",
            "updated",
            "absent",
        ),
        "ordering_convergence": ("converged", "converged"),
        "declared_absence": ("unsupported-declaration", "rejected-request"),
        "renewal": ("old-watch", "new-watch", "new-watch"),
    }.get(requirement_type, ())


def _single_topology(label: str) -> dict[str, object]:
    return {
        "tenant_ids": ["tenant-a"],
        "installations": [
            {"tenant_id": "tenant-a", "installation_id": "install-a"}
        ],
        "replicas": [
            {
                "replica_id": "replica-a",
                "process_identity_sha256": _digest(f"{label}:replica-a"),
            }
        ],
    }


def _non_fault_artifact(
    requirement_type: str,
    *,
    operation_linked: bool = True,
) -> dict[str, object]:
    source_id = "google_calendar"
    scenario_id = f"test_{requirement_type}"
    execution_id = f"execution-{requirement_type}"
    executable_sha256 = _digest(f"executable:{requirement_type}")
    operation_id = "events.list" if operation_linked else None
    requirements = [
        {
            "requirement_id": "subject",
            "requirement_type": requirement_type,
            "operation_id": operation_id,
            "fault_id": None,
            "checkpoint_ids": list(CHECKPOINT_IDS[requirement_type]),
        },
        {
            "requirement_id": "topology",
            "requirement_type": "topology_participation",
            "operation_id": None,
            "fault_id": None,
            "checkpoint_ids": ["topology_observed"],
        },
    ]
    contract = {
        "schema_version": SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION,
        "source_id": source_id,
        "scenario_id": scenario_id,
        "required_operation_ids": ["events.list"],
        "required_faults": [],
        "requirements": requirements,
    }
    contract_sha256 = scenario_contract_sha256(contract)
    infrastructure = _infrastructure(requirement_type)
    topology = _single_topology(requirement_type)
    context_sha256 = execution_context_sha256(
        source_id=source_id,
        scenario_id=scenario_id,
        executable_sha256=executable_sha256,
        scenario_contract_sha256=contract_sha256,
        execution_id=execution_id,
        infrastructure=infrastructure,
        topology=topology,
    )
    operation = _record(
        {
            "sequence": 1,
            "execution_id": execution_id,
            "context_sha256": context_sha256,
            "operation_id": "events.list",
            "tenant_id": "tenant-a",
            "installation_id": "install-a",
            "replica_id": "replica-a",
            "attempt": 1,
            "phase": "checkpoint",
            "outcome": (
                "rejected_as_declared"
                if requirement_type == "declared_absence"
                else "succeeded"
            ),
            "started_at": _at(1),
            "completed_at": _at(1.5),
            "request_sha256": _digest(
                f"{requirement_type}:request",
            ),
            "response_sha256": _digest(
                f"{requirement_type}:response",
            ),
        }
    )
    states = _non_fault_states(requirement_type)
    checkpoints = [
        _checkpoint(
            sequence=index,
            checkpoint_id=checkpoint_id,
            observed_at=2 + index,
            state=states[index - 1],
            receipt=f"{requirement_type}:{checkpoint_id}",
        )
        for index, checkpoint_id in enumerate(
            CHECKPOINT_IDS[requirement_type],
            start=1,
        )
    ]
    backlog = None
    cursor = None
    if requirement_type == "backlog_drain":
        backlog = {
            "baseline_count": 1,
            "peak_count": 4,
            "recovered_count": 1,
            "baseline_observed_at": _at(2),
            "peak_observed_at": _at(3),
            "recovered_observed_at": _at(4),
            "sample_sha256": _digest("scenario-level-backlog"),
        }
    if requirement_type == "cursor_hold_then_resume":
        cursor = {
            "before_sha256": _digest("cursor-v1"),
            "held_sha256": _digest("cursor-v1"),
            "after_sha256": _digest("cursor-v2"),
            "before_observed_at": _at(2),
            "held_observed_at": _at(3),
            "after_observed_at": _at(4),
            "sample_sha256": _digest("scenario-level-cursor"),
        }
    operation_hash = (
        str(operation["evidence_sha256"]) if operation_linked else None
    )
    requirement_evidence = [
        _requirement_row(
            sequence=1,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="subject",
            requirement_type=requirement_type,
            operation_id=operation_id,
            fault_id=None,
            operation_hash=operation_hash,
            checkpoints=checkpoints,
            backlog=backlog,
            cursor=cursor,
        ),
        _requirement_row(
            sequence=2,
            execution_id=execution_id,
            context_sha256=context_sha256,
            requirement_id="topology",
            requirement_type="topology_participation",
            operation_id=None,
            fault_id=None,
            operation_hash=None,
            checkpoints=[
                _checkpoint(
                    sequence=1,
                    checkpoint_id="topology_observed",
                    observed_at=8,
                    state="single-topology-observed",
                    receipt=f"{requirement_type}:topology",
                )
            ],
        ),
    ]
    return seal_scenario_execution_artifact(
        {
            "schema_version": SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION,
            "source_id": source_id,
            "scenario_id": scenario_id,
            "executable_sha256": executable_sha256,
            "scenario_contract": contract,
            "scenario_contract_sha256": contract_sha256,
            "execution": {
                "execution_id": execution_id,
                "context_sha256": context_sha256,
                "started_at": _at(0),
                "completed_at": _at(10),
            },
            "infrastructure": infrastructure,
            "topology": topology,
            "operation_evidence": [operation],
            "fault_ledger": [],
            "requirement_evidence": requirement_evidence,
        }
    )


def _validate(artifact: dict[str, object]):
    return validate_scenario_execution_artifact(
        artifact,
        expected_source_id=str(artifact["source_id"]),
        expected_scenario_id=str(artifact["scenario_id"]),
        expected_executable_sha256=str(artifact["executable_sha256"]),
        expected_contract_sha256=str(
            artifact["scenario_contract_sha256"]
        ),
        expected_execution_context_sha256=str(
            artifact["execution"]["context_sha256"]  # type: ignore[index]
        ),
    )


def test_complete_fault_proof_derives_pass_without_caller_state() -> None:
    artifact = _fault_artifact()

    verification = _validate(artifact)

    assert verification.state == "passed"
    assert verification.promotion_eligible is True
    assert verification.requirement_ids == (
        "429-recovery",
        "backlog-drain",
        "cursor-hold",
        "topology",
    )
    assert "state" not in artifact
    assert "promotion_eligible" not in artifact


@pytest.mark.parametrize(
    "requirement_type",
    (
        "pagination_resume",
        "lifecycle_transition",
        "ordering_convergence",
        "declared_absence",
        "renewal",
    ),
)
def test_non_fault_typed_checkpoint_protocol_derives_pass(
    requirement_type: str,
) -> None:
    artifact = _non_fault_artifact(requirement_type)

    verification = _validate(artifact)

    assert verification.state == "passed"
    assert verification.requirement_ids == ("subject", "topology")
    assert artifact["fault_ledger"] == []


@pytest.mark.parametrize(
    "requirement_type",
    ("backlog_drain", "cursor_hold_then_resume"),
)
def test_scenario_level_invariants_do_not_require_a_fault(
    requirement_type: str,
) -> None:
    artifact = _non_fault_artifact(
        requirement_type,
        operation_linked=False,
    )

    verification = _validate(artifact)

    assert verification.state == "passed"
    assert artifact["scenario_contract"]["required_faults"] == []  # type: ignore[index]


def test_caller_cannot_supply_promotion_boolean() -> None:
    artifact = _fault_artifact()
    artifact["promotion_eligible"] = True
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="fields differ",
    ):
        _validate(artifact)


def test_artifact_self_hash_detects_unsealed_tampering() -> None:
    artifact = _fault_artifact()
    artifact["scenario_id"] = "different-scenario"

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="self hash differs",
    ):
        _validate(artifact)


def test_rehashed_splice_from_an_unrelated_execution_is_rejected() -> None:
    artifact = _fault_artifact(execution_id="execution-a")
    unrelated = _fault_artifact(execution_id="execution-b")
    unrelated_observer = copy.deepcopy(unrelated["operation_evidence"][1])  # type: ignore[index]
    artifact["operation_evidence"][1] = unrelated_observer  # type: ignore[index]
    fault = artifact["fault_ledger"][0]  # type: ignore[index]
    fault["observer_operation_evidence_sha256"] = unrelated_observer[  # type: ignore[index]
        "evidence_sha256"
    ]
    _rehash_record(fault)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="unrelated execution context",
    ):
        _validate(artifact)


def test_fault_cannot_reference_an_unrelated_control_operation() -> None:
    artifact = _fault_artifact()
    fault = artifact["fault_ledger"][0]  # type: ignore[index]
    control = artifact["operation_evidence"][3]  # type: ignore[index]
    fault["observer_operation_evidence_sha256"] = control[  # type: ignore[index]
        "evidence_sha256"
    ]
    _rehash_record(fault)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="does not link inject/observe/recover",
    ):
        _validate(artifact)


def test_rehashed_semantic_tamper_cannot_fake_backlog_drain() -> None:
    artifact = _fault_artifact()
    evidence = artifact["requirement_evidence"][1]  # type: ignore[index]
    evidence["backlog"]["recovered_count"] = 1  # type: ignore[index]
    _rehash_record(evidence)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="rise then drain to baseline",
    ):
        _validate(artifact)


def test_rehashed_semantic_tamper_cannot_fake_cursor_hold() -> None:
    artifact = _fault_artifact()
    evidence = artifact["requirement_evidence"][2]  # type: ignore[index]
    evidence["cursor"]["held_sha256"] = _digest("cursor-advanced")  # type: ignore[index]
    _rehash_record(evidence)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="hold then advance",
    ):
        _validate(artifact)


def test_requirement_evidence_must_cover_the_exact_contract() -> None:
    artifact = _fault_artifact()
    artifact["requirement_evidence"].pop()  # type: ignore[union-attr]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="exact contract",
    ):
        _validate(artifact)


def test_each_fault_requires_exactly_one_recovery_requirement() -> None:
    artifact = _fault_artifact()
    contract = copy.deepcopy(artifact["scenario_contract"])
    duplicate = copy.deepcopy(contract["requirements"][0])
    duplicate["requirement_id"] = "duplicate-429-recovery"
    contract["requirements"].append(duplicate)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="exactly one fault_recovery",
    ):
        scenario_contract_sha256(contract)


def test_fault_checkpoints_must_match_the_linked_fault_timeline() -> None:
    artifact = _fault_artifact()
    evidence = artifact["requirement_evidence"][0]  # type: ignore[index]
    evidence["checkpoints"][0]["observed_at"] = _at(1.3)  # type: ignore[index]
    _rehash_record(evidence)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="timestamps differ from its fault ledger",
    ):
        _validate(artifact)


def test_fault_invariants_must_link_the_recovery_operation() -> None:
    artifact = _fault_artifact()
    evidence = artifact["requirement_evidence"][1]  # type: ignore[index]
    injected = artifact["operation_evidence"][0]  # type: ignore[index]
    evidence["operation_evidence_sha256"] = injected["evidence_sha256"]  # type: ignore[index]
    _rehash_record(evidence)  # type: ignore[arg-type]
    _rehash_artifact(artifact)

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="not linked to the fault recovery operation",
    ):
        _validate(artifact)


def test_expected_executable_and_context_are_independent_receipt_inputs() -> None:
    artifact = _fault_artifact()
    kwargs = {
        "expected_source_id": str(artifact["source_id"]),
        "expected_scenario_id": str(artifact["scenario_id"]),
        "expected_contract_sha256": str(
            artifact["scenario_contract_sha256"]
        ),
        "expected_execution_context_sha256": str(
            artifact["execution"]["context_sha256"]  # type: ignore[index]
        ),
    }

    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="executable differs",
    ):
        validate_scenario_execution_artifact(
            artifact,
            expected_executable_sha256=_digest("different-executable"),
            **kwargs,
        )
    with pytest.raises(
        ScenarioExecutionArtifactError,
        match="context differs",
    ):
        validate_scenario_execution_artifact(
            artifact,
            expected_executable_sha256=str(
                artifact["executable_sha256"]
            ),
            expected_source_id=kwargs["expected_source_id"],
            expected_scenario_id=kwargs["expected_scenario_id"],
            expected_contract_sha256=kwargs["expected_contract_sha256"],
            expected_execution_context_sha256=_digest(
                "different-context"
            ),
        )
