"""Strict, dependency-light evidence protocol for one certification scenario.

The artifact contains no caller-controlled pass or promotion flag.  A verifier
derives that result only after checking an independently expected source,
scenario, executable digest, scenario-contract digest, and execution-context
digest, plus exact coverage of every typed requirement.

Every evidence record carries the same execution binding and its own content
hash.  Faults reference exact operation-record hashes.  This rejects splicing
otherwise valid records from unrelated probes.  The hashes are tamper
evidence, not authorship; a command receipt must still bind the final artifact
hash to the executable that produced it.

This module intentionally imports no catalog, driver, provider, or
infrastructure code.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal


SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION = (
    "fyralis.source-certification-scenario-execution.v1"
)
SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION = (
    "fyralis.source-certification-scenario-contract.v1"
)
SCENARIO_EXECUTION_ISOLATION_ACK = (
    "dedicated-loopback-scenario-execution-v1"
)

RequirementType = Literal[
    "operation_execution",
    "fault_recovery",
    "backlog_drain",
    "cursor_hold_then_resume",
    "pagination_resume",
    "lifecycle_transition",
    "ordering_convergence",
    "declared_absence",
    "renewal",
    "topology_participation",
]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CHECKPOINT_IDS: Mapping[str, tuple[str, ...]] = {
    "operation_execution": ("completed",),
    "fault_recovery": ("fault_injected", "fault_observed", "recovered"),
    "backlog_drain": (),
    "cursor_hold_then_resume": (),
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
    "topology_participation": ("topology_observed",),
}
_BODY_FIELDS = frozenset(
    {
        "schema_version",
        "source_id",
        "scenario_id",
        "executable_sha256",
        "scenario_contract",
        "scenario_contract_sha256",
        "execution",
        "infrastructure",
        "topology",
        "operation_evidence",
        "fault_ledger",
        "requirement_evidence",
    }
)
_CONTRACT_FIELDS = frozenset(
    {
        "schema_version",
        "source_id",
        "scenario_id",
        "required_operation_ids",
        "required_faults",
        "requirements",
    }
)
_FAULT_CONTRACT_FIELDS = frozenset(
    {"fault_id", "fault_kind", "operation_id", "observer_scope"}
)
_REQUIREMENT_FIELDS = frozenset(
    {
        "requirement_id",
        "requirement_type",
        "operation_id",
        "fault_id",
        "checkpoint_ids",
    }
)
_EXECUTION_FIELDS = frozenset(
    {"execution_id", "context_sha256", "started_at", "completed_at"}
)
_INFRASTRUCTURE_FIELDS = frozenset(
    {
        "isolation_ack",
        "network_scope",
        "database_identity_sha256",
        "kafka_cluster_identity_sha256",
        "object_store_identity_sha256",
        "redis_identity_sha256",
        "provider_lab_identity_sha256",
    }
)
_TOPOLOGY_FIELDS = frozenset({"tenant_ids", "installations", "replicas"})
_INSTALLATION_FIELDS = frozenset({"tenant_id", "installation_id"})
_REPLICA_FIELDS = frozenset({"replica_id", "process_identity_sha256"})
_OPERATION_FIELDS = frozenset(
    {
        "sequence",
        "execution_id",
        "context_sha256",
        "operation_id",
        "tenant_id",
        "installation_id",
        "replica_id",
        "attempt",
        "phase",
        "outcome",
        "started_at",
        "completed_at",
        "request_sha256",
        "response_sha256",
        "evidence_sha256",
    }
)
_FAULT_FIELDS = frozenset(
    {
        "sequence",
        "execution_id",
        "context_sha256",
        "fault_id",
        "fault_kind",
        "operation_id",
        "injected_operation_evidence_sha256",
        "observer_operation_evidence_sha256",
        "recovery_operation_evidence_sha256",
        "injected_at",
        "observed_at",
        "recovered_at",
        "evidence_sha256",
    }
)
_REQUIREMENT_EVIDENCE_FIELDS = frozenset(
    {
        "sequence",
        "execution_id",
        "context_sha256",
        "requirement_id",
        "requirement_type",
        "operation_id",
        "fault_id",
        "operation_evidence_sha256",
        "checkpoints",
        "backlog",
        "cursor",
        "evidence_sha256",
    }
)
_CHECKPOINT_FIELDS = frozenset(
    {
        "sequence",
        "checkpoint_id",
        "observed_at",
        "state_sha256",
        "receipt_sha256",
    }
)
_BACKLOG_FIELDS = frozenset(
    {
        "baseline_count",
        "peak_count",
        "recovered_count",
        "baseline_observed_at",
        "peak_observed_at",
        "recovered_observed_at",
        "sample_sha256",
    }
)
_CURSOR_FIELDS = frozenset(
    {
        "before_sha256",
        "held_sha256",
        "after_sha256",
        "before_observed_at",
        "held_observed_at",
        "after_observed_at",
        "sample_sha256",
    }
)


class ScenarioExecutionArtifactError(ValueError):
    """Scenario evidence is malformed, incomplete, or unrelated."""


@dataclass(frozen=True, slots=True)
class ScenarioExecutionVerification:
    """A pass derived by the verifier, never asserted by artifact input."""

    source_id: str
    scenario_id: str
    artifact_sha256: str
    requirement_ids: tuple[str, ...]
    state: Literal["passed"] = "passed"
    promotion_eligible: bool = True


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ScenarioExecutionArtifactError(f"{field} must be an object")
    return value


def _sequence(value: object, field: str) -> Sequence[Any]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes, bytearray))
    ):
        raise ScenarioExecutionArtifactError(f"{field} must be an array")
    return value


def _exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
    field: str,
) -> None:
    missing = expected - value.keys()
    extra = value.keys() - expected
    if not missing and not extra:
        return
    details: list[str] = []
    if missing:
        details.append(f"missing {sorted(missing)!r}")
    if extra:
        details.append(f"unknown {sorted(extra)!r}")
    raise ScenarioExecutionArtifactError(
        f"{field} fields differ: {', '.join(details)}"
    )


def _label(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 256
        or any(ord(character) < 32 for character in value)
    ):
        raise ScenarioExecutionArtifactError(
            f"{field} must be a non-empty canonical string"
        )
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ScenarioExecutionArtifactError(
            f"{field} must be a lowercase SHA-256 digest"
        )
    return value


def _optional_digest(value: object, field: str) -> str | None:
    return None if value is None else _digest(value, field)


def _optional_label(value: object, field: str) -> str | None:
    return None if value is None else _label(value, field)


def _integer(value: object, field: str, *, positive: bool) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or (positive and value < 1)
        or (not positive and value < 0)
    ):
        qualifier = "positive" if positive else "non-negative"
        raise ScenarioExecutionArtifactError(
            f"{field} must be a {qualifier} integer"
        )
    return value


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ScenarioExecutionArtifactError(
            f"{field} must be an ISO-8601 timestamp"
        )
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ScenarioExecutionArtifactError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ScenarioExecutionArtifactError(
            f"{field} must be timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _inside(
    value: datetime,
    start: datetime,
    end: datetime,
    field: str,
) -> None:
    if value < start or value > end:
        raise ScenarioExecutionArtifactError(
            f"{field} falls outside its receipt window"
        )


def _canonical_bytes(value: object) -> bytes:
    try:
        rendered = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ScenarioExecutionArtifactError(
            "artifact must contain only canonical JSON values"
        ) from exc
    return (rendered + "\n").encode("utf-8")


def _value_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def evidence_record_sha256(value: Mapping[str, Any]) -> str:
    """Hash one evidence record without its own ``evidence_sha256`` field."""

    record = dict(_mapping(value, "evidence record"))
    record.pop("evidence_sha256", None)
    return _value_sha256(record)


def _record_hash(value: Mapping[str, Any], field: str) -> str:
    digest = _digest(value.get("evidence_sha256"), f"{field}.evidence_sha256")
    if evidence_record_sha256(value) != digest:
        raise ScenarioExecutionArtifactError(
            f"{field}.evidence_sha256 differs"
        )
    return digest


def _checkpoint_ids(value: object, field: str) -> tuple[str, ...]:
    return tuple(
        _label(item, f"{field}[{index}]")
        for index, item in enumerate(_sequence(value, field))
    )


def _validate_fault_contracts(
    value: object,
    operations: tuple[str, ...],
) -> tuple[Mapping[str, Any], ...]:
    faults: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(_sequence(value, "scenario_contract.required_faults")):
        field = f"scenario_contract.required_faults[{index}]"
        fault = _mapping(raw, field)
        _exact_fields(fault, _FAULT_CONTRACT_FIELDS, field)
        fault_id = _label(fault.get("fault_id"), f"{field}.fault_id")
        _label(fault.get("fault_kind"), f"{field}.fault_kind")
        operation_id = _label(
            fault.get("operation_id"),
            f"{field}.operation_id",
        )
        if operation_id not in operations:
            raise ScenarioExecutionArtifactError(
                f"{field}.operation_id is not a required operation"
            )
        if fault.get("observer_scope") not in {
            "same_replica",
            "cross_replica",
        }:
            raise ScenarioExecutionArtifactError(
                f"{field}.observer_scope is invalid"
            )
        if fault_id in seen:
            raise ScenarioExecutionArtifactError(
                "required_faults must have unique fault_id values"
            )
        seen.add(fault_id)
        faults.append(fault)
    return tuple(faults)


def _validate_requirement_contracts(
    value: object,
    operations: tuple[str, ...],
    faults: tuple[Mapping[str, Any], ...],
) -> tuple[Mapping[str, Any], ...]:
    requirements: list[Mapping[str, Any]] = []
    ids: set[str] = set()
    fault_by_id = {str(item["fault_id"]): item for item in faults}
    fault_recovery_ids: list[str] = []
    for index, raw in enumerate(_sequence(value, "scenario_contract.requirements")):
        field = f"scenario_contract.requirements[{index}]"
        requirement = _mapping(raw, field)
        _exact_fields(requirement, _REQUIREMENT_FIELDS, field)
        requirement_id = _label(
            requirement.get("requirement_id"),
            f"{field}.requirement_id",
        )
        requirement_type = requirement.get("requirement_type")
        if requirement_type not in _CHECKPOINT_IDS:
            raise ScenarioExecutionArtifactError(
                f"{field}.requirement_type is unsupported"
            )
        operation_id = _optional_label(
            requirement.get("operation_id"),
            f"{field}.operation_id",
        )
        fault_id = _optional_label(
            requirement.get("fault_id"),
            f"{field}.fault_id",
        )
        if operation_id is not None and operation_id not in operations:
            raise ScenarioExecutionArtifactError(
                f"{field}.operation_id is not required"
            )
        if fault_id is not None and fault_id not in fault_by_id:
            raise ScenarioExecutionArtifactError(
                f"{field}.fault_id is not required"
            )
        if fault_id is not None:
            fault_operation = fault_by_id[fault_id]["operation_id"]
            if operation_id is not None and operation_id != fault_operation:
                raise ScenarioExecutionArtifactError(
                    f"{field} crosses fault operation ownership"
                )
        operation_required = requirement_type in {
            "operation_execution",
            "fault_recovery",
            "pagination_resume",
            "lifecycle_transition",
            "ordering_convergence",
            "declared_absence",
            "renewal",
        }
        operation_optional = requirement_type in {
            "backlog_drain",
            "cursor_hold_then_resume",
        }
        if (
            operation_required
            and operation_id is None
            or not operation_required
            and not operation_optional
            and operation_id is not None
        ):
            raise ScenarioExecutionArtifactError(
                f"{field}.operation_id disagrees with its requirement type"
            )
        if requirement_type == "fault_recovery":
            if fault_id is None:
                raise ScenarioExecutionArtifactError(
                    f"{field}.fault_id is required for fault recovery"
                )
            fault_recovery_ids.append(fault_id)
        elif requirement_type not in {
            "backlog_drain",
            "cursor_hold_then_resume",
        } and fault_id is not None:
            raise ScenarioExecutionArtifactError(
                f"{field}.fault_id is invalid for its requirement type"
            )
        expected_checkpoints = _CHECKPOINT_IDS[str(requirement_type)]
        actual_checkpoints = _checkpoint_ids(
            requirement.get("checkpoint_ids"),
            f"{field}.checkpoint_ids",
        )
        if actual_checkpoints != expected_checkpoints:
            raise ScenarioExecutionArtifactError(
                f"{field}.checkpoint_ids differ from the typed protocol"
            )
        if requirement_id in ids:
            raise ScenarioExecutionArtifactError(
                "requirements must have unique requirement_id values"
            )
        ids.add(requirement_id)
        requirements.append(requirement)
    if not requirements:
        raise ScenarioExecutionArtifactError(
            "scenario_contract.requirements must not be empty"
        )
    if (
        len(fault_recovery_ids) != len(fault_by_id)
        or set(fault_recovery_ids) != set(fault_by_id)
    ):
        raise ScenarioExecutionArtifactError(
            "every required fault needs exactly one fault_recovery requirement"
        )
    return tuple(requirements)


def _validate_contract(
    contract: Mapping[str, Any],
) -> tuple[
    tuple[str, ...],
    tuple[Mapping[str, Any], ...],
    tuple[Mapping[str, Any], ...],
]:
    _exact_fields(contract, _CONTRACT_FIELDS, "scenario_contract")
    if contract.get("schema_version") != (
        SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION
    ):
        raise ScenarioExecutionArtifactError(
            "scenario_contract schema_version is unsupported"
        )
    _label(contract.get("source_id"), "scenario_contract.source_id")
    _label(contract.get("scenario_id"), "scenario_contract.scenario_id")
    operations = _checkpoint_ids(
        contract.get("required_operation_ids"),
        "scenario_contract.required_operation_ids",
    )
    if not operations or len(operations) != len(set(operations)):
        raise ScenarioExecutionArtifactError(
            "required_operation_ids must be non-empty and unique"
        )
    faults = _validate_fault_contracts(
        contract.get("required_faults"),
        operations,
    )
    requirements = _validate_requirement_contracts(
        contract.get("requirements"),
        operations,
        faults,
    )
    return operations, faults, requirements


def scenario_contract_sha256(value: Mapping[str, Any]) -> str:
    """Validate and hash the exact contract expected by a verifier."""

    contract = _mapping(value, "scenario_contract")
    _validate_contract(contract)
    return _value_sha256(contract)


def _validate_infrastructure(value: Mapping[str, Any]) -> None:
    _exact_fields(value, _INFRASTRUCTURE_FIELDS, "infrastructure")
    if value.get("isolation_ack") != SCENARIO_EXECUTION_ISOLATION_ACK:
        raise ScenarioExecutionArtifactError(
            "infrastructure isolation_ack is invalid"
        )
    if value.get("network_scope") != "loopback":
        raise ScenarioExecutionArtifactError(
            "scenario execution infrastructure must be loopback"
        )
    for name in sorted(
        _INFRASTRUCTURE_FIELDS - {"isolation_ack", "network_scope"}
    ):
        _digest(value.get(name), f"infrastructure.{name}")


def _validate_topology(
    value: Mapping[str, Any],
) -> tuple[tuple[str, ...], dict[str, str], tuple[str, ...]]:
    _exact_fields(value, _TOPOLOGY_FIELDS, "topology")
    tenants = _checkpoint_ids(value.get("tenant_ids"), "topology.tenant_ids")
    if (
        not tenants
        or len(tenants) != len(set(tenants))
        or tenants != tuple(sorted(tenants))
    ):
        raise ScenarioExecutionArtifactError(
            "tenant_ids must be non-empty, unique, and sorted"
        )
    installations: dict[str, str] = {}
    order: list[tuple[str, str]] = []
    for index, raw in enumerate(_sequence(value.get("installations"), "installations")):
        field = f"topology.installations[{index}]"
        item = _mapping(raw, field)
        _exact_fields(item, _INSTALLATION_FIELDS, field)
        tenant_id = _label(item.get("tenant_id"), f"{field}.tenant_id")
        installation_id = _label(
            item.get("installation_id"),
            f"{field}.installation_id",
        )
        if tenant_id not in tenants or installation_id in installations:
            raise ScenarioExecutionArtifactError(
                f"{field} is duplicate or belongs to an unknown tenant"
            )
        installations[installation_id] = tenant_id
        order.append((tenant_id, installation_id))
    if (
        not installations
        or order != sorted(order)
        or set(installations.values()) != set(tenants)
    ):
        raise ScenarioExecutionArtifactError(
            "installations must be sorted and cover every tenant"
        )
    replicas: list[str] = []
    for index, raw in enumerate(_sequence(value.get("replicas"), "replicas")):
        field = f"topology.replicas[{index}]"
        item = _mapping(raw, field)
        _exact_fields(item, _REPLICA_FIELDS, field)
        replicas.append(_label(item.get("replica_id"), f"{field}.replica_id"))
        _digest(
            item.get("process_identity_sha256"),
            f"{field}.process_identity_sha256",
        )
    if (
        not replicas
        or len(replicas) != len(set(replicas))
        or replicas != sorted(replicas)
    ):
        raise ScenarioExecutionArtifactError(
            "replicas must be non-empty, unique, and sorted"
        )
    return tenants, installations, tuple(replicas)


def execution_context_sha256(
    *,
    source_id: str,
    scenario_id: str,
    executable_sha256: str,
    scenario_contract_sha256: str,
    execution_id: str,
    infrastructure: Mapping[str, Any],
    topology: Mapping[str, Any],
) -> str:
    """Hash immutable execution identity shared by every evidence record."""

    return _value_sha256(
        {
            "source_id": _label(source_id, "source_id"),
            "scenario_id": _label(scenario_id, "scenario_id"),
            "executable_sha256": _digest(
                executable_sha256,
                "executable_sha256",
            ),
            "scenario_contract_sha256": _digest(
                scenario_contract_sha256,
                "scenario_contract_sha256",
            ),
            "execution_id": _label(execution_id, "execution.execution_id"),
            "infrastructure": dict(infrastructure),
            "topology": dict(topology),
        }
    )


def _bound(
    row: Mapping[str, Any],
    execution_id: str,
    context_sha256: str,
    field: str,
) -> None:
    if (
        row.get("execution_id") != execution_id
        or row.get("context_sha256") != context_sha256
    ):
        raise ScenarioExecutionArtifactError(
            f"{field} belongs to an unrelated execution context"
        )


def _validate_operations(
    value: object,
    *,
    execution_id: str,
    context_sha256: str,
    required_operations: tuple[str, ...],
    tenants: tuple[str, ...],
    installations: Mapping[str, str],
    replicas: tuple[str, ...],
    started_at: datetime,
    completed_at: datetime,
) -> dict[str, Mapping[str, Any]]:
    by_hash: dict[str, Mapping[str, Any]] = {}
    seen_operations: set[str] = set()
    seen_tenants: set[str] = set()
    seen_installations: set[str] = set()
    seen_replicas: set[str] = set()
    previous_start: datetime | None = None
    allowed = {
        "control": {"succeeded"},
        "checkpoint": {"succeeded", "rejected_as_declared"},
        "inject": {"fault_observed"},
        "observe": {"fault_observed", "shared_state_blocked"},
        "recover": {"succeeded"},
    }
    rows = _sequence(value, "operation_evidence")
    for index, raw in enumerate(rows):
        field = f"operation_evidence[{index}]"
        row = _mapping(raw, field)
        _exact_fields(row, _OPERATION_FIELDS, field)
        if row.get("sequence") != index + 1:
            raise ScenarioExecutionArtifactError(
                "operation evidence sequence must be contiguous from 1"
            )
        _bound(row, execution_id, context_sha256, field)
        operation_id = _label(row.get("operation_id"), f"{field}.operation_id")
        tenant_id = _label(row.get("tenant_id"), f"{field}.tenant_id")
        installation_id = _label(
            row.get("installation_id"),
            f"{field}.installation_id",
        )
        replica_id = _label(row.get("replica_id"), f"{field}.replica_id")
        if operation_id not in required_operations:
            raise ScenarioExecutionArtifactError(
                f"{field}.operation_id is not required"
            )
        if (
            tenant_id not in tenants
            or installations.get(installation_id) != tenant_id
            or replica_id not in replicas
        ):
            raise ScenarioExecutionArtifactError(
                f"{field} is outside the declared topology"
            )
        _integer(row.get("attempt"), f"{field}.attempt", positive=True)
        phase = row.get("phase")
        outcome = row.get("outcome")
        if phase not in allowed or outcome not in allowed[phase]:
            raise ScenarioExecutionArtifactError(
                f"{field} phase/outcome combination is invalid"
            )
        if _optional_digest(
            row.get("request_sha256"),
            f"{field}.request_sha256",
        ) is None:
            raise ScenarioExecutionArtifactError(
                f"{field}.request_sha256 must bind request intent"
            )
        response = _optional_digest(
            row.get("response_sha256"),
            f"{field}.response_sha256",
        )
        if (outcome == "shared_state_blocked") != (response is None):
            raise ScenarioExecutionArtifactError(
                f"{field}.response_sha256 disagrees with outcome"
            )
        row_start = _timestamp(row.get("started_at"), f"{field}.started_at")
        row_end = _timestamp(row.get("completed_at"), f"{field}.completed_at")
        if row_end < row_start:
            raise ScenarioExecutionArtifactError(
                f"{field} completed before it started"
            )
        _inside(row_start, started_at, completed_at, f"{field}.started_at")
        _inside(row_end, started_at, completed_at, f"{field}.completed_at")
        if previous_start is not None and row_start < previous_start:
            raise ScenarioExecutionArtifactError(
                "operation evidence differs from start-time order"
            )
        previous_start = row_start
        record_hash = _record_hash(row, field)
        if record_hash in by_hash:
            raise ScenarioExecutionArtifactError(
                "operation evidence hashes must be unique"
            )
        by_hash[record_hash] = row
        seen_operations.add(operation_id)
        seen_tenants.add(tenant_id)
        seen_installations.add(installation_id)
        seen_replicas.add(replica_id)
    if seen_operations != set(required_operations):
        raise ScenarioExecutionArtifactError(
            "operation evidence does not cover every required operation"
        )
    if (
        seen_tenants != set(tenants)
        or seen_installations != set(installations)
        or seen_replicas != set(replicas)
    ):
        raise ScenarioExecutionArtifactError(
            "operation evidence does not exercise the exact topology"
        )
    return by_hash


def _operation_time(row: Mapping[str, Any], name: str) -> datetime:
    return _timestamp(row.get(name), f"operation.{name}")


def _validate_faults(
    value: object,
    *,
    execution_id: str,
    context_sha256: str,
    required_faults: tuple[Mapping[str, Any], ...],
    operations: Mapping[str, Mapping[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> dict[str, Mapping[str, Any]]:
    rows = _sequence(value, "fault_ledger")
    if len(rows) != len(required_faults):
        raise ScenarioExecutionArtifactError(
            "fault ledger differs from required faults"
        )
    by_id: dict[str, Mapping[str, Any]] = {}
    for index, (raw, contract) in enumerate(
        zip(rows, required_faults, strict=True)
    ):
        field = f"fault_ledger[{index}]"
        row = _mapping(raw, field)
        _exact_fields(row, _FAULT_FIELDS, field)
        if row.get("sequence") != index + 1:
            raise ScenarioExecutionArtifactError(
                "fault ledger sequence must be contiguous from 1"
            )
        _bound(row, execution_id, context_sha256, field)
        for name in ("fault_id", "fault_kind", "operation_id"):
            if row.get(name) != contract.get(name):
                raise ScenarioExecutionArtifactError(
                    f"{field}.{name} differs from the contract"
                )
        references = tuple(
            _digest(row.get(name), f"{field}.{name}")
            for name in (
                "injected_operation_evidence_sha256",
                "observer_operation_evidence_sha256",
                "recovery_operation_evidence_sha256",
            )
        )
        if len(set(references)) != 3:
            raise ScenarioExecutionArtifactError(
                f"{field} must reference three distinct operations"
            )
        try:
            injected, observer, recovery = (
                operations[reference] for reference in references
            )
        except KeyError as exc:
            raise ScenarioExecutionArtifactError(
                f"{field} references an operation from another probe"
            ) from exc
        if any(
            item.get("operation_id") != contract["operation_id"]
            for item in (injected, observer, recovery)
        ):
            raise ScenarioExecutionArtifactError(
                f"{field} combines unrelated operations"
            )
        if (
            (injected.get("phase"), injected.get("outcome"))
            != ("inject", "fault_observed")
            or observer.get("phase") != "observe"
            or (recovery.get("phase"), recovery.get("outcome"))
            != ("recover", "succeeded")
        ):
            raise ScenarioExecutionArtifactError(
                f"{field} does not link inject/observe/recover evidence"
            )
        scopes = {
            (item.get("tenant_id"), item.get("installation_id"))
            for item in (injected, observer, recovery)
        }
        if len(scopes) != 1:
            raise ScenarioExecutionArtifactError(
                f"{field} crosses tenant or installation scope"
            )
        if recovery.get("replica_id") != observer.get("replica_id"):
            raise ScenarioExecutionArtifactError(
                f"{field} recovery does not belong to the observer replica"
            )
        if contract["observer_scope"] == "cross_replica":
            if (
                observer.get("replica_id") == injected.get("replica_id")
                or observer.get("outcome") != "shared_state_blocked"
            ):
                raise ScenarioExecutionArtifactError(
                    f"{field} lacks cross-replica shared-state evidence"
                )
        elif observer.get("replica_id") != injected.get("replica_id"):
            raise ScenarioExecutionArtifactError(
                f"{field} observer scope differs from the contract"
            )
        sequences = [
            int(item["sequence"]) for item in (injected, observer, recovery)
        ]
        if sequences != sorted(sequences):
            raise ScenarioExecutionArtifactError(
                f"{field} operation references are out of order"
            )
        times = tuple(
            _timestamp(row.get(name), f"{field}.{name}")
            for name in ("injected_at", "observed_at", "recovered_at")
        )
        if tuple(sorted(times)) != times:
            raise ScenarioExecutionArtifactError(
                f"{field} timestamps are out of order"
            )
        for timestamp, name, operation in zip(
            times,
            ("injected_at", "observed_at", "recovered_at"),
            (injected, observer, recovery),
            strict=True,
        ):
            _inside(timestamp, started_at, completed_at, f"{field}.{name}")
            _inside(
                timestamp,
                _operation_time(operation, "started_at"),
                _operation_time(operation, "completed_at"),
                f"{field}.{name}",
            )
        _record_hash(row, field)
        by_id[str(row["fault_id"])] = row
    return by_id


def _validate_checkpoints(
    value: object,
    expected_ids: tuple[str, ...],
    *,
    field: str,
    started_at: datetime,
    completed_at: datetime,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[datetime, ...]]:
    rows = _sequence(value, field)
    if len(rows) != len(expected_ids):
        raise ScenarioExecutionArtifactError(
            f"{field} does not cover exact checkpoint IDs"
        )
    states: list[str] = []
    receipts: list[str] = []
    observation_times: list[datetime] = []
    previous: datetime | None = None
    for index, (raw, checkpoint_id) in enumerate(
        zip(rows, expected_ids, strict=True)
    ):
        item_field = f"{field}[{index}]"
        item = _mapping(raw, item_field)
        _exact_fields(item, _CHECKPOINT_FIELDS, item_field)
        if (
            item.get("sequence") != index + 1
            or item.get("checkpoint_id") != checkpoint_id
        ):
            raise ScenarioExecutionArtifactError(
                f"{item_field} identity differs from the contract"
            )
        observed = _timestamp(
            item.get("observed_at"),
            f"{item_field}.observed_at",
        )
        _inside(observed, started_at, completed_at, f"{item_field}.observed_at")
        if previous is not None and observed < previous:
            raise ScenarioExecutionArtifactError(
                f"{field} is not in observation-time order"
            )
        previous = observed
        observation_times.append(observed)
        states.append(
            _digest(item.get("state_sha256"), f"{item_field}.state_sha256")
        )
        receipts.append(
            _digest(
                item.get("receipt_sha256"),
                f"{item_field}.receipt_sha256",
            )
        )
    if len(receipts) != len(set(receipts)):
        raise ScenarioExecutionArtifactError(
            f"{field} reuses a checkpoint receipt"
        )
    return tuple(states), tuple(receipts), tuple(observation_times)


def _checkpoint_semantics(
    requirement_type: str,
    states: tuple[str, ...],
    field: str,
) -> None:
    if requirement_type == "ordering_convergence" and states[0] != states[1]:
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove ordered/out-of-order convergence"
        )
    if requirement_type == "pagination_resume" and states[0] == states[2]:
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove progress after resume"
        )
    if requirement_type == "lifecycle_transition" and not (
        states[0] != states[1]
        and states[1] != states[2]
        and states[0] == states[3]
    ):
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove create/update/delete-or-absence"
        )
    if requirement_type == "fault_recovery" and states[1] == states[2]:
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove recovery from the fault state"
        )
    if requirement_type == "renewal" and not (
        states[0] != states[1] and states[1] == states[2]
    ):
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove renewal persisted past expiry"
        )


def _validate_backlog(
    value: object,
    *,
    field: str,
    started_at: datetime,
    completed_at: datetime,
    fault: Mapping[str, Any] | None,
) -> None:
    item = _mapping(value, field)
    _exact_fields(item, _BACKLOG_FIELDS, field)
    baseline = _integer(
        item.get("baseline_count"),
        f"{field}.baseline_count",
        positive=False,
    )
    peak = _integer(
        item.get("peak_count"),
        f"{field}.peak_count",
        positive=False,
    )
    recovered = _integer(
        item.get("recovered_count"),
        f"{field}.recovered_count",
        positive=False,
    )
    if peak <= baseline or recovered != baseline:
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove rise then drain to baseline"
        )
    times = tuple(
        _timestamp(item.get(name), f"{field}.{name}")
        for name in (
            "baseline_observed_at",
            "peak_observed_at",
            "recovered_observed_at",
        )
    )
    if tuple(sorted(times)) != times:
        raise ScenarioExecutionArtifactError(
            f"{field} timestamps are out of order"
        )
    for timestamp, name in zip(
        times,
        (
            "baseline_observed_at",
            "peak_observed_at",
            "recovered_observed_at",
        ),
        strict=True,
    ):
        _inside(timestamp, started_at, completed_at, f"{field}.{name}")
    if fault is not None:
        injected = _timestamp(fault.get("injected_at"), "fault.injected_at")
        recovered_at = _timestamp(
            fault.get("recovered_at"),
            "fault.recovered_at",
        )
        if not (
            times[0] <= injected <= times[1]
            and times[2] >= recovered_at
        ):
            raise ScenarioExecutionArtifactError(
                f"{field} does not surround its linked fault"
            )
    _digest(item.get("sample_sha256"), f"{field}.sample_sha256")


def _validate_cursor(
    value: object,
    *,
    field: str,
    started_at: datetime,
    completed_at: datetime,
    fault: Mapping[str, Any] | None,
) -> None:
    item = _mapping(value, field)
    _exact_fields(item, _CURSOR_FIELDS, field)
    before = _digest(item.get("before_sha256"), f"{field}.before_sha256")
    held = _digest(item.get("held_sha256"), f"{field}.held_sha256")
    after = _digest(item.get("after_sha256"), f"{field}.after_sha256")
    if before != held or after == held:
        raise ScenarioExecutionArtifactError(
            f"{field} does not prove hold then advance"
        )
    times = tuple(
        _timestamp(item.get(name), f"{field}.{name}")
        for name in (
            "before_observed_at",
            "held_observed_at",
            "after_observed_at",
        )
    )
    if tuple(sorted(times)) != times:
        raise ScenarioExecutionArtifactError(
            f"{field} timestamps are out of order"
        )
    for timestamp, name in zip(
        times,
        (
            "before_observed_at",
            "held_observed_at",
            "after_observed_at",
        ),
        strict=True,
    ):
        _inside(timestamp, started_at, completed_at, f"{field}.{name}")
    if fault is not None:
        injected = _timestamp(fault.get("injected_at"), "fault.injected_at")
        recovered = _timestamp(fault.get("recovered_at"), "fault.recovered_at")
        if not (
            times[0] <= injected <= times[1] <= recovered <= times[2]
        ):
            raise ScenarioExecutionArtifactError(
                f"{field} does not surround its linked fault"
            )
    _digest(item.get("sample_sha256"), f"{field}.sample_sha256")


def _validate_requirements(
    value: object,
    *,
    execution_id: str,
    context_sha256: str,
    requirements: tuple[Mapping[str, Any], ...],
    operations: Mapping[str, Mapping[str, Any]],
    faults: Mapping[str, Mapping[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> tuple[str, ...]:
    rows = _sequence(value, "requirement_evidence")
    if len(rows) != len(requirements):
        raise ScenarioExecutionArtifactError(
            "requirement evidence does not cover the exact contract"
        )
    receipt_hashes: set[str] = set()
    requirement_ids: list[str] = []
    for index, (raw, contract) in enumerate(
        zip(rows, requirements, strict=True)
    ):
        field = f"requirement_evidence[{index}]"
        row = _mapping(raw, field)
        _exact_fields(row, _REQUIREMENT_EVIDENCE_FIELDS, field)
        if row.get("sequence") != index + 1:
            raise ScenarioExecutionArtifactError(
                "requirement evidence sequence must be contiguous from 1"
            )
        _bound(row, execution_id, context_sha256, field)
        for name in (
            "requirement_id",
            "requirement_type",
            "operation_id",
            "fault_id",
        ):
            if row.get(name) != contract.get(name):
                raise ScenarioExecutionArtifactError(
                    f"{field}.{name} differs from the contract"
                )
        requirement_type = str(contract["requirement_type"])
        operation_reference = _optional_digest(
            row.get("operation_evidence_sha256"),
            f"{field}.operation_evidence_sha256",
        )
        operation_id = contract.get("operation_id")
        if (operation_reference is None) != (operation_id is None):
            raise ScenarioExecutionArtifactError(
                f"{field} operation evidence linkage is incomplete"
            )
        if operation_reference is not None:
            try:
                operation = operations[operation_reference]
            except KeyError as exc:
                raise ScenarioExecutionArtifactError(
                    f"{field} references an operation from another probe"
                ) from exc
            if operation.get("operation_id") != operation_id:
                raise ScenarioExecutionArtifactError(
                    f"{field} references an unrelated operation"
                )
        fault_id = contract.get("fault_id")
        fault = faults.get(str(fault_id)) if fault_id is not None else None
        checkpoint_ids = tuple(contract["checkpoint_ids"])
        states, receipts, checkpoint_times = _validate_checkpoints(
            row.get("checkpoints"),
            checkpoint_ids,
            field=f"{field}.checkpoints",
            started_at=started_at,
            completed_at=completed_at,
        )
        if receipt_hashes.intersection(receipts):
            raise ScenarioExecutionArtifactError(
                "checkpoint receipts are reused across requirements"
            )
        receipt_hashes.update(receipts)
        _checkpoint_semantics(requirement_type, states, field)
        backlog = row.get("backlog")
        cursor = row.get("cursor")
        if requirement_type == "backlog_drain":
            if cursor is not None:
                raise ScenarioExecutionArtifactError(
                    f"{field}.cursor is invalid for backlog evidence"
                )
            _validate_backlog(
                backlog,
                field=f"{field}.backlog",
                started_at=started_at,
                completed_at=completed_at,
                fault=fault,
            )
        elif requirement_type == "cursor_hold_then_resume":
            if backlog is not None:
                raise ScenarioExecutionArtifactError(
                    f"{field}.backlog is invalid for cursor evidence"
                )
            _validate_cursor(
                cursor,
                field=f"{field}.cursor",
                started_at=started_at,
                completed_at=completed_at,
                fault=fault,
            )
        elif backlog is not None or cursor is not None:
            raise ScenarioExecutionArtifactError(
                f"{field} has invariants unrelated to its requirement type"
            )
        if requirement_type == "fault_recovery":
            assert fault is not None
            expected_recovery = fault["recovery_operation_evidence_sha256"]
            if operation_reference != expected_recovery:
                raise ScenarioExecutionArtifactError(
                    f"{field} is not linked to the fault recovery operation"
                )
            fault_times = tuple(
                _timestamp(fault.get(name), f"fault.{name}")
                for name in ("injected_at", "observed_at", "recovered_at")
            )
            if checkpoint_times != fault_times:
                raise ScenarioExecutionArtifactError(
                    f"{field} checkpoint timestamps differ from its fault ledger"
                )
        elif fault is not None and operation_reference is not None:
            expected_recovery = fault["recovery_operation_evidence_sha256"]
            if operation_reference != expected_recovery:
                raise ScenarioExecutionArtifactError(
                    f"{field} is not linked to the fault recovery operation"
                )
        _record_hash(row, field)
        requirement_ids.append(str(contract["requirement_id"]))
    return tuple(requirement_ids)


def _validate_body(
    body: Mapping[str, Any],
    *,
    expected_source_id: str | None = None,
    expected_scenario_id: str | None = None,
    expected_executable_sha256: str | None = None,
    expected_contract_sha256: str | None = None,
    expected_context_sha256: str | None = None,
) -> tuple[str, str, tuple[str, ...]]:
    _exact_fields(body, _BODY_FIELDS, "scenario execution artifact")
    if body.get("schema_version") != (
        SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION
    ):
        raise ScenarioExecutionArtifactError("unsupported artifact schema")
    source_id = _label(body.get("source_id"), "source_id")
    scenario_id = _label(body.get("scenario_id"), "scenario_id")
    executable_sha256 = _digest(
        body.get("executable_sha256"),
        "executable_sha256",
    )
    if expected_source_id is not None and source_id != expected_source_id:
        raise ScenarioExecutionArtifactError(
            "source differs from verifier expectation"
        )
    if expected_scenario_id is not None and scenario_id != expected_scenario_id:
        raise ScenarioExecutionArtifactError(
            "scenario differs from verifier expectation"
        )
    if (
        expected_executable_sha256 is not None
        and executable_sha256 != expected_executable_sha256
    ):
        raise ScenarioExecutionArtifactError(
            "executable differs from verifier expectation"
        )
    contract = _mapping(body.get("scenario_contract"), "scenario_contract")
    operations, fault_contracts, requirements = _validate_contract(contract)
    if (
        contract.get("source_id") != source_id
        or contract.get("scenario_id") != scenario_id
    ):
        raise ScenarioExecutionArtifactError(
            "scenario contract identity differs from artifact"
        )
    contract_sha256 = _digest(
        body.get("scenario_contract_sha256"),
        "scenario_contract_sha256",
    )
    if scenario_contract_sha256(contract) != contract_sha256:
        raise ScenarioExecutionArtifactError("scenario contract hash differs")
    if (
        expected_contract_sha256 is not None
        and contract_sha256 != expected_contract_sha256
    ):
        raise ScenarioExecutionArtifactError(
            "scenario contract differs from verifier expectation"
        )
    infrastructure = _mapping(body.get("infrastructure"), "infrastructure")
    topology = _mapping(body.get("topology"), "topology")
    _validate_infrastructure(infrastructure)
    tenants, installations, replicas = _validate_topology(topology)
    execution = _mapping(body.get("execution"), "execution")
    _exact_fields(execution, _EXECUTION_FIELDS, "execution")
    execution_id = _label(
        execution.get("execution_id"),
        "execution.execution_id",
    )
    context_sha256 = _digest(
        execution.get("context_sha256"),
        "execution.context_sha256",
    )
    actual_context = execution_context_sha256(
        source_id=source_id,
        scenario_id=scenario_id,
        executable_sha256=executable_sha256,
        scenario_contract_sha256=contract_sha256,
        execution_id=execution_id,
        infrastructure=infrastructure,
        topology=topology,
    )
    if context_sha256 != actual_context:
        raise ScenarioExecutionArtifactError("execution context hash differs")
    if (
        expected_context_sha256 is not None
        and context_sha256 != expected_context_sha256
    ):
        raise ScenarioExecutionArtifactError(
            "execution context differs from verifier expectation"
        )
    started_at = _timestamp(
        execution.get("started_at"),
        "execution.started_at",
    )
    completed_at = _timestamp(
        execution.get("completed_at"),
        "execution.completed_at",
    )
    if completed_at < started_at:
        raise ScenarioExecutionArtifactError(
            "execution completed before it started"
        )
    operation_rows = _validate_operations(
        body.get("operation_evidence"),
        execution_id=execution_id,
        context_sha256=context_sha256,
        required_operations=operations,
        tenants=tenants,
        installations=installations,
        replicas=replicas,
        started_at=started_at,
        completed_at=completed_at,
    )
    fault_rows = _validate_faults(
        body.get("fault_ledger"),
        execution_id=execution_id,
        context_sha256=context_sha256,
        required_faults=fault_contracts,
        operations=operation_rows,
        started_at=started_at,
        completed_at=completed_at,
    )
    requirement_ids = _validate_requirements(
        body.get("requirement_evidence"),
        execution_id=execution_id,
        context_sha256=context_sha256,
        requirements=requirements,
        operations=operation_rows,
        faults=fault_rows,
        started_at=started_at,
        completed_at=completed_at,
    )
    return source_id, scenario_id, requirement_ids


def seal_scenario_execution_artifact(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate an evidence body and add its canonical self hash."""

    body = copy.deepcopy(dict(_mapping(value, "scenario artifact body")))
    if "artifact_sha256" in body:
        raise ScenarioExecutionArtifactError(
            "artifact body must not already contain artifact_sha256"
        )
    _validate_body(body)
    body["artifact_sha256"] = _value_sha256(body)
    return body


def validate_scenario_execution_artifact(
    value: object,
    *,
    expected_source_id: str,
    expected_scenario_id: str,
    expected_executable_sha256: str,
    expected_contract_sha256: str,
    expected_execution_context_sha256: str,
) -> ScenarioExecutionVerification:
    """Derive a promotable pass only after every strict proof check succeeds."""

    artifact = _mapping(value, "scenario execution artifact")
    _exact_fields(
        artifact,
        _BODY_FIELDS | {"artifact_sha256"},
        "scenario execution artifact",
    )
    artifact_sha256 = _digest(
        artifact.get("artifact_sha256"),
        "artifact_sha256",
    )
    body = dict(artifact)
    del body["artifact_sha256"]
    if _value_sha256(body) != artifact_sha256:
        raise ScenarioExecutionArtifactError("artifact self hash differs")
    source_id, scenario_id, requirement_ids = _validate_body(
        body,
        expected_source_id=expected_source_id,
        expected_scenario_id=expected_scenario_id,
        expected_executable_sha256=expected_executable_sha256,
        expected_contract_sha256=expected_contract_sha256,
        expected_context_sha256=expected_execution_context_sha256,
    )
    return ScenarioExecutionVerification(
        source_id=source_id,
        scenario_id=scenario_id,
        artifact_sha256=artifact_sha256,
        requirement_ids=requirement_ids,
    )


__all__ = [
    "SCENARIO_EXECUTION_ARTIFACT_SCHEMA_VERSION",
    "SCENARIO_EXECUTION_CONTRACT_SCHEMA_VERSION",
    "SCENARIO_EXECUTION_ISOLATION_ACK",
    "ScenarioExecutionArtifactError",
    "ScenarioExecutionVerification",
    "evidence_record_sha256",
    "execution_context_sha256",
    "scenario_contract_sha256",
    "seal_scenario_execution_artifact",
    "validate_scenario_execution_artifact",
]
