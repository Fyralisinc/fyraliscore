"""Typed, fail-closed validation for executable certification artifacts.

``CertificationInput`` is intentionally a compact release summary.  It cannot
by itself prove that a command actually executed the scenarios it labels as
passing.  Every executed stage therefore emits a typed ``stage.json`` artifact
that is checked both when the producer accepts command output and when a
downloaded evidence bundle is replayed.

Version 3 deliberately permits only two kinds of positive claim families:

* the three isolated raw-to-T1/replay scenarios plus the historical-only
  2×2×2 topology scenarios emitted by the built-in local driver; and
* a real-provider canary backed by the request ledger defined below.

The built-in load and fault drivers remain diagnostic-only.  A future
release-capable implementation must introduce a new, explicitly validated
artifact schema rather than flipping a boolean in this one.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from services.ingest.source_certification.models import (
    CertificationInput,
    CertificationInvariantError,
    SourceCertificationSpec,
)
from services.ingest.source_certification.pipeline_load_runner import (
    PipelineLoadArtifactError,
    diagnostic_pipeline_load_config_from_suite,
    validate_pipeline_load_artifact,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_PROBE_SCHEMA_VERSION,
    PIPELINE_SCENARIO_IDS,
    PIPELINE_TOPOLOGY_SCENARIO_IDS,
    PipelineProbeError,
    pipeline_scenario_ids_for_source,
    validate_history_topology_proof,
    validate_replay_idempotency_proof,
)


STAGE_ARTIFACT_SCHEMA_VERSION = (
    "fyralis.source-certification-stage-artifact.v3"
)
CANARY_EXECUTION_SCHEMA_VERSION = (
    "fyralis.source-certification-canary-execution.v1"
)
RECEIPT_CLOCK_SKEW = timedelta(seconds=5)
Stage = Literal["local_correctness", "load", "fault_recovery", "canary"]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "source_id",
        "stage",
        "spec_hash",
        "execution_plan_sha256",
        "execution_plan",
        "generated_at",
        "synthetic_promotion_allowed",
        "claim_boundary",
    }
)
_STAGE_FIELDS: Mapping[str, frozenset[str]] = {
    "local_correctness": frozenset(
        {
            "fixture_and_binding_probe",
            "provider_lab_used_surface",
            "pipeline_probe",
            "scenario_execution_ledger",
        }
    ),
    "load": frozenset(
        {
            "load_diagnostic",
            "offered_load",
            "declared_load_suites",
            "pipeline_load_artifacts",
        }
    ),
    "fault_recovery": frozenset(
        {
            "fault_recovery_diagnostic",
            "declared_fault_targets",
        }
    ),
    "canary": frozenset(
        {
            "credential_environment_names_present",
            "credential_values_recorded",
            "real_provider_requests_sent",
            "canary_execution",
        }
    ),
}


class StageArtifactError(CertificationInvariantError):
    """A stage artifact does not support the claims in its result."""


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StageArtifactError(f"{field} must be an object")
    return value


def _sequence(value: object, *, field: str) -> Sequence[Any]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes, bytearray))
    ):
        raise StageArtifactError(f"{field} must be an array")
    return value


def _exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    field: str,
) -> None:
    missing = expected - value.keys()
    extra = value.keys() - expected
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {sorted(missing)!r}")
        if extra:
            details.append(f"unknown {sorted(extra)!r}")
        raise StageArtifactError(
            f"{field} fields are invalid: {', '.join(details)}"
        )


def _timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise StageArtifactError(f"{field} must be an ISO-8601 timestamp")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise StageArtifactError(
            f"{field} must be a valid ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StageArtifactError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _within_receipt(
    value: datetime,
    *,
    started_at: datetime,
    completed_at: datetime,
    field: str,
) -> None:
    lower = started_at.astimezone(timezone.utc) - RECEIPT_CLOCK_SKEW
    upper = completed_at.astimezone(timezone.utc) + RECEIPT_CLOCK_SKEW
    if value < lower or value > upper:
        raise StageArtifactError(
            f"{field} falls outside the command receipt window"
        )


def _canonical_plan_sha256(value: object) -> str:
    rendered = (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _stage_claim_states(
    supplied: CertificationInput,
    stage: Stage,
) -> tuple[str, ...]:
    if stage == "local_correctness":
        return (
            supplied.local_correctness,
            *(result.state for result in supplied.scenario_results),
        )
    if stage == "load":
        return tuple(
            result.state
            for result in (
                *supplied.provider_safe_suites,
                *supplied.fyralis_ceiling_suites,
            )
        )
    if stage == "fault_recovery":
        return tuple(
            result.state for result in supplied.fault_recovery_suites
        )
    return (
        supplied.canary.state,
        *(result.state for result in supplied.canary.operation_results),
    )


def _validate_common(
    raw: Mapping[str, Any],
    *,
    spec: SourceCertificationSpec,
    stage: Stage,
    started_at: datetime,
    completed_at: datetime,
    expected_plan_sha256: str,
) -> bool:
    expected_fields = _COMMON_FIELDS | _STAGE_FIELDS[stage]
    _exact_keys(raw, expected_fields, field=f"{stage} stage artifact")
    if raw.get("schema_version") != STAGE_ARTIFACT_SCHEMA_VERSION:
        raise StageArtifactError(
            f"{stage} stage artifact schema_version is unsupported"
        )
    if raw.get("source_id") != spec.source_id or raw.get("stage") != stage:
        raise StageArtifactError(f"{stage} stage artifact identity differs")
    if raw.get("spec_hash") != spec.declaration_hash():
        raise StageArtifactError(f"{stage} stage artifact spec_hash is stale")
    plan = _mapping(raw.get("execution_plan"), field="execution_plan")
    if (
        plan.get("source_id") != spec.source_id
        or plan.get("spec_hash") != spec.declaration_hash()
    ):
        raise StageArtifactError("execution_plan identity differs")
    plan_sha = raw.get("execution_plan_sha256")
    if (
        not isinstance(plan_sha, str)
        or _SHA256_RE.fullmatch(plan_sha) is None
        or plan_sha != _canonical_plan_sha256(plan)
        or plan_sha != expected_plan_sha256
    ):
        raise StageArtifactError(
            "execution_plan_sha256 differs from the binding and embedded plan"
        )
    generated_at = _timestamp(raw.get("generated_at"), field="generated_at")
    _within_receipt(
        generated_at,
        started_at=started_at,
        completed_at=completed_at,
        field="generated_at",
    )
    promotion_allowed = raw.get("synthetic_promotion_allowed")
    if not isinstance(promotion_allowed, bool):
        raise StageArtifactError(
            "synthetic_promotion_allowed must be a boolean"
        )
    boundary = raw.get("claim_boundary")
    if not isinstance(boundary, str) or not boundary.strip():
        raise StageArtifactError("claim_boundary must be non-empty")
    return promotion_allowed


def _validate_local(
    raw: Mapping[str, Any],
    *,
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
    promotion_allowed: bool,
) -> None:
    if promotion_allowed:
        raise StageArtifactError(
            "stage artifact v3 has no release-capable local schema"
        )
    if supplied.local_correctness == "passed":
        raise StageArtifactError(
            "diagnostic local artifact cannot pass aggregate correctness"
        )
    expected_ids = tuple(spec.required_scenarios)
    results = {result.scenario_id: result for result in supplied.scenario_results}
    if tuple(results) != expected_ids:
        raise StageArtifactError(
            "scenario results differ from the declared source scenario order"
        )
    ledger = _sequence(
        raw.get("scenario_execution_ledger"),
        field="scenario_execution_ledger",
    )
    rows: dict[str, Mapping[str, Any]] = {}
    row_fields = frozenset(
        {
            "scenario_id",
            "certification_state",
            "declared_probe_ids",
            "measured_probe_ids",
            "unmeasured_probe_ids",
            "unproven_requirements",
        }
    )
    for index, value in enumerate(ledger):
        row = _mapping(value, field=f"scenario_execution_ledger[{index}]")
        _exact_keys(
            row,
            row_fields,
            field=f"scenario_execution_ledger[{index}]",
        )
        scenario_id = row.get("scenario_id")
        if not isinstance(scenario_id, str) or scenario_id in rows:
            raise StageArtifactError(
                "scenario_execution_ledger has an invalid or duplicate ID"
            )
        for field in (
            "declared_probe_ids",
            "measured_probe_ids",
            "unmeasured_probe_ids",
            "unproven_requirements",
        ):
            values = _sequence(
                row.get(field),
                field=f"scenario_execution_ledger[{index}].{field}",
            )
            if not all(isinstance(item, str) for item in values):
                raise StageArtifactError(
                    f"scenario_execution_ledger[{index}].{field} "
                    "must contain strings"
                )
        rows[scenario_id] = row
    if tuple(rows) != expected_ids:
        raise StageArtifactError(
            "scenario_execution_ledger differs from declared scenarios"
        )

    pipeline = _mapping(raw.get("pipeline_probe"), field="pipeline_probe")
    if (
        pipeline.get("schema_version") != PIPELINE_PROBE_SCHEMA_VERSION
        or pipeline.get("source_id") != spec.source_id
    ):
        raise StageArtifactError("pipeline_probe identity differs")
    pipeline_state = pipeline.get("state")
    certified = pipeline.get("certified_scenarios")
    certified_ids = (
        frozenset(
            item
            for item in certified
            if isinstance(item, str)
        )
        if isinstance(certified, Sequence)
        and not isinstance(certified, (str, bytes, bytearray))
        else frozenset()
    )
    passed_ids = {
        result.scenario_id
        for result in supplied.scenario_results
        if result.state == "passed"
    }
    for scenario_id, result in results.items():
        row = rows[scenario_id]
        if row.get("certification_state") != result.state:
            raise StageArtifactError(
                f"scenario ledger state differs for {scenario_id}"
            )
        if result.state == "passed" and (
            row.get("unmeasured_probe_ids") != []
            or row.get("unproven_requirements") != []
        ):
            raise StageArtifactError(
                f"passing scenario {scenario_id} retains unproven requirements"
            )
    expected_pipeline_ids = pipeline_scenario_ids_for_source(spec.source_id)
    if passed_ids:
        if (
            not passed_ids.issubset(expected_pipeline_ids)
            or pipeline_state != "passed"
            or certified_ids != expected_pipeline_ids
        ):
            raise StageArtifactError(
                "only the exact source-capable pipeline boundary may be "
                "promoted by stage artifact v3"
            )
        try:
            if PIPELINE_TOPOLOGY_SCENARIO_IDS.issubset(
                expected_pipeline_ids,
            ):
                validate_history_topology_proof(pipeline)
            else:
                validate_replay_idempotency_proof(pipeline)
        except PipelineProbeError as exc:
            raise StageArtifactError(
                f"pipeline proof is invalid: {exc}"
            ) from exc


def _suite_kinds(
    supplied: CertificationInput,
    stage: Literal["load", "fault_recovery"],
) -> tuple[str, ...]:
    if stage == "load":
        return tuple(
            suite.kind
            for suite in (
                *supplied.provider_safe_suites,
                *supplied.fyralis_ceiling_suites,
            )
        )
    return tuple(suite.kind for suite in supplied.fault_recovery_suites)


def _declared_load_suite_plan(spec: SourceCertificationSpec) -> list[dict[str, object]]:
    """Render the exact typed load subset sealed into the execution plan.

    The execution driver owns the surrounding plan, but the source
    certification contract owns this subset. Keeping the projection here
    lets the stage verifier reject an artifact whose self-hashed plan is
    internally consistent yet omits or rewrites a typed load declaration.
    """

    return [
        {
            "kind": suite.kind,
            "workload": suite.execution_workload_dict(),
            # Legacy consumers may still render these labels, but this field
            # is explicitly not used for scheduling, coverage, or promotion.
            "compatibility_operation_mix": list(suite.operation_mix),
            "tenants": suite.tenants,
            "installations_per_tenant": suite.installations_per_tenant,
            "replicas": suite.replicas,
            "warmup_seconds": suite.warmup_seconds,
            "stable_seconds": suite.stable_seconds,
            "weekly_soak_seconds": suite.weekly_soak_seconds,
            "step_percent": suite.step_percent,
            "search_tolerance_percent": suite.search_tolerance_percent,
        }
        for suite in spec.load_suites
    ]


def _validate_pipeline_load_artifacts(
    raw: Mapping[str, Any],
    *,
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
) -> None:
    """Verify the six typed pipeline artifacts owned by a load stage.

    Stage-artifact v3 remains diagnostic-only: it accepts only blocked/failed
    applicable results and neutral not-applicable results. The nested artifact
    is nevertheless parsed as a full v2 pipeline artifact so a future driver
    cannot replace a typed receipt-bound workload with string mix labels.
    """

    expected_plan = _declared_load_suite_plan(spec)
    declared = _sequence(
        raw.get("declared_load_suites"),
        field="declared_load_suites",
    )
    if list(declared) != expected_plan:
        raise StageArtifactError(
            "declared_load_suites differ from the full typed source plan"
        )

    execution_plan = _mapping(raw.get("execution_plan"), field="execution_plan")
    plan_suites = _sequence(
        execution_plan.get("load_suites"),
        field="execution_plan.load_suites",
    )
    if list(plan_suites) != expected_plan:
        raise StageArtifactError(
            "execution_plan.load_suites differ from the full typed source plan"
        )
    if list(declared) != list(plan_suites):
        raise StageArtifactError(
            "declared_load_suites differ from execution_plan.load_suites"
        )

    artifacts = _mapping(
        raw.get("pipeline_load_artifacts"),
        field="pipeline_load_artifacts",
    )
    _exact_keys(
        artifacts,
        frozenset({"provider_safe", "fyralis_ceiling"}),
        field="pipeline_load_artifacts",
    )
    result_sets = {
        "provider_safe": supplied.provider_safe_suites,
        "fyralis_ceiling": supplied.fyralis_ceiling_suites,
    }
    suites_by_kind = {suite.kind: suite for suite in spec.load_suites}

    for mode, results in result_sets.items():
        mode_artifacts = _mapping(
            artifacts.get(mode),
            field=f"pipeline_load_artifacts.{mode}",
        )
        _exact_keys(
            mode_artifacts,
            frozenset(suites_by_kind),
            field=f"pipeline_load_artifacts.{mode}",
        )
        results_by_kind = {result.kind: result for result in results}
        if tuple(results_by_kind) != tuple(suites_by_kind):
            raise StageArtifactError(
                f"{mode} suite results differ from the typed source plan"
            )

        for kind, suite in suites_by_kind.items():
            artifact = _mapping(
                mode_artifacts.get(kind),
                field=f"pipeline_load_artifacts.{mode}.{kind}",
            )
            try:
                validate_pipeline_load_artifact(artifact)
            except PipelineLoadArtifactError as exc:
                raise StageArtifactError(
                    f"pipeline_load_artifacts.{mode}.{kind} is invalid: {exc}"
                ) from exc
            if artifact.get("source_id") != spec.source_id:
                raise StageArtifactError(
                    f"pipeline_load_artifacts.{mode}.{kind} source differs"
                )
            if artifact.get("mode") != mode:
                raise StageArtifactError(
                    f"pipeline_load_artifacts.{mode}.{kind} mode differs"
                )
            workload = _mapping(
                artifact.get("workload"),
                field=f"pipeline_load_artifacts.{mode}.{kind}.workload",
            )
            if dict(workload) != suite.execution_workload_dict():
                raise StageArtifactError(
                    f"pipeline_load_artifacts.{mode}.{kind} typed workload "
                    "differs from the source declaration"
                )
            state = artifact.get("state")
            if state == "passed" or artifact.get("promotion_eligible") is True:
                raise StageArtifactError(
                    "stage artifact v3 cannot accept promotion-eligible "
                    "pipeline load claims"
                )
            if artifact.get("configuration") != (
                diagnostic_pipeline_load_config_from_suite(suite).to_dict()
            ):
                raise StageArtifactError(
                    f"pipeline_load_artifacts.{mode}.{kind} configuration "
                    "differs from the typed source suite"
                )

            result = results_by_kind[kind]
            if suite.non_applicability is not None:
                if state != "not_applicable":
                    raise StageArtifactError(
                        f"pipeline_load_artifacts.{mode}.{kind} must be "
                        "not_applicable for the declared unsupported workload"
                    )
                if result.state != "not_applicable":
                    raise StageArtifactError(
                        f"{mode}.{kind} SuiteResult must be not_applicable "
                        "for the declared unsupported workload"
                    )
            else:
                if state == "not_applicable":
                    raise StageArtifactError(
                        f"pipeline_load_artifacts.{mode}.{kind} is "
                        "not_applicable for an executable workload"
                    )
                if result.state not in {"blocked", "failed"}:
                    raise StageArtifactError(
                        f"{mode}.{kind} SuiteResult must remain blocked or "
                        "failed under stage artifact v3"
                    )


def _validate_diagnostic_suites(
    raw: Mapping[str, Any],
    *,
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
    stage: Literal["load", "fault_recovery"],
    promotion_allowed: bool,
) -> None:
    if promotion_allowed:
        raise StageArtifactError(
            f"stage artifact v3 has no release-capable {stage} schema"
        )
    expected = tuple(suite.kind for suite in spec.load_suites)
    actual = _suite_kinds(supplied, stage)
    if stage == "load":
        if actual != (*expected, *expected):
            raise StageArtifactError(
                "load result suite order differs from the declaration"
            )
    elif actual != expected:
        raise StageArtifactError(
            "fault-recovery result suite order differs from the declaration"
        )
    if any(state == "passed" for state in _stage_claim_states(supplied, stage)):
        raise StageArtifactError(
            f"diagnostic {stage} artifact cannot contain passing claims"
        )
    if stage == "load":
        _validate_pipeline_load_artifacts(
            raw,
            spec=spec,
            supplied=supplied,
        )
        offered = _mapping(raw.get("offered_load"), field="offered_load")
        if (
            offered.get("state") in {"passed", "promotion_eligible"}
            or offered.get("promotion_eligible") is True
        ):
            raise StageArtifactError(
                "offered_load must remain a non-promoting Provider Lab diagnostic"
            )
        _mapping(raw.get("load_diagnostic"), field="load_diagnostic")
    else:
        _mapping(
            raw.get("fault_recovery_diagnostic"),
            field="fault_recovery_diagnostic",
        )
        targets = _sequence(
            raw.get("declared_fault_targets"),
            field="declared_fault_targets",
        )
        if not all(isinstance(item, Mapping) for item in targets):
            raise StageArtifactError(
                "declared_fault_targets must contain objects"
            )


def _validate_canary(
    raw: Mapping[str, Any],
    *,
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
    promotion_allowed: bool,
    started_at: datetime,
    completed_at: datetime,
) -> None:
    names = _sequence(
        raw.get("credential_environment_names_present"),
        field="credential_environment_names_present",
    )
    if not all(isinstance(name, str) and name for name in names):
        raise StageArtifactError(
            "credential_environment_names_present must contain strings"
        )
    if raw.get("credential_values_recorded") is not False:
        raise StageArtifactError("canary artifact must never record credentials")
    request_total = raw.get("real_provider_requests_sent")
    if (
        isinstance(request_total, bool)
        or not isinstance(request_total, int)
        or request_total < 0
    ):
        raise StageArtifactError(
            "real_provider_requests_sent must be a non-negative integer"
        )

    execution = _mapping(
        raw.get("canary_execution"),
        field="canary_execution",
    )
    _exact_keys(
        execution,
        frozenset(
            {
                "schema_version",
                "source_id",
                "canary_id",
                "promotion_eligible",
                "account_identity_sha256",
                "account_type",
                "api_version",
                "started_at",
                "completed_at",
                "request_ledger",
                "cleanup",
            }
        ),
        field="canary_execution",
    )
    if (
        execution.get("schema_version") != CANARY_EXECUTION_SCHEMA_VERSION
        or execution.get("source_id") != spec.source_id
        or execution.get("canary_id") != spec.canary.canary_id
    ):
        raise StageArtifactError("canary_execution identity differs")
    execution_started = _timestamp(
        execution.get("started_at"),
        field="canary_execution.started_at",
    )
    execution_completed = _timestamp(
        execution.get("completed_at"),
        field="canary_execution.completed_at",
    )
    if execution_completed < execution_started:
        raise StageArtifactError(
            "canary_execution.completed_at precedes started_at"
        )
    for field, value in (
        ("canary_execution.started_at", execution_started),
        ("canary_execution.completed_at", execution_completed),
    ):
        _within_receipt(
            value,
            started_at=started_at,
            completed_at=completed_at,
            field=field,
        )

    ledger = _sequence(
        execution.get("request_ledger"),
        field="canary_execution.request_ledger",
    )
    ledger_fields = frozenset(
        {
            "sequence",
            "operation_id",
            "request_kind",
            "mutation_action",
            "started_at",
            "completed_at",
            "state",
        }
    )
    operation_states: dict[str, list[str]] = {}
    executed_mutations: list[str] = []
    cleanup_attempts: dict[str, list[tuple[str, datetime]]] = {}
    cleanup_phase_started = False
    for index, value in enumerate(ledger):
        row = _mapping(
            value,
            field=f"canary_execution.request_ledger[{index}]",
        )
        _exact_keys(
            row,
            ledger_fields,
            field=f"canary_execution.request_ledger[{index}]",
        )
        if row.get("sequence") != index + 1:
            raise StageArtifactError(
                "canary request ledger sequence must be contiguous from 1"
            )
        operation_id = row.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            raise StageArtifactError(
                "canary request ledger contains an invalid operation"
            )
        request_kind = row.get("request_kind")
        mutation_action = row.get("mutation_action")
        is_cleanup = request_kind == "cleanup"
        if is_cleanup:
            cleanup_phase_started = True
            if operation_id not in executed_mutations:
                raise StageArtifactError(
                    "canary cleanup request has no preceding contracted "
                    "mutation"
                )
            if mutation_action is not None:
                raise StageArtifactError(
                    "canary cleanup request cannot declare a mutation action"
                )
        else:
            if cleanup_phase_started:
                raise StageArtifactError(
                    "canary provider operations cannot execute after cleanup "
                    "has started"
                )
            if operation_id not in spec.canary.required_operations:
                raise StageArtifactError(
                    "canary request ledger contains an undeclared operation"
                )
            operation_contract = spec.canary.operation_contract_for(
                operation_id
            )
            if operation_contract.mutability == "unclassified":
                raise StageArtifactError(
                    "canary request mutability is unclassified by its "
                    "operation contract"
                )
            if request_kind != operation_contract.mutability:
                raise StageArtifactError(
                    "canary request_kind differs from its operation contract"
                )
            if operation_contract.mutability == "read":
                if mutation_action is not None:
                    raise StageArtifactError(
                        "read canary request cannot declare a mutation action"
                    )
            else:
                if (
                    not isinstance(mutation_action, str)
                    or mutation_action != operation_contract.cleanup_action
                ):
                    raise StageArtifactError(
                        "canary request cleanup action differs from its "
                        "operation contract"
                    )
                executed_mutations.append(mutation_action)
        state = row.get("state")
        if state not in {"passed", "failed"}:
            raise StageArtifactError(
                "canary request state must be passed or failed"
            )
        request_started = _timestamp(
            row.get("started_at"),
            field=f"canary request {index + 1}.started_at",
        )
        request_completed = _timestamp(
            row.get("completed_at"),
            field=f"canary request {index + 1}.completed_at",
        )
        if request_completed < request_started:
            raise StageArtifactError(
                f"canary request {index + 1} completed before it started"
            )
        if (
            request_started < execution_started
            or request_completed > execution_completed
        ):
            raise StageArtifactError(
                f"canary request {index + 1} falls outside execution window"
            )
        if is_cleanup:
            cleanup_attempts.setdefault(operation_id, []).append(
                (state, request_completed)
            )
        else:
            operation_states.setdefault(operation_id, []).append(state)

    if len(ledger) != request_total:
        raise StageArtifactError(
            "real_provider_requests_sent differs from the canary request ledger"
        )
    cleanup = _mapping(
        execution.get("cleanup"),
        field="canary_execution.cleanup",
    )
    _exact_keys(
        cleanup,
        frozenset({"required", "state", "completed_at", "actions"}),
        field="canary_execution.cleanup",
    )
    cleanup_required = bool(executed_mutations)
    if cleanup.get("required") is not cleanup_required:
        raise StageArtifactError(
            "canary cleanup requirement differs from executed mutations"
        )
    cleanup_state = cleanup.get("state")
    cleanup_actions = _sequence(
        cleanup.get("actions"),
        field="canary_execution.cleanup.actions",
    )
    if cleanup_required:
        if cleanup_state != "passed":
            raise StageArtifactError(
                "mutating canary requires successful cleanup"
            )
        cleanup_completed = _timestamp(
            cleanup.get("completed_at"),
            field="canary_execution.cleanup.completed_at",
        )
        if (
            cleanup_completed < execution_started
            or cleanup_completed > execution_completed
        ):
            raise StageArtifactError(
                "canary cleanup falls outside execution window"
            )
        expected_actions = tuple(dict.fromkeys(executed_mutations))
        actual_actions: list[str] = []
        if set(cleanup_attempts) != set(expected_actions) or any(
            not attempts or attempts[-1][0] != "passed"
            for attempts in cleanup_attempts.values()
        ):
            raise StageArtifactError(
                "canary cleanup request ledger does not terminally pass every "
                "contracted cleanup action"
            )
        for index, value in enumerate(cleanup_actions):
            action = _mapping(
                value,
                field=f"canary_execution.cleanup.actions[{index}]",
            )
            _exact_keys(
                action,
                frozenset({"action_id", "state", "completed_at"}),
                field=f"canary_execution.cleanup.actions[{index}]",
            )
            action_id = action.get("action_id")
            if (
                not isinstance(action_id, str)
                or action_id not in cleanup_attempts
                or action.get("state") != "passed"
            ):
                raise StageArtifactError(
                    "canary cleanup action identity/state is invalid"
                )
            action_completed = _timestamp(
                action.get("completed_at"),
                field=(
                    f"canary_execution.cleanup.actions[{index}].completed_at"
                ),
            )
            if (
                action_completed < execution_started
                or action_completed > execution_completed
            ):
                raise StageArtifactError(
                    "canary cleanup action falls outside execution window"
                )
            if action_completed != cleanup_attempts[action_id][-1][1]:
                raise StageArtifactError(
                    "canary cleanup action timestamp differs from its terminal "
                    "request"
                )
            actual_actions.append(action_id)
        if tuple(actual_actions) != expected_actions:
            raise StageArtifactError(
                "canary cleanup actions differ from operation contracts"
            )
        if cleanup_completed != max(
            attempts[-1][1] for attempts in cleanup_attempts.values()
        ):
            raise StageArtifactError(
                "canary cleanup completion differs from its terminal requests"
            )
    elif (
        cleanup_state != "not_required"
        or cleanup.get("completed_at") is not None
        or cleanup_actions
        or cleanup_attempts
    ):
        raise StageArtifactError(
            "read-only canary cleanup must be not_required with no timestamp"
        )

    canary = supplied.canary
    if canary.state != "passed":
        if (
            promotion_allowed
            or execution.get("promotion_eligible") is not False
            or request_total != 0
            or ledger
        ):
            raise StageArtifactError(
                "blocked canary artifact cannot claim promotion or requests"
            )
        return

    if (
        not promotion_allowed
        or execution.get("promotion_eligible") is not True
    ):
        raise StageArtifactError(
            "passing canary requires an explicitly promotion-eligible ledger"
        )
    if not 0 < request_total <= spec.canary.max_requests:
        raise StageArtifactError(
            "canary request count exceeds its declared low-rate budget"
        )
    identity_sha = execution.get("account_identity_sha256")
    if (
        not isinstance(identity_sha, str)
        or _SHA256_RE.fullmatch(identity_sha) is None
        or identity_sha != canary.account_identity_sha256
    ):
        raise StageArtifactError(
            "canary account identity hash is missing or differs"
        )
    if (
        execution.get("account_type") != spec.canary.account_type
        or execution.get("account_type") != canary.account_type
        or execution.get("api_version") != spec.provider_api_version
        or execution.get("api_version") != canary.api_version
    ):
        raise StageArtifactError(
            "canary account/API metadata differs from the certification spec"
        )
    if canary.request_count != request_total:
        raise StageArtifactError(
            "canary request_count differs from its request ledger"
        )
    if tuple(dict.fromkeys(executed_mutations)) != canary.mutation_actions:
        raise StageArtifactError(
            "canary mutation summary differs from its request ledger"
        )
    if canary.cleanup_state != cleanup_state:
        raise StageArtifactError(
            "canary cleanup summary differs from its request ledger"
        )
    if canary.tested_at is None:
        raise StageArtifactError("passing canary tested_at is missing")
    tested_at = canary.tested_at.astimezone(timezone.utc)
    if tested_at < execution_started or tested_at > execution_completed:
        raise StageArtifactError(
            "canary tested_at falls outside the execution window"
        )
    expected_operations = set(spec.canary.required_operations)
    if set(operation_states) != expected_operations or any(
        not states or states[-1] != "passed"
        for states in operation_states.values()
    ):
        raise StageArtifactError(
            "passing canary requires a terminal successful request for every "
            "declared operation"
        )
    result_states = {
        result.operation_id: result.state
        for result in canary.operation_results
    }
    if result_states != {
        operation_id: "passed"
        for operation_id in spec.canary.required_operations
    }:
        raise StageArtifactError(
            "canary operation results differ from the request ledger"
        )


def validate_stage_artifact(
    value: object,
    *,
    spec: SourceCertificationSpec,
    stage: Stage,
    supplied: CertificationInput,
    started_at: datetime,
    completed_at: datetime,
    expected_plan_sha256: str,
) -> None:
    """Validate one typed artifact against its command result and receipt."""

    if (
        started_at.tzinfo is None
        or started_at.utcoffset() is None
        or completed_at.tzinfo is None
        or completed_at.utcoffset() is None
        or completed_at < started_at
    ):
        raise StageArtifactError("command receipt window is invalid")
    raw = _mapping(value, field=f"{stage} stage artifact")
    promotion_allowed = _validate_common(
        raw,
        spec=spec,
        stage=stage,
        started_at=started_at,
        completed_at=completed_at,
        expected_plan_sha256=expected_plan_sha256,
    )
    if stage == "local_correctness":
        _validate_local(
            raw,
            spec=spec,
            supplied=supplied,
            promotion_allowed=promotion_allowed,
        )
    elif stage == "load":
        _validate_diagnostic_suites(
            raw,
            spec=spec,
            supplied=supplied,
            stage="load",
            promotion_allowed=promotion_allowed,
        )
    elif stage == "fault_recovery":
        _validate_diagnostic_suites(
            raw,
            spec=spec,
            supplied=supplied,
            stage="fault_recovery",
            promotion_allowed=promotion_allowed,
        )
    else:
        _validate_canary(
            raw,
            spec=spec,
            supplied=supplied,
            promotion_allowed=promotion_allowed,
            started_at=started_at,
            completed_at=completed_at,
        )


__all__ = [
    "CANARY_EXECUTION_SCHEMA_VERSION",
    "PIPELINE_PROBE_SCHEMA_VERSION",
    "PIPELINE_SCENARIO_IDS",
    "RECEIPT_CLOCK_SKEW",
    "STAGE_ARTIFACT_SCHEMA_VERSION",
    "Stage",
    "StageArtifactError",
    "validate_stage_artifact",
]
