"""Strict JSON input/output helpers for source certification artifacts."""
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    CertificationInvariantError,
    ScenarioResult,
    SuiteResult,
)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CertificationInvariantError(f"{field} must be an object")
    return value


def _keys(
    value: Mapping[str, Any],
    *,
    field: str,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> None:
    missing = required - value.keys()
    extra = value.keys() - required - optional
    if missing:
        raise CertificationInvariantError(
            f"{field} is missing fields: {', '.join(sorted(missing))}"
        )
    if extra:
        raise CertificationInvariantError(
            f"{field} has unknown fields: {', '.join(sorted(extra))}"
        )


def _sequence(value: object, field: str) -> Sequence[Any]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes, bytearray))
    ):
        raise CertificationInvariantError(f"{field} must be an array")
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    values = _sequence(value, field)
    if not all(isinstance(item, str) for item in values):
        raise CertificationInvariantError(f"{field} must contain only strings")
    return tuple(values)


def _timestamp(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CertificationInvariantError(
            f"{field} must be an ISO-8601 string or null"
        )
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CertificationInvariantError(
            f"{field} must be a valid ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CertificationInvariantError(
            f"{field} must include a timezone offset"
        )
    return parsed


def _metrics(value: object, field: str) -> tuple[tuple[str, float], ...]:
    values = _mapping(value, field)
    result: list[tuple[str, float]] = []
    for name, raw in sorted(values.items()):
        if (
            isinstance(raw, bool)
            or not isinstance(raw, (int, float))
        ):
            raise CertificationInvariantError(
                f"{field}.{name} must be a number"
            )
        result.append((name, float(raw)))
    return tuple(result)


def _suite(value: object, field: str) -> SuiteResult:
    item = _mapping(value, field)
    _keys(
        item,
        field=field,
        required=frozenset({"kind", "state", "metrics"}),
        optional=frozenset(
            {
                "artifact_uri",
                "started_at",
                "completed_at",
                "limiting_component",
                "failures",
            }
        ),
    )
    return SuiteResult(
        kind=item["kind"],
        state=item["state"],
        artifact_uri=item.get("artifact_uri"),
        started_at=_timestamp(item.get("started_at"), f"{field}.started_at"),
        completed_at=_timestamp(
            item.get("completed_at"),
            f"{field}.completed_at",
        ),
        metrics=_metrics(item["metrics"], f"{field}.metrics"),
        limiting_component=item.get("limiting_component"),
        failures=_strings(item.get("failures", []), f"{field}.failures"),
    )


def _suites(value: object, field: str) -> tuple[SuiteResult, ...]:
    return tuple(
        _suite(item, f"{field}[{index}]")
        for index, item in enumerate(_sequence(value, field))
    )


def _scenario_result(value: object, field: str) -> ScenarioResult:
    item = _mapping(value, field)
    _keys(
        item,
        field=field,
        required=frozenset({"scenario_id", "state", "artifact_uri"}),
        optional=frozenset({"failures"}),
    )
    return ScenarioResult(
        scenario_id=item["scenario_id"],
        state=item["state"],
        artifact_uri=item["artifact_uri"],
        failures=_strings(item.get("failures", []), f"{field}.failures"),
    )


def _scenario_results(value: object) -> tuple[ScenarioResult, ...]:
    field = "scenario_results"
    return tuple(
        _scenario_result(item, f"{field}[{index}]")
        for index, item in enumerate(_sequence(value, field))
    )


def _canary_operation_result(
    value: object,
    field: str,
) -> CanaryOperationResult:
    item = _mapping(value, field)
    _keys(
        item,
        field=field,
        required=frozenset({"operation_id", "state", "artifact_uri"}),
        optional=frozenset({"failures"}),
    )
    return CanaryOperationResult(
        operation_id=item["operation_id"],
        state=item["state"],
        artifact_uri=item["artifact_uri"],
        failures=_strings(item.get("failures", []), f"{field}.failures"),
    )


def _canary_operation_results(
    value: object,
) -> tuple[CanaryOperationResult, ...]:
    field = "canary.operation_results"
    return tuple(
        _canary_operation_result(item, f"{field}[{index}]")
        for index, item in enumerate(_sequence(value, field))
    )


def _canary(value: object) -> CanaryResult:
    item = _mapping(value, "canary")
    _keys(
        item,
        field="canary",
        required=frozenset({"state", "operation_results"}),
        optional=frozenset(
            {
                "tested_at",
                "account_type",
                "api_version",
                "artifact_uri",
                "request_count",
                "account_identity_sha256",
                "mutation_actions",
                "cleanup_state",
                "failures",
            }
        ),
    )
    return CanaryResult(
        state=item["state"],
        operation_results=_canary_operation_results(
            item["operation_results"]
        ),
        tested_at=_timestamp(item.get("tested_at"), "canary.tested_at"),
        account_type=item.get("account_type"),
        api_version=item.get("api_version"),
        artifact_uri=item.get("artifact_uri"),
        request_count=item.get("request_count", 0),
        account_identity_sha256=item.get("account_identity_sha256"),
        mutation_actions=_strings(
            item.get("mutation_actions", []),
            "canary.mutation_actions",
        ),
        cleanup_state=item.get("cleanup_state", "not_required"),
        failures=_strings(item.get("failures", []), "canary.failures"),
    )


def parse_certification_input(value: object) -> CertificationInput:
    """Parse one fail-closed certification input from decoded JSON."""

    item = _mapping(value, "certification input")
    required = frozenset(
        {
            "spec_hash",
            "local_correctness",
            "local_correctness_artifact",
            "scenario_results",
            "provider_safe_suites",
            "fyralis_ceiling_suites",
            "fault_recovery_suites",
            "canary",
            "legacy_reference_count",
        }
    )
    _keys(
        item,
        field="certification input",
        required=required,
        optional=frozenset({"skipped_tests", "todos"}),
    )
    return CertificationInput(
        spec_hash=item["spec_hash"],
        local_correctness=item["local_correctness"],
        local_correctness_artifact=item["local_correctness_artifact"],
        scenario_results=_scenario_results(item["scenario_results"]),
        provider_safe_suites=_suites(
            item["provider_safe_suites"],
            "provider_safe_suites",
        ),
        fyralis_ceiling_suites=_suites(
            item["fyralis_ceiling_suites"],
            "fyralis_ceiling_suites",
        ),
        fault_recovery_suites=_suites(
            item["fault_recovery_suites"],
            "fault_recovery_suites",
        ),
        canary=_canary(item["canary"]),
        legacy_reference_count=item["legacy_reference_count"],
        skipped_tests=_strings(item.get("skipped_tests", []), "skipped_tests"),
        todos=_strings(item.get("todos", []), "todos"),
    )


def load_certification_input(path: Path) -> CertificationInput:
    """Read one source artifact, rejecting malformed or ambiguous JSON."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CertificationInvariantError(
            f"cannot load certification input {path}: {exc}"
        ) from exc
    return parse_certification_input(value)


def _render_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _suite_dict(result: SuiteResult) -> dict[str, Any]:
    return {
        "kind": result.kind,
        "state": result.state,
        "artifact_uri": result.artifact_uri,
        "started_at": _render_timestamp(result.started_at),
        "completed_at": _render_timestamp(result.completed_at),
        "metrics": dict(result.metrics),
        "limiting_component": result.limiting_component,
        "failures": list(result.failures),
    }


def certification_input_dict(value: CertificationInput) -> dict[str, Any]:
    """Return the canonical JSON shape accepted by the strict parser."""

    return {
        "spec_hash": value.spec_hash,
        "local_correctness": value.local_correctness,
        "local_correctness_artifact": value.local_correctness_artifact,
        "scenario_results": [
            {
                "scenario_id": result.scenario_id,
                "state": result.state,
                "artifact_uri": result.artifact_uri,
                "failures": list(result.failures),
            }
            for result in value.scenario_results
        ],
        "provider_safe_suites": [
            _suite_dict(result) for result in value.provider_safe_suites
        ],
        "fyralis_ceiling_suites": [
            _suite_dict(result) for result in value.fyralis_ceiling_suites
        ],
        "fault_recovery_suites": [
            _suite_dict(result) for result in value.fault_recovery_suites
        ],
        "canary": {
            "state": value.canary.state,
            "operation_results": [
                {
                    "operation_id": result.operation_id,
                    "state": result.state,
                    "artifact_uri": result.artifact_uri,
                    "failures": list(result.failures),
                }
                for result in value.canary.operation_results
            ],
            "tested_at": _render_timestamp(value.canary.tested_at),
            "account_type": value.canary.account_type,
            "api_version": value.canary.api_version,
            "artifact_uri": value.canary.artifact_uri,
            "request_count": value.canary.request_count,
            "account_identity_sha256": (
                value.canary.account_identity_sha256
            ),
            "mutation_actions": list(value.canary.mutation_actions),
            "cleanup_state": value.canary.cleanup_state,
            "failures": list(value.canary.failures),
        },
        "legacy_reference_count": value.legacy_reference_count,
        "skipped_tests": list(value.skipped_tests),
        "todos": list(value.todos),
    }


def write_certification_input(
    path: Path,
    value: CertificationInput,
) -> None:
    """Atomically write one parser-round-trippable certification input."""

    rendered = json.dumps(
        certification_input_dict(value),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


__all__ = [
    "certification_input_dict",
    "load_certification_input",
    "parse_certification_input",
    "write_certification_input",
]
