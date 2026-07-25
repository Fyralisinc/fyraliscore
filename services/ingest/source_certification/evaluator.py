"""Fail-closed source and release certification evaluation."""
from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Mapping

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.models import (
    CertificationDecision,
    CertificationInput,
    SourceCertificationSpec,
    SuiteResult,
)


_REQUIRED_SUITE_KINDS = frozenset({"historical", "live", "combined"})
_ZERO_METRICS = (
    "missing_records",
    "cross_tenant_leaks",
    "unexpected_duplicates",
    "cooldown_violations",
    "cursor_consistency_errors",
    "hot_loops",
)
_REQUIRED_REPORT_METRICS = frozenset(
    {
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "requests_per_second",
        "quota_units_per_second",
        "records_per_second",
        "bytes_per_second",
        "kafka_lag",
        "observation_p99_latency_ms",
        "retries",
        "rate_limited_responses",
        "dlq_entries",
        "unexpected_duplicates",
        "missing_records",
        "cursor_consistency_errors",
        "cpu_percent",
        "memory_bytes",
        "backlog_growth_per_second",
        "cooldown_violations",
        "cross_tenant_leaks",
        "hot_loops",
        "lab_capacity_ratio",
        "lab_p99_timeout_ratio",
        "warmup_seconds",
        "validation_seconds",
        "soak_seconds",
        "search_tolerance_ratio",
        "offered_rate",
        "stable_rate",
    }
)


def _metrics(result: SuiteResult) -> dict[str, float]:
    return dict(result.metrics)


def _coverage_failures(
    label: str,
    expected_ids: tuple[str, ...],
    results: tuple[object, ...],
    *,
    identity_field: str,
) -> list[str]:
    """Require one passing, artifact-backed result for every declared ID."""

    actual_ids = [
        getattr(result, identity_field, None)
        for result in results
    ]
    duplicates = sorted(
        {
            result_id
            for result_id in actual_ids
            if isinstance(result_id, str) and actual_ids.count(result_id) > 1
        }
    )
    missing = [result_id for result_id in expected_ids if result_id not in actual_ids]
    extras = sorted(
        str(result_id)
        for result_id in actual_ids
        if not isinstance(result_id, str) or result_id not in expected_ids
    )
    failures: list[str] = []
    if missing:
        failures.append(f"{label} coverage missing: {', '.join(missing)}")
    if extras:
        failures.append(
            f"{label} coverage has unexpected IDs: "
            + ", ".join(extras)
        )
    if duplicates:
        failures.append(
            f"{label} coverage has duplicate IDs: {', '.join(duplicates)}"
        )
    for result in results:
        result_id = getattr(result, identity_field, "<unknown>")
        state = getattr(result, "state", None)
        artifact_uri = getattr(result, "artifact_uri", None)
        result_failures = getattr(result, "failures", ())
        if state != "passed":
            failures.append(f"{label}.{result_id} state is {state}")
        if not artifact_uri:
            failures.append(f"{label}.{result_id} artifact is missing")
        if result_failures:
            failures.append(f"{label}.{result_id} contains failures")
    return failures


def _suite_failures(
    label: str,
    suites: tuple[SuiteResult, ...],
    *,
    provider_safe: bool = False,
    ceiling: bool = False,
    fault_recovery: bool = False,
) -> list[str]:
    failures: list[str] = []
    kinds = {suite.kind for suite in suites}
    if kinds != _REQUIRED_SUITE_KINDS or len(suites) != 3:
        failures.append(
            f"{label} must contain exactly historical/live/combined results"
        )
    for suite in suites:
        prefix = f"{label}.{suite.kind}"
        if suite.state != "passed":
            failures.append(f"{prefix} state is {suite.state}")
            continue
        metrics = _metrics(suite)
        missing_metrics = sorted(_REQUIRED_REPORT_METRICS - metrics.keys())
        if missing_metrics:
            failures.append(
                f"{prefix} is missing report metrics: "
                f"{', '.join(missing_metrics)}"
            )
        if not suite.limiting_component:
            failures.append(f"{prefix}.limiting_component is missing")
        for name in _ZERO_METRICS:
            if metrics.get(name) != 0:
                failures.append(f"{prefix}.{name} must equal 0")
        if metrics.get("backlog_growth_per_second") != 0:
            failures.append(f"{prefix}.backlog_growth_per_second must equal 0")
        if metrics.get("lab_capacity_ratio", 0) < 2:
            failures.append(f"{prefix}.lab_capacity_ratio must be >= 2")
        if metrics.get("lab_p99_timeout_ratio", 1) > 0.1:
            failures.append(f"{prefix}.lab_p99_timeout_ratio must be <= 0.1")
        if metrics.get("warmup_seconds", 0) < 120:
            failures.append(f"{prefix}.warmup_seconds must be >= 120")
        if metrics.get("validation_seconds", 0) < 900:
            failures.append(f"{prefix}.validation_seconds must be >= 900")
        if metrics.get("soak_seconds", 0) < 3_600:
            failures.append(f"{prefix}.soak_seconds must be >= 3600")
        if metrics.get("search_tolerance_ratio", 1) > 0.05:
            failures.append(
                f"{prefix}.search_tolerance_ratio must be <= 0.05"
            )
        if metrics.get("offered_rate", 0) <= 0:
            failures.append(f"{prefix}.offered_rate must be > 0")
        if metrics.get("stable_rate", 0) <= 0:
            failures.append(f"{prefix}.stable_rate must be > 0")
        if provider_safe and metrics.get("quota_utilization_ratio", 0) < 0.9:
            failures.append(f"{prefix}.quota_utilization_ratio must be >= 0.9")
        if ceiling and metrics.get("headroom_ratio", 0) < 1:
            failures.append(f"{prefix}.headroom_ratio must be >= 1")
        if fault_recovery and metrics.get("recovered_faults_ratio", 0) < 1:
            failures.append(f"{prefix}.recovered_faults_ratio must equal 1")
    return failures


def evaluate_certification(
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
    *,
    now: datetime | None = None,
) -> CertificationDecision:
    """Evaluate one source without waivers or synthetic-only promotion."""

    evaluated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    failures: list[str] = []
    expected_hash = spec.declaration_hash()
    if supplied.spec_hash != expected_hash:
        failures.append("spec hash differs from the evaluated declaration")

    unverified = [item.behavior_id for item in spec.evidence if not item.verified]
    if unverified:
        failures.append(f"unverified evidence: {', '.join(sorted(unverified))}")
    surface = next(
        (item for item in spec.evidence if item.behavior_id == "used_api_surface"),
        None,
    )
    if surface is None or surface.schema_sha256 is None:
        failures.append("used API surface has no pinned schema checksum")

    if supplied.local_correctness != "passed":
        failures.append(
            f"local correctness state is {supplied.local_correctness}"
        )
    if not supplied.local_correctness_artifact:
        failures.append("local correctness artifact is missing")
    failures.extend(
        _coverage_failures(
            "local correctness scenario",
            spec.required_scenarios,
            tuple(supplied.scenario_results),
            identity_field="scenario_id",
        )
    )

    failures.extend(
        _suite_failures(
            "provider_safe",
            supplied.provider_safe_suites,
            provider_safe=True,
        )
    )
    failures.extend(
        _suite_failures(
            "fyralis_ceiling",
            supplied.fyralis_ceiling_suites,
            ceiling=True,
        )
    )
    failures.extend(
        _suite_failures(
            "fault_recovery",
            supplied.fault_recovery_suites,
            fault_recovery=True,
        )
    )

    canary = supplied.canary
    if canary.state != "passed":
        failures.append(f"real-provider canary state is {canary.state}")
    if canary.api_version != spec.provider_api_version:
        failures.append("canary API version differs from certification spec")
    if canary.account_type != spec.canary.account_type:
        failures.append("canary account type differs from certification spec")
    failures.extend(
        _coverage_failures(
            "real-provider canary operation",
            spec.canary.required_operations,
            tuple(canary.operation_results),
            identity_field="operation_id",
        )
    )
    if supplied.legacy_reference_count != 0:
        failures.append(
            f"{supplied.legacy_reference_count} legacy binding references remain"
        )
    if supplied.skipped_tests:
        failures.append(f"skipped tests: {', '.join(supplied.skipped_tests)}")
    if supplied.todos:
        failures.append(f"TODOs: {', '.join(supplied.todos)}")

    state = "passed" if not failures else "blocked"
    artifact = {
        "source_id": spec.source_id,
        "spec_version": spec.spec_version,
        "spec_hash": expected_hash,
        "state": state,
        "evaluated_at": evaluated_at.isoformat(),
        "provider_api_version": spec.provider_api_version,
        "failures": failures,
        "evidence": [
            {
                "behavior_id": item.behavior_id,
                "kind": item.kind,
                "uri": item.uri,
                "schema_sha256": item.schema_sha256,
                "verified_at": (
                    item.verified_at.astimezone(timezone.utc).isoformat()
                    if item.verified_at
                    else None
                ),
            }
            for item in spec.evidence
        ],
        "input": asdict(supplied),
    }
    return CertificationDecision(
        source_id=spec.source_id,
        state=state,
        spec_hash=expected_hash,
        evaluated_at=evaluated_at,
        failures=tuple(failures),
        artifact=artifact,
    )


def release_manifest(
    inputs: Mapping[str, CertificationInput],
    *,
    legacy_ratchet_clean: bool,
    now: datetime | None = None,
) -> dict[str, object]:
    """Evaluate all 27; missing inputs remain explicit blockers."""

    evaluated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    decisions: list[CertificationDecision] = []
    missing: list[str] = []
    for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items():
        supplied = inputs.get(source_id)
        if supplied is None:
            missing.append(source_id)
            continue
        decisions.append(evaluate_certification(spec, supplied, now=evaluated_at))
    failures = {
        decision.source_id: list(decision.failures)
        for decision in decisions
        if decision.state != "passed"
    }
    if not legacy_ratchet_clean:
        failures["_architecture"] = ["strict legacy architecture ratchet failed"]
    state = (
        "passed"
        if not missing
        and not failures
        and len(decisions) == len(SOURCE_CERTIFICATION_CATALOG)
        else "blocked"
    )
    return {
        "manifest_version": 2,
        "state": state,
        "evaluated_at": evaluated_at.isoformat(),
        "required_sources": len(SOURCE_CERTIFICATION_CATALOG),
        "passed_sources": sum(item.state == "passed" for item in decisions),
        "missing_sources": missing,
        "failures": failures,
        "sources": [item.artifact for item in decisions],
        "legacy_ratchet_clean": legacy_ratchet_clean,
    }


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def sign_manifest(manifest: Mapping[str, object], signing_key: bytes) -> dict[str, str]:
    """Return content digest + HMAC without embedding the signing key."""

    if not signing_key:
        raise ValueError("signing_key must not be empty")
    body = canonical_json(manifest)
    return {
        "sha256": hashlib.sha256(body).hexdigest(),
        "hmac_sha256": hmac.new(signing_key, body, hashlib.sha256).hexdigest(),
    }


__all__ = [
    "canonical_json",
    "evaluate_certification",
    "release_manifest",
    "sign_manifest",
]
