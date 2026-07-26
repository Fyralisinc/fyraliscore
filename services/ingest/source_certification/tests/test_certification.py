from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone

import pytest

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
    SOURCE_CERTIFICATION_SPECS,
)
from services.ingest.source_certification.evaluator import (
    evaluate_certification,
    release_manifest,
    sign_manifest,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    CertificationInvariantError,
    ScenarioResult,
    SuiteResult,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    source_definition,
)


NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
GOOD_METRICS = (
    ("p50_latency_ms", 5.0),
    ("p95_latency_ms", 10.0),
    ("p99_latency_ms", 20.0),
    ("requests_per_second", 100.0),
    ("quota_units_per_second", 100.0),
    ("records_per_second", 100.0),
    ("bytes_per_second", 10_000.0),
    ("kafka_lag", 0.0),
    ("observation_p99_latency_ms", 50.0),
    ("retries", 0.0),
    ("rate_limited_responses", 0.0),
    ("dlq_entries", 0.0),
    ("missing_records", 0.0),
    ("cross_tenant_leaks", 0.0),
    ("unexpected_duplicates", 0.0),
    ("cooldown_violations", 0.0),
    ("cursor_consistency_errors", 0.0),
    ("hot_loops", 0.0),
    ("backlog_growth_per_second", 0.0),
    ("lab_capacity_ratio", 2.5),
    ("lab_p99_timeout_ratio", 0.05),
    ("quota_utilization_ratio", 0.95),
    ("headroom_ratio", 1.2),
    ("recovered_faults_ratio", 1.0),
    ("cpu_percent", 50.0),
    ("memory_bytes", 1_024.0),
    ("warmup_seconds", 120.0),
    ("validation_seconds", 900.0),
    ("soak_seconds", 3_600.0),
    ("search_tolerance_ratio", 0.04),
    ("offered_rate", 100.0),
    ("stable_rate", 95.0),
)


def _suite(kind: str) -> SuiteResult:
    return SuiteResult(
        kind=kind,  # type: ignore[arg-type]
        state="passed",
        artifact_uri=f"artifact://{kind}",
        started_at=NOW,
        completed_at=NOW,
        metrics=GOOD_METRICS,
        limiting_component="provider_quota",
    )


def _passing_input(spec) -> CertificationInput:  # noqa: ANN001
    suites = (_suite("historical"), _suite("live"), _suite("combined"))
    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="passed",
        local_correctness_artifact="artifact://correctness",
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state="passed",
                artifact_uri=f"artifact://scenario/{scenario_id}",
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=suites,
        fyralis_ceiling_suites=suites,
        fault_recovery_suites=suites,
        canary=CanaryResult(
            state="passed",
            operation_results=tuple(
                CanaryOperationResult(
                    operation_id=operation_id,
                    state="passed",
                    artifact_uri=f"artifact://canary/{operation_id}",
                )
                for operation_id in spec.canary.required_operations
            ),
            tested_at=NOW,
            account_type=spec.canary.account_type,
            api_version=spec.provider_api_version,
            artifact_uri="artifact://canary",
        ),
        legacy_reference_count=0,
    )


def _lock_evidence(spec):  # noqa: ANN001
    locked = tuple(
        replace(
            item,
            verified_at=NOW,
            schema_sha256=(
                "a" * 64
                if item.behavior_id == "used_api_surface"
                else item.schema_sha256
            ),
        )
        for item in spec.evidence
    )
    return replace(spec, evidence=locked)


def test_exactly_one_certification_spec_exists_for_every_source() -> None:
    assert tuple(SOURCE_CERTIFICATION_CATALOG) == CANONICAL_SOURCE_IDS
    assert len(SOURCE_CERTIFICATION_SPECS) == 27
    for spec in SOURCE_CERTIFICATION_SPECS:
        declared = source_definition(spec.source_id).certification
        assert declared.test_kit_id == f"ingest.test_kit.{spec.source_id}"
        assert declared.evidence_id == f"ingest.evidence.{spec.source_id}"
        assert spec.evidence_pack_id == declared.evidence_id
        assert spec.evidence_pack_version == "1.0.0"
        assert len(spec.evidence_pack_sha256) == 64
        assert declared.canary_id == spec.canary.canary_id


def test_every_source_declares_all_three_load_shapes() -> None:
    for spec in SOURCE_CERTIFICATION_SPECS:
        assert {suite.kind for suite in spec.load_suites} == {
            "historical",
            "live",
            "combined",
        }
    whatsapp = SOURCE_CERTIFICATION_CATALOG["whatsapp"]
    historical = next(s for s in whatsapp.load_suites if s.kind == "historical")
    assert historical.operation_mix == (
        "whatsapp.assert_history_unsupported",
    )


def test_canary_operations_cover_contract_requests_and_live_transports() -> None:
    for spec in SOURCE_CERTIFICATION_SPECS:
        source = source_definition(spec.source_id)
        assert spec.canary.required_operations == (
            "auth.conformance",
            *(
                f"provider_request.{operation_id}"
                for operation_id in source.operation_policy_ids
            ),
            *(
                f"live_transport.{transport}"
                for transport in source.live_transports
            ),
        )
        assert len(spec.canary.required_operations) <= spec.canary.max_requests


def test_unlocked_evidence_blocks_even_otherwise_passing_results() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    decision = evaluate_certification(
        spec,
        _passing_input(spec),
        now=NOW,
    )
    assert decision.state == "blocked"
    assert any("unverified evidence" in failure for failure in decision.failures)
    assert any("schema checksum" in failure for failure in decision.failures)


def test_locked_evidence_and_complete_results_can_pass() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    decision = evaluate_certification(
        spec,
        _passing_input(spec),
        now=NOW,
    )
    assert decision.state == "passed"
    assert decision.failures == ()


def test_no_todos_skips_or_legacy_references_can_be_waived() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["github"])
    supplied = replace(
        _passing_input(spec),
        skipped_tests=("secondary_limit_recovery",),
        todos=("pin webhook schema",),
        legacy_reference_count=1,
    )
    decision = evaluate_certification(spec, supplied, now=NOW)
    assert decision.state == "blocked"
    assert any("skipped tests" in item for item in decision.failures)
    assert any("TODOs" in item for item in decision.failures)
    assert any("legacy binding" in item for item in decision.failures)


def test_local_correctness_requires_exact_artifact_backed_scenario_coverage() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)

    missing = replace(
        passing,
        scenario_results=passing.scenario_results[:-1],
    )
    decision = evaluate_certification(spec, missing, now=NOW)
    assert decision.state == "blocked"
    assert any("scenario coverage missing" in item for item in decision.failures)

    extra = replace(
        passing,
        scenario_results=(
            *passing.scenario_results,
            ScenarioResult(
                scenario_id="undeclared_scenario",
                state="passed",
                artifact_uri="artifact://scenario/undeclared",
            ),
        ),
    )
    decision = evaluate_certification(spec, extra, now=NOW)
    assert decision.state == "blocked"
    assert any("unexpected IDs" in item for item in decision.failures)

    first = passing.scenario_results[0]
    failed = replace(
        passing,
        scenario_results=(
            replace(
                first,
                state="failed",
                failures=("assertion failed",),
            ),
            *passing.scenario_results[1:],
        ),
    )
    decision = evaluate_certification(spec, failed, now=NOW)
    assert decision.state == "blocked"
    assert any(
        f"scenario.{first.scenario_id} state is failed" in item
        for item in decision.failures
    )


def test_canary_requires_exact_artifact_backed_operation_coverage() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["github"])
    passing = _passing_input(spec)

    missing = replace(
        passing,
        canary=replace(
            passing.canary,
            operation_results=passing.canary.operation_results[:-1],
        ),
    )
    decision = evaluate_certification(spec, missing, now=NOW)
    assert decision.state == "blocked"
    assert any(
        "canary operation coverage missing" in item
        for item in decision.failures
    )

    extra = replace(
        passing,
        canary=replace(
            passing.canary,
            operation_results=(
                *passing.canary.operation_results,
                CanaryOperationResult(
                    operation_id="undeclared.operation",
                    state="passed",
                    artifact_uri="artifact://canary/undeclared",
                ),
            ),
        ),
    )
    decision = evaluate_certification(spec, extra, now=NOW)
    assert decision.state == "blocked"
    assert any("unexpected IDs" in item for item in decision.failures)

    first = passing.canary.operation_results[0]
    failed = replace(
        passing,
        canary=replace(
            passing.canary,
            operation_results=(
                replace(
                    first,
                    state="failed",
                    failures=("provider response mismatch",),
                ),
                *passing.canary.operation_results[1:],
            ),
        ),
    )
    decision = evaluate_certification(spec, failed, now=NOW)
    assert decision.state == "blocked"
    assert any(
        f"operation.{first.operation_id} state is failed" in item
        for item in decision.failures
    )


def test_duplicate_results_and_skipped_state_are_structurally_rejected() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    passing = _passing_input(spec)
    duplicate = passing.scenario_results[0]
    with pytest.raises(CertificationInvariantError, match="duplicate scenario"):
        replace(
            passing,
            scenario_results=(
                *passing.scenario_results,
                duplicate,
            ),
        )
    with pytest.raises(CertificationInvariantError, match="scenario state"):
        ScenarioResult(
            scenario_id="auth_success_and_expiry",
            state="skipped",  # type: ignore[arg-type]
            artifact_uri="artifact://scenario/skipped",
        )
    operation = passing.canary.operation_results[0]
    with pytest.raises(CertificationInvariantError, match="duplicate operation"):
        replace(
            passing.canary,
            operation_results=(
                *passing.canary.operation_results,
                operation,
            ),
        )
    with pytest.raises(FrozenInstanceError):
        passing.scenario_results[0].state = "failed"  # type: ignore[misc]


def test_release_manifest_requires_all_27_and_strict_ratchet() -> None:
    manifest = release_manifest({}, legacy_ratchet_clean=False, now=NOW)
    assert manifest["manifest_version"] == 2
    assert manifest["state"] == "blocked"
    assert manifest["required_sources"] == 27
    assert manifest["passed_sources"] == 0
    assert manifest["missing_sources"] == list(CANONICAL_SOURCE_IDS)
    assert "_architecture" in manifest["failures"]


def test_manifest_signature_is_deterministic_and_keyed() -> None:
    manifest = {"state": "blocked", "sources": []}
    first = sign_manifest(manifest, b"test-key")
    second = sign_manifest(manifest, b"test-key")
    other = sign_manifest(manifest, b"other-key")
    assert first == second
    assert first["sha256"] == other["sha256"]
    assert first["hmac_sha256"] != other["hmac_sha256"]
