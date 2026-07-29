from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

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
    ("step_seconds", 120.0),
    ("validation_seconds", 900.0),
    ("soak_seconds", 3_600.0),
    ("search_tolerance_ratio", 0.04),
    ("offered_rate", 100.0),
    ("stable_rate", 95.0),
    ("tenants", 2.0),
    ("installations_per_tenant", 2.0),
    ("replicas", 2.0),
    ("clock_mode_wall", 1.0),
    ("lab_calibration_elapsed_seconds", 30.0),
    ("typed_workload_declaration_bound", 1.0),
    ("executable_operation_coverage_ratio", 1.0),
    ("control_operation_coverage_ratio", 1.0),
    ("pipeline_e2e_proven", 1.0),
    ("promotion_eligible", 1.0),
    ("quota_config_verified", 1.0),
    ("wall_clock_duration_ratio", 1.0),
)


def _suite(
    kind: str,
    *,
    stable_rate: float = 95.0,
    offered_rate: float = 100.0,
    headroom_ratio: float = 1.2,
) -> SuiteResult:
    metrics = dict(GOOD_METRICS)
    metrics.update(
        stable_rate=stable_rate,
        offered_rate=offered_rate,
        headroom_ratio=headroom_ratio,
    )
    return SuiteResult(
        kind=kind,  # type: ignore[arg-type]
        state="passed",
        artifact_uri=f"artifact://{kind}",
        started_at=NOW,
        completed_at=NOW,
        metrics=tuple(metrics.items()),
        limiting_component="provider_quota",
    )


def _passing_input(spec) -> CertificationInput:  # noqa: ANN001
    provider_safe_suites = tuple(
        _suite(kind) for kind in ("historical", "live", "combined")
    )
    fyralis_ceiling_suites = tuple(
        _suite(
            kind,
            stable_rate=114.0,
            offered_rate=120.0,
            headroom_ratio=1.2,
        )
        for kind in ("historical", "live", "combined")
    )
    fault_recovery_suites = tuple(
        _suite(kind) for kind in ("historical", "live", "combined")
    )
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
        provider_safe_suites=provider_safe_suites,
        fyralis_ceiling_suites=fyralis_ceiling_suites,
        fault_recovery_suites=fault_recovery_suites,
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
            request_count=len(spec.canary.required_operations),
            account_identity_sha256="b" * 64,
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
    classified_canary = replace(
        spec.canary,
        operation_contracts=tuple(
            replace(
                contract,
                mutability="read",
                cleanup_action=None,
                classification_basis="test-owned explicit read classification",
            )
            for contract in spec.canary.operation_contracts
        ),
    )
    return replace(spec, evidence=locked, canary=classified_canary)


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
        for suite in spec.load_suites:
            assert bool(suite.executable_operations) != (
                suite.non_applicability is not None
            )
            for operation in suite.executable_operations:
                assert operation.operation_id.startswith(f"{spec.source_id}.")
                assert operation.evidence_id.startswith(
                    f"{spec.evidence_pack_id}."
                )
    whatsapp = SOURCE_CERTIFICATION_CATALOG["whatsapp"]
    historical = next(s for s in whatsapp.load_suites if s.kind == "historical")
    assert historical.operation_mix == (
        "whatsapp.assert_history_unsupported",
    )
    assert historical.executable_operations == ()
    assert historical.non_applicability is not None


def test_required_renewal_contract_gaps_block_combined_execution() -> None:
    renewal_sources = {
        "gmail",
        "google_calendar",
        "google_drive",
        "quickbooks",
        "ramp",
        "gusto",
        "carta",
        "linkedin",
    }
    for spec in SOURCE_CERTIFICATION_SPECS:
        combined = next(
            suite for suite in spec.load_suites if suite.kind == "combined"
        )
        renewal = next(
            assertion
            for assertion in combined.contract_absence_assertions
            if assertion.operation_id.endswith(
                ".token_or_watch_renewal"
            )
        )
        assert renewal.blocks_execution == (
            spec.source_id in renewal_sources
        )


def test_operation_mix_is_compatibility_only_for_typed_execution() -> None:
    combined = next(
        suite
        for suite in SOURCE_CERTIFICATION_CATALOG["slack"].load_suites
        if suite.kind == "combined"
    )
    compatibility_projection = replace(
        combined,
        operation_mix=(),
    )

    assert compatibility_projection.execution_workload_dict() == (
        combined.execution_workload_dict()
    )
    assert compatibility_projection.execution_workload_sha256 == (
        combined.execution_workload_sha256
    )
    assert compatibility_projection.data_operations == combined.data_operations
    assert (
        compatibility_projection.control_operations
        == combined.control_operations
    )


def test_historical_data_operations_require_positive_pipeline_outputs() -> None:
    historical = next(
        suite
        for suite in SOURCE_CERTIFICATION_CATALOG["slack"].load_suites
        if suite.kind == "historical"
    )
    fetch = next(iter(historical.data_operations))
    invalid_operations = tuple(
        replace(operation, raw_cardinality="zero_or_more")
        if operation.operation_id == fetch.operation_id
        else operation
        for operation in historical.executable_operations
    )

    with pytest.raises(
        CertificationInvariantError,
        match="historical data operations require positive",
    ):
        replace(historical, executable_operations=invalid_operations)


@pytest.mark.parametrize(
    ("changes", "match"),
    (
        ({"executable_binding": ""}, "executable_binding"),
        ({"evidence_id": ""}, "load evidence_id"),
        ({"quota_mappings": ()}, "quota_mapping receipt proof"),
        ({"raw_cardinality": "none"}, "data operations require"),
        ({"cursor_applicability": "optional"}, "cursor_consistency proof"),
    ),
)
def test_historical_fetch_rejects_missing_typed_declarations(
    changes: dict[str, object],
    match: str,
) -> None:
    historical = next(
        suite
        for suite in SOURCE_CERTIFICATION_CATALOG["slack"].load_suites
        if suite.kind == "historical"
    )
    fetch = next(iter(historical.data_operations))

    with pytest.raises(CertificationInvariantError, match=match):
        replace(fetch, **changes)


@pytest.mark.parametrize(
    ("changes", "match"),
    (
        ({"quota_mappings": ()}, "require quota mappings"),
        ({"cursor_applicability": "optional"}, "require cursor consistency"),
    ),
)
def test_historical_suite_rejects_missing_quota_or_cursor_declarations(
    changes: dict[str, object],
    match: str,
) -> None:
    historical = next(
        suite
        for suite in SOURCE_CERTIFICATION_CATALOG["slack"].load_suites
        if suite.kind == "historical"
    )
    fetch = next(iter(historical.data_operations))
    removed_proof = (
        "quota_mapping"
        if "quota_mappings" in changes
        else "cursor_consistency"
    )
    replacement = replace(
        fetch,
        **changes,
        receipt_proof_requirements=tuple(
            proof
            for proof in fetch.receipt_proof_requirements
            if proof != removed_proof
        ),
    )
    operations = tuple(
        replacement if operation.operation_id == fetch.operation_id else operation
        for operation in historical.executable_operations
    )

    with pytest.raises(CertificationInvariantError, match=match):
        replace(historical, executable_operations=operations)


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


def test_canary_mutability_uses_exact_http_binding_and_fails_closed() -> None:
    slack = SOURCE_CERTIFICATION_CATALOG["slack"].canary
    assert (
        slack.operation_contract_for(
            "provider_request.conversations.list"
        ).mutability
        == "read"
    )
    assert (
        slack.operation_contract_for(
            "provider_request.chat.postMessage"
        ).mutability
        == "unclassified"
    )
    assert (
        slack.operation_contract_for("live_transport.webhook").mutability
        == "unclassified"
    )

    # A safe-looking HTTP method cannot override an unsafe source policy.
    facebook = SOURCE_CERTIFICATION_CATALOG["facebook_pages"].canary
    assert (
        facebook.operation_contract_for(
            "provider_request.oauth.token.exchange"
        ).mutability
        == "unclassified"
    )
    assert (
        facebook.operation_contract_for("live_transport.api_poll").mutability
        == "read"
    )

    # Protocol-only operations have no HTTP-method evidence to infer from.
    telegram = SOURCE_CERTIFICATION_CATALOG["telegram"].canary
    assert (
        telegram.operation_contract_for(
            "provider_request.session.connect"
        ).mutability
        == "unclassified"
    )
    assert (
        telegram.operation_contract_for("live_transport.mtproto").mutability
        == "unclassified"
    )


def test_unlocked_evidence_blocks_even_otherwise_passing_results() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    decision = evaluate_certification(
        spec,
        _passing_input(spec),
        now=NOW,
    )
    assert decision.state == "blocked"
    assert any("unverified evidence" in failure for failure in decision.failures)
    assert not any("schema checksum" in failure for failure in decision.failures)
    surface = next(
        item
        for item in spec.evidence
        if item.behavior_id == "used_api_surface"
    )
    assert surface.schema_sha256 is not None


def test_locked_evidence_and_complete_results_can_pass() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    decision = evaluate_certification(
        spec,
        _passing_input(spec),
        now=NOW,
    )
    assert decision.state == "passed"
    assert decision.failures == ()


def test_declared_not_applicable_load_shape_is_neutral() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["whatsapp"])
    passing = _passing_input(spec)

    def neutralize_historical(
        suites: tuple[SuiteResult, ...],
    ) -> tuple[SuiteResult, ...]:
        return tuple(
            SuiteResult(
                kind="historical",
                state="not_applicable",
                artifact_uri="artifact://historical-not-applicable",
            )
            if suite.kind == "historical"
            else suite
            for suite in suites
        )

    supplied = replace(
        passing,
        provider_safe_suites=neutralize_historical(
            passing.provider_safe_suites,
        ),
        fyralis_ceiling_suites=neutralize_historical(
            passing.fyralis_ceiling_suites,
        ),
        fault_recovery_suites=neutralize_historical(
            passing.fault_recovery_suites,
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "passed"
    assert decision.failures == ()


def test_not_applicable_load_shape_is_rejected_for_executable_declaration() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        provider_safe_suites=tuple(
            SuiteResult(
                kind="historical",
                state="not_applicable",
                artifact_uri="artifact://invalid-not-applicable",
            )
            if suite.kind == "historical"
            else suite
            for suite in passing.provider_safe_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert (
        "provider_safe.historical is not_applicable but the source declaration "
        "is executable"
    ) in decision.failures


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (
            NOW - timedelta(hours=25),
            "timestamps are older than 24 hours",
        ),
        (
            NOW + timedelta(minutes=6),
            "timestamps are in the future",
        ),
    ],
)
def test_passing_suite_timestamps_must_be_fresh_and_not_future(
    timestamp: datetime,
    expected: str,
) -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        provider_safe_suites=tuple(
            replace(
                suite,
                started_at=timestamp,
                completed_at=timestamp,
            )
            if suite.kind == "live"
            else suite
            for suite in passing.provider_safe_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert any(expected in failure for failure in decision.failures)


def test_passing_suite_requires_a_complete_execution_window() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        provider_safe_suites=tuple(
            replace(suite, started_at=None, completed_at=None)
            if suite.kind == "live"
            else suite
            for suite in passing.provider_safe_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert (
        "provider_safe.live requires started_at and completed_at"
        in decision.failures
    )


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (
            NOW - timedelta(hours=25),
            "tested_at is older than 24 hours",
        ),
        (
            NOW + timedelta(minutes=6),
            "tested_at is in the future",
        ),
    ],
)
def test_passing_canary_timestamp_must_be_fresh_and_not_future(
    timestamp: datetime,
    expected: str,
) -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        canary=replace(passing.canary, tested_at=timestamp),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert any(expected in failure for failure in decision.failures)


def test_suite_topology_must_match_catalog_declaration() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    live = next(
        suite for suite in passing.provider_safe_suites if suite.kind == "live"
    )
    metrics = dict(live.metrics)
    metrics["replicas"] = 1.0
    supplied = replace(
        passing,
        provider_safe_suites=tuple(
            replace(suite, metrics=tuple(metrics.items()))
            if suite.kind == "live"
            else suite
            for suite in passing.provider_safe_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert (
        "provider_safe.live.replicas must equal declared topology value 2"
        in decision.failures
    )


def test_short_or_synthetic_load_provenance_cannot_pass() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    live = next(
        suite for suite in passing.provider_safe_suites if suite.kind == "live"
    )
    metrics = dict(live.metrics)
    metrics.update(
        clock_mode_wall=0.0,
        lab_calibration_elapsed_seconds=0.01,
        typed_workload_declaration_bound=0.0,
        executable_operation_coverage_ratio=0.5,
        control_operation_coverage_ratio=0.5,
        pipeline_e2e_proven=0.0,
        promotion_eligible=0.0,
        quota_config_verified=0.0,
        step_seconds=1.0,
        wall_clock_duration_ratio=0.01,
    )
    supplied = replace(
        passing,
        provider_safe_suites=tuple(
            replace(suite, metrics=tuple(metrics.items()))
            if suite.kind == "live"
            else suite
            for suite in passing.provider_safe_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    expected = {
        "provider_safe.live.clock_mode_wall must equal 1",
        "provider_safe.live.promotion_eligible must equal 1",
        "provider_safe.live.pipeline_e2e_proven must equal 1",
        "provider_safe.live.typed_workload_declaration_bound must equal 1",
        "provider_safe.live.executable_operation_coverage_ratio must equal 1",
        "provider_safe.live.control_operation_coverage_ratio must equal 1",
        "provider_safe.live.quota_config_verified must equal 1",
    }
    assert expected <= set(decision.failures)


@pytest.mark.parametrize(
    ("ceiling_rate", "headroom", "expected_failure"),
    [
        (
            90.0,
            90.0 / 95.0,
            "stable_rate must be >= provider_safe.live.stable_rate",
        ),
        (
            114.0,
            1.1,
            "headroom_ratio must equal ceiling stable_rate / "
            "provider-safe stable_rate",
        ),
    ],
)
def test_ceiling_and_headroom_must_match_measured_stable_rates(
    ceiling_rate: float,
    headroom: float,
    expected_failure: str,
) -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        fyralis_ceiling_suites=tuple(
            _suite(
                suite.kind,
                stable_rate=ceiling_rate,
                offered_rate=max(100.0, ceiling_rate),
                headroom_ratio=headroom,
            )
            if suite.kind == "live"
            else suite
            for suite in passing.fyralis_ceiling_suites
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert any(
        expected_failure in failure for failure in decision.failures
    )


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


def test_canary_request_budget_is_enforced_by_the_evaluator() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    passing = _passing_input(spec)
    supplied = replace(
        passing,
        canary=replace(
            passing.canary,
            request_count=spec.canary.max_requests + 1,
        ),
    )

    decision = evaluate_certification(spec, supplied, now=NOW)

    assert decision.state == "blocked"
    assert any(
        "request count exceeds" in failure for failure in decision.failures
    )


def test_canary_with_unclassified_operation_cannot_pass() -> None:
    spec = _lock_evidence(SOURCE_CERTIFICATION_CATALOG["slack"])
    provider_operation = next(
        contract
        for contract in spec.canary.operation_contracts
        if contract.operation_id.startswith("provider_request.")
    )
    spec = replace(
        spec,
        canary=replace(
            spec.canary,
            operation_contracts=tuple(
                replace(
                    contract,
                    mutability="unclassified",
                    cleanup_action=None,
                    classification_basis=(
                        "test deliberately leaves this operation unclassified"
                    ),
                )
                if contract.operation_id == provider_operation.operation_id
                else contract
                for contract in spec.canary.operation_contracts
            ),
        ),
    )

    decision = evaluate_certification(spec, _passing_input(spec), now=NOW)

    assert decision.state == "blocked"
    assert any(
        provider_operation.operation_id in failure
        and "mutability is unclassified" in failure
        for failure in decision.failures
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
