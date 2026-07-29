from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.ingest.source_certification.distributed_transport_diagnostic import (
    DISTRIBUTED_TRANSPORT_REDIS_ENV,
)
from services.ingest.source_certification.execution_driver import (
    ExecutionDriverError,
    LoadStageOptions,
    STAGE_ARTIFACT_SCHEMA_VERSION,
    declared_execution_plan_sha256,
    run_stage,
)
from services.ingest.source_certification.io import load_certification_input
from services.ingest.source_certification.load_search import (
    LoadTopology,
    VerifiedQuotaEvidence,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_DATA_PLANE_SCENARIO_IDS,
    PIPELINE_SCENARIO_IDS,
    PIPELINE_TOPOLOGY_SCENARIO_IDS,
)
from services.ingest.source_certification.tests.pipeline_test_fixtures import (
    passing_pipeline_probe,
)
from services.ingest.source_certification.stage_artifacts import (
    CANARY_EXECUTION_SCHEMA_VERSION,
    validate_stage_artifact,
)
from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


def _passing_pipeline_probe(source_id: str) -> dict[str, object]:
    return passing_pipeline_probe(source_id)


def test_idempotency_replay_promotion_applies_to_all_27_sources() -> None:
    assert len(CANONICAL_SOURCE_IDS) == 27
    assert all(
        "duplicate_delivery_and_idempotency"
        in SOURCE_CERTIFICATION_CATALOG[source_id].required_scenarios
        for source_id in CANONICAL_SOURCE_IDS
    )
    assert "duplicate_delivery_and_idempotency" in PIPELINE_SCENARIO_IDS


async def test_all_catalog_load_suites_emit_typed_pipeline_artifacts(
    tmp_path: Path,
) -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items():
        artifacts, provider_safe, fyralis_ceiling = (
            await driver._run_typed_pipeline_loads(  # noqa: SLF001
                source_id,
                ambient_env={},
                artifact_dir=tmp_path / source_id,
                adapter_factory=None,
            )
        )
        declared_kinds = {suite.kind for suite in spec.load_suites}
        assert set(artifacts) == {"provider_safe", "fyralis_ceiling"}
        assert all(set(mode_artifacts) == declared_kinds for mode_artifacts in artifacts.values())
        assert {result.kind for result in provider_safe} == declared_kinds
        assert {result.kind for result in fyralis_ceiling} == declared_kinds
        for suite in spec.load_suites:
            for mode in artifacts:
                artifact = artifacts[mode][suite.kind]
                assert artifact["workload"] == suite.execution_workload_dict()
                assert (
                    tmp_path / source_id / "pipeline_load" / suite.kind / f"{mode}.json"
                ).is_file()
                expected_state = (
                    "not_applicable"
                    if suite.non_applicability is not None
                    else "blocked"
                )
                assert artifact["state"] == expected_state


async def test_local_driver_writes_strict_source_isolated_used_surface_evidence(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"

    supplied = await run_stage(
        source_id="slack",
        stage="local_correctness",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={},
    )

    parsed = load_certification_input(result_path)
    artifact = json.loads(
        (artifact_dir / "stage.json").read_text(encoding="utf-8")
    )
    surface = artifact["provider_lab_used_surface"]
    assert parsed == supplied
    assert parsed.local_correctness == "blocked"
    assert artifact["schema_version"] == STAGE_ARTIFACT_SCHEMA_VERSION
    assert artifact["source_id"] == "slack"
    assert artifact["synthetic_promotion_allowed"] is False
    assert artifact["execution_plan_sha256"] == (
        declared_execution_plan_sha256("slack")
    )
    fixture_probe = artifact["fixture_and_binding_probe"]
    assert fixture_probe["deterministic_repeat"] is True
    assert fixture_probe["sibling_fixture_is_distinct"] is True
    assert fixture_probe["exact_observation_count"] == 150
    assert fixture_probe["count_oracle_deterministic"] is True
    assert fixture_probe["all_callable_bindings_resolved"] is True
    assert fixture_probe["live_target"]["state"] == "constructed"
    assert surface["source_isolation"] is True
    assert surface["unknown_route_status"] == 501
    assert surface["four_scope_request_ledger"]["passed"] is True
    assert {
        result["route_id"] for result in surface["route_results"]
    } == {
        "slack.conversations_list",
        "slack.conversations_history",
        "slack.conversations_info",
        "slack.users_info",
        "slack.chat_post_message",
        "slack.oauth_access",
    }
    scenario_rows = artifact["scenario_execution_ledger"]
    assert [row["scenario_id"] for row in scenario_rows] == [
        result.scenario_id for result in supplied.scenario_results
    ]
    assert all(
        row["certification_state"] == "blocked" for row in scenario_rows
    )
    special = next(
        row
        for row in scenario_rows
        if row["scenario_id"] == "thread_refetch_and_full_thread_upsert"
    )
    assert special["measured_probe_ids"] == []
    assert special["unmeasured_probe_ids"] == [
        "source_specific_executor_absent"
    ]


async def test_local_driver_promotes_only_executed_pipeline_scenarios(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _pipeline(**_kwargs) -> dict[str, object]:
        return _passing_pipeline_probe("slack")

    monkeypatch.setattr(
        "services.ingest.source_certification.execution_driver."
        "run_pipeline_probe",
        _pipeline,
    )
    supplied = await run_stage(
        source_id="slack",
        stage="local_correctness",
        result_path=tmp_path / "result.json",
        artifact_dir=tmp_path / "artifacts",
        ambient_env={},
    )

    by_id = {
        result.scenario_id: result for result in supplied.scenario_results
    }
    assert supplied.local_correctness == "blocked"
    assert {
        scenario_id
        for scenario_id, result in by_id.items()
        if result.state == "passed"
    } == PIPELINE_SCENARIO_IDS
    assert all(
        result.failures == ()
        for scenario_id, result in by_id.items()
        if scenario_id in PIPELINE_SCENARIO_IDS
    )
    assert all(
        result.state == "blocked"
        for scenario_id, result in by_id.items()
        if scenario_id not in PIPELINE_SCENARIO_IDS
    )
    artifact = json.loads(
        (tmp_path / "artifacts" / "stage.json").read_text(
            encoding="utf-8",
        ),
    )
    states = {
        row["scenario_id"]: row["certification_state"]
        for row in artifact["scenario_execution_ledger"]
    }
    assert {
        scenario_id
        for scenario_id, state in states.items()
        if state == "passed"
    } == PIPELINE_SCENARIO_IDS
    duplicate = next(
        row
        for row in artifact["scenario_execution_ledger"]
        if row["scenario_id"] == "duplicate_delivery_and_idempotency"
    )
    assert duplicate["declared_probe_ids"] == [
        "idempotency_builder_resolution",
        "observation_idempotency_replay",
    ]
    assert duplicate["measured_probe_ids"] == duplicate["declared_probe_ids"]
    assert duplicate["unmeasured_probe_ids"] == []
    assert duplicate["unproven_requirements"] == []


async def test_whatsapp_ledger_keeps_history_topology_explicitly_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _pipeline(**_kwargs) -> dict[str, object]:
        return _passing_pipeline_probe("whatsapp")

    monkeypatch.setattr(
        "services.ingest.source_certification.execution_driver."
        "run_pipeline_probe",
        _pipeline,
    )
    supplied = await run_stage(
        source_id="whatsapp",
        stage="local_correctness",
        result_path=tmp_path / "result.json",
        artifact_dir=tmp_path / "artifacts",
        ambient_env={},
    )

    passed = {
        result.scenario_id
        for result in supplied.scenario_results
        if result.state == "passed"
    }
    assert passed == PIPELINE_DATA_PLANE_SCENARIO_IDS
    artifact = json.loads(
        (tmp_path / "artifacts" / "stage.json").read_text(
            encoding="utf-8",
        ),
    )
    topology_rows = [
        row
        for row in artifact["scenario_execution_ledger"]
        if row["scenario_id"] in PIPELINE_TOPOLOGY_SCENARIO_IDS
    ]
    assert len(topology_rows) == 2
    assert all(row["certification_state"] == "blocked" for row in topology_rows)
    assert all(
        row["unproven_requirements"]
        == [
            "live-only topology blocker: WhatsApp has no historical "
            "onboarding work, and the current Kafka normalizer/writer path "
            "exposes no durable per-replica work attribution"
        ]
        for row in topology_rows
    )


async def test_load_driver_measures_lab_but_does_not_guess_provider_quota(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"

    supplied = await run_stage(
        source_id="github",
        stage="load",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={},
        load_request_count=4,
    )

    artifact = json.loads(
        (artifact_dir / "stage.json").read_text(encoding="utf-8")
    )
    diagnostic = artifact["load_diagnostic"]
    assert all(
        suite.state == "blocked" for suite in supplied.provider_safe_suites
    )
    assert all(
        suite.state == "blocked" for suite in supplied.fyralis_ceiling_suites
    )
    assert diagnostic["quota_disabled"]["request_count"] == 4
    assert diagnostic["quota_disabled"]["requests_per_second"] > 0
    assert diagnostic["declared_surface_mix"]["state"] == "diagnostic_only"
    assert set(
        diagnostic["declared_surface_mix"]["http_operation_ids"]
    ) == set(
        diagnostic["declared_surface_mix"]["declared_operation_ids"]
    )
    assert diagnostic["provider_safe"]["state"] == "blocked"
    assert "FYRALIS_PROVIDER_QUOTAS_JSON" in diagnostic["provider_safe"]["reason"]
    distributed = diagnostic["distributed_provider_transport"]
    assert distributed["state"] == "blocked"
    assert distributed["exact_assertions_passed"] is False
    assert distributed["synthetic_promotion_allowed"] is False
    offered = artifact["offered_load"]
    assert offered["state"] == "diagnostic_only"
    assert offered["clock_mode"] == "virtual"
    assert offered["quota_configuration"]["state"] == "blocked"
    assert {
        suite["kind"] for suite in offered["suites"]
    } == {"historical", "live", "combined"}
    typed = artifact["pipeline_load_artifacts"]
    assert set(typed) == {"provider_safe", "fyralis_ceiling"}
    assert all(
        suite["state"] == "blocked"
        for mode in typed.values()
        for suite in mode.values()
    )
    for suite in offered["suites"]:
        ceiling = suite["provider_lab_diagnostic"]["fyralis_ceiling"]
        assert ceiling["state"] == "blocked"
        assert (
            "safety cap" in ceiling["reason"]
            or "calibration did not successfully exercise every" in ceiling["reason"]
        )
        assert not (
            artifact_dir
            / "provider_lab_load"
            / suite["kind"]
            / "fyralis_ceiling.json"
        ).exists()


async def test_provider_safe_diagnostic_requires_exact_evidence_labelled_budget(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"
    quota = {
        "slack": {
            "bucket": "web-api",
            "capacity": 1,
            "cost": 1,
            "refill_per_second": 0,
            "scope": "method/workspace/app",
            "limit_id": "web-api-steady",
            "evidence_uri": "https://api.slack.com/docs/rate-limits",
            "verified_at": "2026-07-27T00:00:00+00:00",
        }
    }

    supplied = await run_stage(
        source_id="slack",
        stage="load",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={"FYRALIS_PROVIDER_QUOTAS_JSON": json.dumps(quota)},
        load_request_count=3,
    )

    artifact = json.loads(
        (artifact_dir / "stage.json").read_text(encoding="utf-8")
    )
    diagnostic = artifact["load_diagnostic"]["provider_safe"]
    assert diagnostic["state"] == "diagnostic_only"
    assert diagnostic["status_counts"] == {"200": 1, "429": 2}
    assert all(
        suite.state == "blocked" for suite in supplied.provider_safe_suites
    )
    for suite in artifact["offered_load"]["suites"]:
        provider_safe = suite["provider_lab_diagnostic"]["provider_safe"]
        assert provider_safe["state"] == "blocked"
        assert "safety cap" in provider_safe["reason"]
        assert not (
            artifact_dir
            / "provider_lab_load"
            / suite["kind"]
            / "provider_safe.json"
        ).exists()
    quota_configuration = artifact["offered_load"]["quota_configuration"]
    assert quota_configuration["state"] == "verified"
    assert quota_configuration["evidence"][0]["bucket"] == "web-api"
    assert quota_configuration["evidence"][0]["cost"] == 1
    assert (
        quota_configuration["evidence"][0]["limit_id"]
        == "web-api-steady"
    )
    assert all(
        dict(suite.metrics)["quota_config_verified"] == 0
        for suite in supplied.provider_safe_suites
    )
    assert all(
        pipeline_artifact["quota"] is None
        for pipeline_artifact in artifact["pipeline_load_artifacts"][
            "provider_safe"
        ].values()
    )


async def test_live_only_ingress_load_uses_lab_without_inventing_an_operation(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"

    await run_stage(
        source_id="whatsapp",
        stage="load",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={},
        load_request_count=3,
    )

    artifact = json.loads(
        (artifact_dir / "stage.json").read_text(encoding="utf-8")
    )
    diagnostic = artifact["load_diagnostic"]["quota_disabled"]
    assert diagnostic["execution_boundary"] == "provider_lab_live_ingress"
    assert diagnostic["operation_id"] is None
    assert diagnostic["request_count"] == 3


def test_quota_scope_evidence_controls_shared_budget_identity() -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    lanes = driver._load_lanes(  # noqa: SLF001
        LoadTopology(tenants=2, installations_per_tenant=2, replicas=2),
    )
    operation = driver._offered_operation_plan(  # noqa: SLF001
        "slack",
        SOURCE_CERTIFICATION_CATALOG["slack"].load_suites[0],
    )[0]

    def scopes(scope: str) -> set[str]:
        evidence = VerifiedQuotaEvidence(
            bucket="web-api",
            scope=scope,
            capacity=1,
            refill_per_second=1,
            evidence_uri="https://docs.example.test/quota",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        )
        return {
            driver._quota_scope_key(  # noqa: SLF001
                source_id="slack",
                evidence=evidence,
                lane=lane,
                operation=operation,
            )
            for lane in lanes
        }

    assert len(scopes("app")) == 1
    assert len(scopes("tenant")) == 2
    assert len(scopes("workspace/app")) == 4
    # Replicas share provider quota state; they never create extra scopes.
    assert len(scopes("installation")) == 4


def test_provider_lab_http_plan_uses_exact_operation_bindings() -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    gmail = driver._provider_lab_http_operation_plan("gmail")  # noqa: SLF001
    topic = [
        operation
        for operation in gmail
        if operation.route.route_id == "gmail.pubsub_topic"
    ]

    assert [
        (operation.operation_id, operation.method)
        for operation in topic
    ] == [
        ("pubsub.topic.create", "PUT"),
        ("pubsub.topic.delete", "DELETE"),
    ]
    assert all(
        operation.binding
        == operation.route.binding_for(operation.operation_id)
        for operation in gmail
        if operation.operation_id is not None
    )
    fireflies = [
        operation
        for operation in driver._provider_lab_http_operation_plan(  # noqa: SLF001
            "fireflies",
        )
        if operation.operation_id is not None
    ]
    assert len({operation.binding.body for operation in fireflies}) == 3


async def test_runner_models_independent_quota_scopes_costs_and_cardinality() -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    suite = SOURCE_CERTIFICATION_CATALOG["slack"].load_suites[0]
    topology = LoadTopology(
        tenants=2,
        installations_per_tenant=2,
        replicas=2,
    )
    evidence = (
        VerifiedQuotaEvidence(
            bucket="web-api",
            scope="app",
            limit_id="hour",
            cost=2,
            capacity=10,
            refill_per_second=0,
            evidence_uri="https://docs.example.test/app-hour",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        ),
        VerifiedQuotaEvidence(
            bucket="web-api",
            scope="workspace",
            limit_id="minute",
            cost=1,
            capacity=5,
            refill_per_second=0,
            evidence_uri="https://docs.example.test/workspace-minute",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        ),
    )
    app = driver.build_provider_lab_app(
        registry=driver.build_lab_adapter_registry(),
        fixtures={"slack": [dict(driver._golden_fixture("slack"))]},  # noqa: SLF001
    )
    runner = driver._ProviderLabOfferedLoadRunner(  # noqa: SLF001
        source_id="slack",
        suite=suite,
        topology=topology,
        options=LoadStageOptions(offer_limit_rate=10),
        app=app,
        quota_evidence=evidence,
    )

    measured = await runner(
        1,
        1,
        "validation",
        "provider_safe",
    )
    snapshot = app.state.provider_lab.quotas.snapshot()
    by_limit = {
        limit_id: [item for item in snapshot if item["limit_id"] == limit_id]
        for limit_id in {"hour", "minute"}
    }

    # App quota is global to the source; workspace quota is per installation.
    # Neither cardinality is multiplied by the two worker replicas.
    assert len(by_limit["hour"]) == 1
    assert len(by_limit["minute"]) == 4
    assert measured.quota_units_per_second == 3
    assert by_limit["hour"][0]["tokens"] == 8
    assert sorted(item["tokens"] for item in by_limit["minute"]) == [4, 5, 5, 5]


def test_quota_evidence_accepts_same_route_bucket_with_distinct_constraints() -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    entries = [
        {
            "bucket": "web-api",
            "scope": "app",
            "limit_id": "hour",
            "cost": 2,
            "capacity": 100,
            "refill_per_second": 1,
            "evidence_uri": "https://docs.example.test/app-hour",
            "verified_at": "2026-07-27T00:00:00+00:00",
        },
        {
            "bucket": "web-api",
            "scope": "workspace",
            "limit_id": "minute",
            "cost": 1,
            "capacity": 20,
            "refill_per_second": 2,
            "evidence_uri": "https://docs.example.test/workspace-minute",
            "verified_at": "2026-07-27T00:00:00+00:00",
        },
    ]
    routes = driver.build_lab_adapter_registry().require("slack").routes
    evidence, note = driver._load_quota_evidence(  # noqa: SLF001
        "slack",
        routes,
        ambient_env={
            "FYRALIS_PROVIDER_QUOTAS_JSON": json.dumps({"slack": entries}),
        },
    )

    assert len(evidence) == 2
    assert {(item.scope, item.limit_id, item.cost) for item in evidence} == {
        ("app", "hour", 2),
        ("workspace", "minute", 1),
    }
    assert "independent" in note

    duplicated, duplicate_note = driver._load_quota_evidence(  # noqa: SLF001
        "slack",
        routes,
        ambient_env={
            "FYRALIS_PROVIDER_QUOTAS_JSON": json.dumps(
                {"slack": [entries[0], entries[0]]},
            ),
        },
    )
    assert duplicated == ()
    assert "duplicate constraints" in duplicate_note


async def test_offer_limit_is_a_safety_cap_not_synthetic_instability() -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    suite = SOURCE_CERTIFICATION_CATALOG["slack"].load_suites[0]
    app = driver.build_provider_lab_app(
        registry=driver.build_lab_adapter_registry(),
        fixtures={"slack": [dict(driver._golden_fixture("slack"))]},  # noqa: SLF001
    )
    runner = driver._ProviderLabOfferedLoadRunner(  # noqa: SLF001
        source_id="slack",
        suite=suite,
        topology=LoadTopology.from_suite(suite),
        options=LoadStageOptions(offer_limit_rate=5),
        app=app,
        quota_evidence=(),
    )

    with pytest.raises(ExecutionDriverError, match="safety cap"):
        await runner(5.01, 1, "step", "fyralis_ceiling")
    assert app.state.provider_lab.ledger.list(source="slack") == []


async def test_calibration_exercises_every_typed_data_operation_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )
    from services.ingest.synthetic.provider_lab.calibration import (  # noqa: PLC0415
        LabCalibration,
    )

    suite = SOURCE_CERTIFICATION_CATALOG["slack"].load_suites[0]
    app = driver.build_provider_lab_app(
        registry=driver.build_lab_adapter_registry(),
        fixtures={"slack": [dict(driver._golden_fixture("slack"))]},  # noqa: SLF001
    )
    runner = driver._ProviderLabOfferedLoadRunner(  # noqa: SLF001
        source_id="slack",
        suite=suite,
        topology=LoadTopology.from_suite(suite),
        options=LoadStageOptions(offer_limit_rate=5),
        app=app,
        quota_evidence=(),
    )
    captured_minimum_samples = 0

    async def _calibrate(call, *, config):  # noqa: ANN001,ANN202
        nonlocal captured_minimum_samples
        captured_minimum_samples = config.minimum_samples
        for _index in range(config.minimum_samples):
            assert await call() is True
        return LabCalibration(
            target_fyralis_rps=config.target_fyralis_rps,
            elapsed_seconds=1,
            attempted_requests=config.minimum_samples,
            successful_requests=config.minimum_samples,
            failed_requests=0,
            achieved_requests_per_second=config.target_fyralis_rps * 2,
            capacity_ratio=2,
            p99_latency_ms=1,
            p99_timeout_ratio=0.001,
            minimum_samples=config.minimum_samples,
            minimum_capacity_ratio=2,
            maximum_p99_timeout_ratio=0.1,
        )

    monkeypatch.setattr(driver, "calibrate_provider_lab", _calibrate)
    calibration = await driver._calibrate_offered_load_runner(  # noqa: SLF001
        runner,
    )

    assert captured_minimum_samples == len(runner.operation_plan)
    assert calibration.passed is True


async def test_wall_clock_runner_fails_when_achieved_rate_lags_offer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.source_certification import (  # noqa: PLC0415
        execution_driver as driver,
    )

    suite = SOURCE_CERTIFICATION_CATALOG["slack"].load_suites[0]
    registry = driver.build_lab_adapter_registry()
    app = driver.build_provider_lab_app(
        registry=registry,
        fixtures={"slack": [dict(driver._golden_fixture("slack"))]},  # noqa: SLF001
    )
    runner = driver._ProviderLabOfferedLoadRunner(  # noqa: SLF001
        source_id="slack",
        suite=suite,
        topology=LoadTopology.from_suite(suite),
        options=LoadStageOptions(
            clock_mode="wall",
            offer_limit_rate=10,
        ),
        app=app,
        quota_evidence=(),
    )
    original_request = driver._provider_request  # noqa: SLF001

    async def _slow_request(*args, **kwargs):  # noqa: ANN002,ANN003,ANN202
        await asyncio.sleep(0.6)
        return await original_request(*args, **kwargs)

    monkeypatch.setattr(driver, "_provider_request", _slow_request)
    measured = await runner(
        2,
        1,
        "validation",
        "fyralis_ceiling",
    )

    assert measured.wall_elapsed_seconds >= 1.2
    assert measured.requests_per_second < measured.offered_rate * 0.99
    assert measured.backlog_growth_per_second > 0
    assert measured.stable is False
    assert measured.limiting_component == "provider_lab_capacity"


def test_promotion_load_options_require_wall_clock_soak_and_calibration() -> None:
    with pytest.raises(ValueError, match="wall-clock"):
        LoadStageOptions(promotion=True, include_soak=True)
    with pytest.raises(ValueError, match="weekly soak"):
        LoadStageOptions(promotion=True, clock_mode="wall")
    with pytest.raises(ValueError, match="30 seconds"):
        LoadStageOptions(
            promotion=True,
            clock_mode="wall",
            include_soak=True,
            calibration_probe_seconds=29,
        )


async def test_fault_driver_uses_provider_transport_without_claiming_full_recovery(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"

    supplied = await run_stage(
        source_id="slack",
        stage="fault_recovery",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={},
    )

    artifact = json.loads(
        (artifact_dir / "stage.json").read_text(encoding="utf-8")
    )
    recovered = artifact["fault_recovery_diagnostic"]["recovered_faults"]
    assert artifact["fault_recovery_diagnostic"]["retry_safe_operation_count"] == 4
    assert len(recovered) == 8
    assert {item["operation_id"] for item in recovered} == {
        "conversations.list",
        "conversations.history",
        "conversations.info",
        "users.info",
    }
    assert {item["injected_status"] for item in recovered} == {429, 503}
    assert all(item["fault_hits"] == 1 for item in recovered)
    assert all(item["ledger_attempts"] >= 2 for item in recovered)
    assert artifact["fault_recovery_diagnostic"]["failed_faults"] == []
    distributed = artifact["fault_recovery_diagnostic"][
        "distributed_provider_transport"
    ]
    assert distributed["state"] == "blocked"
    assert distributed["exact_assertions_passed"] is False
    assert distributed["synthetic_promotion_allowed"] is False
    assert all(
        suite.state == "blocked" for suite in supplied.fault_recovery_suites
    )


@pytest.mark.requires_infra
@pytest.mark.skipif(
    not os.environ.get("FYRALIS_TEST_REDIS_URL"),
    reason="FYRALIS_TEST_REDIS_URL is required for the real-Redis diagnostic",
)
async def test_load_and_fault_artifacts_record_exact_two_replica_redis_proof(
    tmp_path: Path,
) -> None:
    redis_url = os.environ["FYRALIS_TEST_REDIS_URL"]

    for stage in ("load", "fault_recovery"):
        stage_dir = tmp_path / stage
        supplied = await run_stage(
            source_id="slack",
            stage=stage,
            result_path=stage_dir / "result.json",
            artifact_dir=stage_dir / "artifacts",
            ambient_env={
                DISTRIBUTED_TRANSPORT_REDIS_ENV: redis_url,
            },
            load_request_count=2,
        )

        artifact = json.loads(
            (stage_dir / "artifacts" / "stage.json").read_text(
                encoding="utf-8",
            )
        )
        diagnostic = (
            artifact["load_diagnostic"]
            if stage == "load"
            else artifact["fault_recovery_diagnostic"]
        )
        distributed = diagnostic["distributed_provider_transport"]
        assert distributed["state"] == "passed"
        assert distributed["exact_assertions_passed"] is True
        assert distributed["failed_assertions"] == []
        assert all(distributed["assertions"].values())
        assert distributed["replica_count"] == 2
        assert len(set(distributed["redis_connection_ids"])) == 2
        assert (
            distributed["cooldown"][
                "observer_callback_count_before_deadline"
            ]
            == 0
        )
        assert distributed["weighted_tenant_isolation"][
            "tenant_b_second_result"
        ] == "tenant-b-2"
        assert distributed["synthetic_promotion_allowed"] is False
        assert artifact["synthetic_promotion_allowed"] is False
        assert all(
            suite.state == "blocked"
            for suite in (
                supplied.provider_safe_suites
                + supplied.fyralis_ceiling_suites
                + supplied.fault_recovery_suites
            )
        )


async def test_canary_is_fail_closed_even_when_source_credentials_are_present(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    artifact_dir = tmp_path / "artifacts"
    secret = "must-never-be-serialized"

    receipt_started_at = datetime.now(timezone.utc)
    supplied = await run_stage(
        source_id="slack",
        stage="canary",
        result_path=result_path,
        artifact_dir=artifact_dir,
        ambient_env={"FYRALIS_CANARY_SLACK": secret},
    )
    receipt_completed_at = datetime.now(timezone.utc)

    artifact_text = (artifact_dir / "stage.json").read_text(encoding="utf-8")
    artifact = json.loads(artifact_text)
    validate_stage_artifact(
        artifact,
        spec=SOURCE_CERTIFICATION_CATALOG["slack"],
        stage="canary",
        supplied=supplied,
        started_at=receipt_started_at,
        completed_at=receipt_completed_at,
        expected_plan_sha256=declared_execution_plan_sha256("slack"),
    )
    assert supplied.canary.state == "blocked"
    assert all(
        operation.state == "blocked"
        for operation in supplied.canary.operation_results
    )
    assert artifact["credential_environment_names_present"] == [
        "FYRALIS_CANARY_SLACK"
    ]
    assert artifact["credential_values_recorded"] is False
    assert artifact["real_provider_requests_sent"] == 0
    execution = artifact["canary_execution"]
    assert execution == {
        "schema_version": CANARY_EXECUTION_SCHEMA_VERSION,
        "source_id": "slack",
        "canary_id": SOURCE_CERTIFICATION_CATALOG["slack"].canary.canary_id,
        "promotion_eligible": False,
        "account_identity_sha256": None,
        "account_type": SOURCE_CERTIFICATION_CATALOG[
            "slack"
        ].canary.account_type,
        "api_version": SOURCE_CERTIFICATION_CATALOG[
            "slack"
        ].provider_api_version,
        "started_at": execution["started_at"],
        "completed_at": execution["completed_at"],
        "request_ledger": [],
        "cleanup": {
            "required": False,
            "state": "not_required",
            "completed_at": None,
            "actions": [],
        },
    }
    assert datetime.fromisoformat(
        execution["completed_at"]
    ) >= datetime.fromisoformat(execution["started_at"])
    assert secret not in artifact_text


async def test_binding_plan_hash_is_checked_before_stage_execution(
    tmp_path: Path,
) -> None:
    with pytest.raises(ExecutionDriverError, match="execution plan hash is stale"):
        await run_stage(
            source_id="slack",
            stage="local_correctness",
            result_path=tmp_path / "result.json",
            artifact_dir=tmp_path / "artifacts",
            ambient_env={},
            expected_plan_sha256="0" * 64,
        )


async def test_live_only_fixture_constructs_exact_whatsapp_target(
    tmp_path: Path,
) -> None:
    await run_stage(
        source_id="whatsapp",
        stage="local_correctness",
        result_path=tmp_path / "result.json",
        artifact_dir=tmp_path / "artifacts",
        ambient_env={},
    )

    artifact = json.loads(
        (tmp_path / "artifacts" / "stage.json").read_text(encoding="utf-8")
    )
    probe = artifact["fixture_and_binding_probe"]
    assert probe["fixture_factory_kind"] == "live_only"
    assert probe["sibling_fixture_is_distinct"] is True
    assert probe["live_target"]["state"] == "constructed"
    assert (
        probe["live_target"]["non_null_fields"]["whatsapp_phone_number_id"]
        == "certification-execution-whatsapp"
    )


@pytest.mark.parametrize("source_id", CANONICAL_SOURCE_IDS)
async def test_every_source_executes_its_exact_local_plan(
    source_id: str,
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / source_id
    await run_stage(
        source_id=source_id,
        stage="local_correctness",
        result_path=source_dir / "result.json",
        artifact_dir=source_dir / "artifacts",
        ambient_env={},
        expected_plan_sha256=declared_execution_plan_sha256(source_id),
    )

    artifact = json.loads(
        (source_dir / "artifacts" / "stage.json").read_text(
            encoding="utf-8",
        )
    )
    probe = artifact["fixture_and_binding_probe"]
    assert probe["deterministic_repeat"] is True
    assert probe["sibling_fixture_is_distinct"] is True
    assert probe["all_callable_bindings_resolved"] is True
    assert probe["live_target"]["state"] == "constructed"
    if probe["fixture_factory_kind"] == "historical":
        assert probe["count_oracle_deterministic"] is True
        assert probe["exact_observation_count"] > 0
        assert probe["installation_seeder_resolved"] is True
    else:
        assert probe["count_oracle_deterministic"] is None
        assert probe["exact_observation_count"] is None
        assert probe["installation_seeder_resolved"] is False
    assert artifact["provider_lab_used_surface"]["source_isolation"] is True
    assert (
        artifact["provider_lab_used_surface"]["four_scope_request_ledger"][
            "passed"
        ]
        is True
    )
    assert [
        row["scenario_id"]
        for row in artifact["scenario_execution_ledger"]
    ] == list(
        SOURCE_CERTIFICATION_CATALOG[source_id].required_scenarios,
    )


async def test_all_27_sources_execute_declared_load_and_fault_diagnostics(
    tmp_path: Path,
) -> None:
    for source_id in CANONICAL_SOURCE_IDS:
        load_dir = tmp_path / source_id / "load"
        await run_stage(
            source_id=source_id,
            stage="load",
            result_path=load_dir / "result.json",
            artifact_dir=load_dir / "artifacts",
            ambient_env={},
            load_request_count=2,
            expected_plan_sha256=declared_execution_plan_sha256(source_id),
        )
        load_artifact = json.loads(
            (load_dir / "artifacts" / "stage.json").read_text(
                encoding="utf-8",
            )
        )
        mix = load_artifact["load_diagnostic"]["declared_surface_mix"]
        assert (
            set(mix["http_operation_ids"])
            | set(mix["uncovered_http_operation_ids"])
            | set(mix["protocol_operation_ids_not_load_exercised"])
        ) == set(mix["declared_operation_ids"])
        offered = load_artifact["offered_load"]
        assert {suite["kind"] for suite in offered["suites"]} == {
            "historical",
            "live",
            "combined",
        }
        for suite in offered["suites"]:
            provider_lab = suite["provider_lab_diagnostic"]
            if provider_lab.get("state") == "not_applicable":
                assert suite["kind"] == "historical"
                assert source_id == "whatsapp"
                continue
            ceiling = provider_lab["fyralis_ceiling"]
            assert ceiling["state"] == "blocked"
            assert ceiling["reason"]

        typed = load_artifact["pipeline_load_artifacts"]
        for mode in typed.values():
            for kind, pipeline_artifact in mode.items():
                declared = next(
                    suite
                    for suite in SOURCE_CERTIFICATION_CATALOG[
                        source_id
                    ].load_suites
                    if suite.kind == kind
                )
                assert pipeline_artifact["workload"] == (
                    declared.execution_workload_dict()
                )
                assert pipeline_artifact["state"] == (
                    "not_applicable"
                    if declared.non_applicability is not None
                    else "blocked"
                )

        fault_dir = tmp_path / source_id / "fault"
        await run_stage(
            source_id=source_id,
            stage="fault_recovery",
            result_path=fault_dir / "result.json",
            artifact_dir=fault_dir / "artifacts",
            ambient_env={},
            expected_plan_sha256=declared_execution_plan_sha256(source_id),
        )
        fault_artifact = json.loads(
            (fault_dir / "artifacts" / "stage.json").read_text(
                encoding="utf-8",
            )
        )
        diagnostic = fault_artifact["fault_recovery_diagnostic"]
        target_count = len(fault_artifact["declared_fault_targets"])
        if target_count == 0:
            assert diagnostic["state"] == "blocked"
        else:
            assert diagnostic["state"] == "diagnostic_only"
            assert diagnostic["failed_faults"] == []
            assert len(diagnostic["recovered_faults"]) == target_count * 2
