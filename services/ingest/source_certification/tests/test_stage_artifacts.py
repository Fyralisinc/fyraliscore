from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    ScenarioResult,
    SuiteResult,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_TOPOLOGY_SCENARIO_IDS,
    pipeline_scenario_ids_for_source,
)
from services.ingest.source_certification.pipeline_load_runner import (
    PipelineLoadRunConfig,
    PipelineLoadTiming,
    declared_pipeline_workload_from_suite,
    diagnostic_pipeline_load_config_from_suite,
    run_pipeline_load,
    validate_pipeline_load_artifact,
)
from services.ingest.source_certification.stage_artifacts import (
    CANARY_EXECUTION_SCHEMA_VERSION,
    STAGE_ARTIFACT_SCHEMA_VERSION,
    StageArtifactError,
    validate_stage_artifact,
)
from services.ingest.source_certification.tests.pipeline_test_fixtures import (
    passing_pipeline_probe,
)


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
ACCOUNT_SHA = "b" * 64


def _plan(spec) -> tuple[dict[str, object], str]:  # noqa: ANN001
    plan: dict[str, object] = {
        "schema_version": "test-plan.v1",
        "source_id": spec.source_id,
        "spec_hash": spec.declaration_hash(),
    }
    rendered = (
        json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return plan, hashlib.sha256(rendered).hexdigest()


def _blocked_suites() -> tuple[SuiteResult, ...]:
    return tuple(
        SuiteResult(kind=kind, state="blocked")
        for kind in ("historical", "live", "combined")
    )


def _load_input(spec) -> CertificationInput:  # noqa: ANN001
    """Build the stage-owned, fail-closed result summary for load tests."""

    artifact = "evidence-file:stage.json"

    def suites() -> tuple[SuiteResult, ...]:
        return tuple(
            SuiteResult(
                kind=suite.kind,
                state=(
                    "not_applicable"
                    if suite.non_applicability is not None
                    else "blocked"
                ),
                artifact_uri=(
                    artifact if suite.non_applicability is not None else None
                ),
            )
            for suite in spec.load_suites
        )

    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="blocked",
        local_correctness_artifact=artifact,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state="blocked",
                artifact_uri=artifact,
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=suites(),
        fyralis_ceiling_suites=suites(),
        fault_recovery_suites=_blocked_suites(),
        canary=CanaryResult(state="blocked", operation_results=()),
        legacy_reference_count=0,
    )


def _load_artifact(spec) -> tuple[dict[str, object], str]:  # noqa: ANN001
    """Build a real nested typed-pipeline artifact without an adapter.

    An absent isolated adapter intentionally produces the durable blocked
    artifacts that stage schema v3 can accept. This keeps the test focused on
    typed declaration validation rather than fabricating release evidence.
    """

    from services.ingest.source_certification.execution_driver import (  # noqa: PLC0415
        build_declared_execution_plan,
        declared_execution_plan_sha256,
    )

    plan = build_declared_execution_plan(spec.source_id)
    artifacts: dict[str, dict[str, object]] = {}
    for mode in ("provider_safe", "fyralis_ceiling"):
        artifacts[mode] = {
            suite.kind: asyncio.run(
                run_pipeline_load(
                    source_id=spec.source_id,
                    mode=mode,
                    workload=declared_pipeline_workload_from_suite(suite),
                    ambient_env={},
                    adapter_factory=None,
                    config=diagnostic_pipeline_load_config_from_suite(suite),
                )
            )
            for suite in spec.load_suites
        }
    return (
        {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "source_id": spec.source_id,
            "stage": "load",
            "spec_hash": spec.declaration_hash(),
            "execution_plan_sha256": declared_execution_plan_sha256(
                spec.source_id,
            ),
            "execution_plan": plan,
            "generated_at": NOW.isoformat(),
            "synthetic_promotion_allowed": False,
            "load_diagnostic": {"state": "diagnostic_only"},
            "offered_load": {"state": "measured_blocked"},
            "declared_load_suites": plan["load_suites"],
            "pipeline_load_artifacts": artifacts,
            "claim_boundary": "typed pipeline load diagnostics remain blocked",
        },
        declared_execution_plan_sha256(spec.source_id),
    )


def _rehash_pipeline_load_artifact(artifact: dict[str, object]) -> None:
    unhashed = dict(artifact)
    unhashed.pop("artifact_sha256", None)
    rendered = (
        json.dumps(
            unhashed,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    artifact["artifact_sha256"] = hashlib.sha256(rendered).hexdigest()


def _classified_slack_spec():  # noqa: ANN202
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    contracts = tuple(
        replace(
            contract,
            mutability=(
                "mutation"
                if contract.operation_id
                == "provider_request.chat.postMessage"
                else "read"
            ),
            cleanup_action=(
                "delete_disposable_test_message"
                if contract.operation_id
                == "provider_request.chat.postMessage"
                else None
            ),
            classification_basis="test-owned explicit operation policy",
        )
        for contract in spec.canary.operation_contracts
    )
    return replace(
        spec,
        canary=replace(spec.canary, operation_contracts=contracts),
    )


def _passing_canary_input(spec) -> CertificationInput:  # noqa: ANN001
    artifact = "evidence-file:stage.json"
    request_count = (
        len(spec.canary.required_operations)
        + len(spec.canary.mutating_actions)
    )
    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="blocked",
        local_correctness_artifact=artifact,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state="blocked",
                artifact_uri=artifact,
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=_blocked_suites(),
        fyralis_ceiling_suites=_blocked_suites(),
        fault_recovery_suites=_blocked_suites(),
        canary=CanaryResult(
            state="passed",
            operation_results=tuple(
                CanaryOperationResult(
                    operation_id=operation_id,
                    state="passed",
                    artifact_uri=artifact,
                )
                for operation_id in spec.canary.required_operations
            ),
            tested_at=NOW,
            account_type=spec.canary.account_type,
            api_version=spec.provider_api_version,
            artifact_uri=artifact,
            request_count=request_count,
            account_identity_sha256=ACCOUNT_SHA,
            mutation_actions=spec.canary.mutating_actions,
            cleanup_state="passed",
        ),
        legacy_reference_count=0,
    )


def _passing_pipeline_probe(source_id: str) -> dict[str, object]:
    return passing_pipeline_probe(source_id)


def _partial_local_input(spec) -> CertificationInput:  # noqa: ANN001
    artifact = "evidence-file:stage.json"
    pipeline_ids = pipeline_scenario_ids_for_source(spec.source_id)
    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="blocked",
        local_correctness_artifact=artifact,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state=(
                    "passed"
                    if scenario_id in pipeline_ids
                    else "blocked"
                ),
                artifact_uri=artifact,
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=_blocked_suites(),
        fyralis_ceiling_suites=_blocked_suites(),
        fault_recovery_suites=_blocked_suites(),
        canary=CanaryResult(
            state="blocked",
            operation_results=(),
        ),
        legacy_reference_count=0,
    )


def _local_artifact(spec) -> tuple[dict[str, object], str]:  # noqa: ANN001
    plan, plan_sha = _plan(spec)
    pipeline_ids = pipeline_scenario_ids_for_source(spec.source_id)
    ledger = [
        {
            "scenario_id": scenario_id,
            "certification_state": (
                "passed"
                if scenario_id in pipeline_ids
                else "blocked"
            ),
            "declared_probe_ids": (
                [
                    "idempotency_builder_resolution",
                    "observation_idempotency_replay",
                ]
                if scenario_id == "duplicate_delivery_and_idempotency"
                else ["pipeline_probe"]
                if scenario_id in pipeline_ids
                else ["source_specific_executor_absent"]
            ),
            "measured_probe_ids": (
                [
                    "idempotency_builder_resolution",
                    "observation_idempotency_replay",
                ]
                if scenario_id == "duplicate_delivery_and_idempotency"
                else ["pipeline_probe"]
                if scenario_id in pipeline_ids
                else []
            ),
            "unmeasured_probe_ids": (
                []
                if scenario_id in pipeline_ids
                else ["source_specific_executor_absent"]
            ),
            "unproven_requirements": (
                []
                if scenario_id in pipeline_ids
                else ["source-specific proof remains blocked"]
            ),
        }
        for scenario_id in spec.required_scenarios
    ]
    return (
        {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "source_id": spec.source_id,
            "stage": "local_correctness",
            "spec_hash": spec.declaration_hash(),
            "execution_plan_sha256": plan_sha,
            "execution_plan": plan,
            "generated_at": NOW.isoformat(),
            "synthetic_promotion_allowed": False,
            "fixture_and_binding_probe": {},
            "provider_lab_used_surface": {},
            "pipeline_probe": _passing_pipeline_probe(spec.source_id),
            "scenario_execution_ledger": ledger,
            "claim_boundary": "typed local replay proof",
        },
        plan_sha,
    )


def test_typed_load_artifact_requires_all_nested_contract_bound_workloads() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _load_input(spec)
    artifact, plan_sha = _load_artifact(spec)

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="load",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )

    nested = artifact["pipeline_load_artifacts"]
    assert isinstance(nested, dict)
    provider_safe = nested["provider_safe"]
    assert isinstance(provider_safe, dict)
    # Substitute another canonical typed artifact: it remains self-valid but
    # is not the live suite declaration, so the stage must reject it.
    provider_safe["live"] = provider_safe["historical"]
    with pytest.raises(StageArtifactError, match="typed workload differs"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="load",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_typed_load_artifact_rejects_rehashed_configuration_drift() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _load_input(spec)
    artifact, plan_sha = _load_artifact(spec)
    nested = artifact["pipeline_load_artifacts"]
    assert isinstance(nested, dict)
    provider_safe = nested["provider_safe"]
    assert isinstance(provider_safe, dict)
    historical = provider_safe["historical"]
    assert isinstance(historical, dict)
    configuration = historical["configuration"]
    assert isinstance(configuration, dict)
    topology = configuration["topology"]
    assert isinstance(topology, dict)
    topology["replicas"] = 1
    _rehash_pipeline_load_artifact(historical)

    with pytest.raises(
        StageArtifactError,
        match="configuration differs from the typed source suite",
    ):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="load",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_typed_load_artifact_rejects_valid_promotion_eligible_pipeline_claim() -> None:
    from services.ingest.source_certification.tests.test_pipeline_load_runner import (  # noqa: PLC0415
        _Adapter,
        _Clock,
        _environment,
        _quota,
    )

    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _load_input(spec)
    stage_artifact, plan_sha = _load_artifact(spec)
    historical_suite = next(
        suite for suite in spec.load_suites if suite.kind == "historical"
    )
    clock = _Clock()

    def adapter_factory(
        infrastructure,
        source_id,
        mode,
        workload,
        topology,
        _quota_configuration,
    ):
        return _Adapter(
            infrastructure,
            source_id,
            mode,
            workload,
            topology,
            stable_through=1.0,
            clock=clock,
        )

    promoted = asyncio.run(
        run_pipeline_load(
            source_id=spec.source_id,
            mode="provider_safe",
            workload=declared_pipeline_workload_from_suite(historical_suite),
            ambient_env=_environment(),
            adapter_factory=adapter_factory,
            quota=_quota(rate=0.1),
            config=PipelineLoadRunConfig(
                timing=PipelineLoadTiming(
                    warmup_seconds=120,
                    step_seconds=120,
                    validation_seconds=900,
                    soak_seconds=3_600,
                ),
                initial_rate=0.1,
                maximum_offered_rate=0.1,
                release=True,
            ),
            clock=clock,
        )
    )
    boundary = promoted["boundary"]
    assert isinstance(boundary, dict)
    boundary["evidence_class"] = "exact_pipeline"
    promoted.update(
        state="passed",
        promotion_eligible=True,
        clock="system_wall_clock",
    )
    _rehash_pipeline_load_artifact(promoted)
    validate_pipeline_load_artifact(promoted)

    nested = stage_artifact["pipeline_load_artifacts"]
    assert isinstance(nested, dict)
    provider_safe = nested["provider_safe"]
    assert isinstance(provider_safe, dict)
    provider_safe["historical"] = promoted

    with pytest.raises(
        StageArtifactError,
        match="cannot accept promotion-eligible pipeline load claims",
    ):
        validate_stage_artifact(
            stage_artifact,
            spec=spec,
            stage="load",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_typed_load_artifact_neutralizes_only_declared_not_applicable_workloads() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["whatsapp"]
    supplied = _load_input(spec)
    artifact, plan_sha = _load_artifact(spec)

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="load",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )

    historical = next(
        result
        for result in supplied.provider_safe_suites
        if result.kind == "historical"
    )
    assert historical.state == "not_applicable"
    bad_results = tuple(
        replace(result, state="blocked", artifact_uri=None)
        if result.kind == "historical"
        else result
        for result in supplied.provider_safe_suites
    )
    with pytest.raises(
        StageArtifactError,
        match="SuiteResult must be not_applicable",
    ):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="load",
            supplied=replace(supplied, provider_safe_suites=bad_results),
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_typed_load_artifact_rejects_legacy_plan_projection_and_promoting_diagnostic() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _load_input(spec)
    artifact, plan_sha = _load_artifact(spec)

    artifact["declared_load_suites"] = [
        {"kind": suite.kind} for suite in spec.load_suites
    ]
    with pytest.raises(StageArtifactError, match="full typed source plan"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="load",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )

    artifact, plan_sha = _load_artifact(spec)
    artifact["offered_load"] = {"state": "promotion_eligible"}
    with pytest.raises(StageArtifactError, match="non-promoting Provider Lab"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="load",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_typed_local_artifact_accepts_exact_idempotency_replay_proof() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _partial_local_input(spec)
    artifact, plan_sha = _local_artifact(spec)

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="local_correctness",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )


def test_typed_local_artifact_accepts_unique_parent_replay_subset() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _partial_local_input(spec)
    artifact, plan_sha = _local_artifact(spec)
    pipeline = artifact["pipeline_probe"]
    assert isinstance(pipeline, dict)
    pipeline_body = pipeline["pipeline"]
    assert isinstance(pipeline_body, dict)
    tenant_pipeline = pipeline_body["tenant_pipelines"][0]
    replay = tenant_pipeline["replay"]

    tenant_pipeline["raw_topic"]["before_replay"]["count"] = 3
    tenant_pipeline["raw_topic"]["after_replay"]["count"] = 5
    replay.update(
        {
            "raw_records_before": 3,
            "raw_records_replayed": 2,
            "raw_records_after": 5,
            "raw_topic_record_growth": 2,
        },
    )

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="local_correctness",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )


def test_typed_local_artifact_accepts_whatsapp_without_topology_claims() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["whatsapp"]
    supplied = _partial_local_input(spec)
    artifact, plan_sha = _local_artifact(spec)

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="local_correctness",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )


def test_typed_local_artifact_rejects_whatsapp_topology_promotion() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["whatsapp"]
    supplied = _partial_local_input(spec)
    scenario_results = tuple(
        replace(result, state="passed")
        if result.scenario_id in PIPELINE_TOPOLOGY_SCENARIO_IDS
        else result
        for result in supplied.scenario_results
    )
    supplied = replace(supplied, scenario_results=scenario_results)
    artifact, plan_sha = _local_artifact(spec)
    for row in artifact["scenario_execution_ledger"]:  # type: ignore[index]
        if row["scenario_id"] in PIPELINE_TOPOLOGY_SCENARIO_IDS:
            row["certification_state"] = "passed"
            row["unmeasured_probe_ids"] = []
            row["unproven_requirements"] = []

    with pytest.raises(
        StageArtifactError,
        match="source-capable pipeline boundary",
    ):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="local_correctness",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    (
        (
            "raw_records_after",
            5,
            "raw-record counters are inconsistent",
        ),
        (
            "observation_identity_set_sha256_after",
            "c" * 64,
            "Observation identity changed",
        ),
    ),
)
def test_typed_local_artifact_rejects_replay_counter_or_hash_drift(
    field: str,
    value: object,
    match: str,
) -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _partial_local_input(spec)
    artifact, plan_sha = _local_artifact(spec)
    pipeline = artifact["pipeline_probe"]
    assert isinstance(pipeline, dict)
    pipeline_body = pipeline["pipeline"]
    assert isinstance(pipeline_body, dict)
    replay = pipeline_body["tenant_pipelines"][0]["replay"]
    assert isinstance(replay, dict)
    replay[field] = value

    with pytest.raises(StageArtifactError, match=match):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="local_correctness",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    (
        (
            "participating_oauth_replicas",
            1,
            "participating_oauth_replicas must equal 2",
        ),
        (
            "cross_tenant_leak_count",
            1,
            "did not establish every required",
        ),
    ),
)
def test_typed_local_artifact_rejects_history_topology_drift(
    field: str,
    value: object,
    match: str,
) -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    supplied = _partial_local_input(spec)
    artifact, plan_sha = _local_artifact(spec)
    topology = artifact["pipeline_probe"]["pipeline"]["topology"]  # type: ignore[index]
    topology[field] = value

    with pytest.raises(StageArtifactError, match=match):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="local_correctness",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def _canary_artifact(spec) -> tuple[dict[str, object], str]:  # noqa: ANN001
    plan, plan_sha = _plan(spec)
    ledger = [
        {
            "sequence": index,
            "operation_id": contract.operation_id,
            "request_kind": contract.mutability,
            "mutation_action": contract.cleanup_action,
            "started_at": NOW.isoformat(),
            "completed_at": NOW.isoformat(),
            "state": "passed",
        }
        for index, contract in enumerate(
            spec.canary.operation_contracts,
            start=1,
        )
    ]
    first_cleanup_sequence = len(ledger)
    ledger.extend(
        [
            {
                "sequence": first_cleanup_sequence + index,
                "operation_id": action,
                "request_kind": "cleanup",
                "mutation_action": None,
                "started_at": NOW.isoformat(),
                "completed_at": NOW.isoformat(),
                "state": "passed",
            }
            for index, action in enumerate(
                spec.canary.mutating_actions,
                start=1,
            )
        ]
    )
    cleanup_actions = [
        {
            "action_id": action,
            "state": "passed",
            "completed_at": NOW.isoformat(),
        }
        for action in spec.canary.mutating_actions
    ]
    return (
        {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "source_id": spec.source_id,
            "stage": "canary",
            "spec_hash": spec.declaration_hash(),
            "execution_plan_sha256": plan_sha,
            "execution_plan": plan,
            "generated_at": NOW.isoformat(),
            "synthetic_promotion_allowed": True,
            "credential_environment_names_present": [
                spec.canary.credential_env_prefix
            ],
            "credential_values_recorded": False,
            "real_provider_requests_sent": len(ledger),
            "canary_execution": {
                "schema_version": CANARY_EXECUTION_SCHEMA_VERSION,
                "source_id": spec.source_id,
                "canary_id": spec.canary.canary_id,
                "promotion_eligible": True,
                "account_identity_sha256": ACCOUNT_SHA,
                "account_type": spec.canary.account_type,
                "api_version": spec.provider_api_version,
                "started_at": NOW.isoformat(),
                "completed_at": NOW.isoformat(),
                "request_ledger": ledger,
                "cleanup": {
                    "required": True,
                    "state": "passed",
                    "completed_at": NOW.isoformat(),
                    "actions": cleanup_actions,
                },
            },
            "claim_boundary": "typed real-provider canary test artifact",
        },
        plan_sha,
    )


def test_typed_canary_ledger_enforces_operation_bound_mutability() -> None:
    spec = _classified_slack_spec()
    supplied = _passing_canary_input(spec)
    artifact, plan_sha = _canary_artifact(spec)

    validate_stage_artifact(
        artifact,
        spec=spec,
        stage="canary",
        supplied=supplied,
        started_at=NOW,
        completed_at=NOW,
        expected_plan_sha256=plan_sha,
    )

    ledger = artifact["canary_execution"]["request_ledger"]  # type: ignore[index]
    mutation = next(  # type: ignore[arg-type]
        row
        for row in ledger
        if row["operation_id"] == "provider_request.chat.postMessage"
    )
    mutation["request_kind"] = "read"
    mutation["mutation_action"] = None
    with pytest.raises(StageArtifactError, match="request_kind differs"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="canary",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_unclassified_canary_operation_cannot_be_self_labelled_read() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["slack"]
    classified = _classified_slack_spec()
    supplied = _passing_canary_input(classified)
    artifact, _classified_plan_sha = _canary_artifact(classified)
    plan, plan_sha = _plan(spec)
    artifact["spec_hash"] = spec.declaration_hash()
    artifact["execution_plan"] = plan
    artifact["execution_plan_sha256"] = plan_sha
    supplied = replace(supplied, spec_hash=spec.declaration_hash())

    with pytest.raises(StageArtifactError, match="mutability is unclassified"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="canary",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_canary_request_budget_and_cleanup_actions_are_exact() -> None:
    spec = _classified_slack_spec()
    supplied = _passing_canary_input(spec)
    artifact, plan_sha = _canary_artifact(spec)
    cleanup = artifact["canary_execution"]["cleanup"]  # type: ignore[index]
    cleanup["actions"] = []  # type: ignore[index]

    with pytest.raises(StageArtifactError, match="cleanup actions differ"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="canary",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )


def test_canary_request_budget_includes_cleanup_requests() -> None:
    base = _classified_slack_spec()
    spec = replace(
        base,
        canary=replace(
            base.canary,
            max_requests=len(base.canary.required_operations),
        ),
    )
    supplied = _passing_canary_input(spec)
    artifact, plan_sha = _canary_artifact(spec)

    with pytest.raises(StageArtifactError, match="request count exceeds"):
        validate_stage_artifact(
            artifact,
            spec=spec,
            stage="canary",
            supplied=supplied,
            started_at=NOW,
            completed_at=NOW,
            expected_plan_sha256=plan_sha,
        )
