from __future__ import annotations

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
