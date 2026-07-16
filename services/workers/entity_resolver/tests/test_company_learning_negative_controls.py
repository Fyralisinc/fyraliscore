from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from lib.evaluation.company_learning_experiment import (
    ConsumerTerminalFate,
    RecurrenceCaseKind,
)
from scripts.company_learning_recurrence_runtime import (
    NegativeControlExecutionPlan,
    build_negative_control_plan,
    load_negative_control_fixture,
)
from scripts.run_company_learning_negative_controls import main


def test_negative_control_fixture_seals_the_four_required_case_kinds() -> None:
    fixture = load_negative_control_fixture()

    assert len(fixture.cases) == 4
    assert {case.kind for case in fixture.cases} == {
        RecurrenceCaseKind.CONTEXTUAL_PHRASE_NEGATIVE,
        RecurrenceCaseKind.UNRELATED_NEGATIVE_CONTROL,
        RecurrenceCaseKind.HOMONYM_LOCAL_ASSOCIATION,
        RecurrenceCaseKind.CONFLICTING_SOURCE_HINT,
    }
    assert all(case.entity_type for case in fixture.cases)
    assert all(case.slack_context for case in fixture.cases)
    assert all(case.wording_variant for case in fixture.cases)
    assert all(case.consequence for case in fixture.cases)
    assert all(case.recurrence_distance >= 1 for case in fixture.cases)
    assert all(case.alias_surface for case in fixture.cases)
    assert len(fixture.digest) == 64


def test_negative_control_plan_is_typed_sealed_and_not_fabricated() -> None:
    fixture = load_negative_control_fixture()
    plan = build_negative_control_plan(
        fixture,
        run_id="pytest-negative-controls",
        system_version="pytest-system",
        created_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )

    assert plan.status == "not_executed"
    assert len(plan.assignments) == 4
    assert (
        len(
            {
                tenant_id
                for assignment in plan.assignments
                for tenant_id in (
                    assignment.adaptive_tenant_id,
                    assignment.frozen_tenant_id,
                )
            }
        )
        == 8
    )
    cases = {case.case_id: case for case in plan.spec.cases}
    conflict = cases["conflicting-source-hint"]
    assert ConsumerTerminalFate.RESOLVED_FOR_CONSUMER not in (
        conflict.adaptive_expectation.allowed_consumer_fates
    )
    assert ConsumerTerminalFate.RESOLVED_FOR_CONSUMER not in (
        conflict.frozen_expectation.allowed_consumer_fates
    )
    assert conflict.adaptive_expectation.expected_model_count == 0
    assert conflict.frozen_expectation.expected_model_count == 0
    assert conflict.adaptive_expectation.expected_entity_ref is None
    assert conflict.frozen_expectation.expected_entity_ref is None
    for case_id in {
        "contextual-non-entity",
        "unrelated-alias",
        "same-surface-homonym",
    }:
        case = cases[case_id]
        assert case.adaptive_expectation.expected_entity_ref is None
        assert case.frozen_expectation.expected_entity_ref is None
        assert case.adaptive_expectation.expected_model_count == 0
        assert case.frozen_expectation.expected_model_count == 0
        assert (
            ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            not in case.adaptive_expectation.allowed_consumer_fates
        )
    assert len(plan.spec.digest) == 64
    assert len(plan.digest) == 64


def test_negative_control_runner_writes_honest_plan_artifact(
    tmp_path: Path,
) -> None:
    exit_code = main(
        [
            "--output-dir",
            str(tmp_path),
            "--run-id",
            "pytest-negative-control-runner",
            "--system-version",
            "pytest-system",
        ]
    )

    assert exit_code == 0
    artifact_path = tmp_path / "company_learning_negative_controls_plan.json"
    payload = json.loads(artifact_path.read_text())
    plan_digest = payload.pop("plan_digest")
    plan = NegativeControlExecutionPlan.model_validate(payload)
    assert plan.status == "not_executed"
    assert plan_digest == plan.digest
    assert "report" not in payload
    assert "pairs" not in payload
    assert "assessments" not in payload


def test_negative_control_plan_rejects_assignment_tampering() -> None:
    plan = build_negative_control_plan(
        load_negative_control_fixture(),
        run_id="pytest-negative-control-tampering",
        system_version="pytest-system",
        created_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    payload = plan.model_dump(mode="json")
    payload["assignments"][1]["adaptive_tenant_id"] = payload["assignments"][0][
        "adaptive_tenant_id"
    ]

    with pytest.raises(
        ValidationError,
        match="globally distinct tenants",
    ):
        NegativeControlExecutionPlan.model_validate(payload)
