from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from services.platform.runtime.byoc_agent_apply_plan import (
    build_apply_revision_plan,
    validate_apply_plan_contract,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateResponse,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")


def _desired_state() -> ByocAgentDesiredStateResponse:
    return ByocAgentDesiredStateResponse(
        schema_version="fyralis.byoc.agent.desired_state.v1",
        status="accepted",
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id="agt_apply001",
        current_revision=MANIFEST.artifact_revision,
        desired_revision="2026.06.26-2",
        rollout_action="apply_revision",
        config_epoch=7,
        config_scope="metadata_only",
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
        evidence_package_required=False,
        accepted_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
        stored_scope="sanitized_agent_metadata_only",
    )


def test_apply_revision_plan_is_non_mutating_and_sanitized() -> None:
    plan = build_apply_revision_plan(
        MANIFEST,
        _desired_state(),
        generated_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
    )
    serialized = json.dumps(plan.model_dump(mode="json"), sort_keys=True)

    assert validate_apply_plan_contract(plan) == []
    assert plan.schema_version == "fyralis.byoc.agent.apply_plan.v1"
    assert plan.execution_mode == "plan_only"
    assert plan.mutating_step_count == 0
    assert plan.planned_step_count == len(plan.steps)
    assert {step.execution_mode for step in plan.steps} == {"plan_only"}
    assert {step.mutates_customer_resources for step in plan.steps} == {False}
    assert "://" not in serialized
    assert "bearer " not in serialized.lower()
    assert "signature" not in serialized.lower()
    assert "payload" not in serialized.lower()
    assert MANIFEST.connectivity.control_plane_url not in serialized


def test_apply_revision_plan_rejects_unchanged_revision() -> None:
    desired_state = _desired_state().model_copy(
        update={"desired_revision": MANIFEST.artifact_revision}
    )
    plan = build_apply_revision_plan(MANIFEST, desired_state)

    assert [violation.code for violation in validate_apply_plan_contract(plan)] == [
        "revision_unchanged"
    ]


def test_apply_revision_plan_rejects_declared_mutating_steps() -> None:
    plan = build_apply_revision_plan(MANIFEST, _desired_state()).model_copy(
        update={"mutating_step_count": 1}
    )

    assert [violation.code for violation in validate_apply_plan_contract(plan)] == [
        "mutating_step_declared"
    ]
