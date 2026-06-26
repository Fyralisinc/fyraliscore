from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerInputs,
    render_agent_runner_report_json,
    run_byoc_agent_runner,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
MANIFEST = load_byoc_manifest(MANIFEST_PATH)
INSTALL_TOKEN = "local-install-token-for-agent-runner-tests"


@pytest.mark.asyncio
async def test_agent_runner_enrolls_polls_and_heartbeats_for_bounded_iterations() -> None:
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_runner001",
            agent_version="2026.06.26-test",
            nonce_prefix="nonce-agent-runner-test",
            starting_sequence=7,
            iterations=2,
            requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
            sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
        )
    )

    payload = json.loads(render_agent_runner_report_json(report))
    serialized = json.dumps(payload, sort_keys=True)

    assert report.required_checks_passed is True
    assert payload["status"] == "pass"
    assert payload["control_plane_mode"] == "mock"
    assert payload["enrollment_status"] == "pass"
    assert payload["iterations_requested"] == 2
    assert payload["iterations_completed"] == 2
    assert payload["desired_state_poll_count"] == 2
    assert payload["heartbeat_count"] == 2
    assert payload["final_desired_revision"] == MANIFEST.artifact_revision
    assert payload["final_rollout_action"] == "none"
    assert payload["next_poll_after_seconds"] == (
        MANIFEST.connectivity.agent_poll_interval_seconds
    )
    assert [iteration["sequence"] for iteration in payload["iterations"]] == [7, 8]
    assert "desired_state_request" in {check["name"] for check in payload["checks"]}
    assert "heartbeat_request" in {check["name"] for check in payload["checks"]}
    assert INSTALL_TOKEN not in serialized
    assert MANIFEST.connectivity.control_plane_url not in serialized
    assert '"value"' not in serialized


@pytest.mark.asyncio
async def test_agent_runner_builds_non_mutating_apply_plan_for_revision_change() -> None:
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_runner001",
            agent_version="2026.06.26-test",
            nonce_prefix="nonce-agent-runner-test",
            iterations=1,
            mock_desired_revision="2026.06.26-2",
            mock_config_epoch=3,
            requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
            sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
        )
    )

    payload = json.loads(render_agent_runner_report_json(report))
    serialized = json.dumps(payload, sort_keys=True)
    serialized_apply_plans = json.dumps(payload["apply_plans"], sort_keys=True)

    assert report.required_checks_passed is True
    assert payload["final_rollout_action"] == "apply_revision"
    assert payload["final_desired_revision"] == "2026.06.26-2"
    assert payload["final_config_epoch"] == 3
    assert payload["apply_plan_count"] == 1
    assert payload["apply_plans"][0]["schema_version"] == (
        "fyralis.byoc.agent.apply_plan_evidence.v1"
    )
    assert payload["apply_plans"][0]["status"] == "pass"
    assert payload["apply_plans"][0]["current_revision"] == MANIFEST.artifact_revision
    assert payload["apply_plans"][0]["desired_revision"] == "2026.06.26-2"
    assert payload["apply_plans"][0]["execution_mode"] == "plan_only"
    assert payload["apply_plans"][0]["mutating_step_count"] == 0
    assert payload["iterations"][0]["apply_plan_status"] == "pass"
    assert payload["iterations"][0]["apply_plan_id"] == (
        payload["apply_plans"][0]["plan_id"]
    )
    assert "apply_plan_contract" in {check["name"] for check in payload["checks"]}
    assert INSTALL_TOKEN not in serialized
    assert MANIFEST.connectivity.control_plane_url not in serialized
    assert "signature" not in serialized_apply_plans.lower()
    assert "payload" not in serialized_apply_plans.lower()


@pytest.mark.asyncio
async def test_agent_runner_fails_without_install_token_material() -> None:
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token="",
            agent_id="agt_runner001",
            nonce_prefix="nonce-agent-runner-test",
        )
    )

    assert report.status == "fail"
    assert report.required_checks_passed is False
    assert report.enrollment_status is None
    assert report.desired_state_poll_count == 0
    assert report.heartbeat_count == 0
    assert "install_token_available" in {
        check.name for check in report.checks if check.status == "fail"
    }


@pytest.mark.asyncio
async def test_agent_runner_fails_when_iterations_are_unbounded() -> None:
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_runner001",
            nonce_prefix="nonce-agent-runner-test",
            iterations=11,
        )
    )

    assert report.status == "fail"
    assert report.enrollment_status is None
    assert report.iterations_completed == 0
    assert "iteration_bound" in {
        check.name for check in report.checks if check.status == "fail"
    }


@pytest.mark.asyncio
async def test_agent_runner_live_url_failure_does_not_serialize_url() -> None:
    control_plane_url = "http://control.example.com/private-path"

    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_runner001",
            nonce_prefix="nonce-agent-runner-test",
            control_plane_url=control_plane_url,
        )
    )
    serialized = render_agent_runner_report_json(report)

    assert report.status == "fail"
    assert report.control_plane_mode == "live"
    assert report.control_plane_url_supplied is True
    assert "control_plane_endpoint" in {
        check.name for check in report.checks if check.status == "fail"
    }
    assert control_plane_url not in serialized
