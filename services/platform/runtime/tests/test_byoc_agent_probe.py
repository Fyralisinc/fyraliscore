from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.platform.runtime.byoc_agent_probe import (
    ByocAgentProbeInputs,
    render_agent_probe_report_json,
    run_byoc_agent_probe,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
MANIFEST = load_byoc_manifest(MANIFEST_PATH)
INSTALL_TOKEN = "local-install-token-for-agent-probe-tests"


@pytest.mark.asyncio
async def test_agent_probe_enrolls_and_sends_sanitized_heartbeat() -> None:
    report = await run_byoc_agent_probe(
        ByocAgentProbeInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_probe001",
            agent_version="2026.06.26-test",
            nonce="nonce-agent-probe-test-001",
            requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
            sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
        )
    )

    payload = json.loads(render_agent_probe_report_json(report))
    serialized = json.dumps(payload, sort_keys=True)

    assert report.required_checks_passed is True
    assert payload["status"] == "pass"
    assert payload["control_plane_mode"] == "mock"
    assert payload["deployment_id"] == MANIFEST.deployment_id
    assert payload["customer_id"] == MANIFEST.customer_id
    assert payload["desired_revision"] == MANIFEST.artifact_revision
    assert payload["heartbeat_interval_seconds"] == (
        MANIFEST.connectivity.heartbeat_interval_seconds
    )
    assert payload["poll_after_seconds"] == (
        MANIFEST.connectivity.agent_poll_interval_seconds
    )
    assert INSTALL_TOKEN not in serialized
    assert MANIFEST.connectivity.control_plane_url not in serialized
    assert '"value"' not in serialized


@pytest.mark.asyncio
async def test_agent_probe_fails_without_install_token_material() -> None:
    report = await run_byoc_agent_probe(
        ByocAgentProbeInputs(
            manifest_path=MANIFEST_PATH,
            install_token="",
            agent_id="agt_probe001",
            nonce="nonce-agent-probe-test-001",
        )
    )

    assert report.status == "fail"
    assert report.required_checks_passed is False
    assert "install_token_available" in {
        check.name for check in report.checks if check.status == "fail"
    }
    assert report.enrollment_status is None
    assert report.heartbeat_status is None


@pytest.mark.asyncio
async def test_agent_probe_live_url_failure_does_not_serialize_url() -> None:
    control_plane_url = "http://control.example.com/private-path"

    report = await run_byoc_agent_probe(
        ByocAgentProbeInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_probe001",
            nonce="nonce-agent-probe-test-001",
            control_plane_url=control_plane_url,
        )
    )
    serialized = render_agent_probe_report_json(report)

    assert report.status == "fail"
    assert report.control_plane_mode == "live"
    assert report.control_plane_url_supplied is True
    assert "control_plane_endpoint" in {
        check.name for check in report.checks if check.status == "fail"
    }
    assert control_plane_url not in serialized
