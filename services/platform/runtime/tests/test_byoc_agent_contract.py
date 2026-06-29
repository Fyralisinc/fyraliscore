from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_agent_contract import (
    ByocAgentComponentStatus,
    ByocAgentEnrollmentRequest,
    ByocAgentHeartbeat,
    build_mock_control_plane_app,
    canonical_enrollment_payload,
    enrollment_payload_from_manifest,
    heartbeat_from_manifest,
    model_json_schema_bundle,
    signed_enrollment_request,
    validate_heartbeat_contract,
    verify_enrollment_request,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")
INSTALL_TOKEN = "local-install-token-for-contract-tests"
AGENT_ID = "agt_contract001"
AGENT_VERSION = "0.1.0"
REQUESTED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


def _payload():
    return enrollment_payload_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-contract-test-001",
        requested_at=REQUESTED_AT,
    )


def test_signed_enrollment_request_uses_secret_ref_not_raw_token() -> None:
    payload = _payload()
    request = signed_enrollment_request(payload, install_token=INSTALL_TOKEN)

    serialized = request.model_dump_json()
    assert INSTALL_TOKEN not in serialized
    assert request.install_token_secret_ref == (
        MANIFEST.secrets.bootstrap_token_secret_ref
    )
    assert request.signature.key_ref == request.install_token_secret_ref
    assert verify_enrollment_request(
        request,
        manifest=MANIFEST,
        install_token=INSTALL_TOKEN,
    ) == []


def test_enrollment_signature_is_canonical_and_detects_tampering() -> None:
    payload = _payload()
    request = signed_enrollment_request(payload, install_token=INSTALL_TOKEN)
    payload_bytes = canonical_enrollment_payload(payload)

    assert payload_bytes == canonical_enrollment_payload(payload)
    assert json.loads(payload_bytes)["agent_id"] == AGENT_ID

    tampered = ByocAgentEnrollmentRequest.model_validate(
        {
            **request.model_dump(mode="json"),
            "region": "eu-west-1",
        }
    )
    violations = verify_enrollment_request(
        tampered,
        manifest=MANIFEST,
        install_token=INSTALL_TOKEN,
    )

    assert [violation.code for violation in violations] == [
        "cloud_mismatch",
        "invalid_signature",
    ]


def test_enrollment_rejects_raw_token_extra_field() -> None:
    request = signed_enrollment_request(_payload(), install_token=INSTALL_TOKEN)
    data = request.model_dump(mode="json")
    data["install_token"] = INSTALL_TOKEN

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ByocAgentEnrollmentRequest.model_validate(data)


def test_heartbeat_contract_is_privacy_safe_and_bounded() -> None:
    heartbeat = heartbeat_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        sequence=1,
        validation_status="passing",
        control_plane_connected=True,
        components=(
            ByocAgentComponentStatus(
                name="gateway",
                kind="gateway",
                status="ok",
                detail_code="ready",
            ),
        ),
        sent_at=REQUESTED_AT,
    )

    assert validate_heartbeat_contract(heartbeat, manifest=MANIFEST) == []
    data = heartbeat.model_dump(mode="json")
    assert "tenant_id" not in data
    assert data["telemetry"]["raw_payloads_allowed"] is False
    assert data["telemetry"]["raw_prompts_allowed"] is False
    assert data["telemetry"]["pii_allowed"] is False
    assert data["components"][0]["detail_code"] == "ready"


def test_heartbeat_rejects_raw_privacy_flags_and_freeform_component_data() -> None:
    heartbeat = heartbeat_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        sequence=1,
        validation_status="passing",
        control_plane_connected=True,
    )
    data = heartbeat.model_dump(mode="json")
    data["telemetry"]["raw_logs_allowed"] = True

    with pytest.raises(ValidationError, match="False"):
        ByocAgentHeartbeat.model_validate(data)

    data = heartbeat.model_dump(mode="json")
    data["components"] = [
        {
            "name": "owner@example.com",
            "kind": "gateway",
            "status": "ok",
        }
    ]
    with pytest.raises(ValidationError, match="customer data"):
        ByocAgentHeartbeat.model_validate(data)


def test_heartbeat_contract_flags_unenrolled_or_duplicate_components() -> None:
    heartbeat = heartbeat_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        sequence=1,
        validation_status="degraded",
        control_plane_connected=True,
        components=(
            ByocAgentComponentStatus(name="gateway", kind="gateway", status="ok"),
            ByocAgentComponentStatus(name="gateway", kind="gateway", status="ok"),
        ),
    )

    violations = validate_heartbeat_contract(
        heartbeat,
        manifest=MANIFEST,
        enrolled_agent_ids={"agt_other001"},
    )

    assert [violation.code for violation in violations] == [
        "agent_not_enrolled",
        "duplicate_component",
    ]


@pytest.mark.asyncio
async def test_mock_control_plane_accepts_enrollment_and_heartbeat() -> None:
    app = build_mock_control_plane_app(MANIFEST, install_token=INSTALL_TOKEN)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        enrollment = signed_enrollment_request(_payload(), install_token=INSTALL_TOKEN)
        enroll_response = await client.post(
            "/byoc/agent/enroll",
            json=enrollment.model_dump(mode="json"),
        )
        assert enroll_response.status_code == 200
        assert enroll_response.json()["status"] == "accepted"

        heartbeat = heartbeat_from_manifest(
            MANIFEST,
            agent_id=AGENT_ID,
            agent_version=AGENT_VERSION,
            sequence=1,
            validation_status="passing",
            control_plane_connected=True,
        )
        heartbeat_response = await client.post(
            "/byoc/agent/heartbeat",
            json=heartbeat.model_dump(mode="json"),
        )

    assert heartbeat_response.status_code == 200
    assert heartbeat_response.json()["status"] == "accepted"
    assert len(app.state.heartbeats) == 1


@pytest.mark.asyncio
async def test_mock_control_plane_rejects_bad_signature_and_unenrolled_heartbeat() -> None:
    app = build_mock_control_plane_app(MANIFEST, install_token=INSTALL_TOKEN)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        bad_enrollment = signed_enrollment_request(_payload(), install_token="bad-token")
        enroll_response = await client.post(
            "/byoc/agent/enroll",
            json=bad_enrollment.model_dump(mode="json"),
        )
        assert enroll_response.status_code == 403
        assert "invalid_signature" in str(enroll_response.json())

        heartbeat = heartbeat_from_manifest(
            MANIFEST,
            agent_id=AGENT_ID,
            agent_version=AGENT_VERSION,
            sequence=1,
            validation_status="passing",
            control_plane_connected=True,
        )
        heartbeat_response = await client.post(
            "/byoc/agent/heartbeat",
            json=heartbeat.model_dump(mode="json"),
        )

    assert heartbeat_response.status_code == 403
    assert "agent_not_enrolled" in str(heartbeat_response.json())


def test_agent_contract_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["enrollment_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.agent.enrollment.v1"
    )
    assert bundle["heartbeat"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.agent.heartbeat.v1"
    )
