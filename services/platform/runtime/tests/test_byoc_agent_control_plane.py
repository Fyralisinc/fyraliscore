from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from services.platform.runtime.byoc_agent_contract import (
    ByocAgentComponentStatus,
    heartbeat_from_manifest,
    enrollment_payload_from_manifest,
    signed_enrollment_request,
)
from services.platform.runtime.byoc_agent_control_plane import (
    InMemoryByocAgentRegistryStore,
    desired_state_poll_payload,
    signed_desired_state_poll_request,
    validate_agent_enrollment_request,
    validate_agent_heartbeat_request,
    validate_desired_state_poll_request,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")
INSTALL_TOKEN = "local-install-token-for-control-plane-agent-tests"
AGENT_ID = "agt_control01"
AGENT_VERSION = "0.1.0"
REQUESTED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


def _enrollment(*, install_token: str = INSTALL_TOKEN):
    payload = enrollment_payload_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-control-plane-agent-001",
        requested_at=REQUESTED_AT,
    )
    return signed_enrollment_request(payload, install_token=install_token)


def _heartbeat(sequence: int = 1):
    return heartbeat_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        sequence=sequence,
        validation_status="passing",
        control_plane_connected=True,
        components=(
            ByocAgentComponentStatus(
                name="gateway",
                kind="gateway",
                status="ok",
                detail_code="ready",
            ),
            ByocAgentComponentStatus(
                name="agent",
                kind="agent",
                status="ok",
                detail_code="probe",
            ),
        ),
        sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
    )


def test_agent_enrollment_validation_verifies_signature_and_identity() -> None:
    request = _enrollment()

    assert validate_agent_enrollment_request(
        request,
        install_token=INSTALL_TOKEN,
        expected_install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
        expected_deployment_id=MANIFEST.deployment_id,
        expected_customer_id=MANIFEST.customer_id,
        expected_cloud_provider=MANIFEST.cloud_provider,
        expected_region=MANIFEST.region,
    ) == []

    bad = _enrollment(install_token="wrong-token")
    violations = validate_agent_enrollment_request(
        bad,
        install_token=INSTALL_TOKEN,
        expected_install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
    )
    assert [violation.code for violation in violations] == ["invalid_signature"]


def test_agent_heartbeat_validation_rejects_duplicate_components() -> None:
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

    violations = validate_agent_heartbeat_request(
        heartbeat,
        expected_deployment_id=MANIFEST.deployment_id,
        expected_customer_id=MANIFEST.customer_id,
        expected_telemetry_mode=MANIFEST.telemetry.mode,
        expected_telemetry_contract=MANIFEST.telemetry.contract,
    )

    assert [violation.code for violation in violations] == ["duplicate_component"]


async def test_in_memory_agent_store_keeps_sanitized_latest_heartbeat() -> None:
    store = InMemoryByocAgentRegistryStore()
    enrollment = _enrollment()
    enrolled = await store.enroll(
        enrollment,
        enrolled_at=REQUESTED_AT,
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )
    heartbeat = _heartbeat()
    heartbeat_response = await store.heartbeat(
        heartbeat,
        accepted_at=datetime(2026, 6, 26, 12, 2, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
    )

    state = await store.get(
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id=AGENT_ID,
    )
    assert enrolled.status == "accepted"
    assert heartbeat_response is not None
    assert heartbeat_response.status == "accepted"
    assert state is not None
    assert state.stored_scope == "sanitized_agent_metadata_only"
    assert state.latest_heartbeat is not None
    assert state.latest_heartbeat.component_count == 2
    assert state.latest_heartbeat.ok_component_count == 2
    serialized = json.dumps(state.model_dump(mode="json"), sort_keys=True)
    assert INSTALL_TOKEN not in serialized
    assert "signature" not in serialized.lower()
    assert "payload" not in serialized.lower()
    assert MANIFEST.connectivity.control_plane_url not in serialized


async def test_in_memory_agent_store_rejects_unenrolled_heartbeat() -> None:
    store = InMemoryByocAgentRegistryStore()

    response = await store.heartbeat(_heartbeat())

    assert response is None


def _desired_state_poll(*, install_token: str = INSTALL_TOKEN):
    payload = desired_state_poll_payload(
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        artifact_revision=MANIFEST.artifact_revision,
        install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
        nonce="nonce-control-plane-agent-desired-001",
        last_seen_desired_revision=MANIFEST.artifact_revision,
        requested_at=datetime(2026, 6, 26, 12, 3, tzinfo=UTC),
    )
    return signed_desired_state_poll_request(payload, install_token=install_token)


def test_desired_state_poll_validation_verifies_signature_and_identity() -> None:
    request = _desired_state_poll()

    assert validate_desired_state_poll_request(
        request,
        install_token=INSTALL_TOKEN,
        expected_install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
        expected_deployment_id=MANIFEST.deployment_id,
        expected_customer_id=MANIFEST.customer_id,
    ) == []

    bad = _desired_state_poll(install_token="wrong-token")
    violations = validate_desired_state_poll_request(
        bad,
        install_token=INSTALL_TOKEN,
        expected_install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
    )
    assert [violation.code for violation in violations] == ["invalid_signature"]


async def test_in_memory_agent_store_returns_sanitized_desired_state() -> None:
    store = InMemoryByocAgentRegistryStore()
    await store.enroll(
        _enrollment(),
        enrolled_at=REQUESTED_AT,
        desired_revision="2026.06.26-2",
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )

    response = await store.desired_state(
        _desired_state_poll(),
        accepted_at=datetime(2026, 6, 26, 12, 4, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
        config_epoch=2,
        evidence_package_required=True,
    )

    assert response is not None
    assert response.status == "accepted"
    assert response.current_revision == MANIFEST.artifact_revision
    assert response.desired_revision == "2026.06.26-2"
    assert response.rollout_action == "apply_revision"
    assert response.config_scope == "metadata_only"
    assert response.evidence_package_required is True
    serialized = response.model_dump_json()
    assert INSTALL_TOKEN not in serialized
    assert "signature" not in serialized.lower()
    assert "payload" not in serialized.lower()
    assert MANIFEST.connectivity.control_plane_url not in serialized


async def test_in_memory_agent_store_rejects_unenrolled_desired_state_poll() -> None:
    store = InMemoryByocAgentRegistryStore()

    response = await store.desired_state(_desired_state_poll())

    assert response is None
