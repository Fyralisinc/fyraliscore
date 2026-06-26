from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

import services.app.gateway.byoc_agent_keys as agent_keys
from services.app.gateway.byoc_agent_router import build_byoc_agent_router
from services.app.gateway.settings import GatewaySettings
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
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")
INSTALL_TOKEN = "local-install-token-for-agent-router-tests"
INSTALL_TOKEN_REF = MANIFEST.secrets.bootstrap_token_secret_ref
AGENT_ID = "agt_router01"
AGENT_VERSION = "0.1.0"
REQUESTED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


class _FakeAgentPool:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str], dict] = {}
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        query_start = query.lstrip().upper()
        if query_start.startswith("INSERT"):
            key = (args[0], args[1], args[2])
            row = {
                "deployment_id": args[0],
                "customer_id": args[1],
                "agent_id": args[2],
                "agent_version": args[3],
                "artifact_revision": args[4],
                "cloud_provider": args[5],
                "region": args[6],
                "install_token_secret_ref": args[7],
                "desired_revision": args[8],
                "heartbeat_interval_seconds": args[9],
                "telemetry_contract": args[10],
                "enrolled_at": args[11],
                "stored_scope": args[12],
                "latest_heartbeat_sequence": None,
            }
            self.rows[key] = row
            return {
                "deployment_id": row["deployment_id"],
                "agent_id": row["agent_id"],
                "desired_revision": row["desired_revision"],
                "heartbeat_interval_seconds": row["heartbeat_interval_seconds"],
                "telemetry_contract": row["telemetry_contract"],
                "enrolled_at": row["enrolled_at"],
            }
        if query_start.startswith("UPDATE"):
            key = (args[0], args[1], args[2])
            row = self.rows.get(key)
            if row is None:
                return None
            row.update(
                {
                    "agent_version": args[3],
                    "artifact_revision": args[4],
                    "desired_revision": args[5] or row["desired_revision"],
                    "latest_heartbeat_sequence": args[6],
                    "latest_validation_status": args[7],
                    "latest_control_plane_connected": args[8],
                    "latest_telemetry_mode": args[9],
                    "latest_telemetry_contract": args[10],
                    "latest_component_count": args[11],
                    "latest_ok_component_count": args[12],
                    "latest_degraded_component_count": args[13],
                    "latest_failed_component_count": args[14],
                    "latest_unknown_component_count": args[15],
                    "latest_queued_batches": args[16],
                    "latest_dropped_batches": args[17],
                    "latest_heartbeat_sent_at": args[18],
                    "latest_heartbeat_accepted_at": args[19],
                }
            )
            return {
                "deployment_id": row["deployment_id"],
                "agent_id": row["agent_id"],
                "desired_revision": row["desired_revision"],
            }
        key = (args[0], args[1], args[2])
        return self.rows.get(key)


def _enrollment(*, install_token: str = INSTALL_TOKEN):
    payload = enrollment_payload_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-agent-router-001",
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
        ),
        sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
    )


def _desired_state_poll(*, install_token: str = INSTALL_TOKEN):
    payload = desired_state_poll_payload(
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        artifact_revision=MANIFEST.artifact_revision,
        install_token_secret_ref=INSTALL_TOKEN_REF,
        nonce="nonce-agent-router-desired-001",
        last_seen_desired_revision=MANIFEST.artifact_revision,
        requested_at=datetime(2026, 6, 26, 12, 3, tzinfo=UTC),
    )
    return signed_desired_state_poll_request(payload, install_token=install_token)


def _app(*, configured: bool = True) -> tuple[FastAPI, InMemoryByocAgentRegistryStore]:
    store = InMemoryByocAgentRegistryStore()
    app = FastAPI()
    app.state.byoc_agent_registry_store = store
    if configured:
        app.state.byoc_agent_install_token = INSTALL_TOKEN
        app.state.byoc_agent_install_token_secret_ref = INSTALL_TOKEN_REF
    app.include_router(build_byoc_agent_router())
    return app, store


@pytest.mark.asyncio
async def test_byoc_agent_router_accepts_enrollment_and_heartbeat() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        enrollment_response = await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )
        heartbeat_response = await client.post(
            "/byoc/agent/heartbeat",
            json=_heartbeat().model_dump(mode="json"),
        )
        desired_state_response = await client.post(
            "/byoc/agent/desired-state",
            json=_desired_state_poll().model_dump(mode="json"),
        )

    assert enrollment_response.status_code == 200
    assert enrollment_response.json()["status"] == "accepted"
    assert heartbeat_response.status_code == 200
    assert heartbeat_response.json()["status"] == "accepted"
    assert desired_state_response.status_code == 200
    desired_state = desired_state_response.json()
    assert desired_state["status"] == "accepted"
    assert desired_state["rollout_action"] == "none"
    assert desired_state["config_scope"] == "metadata_only"
    assert INSTALL_TOKEN not in desired_state_response.text
    assert "signature" not in desired_state_response.text.lower()
    assert len(store.records) == 1
    serialized_state = store.records[0].model_dump_json()
    assert INSTALL_TOKEN not in serialized_state
    assert "signature" not in serialized_state.lower()
    assert "payload" not in serialized_state.lower()
    assert MANIFEST.connectivity.control_plane_url not in serialized_state


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_bad_signature() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/agent/enroll",
            json=_enrollment(install_token="wrong-token").model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_heartbeat_before_enrollment() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/agent/heartbeat",
            json=_heartbeat().model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert "agent_not_enrolled" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_desired_state_before_enrollment() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/agent/desired-state",
            json=_desired_state_poll().model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert "agent_not_enrolled" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_bad_desired_state_signature() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )
        response = await client.post(
            "/byoc/agent/desired-state",
            json=_desired_state_poll(install_token="wrong-token").model_dump(
                mode="json"
            ),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_duplicate_heartbeat_components() -> None:
    app, _ = _app()
    duplicate = heartbeat_from_manifest(
        MANIFEST,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        sequence=1,
        validation_status="passing",
        control_plane_connected=True,
        components=(
            ByocAgentComponentStatus(name="gateway", kind="gateway", status="ok"),
            ByocAgentComponentStatus(name="gateway", kind="gateway", status="ok"),
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )
        response = await client.post(
            "/byoc/agent/heartbeat",
            json=duplicate.model_dump(mode="json"),
        )

    assert response.status_code == 400
    assert "duplicate_component" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_rejects_legacy_static_token_in_production() -> None:
    app, _ = _app()
    app.state.gateway_settings = GatewaySettings(environment="production")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_agent_router_uses_managed_install_token_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _fake_load_secret_text_from_config(secret_ref: str, *_, **__) -> str:
        calls.append(secret_ref)
        return INSTALL_TOKEN

    monkeypatch.setattr(
        agent_keys,
        "load_secret_text_from_config",
        _fake_load_secret_text_from_config,
    )
    store = InMemoryByocAgentRegistryStore()
    app = FastAPI()
    app.state.byoc_agent_registry_store = store
    app.state.gateway_settings = GatewaySettings(
        environment="production",
        deployment_mode="byoc",
        byoc_deployment_id=MANIFEST.deployment_id,
        byoc_customer_id=MANIFEST.customer_id,
        byoc_cloud_provider=MANIFEST.cloud_provider,
        byoc_region=MANIFEST.region,
        data_plane_agent_install_token_secret_ref=INSTALL_TOKEN_REF,
        telemetry_mode=MANIFEST.telemetry.mode,
    )
    app.include_router(build_byoc_agent_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert calls == [INSTALL_TOKEN_REF]


@pytest.mark.asyncio
async def test_byoc_agent_router_uses_postgres_store_from_gateway_deps() -> None:
    pool = _FakeAgentPool()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.byoc_agent_install_token = INSTALL_TOKEN
    app.state.byoc_agent_install_token_secret_ref = INSTALL_TOKEN_REF
    app.include_router(build_byoc_agent_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        enrollment_response = await client.post(
            "/byoc/agent/enroll",
            json=_enrollment().model_dump(mode="json"),
        )
        heartbeat_response = await client.post(
            "/byoc/agent/heartbeat",
            json=_heartbeat().model_dump(mode="json"),
        )
        desired_state_response = await client.post(
            "/byoc/agent/desired-state",
            json=_desired_state_poll().model_dump(mode="json"),
        )

    assert enrollment_response.status_code == 200
    assert heartbeat_response.status_code == 200
    assert desired_state_response.status_code == 200
    flattened_args = " ".join(str(arg) for _, args in pool.calls for arg in args)
    assert INSTALL_TOKEN not in flattened_args
    assert "signature" not in flattened_args.lower()
    assert "payload" not in flattened_args.lower()
    assert MANIFEST.connectivity.control_plane_url not in flattened_args
