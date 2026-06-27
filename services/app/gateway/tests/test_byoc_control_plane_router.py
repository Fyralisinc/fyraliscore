from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

import services.app.gateway.byoc_control_plane_keys as key_resolvers
from services.app.gateway.byoc_control_plane_router import (
    build_byoc_control_plane_router,
)
from services.app.gateway.settings import GatewaySettings
from services.platform.runtime.byoc_agent_contract import (
    ByocAgentComponentStatus,
    enrollment_payload_from_manifest,
    heartbeat_from_manifest,
    signed_enrollment_request,
)
from services.platform.runtime.byoc_agent_control_plane import (
    InMemoryByocAgentRegistryStore,
    desired_state_poll_payload,
    desired_state_update_payload,
    signed_desired_state_poll_request,
    signed_desired_state_update_request,
)
from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerInputs,
    run_byoc_agent_runner,
)
from services.platform.runtime.byoc_control_plane_intake import (
    InMemoryByocEvidencePackageIntakeStore,
    evidence_package_submission_payload,
    signed_evidence_receipt_read_headers,
    signed_evidence_package_submission,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_package import load_byoc_evidence_package
from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    run_byoc_preflight_bundle,
)
from services.platform.runtime.byoc_preflight_intake import (
    InMemoryByocPreflightReportIntakeStore,
    preflight_report_submission_payload,
    signed_preflight_report_submission,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    InMemoryByocRunnerEvidenceIntakeStore,
    runner_evidence_submission_payload,
    runner_evidence_summary_from_report,
    signed_runner_evidence_submission,
)


ROOT = Path(__file__).resolve().parents[4]
PACKAGE = load_byoc_evidence_package(ROOT / "deploy/byoc/evidence-package.example.yaml")
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
MANIFEST = load_byoc_manifest(MANIFEST_PATH)
BUNDLE_NEXT_PATH = ROOT / "deploy/byoc/bootstrap-bundle.next.example.yaml"
PERMISSIONS_PATH = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE_PATH = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
IAC_PACKAGE_PATH = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
BUNDLE_PATH = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN_PATH = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"
SIGNING_SECRET = "local-control-plane-intake-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_intake01"
DESIRED_STATE_AGENT_ID = "agt_controlrouter01"
AGENT_VERSION = "0.1.0"
SUBMITTED_AT = datetime(2026, 6, 26, 12, 30, tzinfo=UTC)
INSTALL_TOKEN = "local-control-plane-runner-evidence-token"
AGENT_INSTALL_TOKEN = "local-control-plane-agent-install-token"
AGENT_INSTALL_TOKEN_REF = MANIFEST.secrets.bootstrap_token_secret_ref


def _submission(*, signing_secret: str = SIGNING_SECRET):
    payload = evidence_package_submission_payload(
        package=PACKAGE,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-intake-router-001",
        submitted_at=SUBMITTED_AT,
    )
    return signed_evidence_package_submission(
        payload,
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


async def _runner_submission(*, signing_secret: str = SIGNING_SECRET):
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id="agt_runnerrouter01",
            agent_version="2026.06.26-router",
            nonce_prefix="nonce-runner-router",
            iterations=1,
            mock_desired_revision="2026.06.26-2",
            mock_config_epoch=3,
            bootstrap_bundle_path=BUNDLE_NEXT_PATH,
            verify_local_bundle_files=True,
            repo_root=ROOT,
            requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
            sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
        )
    )
    payload = runner_evidence_submission_payload(
        evidence=runner_evidence_summary_from_report(report),
        nonce="nonce-runner-router-001",
        submitted_at=SUBMITTED_AT,
    )
    return signed_runner_evidence_submission(
        payload,
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


def _preflight_submission(*, signing_secret: str = SIGNING_SECRET):
    report = run_byoc_preflight_bundle(
        ByocPreflightBundleInputs(
            dataplane_manifest_path=MANIFEST_PATH,
            permissions_manifest_path=PERMISSIONS_PATH,
            iam_template_path=IAM_TEMPLATE_PATH,
            iac_package_path=IAC_PACKAGE_PATH,
            bootstrap_bundle_path=BUNDLE_PATH,
            bootstrap_plan_path=PLAN_PATH,
            env_path=ENV_TEMPLATE,
            repo_root=ROOT,
        )
    )
    payload = preflight_report_submission_payload(
        preflight_report=report,
        agent_id="agt_preflightrouter01",
        agent_version="2026.06.26-preflight-router",
        nonce="nonce-preflight-router-001",
        submitted_at=SUBMITTED_AT,
    )
    return signed_preflight_report_submission(
        payload,
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


def _agent_enrollment():
    payload = enrollment_payload_from_manifest(
        MANIFEST,
        agent_id=DESIRED_STATE_AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-control-plane-router-agent-001",
        requested_at=datetime(2026, 6, 26, 12, 20, tzinfo=UTC),
    )
    return signed_enrollment_request(payload, install_token=AGENT_INSTALL_TOKEN)


def _agent_heartbeat():
    return heartbeat_from_manifest(
        MANIFEST,
        agent_id=DESIRED_STATE_AGENT_ID,
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
        sent_at=datetime(2026, 6, 26, 12, 22, tzinfo=UTC),
    )


def _desired_state_update(*, signing_secret: str = SIGNING_SECRET):
    payload = desired_state_update_payload(
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id=DESIRED_STATE_AGENT_ID,
        desired_revision="2026.06.26-2",
        config_epoch=3,
        evidence_package_required=True,
        reason_code="rollout_rehearsal",
        requested_by="ops_backend",
        nonce="nonce-control-plane-router-desired-update-001",
        requested_at=datetime(2026, 6, 26, 12, 25, tzinfo=UTC),
    )
    return signed_desired_state_update_request(
        payload,
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


def _desired_state_poll():
    payload = desired_state_poll_payload(
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id=DESIRED_STATE_AGENT_ID,
        agent_version=AGENT_VERSION,
        artifact_revision=MANIFEST.artifact_revision,
        install_token_secret_ref=AGENT_INSTALL_TOKEN_REF,
        nonce="nonce-control-plane-router-desired-poll-001",
        last_seen_desired_revision=MANIFEST.artifact_revision,
        requested_at=datetime(2026, 6, 26, 12, 26, tzinfo=UTC),
    )
    return signed_desired_state_poll_request(
        payload,
        install_token=AGENT_INSTALL_TOKEN,
    )


def _read_headers(
    path: str,
    *,
    query: str = "",
    nonce: str = "nonce-intake-router-read-001",
) -> dict[str, str]:
    return signed_evidence_receipt_read_headers(
        method="GET",
        path=path,
        query=query,
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
        nonce=nonce,
    )


class _FakeReceiptPool:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        if (
            query.lstrip().upper().startswith("INSERT")
            and "byoc_runner_evidence_receipts" in query
        ):
            row = {
                "receipt_id": args[0],
                "deployment_id": args[1],
                "customer_id": args[2],
                "agent_id": args[3],
                "agent_version": args[4],
                "cloud_provider": args[5],
                "region": args[6],
                "control_plane_mode": args[7],
                "evidence_digest": args[8],
                "current_artifact_revision": args[9],
                "desired_revision": args[10],
                "rollout_action": args[11],
                "runner_status": args[12],
                "required_checks_passed": args[13],
                "apply_plan_count": args[14],
                "artifact_verification_count": args[15],
                "digest_pinned_artifact_count": args[16],
                "local_digest_checked_count": args[17],
                "submitted_at": args[18],
                "accepted_at": args[19],
                "stored_scope": args[20],
            }
            self.rows[row["receipt_id"]] = row
            return row
        if (
            query.lstrip().upper().startswith("INSERT")
            and "byoc_preflight_report_receipts" in query
        ):
            row = {
                "receipt_id": args[0],
                "deployment_id": args[1],
                "customer_id": args[2],
                "agent_id": args[3],
                "agent_version": args[4],
                "artifact_revision": args[5],
                "cloud_provider": args[6],
                "region": args[7],
                "report_digest": args[8],
                "preflight_status": args[9],
                "required_sections_passed": args[10],
                "section_count": args[11],
                "failed_section_count": args[12],
                "terraform_validate_executed": args[13],
                "submitted_at": args[14],
                "accepted_at": args[15],
                "stored_scope": args[16],
            }
            self.rows[row["receipt_id"]] = row
            return row
        if query.lstrip().upper().startswith("INSERT"):
            row = {
                "receipt_id": args[0],
                "deployment_id": args[1],
                "customer_id": args[2],
                "agent_id": args[3],
                "agent_version": args[4],
                "artifact_revision": args[5],
                "cloud_provider": args[6],
                "region": args[7],
                "package_digest": args[8],
                "package_generated_at": args[9],
                "ledger_overall_status": args[10],
                "required_evidence_passed": args[11],
                "live_report_envelope_digest": args[12],
                "submitted_at": args[13],
                "accepted_at": args[14],
                "stored_scope": args[15],
            }
            self.rows[row["receipt_id"]] = row
            return row
        return self.rows.get(args[0])

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        rows = list(self.rows.values())
        arg_index = 0
        if "deployment_id = $" in query:
            deployment_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["deployment_id"] == deployment_id]
        if "customer_id = $" in query:
            customer_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["customer_id"] == customer_id]
        limit = args[-1]
        rows.sort(
            key=lambda row: (row["accepted_at"], row["receipt_id"]),
            reverse=True,
        )
        return rows[:limit]


def _app(
    *,
    configured: bool = True,
) -> tuple[FastAPI, InMemoryByocEvidencePackageIntakeStore]:
    store = InMemoryByocEvidencePackageIntakeStore()
    app = FastAPI()
    app.state.byoc_evidence_intake_store = store
    if configured:
        app.state.byoc_evidence_intake_secret = SIGNING_SECRET
        app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    return app, store


@pytest.mark.asyncio
async def test_byoc_control_plane_accepts_signed_evidence_package() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )
        assert response.status_code == 202
        receipt = response.json()
        lookup_path = (
            f"/byoc/control-plane/evidence-packages/{receipt['receipt_id']}"
        )
        lookup = await client.get(
            lookup_path,
            headers=_read_headers(
                lookup_path,
                nonce="nonce-intake-router-read-lookup",
            ),
        )
        query = f"deployment_id={PACKAGE.deployment_id}&limit=10"
        list_path = "/byoc/control-plane/evidence-packages"
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-intake-router-read-list",
            ),
        )

    assert receipt["status"] == "accepted"
    assert receipt["stored_scope"] == "sanitized_metadata_only"
    assert receipt["deployment_id"] == PACKAGE.deployment_id
    assert len(store.records) == 1
    assert lookup.status_code == 200
    assert '"ledger":' not in lookup.text
    assert "source_artifacts" not in lookup.text
    assert '"evidence":' not in lookup.text
    assert "gateway.customer.internal" not in lookup.text
    assert "postgresql://" not in lookup.text
    assert receipt_list.status_code == 200
    assert receipt_list.json()["result_count"] == 1
    assert "source_artifacts" not in receipt_list.text
    assert "gateway.customer.internal" not in receipt_list.text


@pytest.mark.asyncio
async def test_byoc_control_plane_accepts_signed_runner_evidence() -> None:
    runner_store = InMemoryByocRunnerEvidenceIntakeStore()
    app, _ = _app()
    app.state.byoc_runner_evidence_intake_store = runner_store
    list_path = "/byoc/control-plane/runner-evidence"
    query = f"deployment_id={MANIFEST.deployment_id}&limit=10"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/runner-evidence",
            json=(await _runner_submission()).model_dump(mode="json"),
        )
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-runner-router-list",
            ),
        )

    assert response.status_code == 202
    receipt = response.json()
    assert receipt["status"] == "accepted"
    assert receipt["stored_scope"] == "sanitized_metadata_only"
    assert receipt["apply_plan_count"] == 1
    assert receipt["artifact_verification_count"] == 1
    assert len(runner_store.records) == 1
    assert '"checks":' not in response.text
    assert "iterations" not in response.text
    assert "gateway_image" not in response.text
    assert INSTALL_TOKEN not in response.text
    assert receipt_list.status_code == 200
    assert receipt_list.json()["schema_version"] == (
        "fyralis.byoc.runner_evidence_receipt_list.v1"
    )
    assert receipt_list.json()["result_count"] == 1
    assert '"checks":' not in receipt_list.text
    assert "iterations" not in receipt_list.text
    assert "gateway_image" not in receipt_list.text
    assert INSTALL_TOKEN not in receipt_list.text


@pytest.mark.asyncio
async def test_byoc_control_plane_accepts_signed_preflight_report() -> None:
    preflight_store = InMemoryByocPreflightReportIntakeStore()
    app, _ = _app()
    app.state.byoc_preflight_report_intake_store = preflight_store
    list_path = "/byoc/control-plane/preflight-reports"
    query = f"deployment_id={MANIFEST.deployment_id}&limit=10"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/preflight-reports",
            json=_preflight_submission().model_dump(mode="json"),
        )
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-preflight-router-list",
            ),
        )

    assert response.status_code == 202
    receipt = response.json()
    assert receipt["status"] == "accepted"
    assert receipt["stored_scope"] == "sanitized_metadata_only"
    assert receipt["preflight_status"] == "pass"
    assert receipt["section_count"] == 7
    assert len(preflight_store.records) == 1
    assert '"sections":' not in response.text
    assert '"preflight_report":' not in response.text
    assert "ghcr.io" not in response.text
    assert "postgresql://" not in response.text
    assert receipt_list.status_code == 200
    assert receipt_list.json()["schema_version"] == (
        "fyralis.byoc.preflight_report_receipt_list.v1"
    )
    assert receipt_list.json()["result_count"] == 1
    assert '"sections":' not in receipt_list.text
    assert '"preflight_report":' not in receipt_list.text
    assert "ghcr.io" not in receipt_list.text
    assert "postgresql://" not in receipt_list.text


@pytest.mark.asyncio
async def test_byoc_control_plane_accepts_signed_agent_desired_state_update() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    await agent_store.enroll(
        _agent_enrollment(),
        enrolled_at=datetime(2026, 6, 26, 12, 21, tzinfo=UTC),
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )
    app, _ = _app()
    app.state.byoc_agent_registry_store = agent_store
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/agent-desired-state",
            json=_desired_state_update().model_dump(mode="json"),
        )

    assert response.status_code == 202
    receipt = response.json()
    assert receipt["status"] == "accepted"
    assert receipt["stored_scope"] == "sanitized_agent_metadata_only"
    assert receipt["previous_desired_revision"] == MANIFEST.artifact_revision
    assert receipt["desired_revision"] == "2026.06.26-2"
    assert receipt["config_epoch"] == 3
    assert receipt["evidence_package_required"] is True
    assert SIGNING_SECRET not in response.text
    assert "signature" not in response.text.lower()
    assert "payload" not in response.text.lower()

    desired = await agent_store.desired_state(
        _desired_state_poll(),
        accepted_at=datetime(2026, 6, 26, 12, 27, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
    )

    assert desired is not None
    assert desired.desired_revision == "2026.06.26-2"
    assert desired.rollout_action == "apply_revision"
    assert desired.config_epoch == 3
    assert desired.evidence_package_required is True


@pytest.mark.asyncio
async def test_byoc_control_plane_lists_signed_agent_fleet_metadata() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    await agent_store.enroll(
        _agent_enrollment(),
        enrolled_at=datetime(2026, 6, 26, 12, 21, tzinfo=UTC),
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )
    await agent_store.heartbeat(
        _agent_heartbeat(),
        accepted_at=datetime(2026, 6, 26, 12, 23, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
    )
    await agent_store.update_desired_state(
        _desired_state_update(),
        accepted_at=datetime(2026, 6, 26, 12, 26, tzinfo=UTC),
    )
    app, _ = _app()
    app.state.byoc_agent_registry_store = agent_store
    list_path = "/byoc/control-plane/agents"
    query = f"deployment_id={MANIFEST.deployment_id}&limit=10"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-control-plane-agent-fleet-list",
            ),
        )

    assert response.status_code == 200
    listing = response.json()
    assert listing["schema_version"] == "fyralis.byoc.agent_fleet_list.v1"
    assert listing["stored_scope"] == "sanitized_agent_metadata_only"
    assert listing["result_count"] == 1
    item = listing["items"][0]
    assert item["schema_version"] == "fyralis.byoc.agent_fleet_item.v1"
    assert item["agent_id"] == DESIRED_STATE_AGENT_ID
    assert item["desired_revision"] == "2026.06.26-2"
    assert item["desired_config_epoch"] == 3
    assert item["evidence_package_required"] is True
    assert item["latest_validation_status"] == "passing"
    assert item["latest_component_count"] == 1
    assert AGENT_INSTALL_TOKEN not in response.text
    assert "install_token" not in response.text.lower()
    assert "secret_ref" not in response.text.lower()
    assert "signature" not in response.text.lower()
    assert "payload" not in response.text.lower()
    assert MANIFEST.connectivity.control_plane_url not in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_serves_signed_deployment_overview_metadata() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    preflight_store = InMemoryByocPreflightReportIntakeStore()
    runner_store = InMemoryByocRunnerEvidenceIntakeStore()
    await agent_store.enroll(
        _agent_enrollment(),
        enrolled_at=datetime(2026, 6, 26, 12, 21, tzinfo=UTC),
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )
    await agent_store.heartbeat(
        _agent_heartbeat(),
        accepted_at=datetime(2026, 6, 26, 12, 23, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
    )
    await agent_store.update_desired_state(
        _desired_state_update(),
        accepted_at=datetime(2026, 6, 26, 12, 26, tzinfo=UTC),
    )
    app, evidence_store = _app()
    app.state.byoc_agent_registry_store = agent_store
    app.state.byoc_preflight_report_intake_store = preflight_store
    app.state.byoc_runner_evidence_intake_store = runner_store
    await evidence_store.put(
        _submission(),
        accepted_at=datetime(2026, 6, 26, 12, 28, tzinfo=UTC),
    )
    await preflight_store.put(
        _preflight_submission(),
        accepted_at=datetime(2026, 6, 26, 12, 29, tzinfo=UTC),
    )
    await runner_store.put(
        await _runner_submission(),
        accepted_at=datetime(2026, 6, 26, 12, 30, tzinfo=UTC),
    )
    path = "/byoc/control-plane/deployment-overview"
    query = f"deployment_id={MANIFEST.deployment_id}&customer_id={MANIFEST.customer_id}"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"{path}?{query}",
            headers=_read_headers(
                path,
                query=query,
                nonce="nonce-control-plane-overview-read",
            ),
        )

    assert response.status_code == 200
    overview = response.json()
    assert overview["schema_version"] == "fyralis.byoc.deployment_overview.v1"
    assert overview["deployment_id"] == MANIFEST.deployment_id
    assert overview["customer_id"] == MANIFEST.customer_id
    assert overview["stored_scope"] == "sanitized_deployment_metadata_only"
    assert overview["status"] == "ready"
    assert overview["next_action"] == "none"
    assert overview["metadata_sources"] == [
        "agent_fleet",
        "evidence_package_receipts",
        "preflight_report_receipts",
        "runner_evidence_receipts",
    ]
    assert overview["agent_summary"]["enrolled_count"] == 1
    assert overview["agent_summary"]["passing_count"] == 1
    assert overview["agent_summary"]["evidence_package_required_count"] == 1
    assert overview["evidence_summary"]["receipt_count"] == 1
    assert overview["evidence_summary"]["latest_required_evidence_passed"] is True
    assert overview["preflight_summary"]["receipt_count"] == 1
    assert overview["preflight_summary"]["latest_preflight_status"] == "pass"
    assert overview["runner_summary"]["receipt_count"] == 1
    assert overview["runner_summary"]["latest_runner_status"] == "pass"
    assert AGENT_INSTALL_TOKEN not in response.text
    assert SIGNING_SECRET not in response.text
    assert "install_token" not in response.text.lower()
    assert "secret_ref" not in response.text.lower()
    assert "signature" not in response.text.lower()
    assert "payload" not in response.text.lower()
    assert '"preflight_report":' not in response.text
    assert '"checks":' not in response.text
    assert "iterations" not in response.text
    assert "source_artifacts" not in response.text
    assert MANIFEST.connectivity.control_plane_url not in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_serves_signed_control_panel_state() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    preflight_store = InMemoryByocPreflightReportIntakeStore()
    runner_store = InMemoryByocRunnerEvidenceIntakeStore()
    await agent_store.enroll(
        _agent_enrollment(),
        enrolled_at=datetime(2026, 6, 26, 12, 21, tzinfo=UTC),
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
    )
    await agent_store.heartbeat(
        _agent_heartbeat(),
        accepted_at=datetime(2026, 6, 26, 12, 23, tzinfo=UTC),
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
    )
    await agent_store.update_desired_state(
        _desired_state_update(),
        accepted_at=datetime(2026, 6, 26, 12, 26, tzinfo=UTC),
    )
    app, evidence_store = _app()
    app.state.byoc_agent_registry_store = agent_store
    app.state.byoc_preflight_report_intake_store = preflight_store
    app.state.byoc_runner_evidence_intake_store = runner_store
    await evidence_store.put(
        _submission(),
        accepted_at=datetime(2026, 6, 26, 12, 28, tzinfo=UTC),
    )
    await preflight_store.put(
        _preflight_submission(),
        accepted_at=datetime(2026, 6, 26, 12, 29, tzinfo=UTC),
    )
    await runner_store.put(
        await _runner_submission(),
        accepted_at=datetime(2026, 6, 26, 12, 30, tzinfo=UTC),
    )
    path = "/byoc/control-plane/control-panel-state"
    query = (
        f"deployment_id={MANIFEST.deployment_id}"
        f"&customer_id={MANIFEST.customer_id}"
        "&recent_limit=5"
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"{path}?{query}",
            headers=_read_headers(
                path,
                query=query,
                nonce="nonce-control-panel-state-read",
            ),
        )

    assert response.status_code == 200
    state = response.json()
    assert state["schema_version"] == "fyralis.byoc.control_panel_state.v1"
    assert state["deployment_id"] == MANIFEST.deployment_id
    assert state["customer_id"] == MANIFEST.customer_id
    assert state["stored_scope"] == "sanitized_control_panel_metadata_only"
    assert state["overview"]["schema_version"] == "fyralis.byoc.deployment_overview.v1"
    assert state["overview"]["status"] == "ready"
    assert state["overview"]["next_action"] == "none"
    assert state["actions"] == []
    assert {section["key"]: section["status"] for section in state["sections"]} == {
        "deployment_overview": "ready",
        "agent_fleet": "ready",
        "evidence_packages": "ready",
        "preflight_reports": "ready",
        "runner_evidence": "ready",
    }
    assert state["agent_fleet"]["result_count"] == 1
    assert state["evidence_packages"]["result_count"] == 1
    assert state["preflight_reports"]["result_count"] == 1
    assert state["runner_evidence"]["result_count"] == 1
    assert AGENT_INSTALL_TOKEN not in response.text
    assert SIGNING_SECRET not in response.text
    assert "install_token" not in response.text.lower()
    assert "secret_ref" not in response.text.lower()
    assert "signature" not in response.text.lower()
    assert "payload" not in response.text.lower()
    assert '"preflight_report":' not in response.text
    assert '"checks":' not in response.text
    assert "iterations" not in response.text
    assert "source_artifacts" not in response.text
    assert MANIFEST.connectivity.control_plane_url not in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_agent_fleet_reads_require_signed_headers() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/byoc/control-plane/agents?deployment_id={MANIFEST.deployment_id}"
        )

    assert response.status_code == 403
    assert "missing_read_auth_headers" in response.text


@pytest.mark.asyncio
async def test_byoc_control_panel_state_requires_signed_headers() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/byoc/control-plane/control-panel-state"
            f"?deployment_id={MANIFEST.deployment_id}"
        )

    assert response.status_code == 403
    assert "missing_read_auth_headers" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_bad_desired_state_signature() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    await agent_store.enroll(
        _agent_enrollment(),
        enrolled_at=datetime(2026, 6, 26, 12, 21, tzinfo=UTC),
    )
    app, _ = _app()
    app.state.byoc_agent_registry_store = agent_store
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/agent-desired-state",
            json=_desired_state_update(signing_secret="wrong-secret").model_dump(
                mode="json"
            ),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_unenrolled_agent_desired_state_update() -> None:
    agent_store = InMemoryByocAgentRegistryStore()
    app, _ = _app()
    app.state.byoc_agent_registry_store = agent_store
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/agent-desired-state",
            json=_desired_state_update().model_dump(mode="json"),
        )

    assert response.status_code == 404
    assert "agent_not_enrolled" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_receipt_reads_require_signed_headers() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/byoc/control-plane/evidence-packages/evpkg_0")

    assert response.status_code == 403
    assert "missing_read_auth_headers" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_receipt_list_requires_query_bound() -> None:
    app, _ = _app()
    list_path = "/byoc/control-plane/evidence-packages"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            list_path,
            headers=_read_headers(
                list_path,
                nonce="nonce-intake-router-unbounded-list",
            ),
        )

    assert response.status_code == 400
    assert "deployment_id or customer_id" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_unknown_key_ref() -> None:
    app, _ = _app()
    submission = _submission().model_dump(mode="json")
    submission["signature"]["key_ref"] = "control-plane/byoc/unknown-key"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=submission,
        )

    assert response.status_code == 403
    assert "unknown_key_ref" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_legacy_static_secret_in_production() -> None:
    app, _ = _app()
    app.state.gateway_settings = GatewaySettings(environment="production")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_managed_key_resolver_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, bool | None]] = []

    def _fake_load_app_secret_text_from_env(
        name: str,
        *,
        production: bool | None = None,
        **_,
    ) -> str:
        calls.append((name, production))
        return SIGNING_SECRET

    monkeypatch.setattr(
        key_resolvers,
        "load_app_secret_text_from_env",
        _fake_load_app_secret_text_from_env,
    )
    store = InMemoryByocEvidencePackageIntakeStore()
    app = FastAPI()
    app.state.byoc_evidence_intake_store = store
    app.state.gateway_settings = GatewaySettings(
        environment="production",
        byoc_evidence_intake_key_ref=SIGNING_KEY_REF,
        byoc_evidence_intake_signing_key_secret_ref=(
            "prod/fyralis/dep-test/evidence-intake-signing-key"
        ),
        byoc_evidence_read_key_ref=SIGNING_KEY_REF,
        byoc_evidence_read_signing_key_secret_ref=(
            "prod/fyralis/dep-test/evidence-read-signing-key"
        ),
    )
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )
        receipt = response.json()
        lookup_path = (
            f"/byoc/control-plane/evidence-packages/{receipt['receipt_id']}"
        )
        lookup = await client.get(
            lookup_path,
            headers=_read_headers(
                lookup_path,
                nonce="nonce-intake-managed-read-lookup",
            ),
        )

    assert response.status_code == 202
    assert lookup.status_code == 200
    assert calls == [
        ("FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY", True),
        ("FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY", True),
    ]


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_postgres_store_from_gateway_deps() -> None:
    pool = _FakeReceiptPool()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.byoc_evidence_intake_secret = SIGNING_SECRET
    app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )
        query = f"deployment_id={PACKAGE.deployment_id}&customer_id={PACKAGE.customer_id}"
        list_path = "/byoc/control-plane/evidence-packages"
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-intake-router-pg-list",
            ),
        )

    assert response.status_code == 202
    assert receipt_list.status_code == 200
    assert receipt_list.json()["result_count"] == 1
    assert pool.calls
    flattened_args = " ".join(str(arg) for _, args in pool.calls for arg in args)
    assert "source_artifacts" not in flattened_args
    assert "fyralis.byoc.evidence_package.v1" not in flattened_args


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_postgres_store_for_runner_evidence() -> None:
    pool = _FakeReceiptPool()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.byoc_evidence_intake_secret = SIGNING_SECRET
    app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/runner-evidence",
            json=(await _runner_submission()).model_dump(mode="json"),
        )

    assert response.status_code == 202
    assert pool.calls
    flattened_args = " ".join(str(arg) for _, args in pool.calls for arg in args)
    assert "fyralis.byoc.runner_evidence_summary.v1" not in flattened_args
    assert "apply_plan_ids" not in flattened_args
    assert "artifact_verification_ids" not in flattened_args
    assert "gateway_image" not in flattened_args
    assert INSTALL_TOKEN not in flattened_args


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_postgres_store_for_preflight_reports() -> None:
    pool = _FakeReceiptPool()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.byoc_evidence_intake_secret = SIGNING_SECRET
    app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/preflight-reports",
            json=_preflight_submission().model_dump(mode="json"),
        )

    assert response.status_code == 202
    assert pool.calls
    flattened_args = " ".join(str(arg) for _, args in pool.calls for arg in args)
    assert "fyralis.byoc.preflight_bundle.v1" not in flattened_args
    assert '"preflight_report":' not in flattened_args
    assert "sections" not in flattened_args
    assert "ghcr.io" not in flattened_args


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_bad_signature() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission(signing_secret="wrong-secret").model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_bad_runner_evidence_signature() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/runner-evidence",
            json=(await _runner_submission(signing_secret="wrong-secret")).model_dump(
                mode="json"
            ),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_requires_configured_intake_secret() -> None:
    app, _ = _app(configured=False)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_requires_configured_runner_evidence_secret() -> None:
    app, _ = _app(configured=False)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/runner-evidence",
            json=(await _runner_submission()).model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_raw_report_extra_field() -> None:
    app, _ = _app()
    submission = _submission().model_dump(mode="json")
    submission["package"]["raw_report"] = {
        "details": "https://gateway.customer.internal token=secret",
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=submission,
        )

    assert response.status_code == 422
    assert "Extra inputs are not permitted" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_runner_evidence_raw_extra_field() -> None:
    app, _ = _app()
    submission = (await _runner_submission()).model_dump(mode="json")
    submission["evidence"]["checks"] = [
        {"details": "https://gateway.customer.internal token=secret"}
    ]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/runner-evidence",
            json=submission,
        )

    assert response.status_code == 422
    assert "Extra inputs are not permitted" in response.text
