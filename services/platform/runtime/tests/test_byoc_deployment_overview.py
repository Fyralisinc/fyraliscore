from __future__ import annotations

from datetime import UTC, datetime

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentFleetItem,
    ByocAgentFleetList,
)
from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageIntakeRecord,
    ByocEvidencePackageReceipt,
    ByocEvidencePackageReceiptList,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverviewQuery,
    build_byoc_deployment_overview,
)


DEPLOYMENT_ID = "dep_overview01"
CUSTOMER_ID = "cus_overview01"
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def _agent(
    *,
    agent_id: str = "agt_overview01",
    validation_status: str | None = "passing",
    connected: bool | None = True,
    evidence_required: bool = True,
) -> ByocAgentFleetItem:
    return ByocAgentFleetItem(
        schema_version="fyralis.byoc.agent_fleet_item.v1",
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        agent_id=agent_id,
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-1",
        cloud_provider="aws",
        region="us-east-1",
        desired_revision="2026.06.27-2",
        desired_config_epoch=2,
        evidence_package_required=evidence_required,
        heartbeat_interval_seconds=30,
        telemetry_contract="fyralis.byoc.agent.telemetry.v1",
        enrolled_at=datetime(2026, 6, 27, 11, 45, tzinfo=UTC),
        desired_state_updated_at=datetime(2026, 6, 27, 11, 55, tzinfo=UTC),
        latest_heartbeat_sequence=3,
        latest_validation_status=validation_status,
        latest_control_plane_connected=connected,
        latest_telemetry_mode="aggregate-only",
        latest_component_count=3,
        latest_ok_component_count=3,
        latest_degraded_component_count=0,
        latest_failed_component_count=0,
        latest_unknown_component_count=0,
        latest_queued_batches=0,
        latest_dropped_batches=0,
        latest_heartbeat_sent_at=datetime(2026, 6, 27, 11, 59, tzinfo=UTC),
        latest_heartbeat_accepted_at=datetime(2026, 6, 27, 11, 59, tzinfo=UTC),
        stored_scope="sanitized_agent_metadata_only",
    )


def _agents(*items: ByocAgentFleetItem) -> ByocAgentFleetList:
    return ByocAgentFleetList(
        schema_version="fyralis.byoc.agent_fleet_list.v1",
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        limit=100,
        result_count=len(items),
        stored_scope="sanitized_agent_metadata_only",
        items=tuple(items),
    )


def _evidence(
    *,
    ledger_status: str = "pass",
    required_evidence_passed: bool = True,
) -> ByocEvidencePackageIntakeRecord:
    return ByocEvidencePackageIntakeRecord(
        receipt=ByocEvidencePackageReceipt(
            schema_version="fyralis.byoc.evidence_package_receipt.v1",
            status="accepted",
            receipt_id="evpkg_0123456789abcdef0123456789abcdef",
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
            agent_id="agt_overview01",
            package_digest=f"sha256:{'a' * 64}",
            package_generated_at=datetime(2026, 6, 27, 11, 58, tzinfo=UTC),
            ledger_overall_status=ledger_status,
            required_evidence_passed=required_evidence_passed,
            live_report_envelope_digest=None,
            accepted_at=datetime(2026, 6, 27, 11, 59, tzinfo=UTC),
            stored_scope="sanitized_metadata_only",
        ),
        submitted_at=datetime(2026, 6, 27, 11, 58, tzinfo=UTC),
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-1",
        cloud_provider="aws",
        region="us-east-1",
    )


def _evidence_list(
    *items: ByocEvidencePackageIntakeRecord,
) -> ByocEvidencePackageReceiptList:
    return ByocEvidencePackageReceiptList(
        schema_version="fyralis.byoc.evidence_package_receipt_list.v1",
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        limit=20,
        result_count=len(items),
        stored_scope="sanitized_metadata_only",
        items=tuple(items),
    )


def _overview(
    *,
    agents: ByocAgentFleetList,
    evidence_packages: ByocEvidencePackageReceiptList,
):
    return build_byoc_deployment_overview(
        query=ByocDeploymentOverviewQuery(
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
        ),
        agents=agents,
        evidence_packages=evidence_packages,
        generated_at=GENERATED_AT,
    )


def test_deployment_overview_reports_ready_from_sanitized_metadata() -> None:
    overview = _overview(
        agents=_agents(_agent()),
        evidence_packages=_evidence_list(_evidence()),
    )

    assert overview.schema_version == "fyralis.byoc.deployment_overview.v1"
    assert overview.status == "ready"
    assert overview.next_action == "none"
    assert overview.stored_scope == "sanitized_deployment_metadata_only"
    assert overview.agent_summary.enrolled_count == 1
    assert overview.agent_summary.passing_count == 1
    assert overview.evidence_summary.receipt_count == 1
    assert overview.evidence_summary.latest_required_evidence_passed is True


def test_deployment_overview_requires_evidence_when_agent_requested_it() -> None:
    overview = _overview(
        agents=_agents(_agent()),
        evidence_packages=_evidence_list(),
    )

    assert overview.status == "action_required"
    assert overview.next_action == "submit_evidence_package"
    assert overview.evidence_summary.latest_ledger_status == "not_submitted"


def test_deployment_overview_prioritizes_agent_health_failures() -> None:
    overview = _overview(
        agents=_agents(_agent(validation_status="failing", connected=False)),
        evidence_packages=_evidence_list(_evidence()),
    )

    assert overview.status == "action_required"
    assert overview.next_action == "restore_agent_health"
    assert overview.agent_summary.failing_count == 1
    assert overview.agent_summary.disconnected_count == 1
