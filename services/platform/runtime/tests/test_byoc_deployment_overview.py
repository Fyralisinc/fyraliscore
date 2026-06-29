from __future__ import annotations

from datetime import UTC, datetime

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentFleetItem,
    ByocAgentFleetList,
)
from services.platform.runtime.byoc_control_panel_state import (
    ByocControlPanelStateQuery,
    build_byoc_control_panel_state,
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
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportIntakeRecord,
    ByocPreflightReportReceipt,
    ByocPreflightReportReceiptList,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceIntakeRecord,
    ByocRunnerEvidenceReceipt,
    ByocRunnerEvidenceReceiptList,
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


def _preflight(
    *,
    receipt_id: str = "pfrep_0123456789abcdef0123456789abcdef",
    preflight_status: str = "pass",
    required_sections_passed: bool = True,
    failed_section_count: int = 0,
) -> ByocPreflightReportIntakeRecord:
    return ByocPreflightReportIntakeRecord(
        receipt=ByocPreflightReportReceipt(
            schema_version="fyralis.byoc.preflight_report_receipt.v1",
            status="accepted",
            receipt_id=receipt_id,
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
            agent_id="agt_overview01",
            report_digest=f"sha256:{'b' * 64}",
            preflight_status=preflight_status,
            required_sections_passed=required_sections_passed,
            section_count=7,
            failed_section_count=failed_section_count,
            terraform_validate_executed=True,
            submitted_at=datetime(2026, 6, 27, 11, 56, tzinfo=UTC),
            accepted_at=datetime(2026, 6, 27, 11, 57, tzinfo=UTC),
            stored_scope="sanitized_metadata_only",
        ),
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-1",
        cloud_provider="aws",
        region="us-east-1",
    )


def _preflight_list(
    *items: ByocPreflightReportIntakeRecord,
) -> ByocPreflightReportReceiptList:
    return ByocPreflightReportReceiptList(
        schema_version="fyralis.byoc.preflight_report_receipt_list.v1",
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        limit=20,
        result_count=len(items),
        stored_scope="sanitized_metadata_only",
        items=tuple(items),
    )


def _runner(
    *,
    receipt_id: str = "runev_0123456789abcdef0123456789abcdef",
    runner_status: str = "pass",
    required_checks_passed: bool = True,
) -> ByocRunnerEvidenceIntakeRecord:
    return ByocRunnerEvidenceIntakeRecord(
        receipt=ByocRunnerEvidenceReceipt(
            schema_version="fyralis.byoc.runner_evidence_receipt.v1",
            status="accepted",
            receipt_id=receipt_id,
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
            agent_id="agt_overview01",
            evidence_digest=f"sha256:{'c' * 64}",
            current_artifact_revision="2026.06.27-1",
            desired_revision="2026.06.27-2",
            rollout_action="apply_revision",
            runner_status=runner_status,
            required_checks_passed=required_checks_passed,
            apply_plan_count=1,
            artifact_verification_count=1,
            digest_pinned_artifact_count=7,
            local_digest_checked_count=1,
            submitted_at=datetime(2026, 6, 27, 11, 57, tzinfo=UTC),
            accepted_at=datetime(2026, 6, 27, 11, 58, tzinfo=UTC),
            stored_scope="sanitized_metadata_only",
        ),
        agent_version="2026.06.27",
        cloud_provider="aws",
        region="us-east-1",
        control_plane_mode="mock",
    )


def _runner_list(
    *items: ByocRunnerEvidenceIntakeRecord,
) -> ByocRunnerEvidenceReceiptList:
    return ByocRunnerEvidenceReceiptList(
        schema_version="fyralis.byoc.runner_evidence_receipt_list.v1",
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
    preflight_reports: ByocPreflightReportReceiptList | None = None,
    runner_evidence: ByocRunnerEvidenceReceiptList | None = None,
):
    return build_byoc_deployment_overview(
        query=ByocDeploymentOverviewQuery(
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
        ),
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports or _preflight_list(),
        runner_evidence=runner_evidence or _runner_list(),
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
    assert overview.preflight_summary.latest_preflight_status == "not_submitted"
    assert overview.runner_summary.latest_runner_status == "not_submitted"


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


def test_deployment_overview_surfaces_preflight_failures() -> None:
    overview = _overview(
        agents=_agents(_agent()),
        evidence_packages=_evidence_list(_evidence()),
        preflight_reports=_preflight_list(
            _preflight(
                preflight_status="fail",
                required_sections_passed=False,
                failed_section_count=2,
            )
        ),
        runner_evidence=_runner_list(_runner()),
    )

    assert overview.status == "action_required"
    assert overview.next_action == "review_preflight_failures"
    assert overview.preflight_summary.failed_receipt_count == 1
    assert overview.preflight_summary.latest_failed_section_count == 2


def test_deployment_overview_surfaces_runner_failures() -> None:
    overview = _overview(
        agents=_agents(_agent()),
        evidence_packages=_evidence_list(_evidence()),
        preflight_reports=_preflight_list(_preflight()),
        runner_evidence=_runner_list(
            _runner(runner_status="fail", required_checks_passed=False)
        ),
    )

    assert overview.status == "action_required"
    assert overview.next_action == "review_runner_failures"
    assert overview.runner_summary.failed_receipt_count == 1


def test_control_panel_state_composes_sanitized_deployment_reads() -> None:
    agents = _agents(_agent())
    evidence_packages = _evidence_list(_evidence())
    preflight_reports = _preflight_list(_preflight())
    runner_evidence = _runner_list(_runner())
    overview = _overview(
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
    )

    state = build_byoc_control_panel_state(
        query=ByocControlPanelStateQuery(
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
            recent_limit=10,
        ),
        overview=overview,
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
        generated_at=GENERATED_AT,
    )

    assert state.schema_version == "fyralis.byoc.control_panel_state.v1"
    assert state.stored_scope == "sanitized_control_panel_metadata_only"
    assert state.overview.status == "ready"
    assert state.actions == ()
    assert {section.key: section.status for section in state.sections} == {
        "deployment_overview": "ready",
        "agent_fleet": "ready",
        "evidence_packages": "ready",
        "preflight_reports": "ready",
        "runner_evidence": "ready",
    }
    serialized = state.model_dump_json()
    assert "install_token" not in serialized.lower()
    assert "secret_ref" not in serialized.lower()
    assert "signature" not in serialized.lower()
    assert "payload" not in serialized.lower()
    assert '"preflight_report":' not in serialized
    assert '"checks":' not in serialized


def test_control_panel_state_surfaces_action_codes_without_raw_context() -> None:
    agents = _agents(_agent())
    overview = _overview(
        agents=agents,
        evidence_packages=_evidence_list(),
    )

    state = build_byoc_control_panel_state(
        query=ByocControlPanelStateQuery(
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
        ),
        overview=overview,
        agents=agents,
        evidence_packages=_evidence_list(),
        preflight_reports=_preflight_list(),
        runner_evidence=_runner_list(),
        generated_at=GENERATED_AT,
    )

    assert [action.code for action in state.actions] == ["submit_evidence_package"]
    assert state.actions[0].target_section == "evidence_packages"
    assert state.actions[0].priority == "warning"
    assert {section.key: section.status for section in state.sections}[
        "evidence_packages"
    ] == "action_required"


def test_control_panel_state_flags_desired_revision_drift() -> None:
    first = _agent(agent_id="agt_overview01")
    second = _agent(agent_id="agt_overview02").model_copy(
        update={"desired_revision": "2026.06.27-3"}
    )
    agents = _agents(first, second)
    evidence_packages = _evidence_list(_evidence())
    overview = _overview(
        agents=agents,
        evidence_packages=evidence_packages,
    )

    state = build_byoc_control_panel_state(
        query=ByocControlPanelStateQuery(
            deployment_id=DEPLOYMENT_ID,
            customer_id=CUSTOMER_ID,
        ),
        overview=overview,
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=_preflight_list(),
        runner_evidence=_runner_list(),
        generated_at=GENERATED_AT,
    )

    assert overview.agent_summary.mixed_desired_revisions is True
    assert state.actions[0].code == "review_desired_state_drift"
    assert state.actions[0].source == "agent_fleet"
    assert state.actions[0].priority == "warning"
