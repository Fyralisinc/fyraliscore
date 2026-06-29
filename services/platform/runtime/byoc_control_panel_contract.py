"""Exportable BYOC control-panel state schema and sanitized example."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentFleetItem,
    ByocAgentFleetList,
)
from services.platform.runtime.byoc_control_panel_state import (
    ByocControlPanelState,
    ByocControlPanelStateQuery,
    build_byoc_control_panel_state,
)
from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageIntakeRecord,
    ByocEvidencePackageReceipt,
    ByocEvidencePackageReceiptList,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverview,
    ByocDeploymentOverviewQuery,
    build_byoc_deployment_overview,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportIntakeRecord,
    ByocPreflightReportReceipt,
    ByocPreflightReportReceiptList,
)
from services.platform.runtime.byoc_product_health import (
    ByocProductHealth,
    ByocProductHealthIssue,
    ByocProductModelHealth,
    ByocProductPipelineHealth,
    ByocProductSourceHealth,
    ByocProductThinkHealth,
    ByocProductVectorHealth,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceIntakeRecord,
    ByocRunnerEvidenceReceipt,
    ByocRunnerEvidenceReceiptList,
)


EXAMPLE_DEPLOYMENT_ID = "dep_control01"
EXAMPLE_CUSTOMER_ID = "cus_control01"
EXAMPLE_AGENT_ID = "agt_control01"
EXAMPLE_GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
SCHEMA_BUNDLE_VERSION = "fyralis.byoc.control_panel_contract_bundle.v1"


def model_json_schema_bundle() -> dict[str, Any]:
    """Return the schema bundle a control-panel consumer can code against."""

    return {
        "schema_version": SCHEMA_BUNDLE_VERSION,
        "query": ByocControlPanelStateQuery.model_json_schema(),
        "control_panel_state": ByocControlPanelState.model_json_schema(),
        "deployment_overview": ByocDeploymentOverview.model_json_schema(),
        "product_health": ByocProductHealth.model_json_schema(),
        "stored_scope": "sanitized_control_panel_metadata_only",
    }


def build_example_control_panel_state(
    *,
    generated_at: datetime = EXAMPLE_GENERATED_AT,
) -> ByocControlPanelState:
    """Build a deterministic metadata-only example for UI and API consumers."""

    agents = _agent_fleet(generated_at=generated_at)
    evidence_packages = _evidence_package_receipts(generated_at=generated_at)
    preflight_reports = _preflight_report_receipts(generated_at=generated_at)
    runner_evidence = _runner_evidence_receipts(generated_at=generated_at)
    product_health = _product_health(generated_at=generated_at)
    overview = build_byoc_deployment_overview(
        query=ByocDeploymentOverviewQuery(
            deployment_id=EXAMPLE_DEPLOYMENT_ID,
            customer_id=EXAMPLE_CUSTOMER_ID,
        ),
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
        generated_at=generated_at,
    )
    return build_byoc_control_panel_state(
        query=ByocControlPanelStateQuery(
            deployment_id=EXAMPLE_DEPLOYMENT_ID,
            customer_id=EXAMPLE_CUSTOMER_ID,
            recent_limit=10,
        ),
        overview=overview,
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
        product_health=product_health,
        generated_at=generated_at,
    )


def render_control_panel_schema_bundle_json() -> str:
    return json.dumps(model_json_schema_bundle(), indent=2, sort_keys=True) + "\n"


def render_control_panel_state_example_json(
    state: ByocControlPanelState | None = None,
) -> str:
    example = state or build_example_control_panel_state()
    return json.dumps(example.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def _agent_fleet(*, generated_at: datetime) -> ByocAgentFleetList:
    return ByocAgentFleetList(
        schema_version="fyralis.byoc.agent_fleet_list.v1",
        deployment_id=EXAMPLE_DEPLOYMENT_ID,
        customer_id=EXAMPLE_CUSTOMER_ID,
        limit=100,
        result_count=1,
        stored_scope="sanitized_agent_metadata_only",
        items=(
            ByocAgentFleetItem(
                schema_version="fyralis.byoc.agent_fleet_item.v1",
                deployment_id=EXAMPLE_DEPLOYMENT_ID,
                customer_id=EXAMPLE_CUSTOMER_ID,
                agent_id=EXAMPLE_AGENT_ID,
                agent_version="2026.06.27",
                artifact_revision="2026.06.27-1",
                cloud_provider="aws",
                region="us-east-1",
                desired_revision="2026.06.27-2",
                desired_config_epoch=2,
                evidence_package_required=True,
                heartbeat_interval_seconds=30,
                telemetry_contract="fyralis.byoc.agent.telemetry.v1",
                enrolled_at=generated_at.replace(hour=11, minute=45),
                desired_state_updated_at=generated_at.replace(hour=11, minute=55),
                latest_heartbeat_sequence=3,
                latest_validation_status="passing",
                latest_control_plane_connected=True,
                latest_telemetry_mode="aggregate-only",
                latest_component_count=3,
                latest_ok_component_count=3,
                latest_degraded_component_count=0,
                latest_failed_component_count=0,
                latest_unknown_component_count=0,
                latest_queued_batches=0,
                latest_dropped_batches=0,
                latest_heartbeat_sent_at=generated_at.replace(hour=11, minute=59),
                latest_heartbeat_accepted_at=generated_at.replace(
                    hour=11,
                    minute=59,
                ),
                stored_scope="sanitized_agent_metadata_only",
            ),
        ),
    )


def _product_health(*, generated_at: datetime) -> ByocProductHealth:
    return ByocProductHealth(
        schema_version="fyralis.byoc.product_health.v1",
        deployment_id=EXAMPLE_DEPLOYMENT_ID,
        customer_id=EXAMPLE_CUSTOMER_ID,
        generated_at=generated_at,
        observed=True,
        latest_snapshot_id="phs_0123456789abcdef0123456789abcdef",
        latest_collected_at=generated_at.replace(hour=11, minute=59),
        overall_status="ready",
        sources=(
            ByocProductSourceHealth(
                source="slack",
                status="ready",
                auth_status="ready",
                backfill_status="idle",
                items_ingested_count=1280,
                items_failed_count=0,
                queue_depth_count=0,
                lag_seconds=12,
                last_success_at=generated_at.replace(hour=11, minute=58),
            ),
            ByocProductSourceHealth(
                source="github",
                status="ready",
                auth_status="ready",
                backfill_status="idle",
                items_ingested_count=342,
                items_failed_count=1,
                queue_depth_count=2,
                lag_seconds=45,
                last_success_at=generated_at.replace(hour=11, minute=57),
            ),
        ),
        pipeline=ByocProductPipelineHealth(
            status="ready",
            queue_lag_count=2,
            dead_letter_count=0,
            retry_backlog_count=1,
            dropped_item_count=0,
        ),
        think=ByocProductThinkHealth(
            status="ready",
            run_count=84,
            failed_run_count=1,
            queued_run_count=0,
            latest_run_at=generated_at.replace(hour=11, minute=54),
            breaker_status="closed",
        ),
        models=ByocProductModelHealth(
            status="ready",
            model_count=37,
            model_build_count=9,
            failed_build_count=0,
            model_relation_count=112,
            orphan_model_count=1,
            stale_relation_count=0,
            latest_build_at=generated_at.replace(hour=11, minute=52),
            graph_status="ready",
        ),
        vector_index=ByocProductVectorHealth(
            status="ready",
            vector_count=7812,
            backlog_count=3,
            failed_job_count=0,
            latest_job_at=generated_at.replace(hour=11, minute=55),
            retrieval_status="ready",
        ),
        issues=(
            ByocProductHealthIssue(
                code="github_minor_ingest_retry",
                severity="info",
                component="source_ingestion",
                observed_count=1,
                first_observed_at=generated_at.replace(hour=11, minute=47),
                latest_observed_at=generated_at.replace(hour=11, minute=49),
            ),
        ),
        stored_scope="sanitized_product_health_metadata_only",
    )


def _evidence_package_receipts(
    *,
    generated_at: datetime,
) -> ByocEvidencePackageReceiptList:
    return ByocEvidencePackageReceiptList(
        schema_version="fyralis.byoc.evidence_package_receipt_list.v1",
        deployment_id=EXAMPLE_DEPLOYMENT_ID,
        customer_id=EXAMPLE_CUSTOMER_ID,
        limit=10,
        result_count=1,
        stored_scope="sanitized_metadata_only",
        items=(
            ByocEvidencePackageIntakeRecord(
                receipt=ByocEvidencePackageReceipt(
                    schema_version="fyralis.byoc.evidence_package_receipt.v1",
                    status="accepted",
                    receipt_id="evpkg_0123456789abcdef0123456789abcdef",
                    deployment_id=EXAMPLE_DEPLOYMENT_ID,
                    customer_id=EXAMPLE_CUSTOMER_ID,
                    agent_id=EXAMPLE_AGENT_ID,
                    package_digest=f"sha256:{'a' * 64}",
                    package_generated_at=generated_at.replace(hour=11, minute=58),
                    ledger_overall_status="pass",
                    required_evidence_passed=True,
                    live_report_envelope_digest=None,
                    accepted_at=generated_at.replace(hour=11, minute=59),
                    stored_scope="sanitized_metadata_only",
                ),
                submitted_at=generated_at.replace(hour=11, minute=58),
                agent_version="2026.06.27",
                artifact_revision="2026.06.27-1",
                cloud_provider="aws",
                region="us-east-1",
            ),
        ),
    )


def _preflight_report_receipts(
    *,
    generated_at: datetime,
) -> ByocPreflightReportReceiptList:
    return ByocPreflightReportReceiptList(
        schema_version="fyralis.byoc.preflight_report_receipt_list.v1",
        deployment_id=EXAMPLE_DEPLOYMENT_ID,
        customer_id=EXAMPLE_CUSTOMER_ID,
        limit=10,
        result_count=1,
        stored_scope="sanitized_metadata_only",
        items=(
            ByocPreflightReportIntakeRecord(
                receipt=ByocPreflightReportReceipt(
                    schema_version="fyralis.byoc.preflight_report_receipt.v1",
                    status="accepted",
                    receipt_id="pfrep_0123456789abcdef0123456789abcdef",
                    deployment_id=EXAMPLE_DEPLOYMENT_ID,
                    customer_id=EXAMPLE_CUSTOMER_ID,
                    agent_id=EXAMPLE_AGENT_ID,
                    report_digest=f"sha256:{'b' * 64}",
                    preflight_status="pass",
                    required_sections_passed=True,
                    section_count=7,
                    failed_section_count=0,
                    terraform_validate_executed=True,
                    submitted_at=generated_at.replace(hour=11, minute=56),
                    accepted_at=generated_at.replace(hour=11, minute=57),
                    stored_scope="sanitized_metadata_only",
                ),
                agent_version="2026.06.27",
                artifact_revision="2026.06.27-1",
                cloud_provider="aws",
                region="us-east-1",
            ),
        ),
    )


def _runner_evidence_receipts(
    *,
    generated_at: datetime,
) -> ByocRunnerEvidenceReceiptList:
    return ByocRunnerEvidenceReceiptList(
        schema_version="fyralis.byoc.runner_evidence_receipt_list.v1",
        deployment_id=EXAMPLE_DEPLOYMENT_ID,
        customer_id=EXAMPLE_CUSTOMER_ID,
        limit=10,
        result_count=1,
        stored_scope="sanitized_metadata_only",
        items=(
            ByocRunnerEvidenceIntakeRecord(
                receipt=ByocRunnerEvidenceReceipt(
                    schema_version="fyralis.byoc.runner_evidence_receipt.v1",
                    status="accepted",
                    receipt_id="runev_0123456789abcdef0123456789abcdef",
                    deployment_id=EXAMPLE_DEPLOYMENT_ID,
                    customer_id=EXAMPLE_CUSTOMER_ID,
                    agent_id=EXAMPLE_AGENT_ID,
                    evidence_digest=f"sha256:{'c' * 64}",
                    current_artifact_revision="2026.06.27-1",
                    desired_revision="2026.06.27-2",
                    rollout_action="apply_revision",
                    runner_status="pass",
                    required_checks_passed=True,
                    apply_plan_count=1,
                    artifact_verification_count=1,
                    digest_pinned_artifact_count=7,
                    local_digest_checked_count=1,
                    submitted_at=generated_at.replace(hour=11, minute=57),
                    accepted_at=generated_at.replace(hour=11, minute=58),
                    stored_scope="sanitized_metadata_only",
                ),
                agent_version="2026.06.27",
                cloud_provider="aws",
                region="us-east-1",
                control_plane_mode="mock",
            ),
        ),
    )


__all__ = [
    "SCHEMA_BUNDLE_VERSION",
    "build_example_control_panel_state",
    "model_json_schema_bundle",
    "render_control_panel_schema_bundle_json",
    "render_control_panel_state_example_json",
]
