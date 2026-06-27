"""Metadata-only BYOC control-panel deployment state contract."""
from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_agent_control_plane import ByocAgentFleetList
from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageReceiptList,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverview,
    ByocDeploymentOverviewQuery,
    DeploymentOverviewNextAction,
    DeploymentOverviewStatus,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportReceiptList,
)
from services.platform.runtime.byoc_product_health import (
    ByocProductHealth,
    ByocProductHealthQuery,
    unknown_product_health,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceReceiptList,
)


_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")

ControlPanelStoredScope = Literal["sanitized_control_panel_metadata_only"]
ControlPanelSectionKey = Literal[
    "deployment_overview",
    "agent_fleet",
    "product_health",
    "evidence_packages",
    "preflight_reports",
    "runner_evidence",
]
ControlPanelSectionStatus = Literal[
    "ready",
    "action_required",
    "degraded",
    "unknown",
    "empty",
]
ControlPanelActionCode = Literal[
    "enroll_agent",
    "restore_agent_health",
    "submit_evidence_package",
    "review_evidence_failures",
    "review_preflight_failures",
    "review_runner_failures",
    "review_desired_state_drift",
    "review_product_health",
]
ControlPanelActionPriority = Literal["critical", "warning", "info"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocControlPanelStateQuery(ByocDeploymentOverviewQuery):
    recent_limit: int = Field(default=10, ge=1, le=20)


class ByocControlPanelSection(_StrictModel):
    key: ControlPanelSectionKey
    status: ControlPanelSectionStatus
    item_count: int = Field(ge=0)
    latest_observed_at: datetime | None = None
    source_schema_version: str


class ByocControlPanelAction(_StrictModel):
    code: ControlPanelActionCode
    priority: ControlPanelActionPriority
    source: ControlPanelSectionKey
    target_section: ControlPanelSectionKey
    deployment_id: str
    customer_id: str | None = None
    status: Literal["open"] = "open"

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value


class ByocControlPanelState(_StrictModel):
    schema_version: Literal["fyralis.byoc.control_panel_state.v1"]
    deployment_id: str
    customer_id: str | None = None
    generated_at: datetime
    overview: ByocDeploymentOverview
    sections: tuple[ByocControlPanelSection, ...]
    actions: tuple[ByocControlPanelAction, ...]
    agent_fleet: ByocAgentFleetList
    product_health: ByocProductHealth
    evidence_packages: ByocEvidencePackageReceiptList
    preflight_reports: ByocPreflightReportReceiptList
    runner_evidence: ByocRunnerEvidenceReceiptList
    stored_scope: ControlPanelStoredScope = "sanitized_control_panel_metadata_only"

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value


def build_byoc_control_panel_state(
    *,
    query: ByocControlPanelStateQuery,
    overview: ByocDeploymentOverview,
    agents: ByocAgentFleetList,
    evidence_packages: ByocEvidencePackageReceiptList,
    preflight_reports: ByocPreflightReportReceiptList,
    runner_evidence: ByocRunnerEvidenceReceiptList,
    product_health: ByocProductHealth | None = None,
    generated_at: datetime | None = None,
) -> ByocControlPanelState:
    """Build the backend contract a control panel can render from.

    Inputs must already be sanitized BYOC read models. This function does not
    consume raw reports, raw heartbeat payloads, signed headers, endpoint URLs,
    logs, prompts, or customer data.
    """
    generated = generated_at or datetime.now(tz=UTC)
    customer_id = query.customer_id or overview.customer_id
    product_health_state = product_health or unknown_product_health(
        query=ByocProductHealthQuery(
            deployment_id=query.deployment_id,
            customer_id=customer_id,
        ),
        generated_at=generated,
    )
    sections = _sections(
        overview=overview,
        agents=agents,
        product_health=product_health_state,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
    )
    actions = _actions(
        overview=overview,
        agents=agents,
        product_health=product_health_state,
        deployment_id=query.deployment_id,
        customer_id=customer_id,
    )
    return ByocControlPanelState(
        schema_version="fyralis.byoc.control_panel_state.v1",
        deployment_id=query.deployment_id,
        customer_id=customer_id,
        generated_at=generated,
        overview=overview,
        sections=sections,
        actions=actions,
        agent_fleet=agents,
        product_health=product_health_state,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
        stored_scope="sanitized_control_panel_metadata_only",
    )


def _sections(
    *,
    overview: ByocDeploymentOverview,
    agents: ByocAgentFleetList,
    product_health: ByocProductHealth,
    evidence_packages: ByocEvidencePackageReceiptList,
    preflight_reports: ByocPreflightReportReceiptList,
    runner_evidence: ByocRunnerEvidenceReceiptList,
) -> tuple[ByocControlPanelSection, ...]:
    return (
        ByocControlPanelSection(
            key="deployment_overview",
            status=_overview_section_status(overview.status),
            item_count=1,
            latest_observed_at=overview.generated_at,
            source_schema_version=overview.schema_version,
        ),
        ByocControlPanelSection(
            key="agent_fleet",
            status=_agent_section_status(overview),
            item_count=agents.result_count,
            latest_observed_at=overview.agent_summary.latest_heartbeat_accepted_at,
            source_schema_version=agents.schema_version,
        ),
        ByocControlPanelSection(
            key="product_health",
            status=_product_health_section_status(product_health),
            item_count=_product_health_item_count(product_health),
            latest_observed_at=product_health.latest_collected_at,
            source_schema_version=product_health.schema_version,
        ),
        ByocControlPanelSection(
            key="evidence_packages",
            status=_evidence_section_status(overview),
            item_count=evidence_packages.result_count,
            latest_observed_at=overview.evidence_summary.latest_package_accepted_at,
            source_schema_version=evidence_packages.schema_version,
        ),
        ByocControlPanelSection(
            key="preflight_reports",
            status=_preflight_section_status(overview),
            item_count=preflight_reports.result_count,
            latest_observed_at=overview.preflight_summary.latest_report_accepted_at,
            source_schema_version=preflight_reports.schema_version,
        ),
        ByocControlPanelSection(
            key="runner_evidence",
            status=_runner_section_status(overview),
            item_count=runner_evidence.result_count,
            latest_observed_at=overview.runner_summary.latest_evidence_accepted_at,
            source_schema_version=runner_evidence.schema_version,
        ),
    )


def _overview_section_status(
    status: DeploymentOverviewStatus,
) -> ControlPanelSectionStatus:
    if status in ("ready", "degraded", "unknown", "action_required"):
        return status
    return "unknown"


def _agent_section_status(
    overview: ByocDeploymentOverview,
) -> ControlPanelSectionStatus:
    summary = overview.agent_summary
    if summary.enrolled_count == 0:
        return "empty"
    if summary.failing_count > 0 or summary.disconnected_count > 0:
        return "action_required"
    if summary.degraded_count > 0:
        return "degraded"
    if summary.unknown_count > 0 or summary.heartbeat_observed_count < summary.enrolled_count:
        return "unknown"
    return "ready"


def _product_health_section_status(
    product_health: ByocProductHealth,
) -> ControlPanelSectionStatus:
    if not product_health.observed:
        return "unknown"
    if product_health.overall_status == "action_required":
        return "action_required"
    if product_health.overall_status == "degraded":
        return "degraded"
    if product_health.overall_status == "ready":
        return "ready"
    return "unknown"


def _product_health_item_count(product_health: ByocProductHealth) -> int:
    return (
        len(product_health.sources)
        + len(product_health.issues)
        + (1 if product_health.observed else 0)
    )


def _evidence_section_status(
    overview: ByocDeploymentOverview,
) -> ControlPanelSectionStatus:
    summary = overview.evidence_summary
    if summary.failed_receipt_count > 0:
        return "action_required"
    if (
        overview.agent_summary.evidence_package_required_count > 0
        and summary.latest_required_evidence_passed is not True
    ):
        return "action_required"
    if summary.receipt_count == 0:
        return "empty"
    return "ready"


def _preflight_section_status(
    overview: ByocDeploymentOverview,
) -> ControlPanelSectionStatus:
    summary = overview.preflight_summary
    if summary.failed_receipt_count > 0:
        return "action_required"
    if summary.receipt_count == 0:
        return "empty"
    return "ready"


def _runner_section_status(
    overview: ByocDeploymentOverview,
) -> ControlPanelSectionStatus:
    summary = overview.runner_summary
    if summary.failed_receipt_count > 0:
        return "action_required"
    if summary.receipt_count == 0:
        return "empty"
    return "ready"


def _actions(
    *,
    overview: ByocDeploymentOverview,
    agents: ByocAgentFleetList,
    product_health: ByocProductHealth,
    deployment_id: str,
    customer_id: str | None,
) -> tuple[ByocControlPanelAction, ...]:
    actions: list[ByocControlPanelAction] = []
    if overview.next_action != "none":
        actions.append(
            ByocControlPanelAction(
                code=overview.next_action,
                priority=_priority_for_next_action(overview),
                source="deployment_overview",
                target_section=_target_section_for_next_action(overview.next_action),
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        )
    if not product_health.observed or product_health.overall_status in (
        "action_required",
        "degraded",
    ):
        actions.append(
            ByocControlPanelAction(
                code="review_product_health",
                priority=_priority_for_product_health(product_health),
                source="product_health",
                target_section="product_health",
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        )
    if agents.result_count > 1 and overview.agent_summary.mixed_desired_revisions:
        actions.append(
            ByocControlPanelAction(
                code="review_desired_state_drift",
                priority="warning",
                source="agent_fleet",
                target_section="agent_fleet",
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        )
    return tuple(actions)


def _priority_for_product_health(
    product_health: ByocProductHealth,
) -> ControlPanelActionPriority:
    if product_health.overall_status == "action_required":
        return "critical"
    return "warning"


def _priority_for_next_action(
    overview: ByocDeploymentOverview,
) -> ControlPanelActionPriority:
    if overview.next_action in ("enroll_agent", "restore_agent_health"):
        if overview.status == "degraded":
            return "warning"
        return "critical"
    if overview.next_action in (
        "review_evidence_failures",
        "review_preflight_failures",
        "review_runner_failures",
    ):
        return "critical"
    return "warning"


def _target_section_for_next_action(
    next_action: DeploymentOverviewNextAction,
) -> ControlPanelSectionKey:
    if next_action in ("enroll_agent", "restore_agent_health"):
        return "agent_fleet"
    if next_action in ("submit_evidence_package", "review_evidence_failures"):
        return "evidence_packages"
    if next_action == "review_preflight_failures":
        return "preflight_reports"
    if next_action == "review_runner_failures":
        return "runner_evidence"
    return "deployment_overview"


__all__ = [
    "ByocControlPanelAction",
    "ByocControlPanelSection",
    "ByocControlPanelState",
    "ByocControlPanelStateQuery",
    "ControlPanelActionCode",
    "ControlPanelActionPriority",
    "ControlPanelSectionKey",
    "ControlPanelSectionStatus",
    "ControlPanelStoredScope",
    "build_byoc_control_panel_state",
]
