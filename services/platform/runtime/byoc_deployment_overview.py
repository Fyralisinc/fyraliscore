"""Metadata-only BYOC deployment overview read model."""
from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentFleetItem,
    ByocAgentFleetList,
)
from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageIntakeRecord,
    ByocEvidencePackageReceiptList,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportIntakeRecord,
    ByocPreflightReportReceiptList,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceIntakeRecord,
    ByocRunnerEvidenceReceiptList,
)


_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")

OverviewStoredScope = Literal["sanitized_deployment_metadata_only"]
DeploymentOverviewStatus = Literal["ready", "action_required", "degraded", "unknown"]
DeploymentOverviewNextAction = Literal[
    "none",
    "enroll_agent",
    "restore_agent_health",
    "submit_evidence_package",
    "review_evidence_failures",
    "review_preflight_failures",
    "review_runner_failures",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocDeploymentOverviewQuery(_StrictModel):
    deployment_id: str
    customer_id: str | None = None

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


class ByocDeploymentAgentSummary(_StrictModel):
    enrolled_count: int = Field(ge=0)
    passing_count: int = Field(ge=0)
    degraded_count: int = Field(ge=0)
    failing_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    connected_count: int = Field(ge=0)
    disconnected_count: int = Field(ge=0)
    heartbeat_observed_count: int = Field(ge=0)
    evidence_package_required_count: int = Field(ge=0)
    highest_desired_config_epoch: int = Field(ge=0)
    current_desired_revision: str | None = None
    mixed_desired_revisions: bool
    latest_heartbeat_accepted_at: datetime | None = None
    latest_desired_state_updated_at: datetime | None = None

    @field_validator("current_desired_revision")
    @classmethod
    def _revision_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("desired revision must be a bounded identifier")
        return value


class ByocDeploymentEvidenceSummary(_StrictModel):
    receipt_count: int = Field(ge=0)
    passed_receipt_count: int = Field(ge=0)
    failed_receipt_count: int = Field(ge=0)
    skipped_receipt_count: int = Field(ge=0)
    latest_receipt_id: str | None = None
    latest_ledger_status: Literal["pass", "fail", "skipped", "not_submitted"]
    latest_required_evidence_passed: bool | None = None
    latest_package_accepted_at: datetime | None = None


class ByocDeploymentPreflightSummary(_StrictModel):
    receipt_count: int = Field(ge=0)
    passed_receipt_count: int = Field(ge=0)
    failed_receipt_count: int = Field(ge=0)
    skipped_receipt_count: int = Field(ge=0)
    latest_receipt_id: str | None = None
    latest_preflight_status: Literal["pass", "fail", "skipped", "not_submitted"]
    latest_required_sections_passed: bool | None = None
    latest_failed_section_count: int | None = Field(default=None, ge=0)
    latest_report_accepted_at: datetime | None = None


class ByocDeploymentRunnerSummary(_StrictModel):
    receipt_count: int = Field(ge=0)
    passed_receipt_count: int = Field(ge=0)
    failed_receipt_count: int = Field(ge=0)
    latest_receipt_id: str | None = None
    latest_runner_status: Literal["pass", "fail", "not_submitted"]
    latest_required_checks_passed: bool | None = None
    latest_rollout_action: Literal["none", "apply_revision"] | None = None
    latest_apply_plan_count: int | None = Field(default=None, ge=0)
    latest_artifact_verification_count: int | None = Field(default=None, ge=0)
    latest_digest_pinned_artifact_count: int | None = Field(default=None, ge=0)
    latest_local_digest_checked_count: int | None = Field(default=None, ge=0)
    latest_evidence_accepted_at: datetime | None = None


class ByocDeploymentOverview(_StrictModel):
    schema_version: Literal["fyralis.byoc.deployment_overview.v1"]
    deployment_id: str
    customer_id: str | None = None
    generated_at: datetime
    status: DeploymentOverviewStatus
    next_action: DeploymentOverviewNextAction
    metadata_sources: tuple[
        Literal[
            "agent_fleet",
            "evidence_package_receipts",
            "preflight_report_receipts",
            "runner_evidence_receipts",
        ],
        ...,
    ]
    agent_summary: ByocDeploymentAgentSummary
    evidence_summary: ByocDeploymentEvidenceSummary
    preflight_summary: ByocDeploymentPreflightSummary
    runner_summary: ByocDeploymentRunnerSummary
    stored_scope: OverviewStoredScope = "sanitized_deployment_metadata_only"

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


def build_byoc_deployment_overview(
    *,
    query: ByocDeploymentOverviewQuery,
    agents: ByocAgentFleetList,
    evidence_packages: ByocEvidencePackageReceiptList,
    preflight_reports: ByocPreflightReportReceiptList,
    runner_evidence: ByocRunnerEvidenceReceiptList,
    generated_at: datetime | None = None,
) -> ByocDeploymentOverview:
    """Build a control-panel-ready summary from sanitized BYOC metadata only."""
    agent_summary = _summarize_agents(agents.items)
    evidence_summary = _summarize_evidence(evidence_packages.items)
    preflight_summary = _summarize_preflight(preflight_reports.items)
    runner_summary = _summarize_runner(runner_evidence.items)
    status, next_action = _status_from_summaries(
        agent_summary=agent_summary,
        evidence_summary=evidence_summary,
        preflight_summary=preflight_summary,
        runner_summary=runner_summary,
    )
    return ByocDeploymentOverview(
        schema_version="fyralis.byoc.deployment_overview.v1",
        deployment_id=query.deployment_id,
        customer_id=query.customer_id
        or _first_customer_id(
            agents,
            evidence_packages,
            preflight_reports,
            runner_evidence,
        ),
        generated_at=generated_at or datetime.now(tz=UTC),
        status=status,
        next_action=next_action,
        metadata_sources=(
            "agent_fleet",
            "evidence_package_receipts",
            "preflight_report_receipts",
            "runner_evidence_receipts",
        ),
        agent_summary=agent_summary,
        evidence_summary=evidence_summary,
        preflight_summary=preflight_summary,
        runner_summary=runner_summary,
        stored_scope="sanitized_deployment_metadata_only",
    )


def _summarize_agents(
    items: tuple[ByocAgentFleetItem, ...],
) -> ByocDeploymentAgentSummary:
    desired_revisions = {item.desired_revision for item in items}
    latest_heartbeat = max(
        (
            item.latest_heartbeat_accepted_at
            for item in items
            if item.latest_heartbeat_accepted_at is not None
        ),
        default=None,
    )
    latest_desired_state_update = max(
        (
            item.desired_state_updated_at
            for item in items
            if item.desired_state_updated_at is not None
        ),
        default=None,
    )
    return ByocDeploymentAgentSummary(
        enrolled_count=len(items),
        passing_count=sum(
            1 for item in items if item.latest_validation_status == "passing"
        ),
        degraded_count=sum(
            1 for item in items if item.latest_validation_status == "degraded"
        ),
        failing_count=sum(
            1 for item in items if item.latest_validation_status == "failing"
        ),
        unknown_count=sum(
            1
            for item in items
            if item.latest_validation_status in (None, "unknown")
        ),
        connected_count=sum(
            1 for item in items if item.latest_control_plane_connected is True
        ),
        disconnected_count=sum(
            1 for item in items if item.latest_control_plane_connected is False
        ),
        heartbeat_observed_count=sum(
            1 for item in items if item.latest_heartbeat_accepted_at is not None
        ),
        evidence_package_required_count=sum(
            1 for item in items if item.evidence_package_required
        ),
        highest_desired_config_epoch=max(
            (item.desired_config_epoch for item in items),
            default=0,
        ),
        current_desired_revision=next(iter(desired_revisions))
        if len(desired_revisions) == 1
        else None,
        mixed_desired_revisions=len(desired_revisions) > 1,
        latest_heartbeat_accepted_at=latest_heartbeat,
        latest_desired_state_updated_at=latest_desired_state_update,
    )


def _summarize_evidence(
    items: tuple[ByocEvidencePackageIntakeRecord, ...],
) -> ByocDeploymentEvidenceSummary:
    latest = items[0] if items else None
    return ByocDeploymentEvidenceSummary(
        receipt_count=len(items),
        passed_receipt_count=sum(
            1 for item in items if item.receipt.ledger_overall_status == "pass"
        ),
        failed_receipt_count=sum(
            1 for item in items if item.receipt.ledger_overall_status == "fail"
        ),
        skipped_receipt_count=sum(
            1 for item in items if item.receipt.ledger_overall_status == "skipped"
        ),
        latest_receipt_id=latest.receipt.receipt_id if latest is not None else None,
        latest_ledger_status=latest.receipt.ledger_overall_status
        if latest is not None
        else "not_submitted",
        latest_required_evidence_passed=latest.receipt.required_evidence_passed
        if latest is not None
        else None,
        latest_package_accepted_at=latest.receipt.accepted_at
        if latest is not None
        else None,
    )


def _summarize_preflight(
    items: tuple[ByocPreflightReportIntakeRecord, ...],
) -> ByocDeploymentPreflightSummary:
    latest = items[0] if items else None
    return ByocDeploymentPreflightSummary(
        receipt_count=len(items),
        passed_receipt_count=sum(
            1 for item in items if item.receipt.preflight_status == "pass"
        ),
        failed_receipt_count=sum(
            1 for item in items if item.receipt.preflight_status == "fail"
        ),
        skipped_receipt_count=sum(
            1 for item in items if item.receipt.preflight_status == "skipped"
        ),
        latest_receipt_id=latest.receipt.receipt_id if latest is not None else None,
        latest_preflight_status=latest.receipt.preflight_status
        if latest is not None
        else "not_submitted",
        latest_required_sections_passed=latest.receipt.required_sections_passed
        if latest is not None
        else None,
        latest_failed_section_count=latest.receipt.failed_section_count
        if latest is not None
        else None,
        latest_report_accepted_at=latest.receipt.accepted_at
        if latest is not None
        else None,
    )


def _summarize_runner(
    items: tuple[ByocRunnerEvidenceIntakeRecord, ...],
) -> ByocDeploymentRunnerSummary:
    latest = items[0] if items else None
    return ByocDeploymentRunnerSummary(
        receipt_count=len(items),
        passed_receipt_count=sum(
            1 for item in items if item.receipt.runner_status == "pass"
        ),
        failed_receipt_count=sum(
            1 for item in items if item.receipt.runner_status == "fail"
        ),
        latest_receipt_id=latest.receipt.receipt_id if latest is not None else None,
        latest_runner_status=latest.receipt.runner_status
        if latest is not None
        else "not_submitted",
        latest_required_checks_passed=latest.receipt.required_checks_passed
        if latest is not None
        else None,
        latest_rollout_action=latest.receipt.rollout_action
        if latest is not None
        else None,
        latest_apply_plan_count=latest.receipt.apply_plan_count
        if latest is not None
        else None,
        latest_artifact_verification_count=latest.receipt.artifact_verification_count
        if latest is not None
        else None,
        latest_digest_pinned_artifact_count=latest.receipt.digest_pinned_artifact_count
        if latest is not None
        else None,
        latest_local_digest_checked_count=latest.receipt.local_digest_checked_count
        if latest is not None
        else None,
        latest_evidence_accepted_at=latest.receipt.accepted_at
        if latest is not None
        else None,
    )


def _status_from_summaries(
    *,
    agent_summary: ByocDeploymentAgentSummary,
    evidence_summary: ByocDeploymentEvidenceSummary,
    preflight_summary: ByocDeploymentPreflightSummary,
    runner_summary: ByocDeploymentRunnerSummary,
) -> tuple[DeploymentOverviewStatus, DeploymentOverviewNextAction]:
    if agent_summary.enrolled_count == 0:
        return "action_required", "enroll_agent"
    if agent_summary.failing_count > 0 or agent_summary.disconnected_count > 0:
        return "action_required", "restore_agent_health"
    if preflight_summary.failed_receipt_count > 0:
        return "action_required", "review_preflight_failures"
    if runner_summary.failed_receipt_count > 0:
        return "action_required", "review_runner_failures"
    if evidence_summary.failed_receipt_count > 0:
        return "action_required", "review_evidence_failures"
    if (
        agent_summary.evidence_package_required_count > 0
        and evidence_summary.latest_required_evidence_passed is not True
    ):
        return "action_required", "submit_evidence_package"
    if agent_summary.degraded_count > 0:
        return "degraded", "restore_agent_health"
    if (
        agent_summary.unknown_count > 0
        or agent_summary.heartbeat_observed_count < agent_summary.enrolled_count
    ):
        return "unknown", "restore_agent_health"
    return "ready", "none"


def _first_customer_id(
    agents: ByocAgentFleetList,
    evidence_packages: ByocEvidencePackageReceiptList,
    preflight_reports: ByocPreflightReportReceiptList,
    runner_evidence: ByocRunnerEvidenceReceiptList,
) -> str | None:
    for item in agents.items:
        return item.customer_id
    for item in evidence_packages.items:
        return item.receipt.customer_id
    for item in preflight_reports.items:
        return item.receipt.customer_id
    for item in runner_evidence.items:
        return item.receipt.customer_id
    return None


__all__ = [
    "ByocDeploymentAgentSummary",
    "ByocDeploymentEvidenceSummary",
    "ByocDeploymentOverview",
    "ByocDeploymentOverviewQuery",
    "ByocDeploymentPreflightSummary",
    "ByocDeploymentRunnerSummary",
    "DeploymentOverviewNextAction",
    "DeploymentOverviewStatus",
    "OverviewStoredScope",
    "build_byoc_deployment_overview",
]
