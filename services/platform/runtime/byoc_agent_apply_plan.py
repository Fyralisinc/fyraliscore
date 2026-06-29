"""Sanitized BYOC agent desired-state apply plan contract."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateResponse,
)
from services.platform.runtime.byoc_contract import ByocDataPlaneManifest


ApplyPlanStoredScope = Literal["sanitized_agent_metadata_only"]
ApplyPlanExecutionMode = Literal["plan_only"]
ApplyPlanStepAction = Literal[
    "verify_artifact",
    "render_runtime",
    "prepare_rollout",
    "prepare_health_validation",
    "prepare_evidence_handoff",
]

_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_PLAN_ID_RE = re.compile(r"^ap_[a-f0-9]{16}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_RAW_MARKERS = (
    "://",
    "bearer ",
    "secret",
    "signature",
    "payload",
    "prompt",
    "embedding",
    " raw_",
    " pii",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAgentApplyPlanStep(_StrictModel):
    name: str
    action: ApplyPlanStepAction
    execution_mode: ApplyPlanExecutionMode = "plan_only"
    mutates_customer_resources: Literal[False] = False
    required: bool = True
    status: Literal["planned"] = "planned"
    detail_code: str

    @field_validator("name", "detail_code")
    @classmethod
    def _bounded_code(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("apply plan step fields must be bounded codes")
        return value


class ByocAgentApplyPlan(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent.apply_plan.v1"]
    plan_id: str
    deployment_id: str
    customer_id: str
    agent_id: str
    current_revision: str
    desired_revision: str
    rollout_action: Literal["apply_revision"]
    config_epoch: int = Field(ge=0)
    execution_mode: ApplyPlanExecutionMode = "plan_only"
    planned_step_count: int = Field(ge=0)
    mutating_step_count: int = Field(default=0, ge=0)
    generated_at: datetime
    steps: tuple[ByocAgentApplyPlanStep, ...]
    stored_scope: ApplyPlanStoredScope = "sanitized_agent_metadata_only"

    @field_validator("plan_id")
    @classmethod
    def _plan_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _PLAN_ID_RE.match(value):
            raise ValueError("plan_id must look like ap_<digest>")
        return value

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("current_revision", "desired_revision")
    @classmethod
    def _bounded_revision(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("revision fields must be bounded identifiers")
        return value


@dataclass(frozen=True, slots=True)
class ByocAgentApplyPlanViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def build_apply_revision_plan(
    manifest: ByocDataPlaneManifest,
    desired_state: ByocAgentDesiredStateResponse,
    *,
    generated_at: datetime | None = None,
) -> ByocAgentApplyPlan:
    steps = (
        ByocAgentApplyPlanStep(
            name="verify_desired_revision_artifact",
            action="verify_artifact",
            detail_code="digest_verification_required",
        ),
        ByocAgentApplyPlanStep(
            name="render_runtime_release_plan",
            action="render_runtime",
            detail_code="metadata_only_render",
        ),
        ByocAgentApplyPlanStep(
            name="prepare_rollout_window",
            action="prepare_rollout",
            detail_code="non_mutating_preview",
        ),
        ByocAgentApplyPlanStep(
            name="prepare_post_deploy_validation",
            action="prepare_health_validation",
            detail_code="local_contract_checks",
        ),
        ByocAgentApplyPlanStep(
            name="prepare_evidence_handoff",
            action="prepare_evidence_handoff",
            detail_code="sanitized_evidence_only",
        ),
    )
    return ByocAgentApplyPlan(
        schema_version="fyralis.byoc.agent.apply_plan.v1",
        plan_id=_apply_plan_id(desired_state),
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        agent_id=desired_state.agent_id,
        current_revision=desired_state.current_revision,
        desired_revision=desired_state.desired_revision,
        rollout_action="apply_revision",
        config_epoch=desired_state.config_epoch,
        execution_mode="plan_only",
        planned_step_count=len(steps),
        mutating_step_count=0,
        generated_at=generated_at or datetime.now(UTC),
        steps=steps,
        stored_scope="sanitized_agent_metadata_only",
    )


def validate_apply_plan_contract(
    plan: ByocAgentApplyPlan,
) -> list[ByocAgentApplyPlanViolation]:
    violations: list[ByocAgentApplyPlanViolation] = []
    if plan.current_revision == plan.desired_revision:
        violations.append(
            _violation(
                "desired_revision",
                "revision_unchanged",
                "apply plan requires a desired revision different from current",
            )
        )
    if plan.execution_mode != "plan_only":
        violations.append(
            _violation(
                "execution_mode",
                "mutating_execution_mode",
                "apply plan must remain non-mutating until daemon apply is built",
            )
        )
    if plan.planned_step_count != len(plan.steps):
        violations.append(
            _violation(
                "planned_step_count",
                "step_count_mismatch",
                "planned step count must match steps",
            )
        )
    if plan.mutating_step_count != 0:
        violations.append(
            _violation(
                "mutating_step_count",
                "mutating_step_declared",
                "local apply plan must not declare mutating steps",
            )
        )
    for index, step in enumerate(plan.steps):
        if step.execution_mode != "plan_only":
            violations.append(
                _violation(
                    f"steps[{index}].execution_mode",
                    "mutating_step_mode",
                    "apply plan steps must be plan-only",
                )
            )
        if step.mutates_customer_resources:
            violations.append(
                _violation(
                    f"steps[{index}].mutates_customer_resources",
                    "mutating_step",
                    "local apply plan steps must not mutate customer resources",
                )
            )
    serialized = json.dumps(plan.model_dump(mode="json"), sort_keys=True).lower()
    for marker in _RAW_MARKERS:
        if marker in serialized:
            violations.append(
                _violation(
                    "plan",
                    "raw_material_marker",
                    "apply plan must not contain raw URLs, payloads, prompts, or secrets",
                )
            )
            break
    return violations


def _apply_plan_id(desired_state: ByocAgentDesiredStateResponse) -> str:
    material = (
        f"{desired_state.deployment_id}:"
        f"{desired_state.customer_id}:"
        f"{desired_state.agent_id}:"
        f"{desired_state.current_revision}:"
        f"{desired_state.desired_revision}:"
        f"{desired_state.config_epoch}"
    )
    return "ap_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocAgentApplyPlanViolation:
    return ByocAgentApplyPlanViolation(path=path, code=code, message=message)


__all__ = [
    "ByocAgentApplyPlan",
    "ByocAgentApplyPlanStep",
    "ByocAgentApplyPlanViolation",
    "build_apply_revision_plan",
    "validate_apply_plan_contract",
]
