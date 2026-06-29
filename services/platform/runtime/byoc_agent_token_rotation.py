"""Plan-only BYOC data-plane agent install-token rotation contract."""
from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic import model_validator

from services.platform.runtime.byoc_contract import (
    load_byoc_manifest,
    render_validation_errors,
    validate_byoc_manifest_contract,
)


RotationStatus = Literal["pass", "fail"]
RotationExecutionMode = Literal["plan_only"]
_PASS: RotationStatus = "pass"
_FAIL: RotationStatus = "fail"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RAW_SECRET_MARKERS = (
    "bearer ",
    "secret_access_key",
    "private_key",
    "-----begin",
)
_AWS_ACCESS_KEY_RE = re.compile(r"(?:AKIA|ASIA)[A-Z0-9]{16}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAgentTokenRotationPrivacyContract(_StrictModel):
    raw_token_material_included: Literal[False] = False
    secret_refs_included: Literal[False] = False
    secret_ref_digests_included: Literal[True] = True
    signatures_included: Literal[False] = False
    request_bodies_included: Literal[False] = False
    command_output_included: Literal[False] = False
    cloud_credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    urls_included: Literal[False] = False
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    logs_included: Literal[False] = False
    pii_included: Literal[False] = False


class ByocAgentTokenRotationCheckSummary(_StrictModel):
    total: int = Field(ge=0)
    required: int = Field(ge=0)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed_required: int = Field(ge=0)

    @model_validator(mode="after")
    def _counts_must_match_total(self) -> "ByocAgentTokenRotationCheckSummary":
        if self.passed + self.failed + self.skipped != self.total:
            raise ValueError("rotation check status counts must sum to total")
        if self.required > self.total:
            raise ValueError("rotation required count must not exceed total")
        if self.failed_required > self.failed:
            raise ValueError("rotation failed_required must not exceed failed")
        return self


class ByocAgentTokenRotationCheck(_StrictModel):
    name: str
    status: RotationStatus
    required: bool
    details: str
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("rotation check name must be a bounded identifier")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("rotation check details must be bounded")
        return value

    @field_validator("metrics")
    @classmethod
    def _metrics_must_be_bounded(
        cls,
        value: dict[str, int | bool | str],
    ) -> dict[str, int | bool | str]:
        normalized: dict[str, int | bool | str] = {}
        for key, metric in value.items():
            key = key.strip()
            if not key or not _SAFE_CODE_RE.match(key):
                raise ValueError("rotation metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120 or "://" in metric:
                    raise ValueError("rotation string metrics must be bounded metadata")
            normalized[key] = metric
        return normalized


class ByocAgentTokenRotationPlanReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.agent_token_rotation_plan.v1"]
    status: RotationStatus
    required_checks_passed: bool
    execution_mode: RotationExecutionMode = "plan_only"
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    agent_id: str
    rotation_intent: Literal["install_token_secret_ref_rotation"]
    rotation_plan_id: str | None = None
    current_secret_ref_digest: str | None = None
    next_secret_ref_digest: str | None = None
    current_and_next_refs_differ: bool
    overlap_seconds: int = Field(ge=0)
    activation_epoch: int = Field(ge=0)
    manual_customer_secret_write_required: Literal[True] = True
    cloud_secret_updates_executed: Literal[False] = False
    control_plane_mutations_executed: Literal[False] = False
    agent_restart_required: Literal[False] = False
    privacy: ByocAgentTokenRotationPrivacyContract
    check_summary: ByocAgentTokenRotationCheckSummary
    checks: tuple[ByocAgentTokenRotationCheck, ...]

    def as_json(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
        "agent_id",
    )
    @classmethod
    def _identity_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 160 or "://" in value:
            raise ValueError("rotation identity fields must be bounded metadata")
        return value

    @field_validator(
        "rotation_plan_id",
        "current_secret_ref_digest",
        "next_secret_ref_digest",
    )
    @classmethod
    def _digest_must_be_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("rotation digests must look like sha256:<64-hex>")
        return value


@dataclass(frozen=True, slots=True)
class ByocAgentTokenRotationInputs:
    manifest_path: Path
    next_install_token_secret_ref: str
    current_install_token_secret_ref: str | None = None
    agent_id: str = "agt_localrunner001"
    overlap_seconds: int = 3600
    activation_epoch: int = 1


def run_byoc_agent_token_rotation_plan(
    inputs: ByocAgentTokenRotationInputs,
) -> ByocAgentTokenRotationPlanReport:
    started = time.monotonic()
    checks: list[ByocAgentTokenRotationCheck] = []
    manifest = None
    try:
        manifest = load_byoc_manifest(inputs.manifest_path)
    except ValidationError as exc:
        checks.append(
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details="BYOC data-plane manifest schema validation failed.",
                metrics={"error_count": len(render_validation_errors(exc))},
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details="BYOC data-plane manifest could not be loaded.",
                metrics={"error_type": type(exc).__name__},
            )
        )

    current_ref = inputs.current_install_token_secret_ref
    if manifest is not None:
        violations = validate_byoc_manifest_contract(manifest)
        checks.append(
            _check(
                "manifest_contract",
                _PASS if not violations else _FAIL,
                required=True,
                details=(
                    "BYOC data-plane manifest contract passed."
                    if not violations
                    else "BYOC data-plane manifest contract failed."
                ),
                metrics={"violation_count": len(violations)},
            )
        )
        current_ref = current_ref or manifest.secrets.bootstrap_token_secret_ref

    current_safe = _secret_ref_safe(current_ref)
    next_safe = _secret_ref_safe(inputs.next_install_token_secret_ref)
    refs_differ = bool(
        current_ref
        and inputs.next_install_token_secret_ref
        and current_ref != inputs.next_install_token_secret_ref
    )
    checks.extend(
        (
            _check(
                "current_secret_ref_present",
                _PASS if current_ref else _FAIL,
                required=True,
                details="Current install-token secret ref is present.",
            ),
            _check(
                "current_secret_ref_safe",
                _PASS if current_safe else _FAIL,
                required=True,
                details="Current install-token secret ref is bounded metadata.",
            ),
            _check(
                "next_secret_ref_present",
                _PASS if inputs.next_install_token_secret_ref else _FAIL,
                required=True,
                details="Next install-token secret ref is present.",
            ),
            _check(
                "next_secret_ref_safe",
                _PASS if next_safe else _FAIL,
                required=True,
                details="Next install-token secret ref is bounded metadata.",
            ),
            _check(
                "secret_refs_differ",
                _PASS if refs_differ else _FAIL,
                required=True,
                details="Current and next install-token secret refs are distinct.",
            ),
            _check(
                "overlap_window",
                _PASS if 300 <= inputs.overlap_seconds <= 604_800 else _FAIL,
                required=True,
                details="Rotation overlap window is within supported bounds.",
                metrics={"overlap_seconds": inputs.overlap_seconds},
            ),
            _check(
                "activation_epoch",
                _PASS if inputs.activation_epoch >= 1 else _FAIL,
                required=True,
                details="Rotation activation epoch is positive.",
                metrics={"activation_epoch": inputs.activation_epoch},
            ),
            _check(
                "plan_only_execution",
                _PASS,
                required=True,
                details="Rotation report does not mutate secrets or control plane.",
            ),
        )
    )
    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    current_digest = _ref_digest(current_ref) if current_safe else None
    next_digest = (
        _ref_digest(inputs.next_install_token_secret_ref) if next_safe else None
    )
    plan_id = _plan_id(
        deployment_id=getattr(manifest, "deployment_id", None),
        customer_id=getattr(manifest, "customer_id", None),
        agent_id=inputs.agent_id,
        current_digest=current_digest,
        next_digest=next_digest,
        activation_epoch=inputs.activation_epoch,
    )
    return ByocAgentTokenRotationPlanReport(
        schema_version="fyralis.byoc.agent_token_rotation_plan.v1",
        status=_PASS if required_checks_passed else _FAIL,
        required_checks_passed=required_checks_passed,
        execution_mode="plan_only",
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=getattr(manifest, "deployment_id", None),
        customer_id=getattr(manifest, "customer_id", None),
        cloud_provider=getattr(manifest, "cloud_provider", None),
        region=getattr(manifest, "region", None),
        artifact_revision=getattr(manifest, "artifact_revision", None),
        agent_id=inputs.agent_id,
        rotation_intent="install_token_secret_ref_rotation",
        rotation_plan_id=plan_id,
        current_secret_ref_digest=current_digest,
        next_secret_ref_digest=next_digest,
        current_and_next_refs_differ=refs_differ,
        overlap_seconds=inputs.overlap_seconds,
        activation_epoch=inputs.activation_epoch,
        manual_customer_secret_write_required=True,
        cloud_secret_updates_executed=False,
        control_plane_mutations_executed=False,
        agent_restart_required=False,
        privacy=ByocAgentTokenRotationPrivacyContract(),
        check_summary=_check_summary(checks),
        checks=tuple(checks),
    )


def render_agent_token_rotation_plan_json(
    report: ByocAgentTokenRotationPlanReport,
) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_agent_token_rotation_plan_yaml(
    report: ByocAgentTokenRotationPlanReport,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _secret_ref_safe(value: str | None) -> bool:
    if value is None:
        return False
    value = value.strip()
    if not value or len(value) > 300:
        return False
    lowered = value.lower()
    if "://" in lowered or any(marker in lowered for marker in _RAW_SECRET_MARKERS):
        return False
    if any(character.isspace() for character in value):
        return False
    return _AWS_ACCESS_KEY_RE.search(value) is None


def _ref_digest(value: str | None) -> str | None:
    if value is None:
        return None
    payload = f"byoc-agent-install-token-ref:{value.strip()}".encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _plan_id(
    *,
    deployment_id: str | None,
    customer_id: str | None,
    agent_id: str,
    current_digest: str | None,
    next_digest: str | None,
    activation_epoch: int,
) -> str | None:
    if current_digest is None or next_digest is None:
        return None
    payload = json.dumps(
        {
            "agent_id": agent_id,
            "activation_epoch": activation_epoch,
            "current_secret_ref_digest": current_digest,
            "customer_id": customer_id,
            "deployment_id": deployment_id,
            "next_secret_ref_digest": next_digest,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _check_summary(
    checks: Sequence[ByocAgentTokenRotationCheck],
) -> ByocAgentTokenRotationCheckSummary:
    statuses = Counter(check.status for check in checks)
    failed_required = sum(
        1 for check in checks if check.required and check.status == _FAIL
    )
    return ByocAgentTokenRotationCheckSummary(
        total=len(checks),
        required=sum(1 for check in checks if check.required),
        passed=statuses[_PASS],
        failed=statuses[_FAIL],
        skipped=0,
        failed_required=failed_required,
    )


def _check(
    name: str,
    status: RotationStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocAgentTokenRotationCheck:
    return ByocAgentTokenRotationCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


__all__ = [
    "ByocAgentTokenRotationCheck",
    "ByocAgentTokenRotationCheckSummary",
    "ByocAgentTokenRotationInputs",
    "ByocAgentTokenRotationPlanReport",
    "ByocAgentTokenRotationPrivacyContract",
    "render_agent_token_rotation_plan_json",
    "render_agent_token_rotation_plan_yaml",
    "run_byoc_agent_token_rotation_plan",
]
