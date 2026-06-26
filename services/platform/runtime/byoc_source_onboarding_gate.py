"""BYOC gate for enabling the first customer data source.

The gate consumes sanitized evidence package or ledger artifacts only. It does
not inspect raw validator reports, cloud account IDs, ARNs, source credentials,
payloads, prompts, logs, or PII.
"""
from __future__ import annotations

import json
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_evidence_ledger import (
    ByocDeploymentEvidenceLedger,
    ByocEvidenceEntry,
    load_byoc_evidence_ledger,
    render_validation_errors as render_ledger_validation_errors,
)
from services.platform.runtime.byoc_evidence_package import (
    ByocEvidencePackage,
    load_byoc_evidence_package,
    render_validation_errors as render_package_validation_errors,
)


GateStatus = Literal["pass", "fail", "skipped"]
GateMode = Literal["evidence_package", "evidence_ledger"]
_PASS: GateStatus = "pass"
_FAIL: GateStatus = "fail"
_SKIPPED: GateStatus = "skipped"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_REQUIRED_EVIDENCE_KINDS = (
    "bootstrap_plan",
    "terraform_plan_validation",
    "bootstrap_runner",
    "post_deploy_validation",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocSourceOnboardingGateCheck(_StrictModel):
    name: str
    status: GateStatus
    required: bool
    details: str
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("gate check name must be bounded")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("gate check details must be bounded")
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
                raise ValueError("gate metric name must be bounded")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120:
                    raise ValueError("gate string metrics must be bounded")
            normalized[key] = metric
        return normalized


class ByocSourceOnboardingGateReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.source_onboarding_gate.v1"]
    status: GateStatus
    source_onboarding_allowed: bool
    required_checks_passed: bool
    gate_mode: GateMode
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    environment: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    require_aws_live_preflight: bool = False
    require_live_post_deploy: bool = False
    require_signed_post_deploy: bool = False
    checks: tuple[ByocSourceOnboardingGateCheck, ...]

    def as_json(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    @field_validator(
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    )
    @classmethod
    def _identity_fields_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 160:
            raise ValueError("gate identity fields must be bounded")
        return value


@dataclass(frozen=True, slots=True)
class ByocSourceOnboardingGateInputs:
    evidence_package_path: Path | None = None
    evidence_ledger_path: Path | None = None
    require_aws_live_preflight: bool = False
    require_live_post_deploy: bool = False
    require_signed_post_deploy: bool = False


def run_byoc_source_onboarding_gate(
    inputs: ByocSourceOnboardingGateInputs,
) -> ByocSourceOnboardingGateReport:
    started = time.monotonic()
    package, ledger, mode, load_checks = _load_evidence(inputs)
    checks: list[ByocSourceOnboardingGateCheck] = list(load_checks)
    if ledger is not None:
        checks.extend(_ledger_checks(ledger))
        checks.append(_aws_live_preflight_check(ledger, inputs))
        checks.append(_post_deploy_liveness_check(ledger, inputs))
    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    identity_source = ledger or package
    return ByocSourceOnboardingGateReport(
        schema_version="fyralis.byoc.source_onboarding_gate.v1",
        status=_PASS if required_checks_passed else _FAIL,
        source_onboarding_allowed=required_checks_passed,
        required_checks_passed=required_checks_passed,
        gate_mode=mode,
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=_identity(identity_source, "deployment_id"),
        customer_id=_identity(identity_source, "customer_id"),
        environment=_identity(identity_source, "environment"),
        cloud_provider=_identity(identity_source, "cloud_provider"),
        region=_identity(identity_source, "region"),
        artifact_revision=_identity(identity_source, "artifact_revision"),
        require_aws_live_preflight=inputs.require_aws_live_preflight,
        require_live_post_deploy=inputs.require_live_post_deploy,
        require_signed_post_deploy=inputs.require_signed_post_deploy,
        checks=tuple(checks),
    )


def render_source_onboarding_gate_json(
    report: ByocSourceOnboardingGateReport,
) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_source_onboarding_gate_yaml(
    report: ByocSourceOnboardingGateReport,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _load_evidence(
    inputs: ByocSourceOnboardingGateInputs,
) -> tuple[
    ByocEvidencePackage | None,
    ByocDeploymentEvidenceLedger | None,
    GateMode,
    tuple[ByocSourceOnboardingGateCheck, ...],
]:
    if inputs.evidence_package_path is not None:
        try:
            package = load_byoc_evidence_package(inputs.evidence_package_path)
        except ValidationError as exc:
            return None, None, "evidence_package", (
                _check(
                    "evidence_package_schema",
                    _FAIL,
                    required=True,
                    details="Evidence package schema validation failed.",
                    metrics={"error_count": len(render_package_validation_errors(exc))},
                ),
            )
        except Exception as exc:  # noqa: BLE001
            return None, None, "evidence_package", (
                _load_error_check("evidence_package_schema", exc),
            )
        return package, package.ledger, "evidence_package", (
            _check(
                "evidence_package_schema",
                _PASS,
                required=True,
                details="Evidence package schema is valid.",
            ),
        )

    if inputs.evidence_ledger_path is None:
        return None, None, "evidence_ledger", (
            _check(
                "evidence_input",
                _FAIL,
                required=True,
                details="Evidence package or evidence ledger path is required.",
            ),
        )
    try:
        ledger = load_byoc_evidence_ledger(inputs.evidence_ledger_path)
    except ValidationError as exc:
        return None, None, "evidence_ledger", (
            _check(
                "evidence_ledger_schema",
                _FAIL,
                required=True,
                details="Evidence ledger schema validation failed.",
                metrics={"error_count": len(render_ledger_validation_errors(exc))},
            ),
        )
    except Exception as exc:  # noqa: BLE001
        return None, None, "evidence_ledger", (
            _load_error_check("evidence_ledger_schema", exc),
        )
    return None, ledger, "evidence_ledger", (
        _check(
            "evidence_ledger_schema",
            _PASS,
            required=True,
            details="Evidence ledger schema is valid.",
        ),
    )


def _ledger_checks(
    ledger: ByocDeploymentEvidenceLedger,
) -> list[ByocSourceOnboardingGateCheck]:
    checks = [
        _check(
            "ledger_required_evidence_passed",
            _PASS if ledger.required_evidence_passed else _FAIL,
            required=True,
            details=(
                "Required BYOC evidence passed."
                if ledger.required_evidence_passed
                else "Required BYOC evidence has failures."
            ),
            metrics={"evidence_count": len(ledger.evidence)},
        )
    ]
    by_kind = {entry.kind: entry for entry in ledger.evidence}
    for kind in _REQUIRED_EVIDENCE_KINDS:
        entry = by_kind.get(kind)
        checks.append(_required_evidence_check(kind, entry))
    return checks


def _required_evidence_check(
    kind: str,
    entry: ByocEvidenceEntry | None,
) -> ByocSourceOnboardingGateCheck:
    if entry is None:
        return _check(
            f"required_evidence.{kind}",
            _FAIL,
            required=True,
            details="Required BYOC evidence is missing.",
        )
    passed = entry.required_checks_passed and entry.status == "pass"
    return _check(
        f"required_evidence.{kind}",
        _PASS if passed else _FAIL,
        required=True,
        details=(
            "Required BYOC evidence passed."
            if passed
            else "Required BYOC evidence failed."
        ),
        metrics={
            "total_checks": entry.check_summary.total,
            "failed_required": entry.check_summary.failed_required,
        },
    )


def _aws_live_preflight_check(
    ledger: ByocDeploymentEvidenceLedger,
    inputs: ByocSourceOnboardingGateInputs,
) -> ByocSourceOnboardingGateCheck:
    entry = _evidence(ledger, "aws_live_preflight")
    if entry is None:
        return _check(
            "aws_live_preflight_evidence",
            _FAIL if inputs.require_aws_live_preflight else _SKIPPED,
            required=inputs.require_aws_live_preflight,
            details=(
                "AWS live preflight evidence is required but missing."
                if inputs.require_aws_live_preflight
                else "AWS live preflight evidence was not required."
            ),
            metrics={"present": False},
        )
    passed = entry.required_checks_passed and entry.status == "pass"
    return _check(
        "aws_live_preflight_evidence",
        _PASS if passed else _FAIL,
        required=inputs.require_aws_live_preflight,
        details=(
            "AWS live preflight evidence passed."
            if passed
            else "AWS live preflight evidence failed."
        ),
        metrics={
            "present": True,
            "total_checks": entry.check_summary.total,
            "failed_required": entry.check_summary.failed_required,
        },
    )


def _post_deploy_liveness_check(
    ledger: ByocDeploymentEvidenceLedger,
    inputs: ByocSourceOnboardingGateInputs,
) -> ByocSourceOnboardingGateCheck:
    entry = _evidence(ledger, "post_deploy_validation")
    if entry is None:
        return _check(
            "post_deploy_live_evidence",
            _FAIL,
            required=True,
            details="Post-deploy validation evidence is missing.",
        )
    live = entry.source.type in {
        "post_deploy_report_file",
        "signed_post_deploy_report_file",
    }
    signed = entry.source.type == "signed_post_deploy_report_file"
    required = inputs.require_live_post_deploy or inputs.require_signed_post_deploy
    passed = (
        (not inputs.require_live_post_deploy or live)
        and (not inputs.require_signed_post_deploy or signed)
        and entry.required_checks_passed
        and entry.status == "pass"
    )
    return _check(
        "post_deploy_live_evidence",
        _PASS if passed else (_FAIL if required else _SKIPPED),
        required=required,
        details=(
            "Post-deploy live evidence satisfied the gate."
            if passed
            else "Post-deploy live evidence was not required."
            if not required
            else "Post-deploy live evidence did not satisfy the gate."
        ),
        metrics={
            "live_evidence_present": live,
            "signed_evidence_present": signed,
            "signature_verified": entry.signature_verified is True,
        },
    )


def _evidence(
    ledger: ByocDeploymentEvidenceLedger,
    kind: str,
) -> ByocEvidenceEntry | None:
    return next((entry for entry in ledger.evidence if entry.kind == kind), None)


def _load_error_check(name: str, exc: Exception) -> ByocSourceOnboardingGateCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="Evidence artifact could not be loaded.",
        metrics={"error_type": type(exc).__name__},
    )


def _check(
    name: str,
    status: GateStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocSourceOnboardingGateCheck:
    return ByocSourceOnboardingGateCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _identity(source: object | None, field: str) -> str | None:
    if source is None:
        return None
    value = getattr(source, field, None)
    return str(value) if value is not None else None
