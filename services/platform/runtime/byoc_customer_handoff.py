"""Sanitized BYOC customer handoff readiness report.

The handoff report composes the existing offline preflight, evidence package,
and first-source onboarding gates into one customer-side go/no-go artifact. It
never embeds child reports, command output, artifact references, URLs,
credentials, cloud account IDs, ARNs, raw payloads, prompts, logs, or PII.
"""
from __future__ import annotations

import json
import re
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic import model_validator

from services.platform.runtime.byoc_aws_iac_package import load_byoc_aws_iac_package
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_package import (
    load_byoc_evidence_package,
    package_source_digests,
    render_validation_errors as render_package_validation_errors,
    validate_evidence_package_contract,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest
from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    ByocPreflightBundleReport,
    run_byoc_preflight_bundle,
)
from services.platform.runtime.byoc_source_onboarding_gate import (
    ByocSourceOnboardingGateInputs,
    ByocSourceOnboardingGateReport,
    run_byoc_source_onboarding_gate,
)


HandoffStatus = Literal["pass", "fail", "skipped"]
HandoffExecutionMode = Literal["customer_side_local"]
_PASS: HandoffStatus = "pass"
_FAIL: HandoffStatus = "fail"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocCustomerHandoffPrivacyContract(_StrictModel):
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    urls_included: Literal[False] = False
    artifact_refs_included: Literal[False] = False
    command_output_included: Literal[False] = False
    child_report_details_included: Literal[False] = False
    evidence_package_body_included: Literal[False] = False
    terraform_plan_json_included: Literal[False] = False


class ByocCustomerHandoffCheckSummary(_StrictModel):
    total: int = Field(ge=0)
    required: int = Field(ge=0)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed_required: int = Field(ge=0)

    @field_validator("total", "required", "passed", "failed", "skipped")
    @classmethod
    def _counts_must_be_bounded(cls, value: int) -> int:
        if value > 10_000:
            raise ValueError("handoff check counts must be bounded")
        return value

    @model_validator(mode="after")
    def _counts_must_match_total(self) -> "ByocCustomerHandoffCheckSummary":
        if self.passed + self.failed + self.skipped != self.total:
            raise ValueError("handoff check status counts must sum to total")
        if self.required > self.total:
            raise ValueError("handoff required count must not exceed total")
        if self.failed_required > self.failed:
            raise ValueError("handoff failed_required must not exceed failed")
        return self


class ByocCustomerHandoffSection(_StrictModel):
    name: str
    status: HandoffStatus
    required: bool = True
    required_checks_passed: bool
    check_summary: ByocCustomerHandoffCheckSummary
    failed_check_codes: tuple[str, ...] = ()
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff section name must be a bounded identifier")
        return value

    @field_validator("failed_check_codes")
    @classmethod
    def _codes_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(code.strip() for code in value)
        if len(normalized) > 200:
            raise ValueError("failed handoff codes must be bounded")
        if any(not code or not _SAFE_CODE_RE.match(code) for code in normalized):
            raise ValueError("failed handoff codes must be bounded identifiers")
        return normalized

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
                raise ValueError("handoff metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120 or "://" in metric:
                    raise ValueError("handoff string metrics must be bounded metadata")
            normalized[key] = metric
        return normalized


class ByocCustomerHandoffReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.customer_handoff_readiness.v1"]
    status: HandoffStatus
    customer_handoff_ready: bool
    source_onboarding_allowed: bool
    required_sections_passed: bool
    execution_mode: HandoffExecutionMode = "customer_side_local"
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    environment: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    cloud_credentials_required: bool = False
    live_aws_api_calls_executed: bool = False
    terraform_init_executed: bool = False
    terraform_validate_executed: bool = False
    terraform_plan_executed: Literal[False] = False
    mutating_cloud_commands_executed: Literal[False] = False
    privacy: ByocCustomerHandoffPrivacyContract
    sections: tuple[ByocCustomerHandoffSection, ...]

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
    def _optional_strings_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 160 or "://" in value:
            raise ValueError("handoff identity fields must be bounded metadata")
        return value


@dataclass(frozen=True, slots=True)
class ByocCustomerHandoffInputs:
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path
    iac_package_path: Path
    bootstrap_bundle_path: Path
    bootstrap_plan_path: Path
    evidence_package_path: Path
    evidence_ledger_path: Path
    env_path: Path | None = None
    repo_root: Path = field(default_factory=Path.cwd)
    verify_local_bundle_files: bool = True
    run_terraform_init: bool = False
    terraform_init_timeout_seconds: int = 60
    run_terraform_validate: bool = False
    terraform_bin: str = "terraform"
    terraform_validate_timeout_seconds: int = 30
    run_aws_live_preflight: bool = False
    skip_aws_live_preflight_aws: bool = False
    run_aws_readonly_api_probes: bool = False
    run_aws_iam_policy_simulation: bool = False
    aws_simulation_principal_arn: str | None = None
    aws_simulation_role_name: str = "bootstrap_provisioner"
    aws_profile: str | None = None
    aws_region: str | None = None
    expected_aws_account_id: str | None = None
    require_aws_live_preflight: bool = False
    require_live_post_deploy: bool = False
    require_signed_post_deploy: bool = False


def run_byoc_customer_handoff(
    inputs: ByocCustomerHandoffInputs,
) -> ByocCustomerHandoffReport:
    started = time.monotonic()
    preflight = run_byoc_preflight_bundle(
        ByocPreflightBundleInputs(
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            iam_template_path=inputs.iam_template_path,
            iac_package_path=inputs.iac_package_path,
            bootstrap_bundle_path=inputs.bootstrap_bundle_path,
            bootstrap_plan_path=inputs.bootstrap_plan_path,
            env_path=inputs.env_path,
            repo_root=inputs.repo_root,
            verify_local_bundle_files=inputs.verify_local_bundle_files,
            run_terraform_init=inputs.run_terraform_init,
            terraform_init_timeout_seconds=inputs.terraform_init_timeout_seconds,
            run_terraform_validate=inputs.run_terraform_validate,
            terraform_bin=inputs.terraform_bin,
            terraform_validate_timeout_seconds=(
                inputs.terraform_validate_timeout_seconds
            ),
            run_aws_live_preflight=inputs.run_aws_live_preflight,
            skip_aws_live_preflight_aws=inputs.skip_aws_live_preflight_aws,
            run_aws_readonly_api_probes=inputs.run_aws_readonly_api_probes,
            run_aws_iam_policy_simulation=inputs.run_aws_iam_policy_simulation,
            aws_simulation_principal_arn=inputs.aws_simulation_principal_arn,
            aws_simulation_role_name=inputs.aws_simulation_role_name,
            aws_profile=inputs.aws_profile,
            aws_region=inputs.aws_region,
            expected_aws_account_id=inputs.expected_aws_account_id,
        )
    )
    source_gate = run_byoc_source_onboarding_gate(
        ByocSourceOnboardingGateInputs(
            evidence_package_path=inputs.evidence_package_path,
            require_aws_live_preflight=inputs.require_aws_live_preflight,
            require_live_post_deploy=inputs.require_live_post_deploy,
            require_signed_post_deploy=inputs.require_signed_post_deploy,
        )
    )
    sections = (
        _preflight_section(preflight),
        _evidence_package_section(inputs),
        _source_onboarding_section(source_gate),
    )
    required_sections_passed = all(
        section.status != _FAIL for section in sections if section.required
    )
    identity = _identity_from_reports(preflight, source_gate)
    return ByocCustomerHandoffReport(
        schema_version="fyralis.byoc.customer_handoff_readiness.v1",
        status=_PASS if required_sections_passed else _FAIL,
        customer_handoff_ready=required_sections_passed,
        source_onboarding_allowed=source_gate.source_onboarding_allowed,
        required_sections_passed=required_sections_passed,
        execution_mode="customer_side_local",
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        environment=identity.get("environment"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        cloud_credentials_required=preflight.cloud_credentials_required,
        live_aws_api_calls_executed=preflight.aws_live_preflight_executed,
        terraform_init_executed=preflight.terraform_init_executed,
        terraform_validate_executed=preflight.terraform_validate_executed,
        terraform_plan_executed=False,
        mutating_cloud_commands_executed=False,
        privacy=ByocCustomerHandoffPrivacyContract(),
        sections=sections,
    )


def render_customer_handoff_json(report: ByocCustomerHandoffReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_customer_handoff_yaml(report: ByocCustomerHandoffReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _preflight_section(
    report: ByocPreflightBundleReport,
) -> ByocCustomerHandoffSection:
    return ByocCustomerHandoffSection(
        name="preflight_bundle",
        status=_PASS if report.required_sections_passed else _FAIL,
        required=True,
        required_checks_passed=report.required_sections_passed,
        check_summary=_summary_from_sections(report.sections),
        failed_check_codes=_failed_section_codes(report.sections),
        metrics={
            "child_section_count": len(report.sections),
            "aws_live_preflight_requested": report.aws_live_preflight_requested,
            "live_aws_api_calls_executed": report.aws_live_preflight_executed,
            "cloud_credentials_required": report.cloud_credentials_required,
            "terraform_init_executed": report.terraform_init_executed,
            "terraform_validate_executed": report.terraform_validate_executed,
        },
    )


def _evidence_package_section(
    inputs: ByocCustomerHandoffInputs,
) -> ByocCustomerHandoffSection:
    try:
        package = load_byoc_evidence_package(inputs.evidence_package_path)
    except ValidationError as exc:
        return _failed_section(
            "evidence_package",
            ("evidence_package.schema_validation_failed",),
            metrics={"error_count": len(render_package_validation_errors(exc))},
        )
    except Exception as exc:  # noqa: BLE001
        return _failed_section(
            "evidence_package",
            ("evidence_package.input_load_failed",),
            metrics={"error_type": type(exc).__name__},
        )

    dependencies = _load_evidence_dependencies(inputs)
    if dependencies.failures:
        return _failed_section(
            "evidence_package",
            tuple(f"dependency.{failure}" for failure in dependencies.failures),
            metrics={
                "dependency_failure_count": len(dependencies.failures),
                "source_artifact_count": len(package.source_artifacts),
            },
        )

    try:
        digests = package_source_digests(
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            aws_iac_package_path=inputs.iac_package_path,
            bootstrap_bundle_path=inputs.bootstrap_bundle_path,
            plan_path=inputs.bootstrap_plan_path,
            ledger_path=inputs.evidence_ledger_path,
            repo_root=inputs.repo_root,
        )
    except Exception as exc:  # noqa: BLE001
        return _failed_section(
            "evidence_package",
            ("evidence_package.source_digest_failed",),
            metrics={
                "error_type": type(exc).__name__,
                "source_artifact_count": len(package.source_artifacts),
            },
        )

    violations = validate_evidence_package_contract(
        package,
        dataplane_manifest=dependencies.dataplane_manifest,
        permissions_manifest=dependencies.permissions_manifest,
        aws_iac_package=dependencies.aws_iac_package,
        bootstrap_bundle=dependencies.bootstrap_bundle,
        plan=dependencies.bootstrap_plan,
        source_digests=digests,
    )
    if violations:
        return _failed_section(
            "evidence_package",
            tuple(f"contract.{violation.code}" for violation in violations),
            metrics={
                "contract_violation_count": len(violations),
                "source_artifact_count": len(package.source_artifacts),
                "ledger_evidence_count": len(package.ledger.evidence),
            },
        )

    return ByocCustomerHandoffSection(
        name="evidence_package",
        status=_PASS,
        required=True,
        required_checks_passed=True,
        check_summary=ByocCustomerHandoffCheckSummary(
            total=2,
            required=2,
            passed=2,
            failed=0,
            skipped=0,
            failed_required=0,
        ),
        metrics={
            "source_artifact_count": len(package.source_artifacts),
            "ledger_evidence_count": len(package.ledger.evidence),
            "live_report_envelope_present": package.live_report_envelope is not None,
            "contract_violation_count": 0,
        },
    )


def _source_onboarding_section(
    report: ByocSourceOnboardingGateReport,
) -> ByocCustomerHandoffSection:
    return ByocCustomerHandoffSection(
        name="source_onboarding_gate",
        status=_PASS if report.source_onboarding_allowed else _FAIL,
        required=True,
        required_checks_passed=report.required_checks_passed,
        check_summary=_summary_from_checks(report.checks),
        failed_check_codes=_failed_check_codes(report.checks),
        metrics={
            "gate_mode": report.gate_mode,
            "source_onboarding_allowed": report.source_onboarding_allowed,
            "require_aws_live_preflight": report.require_aws_live_preflight,
            "require_live_post_deploy": report.require_live_post_deploy,
            "require_signed_post_deploy": report.require_signed_post_deploy,
        },
    )


@dataclass(frozen=True, slots=True)
class _EvidenceDependencies:
    dataplane_manifest: Any | None = None
    permissions_manifest: Any | None = None
    aws_iac_package: Any | None = None
    bootstrap_bundle: Any | None = None
    bootstrap_plan: Any | None = None
    failures: tuple[str, ...] = ()


def _load_evidence_dependencies(
    inputs: ByocCustomerHandoffInputs,
) -> _EvidenceDependencies:
    failures: list[str] = []
    dataplane_manifest = _load_dependency(
        "dataplane_manifest",
        load_byoc_manifest,
        inputs.dataplane_manifest_path,
        failures,
    )
    permissions_manifest = _load_dependency(
        "permissions_manifest",
        load_byoc_permissions_manifest,
        inputs.permissions_manifest_path,
        failures,
    )
    aws_iac_package = _load_dependency(
        "aws_iac_package",
        load_byoc_aws_iac_package,
        inputs.iac_package_path,
        failures,
    )
    bootstrap_bundle = _load_dependency(
        "bootstrap_bundle",
        load_byoc_bootstrap_bundle,
        inputs.bootstrap_bundle_path,
        failures,
    )
    bootstrap_plan = _load_dependency(
        "bootstrap_plan",
        load_byoc_bootstrap_plan,
        inputs.bootstrap_plan_path,
        failures,
    )
    return _EvidenceDependencies(
        dataplane_manifest=dataplane_manifest,
        permissions_manifest=permissions_manifest,
        aws_iac_package=aws_iac_package,
        bootstrap_bundle=bootstrap_bundle,
        bootstrap_plan=bootstrap_plan,
        failures=tuple(failures),
    )


def _load_dependency(
    name: str,
    loader: Any,
    path: Path,
    failures: list[str],
) -> Any | None:
    try:
        return loader(path)
    except Exception:  # noqa: BLE001
        failures.append(name)
        return None


def _failed_section(
    name: str,
    failed_codes: Sequence[str],
    *,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocCustomerHandoffSection:
    normalized_failed = _unique_codes(failed_codes)
    failed_count = max(1, len(normalized_failed))
    return ByocCustomerHandoffSection(
        name=name,
        status=_FAIL,
        required=True,
        required_checks_passed=False,
        check_summary=ByocCustomerHandoffCheckSummary(
            total=failed_count,
            required=failed_count,
            passed=0,
            failed=failed_count,
            skipped=0,
            failed_required=failed_count,
        ),
        failed_check_codes=normalized_failed,
        metrics=metrics or {},
    )


def _summary_from_sections(sections: Sequence[Any]) -> ByocCustomerHandoffCheckSummary:
    statuses = Counter(str(section.status) for section in sections)
    failed_required = sum(
        1
        for section in sections
        if str(section.status) == _FAIL and bool(getattr(section, "required", False))
    )
    return ByocCustomerHandoffCheckSummary(
        total=len(sections),
        required=sum(
            1
            for section in sections
            if bool(getattr(section, "required", False))
        ),
        passed=statuses[_PASS],
        failed=statuses[_FAIL],
        skipped=statuses["skipped"],
        failed_required=failed_required,
    )


def _summary_from_checks(checks: Sequence[Any]) -> ByocCustomerHandoffCheckSummary:
    statuses = Counter(str(check.status) for check in checks)
    failed_required = sum(
        1
        for check in checks
        if str(check.status) == _FAIL and bool(getattr(check, "required", False))
    )
    return ByocCustomerHandoffCheckSummary(
        total=len(checks),
        required=sum(1 for check in checks if bool(getattr(check, "required", False))),
        passed=statuses[_PASS],
        failed=statuses[_FAIL],
        skipped=statuses["skipped"],
        failed_required=failed_required,
    )


def _failed_section_codes(sections: Sequence[Any]) -> tuple[str, ...]:
    codes: list[str] = []
    for section in sections:
        if str(section.status) != _FAIL:
            continue
        section_name = str(section.name)
        failed_codes = tuple(getattr(section, "failed_check_codes", ()) or ())
        if not failed_codes:
            codes.append(section_name)
            continue
        codes.extend(f"{section_name}.{code}" for code in failed_codes)
    return _unique_codes(codes)


def _failed_check_codes(checks: Sequence[Any]) -> tuple[str, ...]:
    return _unique_codes(
        str(check.name)
        for check in checks
        if str(check.status) == _FAIL
    )


def _unique_codes(codes: Sequence[str] | Any) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for code in codes:
        code = str(code).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(sorted(normalized))


def _identity_from_reports(
    preflight: ByocPreflightBundleReport,
    source_gate: ByocSourceOnboardingGateReport,
) -> dict[str, str | None]:
    identity: dict[str, str | None] = {}
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        value = getattr(preflight, field, None) or getattr(source_gate, field, None)
        identity[field] = str(value) if value is not None else None
    return identity


__all__ = [
    "ByocCustomerHandoffCheckSummary",
    "ByocCustomerHandoffInputs",
    "ByocCustomerHandoffPrivacyContract",
    "ByocCustomerHandoffReport",
    "ByocCustomerHandoffSection",
    "render_customer_handoff_json",
    "render_customer_handoff_yaml",
    "run_byoc_customer_handoff",
]
