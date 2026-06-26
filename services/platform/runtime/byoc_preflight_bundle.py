"""Sanitized BYOC customer-side preflight bundle report.

The preflight bundle is an orchestration layer over existing local BYOC
contracts. It does not shell out to the child CLIs and it does not include raw
child report details, command output, artifact refs, URLs, credentials, or
customer data. Each section is reduced to bounded status metadata, aggregate
check counts, and failed check codes.
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

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    run_byoc_aws_live_preflight,
)
from services.platform.runtime.byoc_aws_iac_package import (
    load_byoc_aws_iac_package,
    render_validation_errors as render_iac_validation_errors,
    validate_aws_iac_package_contract,
)
from services.platform.runtime.byoc_bootstrap_bundle import (
    load_byoc_bootstrap_bundle,
    render_validation_errors as render_bundle_validation_errors,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_bootstrap_runner import (
    ByocBootstrapRunnerInputs,
    run_byoc_bootstrap_runner,
)
from services.platform.runtime.byoc_contract import (
    load_byoc_manifest,
    render_validation_errors as render_dataplane_validation_errors,
    validate_byoc_manifest_contract,
)
from services.platform.runtime.byoc_permissions import (
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
    render_validation_errors as render_permissions_validation_errors,
    validate_aws_iam_template_contract,
    validate_permissions_manifest_contract,
)
from services.platform.runtime.byoc_terraform_plan_validation import (
    ByocTerraformPlanValidationInputs,
    run_byoc_terraform_plan_validation,
)
from services.platform.runtime.byoc_validation import (
    ByocValidationInputs,
    run_byoc_post_deploy_validation,
)


PreflightStatus = Literal["pass", "fail", "skipped"]
PreflightExecutionMode = Literal["customer_side_local"]
_PASS: PreflightStatus = "pass"
_FAIL: PreflightStatus = "fail"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocPreflightPrivacyContract(_StrictModel):
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    credentials_included: Literal[False] = False
    artifact_refs_included: Literal[False] = False
    command_output_included: Literal[False] = False
    child_report_details_included: Literal[False] = False
    terraform_plan_json_included: Literal[False] = False


class ByocPreflightCheckSummary(_StrictModel):
    total: int = Field(ge=0)
    required: int = Field(ge=0)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed_required: int = Field(ge=0)

    @field_validator("total", "required", "passed", "failed", "skipped")
    @classmethod
    def _counts_must_be_reasonable(cls, value: int) -> int:
        if value > 10_000:
            raise ValueError("preflight check counts must be bounded")
        return value

    @model_validator(mode="after")
    def _counts_must_match_total(self) -> "ByocPreflightCheckSummary":
        if self.passed + self.failed + self.skipped != self.total:
            raise ValueError("preflight check status counts must sum to total")
        if self.required > self.total:
            raise ValueError("preflight required count must not exceed total")
        if self.failed_required > self.failed:
            raise ValueError("preflight failed_required must not exceed failed")
        return self


class ByocPreflightSection(_StrictModel):
    name: str
    status: PreflightStatus
    required: bool = True
    required_checks_passed: bool
    check_summary: ByocPreflightCheckSummary
    failed_check_codes: tuple[str, ...] = ()
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("preflight section name must be a bounded identifier")
        return value

    @field_validator("failed_check_codes")
    @classmethod
    def _codes_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(code.strip() for code in value)
        if any(not code or not _SAFE_CODE_RE.match(code) for code in normalized):
            raise ValueError("failed preflight codes must be bounded identifiers")
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
                raise ValueError("preflight metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120:
                    raise ValueError("preflight string metrics must be bounded")
            normalized[key] = metric
        return normalized


class ByocPreflightBundleReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.preflight_bundle.v1"]
    status: PreflightStatus
    required_sections_passed: bool
    execution_mode: PreflightExecutionMode = "customer_side_local"
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    environment: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    terraform_validate_executed: bool = False
    aws_live_preflight_requested: bool = False
    aws_live_preflight_executed: bool = False
    terraform_plan_executed: Literal[False] = False
    cloud_credentials_required: bool = False
    mutating_cloud_commands_executed: Literal[False] = False
    privacy: ByocPreflightPrivacyContract
    sections: tuple[ByocPreflightSection, ...]

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
        if not value or len(value) > 160:
            raise ValueError("preflight identity fields must be bounded")
        return value


@dataclass(frozen=True, slots=True)
class ByocPreflightBundleInputs:
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path
    iac_package_path: Path
    bootstrap_bundle_path: Path
    bootstrap_plan_path: Path
    env_path: Path | None = None
    repo_root: Path = field(default_factory=Path.cwd)
    verify_local_bundle_files: bool = True
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


def run_byoc_preflight_bundle(
    inputs: ByocPreflightBundleInputs,
) -> ByocPreflightBundleReport:
    started = time.monotonic()
    repo_root = inputs.repo_root.resolve()
    sections_list = [
        _dataplane_section(inputs),
        _permissions_section(inputs, repo_root=repo_root),
        _aws_iac_package_section(inputs, repo_root=repo_root),
        _terraform_validation_section(inputs, repo_root=repo_root),
        _bootstrap_bundle_section(inputs, repo_root=repo_root),
        _bootstrap_runner_section(inputs, repo_root=repo_root),
        _post_deploy_validation_section(inputs),
    ]
    if inputs.run_aws_live_preflight:
        sections_list.append(_aws_live_preflight_section(inputs))
    sections = tuple(sections_list)
    required_sections_passed = all(
        section.status != _FAIL for section in sections if section.required
    )
    identity = _identity_from_inputs(inputs)
    terraform_section = next(
        section for section in sections if section.name == "terraform_validation"
    )
    aws_live_section = next(
        (section for section in sections if section.name == "aws_live_preflight"),
        None,
    )
    return ByocPreflightBundleReport(
        schema_version="fyralis.byoc.preflight_bundle.v1",
        status=_PASS if required_sections_passed else _FAIL,
        required_sections_passed=required_sections_passed,
        execution_mode="customer_side_local",
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        environment=identity.get("environment"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        terraform_validate_executed=bool(
            terraform_section.metrics.get("terraform_validate_executed", False)
        ),
        aws_live_preflight_requested=inputs.run_aws_live_preflight,
        aws_live_preflight_executed=bool(
            aws_live_section is not None
            and aws_live_section.metrics.get("live_aws_api_calls_executed", False)
        ),
        terraform_plan_executed=False,
        cloud_credentials_required=bool(
            aws_live_section is not None
            and aws_live_section.metrics.get("cloud_credentials_required", False)
        ),
        mutating_cloud_commands_executed=False,
        privacy=ByocPreflightPrivacyContract(),
        sections=sections,
    )


def render_preflight_report_json(report: ByocPreflightBundleReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_preflight_report_yaml(report: ByocPreflightBundleReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _dataplane_section(
    inputs: ByocPreflightBundleInputs,
) -> ByocPreflightSection:
    try:
        manifest = load_byoc_manifest(inputs.dataplane_manifest_path)
    except ValidationError as exc:
        return _load_error_section(
            "dataplane_manifest",
            "schema_validation_failed",
            len(render_dataplane_validation_errors(exc)),
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "dataplane_manifest",
            "input_load_failed",
            1,
            error_type=type(exc).__name__,
        )
    violations = validate_byoc_manifest_contract(manifest)
    return _contract_section(
        "dataplane_manifest",
        failed_codes=[violation.code for violation in violations],
        metrics={
            "endpoint_count": len(manifest.network.endpoint_exposure),
            "private_service_count": len(manifest.network.private_service_endpoints),
        },
    )


def _permissions_section(
    inputs: ByocPreflightBundleInputs,
    *,
    repo_root: Path,
) -> ByocPreflightSection:
    del repo_root
    dependencies = _load_contract_dependencies(inputs)
    if dependencies.failures:
        return _dependency_error_section("permissions_manifest", dependencies.failures)
    assert dependencies.dataplane is not None
    assert dependencies.permissions is not None
    assert dependencies.iam_template is not None
    violations = [
        *validate_permissions_manifest_contract(
            dependencies.permissions,
            dataplane_manifest=dependencies.dataplane,
        ),
        *validate_aws_iam_template_contract(
            dependencies.iam_template,
            permissions_manifest=dependencies.permissions,
        ),
    ]
    return _contract_section(
        "permissions_manifest",
        failed_codes=[violation.code for violation in violations],
        metrics={"role_count": len(dependencies.permissions.roles)},
    )


def _aws_iac_package_section(
    inputs: ByocPreflightBundleInputs,
    *,
    repo_root: Path,
) -> ByocPreflightSection:
    dependencies = _load_contract_dependencies(inputs)
    if dependencies.failures:
        return _dependency_error_section("aws_iac_package", dependencies.failures)
    assert dependencies.dataplane is not None
    assert dependencies.permissions is not None
    assert dependencies.iam_template is not None
    try:
        package = load_byoc_aws_iac_package(inputs.iac_package_path)
    except ValidationError as exc:
        return _load_error_section(
            "aws_iac_package",
            "schema_validation_failed",
            len(render_iac_validation_errors(exc)),
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "aws_iac_package",
            "input_load_failed",
            1,
            error_type=type(exc).__name__,
        )
    violations = validate_aws_iac_package_contract(
        package,
        dataplane_manifest=dependencies.dataplane,
        permissions_manifest=dependencies.permissions,
        iam_template=dependencies.iam_template,
        repo_root=repo_root,
    )
    return _contract_section(
        "aws_iac_package",
        failed_codes=[violation.code for violation in violations],
        metrics={
            "component_count": len(package.components),
            "terraform_module_count": len(package.terraform.modules),
        },
    )


def _terraform_validation_section(
    inputs: ByocPreflightBundleInputs,
    *,
    repo_root: Path,
) -> ByocPreflightSection:
    try:
        report = run_byoc_terraform_plan_validation(
            ByocTerraformPlanValidationInputs(
                iac_package_path=inputs.iac_package_path,
                dataplane_manifest_path=inputs.dataplane_manifest_path,
                permissions_manifest_path=inputs.permissions_manifest_path,
                iam_template_path=inputs.iam_template_path,
                repo_root=repo_root,
                run_terraform_validate=inputs.run_terraform_validate,
                terraform_bin=inputs.terraform_bin,
                terraform_validate_timeout_seconds=(
                    inputs.terraform_validate_timeout_seconds
                ),
            )
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "terraform_validation",
            "child_report_failed",
            1,
            error_type=type(exc).__name__,
        )
    return _checks_section(
        "terraform_validation",
        report.checks,
        required_checks_passed=report.required_checks_passed,
        metrics={
            "module_count": report.module_count,
            "terraform_file_count": report.terraform_file_count,
            "terraform_validate_executed": report.terraform_validate_executed,
            "terraform_plan_executed": report.terraform_plan_executed,
        },
    )


def _bootstrap_bundle_section(
    inputs: ByocPreflightBundleInputs,
    *,
    repo_root: Path,
) -> ByocPreflightSection:
    dependencies = _load_contract_dependencies(inputs)
    if dependencies.failures:
        return _dependency_error_section("bootstrap_bundle", dependencies.failures)
    assert dependencies.dataplane is not None
    assert dependencies.permissions is not None
    try:
        bundle = load_byoc_bootstrap_bundle(inputs.bootstrap_bundle_path)
    except ValidationError as exc:
        return _load_error_section(
            "bootstrap_bundle",
            "schema_validation_failed",
            len(render_bundle_validation_errors(exc)),
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "bootstrap_bundle",
            "input_load_failed",
            1,
            error_type=type(exc).__name__,
        )
    violations = validate_bootstrap_bundle_contract(
        bundle,
        dataplane_manifest=dependencies.dataplane,
        permissions_manifest=dependencies.permissions,
        verify_local_files=inputs.verify_local_bundle_files,
        repo_root=repo_root,
    )
    return _contract_section(
        "bootstrap_bundle",
        failed_codes=[violation.code for violation in violations],
        metrics={
            "artifact_count": len(bundle.artifacts),
            "local_file_verification": inputs.verify_local_bundle_files,
        },
    )


def _bootstrap_runner_section(
    inputs: ByocPreflightBundleInputs,
    *,
    repo_root: Path,
) -> ByocPreflightSection:
    try:
        report = run_byoc_bootstrap_runner(
            ByocBootstrapRunnerInputs(
                plan_path=inputs.bootstrap_plan_path,
                dataplane_manifest_path=inputs.dataplane_manifest_path,
                permissions_manifest_path=inputs.permissions_manifest_path,
                bootstrap_bundle_path=inputs.bootstrap_bundle_path,
                repo_root=repo_root,
                env_path=inputs.env_path,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "bootstrap_runner",
            "child_report_failed",
            1,
            error_type=type(exc).__name__,
        )
    return _checks_section(
        "bootstrap_runner",
        report.checks,
        required_checks_passed=report.required_checks_passed,
        metrics={
            "step_count": sum(1 for check in report.checks if check.step_id),
            "execution_mode": report.execution_mode or "unknown",
        },
    )


def _post_deploy_validation_section(
    inputs: ByocPreflightBundleInputs,
) -> ByocPreflightSection:
    try:
        report = run_byoc_post_deploy_validation(
            ByocValidationInputs(
                manifest_path=inputs.dataplane_manifest_path,
                env_path=inputs.env_path,
                require_live=False,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "post_deploy_validation",
            "child_report_failed",
            1,
            error_type=type(exc).__name__,
        )
    return _checks_section(
        "post_deploy_validation",
        report.checks,
        required_checks_passed=report.required_checks_passed,
        metrics={"offline_mode": True, "live_probes_required": False},
    )


def _aws_live_preflight_section(
    inputs: ByocPreflightBundleInputs,
) -> ByocPreflightSection:
    try:
        report = run_byoc_aws_live_preflight(
            ByocAwsLivePreflightInputs(
                dataplane_manifest_path=inputs.dataplane_manifest_path,
                permissions_manifest_path=inputs.permissions_manifest_path,
                iam_template_path=inputs.iam_template_path,
                aws_profile=inputs.aws_profile,
                aws_region=inputs.aws_region,
                expected_account_id=inputs.expected_aws_account_id,
                skip_live_aws=inputs.skip_aws_live_preflight_aws,
                run_readonly_api_probes=inputs.run_aws_readonly_api_probes,
                run_iam_policy_simulation=inputs.run_aws_iam_policy_simulation,
                simulation_principal_arn=inputs.aws_simulation_principal_arn,
                simulation_role_name=inputs.aws_simulation_role_name,
            )
        )
    except Exception as exc:  # noqa: BLE001
        return _load_error_section(
            "aws_live_preflight",
            "child_report_failed",
            1,
            error_type=type(exc).__name__,
        )
    return _checks_section(
        "aws_live_preflight",
        report.checks,
        required_checks_passed=report.required_checks_passed,
        metrics={
            "live_aws_api_calls_executed": report.live_aws_api_calls_executed,
            "cloud_credentials_required": report.cloud_credentials_required,
            "mutating_aws_api_calls_executed": report.mutating_aws_api_calls_executed,
            "iam_policy_simulation_requested": (
                report.iam_policy_simulation_requested
            ),
            "iam_policy_simulation_executed": report.iam_policy_simulation_executed,
            "checked_permission_evaluation_count": (
                report.checked_permission_evaluation_count
            ),
            "denied_permission_evaluation_count": (
                report.denied_permission_evaluation_count
            ),
            "readonly_api_probe_requested": report.readonly_api_probe_requested,
            "readonly_api_probe_executed": report.readonly_api_probe_executed,
        },
    )


@dataclass(frozen=True, slots=True)
class _Dependencies:
    dataplane: Any | None = None
    permissions: Any | None = None
    iam_template: Any | None = None
    failures: tuple[str, ...] = ()


def _load_contract_dependencies(inputs: ByocPreflightBundleInputs) -> _Dependencies:
    failures: list[str] = []
    dataplane = None
    permissions = None
    iam_template = None
    try:
        dataplane = load_byoc_manifest(inputs.dataplane_manifest_path)
    except Exception:  # noqa: BLE001
        failures.append("dataplane_manifest_unavailable")
    try:
        permissions = load_byoc_permissions_manifest(inputs.permissions_manifest_path)
    except Exception:  # noqa: BLE001
        failures.append("permissions_manifest_unavailable")
    try:
        iam_template = load_byoc_aws_iam_template(inputs.iam_template_path)
    except Exception:  # noqa: BLE001
        failures.append("iam_template_unavailable")
    return _Dependencies(
        dataplane=dataplane,
        permissions=permissions,
        iam_template=iam_template,
        failures=tuple(failures),
    )


def _contract_section(
    name: str,
    *,
    failed_codes: Sequence[str],
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocPreflightSection:
    normalized_failed = _unique_codes(failed_codes)
    failed = bool(normalized_failed)
    return ByocPreflightSection(
        name=name,
        status=_FAIL if failed else _PASS,
        required=True,
        required_checks_passed=not failed,
        check_summary=ByocPreflightCheckSummary(
            total=2,
            required=2,
            passed=1 if failed else 2,
            failed=1 if failed else 0,
            skipped=0,
            failed_required=1 if failed else 0,
        ),
        failed_check_codes=normalized_failed,
        metrics=metrics or {},
    )


def _checks_section(
    name: str,
    checks: Sequence[Any],
    *,
    required_checks_passed: bool,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocPreflightSection:
    return ByocPreflightSection(
        name=name,
        status=_PASS if required_checks_passed else _FAIL,
        required=True,
        required_checks_passed=required_checks_passed,
        check_summary=_check_summary(checks),
        failed_check_codes=_failed_check_codes(checks),
        metrics=metrics or {},
    )


def _load_error_section(
    name: str,
    code: str,
    error_count: int,
    *,
    error_type: str | None = None,
) -> ByocPreflightSection:
    metrics: dict[str, int | bool | str] = {"error_count": error_count}
    if error_type is not None:
        metrics["error_type"] = error_type
    return ByocPreflightSection(
        name=name,
        status=_FAIL,
        required=True,
        required_checks_passed=False,
        check_summary=ByocPreflightCheckSummary(
            total=1,
            required=1,
            passed=0,
            failed=1,
            skipped=0,
            failed_required=1,
        ),
        failed_check_codes=(code,),
        metrics=metrics,
    )


def _dependency_error_section(
    name: str,
    failed_codes: Sequence[str],
) -> ByocPreflightSection:
    normalized_failed = _unique_codes(failed_codes)
    return ByocPreflightSection(
        name=name,
        status=_FAIL,
        required=True,
        required_checks_passed=False,
        check_summary=ByocPreflightCheckSummary(
            total=len(normalized_failed),
            required=len(normalized_failed),
            passed=0,
            failed=len(normalized_failed),
            skipped=0,
            failed_required=len(normalized_failed),
        ),
        failed_check_codes=normalized_failed,
        metrics={"dependency_failure_count": len(normalized_failed)},
    )


def _check_summary(checks: Sequence[Any]) -> ByocPreflightCheckSummary:
    statuses = Counter(str(check.status) for check in checks)
    failed_required = sum(
        1
        for check in checks
        if str(check.status) == _FAIL and bool(getattr(check, "required", False))
    )
    return ByocPreflightCheckSummary(
        total=len(checks),
        required=sum(1 for check in checks if bool(getattr(check, "required", False))),
        passed=statuses[_PASS],
        failed=statuses[_FAIL],
        skipped=statuses["skipped"],
        failed_required=failed_required,
    )


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


def _identity_from_inputs(
    inputs: ByocPreflightBundleInputs,
) -> dict[str, str | None]:
    for loader, path in (
        (load_byoc_manifest, inputs.dataplane_manifest_path),
        (load_byoc_permissions_manifest, inputs.permissions_manifest_path),
        (load_byoc_aws_iac_package, inputs.iac_package_path),
        (load_byoc_bootstrap_bundle, inputs.bootstrap_bundle_path),
    ):
        try:
            source = loader(path)
        except Exception:  # noqa: BLE001
            continue
        return {
            "deployment_id": str(getattr(source, "deployment_id", "")) or None,
            "customer_id": str(getattr(source, "customer_id", "")) or None,
            "environment": str(getattr(source, "environment", "")) or None,
            "cloud_provider": str(getattr(source, "cloud_provider", "")) or None,
            "region": str(getattr(source, "region", "")) or None,
            "artifact_revision": (
                str(getattr(source, "artifact_revision", "")) or None
            ),
        }
    return {
        "deployment_id": None,
        "customer_id": None,
        "environment": None,
        "cloud_provider": None,
        "region": None,
        "artifact_revision": None,
    }


__all__ = [
    "ByocPreflightBundleInputs",
    "ByocPreflightBundleReport",
    "ByocPreflightCheckSummary",
    "ByocPreflightPrivacyContract",
    "ByocPreflightSection",
    "render_preflight_report_json",
    "render_preflight_report_yaml",
    "run_byoc_preflight_bundle",
]
