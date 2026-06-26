"""Sanitized BYOC Terraform scaffold plan-validation contract.

This module deliberately runs in contract-only mode: it validates the checked-in
Terraform scaffold and module layout without executing ``terraform plan`` or
capturing Terraform command output.
"""
from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_aws_iac_package import (
    ByocAwsIacPackage,
    generate_aws_iac_package,
    load_byoc_aws_iac_package,
    render_terraform_scaffold,
    render_validation_errors as render_iac_validation_errors,
    validate_aws_iac_package_contract,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    load_byoc_manifest,
)
from services.platform.runtime.byoc_permissions import (
    ByocAwsIamTemplateSkeleton,
    ByocPermissionsManifest,
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
)


TerraformValidationStatus = Literal["pass", "fail", "skipped"]
TerraformValidationMode = Literal["contract_only"]
_PASS: TerraformValidationStatus = "pass"
_FAIL: TerraformValidationStatus = "fail"
_SKIPPED: TerraformValidationStatus = "skipped"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocTerraformPlanValidationCheck(_StrictModel):
    name: str
    status: TerraformValidationStatus
    required: bool
    details: str
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("check names must be bounded identifiers")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("check details must be bounded")
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
                raise ValueError("metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120:
                    raise ValueError("string metrics must be bounded")
            normalized[key] = metric
        return normalized


class ByocTerraformPlanValidationReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.terraform_plan_validation.v1"]
    status: TerraformValidationStatus
    required_checks_passed: bool
    execution_mode: TerraformValidationMode = "contract_only"
    iac_package_path: str
    dataplane_manifest_path: str
    permissions_manifest_path: str
    iam_template_path: str
    terraform_root_path: str | None = None
    module_count: int = Field(ge=0)
    terraform_file_count: int = Field(ge=0)
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    environment: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    terraform_plan_json_included: Literal[False] = False
    terraform_command_output_included: Literal[False] = False
    checks: tuple[ByocTerraformPlanValidationCheck, ...]

    def as_json(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    @field_validator(
        "iac_package_path",
        "dataplane_manifest_path",
        "permissions_manifest_path",
        "iam_template_path",
    )
    @classmethod
    def _path_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("report paths must be bounded")
        return value

    @field_validator(
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
        "terraform_root_path",
    )
    @classmethod
    def _optional_strings_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 160:
            raise ValueError("report identity fields must be bounded")
        return value


@dataclass(frozen=True, slots=True)
class ByocTerraformPlanValidationInputs:
    iac_package_path: Path
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path
    repo_root: Path = field(default_factory=Path.cwd)


def run_byoc_terraform_plan_validation(
    inputs: ByocTerraformPlanValidationInputs,
) -> ByocTerraformPlanValidationReport:
    started = time.monotonic()
    repo_root = inputs.repo_root.resolve()
    checks: list[ByocTerraformPlanValidationCheck] = []

    package, package_checks = _load_iac_package(inputs.iac_package_path)
    dataplane, dataplane_checks = _load_dataplane(inputs.dataplane_manifest_path)
    permissions, permissions_checks = _load_permissions(inputs.permissions_manifest_path)
    iam_template, iam_checks = _load_iam_template(inputs.iam_template_path)
    checks.extend(package_checks)
    checks.extend(dataplane_checks)
    checks.extend(permissions_checks)
    checks.extend(iam_checks)

    if (
        package is not None
        and dataplane is not None
        and permissions is not None
        and iam_template is not None
    ):
        checks.extend(
            _evaluate_scaffold(
                package,
                dataplane=dataplane,
                permissions=permissions,
                iam_template=iam_template,
                inputs=inputs,
                repo_root=repo_root,
            )
        )

    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    identity = _report_identity(package, dataplane, permissions, iam_template)
    status: TerraformValidationStatus = _PASS if required_checks_passed else _FAIL
    return ByocTerraformPlanValidationReport(
        schema_version="fyralis.byoc.terraform_plan_validation.v1",
        status=status,
        required_checks_passed=required_checks_passed,
        execution_mode="contract_only",
        iac_package_path=str(inputs.iac_package_path),
        dataplane_manifest_path=str(inputs.dataplane_manifest_path),
        permissions_manifest_path=str(inputs.permissions_manifest_path),
        iam_template_path=str(inputs.iam_template_path),
        terraform_root_path=(
            package.terraform.root_module_path if package is not None else None
        ),
        module_count=len(package.terraform.modules) if package is not None else 0,
        terraform_file_count=(
            len(package.terraform.files)
            + sum(len(module.files) for module in package.terraform.modules)
            if package is not None
            else 0
        ),
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        environment=identity.get("environment"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        terraform_plan_json_included=False,
        terraform_command_output_included=False,
        checks=tuple(checks),
    )


def load_byoc_terraform_plan_validation_report(
    path: Path,
) -> ByocTerraformPlanValidationReport:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC Terraform validation report must be a JSON/YAML object")
    return ByocTerraformPlanValidationReport.model_validate(data)


def render_terraform_plan_validation_json(
    report: ByocTerraformPlanValidationReport,
) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_terraform_plan_validation_yaml(
    report: ByocTerraformPlanValidationReport,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _evaluate_scaffold(
    package: ByocAwsIacPackage,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    iam_template: ByocAwsIamTemplateSkeleton,
    inputs: ByocTerraformPlanValidationInputs,
    repo_root: Path,
) -> list[ByocTerraformPlanValidationCheck]:
    checks: list[ByocTerraformPlanValidationCheck] = []
    violations = validate_aws_iac_package_contract(
        package,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        iam_template=iam_template,
        repo_root=repo_root,
    )
    checks.append(
        _check(
            "iac_package_contract",
            _FAIL if violations else _PASS,
            required=True,
            details=(
                "AWS IaC package contract has bounded violations."
                if violations
                else "AWS IaC package contract passed."
            ),
            metrics={"violation_count": len(violations)},
        )
    )

    generated = generate_aws_iac_package(
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        iam_template=iam_template,
        source_paths=_package_source_paths(package),
        terraform_root_path=Path(package.terraform.root_module_path),
    )
    package_drift = package.model_dump(mode="json") != generated.model_dump(mode="json")
    checks.append(
        _check(
            "generated_iac_package_drift",
            _FAIL if package_drift else _PASS,
            required=True,
            details=(
                "Checked-in AWS IaC package differs from generated contract."
                if package_drift
                else "Checked-in AWS IaC package matches generated contract."
            ),
            metrics={"drift_detected": package_drift},
        )
    )

    rendered_terraform = render_terraform_scaffold(
        generated,
        iam_template=iam_template,
    )
    drift_count = _terraform_drift_count(rendered_terraform, repo_root=repo_root)
    checks.append(
        _check(
            "generated_terraform_scaffold_drift",
            _FAIL if drift_count else _PASS,
            required=True,
            details=(
                "Checked-in Terraform scaffold differs from generated contract."
                if drift_count
                else "Checked-in Terraform scaffold matches generated contract."
            ),
            metrics={"drift_count": drift_count},
        )
    )

    module_components = {module.component for module in package.terraform.modules}
    required_components = {"iam", "network", "data_services", "runtime", "data_plane_agent"}
    missing_modules = required_components - module_components
    checks.append(
        _check(
            "terraform_module_contracts",
            _FAIL if missing_modules else _PASS,
            required=True,
            details=(
                "Required Terraform component module contracts are missing."
                if missing_modules
                else "Required Terraform component module contracts are present."
            ),
            metrics={
                "module_count": len(package.terraform.modules),
                "missing_module_count": len(missing_modules),
            },
        )
    )

    mutating = (
        package.execution.terraform_apply_allowed
        or any(file.declares_resources for file in package.terraform.files)
        or any(
            file.declares_resources
            for module in package.terraform.modules
            for file in module.files
        )
    )
    checks.append(
        _check(
            "terraform_scaffold_non_mutating",
            _FAIL if mutating else _PASS,
            required=True,
            details=(
                "Terraform scaffold allows mutating declarations."
                if mutating
                else "Terraform scaffold is non-mutating."
            ),
            metrics={
                "terraform_apply_allowed": package.execution.terraform_apply_allowed,
                "resource_blocks_declared": mutating,
            },
        )
    )

    contract_only = (
        package.execution.cloud_credentials_required_for_validation is False
        and package.execution.terraform_apply_allowed is False
    )
    checks.append(
        _check(
            "terraform_plan_contract_only",
            _PASS if contract_only else _FAIL,
            required=True,
            details="Terraform validation is contract-only and raw-output-free.",
            metrics={
                "cloud_credentials_required": (
                    package.execution.cloud_credentials_required_for_validation
                ),
                "terraform_plan_json_included": False,
                "terraform_command_output_included": False,
            },
        )
    )
    return checks


def _package_source_paths(package: ByocAwsIacPackage) -> dict[str, Path]:
    return {
        "dataplane_manifest": Path(package.references.dataplane_manifest_path),
        "permissions_manifest": Path(package.references.permissions_manifest_path),
        "iam_skeleton": Path(package.references.iam_skeleton_path),
        "bootstrap_bundle": _component_source_path(package, "runtime"),
    }


def _component_source_path(package: ByocAwsIacPackage, name: str) -> Path:
    for component in package.components:
        if component.name == name:
            return Path(component.source_contract)
    return Path("deploy/byoc/bootstrap-bundle.example.yaml")


def _terraform_drift_count(rendered: Mapping[str, str], *, repo_root: Path) -> int:
    drift = 0
    for rel_path, expected in rendered.items():
        path = repo_root / rel_path
        if not path.exists() or path.read_text(encoding="utf-8") != expected:
            drift += 1
    return drift


def _load_iac_package(
    path: Path,
) -> tuple[ByocAwsIacPackage | None, list[ByocTerraformPlanValidationCheck]]:
    try:
        package = load_byoc_aws_iac_package(path)
    except ValidationError as exc:
        return None, [_schema_failure("iac_package_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("iac_package_schema", exc)]
    return package, [
        _check(
            "iac_package_schema",
            _PASS,
            required=True,
            details="BYOC AWS IaC package schema is valid.",
            metrics={
                "root_file_count": len(package.terraform.files),
                "module_count": len(package.terraform.modules),
            },
        )
    ]


def _load_dataplane(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocTerraformPlanValidationCheck]]:
    try:
        manifest = load_byoc_manifest(path)
    except ValidationError as exc:
        return None, [_schema_failure("dataplane_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("dataplane_manifest_schema", exc)]
    return manifest, [
        _check(
            "dataplane_manifest_schema",
            _PASS,
            required=True,
            details="BYOC data-plane manifest schema is valid.",
        )
    ]


def _load_permissions(
    path: Path,
) -> tuple[ByocPermissionsManifest | None, list[ByocTerraformPlanValidationCheck]]:
    try:
        manifest = load_byoc_permissions_manifest(path)
    except ValidationError as exc:
        return None, [_schema_failure("permissions_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("permissions_manifest_schema", exc)]
    return manifest, [
        _check(
            "permissions_manifest_schema",
            _PASS,
            required=True,
            details="BYOC permissions manifest schema is valid.",
        )
    ]


def _load_iam_template(
    path: Path,
) -> tuple[ByocAwsIamTemplateSkeleton | None, list[ByocTerraformPlanValidationCheck]]:
    try:
        template = load_byoc_aws_iam_template(path)
    except ValidationError as exc:
        return None, [_schema_failure("iam_template_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("iam_template_schema", exc)]
    return template, [
        _check(
            "iam_template_schema",
            _PASS,
            required=True,
            details="BYOC AWS IAM template schema is valid.",
        )
    ]


def _schema_failure(
    name: str,
    exc: ValidationError,
) -> ByocTerraformPlanValidationCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="Schema validation failed with bounded error count.",
        metrics={"error_count": len(render_iac_validation_errors(exc))},
    )


def _load_failure(name: str, exc: Exception) -> ByocTerraformPlanValidationCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="Local scaffold input failed to load.",
        metrics={"error_type": type(exc).__name__},
    )


def _check(
    name: str,
    status: TerraformValidationStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocTerraformPlanValidationCheck:
    return ByocTerraformPlanValidationCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _report_identity(*sources: object | None) -> dict[str, str | None]:
    identity: dict[str, str | None] = {
        "deployment_id": None,
        "customer_id": None,
        "environment": None,
        "cloud_provider": None,
        "region": None,
        "artifact_revision": None,
    }
    for field_name in identity:
        for source in sources:
            if source is not None and hasattr(source, field_name):
                value = getattr(source, field_name)
                identity[field_name] = str(value) if value is not None else None
                break
    return identity


def _load_mapping(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError(
            "YAML reports require PyYAML; use JSON or install the dev extras"
        ) from exc
    return yaml.safe_load(raw)


__all__ = [
    "ByocTerraformPlanValidationCheck",
    "ByocTerraformPlanValidationInputs",
    "ByocTerraformPlanValidationReport",
    "load_byoc_terraform_plan_validation_report",
    "render_terraform_plan_validation_json",
    "render_terraform_plan_validation_yaml",
    "render_validation_errors",
    "run_byoc_terraform_plan_validation",
]
