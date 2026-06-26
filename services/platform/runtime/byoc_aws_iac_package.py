"""AWS BYOC IaC package scaffold contracts."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    DeploymentEnvironment,
)
from services.platform.runtime.byoc_permissions import (
    ByocAwsIamTemplateSkeleton,
    ByocPermissionsManifest,
)


PackageStatus = Literal["scaffold_only"]
ExecutionOwner = Literal["customer_side_runner"]
TerraformBackend = Literal["customer_managed"]
TerraformFileRole = Literal[
    "provider_versions",
    "input_variables",
    "deployment_contract",
    "operator_outputs",
]
ScaffoldComponentStatus = Literal["declared", "placeholder"]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:/@+=,<> -]{1,160}$")
_TERRAFORM_VARIABLE_RE = re.compile(r'^\s*variable\s+"(?P<name>[A-Za-z0-9_-]+)"', re.MULTILINE)
_TERRAFORM_RESOURCE_RE = re.compile(r'^\s*resource\s+"', re.MULTILINE)
_TERRAFORM_BACKEND_RE = re.compile(r'^\s*backend\s+"', re.MULTILINE)
_TERRAFORM_EXTERNAL_DATA_RE = re.compile(r'^\s*data\s+"external"', re.MULTILINE)
_TERRAFORM_PROVISIONER_RE = re.compile(r'^\s*provisioner\s+"', re.MULTILINE)
_FORBIDDEN_TERRAFORM_FRAGMENTS = (
    "aws_iam_access_key",
    "client_secret",
    "local-exec",
    "password",
    "private_key",
    "raw_payload",
    "remote-exec",
    "secret_value",
    "token_value",
)
_REQUIRED_TERRAFORM_FILE_ROLES = frozenset(
    {"provider_versions", "input_variables", "deployment_contract", "operator_outputs"}
)
_REQUIRED_COMPONENTS = frozenset(
    {"iam", "network", "data_services", "runtime", "data_plane_agent"}
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAwsIacReferences(_StrictModel):
    dataplane_manifest_path: str
    permissions_manifest_path: str
    iam_skeleton_path: str

    @field_validator(
        "dataplane_manifest_path",
        "permissions_manifest_path",
        "iam_skeleton_path",
    )
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocAwsIacExecution(_StrictModel):
    owner: ExecutionOwner = "customer_side_runner"
    terraform_apply_allowed: Literal[False] = False
    cloud_credentials_required_for_validation: Literal[False] = False
    control_plane_mutating_access_allowed: Literal[False] = False
    stores_remote_state_in_control_plane: Literal[False] = False
    no_inbound_control_plane_ports: Literal[True] = True


class ByocTerraformFile(_StrictModel):
    path: str
    role: TerraformFileRole
    required: Literal[True] = True
    declares_resources: Literal[False] = False

    @field_validator("path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocTerraformScaffold(_StrictModel):
    root_module_path: str
    required_version: str
    provider_source: Literal["hashicorp/aws"] = "hashicorp/aws"
    provider_region_variable: Literal["region"] = "region"
    backend: TerraformBackend = "customer_managed"
    files: tuple[ByocTerraformFile, ...]

    @field_validator("root_module_path")
    @classmethod
    def _root_path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)

    @field_validator("required_version")
    @classmethod
    def _version_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_CODE_RE.match(value):
            raise ValueError("required_version must be a bounded Terraform constraint")
        return value

    @field_validator("files")
    @classmethod
    def _files_must_be_present(
        cls,
        value: tuple[ByocTerraformFile, ...],
    ) -> tuple[ByocTerraformFile, ...]:
        if not value:
            raise ValueError("files must not be empty")
        return value


class ByocAwsIacSafety(_StrictModel):
    customer_side_bootstrap_required: Literal[True] = True
    raw_customer_data_variables_allowed: Literal[False] = False
    raw_secret_values_allowed: Literal[False] = False
    outbound_control_plane_port: Literal[443] = 443
    required_resource_tag_keys: tuple[str, ...]
    required_variables: tuple[str, ...]

    @field_validator("required_resource_tag_keys", "required_variables")
    @classmethod
    def _values_must_be_present(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("required values must not be empty")
        normalized = tuple(item.strip() for item in value)
        if any(not item for item in normalized):
            raise ValueError("required values must not contain blanks")
        return normalized


class ByocAwsIacComponent(_StrictModel):
    name: str
    source_contract: str
    scaffold_status: ScaffoldComponentStatus

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not re.match(r"^[a-z][a-z0-9_]{1,80}$", value):
            raise ValueError("component name must be a bounded code")
        return value

    @field_validator("source_contract")
    @classmethod
    def _source_contract_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocAwsIacPackage(_StrictModel):
    schema_version: Literal["fyralis.byoc.aws.iac_package.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: Literal["aws"]
    region: str
    artifact_revision: str
    package_status: PackageStatus
    references: ByocAwsIacReferences
    execution: ByocAwsIacExecution
    terraform: ByocTerraformScaffold
    safety: ByocAwsIacSafety
    components: tuple[ByocAwsIacComponent, ...]

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

    @field_validator("region", "artifact_revision")
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("package fields must not be empty")
        return value

    @field_validator("components")
    @classmethod
    def _components_must_be_present(
        cls,
        value: tuple[ByocAwsIacComponent, ...],
    ) -> tuple[ByocAwsIacComponent, ...]:
        if not value:
            raise ValueError("components must not be empty")
        return value


@dataclass(frozen=True, slots=True)
class ByocAwsIacPackageViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def validate_aws_iac_package_contract(
    package: ByocAwsIacPackage,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    permissions_manifest: ByocPermissionsManifest | None = None,
    iam_template: ByocAwsIamTemplateSkeleton | None = None,
    repo_root: Path | None = None,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    if dataplane_manifest is not None:
        violations.extend(_compare_dataplane_manifest(package, dataplane_manifest))
    if permissions_manifest is not None:
        violations.extend(_compare_permissions_manifest(package, permissions_manifest))
    if iam_template is not None:
        violations.extend(_compare_iam_template(package, iam_template))

    terraform_roles = [file.role for file in package.terraform.files]
    for role in sorted(_REQUIRED_TERRAFORM_FILE_ROLES - set(terraform_roles)):
        violations.append(
            _violation(
                "terraform.files",
                "missing_required_terraform_file_role",
                f"{role!r} is required in the AWS IaC scaffold",
            )
        )
    duplicates = sorted({role for role in terraform_roles if terraform_roles.count(role) > 1})
    for role in duplicates:
        violations.append(
            _violation(
                "terraform.files",
                "duplicate_terraform_file_role",
                f"{role!r} is listed more than once",
            )
        )

    component_names = [component.name for component in package.components]
    for name in sorted(_REQUIRED_COMPONENTS - set(component_names)):
        violations.append(
            _violation(
                "components",
                "missing_required_component",
                f"{name!r} is required in the AWS IaC scaffold",
            )
        )

    root = repo_root or Path.cwd()
    all_tf_text = ""
    for file in package.terraform.files:
        violations.extend(_validate_terraform_file(file, package, repo_root=root))
        path = root / file.path
        if path.exists():
            all_tf_text += "\n" + path.read_text(encoding="utf-8", errors="ignore")
    for variable in package.safety.required_variables:
        if variable not in _declared_variables(all_tf_text):
            violations.append(
                _violation(
                    "safety.required_variables",
                    "required_variable_not_declared",
                    f"{variable!r} is not declared by the Terraform scaffold",
                )
            )
    for tag in package.safety.required_resource_tag_keys:
        if tag not in all_tf_text:
            violations.append(
                _violation(
                    "safety.required_resource_tag_keys",
                    "required_tag_not_declared",
                    f"{tag!r} is not declared in the Terraform scaffold",
                )
            )

    return violations


def byoc_aws_iac_package_json_schema() -> dict[str, Any]:
    return ByocAwsIacPackage.model_json_schema()


def load_byoc_aws_iac_package(path: Path) -> ByocAwsIacPackage:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC AWS IaC package must be a JSON/YAML object")
    return ByocAwsIacPackage.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _validate_terraform_file(
    file: ByocTerraformFile,
    package: ByocAwsIacPackage,
    *,
    repo_root: Path,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    root_path = Path(package.terraform.root_module_path)
    file_path = Path(file.path)
    if root_path not in (file_path, *file_path.parents):
        violations.append(
            _violation(
                f"terraform.files.{file.role}.path",
                "terraform_file_outside_root",
                "Terraform scaffold files must live under root_module_path",
            )
        )
    full_path = repo_root / file.path
    if not full_path.exists():
        return [
            _violation(
                f"terraform.files.{file.role}.path",
                "terraform_file_missing",
                "Terraform scaffold file is missing",
            )
        ]
    text = full_path.read_text(encoding="utf-8", errors="ignore")
    if file.declares_resources is False and _TERRAFORM_RESOURCE_RE.search(text):
        violations.append(
            _violation(
                f"terraform.files.{file.role}",
                "terraform_resource_block_forbidden",
                "scaffold-only AWS IaC package must not declare resource blocks",
            )
        )
    if _TERRAFORM_BACKEND_RE.search(text):
        violations.append(
            _violation(
                f"terraform.files.{file.role}",
                "terraform_backend_block_forbidden",
                "scaffold must not hard-code Terraform backend state",
            )
        )
    if _TERRAFORM_EXTERNAL_DATA_RE.search(text):
        violations.append(
            _violation(
                f"terraform.files.{file.role}",
                "terraform_external_data_forbidden",
                "scaffold must not execute external data providers",
            )
        )
    if _TERRAFORM_PROVISIONER_RE.search(text):
        violations.append(
            _violation(
                f"terraform.files.{file.role}",
                "terraform_provisioner_block_forbidden",
                "scaffold must not execute Terraform provisioners",
            )
        )
    lowered = text.lower()
    for fragment in _FORBIDDEN_TERRAFORM_FRAGMENTS:
        if fragment in lowered:
            violations.append(
                _violation(
                    f"terraform.files.{file.role}",
                    "terraform_sensitive_or_exec_fragment_forbidden",
                    f"{fragment!r} must not appear in scaffold-only IaC",
                )
            )
    return violations


def _compare_dataplane_manifest(
    package: ByocAwsIacPackage,
    dataplane: ByocDataPlaneManifest,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(package, field) != getattr(dataplane, field):
            violations.append(
                _violation(
                    field,
                    "dataplane_manifest_mismatch",
                    f"AWS IaC package {field} does not match data-plane manifest",
                )
            )
    if dataplane.network.control_plane_inbound_allowed is not False:
        violations.append(
            _violation(
                "execution.no_inbound_control_plane_ports",
                "control_plane_inbound_forbidden",
                "AWS IaC package requires an egress-only data-plane contract",
            )
        )
    return violations


def _compare_permissions_manifest(
    package: ByocAwsIacPackage,
    permissions: ByocPermissionsManifest,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(package, field) != getattr(permissions, field):
            violations.append(
                _violation(
                    field,
                    "permissions_manifest_mismatch",
                    f"AWS IaC package {field} does not match permissions manifest",
                )
            )
    if permissions.aws is not None:
        missing_tags = set(permissions.aws.required_resource_tag_keys) - set(
            package.safety.required_resource_tag_keys
        )
        for tag in sorted(missing_tags):
            violations.append(
                _violation(
                    "safety.required_resource_tag_keys",
                    "permissions_required_tag_missing",
                    f"{tag!r} is required by the permissions manifest",
                )
            )
    return violations


def _compare_iam_template(
    package: ByocAwsIacPackage,
    template: ByocAwsIamTemplateSkeleton,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    for field in ("deployment_id", "customer_id", "cloud_provider", "region"):
        if getattr(package, field) != getattr(template, field):
            violations.append(
                _violation(
                    field,
                    "iam_template_mismatch",
                    f"AWS IaC package {field} does not match IAM skeleton",
                )
            )
    return violations


def _declared_variables(terraform_text: str) -> set[str]:
    return {match.group("name") for match in _TERRAFORM_VARIABLE_RE.finditer(terraform_text)}


def _relative_path(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("path must not be empty")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("path must be repository-relative")
    if "://" in value:
        raise ValueError("path must not be a URL")
    return value


def _load_mapping(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError(
            "YAML manifests require PyYAML; use JSON or install the dev extras"
        ) from exc
    return yaml.safe_load(raw)


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocAwsIacPackageViolation:
    return ByocAwsIacPackageViolation(path=path, code=code, message=message)


__all__ = [
    "ByocAwsIacPackage",
    "ByocAwsIacPackageViolation",
    "byoc_aws_iac_package_json_schema",
    "load_byoc_aws_iac_package",
    "render_validation_errors",
    "validate_aws_iac_package_contract",
]
