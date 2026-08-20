"""AWS BYOC IaC package scaffold contracts."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
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
    "module_calls",
    "operator_outputs",
]
TerraformModuleFileRole = Literal["module_contract"]
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
    {
        "provider_versions",
        "input_variables",
        "deployment_contract",
        "module_calls",
        "operator_outputs",
    }
)
_REQUIRED_COMPONENTS = frozenset(
    {"iam", "network", "data_services", "runtime", "data_plane_agent"}
)
_COMPONENT_SPECS = (
    ("iam", "permissions_manifest", "declared"),
    ("network", "dataplane_manifest", "placeholder"),
    ("data_services", "dataplane_manifest", "placeholder"),
    ("runtime", "bootstrap_bundle", "placeholder"),
    ("data_plane_agent", "dataplane_manifest", "placeholder"),
)
_DEFAULT_REQUIRED_VARIABLES = (
    "deployment_id",
    "customer_id",
    "environment",
    "region",
    "aws_account_id",
    "cloudformation_stack_prefix",
    "permissions_boundary_policy_arn",
)
_VARIABLE_DESCRIPTIONS = {
    "deployment_id": (
        "Stable Fyralis BYOC deployment identifier from the data-plane manifest."
    ),
    "customer_id": "Stable Fyralis customer identifier from the data-plane manifest.",
    "environment": "Deployment environment from the data-plane manifest.",
    "region": "AWS region for customer-owned data-plane resources.",
    "aws_account_id": "Customer AWS account identifier that owns the data plane.",
    "cloudformation_stack_prefix": (
        "Customer-approved stack prefix for Fyralis BYOC resources."
    ),
    "permissions_boundary_policy_arn": (
        "Customer-owned IAM permissions boundary applied to Fyralis roles."
    ),
}
_TAG_VALUE_EXPRESSION = {
    "fyralis:deployment-id": "var.deployment_id",
    "fyralis:customer-id": "var.customer_id",
    "fyralis:managed": '"true"',
    "fyralis:environment": "var.environment",
}


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


class ByocTerraformModuleFile(_StrictModel):
    path: str
    role: TerraformModuleFileRole = "module_contract"
    required: Literal[True] = True
    declares_resources: Literal[False] = False

    @field_validator("path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocTerraformModule(_StrictModel):
    name: str
    component: str
    source_path: str
    scaffold_status: ScaffoldComponentStatus
    files: tuple[ByocTerraformModuleFile, ...]

    @field_validator("name", "component")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not re.match(r"^[a-z][a-z0-9_]{1,80}$", value):
            raise ValueError("Terraform module names must be bounded codes")
        return value

    @field_validator("source_path")
    @classmethod
    def _source_path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)

    @field_validator("files")
    @classmethod
    def _files_must_be_present(
        cls,
        value: tuple[ByocTerraformModuleFile, ...],
    ) -> tuple[ByocTerraformModuleFile, ...]:
        if not value:
            raise ValueError("Terraform module files must not be empty")
        return value


class ByocTerraformScaffold(_StrictModel):
    root_module_path: str
    required_version: str
    provider_source: Literal["hashicorp/aws"] = "hashicorp/aws"
    provider_region_variable: Literal["region"] = "region"
    backend: TerraformBackend = "customer_managed"
    files: tuple[ByocTerraformFile, ...]
    modules: tuple[ByocTerraformModule, ...]

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

    @field_validator("modules")
    @classmethod
    def _modules_must_be_present(
        cls,
        value: tuple[ByocTerraformModule, ...],
    ) -> tuple[ByocTerraformModule, ...]:
        if not value:
            raise ValueError("modules must not be empty")
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

    module_components = [module.component for module in package.terraform.modules]
    for name in sorted(_REQUIRED_COMPONENTS - set(module_components)):
        violations.append(
            _violation(
                "terraform.modules",
                "missing_required_terraform_module",
                f"{name!r} module is required in the AWS IaC scaffold",
            )
        )
    duplicate_module_components = sorted(
        {
            component
            for component in module_components
            if module_components.count(component) > 1
        }
    )
    for component in duplicate_module_components:
        violations.append(
            _violation(
                "terraform.modules",
                "duplicate_terraform_module_component",
                f"{component!r} is listed by more than one Terraform module",
            )
        )

    root = repo_root or Path.cwd()
    all_tf_text = ""
    for file in package.terraform.files:
        violations.extend(_validate_terraform_file(file, package, repo_root=root))
        path = root / file.path
        if path.exists():
            all_tf_text += "\n" + path.read_text(encoding="utf-8", errors="ignore")
    for module in package.terraform.modules:
        violations.extend(
            _validate_terraform_module(
                module,
                package,
                component_status={
                    component.name: component.scaffold_status
                    for component in package.components
                },
                repo_root=root,
            )
        )
        for file in module.files:
            path = root / file.path
            if path.exists():
                all_tf_text += "\n" + path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                )
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


def generate_aws_iac_package(
    *,
    dataplane_manifest: ByocDataPlaneManifest,
    permissions_manifest: ByocPermissionsManifest,
    iam_template: ByocAwsIamTemplateSkeleton,
    source_paths: Mapping[str, Path],
    terraform_root_path: Path = Path("deploy/byoc/aws/terraform"),
    package_status: PackageStatus = "scaffold_only",
) -> ByocAwsIacPackage:
    if permissions_manifest.aws is None:
        raise ValueError("AWS permissions manifest block is required")
    root_path = _path_string(terraform_root_path)
    terraform_files = (
        ByocTerraformFile(
            path=f"{root_path}/versions.tf",
            role="provider_versions",
            required=True,
            declares_resources=False,
        ),
        ByocTerraformFile(
            path=f"{root_path}/variables.tf",
            role="input_variables",
            required=True,
            declares_resources=False,
        ),
        ByocTerraformFile(
            path=f"{root_path}/locals.tf",
            role="deployment_contract",
            required=True,
            declares_resources=False,
        ),
        ByocTerraformFile(
            path=f"{root_path}/main.tf",
            role="module_calls",
            required=True,
            declares_resources=False,
        ),
        ByocTerraformFile(
            path=f"{root_path}/outputs.tf",
            role="operator_outputs",
            required=True,
            declares_resources=False,
        ),
    )
    return ByocAwsIacPackage(
        schema_version="fyralis.byoc.aws.iac_package.v1",
        deployment_id=dataplane_manifest.deployment_id,
        customer_id=dataplane_manifest.customer_id,
        environment=dataplane_manifest.environment,
        cloud_provider="aws",
        region=dataplane_manifest.region,
        artifact_revision=dataplane_manifest.artifact_revision,
        package_status=package_status,
        references=ByocAwsIacReferences(
            dataplane_manifest_path=_path_string(source_paths["dataplane_manifest"]),
            permissions_manifest_path=_path_string(
                source_paths["permissions_manifest"]
            ),
            iam_skeleton_path=_path_string(source_paths["iam_skeleton"]),
        ),
        execution=ByocAwsIacExecution(
            owner="customer_side_runner",
            terraform_apply_allowed=False,
            cloud_credentials_required_for_validation=False,
            control_plane_mutating_access_allowed=False,
            stores_remote_state_in_control_plane=False,
            no_inbound_control_plane_ports=True,
        ),
        terraform=ByocTerraformScaffold(
            root_module_path=root_path,
            required_version=">= 1.6.0",
            provider_source="hashicorp/aws",
            provider_region_variable="region",
            backend="customer_managed",
            files=terraform_files,
            modules=_terraform_modules(root_path),
        ),
        safety=ByocAwsIacSafety(
            customer_side_bootstrap_required=True,
            raw_customer_data_variables_allowed=False,
            raw_secret_values_allowed=False,
            outbound_control_plane_port=443,
            required_resource_tag_keys=permissions_manifest.aws.required_resource_tag_keys,
            required_variables=_DEFAULT_REQUIRED_VARIABLES,
        ),
        components=tuple(
            ByocAwsIacComponent(
                name=name,
                source_contract=_path_string(source_paths[source_key]),
                scaffold_status=status,
            )
            for name, source_key, status in _COMPONENT_SPECS
        ),
    )


def _terraform_modules(root_path: str) -> tuple[ByocTerraformModule, ...]:
    return tuple(
        ByocTerraformModule(
            name=name,
            component=name,
            source_path=f"{root_path}/modules/{name}",
            scaffold_status=status,
            files=(
                ByocTerraformModuleFile(
                    path=f"{root_path}/modules/{name}/main.tf",
                    role="module_contract",
                    required=True,
                    declares_resources=False,
                ),
            ),
        )
        for name, _, status in _COMPONENT_SPECS
    )


def render_terraform_scaffold(
    package: ByocAwsIacPackage,
    *,
    iam_template: ByocAwsIamTemplateSkeleton,
) -> dict[str, str]:
    by_role = {file.role: file.path for file in package.terraform.files}
    return {
        by_role["provider_versions"]: _render_versions_tf(package),
        by_role["input_variables"]: _render_variables_tf(package),
        by_role["deployment_contract"]: _render_locals_tf(
            package,
            iam_template=iam_template,
        ),
        by_role["module_calls"]: _render_main_tf(package),
        by_role["operator_outputs"]: _render_outputs_tf(package),
        **{
            file.path: _render_module_contract_tf(module)
            for module in package.terraform.modules
            for file in module.files
        },
    }


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
    violations.extend(
        _validate_terraform_text(
            text,
            path=f"terraform.files.{file.role}",
            declares_resources=file.declares_resources,
        )
    )
    return violations


def _validate_terraform_module(
    module: ByocTerraformModule,
    package: ByocAwsIacPackage,
    *,
    component_status: Mapping[str, ScaffoldComponentStatus],
    repo_root: Path,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    root_path = Path(package.terraform.root_module_path)
    modules_root = root_path / "modules"
    module_path = Path(module.source_path)
    if root_path not in (module_path, *module_path.parents):
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.source_path",
                "terraform_module_outside_root",
                "Terraform module source paths must live under root_module_path",
            )
        )
    if module_path.parent != modules_root:
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.source_path",
                "terraform_module_not_direct_child",
                "Terraform module source paths must live under root_module_path/modules",
            )
        )
    if module.name != module.component:
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.component",
                "terraform_module_name_mismatch",
                "Terraform module name must match its BYOC component",
            )
        )
    expected_status = component_status.get(module.component)
    if expected_status is None:
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.component",
                "terraform_module_component_unknown",
                "Terraform module component must match a declared BYOC component",
            )
        )
    elif module.scaffold_status != expected_status:
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.scaffold_status",
                "terraform_module_status_mismatch",
                "Terraform module status must match the declared BYOC component",
            )
        )
    for file in module.files:
        violations.extend(
            _validate_terraform_module_file(
                file,
                module=module,
                repo_root=repo_root,
            )
        )
    return violations


def _validate_terraform_module_file(
    file: ByocTerraformModuleFile,
    *,
    module: ByocTerraformModule,
    repo_root: Path,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    module_path = Path(module.source_path)
    file_path = Path(file.path)
    if module_path not in (file_path, *file_path.parents):
        violations.append(
            _violation(
                f"terraform.modules.{module.name}.files.{file.role}.path",
                "terraform_module_file_outside_module",
                "Terraform module files must live under the module source path",
            )
        )
    full_path = repo_root / file.path
    if not full_path.exists():
        return [
            _violation(
                f"terraform.modules.{module.name}.files.{file.role}.path",
                "terraform_module_file_missing",
                "Terraform module scaffold file is missing",
            )
        ]
    text = full_path.read_text(encoding="utf-8", errors="ignore")
    violations.extend(
        _validate_terraform_text(
            text,
            path=f"terraform.modules.{module.name}.files.{file.role}",
            declares_resources=file.declares_resources,
        )
    )
    return violations


def _validate_terraform_text(
    text: str,
    *,
    path: str,
    declares_resources: bool,
) -> list[ByocAwsIacPackageViolation]:
    violations: list[ByocAwsIacPackageViolation] = []
    if declares_resources is False and _TERRAFORM_RESOURCE_RE.search(text):
        violations.append(
            _violation(
                path,
                "terraform_resource_block_forbidden",
                "scaffold-only AWS IaC package must not declare resource blocks",
            )
        )
    if _TERRAFORM_BACKEND_RE.search(text):
        violations.append(
            _violation(
                path,
                "terraform_backend_block_forbidden",
                "scaffold must not hard-code Terraform backend state",
            )
        )
    if _TERRAFORM_EXTERNAL_DATA_RE.search(text):
        violations.append(
            _violation(
                path,
                "terraform_external_data_forbidden",
                "scaffold must not execute external data providers",
            )
        )
    if _TERRAFORM_PROVISIONER_RE.search(text):
        violations.append(
            _violation(
                path,
                "terraform_provisioner_block_forbidden",
                "scaffold must not execute Terraform provisioners",
            )
        )
    lowered = text.lower()
    for fragment in _FORBIDDEN_TERRAFORM_FRAGMENTS:
        if fragment in lowered:
            violations.append(
                _violation(
                    path,
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


def _render_versions_tf(package: ByocAwsIacPackage) -> str:
    return f"""terraform {{
  required_version = "{package.terraform.required_version}"

  required_providers {{
    aws = {{
      source  = "{package.terraform.provider_source}"
      version = ">= 5.0, < 6.0"
    }}
  }}
}}

provider "aws" {{
  region = var.{package.terraform.provider_region_variable}

  default_tags {{
    tags = local.required_tags
  }}
}}
"""


def _render_variables_tf(package: ByocAwsIacPackage) -> str:
    blocks: list[str] = []
    for name in package.safety.required_variables:
        description = _VARIABLE_DESCRIPTIONS.get(name, f"Required BYOC value {name}.")
        blocks.append(
            f"""variable "{name}" {{
  description = "{description}"
  type        = string
}}"""
        )
    return "\n\n".join(blocks) + "\n"


def _render_locals_tf(
    package: ByocAwsIacPackage,
    *,
    iam_template: ByocAwsIamTemplateSkeleton,
) -> str:
    tag_keys = package.safety.required_resource_tag_keys
    max_key_len = max(len(f'"{key}"') for key in tag_keys)
    tag_lines = []
    for key in tag_keys:
        expression = _TAG_VALUE_EXPRESSION.get(key)
        if expression is None:
            expression = f'"<set-{key.replace(":", "-")}>"'
        rendered_key = f'"{key}"'
        tag_lines.append(
            f"    {rendered_key}{' ' * (max_key_len - len(rendered_key))} = {expression}"
        )
    role_lines = "\n".join(f'    "{role.name}",' for role in iam_template.roles)
    return f"""locals {{
  required_tags = {{
{chr(10).join(tag_lines)}
  }}

  scaffold_contract = {{
    package_status                         = "{package.package_status}"
    customer_side_bootstrap_required       = {str(package.safety.customer_side_bootstrap_required).lower()}
    terraform_apply_allowed                = {str(package.execution.terraform_apply_allowed).lower()}
    control_plane_mutating_access_allowed  = {str(package.execution.control_plane_mutating_access_allowed).lower()}
    stores_remote_state_in_control_plane   = {str(package.execution.stores_remote_state_in_control_plane).lower()}
    no_inbound_control_plane_ports         = {str(package.execution.no_inbound_control_plane_ports).lower()}
    outbound_control_plane_port            = {package.safety.outbound_control_plane_port}
  }}

  expected_role_names = [
{role_lines}
  ]
}}
"""


def _render_main_tf(package: ByocAwsIacPackage) -> str:
    blocks: list[str] = []
    root_path = Path(package.terraform.root_module_path)
    for module in package.terraform.modules:
        module_source = Path(module.source_path).relative_to(root_path).as_posix()
        blocks.append(
            f"""module "{module.name}" {{
  source = "./{module_source}"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}}"""
        )
    return "\n\n".join(blocks) + "\n"


def _render_outputs_tf(package: ByocAwsIacPackage) -> str:
    module_lines = "\n".join(
        f"    {module.name} = module.{module.name}.module_contract"
        for module in package.terraform.modules
    )
    return """output "deployment_id" {
  description = "Deployment identifier accepted by this AWS BYOC scaffold."
  value       = var.deployment_id
}

output "customer_id" {
  description = "Customer identifier accepted by this AWS BYOC scaffold."
  value       = var.customer_id
}

output "required_tags" {
  description = "Tags every future customer-owned Fyralis resource must carry."
  value       = local.required_tags
}

output "scaffold_contract" {
  description = "Non-mutating safety contract enforced by the package validator."
  value       = local.scaffold_contract
}

output "expected_role_names" {
  description = "Runtime/IAM role names expected by the permissions manifest."
  value       = local.expected_role_names
}
""" + f"""
output "module_contracts" {{
  description = "Metadata-only contracts exposed by each non-mutating component module."
  value = {{
{module_lines}
  }}
}}
"""


def _render_module_contract_tf(module: ByocTerraformModule) -> str:
    return f"""variable "deployment_id" {{
  description = "Stable Fyralis BYOC deployment identifier."
  type        = string
}}

variable "customer_id" {{
  description = "Stable Fyralis customer identifier."
  type        = string
}}

variable "environment" {{
  description = "Deployment environment."
  type        = string
}}

variable "region" {{
  description = "AWS region for customer-owned data-plane resources."
  type        = string
}}

variable "aws_account_id" {{
  description = "Customer AWS account identifier that owns the data plane."
  type        = string
}}

variable "cloudformation_stack_prefix" {{
  description = "Customer-approved stack prefix for Fyralis BYOC resources."
  type        = string
}}

variable "permissions_boundary_policy_arn" {{
  description = "Customer-owned IAM permissions boundary applied to Fyralis roles."
  type        = string
}}

variable "required_tags" {{
  description = "Required customer-resource tags supplied by the root module."
  type        = map(string)
}}

locals {{
  module_contract = {{
    component                     = "{module.component}"
    scaffold_status               = "{module.scaffold_status}"
    deployment_id                 = var.deployment_id
    customer_id                   = var.customer_id
    environment                   = var.environment
    region                        = var.region
    aws_account_id                = var.aws_account_id
    cloudformation_stack_prefix   = var.cloudformation_stack_prefix
    permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
    resource_blocks_declared      = false
    mutating_actions_allowed      = false
    customer_data_inputs_allowed  = false
    sensitive_inputs_allowed      = false
    control_plane_inbound_allowed = false
    required_tag_keys             = sort(keys(var.required_tags))
  }}
}}

output "module_contract" {{
  description = "Metadata-only, non-mutating BYOC component module contract."
  value       = local.module_contract
}}
"""


def _path_string(path: Path) -> str:
    return path.as_posix()


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
    "generate_aws_iac_package",
    "load_byoc_aws_iac_package",
    "render_terraform_scaffold",
    "render_validation_errors",
    "validate_aws_iac_package_contract",
]
