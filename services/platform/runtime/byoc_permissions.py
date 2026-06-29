"""BYOC customer-cloud permission and IAM template contracts."""
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


PermissionActor = Literal[
    "customer_bootstrap_runner",
    "fyralis_control_plane",
    "customer_cloudformation",
    "data_plane_agent",
    "gateway_runtime",
    "worker_runtime",
    "migration_runner",
    "observability_runtime",
]
TrustBoundary = Literal[
    "customer_owned",
    "fyralis_control_plane_readonly",
    "aws_service",
    "workload_identity",
]
CustomerDataAccess = Literal[
    "none",
    "aggregate_metadata",
    "encrypted_customer_data",
    "secret_material",
]
ResourceScope = Literal[
    "account_metadata",
    "deployment_stack",
    "deployment_role",
    "deployment_secret_namespace",
    "deployment_kms_key",
    "deployment_object_bucket",
    "deployment_log_group",
    "deployment_metrics_namespace",
    "deployment_cluster",
    "deployment_network",
    "deployment_database",
    "deployment_broker",
    "deployment_cache",
]
ConditionOperator = Literal[
    "StringEquals",
    "StringLike",
    "StringLikeIfExists",
    "ArnLike",
]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:/@+=,-]{0,127}$")
_AWS_ACTION_RE = re.compile(r"^[a-z0-9-]+:[A-Za-z0-9*]+$")
_FORBIDDEN_MANAGED_POLICY_FRAGMENTS = (
    "AdministratorAccess",
    "PowerUserAccess",
    "IAMFullAccess",
)
_REQUIRED_ROLE_NAMES = frozenset(
    {
        "bootstrap_provisioner",
        "data_plane_agent",
        "gateway_runtime",
        "worker_runtime",
        "migration_runner",
    }
)
_DATA_PLANE_SECRET_ACTORS = frozenset(
    {
        "customer_bootstrap_runner",
        "data_plane_agent",
        "gateway_runtime",
        "worker_runtime",
        "migration_runner",
    }
)
_DATA_PLANE_CUSTOMER_DATA_ACTORS = frozenset(
    {
        "gateway_runtime",
        "worker_runtime",
        "migration_runner",
    }
)
_AWS_WILDCARD_RESOURCE_ACTIONS = frozenset(
    {
        "sts:GetCallerIdentity",
        "ec2:DescribeAvailabilityZones",
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeRouteTables",
        "ec2:DescribeVpcEndpoints",
        "eks:DescribeCluster",
        "rds:DescribeDBInstances",
        "elasticache:DescribeCacheClusters",
        "kafka:ListClustersV2",
        "tag:GetResources",
    }
)
_AWS_MUTATING_VERBS = (
    "Add",
    "Attach",
    "Authorize",
    "Create",
    "Delete",
    "Detach",
    "Disable",
    "Enable",
    "Execute",
    "Import",
    "Pass",
    "Put",
    "Remove",
    "Revoke",
    "Start",
    "Stop",
    "Tag",
    "Untag",
    "Update",
)
_REQUIRED_RESOURCE_TAG_KEYS = frozenset(
    {
        "fyralis:deployment-id",
        "fyralis:customer-id",
        "fyralis:managed",
        "fyralis:environment",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocPermissionPrinciples(_StrictModel):
    customer_side_bootstrap_required: Literal[True] = True
    control_plane_mutating_access_allowed: Literal[False] = False
    raw_customer_data_access_from_control_plane: Literal[False] = False
    permissions_boundary_required: Literal[True] = True
    wildcard_admin_actions_allowed: Literal[False] = False
    long_lived_static_keys_allowed: Literal[False] = False


class ByocAwsPermissionTemplate(_StrictModel):
    account_id: str
    partition: Literal["aws", "aws-us-gov"] = "aws"
    region: str
    cloudformation_stack_prefix: str
    permissions_boundary_policy_name: str
    required_resource_tag_keys: tuple[str, ...] = (
        "fyralis:deployment-id",
        "fyralis:customer-id",
        "fyralis:managed",
        "fyralis:environment",
    )
    skeleton_template_path: str | None = None

    @field_validator(
        "account_id",
        "region",
        "cloudformation_stack_prefix",
        "permissions_boundary_policy_name",
    )
    @classmethod
    def _nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("AWS permission template values must not be empty")
        return value

    @field_validator("required_resource_tag_keys")
    @classmethod
    def _tags_must_be_nonempty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("required_resource_tag_keys must not be empty")
        normalized = tuple(tag.strip() for tag in value)
        if any(not tag for tag in normalized):
            raise ValueError("required_resource_tag_keys must not contain blanks")
        return normalized


class ByocPermissionCondition(_StrictModel):
    operator: ConditionOperator
    key: str
    values: tuple[str, ...]

    @field_validator("key")
    @classmethod
    def _key_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("condition key must not be empty")
        return value

    @field_validator("values")
    @classmethod
    def _values_must_be_present(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("condition values must not be empty")
        normalized = tuple(item.strip() for item in value)
        if any(not item for item in normalized):
            raise ValueError("condition values must not contain blanks")
        return normalized


class ByocPermissionGrant(_StrictModel):
    sid: str
    effect: Literal["Allow"] = "Allow"
    actions: tuple[str, ...]
    resource_refs: tuple[str, ...]
    resource_scope: ResourceScope
    customer_data_access: CustomerDataAccess = "none"
    conditions: tuple[ByocPermissionCondition, ...] = ()

    @field_validator("sid")
    @classmethod
    def _sid_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _SAFE_NAME_RE.match(value):
            raise ValueError("sid must be a bounded identifier")
        return value

    @field_validator("actions")
    @classmethod
    def _actions_must_be_valid(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("actions must not be empty")
        normalized = tuple(action.strip() for action in value)
        if any(not _AWS_ACTION_RE.match(action) for action in normalized):
            raise ValueError("actions must look like service:Action")
        return normalized

    @field_validator("resource_refs")
    @classmethod
    def _resources_must_be_present(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("resource_refs must not be empty")
        normalized = tuple(resource.strip() for resource in value)
        if any(not resource for resource in normalized):
            raise ValueError("resource_refs must not contain blanks")
        return normalized


class ByocPermissionRole(_StrictModel):
    name: str
    actor: PermissionActor
    trust_boundary: TrustBoundary
    trusted_principal_ref: str
    permissions_boundary_required: Literal[True] = True
    permissions_boundary_ref: str
    max_session_duration_seconds: int = Field(default=3600, ge=900, le=43200)
    managed_policy_arns: tuple[str, ...] = ()
    grants: tuple[ByocPermissionGrant, ...]

    @field_validator("name", "trusted_principal_ref", "permissions_boundary_ref")
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("role fields must not be empty")
        return value

    @field_validator("grants")
    @classmethod
    def _grants_must_be_present(
        cls,
        value: tuple[ByocPermissionGrant, ...],
    ) -> tuple[ByocPermissionGrant, ...]:
        if not value:
            raise ValueError("grants must not be empty")
        return value


class ByocPermissionsManifest(_StrictModel):
    schema_version: Literal["fyralis.byoc.permissions.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    principles: ByocPermissionPrinciples
    aws: ByocAwsPermissionTemplate | None = None
    roles: tuple[ByocPermissionRole, ...]

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
    def _top_level_strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be empty")
        return value

    @field_validator("roles")
    @classmethod
    def _roles_must_be_present(
        cls,
        value: tuple[ByocPermissionRole, ...],
    ) -> tuple[ByocPermissionRole, ...]:
        if not value:
            raise ValueError("roles must not be empty")
        return value


class ByocAwsIamTemplateRole(_StrictModel):
    name: str
    actor: PermissionActor
    trust_boundary: TrustBoundary
    permissions_boundary_ref: str
    policy_grant_sids: tuple[str, ...]

    @field_validator("name", "permissions_boundary_ref")
    @classmethod
    def _role_fields_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("AWS IAM template role fields must not be empty")
        return value

    @field_validator("policy_grant_sids")
    @classmethod
    def _sids_must_be_present(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("policy_grant_sids must not be empty")
        return value


class ByocAwsIamTemplateSkeleton(_StrictModel):
    schema_version: Literal["fyralis.byoc.aws.iam_skeleton.v1"]
    deployment_id: str
    customer_id: str
    cloud_provider: Literal["aws"]
    region: str
    consumes_permissions_manifest: str
    stack_name: str
    roles: tuple[ByocAwsIamTemplateRole, ...]

    @field_validator(
        "deployment_id",
        "customer_id",
        "region",
        "consumes_permissions_manifest",
        "stack_name",
    )
    @classmethod
    def _strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("AWS IAM template fields must not be empty")
        return value

    @field_validator("roles")
    @classmethod
    def _roles_must_be_present(
        cls,
        value: tuple[ByocAwsIamTemplateRole, ...],
    ) -> tuple[ByocAwsIamTemplateRole, ...]:
        if not value:
            raise ValueError("roles must not be empty")
        return value


@dataclass(frozen=True, slots=True)
class ByocPermissionContractViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def validate_permissions_manifest_contract(
    manifest: ByocPermissionsManifest,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
) -> list[ByocPermissionContractViolation]:
    violations: list[ByocPermissionContractViolation] = []

    if dataplane_manifest is not None:
        violations.extend(_compare_dataplane_manifest(manifest, dataplane_manifest))

    if manifest.cloud_provider != "aws":
        violations.append(
            _violation(
                "cloud_provider",
                "unsupported_provider",
                "only the AWS BYOC permission contract is implemented locally",
            )
        )
    if manifest.cloud_provider == "aws" and manifest.aws is None:
        violations.append(
            _violation("aws", "aws_contract_required", "aws block is required")
        )
    if manifest.aws is not None:
        missing_tags = _REQUIRED_RESOURCE_TAG_KEYS - set(
            manifest.aws.required_resource_tag_keys
        )
        for tag in sorted(missing_tags):
            violations.append(
                _violation(
                    "aws.required_resource_tag_keys",
                    "missing_required_tag",
                    f"{tag!r} must be required on BYOC-managed resources",
                )
            )

    role_names = [role.name for role in manifest.roles]
    duplicates = sorted({name for name in role_names if role_names.count(name) > 1})
    for name in duplicates:
        violations.append(
            _violation("roles", "duplicate_role", f"{name!r} is listed more than once")
        )
    missing_roles = _REQUIRED_ROLE_NAMES - set(role_names)
    for name in sorted(missing_roles):
        violations.append(
            _violation(
                "roles",
                "missing_required_role",
                f"{name!r} is required for the BYOC permission contract",
            )
        )

    for role in manifest.roles:
        violations.extend(_validate_role_contract(role, manifest))

    return violations


def validate_aws_iam_template_contract(
    template: ByocAwsIamTemplateSkeleton,
    *,
    permissions_manifest: ByocPermissionsManifest,
) -> list[ByocPermissionContractViolation]:
    violations: list[ByocPermissionContractViolation] = []

    for field in ("deployment_id", "customer_id", "region"):
        if getattr(template, field) != getattr(permissions_manifest, field):
            violations.append(
                _violation(
                    field,
                    "template_manifest_mismatch",
                    f"AWS IAM template {field} does not match permissions manifest",
                )
            )
    role_by_name = {role.name: role for role in permissions_manifest.roles}
    seen_names: set[str] = set()
    for role in template.roles:
        if role.name in seen_names:
            violations.append(
                _violation(
                    "roles",
                    "duplicate_template_role",
                    f"{role.name!r} is listed more than once",
                )
            )
            continue
        seen_names.add(role.name)
        manifest_role = role_by_name.get(role.name)
        if manifest_role is None:
            violations.append(
                _violation(
                    f"roles.{role.name}",
                    "unknown_template_role",
                    "AWS IAM template role is not present in the permissions manifest",
                )
            )
            continue
        if role.actor != manifest_role.actor:
            violations.append(
                _violation(
                    f"roles.{role.name}.actor",
                    "template_role_actor_mismatch",
                    "AWS IAM template role actor does not match the manifest",
                )
            )
        if role.trust_boundary != manifest_role.trust_boundary:
            violations.append(
                _violation(
                    f"roles.{role.name}.trust_boundary",
                    "template_trust_boundary_mismatch",
                    "AWS IAM template trust boundary does not match the manifest",
                )
            )
        if role.permissions_boundary_ref != manifest_role.permissions_boundary_ref:
            violations.append(
                _violation(
                    f"roles.{role.name}.permissions_boundary_ref",
                    "template_permissions_boundary_mismatch",
                    "AWS IAM template boundary ref does not match the manifest",
                )
            )
        manifest_sids = {grant.sid for grant in manifest_role.grants}
        for sid in role.policy_grant_sids:
            if sid not in manifest_sids:
                violations.append(
                    _violation(
                        f"roles.{role.name}.policy_grant_sids",
                        "unknown_policy_grant_sid",
                        f"{sid!r} is not declared on the manifest role",
                    )
                )

    missing_template_roles = set(role_by_name) - seen_names
    for role_name in sorted(missing_template_roles):
        violations.append(
            _violation(
                "roles",
                "missing_template_role",
                f"{role_name!r} is missing from the AWS IAM skeleton",
            )
        )

    return violations


def byoc_permissions_json_schema() -> dict[str, Any]:
    return ByocPermissionsManifest.model_json_schema()


def byoc_aws_iam_template_json_schema() -> dict[str, Any]:
    return ByocAwsIamTemplateSkeleton.model_json_schema()


def load_byoc_permissions_manifest(path: Path) -> ByocPermissionsManifest:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC permissions manifest must be a JSON/YAML object")
    return ByocPermissionsManifest.model_validate(data)


def load_byoc_aws_iam_template(path: Path) -> ByocAwsIamTemplateSkeleton:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC AWS IAM template must be a JSON/YAML object")
    return ByocAwsIamTemplateSkeleton.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _validate_role_contract(
    role: ByocPermissionRole,
    manifest: ByocPermissionsManifest,
) -> list[ByocPermissionContractViolation]:
    violations: list[ByocPermissionContractViolation] = []

    for policy_arn in role.managed_policy_arns:
        if any(fragment in policy_arn for fragment in _FORBIDDEN_MANAGED_POLICY_FRAGMENTS):
            violations.append(
                _violation(
                    f"roles.{role.name}.managed_policy_arns",
                    "forbidden_managed_policy",
                    "admin-style AWS managed policies are forbidden",
                )
            )

    for grant in role.grants:
        for action in grant.actions:
            if action == "*" or action.endswith(":*") or "*" in action:
                violations.append(
                    _violation(
                        f"roles.{role.name}.{grant.sid}.actions",
                        "wildcard_action_forbidden",
                        "BYOC permission actions must be explicit",
                    )
                )
        if "*" in grant.resource_refs and not _wildcard_resource_is_allowed(grant):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.resource_refs",
                    "wildcard_resource_forbidden",
                    "wildcard resources are allowed only for read-only account preflight",
                )
            )
        if (
            "*" in grant.resource_refs
            and _wildcard_resource_is_allowed(grant)
            and set(grant.actions) != {"sts:GetCallerIdentity"}
            and not _has_condition_value(grant, "aws:RequestedRegion", manifest.region)
        ):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.conditions",
                    "wildcard_resource_region_condition_required",
                    "regional wildcard-resource preflight must constrain region",
                )
            )
        if _grant_is_mutating(grant):
            if role.actor == "fyralis_control_plane":
                violations.append(
                    _violation(
                        f"roles.{role.name}.{grant.sid}",
                        "control_plane_mutation_forbidden",
                        "Fyralis control plane roles must remain read-only",
                    )
                )
            if not _grant_is_deployment_scoped(grant, manifest):
                violations.append(
                    _violation(
                        f"roles.{role.name}.{grant.sid}",
                        "mutating_grant_not_deployment_scoped",
                        "mutating grants must be scoped to deployment resources or tags",
                    )
                )
        if "iam:PassRole" in grant.actions and not _has_condition_key(
            grant,
            "iam:PassedToService",
        ):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.conditions",
                    "pass_role_service_condition_required",
                    "iam:PassRole grants must constrain iam:PassedToService",
                )
            )
        if (
            grant.customer_data_access == "secret_material"
            and role.actor not in _DATA_PLANE_SECRET_ACTORS
        ):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.customer_data_access",
                    "secret_access_not_data_plane",
                    "secret material access is allowed only inside the data plane",
                )
            )
        if (
            grant.customer_data_access == "encrypted_customer_data"
            and role.actor not in _DATA_PLANE_CUSTOMER_DATA_ACTORS
        ):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.customer_data_access",
                    "customer_data_access_not_runtime",
                    "customer data access is allowed only for runtime data-plane roles",
                )
            )
        if (
            role.actor == "fyralis_control_plane"
            and grant.customer_data_access != "none"
        ):
            violations.append(
                _violation(
                    f"roles.{role.name}.{grant.sid}.customer_data_access",
                    "control_plane_customer_data_access_forbidden",
                    "control-plane roles must not access customer data or secrets",
                )
            )

    return violations


def _compare_dataplane_manifest(
    manifest: ByocPermissionsManifest,
    dataplane_manifest: ByocDataPlaneManifest,
) -> list[ByocPermissionContractViolation]:
    violations: list[ByocPermissionContractViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(manifest, field) != getattr(dataplane_manifest, field):
            violations.append(
                _violation(
                    field,
                    "dataplane_manifest_mismatch",
                    f"permissions manifest {field} does not match data-plane manifest",
                )
            )
    return violations


def _grant_is_mutating(grant: ByocPermissionGrant) -> bool:
    return any(_action_is_mutating(action) for action in grant.actions)


def _action_is_mutating(action: str) -> bool:
    try:
        _, name = action.split(":", 1)
    except ValueError:
        return True
    return name.startswith(_AWS_MUTATING_VERBS)


def _wildcard_resource_is_allowed(grant: ByocPermissionGrant) -> bool:
    return (
        grant.customer_data_access == "none"
        and grant.resource_scope == "account_metadata"
        and set(grant.actions) <= _AWS_WILDCARD_RESOURCE_ACTIONS
    )


def _grant_is_deployment_scoped(
    grant: ByocPermissionGrant,
    manifest: ByocPermissionsManifest,
) -> bool:
    deployment_tokens = {
        manifest.deployment_id,
        manifest.deployment_id.replace("_", "-"),
        "<DEPLOYMENT_ID>",
    }
    if any(
        token in resource
        for resource in grant.resource_refs
        for token in deployment_tokens
    ):
        return True
    return _has_condition_key(grant, "aws:RequestTag/fyralis:deployment-id") or (
        _has_condition_key(grant, "aws:ResourceTag/fyralis:deployment-id")
    )


def _has_condition_key(grant: ByocPermissionGrant, key: str) -> bool:
    return any(condition.key == key for condition in grant.conditions)


def _has_condition_value(grant: ByocPermissionGrant, key: str, value: str) -> bool:
    return any(
        condition.key == key and value in condition.values
        for condition in grant.conditions
    )


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocPermissionContractViolation:
    return ByocPermissionContractViolation(path=path, code=code, message=message)


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


__all__ = [
    "ByocAwsIamTemplateSkeleton",
    "ByocPermissionContractViolation",
    "ByocPermissionsManifest",
    "byoc_aws_iam_template_json_schema",
    "byoc_permissions_json_schema",
    "load_byoc_aws_iam_template",
    "load_byoc_permissions_manifest",
    "render_validation_errors",
    "validate_aws_iam_template_contract",
    "validate_permissions_manifest_contract",
]
