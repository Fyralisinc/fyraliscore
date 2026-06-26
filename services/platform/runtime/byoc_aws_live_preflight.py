"""Read-only AWS live preflight checks for BYOC customer data planes.

This module is meant to run inside the customer boundary with customer-owned
AWS credentials. It verifies that the selected AWS identity and optional IAM
simulation are aligned with the BYOC manifests, while emitting only sanitized
status metadata. Account IDs, ARNs, profile names, AWS endpoint URLs, policy
documents, credentials, command output, and customer data are intentionally
excluded from the report.
"""
from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    load_byoc_manifest,
    render_validation_errors as render_dataplane_validation_errors,
    validate_byoc_manifest_contract,
)
from services.platform.runtime.byoc_permissions import (
    ByocAwsIamTemplateSkeleton,
    ByocPermissionCondition,
    ByocPermissionGrant,
    ByocPermissionsManifest,
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
    render_validation_errors as render_permissions_validation_errors,
    validate_aws_iam_template_contract,
    validate_permissions_manifest_contract,
)


AwsLivePreflightStatus = Literal["pass", "fail", "skipped"]
AwsLivePreflightMode = Literal["customer_side_live"]
AwsClientFactory = Callable[[str, str | None, str | None], Any]

_PASS: AwsLivePreflightStatus = "pass"
_FAIL: AwsLivePreflightStatus = "fail"
_SKIPPED: AwsLivePreflightStatus = "skipped"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_ACCOUNT_ID_RE = re.compile(r"\b\d{12}\b")
_ACCESS_KEY_RE = re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b")
_FORBIDDEN_TEXT_FRAGMENTS = (
    "arn:",
    "://",
    "access_key",
    "authorization",
    "aws_secret_access_key",
    "bearer ",
    "password",
    "policy_document",
    "secret",
    "session_token",
    "token",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocAwsLivePreflightPrivacyContract(_StrictModel):
    account_id_included: Literal[False] = False
    caller_arn_included: Literal[False] = False
    role_arn_included: Literal[False] = False
    aws_profile_included: Literal[False] = False
    aws_endpoint_urls_included: Literal[False] = False
    credentials_included: Literal[False] = False
    command_output_included: Literal[False] = False
    policy_documents_included: Literal[False] = False
    raw_customer_data_included: Literal[False] = False


class ByocAwsLivePreflightCheck(_StrictModel):
    name: str
    status: AwsLivePreflightStatus
    required: bool
    details: str
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("AWS live preflight check names must be bounded")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("AWS live preflight details must be bounded")
        _reject_sensitive_text(value)
        return value

    @field_validator("metrics")
    @classmethod
    def _metrics_must_be_safe(
        cls,
        value: dict[str, int | bool | str],
    ) -> dict[str, int | bool | str]:
        normalized: dict[str, int | bool | str] = {}
        for key, metric in value.items():
            key = key.strip()
            if not key or not _SAFE_CODE_RE.match(key):
                raise ValueError("AWS live preflight metric names must be bounded")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120:
                    raise ValueError("AWS live preflight string metrics must be bounded")
                _reject_sensitive_text(metric)
            normalized[key] = metric
        return normalized


class ByocAwsLivePreflightReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.aws_live_preflight.v1"]
    status: AwsLivePreflightStatus
    required_checks_passed: bool
    execution_mode: AwsLivePreflightMode = "customer_side_live"
    elapsed_seconds: float = Field(ge=0)
    dataplane_manifest_path: str
    permissions_manifest_path: str
    iam_template_path: str | None = None
    deployment_id: str | None = None
    customer_id: str | None = None
    environment: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    live_aws_api_calls_executed: bool = False
    cloud_credentials_required: bool = True
    mutating_aws_api_calls_executed: Literal[False] = False
    iam_policy_simulation_requested: bool = False
    iam_policy_simulation_executed: bool = False
    checked_permission_evaluation_count: int = Field(default=0, ge=0)
    denied_permission_evaluation_count: int = Field(default=0, ge=0)
    readonly_api_probe_requested: bool = False
    readonly_api_probe_executed: bool = False
    privacy: ByocAwsLivePreflightPrivacyContract
    checks: tuple[ByocAwsLivePreflightCheck, ...]

    def as_json(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    @field_validator(
        "dataplane_manifest_path",
        "permissions_manifest_path",
        "iam_template_path",
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    )
    @classmethod
    def _strings_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("AWS live preflight report fields must be bounded")
        _reject_sensitive_text(value)
        return value


@dataclass(frozen=True, slots=True)
class ByocAwsLivePreflightInputs:
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path | None = None
    aws_profile: str | None = None
    aws_region: str | None = None
    expected_account_id: str | None = None
    skip_live_aws: bool = False
    run_readonly_api_probes: bool = False
    run_iam_policy_simulation: bool = False
    simulation_principal_arn: str | None = None
    simulation_role_name: str = "bootstrap_provisioner"
    aws_client_factory: AwsClientFactory | None = None
    aws_connect_timeout_seconds: int = 3
    aws_read_timeout_seconds: int = 5


def run_byoc_aws_live_preflight(
    inputs: ByocAwsLivePreflightInputs,
) -> ByocAwsLivePreflightReport:
    started = time.monotonic()
    checks: list[ByocAwsLivePreflightCheck] = []

    dataplane, dataplane_checks = _load_dataplane(inputs.dataplane_manifest_path)
    permissions, permissions_checks = _load_permissions(inputs.permissions_manifest_path)
    iam_template, iam_checks = _load_iam_template(inputs.iam_template_path)
    checks.extend(dataplane_checks)
    checks.extend(permissions_checks)
    checks.extend(iam_checks)

    if dataplane is not None:
        checks.append(_dataplane_contract_check(dataplane))
    if permissions is not None:
        checks.append(_permissions_provider_check(permissions))
    if dataplane is not None and permissions is not None:
        checks.append(_permissions_alignment_check(permissions, dataplane))
    if permissions is not None and iam_template is not None:
        checks.append(_iam_template_alignment_check(iam_template, permissions))

    expected_account_id = _expected_account_id(inputs, permissions)
    region = _expected_region(inputs, dataplane, permissions)

    if inputs.skip_live_aws:
        checks.append(
            _check(
                "aws_sts_identity",
                _SKIPPED,
                required=False,
                details="Live AWS identity check was not requested.",
                metrics={
                    "aws_api_call_executed": False,
                    "account_id_expected": expected_account_id is not None,
                },
            )
        )
        checks.append(_readonly_probe_skipped(inputs))
        checks.append(_iam_simulation_skipped(inputs))
    else:
        checks.append(_sts_identity_check(inputs, region, expected_account_id))
        checks.extend(_readonly_probe_checks(inputs, region))
        checks.append(_iam_simulation_check(inputs, permissions, region))

    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    simulation_check = _find_check(checks, "iam_policy_simulation")
    report = ByocAwsLivePreflightReport(
        schema_version="fyralis.byoc.aws_live_preflight.v1",
        status=_PASS if required_checks_passed else _FAIL,
        required_checks_passed=required_checks_passed,
        execution_mode="customer_side_live",
        elapsed_seconds=round(time.monotonic() - started, 3),
        dataplane_manifest_path=str(inputs.dataplane_manifest_path),
        permissions_manifest_path=str(inputs.permissions_manifest_path),
        iam_template_path=(
            str(inputs.iam_template_path) if inputs.iam_template_path is not None else None
        ),
        deployment_id=_identity_value("deployment_id", dataplane, permissions),
        customer_id=_identity_value("customer_id", dataplane, permissions),
        environment=_identity_value("environment", dataplane, permissions),
        cloud_provider=_identity_value("cloud_provider", dataplane, permissions),
        region=region,
        artifact_revision=_identity_value("artifact_revision", dataplane, permissions),
        live_aws_api_calls_executed=any(
            check.metrics.get("aws_api_call_executed") is True for check in checks
        ),
        cloud_credentials_required=not inputs.skip_live_aws,
        mutating_aws_api_calls_executed=False,
        iam_policy_simulation_requested=inputs.run_iam_policy_simulation,
        iam_policy_simulation_executed=(
            simulation_check is not None
            and simulation_check.metrics.get("aws_api_call_executed") is True
        ),
        checked_permission_evaluation_count=_metric_int(
            simulation_check,
            "evaluation_count",
        ),
        denied_permission_evaluation_count=_metric_int(
            simulation_check,
            "denied_evaluation_count",
        ),
        readonly_api_probe_requested=inputs.run_readonly_api_probes,
        readonly_api_probe_executed=any(
            check.name.startswith("readonly_api_probe.")
            and check.metrics.get("aws_api_call_executed") is True
            for check in checks
        ),
        privacy=ByocAwsLivePreflightPrivacyContract(),
        checks=tuple(checks),
    )
    _assert_report_is_sanitized(report, sensitive_values=(expected_account_id,))
    return report


def render_aws_live_preflight_json(report: ByocAwsLivePreflightReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_aws_live_preflight_yaml(report: ByocAwsLivePreflightReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _load_dataplane(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocAwsLivePreflightCheck]]:
    try:
        manifest = load_byoc_manifest(path)
    except ValidationError as exc:
        return None, [_schema_error_check("dataplane_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_error_check("dataplane_manifest_schema", exc)]
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
) -> tuple[ByocPermissionsManifest | None, list[ByocAwsLivePreflightCheck]]:
    try:
        manifest = load_byoc_permissions_manifest(path)
    except ValidationError as exc:
        return None, [_schema_error_check("permissions_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_error_check("permissions_manifest_schema", exc)]
    return manifest, [
        _check(
            "permissions_manifest_schema",
            _PASS,
            required=True,
            details="BYOC permissions manifest schema is valid.",
        )
    ]


def _load_iam_template(
    path: Path | None,
) -> tuple[ByocAwsIamTemplateSkeleton | None, list[ByocAwsLivePreflightCheck]]:
    if path is None:
        return None, [
            _check(
                "aws_iam_template_schema",
                _SKIPPED,
                required=False,
                details="No AWS IAM skeleton was supplied.",
            )
        ]
    try:
        template = load_byoc_aws_iam_template(path)
    except ValidationError as exc:
        return None, [_schema_error_check("aws_iam_template_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_error_check("aws_iam_template_schema", exc)]
    return template, [
        _check(
            "aws_iam_template_schema",
            _PASS,
            required=True,
            details="BYOC AWS IAM skeleton schema is valid.",
        )
    ]


def _dataplane_contract_check(
    manifest: ByocDataPlaneManifest,
) -> ByocAwsLivePreflightCheck:
    violations = validate_byoc_manifest_contract(manifest)
    return _check(
        "dataplane_manifest_contract",
        _FAIL if violations else _PASS,
        required=True,
        details=(
            "BYOC data-plane manifest has bounded contract violations."
            if violations
            else "BYOC data-plane manifest contract passed."
        ),
        metrics={"violation_count": len(violations)},
    )


def _permissions_provider_check(
    manifest: ByocPermissionsManifest,
) -> ByocAwsLivePreflightCheck:
    ok = manifest.cloud_provider == "aws" and manifest.aws is not None
    return _check(
        "aws_permission_contract_present",
        _PASS if ok else _FAIL,
        required=True,
        details=(
            "AWS permission contract is present."
            if ok
            else "AWS permission contract is missing or unsupported."
        ),
        metrics={
            "aws_block_present": manifest.aws is not None,
            "cloud_provider_is_aws": manifest.cloud_provider == "aws",
        },
    )


def _permissions_alignment_check(
    permissions: ByocPermissionsManifest,
    dataplane: ByocDataPlaneManifest,
) -> ByocAwsLivePreflightCheck:
    violations = validate_permissions_manifest_contract(
        permissions,
        dataplane_manifest=dataplane,
    )
    return _check(
        "permissions_manifest_contract",
        _FAIL if violations else _PASS,
        required=True,
        details=(
            "BYOC permissions manifest has bounded contract violations."
            if violations
            else "BYOC permissions manifest contract passed."
        ),
        metrics={
            "role_count": len(permissions.roles),
            "violation_count": len(violations),
        },
    )


def _iam_template_alignment_check(
    template: ByocAwsIamTemplateSkeleton,
    permissions: ByocPermissionsManifest,
) -> ByocAwsLivePreflightCheck:
    violations = validate_aws_iam_template_contract(
        template,
        permissions_manifest=permissions,
    )
    return _check(
        "aws_iam_template_contract",
        _FAIL if violations else _PASS,
        required=True,
        details=(
            "BYOC AWS IAM skeleton has bounded contract violations."
            if violations
            else "BYOC AWS IAM skeleton contract passed."
        ),
        metrics={
            "template_role_count": len(template.roles),
            "violation_count": len(violations),
        },
    )


def _sts_identity_check(
    inputs: ByocAwsLivePreflightInputs,
    region: str | None,
    expected_account_id: str | None,
) -> ByocAwsLivePreflightCheck:
    try:
        sts = _aws_client(inputs, "sts", region)
        response = sts.get_caller_identity()
    except Exception as exc:  # noqa: BLE001
        return _aws_exception_check(
            "aws_sts_identity",
            exc,
            required=True,
            executed=True,
            requested=True,
        )

    actual_account = _string_or_none(response.get("Account"))
    actual_arn = _string_or_none(response.get("Arn"))
    account_matches = (
        expected_account_id is None or actual_account == expected_account_id
    )
    partition_matches = _partition_matches(expected_account_id, actual_arn, inputs)
    passed = actual_account is not None and account_matches and partition_matches
    return _check(
        "aws_sts_identity",
        _PASS if passed else _FAIL,
        required=True,
        details=(
            "AWS caller identity matched the BYOC account contract."
            if passed
            else "AWS caller identity did not match the BYOC account contract."
        ),
        metrics={
            "aws_api_call_executed": True,
            "account_id_expected": expected_account_id is not None,
            "account_id_present": actual_account is not None,
            "account_id_matches_expected": account_matches,
            "caller_arn_present": actual_arn is not None,
            "partition_matches_expected": partition_matches,
        },
    )


def _readonly_probe_checks(
    inputs: ByocAwsLivePreflightInputs,
    region: str | None,
) -> list[ByocAwsLivePreflightCheck]:
    if not inputs.run_readonly_api_probes:
        return [_readonly_probe_skipped(inputs)]
    probes = (
        (
            "readonly_api_probe.ec2_availability_zones",
            "ec2",
            "describe_availability_zones",
            {"AllAvailabilityZones": False},
        ),
        (
            "readonly_api_probe.ec2_vpcs",
            "ec2",
            "describe_vpcs",
            {"MaxResults": 5},
        ),
        (
            "readonly_api_probe.tag_resources",
            "resourcegroupstaggingapi",
            "get_resources",
            {"ResourcesPerPage": 1},
        ),
    )
    checks: list[ByocAwsLivePreflightCheck] = []
    for name, service, method_name, kwargs in probes:
        try:
            client = _aws_client(inputs, service, region)
            getattr(client, method_name)(**kwargs)
        except Exception as exc:  # noqa: BLE001
            checks.append(
                _aws_exception_check(
                    name,
                    exc,
                    required=True,
                    executed=True,
                    requested=True,
                )
            )
            continue
        checks.append(
            _check(
                name,
                _PASS,
                required=True,
                details="Read-only AWS API probe completed.",
                metrics={
                    "aws_api_call_executed": True,
                    "requested": True,
                    "service": service,
                },
            )
        )
    return checks


def _readonly_probe_skipped(
    inputs: ByocAwsLivePreflightInputs,
) -> ByocAwsLivePreflightCheck:
    del inputs
    return _check(
        "readonly_api_probe",
        _SKIPPED,
        required=False,
        details="Read-only AWS API probes were not requested.",
        metrics={"requested": False, "aws_api_call_executed": False},
    )


def _iam_simulation_check(
    inputs: ByocAwsLivePreflightInputs,
    permissions: ByocPermissionsManifest | None,
    region: str | None,
) -> ByocAwsLivePreflightCheck:
    if not inputs.run_iam_policy_simulation:
        return _iam_simulation_skipped(inputs)
    if not inputs.simulation_principal_arn:
        return _check(
            "iam_policy_simulation",
            _FAIL,
            required=True,
            details="IAM policy simulation was requested without a principal.",
            metrics={
                "requested": True,
                "aws_api_call_executed": False,
                "principal_supplied": False,
            },
        )
    if permissions is None:
        return _check(
            "iam_policy_simulation",
            _FAIL,
            required=True,
            details="IAM policy simulation requires a valid permissions manifest.",
            metrics={"requested": True, "aws_api_call_executed": False},
        )
    role = next(
        (role for role in permissions.roles if role.name == inputs.simulation_role_name),
        None,
    )
    if role is None:
        return _check(
            "iam_policy_simulation",
            _FAIL,
            required=True,
            details="IAM policy simulation role is missing from the manifest.",
            metrics={
                "requested": True,
                "aws_api_call_executed": False,
                "role_found": False,
            },
        )

    try:
        iam = _aws_client(inputs, "iam", region)
        result = _simulate_grants(
            iam,
            principal_arn=inputs.simulation_principal_arn,
            grants=role.grants,
        )
    except Exception as exc:  # noqa: BLE001
        return _aws_exception_check(
            "iam_policy_simulation",
            exc,
            required=True,
            executed=True,
            requested=True,
        )

    denied = result["denied_evaluation_count"]
    evaluation_count = result["evaluation_count"]
    passed = evaluation_count > 0 and denied == 0
    return _check(
        "iam_policy_simulation",
        _PASS if passed else _FAIL,
        required=True,
        details=(
            "IAM policy simulation matched the manifest grants."
            if passed
            else "IAM policy simulation found denied manifest grants."
        ),
        metrics={
            "requested": True,
            "aws_api_call_executed": True,
            "role_found": True,
            "grant_count": len(role.grants),
            "action_count": result["action_count"],
            "evaluation_count": evaluation_count,
            "allowed_evaluation_count": result["allowed_evaluation_count"],
            "denied_evaluation_count": denied,
        },
    )


def _iam_simulation_skipped(
    inputs: ByocAwsLivePreflightInputs,
) -> ByocAwsLivePreflightCheck:
    del inputs
    return _check(
        "iam_policy_simulation",
        _SKIPPED,
        required=False,
        details="IAM policy simulation was not requested.",
        metrics={"requested": False, "aws_api_call_executed": False},
    )


def _simulate_grants(
    iam: Any,
    *,
    principal_arn: str,
    grants: Sequence[ByocPermissionGrant],
) -> dict[str, int]:
    action_count = 0
    evaluation_count = 0
    allowed = 0
    denied = 0
    for grant in grants:
        action_count += len(grant.actions)
        params: dict[str, Any] = {
            "PolicySourceArn": principal_arn,
            "ActionNames": list(grant.actions),
            "ResourceArns": list(grant.resource_refs),
        }
        context_entries = _context_entries(grant.conditions)
        if context_entries:
            params["ContextEntries"] = context_entries
        while True:
            response = iam.simulate_principal_policy(**params)
            for item in response.get("EvaluationResults", []):
                evaluation_count += 1
                if item.get("EvalDecision") == "allowed":
                    allowed += 1
                else:
                    denied += 1
            next_token = response.get("NextToken")
            if not next_token and response.get("IsTruncated"):
                next_token = response.get("Marker")
            if not next_token:
                break
            params["Marker"] = next_token
    return {
        "action_count": action_count,
        "evaluation_count": evaluation_count,
        "allowed_evaluation_count": allowed,
        "denied_evaluation_count": denied,
    }


def _context_entries(
    conditions: Sequence[ByocPermissionCondition],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for condition in conditions:
        entries.append(
            {
                "ContextKeyName": condition.key,
                "ContextKeyValues": list(condition.values),
                "ContextKeyType": "string",
            }
        )
    return entries


def _aws_client(
    inputs: ByocAwsLivePreflightInputs,
    service: str,
    region: str | None,
) -> Any:
    if inputs.aws_client_factory is not None:
        return inputs.aws_client_factory(service, inputs.aws_profile, region)
    try:
        import boto3  # type: ignore[import-untyped]
        from botocore.config import Config  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dependency exists in test env.
        raise RuntimeError("boto3/botocore is required for AWS live preflight") from exc
    session = boto3.Session(profile_name=inputs.aws_profile, region_name=region)
    return session.client(
        service,
        region_name=region,
        config=Config(
            connect_timeout=inputs.aws_connect_timeout_seconds,
            read_timeout=inputs.aws_read_timeout_seconds,
            retries={"max_attempts": 1},
        ),
    )


def _aws_exception_check(
    name: str,
    exc: Exception,
    *,
    required: bool,
    executed: bool,
    requested: bool,
) -> ByocAwsLivePreflightCheck:
    return _check(
        name,
        _FAIL if required else _SKIPPED,
        required=required,
        details="AWS read-only preflight call failed without retaining AWS output.",
        metrics={
            "requested": requested,
            "aws_api_call_executed": executed,
            "error_type": type(exc).__name__,
        },
    )


def _schema_error_check(
    name: str,
    exc: ValidationError,
) -> ByocAwsLivePreflightCheck:
    if name.startswith("dataplane"):
        error_count = len(render_dataplane_validation_errors(exc))
    else:
        error_count = len(render_permissions_validation_errors(exc))
    return _check(
        name,
        _FAIL,
        required=True,
        details="BYOC manifest schema validation failed.",
        metrics={"error_count": error_count},
    )


def _load_error_check(name: str, exc: Exception) -> ByocAwsLivePreflightCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="BYOC manifest could not be loaded.",
        metrics={"error_type": type(exc).__name__},
    )


def _check(
    name: str,
    status: AwsLivePreflightStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocAwsLivePreflightCheck:
    return ByocAwsLivePreflightCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _expected_account_id(
    inputs: ByocAwsLivePreflightInputs,
    permissions: ByocPermissionsManifest | None,
) -> str | None:
    if inputs.expected_account_id:
        return inputs.expected_account_id.strip()
    if permissions is not None and permissions.aws is not None:
        return permissions.aws.account_id
    return None


def _expected_region(
    inputs: ByocAwsLivePreflightInputs,
    dataplane: ByocDataPlaneManifest | None,
    permissions: ByocPermissionsManifest | None,
) -> str | None:
    if inputs.aws_region:
        return inputs.aws_region.strip()
    if dataplane is not None:
        return dataplane.region
    if permissions is not None:
        return permissions.region
    return None


def _partition_matches(
    expected_account_id: str | None,
    actual_arn: str | None,
    inputs: ByocAwsLivePreflightInputs,
) -> bool:
    del expected_account_id
    if actual_arn is None:
        return False
    parts = actual_arn.split(":", 2)
    if len(parts) < 2 or parts[0] != "arn":
        return False
    actual_partition = parts[1]
    if inputs.permissions_manifest_path:
        try:
            permissions = load_byoc_permissions_manifest(inputs.permissions_manifest_path)
            if permissions.aws is not None:
                return actual_partition == permissions.aws.partition
        except Exception:  # noqa: BLE001
            return True
    return True


def _identity_value(
    field: str,
    dataplane: ByocDataPlaneManifest | None,
    permissions: ByocPermissionsManifest | None,
) -> str | None:
    if dataplane is not None:
        return str(getattr(dataplane, field))
    if permissions is not None:
        return str(getattr(permissions, field))
    return None


def _find_check(
    checks: Sequence[ByocAwsLivePreflightCheck],
    name: str,
) -> ByocAwsLivePreflightCheck | None:
    return next((check for check in checks if check.name == name), None)


def _metric_int(check: ByocAwsLivePreflightCheck | None, name: str) -> int:
    if check is None:
        return 0
    value = check.metrics.get(name)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return 0


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _reject_sensitive_text(value: str) -> None:
    lower = value.lower()
    if _ACCOUNT_ID_RE.search(value) or _ACCESS_KEY_RE.search(value):
        raise ValueError("AWS live preflight reports must not include AWS identifiers")
    if any(fragment in lower for fragment in _FORBIDDEN_TEXT_FRAGMENTS):
        raise ValueError("AWS live preflight reports must not include sensitive text")


def _assert_report_is_sanitized(
    report: ByocAwsLivePreflightReport,
    *,
    sensitive_values: Sequence[str | None],
) -> None:
    rendered = json.dumps(report.as_json(), sort_keys=True)
    if _ACCOUNT_ID_RE.search(rendered) or _ACCESS_KEY_RE.search(rendered):
        raise ValueError("AWS live preflight report leaked sensitive AWS metadata")
    for value in sensitive_values:
        if value and value in rendered:
            raise ValueError("AWS live preflight report leaked sensitive AWS metadata")
