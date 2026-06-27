"""Offline readiness report for the next live BYOC AWS test."""
from __future__ import annotations

import configparser
import json
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

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


ReadinessStatus = Literal["pass", "fail", "manual_required"]
ReadinessExecutionMode = Literal["local_offline"]
NextRequiredAction = Literal[
    "fix_contract_inputs",
    "configure_aws_access",
    "run_live_credential_rehearsal",
]

_PASS: ReadinessStatus = "pass"
_FAIL: ReadinessStatus = "fail"
_MANUAL: ReadinessStatus = "manual_required"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
_REQUIRED_OPERATOR_SCRIPTS = (
    "scripts/run_byoc_live_credential_rehearsal.py",
    "scripts/update_byoc_agent_desired_state.py",
    "scripts/list_byoc_agents.py",
    "scripts/submit_byoc_preflight_report.py",
    "scripts/submit_byoc_runner_evidence.py",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocLiveTestReadinessPrivacyContract(_StrictModel):
    aws_api_calls_executed: Literal[False] = False
    credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    profile_names_included: Literal[False] = False
    endpoint_urls_included: Literal[False] = False
    command_output_included: Literal[False] = False
    raw_customer_data_included: Literal[False] = False
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    logs_included: Literal[False] = False
    pii_included: Literal[False] = False


class ByocLiveTestReadinessCheck(_StrictModel):
    name: str
    status: ReadinessStatus
    required: bool
    details: str
    metrics: dict[str, bool | int | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("readiness check names must be bounded identifiers")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240 or "://" in value:
            raise ValueError("readiness check details must be bounded")
        return value

    @field_validator("metrics")
    @classmethod
    def _metrics_must_be_bounded(
        cls,
        value: dict[str, bool | int | str],
    ) -> dict[str, bool | int | str]:
        normalized: dict[str, bool | int | str] = {}
        for key, metric in value.items():
            key = key.strip()
            if not key or not _SAFE_CODE_RE.match(key):
                raise ValueError("readiness metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120 or "://" in metric:
                    raise ValueError("readiness string metrics must be bounded")
            normalized[key] = metric
        return normalized


class ByocLiveTestReadinessReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.live_test_readiness.v1"]
    status: ReadinessStatus
    required_checks_passed: bool
    live_aws_ready: bool
    next_required_action: NextRequiredAction
    execution_mode: ReadinessExecutionMode = "local_offline"
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    aws_profile_supplied: bool
    aws_profile_configured: bool | None = None
    aws_env_credentials_present: bool
    aws_cli_available: bool
    expected_aws_account_contract_present: bool
    mutating_cloud_commands_executed: Literal[False] = False
    privacy: ByocLiveTestReadinessPrivacyContract
    checks: tuple[ByocLiveTestReadinessCheck, ...]

    def as_json(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
    )
    @classmethod
    def _identity_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 160 or "://" in value:
            raise ValueError("readiness identity fields must be bounded metadata")
        return value


@dataclass(frozen=True, slots=True)
class ByocLiveTestReadinessInputs:
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path
    repo_root: Path = field(default_factory=Path.cwd)
    aws_profile: str | None = None
    aws_region: str | None = None
    require_aws_access: bool = False
    aws_config_dir: Path | None = None


def run_byoc_live_test_readiness(
    inputs: ByocLiveTestReadinessInputs,
) -> ByocLiveTestReadinessReport:
    started = time.monotonic()
    checks: list[ByocLiveTestReadinessCheck] = []
    manifest = None
    permissions = None
    iam_template = None

    try:
        manifest = load_byoc_manifest(inputs.dataplane_manifest_path)
        violations = validate_byoc_manifest_contract(manifest)
        checks.append(
            _contract_check(
                "dataplane_manifest_contract",
                violations,
                "BYOC data-plane manifest is ready for live testing.",
            )
        )
    except ValidationError as exc:
        checks.append(
            _schema_check(
                "dataplane_manifest_contract",
                render_dataplane_validation_errors(exc),
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_load_error_check("dataplane_manifest_contract", exc))

    try:
        permissions = load_byoc_permissions_manifest(inputs.permissions_manifest_path)
        violations = validate_permissions_manifest_contract(
            permissions,
            dataplane_manifest=manifest,
        )
        checks.append(
            _contract_check(
                "permissions_manifest_contract",
                violations,
                "BYOC permissions manifest is ready for live testing.",
            )
        )
    except ValidationError as exc:
        checks.append(
            _schema_check(
                "permissions_manifest_contract",
                render_permissions_validation_errors(exc),
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_load_error_check("permissions_manifest_contract", exc))

    try:
        iam_template = load_byoc_aws_iam_template(inputs.iam_template_path)
        violations = (
            validate_aws_iam_template_contract(
                iam_template,
                permissions_manifest=permissions,
            )
            if permissions is not None
            else []
        )
        checks.append(
            _contract_check(
                "aws_iam_template_contract",
                violations,
                "BYOC AWS IAM skeleton is ready for live testing.",
            )
        )
    except ValidationError as exc:
        checks.append(
            _schema_check(
                "aws_iam_template_contract",
                render_permissions_validation_errors(exc),
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_load_error_check("aws_iam_template_contract", exc))

    checks.append(_operator_scripts_check(inputs.repo_root))
    checks.append(_expected_account_contract_check(permissions))
    checks.append(_region_alignment_check(inputs, manifest, permissions))
    aws_cli_available = shutil.which("aws") is not None
    checks.append(_aws_cli_check(aws_cli_available, inputs.require_aws_access))
    aws_env_credentials_present = _aws_env_credentials_present()
    aws_profile_configured = None
    if inputs.aws_profile:
        aws_profile_configured = _aws_profile_exists(
            inputs.aws_profile,
            aws_config_dir=inputs.aws_config_dir,
        )
    checks.append(
        _aws_access_reference_check(
            inputs,
            aws_env_credentials_present=aws_env_credentials_present,
            aws_profile_configured=aws_profile_configured,
        )
    )

    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    live_aws_ready = (
        required_checks_passed
        and aws_cli_available
        and (aws_env_credentials_present or aws_profile_configured is True)
    )
    status: ReadinessStatus
    if not required_checks_passed:
        status = _FAIL
    elif live_aws_ready:
        status = _PASS
    else:
        status = _MANUAL
    report = ByocLiveTestReadinessReport(
        schema_version="fyralis.byoc.live_test_readiness.v1",
        status=status,
        required_checks_passed=required_checks_passed,
        live_aws_ready=live_aws_ready,
        next_required_action=(
            "fix_contract_inputs"
            if not required_checks_passed
            else "run_live_credential_rehearsal"
            if live_aws_ready
            else "configure_aws_access"
        ),
        execution_mode="local_offline",
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=getattr(manifest, "deployment_id", None),
        customer_id=getattr(manifest, "customer_id", None),
        cloud_provider=getattr(manifest, "cloud_provider", None),
        region=inputs.aws_region or getattr(manifest, "region", None),
        artifact_revision=getattr(manifest, "artifact_revision", None),
        aws_profile_supplied=bool(inputs.aws_profile),
        aws_profile_configured=aws_profile_configured,
        aws_env_credentials_present=aws_env_credentials_present,
        aws_cli_available=aws_cli_available,
        expected_aws_account_contract_present=_expected_account_present(permissions),
        mutating_cloud_commands_executed=False,
        privacy=ByocLiveTestReadinessPrivacyContract(),
        checks=tuple(checks),
    )
    _assert_sanitized(report, sensitive_values=(inputs.aws_profile,))
    return report


def render_live_test_readiness_json(report: ByocLiveTestReadinessReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_live_test_readiness_yaml(report: ByocLiveTestReadinessReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def model_json_schema_bundle() -> dict[str, object]:
    return {
        "inputs": {
            "schema_version": "fyralis.byoc.live_test_readiness.inputs.v1",
            "fields": tuple(ByocLiveTestReadinessInputs.__dataclass_fields__),
        },
        "report": ByocLiveTestReadinessReport.model_json_schema(),
    }


def _contract_check(
    name: str,
    violations: list[object],
    pass_details: str,
) -> ByocLiveTestReadinessCheck:
    return _check(
        name,
        _PASS if not violations else _FAIL,
        required=True,
        details=pass_details if not violations else "BYOC contract validation failed.",
        metrics={"violation_count": len(violations)},
    )


def _schema_check(name: str, errors: list[str]) -> ByocLiveTestReadinessCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="BYOC manifest schema validation failed.",
        metrics={"error_count": len(errors)},
    )


def _load_error_check(name: str, exc: Exception) -> ByocLiveTestReadinessCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details="BYOC contract input could not be loaded.",
        metrics={"error_type": type(exc).__name__},
    )


def _operator_scripts_check(repo_root: Path) -> ByocLiveTestReadinessCheck:
    missing = [
        script
        for script in _REQUIRED_OPERATOR_SCRIPTS
        if not (repo_root / script).exists()
    ]
    return _check(
        "operator_scripts_present",
        _PASS if not missing else _FAIL,
        required=True,
        details=(
            "BYOC operator scripts needed for the live test are present."
            if not missing
            else "One or more BYOC operator scripts are missing."
        ),
        metrics={
            "script_count": len(_REQUIRED_OPERATOR_SCRIPTS),
            "missing_script_count": len(missing),
        },
    )


def _expected_account_contract_check(
    permissions: object | None,
) -> ByocLiveTestReadinessCheck:
    present = _expected_account_present(permissions)
    return _check(
        "expected_aws_account_contract",
        _PASS if present else _FAIL,
        required=True,
        details=(
            "Expected AWS account contract is present without serializing account id."
            if present
            else "Expected AWS account contract is missing."
        ),
        metrics={"account_contract_present": present},
    )


def _region_alignment_check(
    inputs: ByocLiveTestReadinessInputs,
    manifest: object | None,
    permissions: object | None,
) -> ByocLiveTestReadinessCheck:
    manifest_region = getattr(manifest, "region", None)
    permissions_region = getattr(permissions, "region", None)
    requested_region = inputs.aws_region or manifest_region
    aligned = (
        manifest_region is not None
        and permissions_region == manifest_region
        and requested_region == manifest_region
    )
    return _check(
        "aws_region_alignment",
        _PASS if aligned else _FAIL,
        required=True,
        details=(
            "AWS region selection matches BYOC manifests."
            if aligned
            else "AWS region selection does not match BYOC manifests."
        ),
        metrics={
            "aws_region_override_supplied": inputs.aws_region is not None,
            "region_aligned": aligned,
        },
    )


def _aws_cli_check(
    available: bool,
    require_aws_access: bool,
) -> ByocLiveTestReadinessCheck:
    return _check(
        "aws_cli_available",
        _PASS if available else (_FAIL if require_aws_access else _MANUAL),
        required=require_aws_access,
        details=(
            "AWS CLI is available for tomorrow's live credential test."
            if available
            else "AWS CLI must be installed before live credential testing."
        ),
        metrics={"aws_cli_available": available},
    )


def _aws_access_reference_check(
    inputs: ByocLiveTestReadinessInputs,
    *,
    aws_env_credentials_present: bool,
    aws_profile_configured: bool | None,
) -> ByocLiveTestReadinessCheck:
    configured = aws_env_credentials_present or aws_profile_configured is True
    status = _PASS if configured else (_FAIL if inputs.require_aws_access else _MANUAL)
    return _check(
        "aws_access_reference",
        status,
        required=inputs.require_aws_access,
        details=(
            "Local AWS access reference is configured without serializing credentials."
            if configured
            else "Configure an AWS profile or temporary environment credentials."
        ),
        metrics={
            "aws_profile_supplied": bool(inputs.aws_profile),
            "aws_profile_configured": bool(aws_profile_configured),
            "aws_env_credentials_present": aws_env_credentials_present,
        },
    )


def _aws_env_credentials_present() -> bool:
    return bool(
        os.environ.get("AWS_ACCESS_KEY_ID")
        and os.environ.get("AWS_SECRET_ACCESS_KEY")
    )


def _aws_profile_exists(
    profile: str,
    *,
    aws_config_dir: Path | None,
) -> bool:
    base = aws_config_dir or (Path.home() / ".aws")
    candidates = (
        (base / "config", f"profile {profile}"),
        (base / "credentials", profile),
    )
    for path, section in candidates:
        parser = configparser.ConfigParser()
        parser.read(path)
        if parser.has_section(section):
            return True
    return False


def _expected_account_present(permissions: object | None) -> bool:
    aws = getattr(permissions, "aws", None)
    account_id = getattr(aws, "account_id", None)
    return bool(isinstance(account_id, str) and re.fullmatch(r"\d{12}", account_id))


def _check(
    name: str,
    status: ReadinessStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, bool | int | str] | None = None,
) -> ByocLiveTestReadinessCheck:
    return ByocLiveTestReadinessCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _assert_sanitized(
    report: ByocLiveTestReadinessReport,
    *,
    sensitive_values: tuple[str | None, ...],
) -> None:
    serialized = json.dumps(report.as_json(), sort_keys=True)
    lower = serialized.lower()
    forbidden = (
        "aws_secret_access_key",
        "aws_session_token",
        "arn:",
        "authorization",
        "bearer ",
        "password",
        "secret=",
        "token=",
    )
    if any(fragment in lower for fragment in forbidden):
        raise ValueError("BYOC live-test readiness report contains sensitive markers")
    for value in sensitive_values:
        if value and value in serialized:
            raise ValueError("BYOC live-test readiness report contains sensitive input")


__all__ = [
    "ByocLiveTestReadinessCheck",
    "ByocLiveTestReadinessInputs",
    "ByocLiveTestReadinessPrivacyContract",
    "ByocLiveTestReadinessReport",
    "model_json_schema_bundle",
    "render_live_test_readiness_json",
    "render_live_test_readiness_yaml",
    "run_byoc_live_test_readiness",
]
