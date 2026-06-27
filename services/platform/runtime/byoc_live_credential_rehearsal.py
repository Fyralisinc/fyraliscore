"""Local BYOC live-credential evidence rehearsal.

This module orchestrates existing sanitized contracts: AWS live preflight,
evidence ledger generation, evidence package generation, and source-onboarding
gate evaluation. It writes child artifacts to a caller-selected directory but
returns only bounded summary metadata.
"""
from __future__ import annotations

import json
import re
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    render_aws_live_preflight_json,
    run_byoc_aws_live_preflight,
)
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import generate_evidence_ledger
from services.platform.runtime.byoc_evidence_package import generate_evidence_package
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest
from services.platform.runtime.byoc_source_onboarding_gate import (
    ByocSourceOnboardingGateInputs,
    run_byoc_source_onboarding_gate,
)


RehearsalStatus = Literal["pass", "fail"]
RehearsalExecutionMode = Literal["customer_side_local"]
_PASS: RehearsalStatus = "pass"
_FAIL: RehearsalStatus = "fail"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
_AWS_REPORT_NAME = "aws-live-preflight.json"
_LEDGER_NAME = "evidence-ledger.yaml"
_PACKAGE_NAME = "evidence-package.yaml"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocLiveCredentialRehearsalPrivacyContract(_StrictModel):
    raw_payloads_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    urls_included: Literal[False] = False
    policy_documents_included: Literal[False] = False
    command_output_included: Literal[False] = False
    child_report_details_included: Literal[False] = False
    artifact_paths_included: Literal[False] = False


class ByocLiveCredentialRehearsalCheckSummary(_StrictModel):
    total: int = Field(ge=0)
    required: int = Field(ge=0)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed_required: int = Field(ge=0)

    @model_validator(mode="after")
    def _counts_must_match_total(self) -> "ByocLiveCredentialRehearsalCheckSummary":
        if self.passed + self.failed + self.skipped != self.total:
            raise ValueError("rehearsal check status counts must sum to total")
        if self.required > self.total:
            raise ValueError("rehearsal required count must not exceed total")
        if self.failed_required > self.failed:
            raise ValueError("rehearsal failed_required must not exceed failed")
        return self


class ByocLiveCredentialRehearsalCheck(_StrictModel):
    name: str
    status: RehearsalStatus
    required: bool
    details: str
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("rehearsal check name must be a bounded identifier")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 240:
            raise ValueError("rehearsal check details must be bounded")
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
                raise ValueError("rehearsal metric names must be bounded identifiers")
            if isinstance(metric, str):
                metric = metric.strip()
                if len(metric) > 120 or "://" in metric:
                    raise ValueError("rehearsal string metrics must be bounded")
            normalized[key] = metric
        return normalized


class ByocLiveCredentialRehearsalReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.live_credential_rehearsal.v1"]
    status: RehearsalStatus
    required_checks_passed: bool
    customer_credential_ready: bool
    source_onboarding_allowed: bool
    execution_mode: RehearsalExecutionMode = "customer_side_local"
    elapsed_seconds: float = Field(ge=0)
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    skip_live_aws: bool
    require_live_aws_api_calls: bool
    live_aws_api_calls_executed: bool
    cloud_credentials_required: bool
    readonly_api_probe_requested: bool
    iam_policy_simulation_requested: bool
    mutating_cloud_commands_executed: Literal[False] = False
    terraform_plan_executed: Literal[False] = False
    artifacts_written: tuple[str, ...]
    privacy: ByocLiveCredentialRehearsalPrivacyContract
    check_summary: ByocLiveCredentialRehearsalCheckSummary
    checks: tuple[ByocLiveCredentialRehearsalCheck, ...]

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
            raise ValueError("rehearsal identity fields must be bounded metadata")
        return value

    @field_validator("artifacts_written")
    @classmethod
    def _artifact_names_must_be_bounded(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        normalized = tuple(name.strip() for name in value)
        if any(not name or not _SAFE_CODE_RE.match(name) for name in normalized):
            raise ValueError("rehearsal artifact names must be bounded identifiers")
        return normalized


@dataclass(frozen=True, slots=True)
class ByocLiveCredentialRehearsalInputs:
    output_dir: Path
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    iam_template_path: Path
    iac_package_path: Path
    bootstrap_bundle_path: Path
    bootstrap_plan_path: Path
    env_path: Path | None = None
    repo_root: Path = field(default_factory=Path.cwd)
    aws_profile: str | None = None
    aws_region: str | None = None
    expected_aws_account_id: str | None = None
    skip_live_aws: bool = False
    require_live_aws_api_calls: bool = False
    run_readonly_api_probes: bool = False
    run_iam_policy_simulation: bool = False
    simulation_principal_arn: str | None = None
    simulation_role_name: str = "bootstrap_provisioner"
    require_live_post_deploy: bool = False
    require_signed_post_deploy: bool = False


def run_byoc_live_credential_rehearsal(
    inputs: ByocLiveCredentialRehearsalInputs,
) -> ByocLiveCredentialRehearsalReport:
    started = time.monotonic()
    checks: list[ByocLiveCredentialRehearsalCheck] = []
    artifacts_written: list[str] = []
    output_dir = inputs.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    repo_root = inputs.repo_root.resolve()
    manifest = None
    aws_report = None
    source_onboarding_allowed = False

    try:
        manifest = load_byoc_manifest(inputs.dataplane_manifest_path)
        plan = load_byoc_bootstrap_plan(inputs.bootstrap_plan_path)
        permissions = load_byoc_permissions_manifest(inputs.permissions_manifest_path)
        bundle = load_byoc_bootstrap_bundle(inputs.bootstrap_bundle_path)
        checks.append(
            _check(
                "contract_inputs_loaded",
                _PASS,
                required=True,
                details="BYOC rehearsal contract inputs loaded.",
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            _check(
                "contract_inputs_loaded",
                _FAIL,
                required=True,
                details="BYOC rehearsal contract inputs could not be loaded.",
                metrics={"error_type": type(exc).__name__},
            )
        )
        return _report(
            inputs,
            manifest=manifest,
            aws_report=None,
            checks=checks,
            artifacts_written=tuple(artifacts_written),
            source_onboarding_allowed=False,
            started=started,
        )

    try:
        aws_report = run_byoc_aws_live_preflight(
            ByocAwsLivePreflightInputs(
                dataplane_manifest_path=inputs.dataplane_manifest_path,
                permissions_manifest_path=inputs.permissions_manifest_path,
                iam_template_path=inputs.iam_template_path,
                aws_profile=inputs.aws_profile,
                aws_region=inputs.aws_region,
                expected_account_id=inputs.expected_aws_account_id,
                skip_live_aws=inputs.skip_live_aws,
                run_readonly_api_probes=inputs.run_readonly_api_probes,
                run_iam_policy_simulation=inputs.run_iam_policy_simulation,
                simulation_principal_arn=inputs.simulation_principal_arn,
                simulation_role_name=inputs.simulation_role_name,
            )
        )
        aws_report_path = output_dir / _AWS_REPORT_NAME
        aws_report_path.write_text(
            render_aws_live_preflight_json(aws_report),
            encoding="utf-8",
        )
        artifacts_written.append(_AWS_REPORT_NAME)
        checks.append(
            _check(
                "aws_live_preflight",
                _PASS if aws_report.required_checks_passed else _FAIL,
                required=True,
                details="AWS live preflight report passed.",
                metrics={
                    "live_aws_api_calls_executed": (
                        aws_report.live_aws_api_calls_executed
                    ),
                    "cloud_credentials_required": (
                        aws_report.cloud_credentials_required
                    ),
                },
            )
        )
        checks.append(
            _check(
                "live_aws_api_calls",
                (
                    _PASS
                    if (
                        not inputs.require_live_aws_api_calls
                        or aws_report.live_aws_api_calls_executed
                    )
                    else _FAIL
                ),
                required=inputs.require_live_aws_api_calls,
                details="Live AWS API calls satisfied the rehearsal requirement.",
                metrics={"required": inputs.require_live_aws_api_calls},
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            _check(
                "aws_live_preflight",
                _FAIL,
                required=True,
                details="AWS live preflight could not be completed.",
                metrics={"error_type": type(exc).__name__},
            )
        )
        return _report(
            inputs,
            manifest=manifest,
            aws_report=aws_report,
            checks=checks,
            artifacts_written=tuple(artifacts_written),
            source_onboarding_allowed=False,
            started=started,
        )

    try:
        ledger_path = output_dir / _LEDGER_NAME
        ledger = generate_evidence_ledger(
            plan=plan,
            dataplane_manifest=manifest,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan_path=inputs.bootstrap_plan_path,
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            bootstrap_bundle_path=inputs.bootstrap_bundle_path,
            iac_package_path=inputs.iac_package_path,
            iam_template_path=inputs.iam_template_path,
            env_path=inputs.env_path,
            aws_live_preflight_report_path=aws_report_path,
            repo_root=repo_root,
        )
        _write_yaml(ledger_path, ledger.model_dump(mode="json", exclude_none=True))
        artifacts_written.append(_LEDGER_NAME)
        package_path = output_dir / _PACKAGE_NAME
        package = generate_evidence_package(
            ledger=ledger,
            dataplane_manifest=manifest,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan=plan,
            ledger_path=ledger_path,
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            aws_iac_package_path=inputs.iac_package_path,
            bootstrap_bundle_path=inputs.bootstrap_bundle_path,
            plan_path=inputs.bootstrap_plan_path,
            repo_root=repo_root,
        )
        _write_yaml(package_path, package.model_dump(mode="json", exclude_none=True))
        artifacts_written.append(_PACKAGE_NAME)
        checks.append(
            _check(
                "evidence_artifacts",
                _PASS,
                required=True,
                details="Sanitized evidence ledger and package were generated.",
                metrics={"artifact_count": len(artifacts_written)},
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            _check(
                "evidence_artifacts",
                _FAIL,
                required=True,
                details="Sanitized evidence artifacts could not be generated.",
                metrics={"error_type": type(exc).__name__},
            )
        )
        return _report(
            inputs,
            manifest=manifest,
            aws_report=aws_report,
            checks=checks,
            artifacts_written=tuple(artifacts_written),
            source_onboarding_allowed=False,
            started=started,
        )

    gate = run_byoc_source_onboarding_gate(
        ByocSourceOnboardingGateInputs(
            evidence_package_path=package_path,
            require_aws_live_preflight=True,
            require_live_post_deploy=inputs.require_live_post_deploy,
            require_signed_post_deploy=inputs.require_signed_post_deploy,
        )
    )
    source_onboarding_allowed = gate.source_onboarding_allowed
    checks.append(
        _check(
            "source_onboarding_gate",
            _PASS if gate.source_onboarding_allowed else _FAIL,
            required=True,
            details="Source-onboarding gate evaluated generated evidence package.",
            metrics={
                "require_live_post_deploy": inputs.require_live_post_deploy,
                "require_signed_post_deploy": inputs.require_signed_post_deploy,
            },
        )
    )
    return _report(
        inputs,
        manifest=manifest,
        aws_report=aws_report,
        checks=checks,
        artifacts_written=tuple(artifacts_written),
        source_onboarding_allowed=source_onboarding_allowed,
        started=started,
    )


def render_live_credential_rehearsal_json(
    report: ByocLiveCredentialRehearsalReport,
) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_live_credential_rehearsal_yaml(
    report: ByocLiveCredentialRehearsalReport,
) -> str:
    return _render_yaml(report.as_json())


def _report(
    inputs: ByocLiveCredentialRehearsalInputs,
    *,
    manifest: object | None,
    aws_report: object | None,
    checks: Sequence[ByocLiveCredentialRehearsalCheck],
    artifacts_written: tuple[str, ...],
    source_onboarding_allowed: bool,
    started: float,
) -> ByocLiveCredentialRehearsalReport:
    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    live_calls_executed = bool(
        getattr(aws_report, "live_aws_api_calls_executed", False)
    )
    customer_credential_ready = (
        required_checks_passed and source_onboarding_allowed and live_calls_executed
    )
    return ByocLiveCredentialRehearsalReport(
        schema_version="fyralis.byoc.live_credential_rehearsal.v1",
        status=_PASS if required_checks_passed else _FAIL,
        required_checks_passed=required_checks_passed,
        customer_credential_ready=customer_credential_ready,
        source_onboarding_allowed=source_onboarding_allowed,
        execution_mode="customer_side_local",
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=getattr(manifest, "deployment_id", None),
        customer_id=getattr(manifest, "customer_id", None),
        cloud_provider=getattr(manifest, "cloud_provider", None),
        region=getattr(manifest, "region", None),
        artifact_revision=getattr(manifest, "artifact_revision", None),
        skip_live_aws=inputs.skip_live_aws,
        require_live_aws_api_calls=inputs.require_live_aws_api_calls,
        live_aws_api_calls_executed=live_calls_executed,
        cloud_credentials_required=bool(
            getattr(aws_report, "cloud_credentials_required", False)
        ),
        readonly_api_probe_requested=inputs.run_readonly_api_probes,
        iam_policy_simulation_requested=inputs.run_iam_policy_simulation,
        mutating_cloud_commands_executed=False,
        terraform_plan_executed=False,
        artifacts_written=artifacts_written,
        privacy=ByocLiveCredentialRehearsalPrivacyContract(),
        check_summary=_check_summary(checks),
        checks=tuple(checks),
    )


def _check_summary(
    checks: Sequence[ByocLiveCredentialRehearsalCheck],
) -> ByocLiveCredentialRehearsalCheckSummary:
    statuses = Counter(check.status for check in checks)
    failed_required = sum(
        1 for check in checks if check.required and check.status == _FAIL
    )
    return ByocLiveCredentialRehearsalCheckSummary(
        total=len(checks),
        required=sum(1 for check in checks if check.required),
        passed=statuses[_PASS],
        failed=statuses[_FAIL],
        skipped=0,
        failed_required=failed_required,
    )


def _check(
    name: str,
    status: RehearsalStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocLiveCredentialRehearsalCheck:
    return ByocLiveCredentialRehearsalCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _write_yaml(path: Path, payload: object) -> None:
    path.write_text(_render_yaml(payload), encoding="utf-8")


def _render_yaml(payload: object) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(payload, sort_keys=False, width=1_000_000)


__all__ = [
    "ByocLiveCredentialRehearsalCheck",
    "ByocLiveCredentialRehearsalCheckSummary",
    "ByocLiveCredentialRehearsalInputs",
    "ByocLiveCredentialRehearsalPrivacyContract",
    "ByocLiveCredentialRehearsalReport",
    "render_live_credential_rehearsal_json",
    "render_live_credential_rehearsal_yaml",
    "run_byoc_live_credential_rehearsal",
]
