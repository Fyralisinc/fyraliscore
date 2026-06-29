"""Offline BYOC customer-pilot package rehearsal.

This composes the product-health install rehearsal, customer-pilot package
builder, and package verifier into one clean local proof. It writes child
artifacts to a repo-local ``tmp/`` directory and returns only bounded summary
metadata suitable for CI/release review.
"""
from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_customer_pilot_package import (
    ByocCustomerPilotPackageInputs,
    ByocCustomerPilotPackageValidationInputs,
    build_byoc_customer_pilot_package,
    render_customer_pilot_package_validation_json,
    validate_byoc_customer_pilot_package,
)
from services.platform.runtime.byoc_product_health_install_rehearsal import (
    ByocProductHealthInstallRehearsalInputs,
    render_product_health_install_rehearsal_json,
    run_product_health_install_rehearsal,
)


CustomerPilotRehearsalStatus = Literal["pass", "manual_required", "fail"]
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:/+=,-]{1,240}$")
_FORBIDDEN_FRAGMENTS = (
    "://",
    "arn:",
    "bearer ",
    "password=",
    "postgresql://",
    "secret=",
    "token=",
)
_INSTALL_REHEARSAL_REPORT = "byoc-product-health-install-rehearsal-report.json"
_PACKAGE_VALIDATION_REPORT = "byoc-customer-pilot-package-validation.json"
_PACKAGE_MANIFEST = "byoc-customer-pilot-package-manifest.json"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocCustomerPilotRehearsalPrivacyContract(_StrictModel):
    artifact_bodies_included: Literal[False] = False
    child_report_bodies_included: Literal[False] = False
    raw_reports_included: Literal[False] = False
    raw_payloads_included: Literal[False] = False
    request_bodies_included: Literal[False] = False
    response_bodies_included: Literal[False] = False
    signed_headers_included: Literal[False] = False
    endpoint_urls_included: Literal[False] = False
    raw_auth_material_included: Literal[False] = False
    credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    command_output_included: Literal[False] = False
    logs_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    pii_included: Literal[False] = False
    cloud_credentials_required: Literal[False] = False
    mutating_cloud_commands_executed: Literal[False] = False


class ByocCustomerPilotRehearsalReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.customer_pilot_rehearsal.v1"]
    generated_at: datetime
    status: CustomerPilotRehearsalStatus
    required_checks_passed: bool
    customer_pilot_ready: bool
    manual_actions_required: bool
    require_ready: bool
    clean_output_dir: bool
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    output_dir: str
    package_manifest_path: str
    package_validation_report_path: str
    product_health_install_rehearsal_report_path: str
    package_status: CustomerPilotRehearsalStatus
    package_validation_status: Literal["pass", "fail"]
    product_health_install_rehearsal_status: Literal["pass", "fail"]
    artifact_count: int = Field(ge=0)
    verified_artifact_count: int = Field(ge=0)
    artifacts_written: tuple[str, ...]
    next_actions: tuple[str, ...]
    privacy: ByocCustomerPilotRehearsalPrivacyContract
    stored_scope: Literal[
        "sanitized_customer_pilot_rehearsal_metadata_only"
    ] = "sanitized_customer_pilot_rehearsal_metadata_only"

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
        "output_dir",
        "package_manifest_path",
        "package_validation_report_path",
        "product_health_install_rehearsal_report_path",
    )
    @classmethod
    def _metadata_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _safe_metadata(value)

    @field_validator("artifacts_written", "next_actions")
    @classmethod
    def _tuples_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 40:
            raise ValueError("customer-pilot rehearsal tuple metadata must be bounded")
        return tuple(_safe_metadata(item) for item in value)


@dataclass(frozen=True, slots=True)
class ByocCustomerPilotRehearsalInputs:
    output_dir: Path = Path("tmp/byoc/customer-pilot-rehearsal")
    repo_root: Path = field(default_factory=Path.cwd)
    clean_output_dir: bool = True
    require_ready: bool = False
    dataplane_manifest_path: Path = Path("deploy/byoc/dataplane.example.yaml")
    permissions_manifest_path: Path = Path("deploy/byoc/permissions.example.yaml")
    iam_template_path: Path = Path("deploy/byoc/aws/iam.bootstrap.template.yaml")
    iac_package_path: Path = Path("deploy/byoc/aws/iac-package.example.yaml")
    bootstrap_bundle_path: Path = Path("deploy/byoc/bootstrap-bundle.example.yaml")
    bootstrap_plan_path: Path = Path("deploy/byoc/bootstrap-plan.example.yaml")
    evidence_package_path: Path = Path("deploy/byoc/evidence-package.example.yaml")
    evidence_ledger_path: Path = Path("deploy/byoc/evidence-ledger.example.yaml")
    product_health_automation_path: Path = Path(
        "deploy/byoc/product-health-automation.example.yaml"
    )
    product_health_install_rehearsal_path: Path = Path(
        "deploy/byoc/product-health-install-rehearsal.example.yaml"
    )
    env_path: Path | None = Path(".env.production.example")
    live_test_readiness_path: Path | None = None
    customer_handoff_report_path: Path | None = None
    control_plane_read_smoke_path: Path | None = None
    control_plane_read_smoke_summary_path: Path | None = None
    generated_at: datetime | None = None


def run_byoc_customer_pilot_rehearsal(
    inputs: ByocCustomerPilotRehearsalInputs,
) -> ByocCustomerPilotRehearsalReport:
    generated_at = inputs.generated_at or datetime.now(tz=UTC)
    repo_root = inputs.repo_root.resolve()
    output_dir = _resolve_output_dir(inputs.output_dir, repo_root)
    _prepare_output_dir(output_dir, repo_root, clean=inputs.clean_output_dir)

    install_report = run_product_health_install_rehearsal(
        ByocProductHealthInstallRehearsalInputs(
            install_plan_path=_resolve_input_path(
                inputs.product_health_install_rehearsal_path,
                repo_root,
            ),
            repo_root=repo_root,
            generated_at=generated_at,
        )
    )
    install_report_path = output_dir / _INSTALL_REHEARSAL_REPORT
    install_report_path.write_text(
        render_product_health_install_rehearsal_json(install_report),
        encoding="utf-8",
    )

    manifest = build_byoc_customer_pilot_package(
        ByocCustomerPilotPackageInputs(
            output_dir=output_dir,
            repo_root=repo_root,
            dataplane_manifest_path=_resolve_input_path(
                inputs.dataplane_manifest_path,
                repo_root,
            ),
            permissions_manifest_path=_resolve_input_path(
                inputs.permissions_manifest_path,
                repo_root,
            ),
            iam_template_path=_resolve_input_path(inputs.iam_template_path, repo_root),
            iac_package_path=_resolve_input_path(inputs.iac_package_path, repo_root),
            bootstrap_bundle_path=_resolve_input_path(
                inputs.bootstrap_bundle_path,
                repo_root,
            ),
            bootstrap_plan_path=_resolve_input_path(
                inputs.bootstrap_plan_path,
                repo_root,
            ),
            evidence_package_path=_resolve_input_path(
                inputs.evidence_package_path,
                repo_root,
            ),
            evidence_ledger_path=_resolve_input_path(
                inputs.evidence_ledger_path,
                repo_root,
            ),
            product_health_automation_path=_resolve_input_path(
                inputs.product_health_automation_path,
                repo_root,
            ),
            product_health_install_rehearsal_path=_resolve_input_path(
                inputs.product_health_install_rehearsal_path,
                repo_root,
            ),
            env_path=(
                _resolve_input_path(inputs.env_path, repo_root)
                if inputs.env_path is not None
                else None
            ),
            live_test_readiness_path=(
                _resolve_input_path(inputs.live_test_readiness_path, repo_root)
                if inputs.live_test_readiness_path is not None
                else None
            ),
            customer_handoff_report_path=(
                _resolve_input_path(inputs.customer_handoff_report_path, repo_root)
                if inputs.customer_handoff_report_path is not None
                else None
            ),
            control_plane_read_smoke_path=(
                _resolve_input_path(inputs.control_plane_read_smoke_path, repo_root)
                if inputs.control_plane_read_smoke_path is not None
                else None
            ),
            control_plane_read_smoke_summary_path=(
                _resolve_input_path(
                    inputs.control_plane_read_smoke_summary_path,
                    repo_root,
                )
                if inputs.control_plane_read_smoke_summary_path is not None
                else None
            ),
            generated_at=generated_at,
        )
    )

    manifest_path = output_dir / _PACKAGE_MANIFEST
    validation = validate_byoc_customer_pilot_package(
        ByocCustomerPilotPackageValidationInputs(
            manifest_path=manifest_path,
            repo_root=repo_root,
            generated_at=generated_at,
        )
    )
    validation_path = output_dir / _PACKAGE_VALIDATION_REPORT
    validation_path.write_text(
        render_customer_pilot_package_validation_json(validation),
        encoding="utf-8",
    )

    required_checks_passed = (
        install_report.status == "pass"
        and validation.status == "pass"
        and manifest.status != "fail"
    )
    status: CustomerPilotRehearsalStatus
    if not required_checks_passed:
        status = "fail"
    else:
        status = manifest.status

    next_actions = set(manifest.next_actions)
    next_actions.update(install_report.next_actions)
    if inputs.require_ready and not manifest.customer_pilot_ready:
        next_actions.add("complete_customer_pilot_rehearsal_ready_evidence")

    return ByocCustomerPilotRehearsalReport(
        schema_version="fyralis.byoc.customer_pilot_rehearsal.v1",
        generated_at=generated_at,
        status=status,
        required_checks_passed=required_checks_passed,
        customer_pilot_ready=manifest.customer_pilot_ready,
        manual_actions_required=manifest.manual_actions_required,
        require_ready=inputs.require_ready,
        clean_output_dir=inputs.clean_output_dir,
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        cloud_provider=manifest.cloud_provider,
        region=manifest.region,
        artifact_revision=manifest.artifact_revision,
        output_dir=_relative_path(output_dir, repo_root),
        package_manifest_path=_relative_path(manifest_path, repo_root),
        package_validation_report_path=_relative_path(validation_path, repo_root),
        product_health_install_rehearsal_report_path=_relative_path(
            install_report_path,
            repo_root,
        ),
        package_status=manifest.status,
        package_validation_status=validation.status,
        product_health_install_rehearsal_status=install_report.status,
        artifact_count=manifest.artifact_count,
        verified_artifact_count=validation.verified_artifact_count,
        artifacts_written=_artifact_names(output_dir),
        next_actions=tuple(sorted(next_actions)),
        privacy=ByocCustomerPilotRehearsalPrivacyContract(),
        stored_scope="sanitized_customer_pilot_rehearsal_metadata_only",
    )


def render_customer_pilot_rehearsal_json(
    report: ByocCustomerPilotRehearsalReport,
) -> str:
    return json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_customer_pilot_rehearsal_yaml(
    report: ByocCustomerPilotRehearsalReport,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(
        report.model_dump(mode="json"),
        sort_keys=False,
        width=1_000_000,
    )


def _prepare_output_dir(output_dir: Path, repo_root: Path, *, clean: bool) -> None:
    relative = _relative_path(output_dir, repo_root)
    parts = Path(relative).parts
    if clean:
        if not parts or parts[0] != "tmp":
            raise ValueError("clean customer-pilot rehearsal output must stay under tmp/")
        if output_dir.exists():
            shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def _resolve_output_dir(path: Path, repo_root: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    _relative_path(resolved, repo_root)
    return resolved


def _resolve_input_path(path: Path, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _relative_path(path: Path, repo_root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = repo_root.resolve()
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("customer-pilot rehearsal paths must stay under repo_root") from exc
    return relative.as_posix()


def _artifact_names(output_dir: Path) -> tuple[str, ...]:
    return tuple(sorted(path.name for path in output_dir.iterdir() if path.is_file()))


def _safe_metadata(value: str) -> str:
    value = value.strip()
    if (
        not value
        or not _SAFE_CODE_RE.match(value)
        or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
    ):
        raise ValueError("customer-pilot rehearsal metadata must be bounded")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("customer-pilot rehearsal paths must be relative")
    return value


__all__ = [
    "ByocCustomerPilotRehearsalInputs",
    "ByocCustomerPilotRehearsalPrivacyContract",
    "ByocCustomerPilotRehearsalReport",
    "render_customer_pilot_rehearsal_json",
    "render_customer_pilot_rehearsal_yaml",
    "run_byoc_customer_pilot_rehearsal",
]
