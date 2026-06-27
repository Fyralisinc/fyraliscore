"""Local BYOC customer-pilot handoff artifact builder."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_control_plane_read_smoke_summary import (
    ByocControlPlaneReadSmokePrivacyContract,
    ByocControlPlaneReadSmokeSummary,
    ByocControlPlaneReadSmokeSummaryInputs,
    ByocControlPlaneReadSmokeSurface,
    build_byoc_control_plane_read_smoke_summary,
    render_control_plane_read_smoke_summary_json,
)
from services.platform.runtime.byoc_customer_handoff import (
    ByocCustomerHandoffInputs,
    render_customer_handoff_json,
    run_byoc_customer_handoff,
)
from services.platform.runtime.byoc_handoff_bundle_index import (
    ByocHandoffBundleIndexInputs,
    build_byoc_handoff_bundle_index,
    render_handoff_bundle_index_json,
)
from services.platform.runtime.byoc_launch_readiness_summary import (
    ByocLaunchReadinessSummaryInputs,
    build_byoc_launch_readiness_summary,
    render_launch_readiness_summary_json,
)
from services.platform.runtime.byoc_live_test_readiness import (
    ByocLiveTestReadinessInputs,
    render_live_test_readiness_json,
    run_byoc_live_test_readiness,
)


PilotPackageStatus = Literal["pass", "fail", "manual_required"]
PilotPackageStoredScope = Literal["sanitized_customer_pilot_package_manifest_only"]
PilotPackageArtifactKind = Literal[
    "evidence_package",
    "evidence_ledger",
    "live_test_readiness",
    "customer_handoff_readiness",
    "control_plane_read_smoke_summary",
    "handoff_bundle_index",
    "launch_readiness_summary",
]

_EXPECTED_SMOKE_SURFACES = (
    "agent_fleet",
    "deployment_overview",
    "evidence_packages",
    "preflight_reports",
    "runner_evidence",
)
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:/+=,-]{1,240}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_FRAGMENTS = (
    "://",
    "arn:",
    "bearer ",
    "password=",
    "postgresql://",
    "secret=",
    "token=",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocCustomerPilotPackagePrivacyContract(_StrictModel):
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


class ByocCustomerPilotPackageArtifact(_StrictModel):
    name: str
    kind: PilotPackageArtifactKind
    path: str
    digest: str
    schema_version: str
    generated_by_builder: bool
    share_with_customer: bool = True
    contents_included: Literal[False] = False

    @field_validator("name", "schema_version")
    @classmethod
    def _code_fields_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if (
            not value
            or not _SAFE_CODE_RE.match(value)
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("customer pilot artifact metadata must be bounded")
        return value

    @field_validator("path")
    @classmethod
    def _path_must_be_relative_safe(cls, value: str) -> str:
        value = value.strip()
        if (
            not value
            or not _SAFE_CODE_RE.match(value)
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("customer pilot artifact path must be bounded")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("customer pilot artifact path must stay under repo root")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("customer pilot artifact digest must look like sha256")
        return value


class ByocCustomerPilotPackageManifest(_StrictModel):
    schema_version: Literal["fyralis.byoc.customer_pilot_package_manifest.v1"]
    generated_at: datetime
    status: PilotPackageStatus
    customer_pilot_ready: bool
    manual_actions_required: bool
    required_checks_passed: bool
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    artifact_count: int = Field(ge=0)
    output_dir: str
    next_actions: tuple[str, ...]
    artifacts: tuple[ByocCustomerPilotPackageArtifact, ...]
    privacy: ByocCustomerPilotPackagePrivacyContract
    stored_scope: PilotPackageStoredScope = (
        "sanitized_customer_pilot_package_manifest_only"
    )

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
        "output_dir",
    )
    @classmethod
    def _identity_fields_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if (
            not value
            or not _SAFE_CODE_RE.match(value)
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("customer pilot manifest metadata must be bounded")
        return value

    @field_validator("next_actions")
    @classmethod
    def _next_actions_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 20:
            raise ValueError("customer pilot next actions must be bounded")
        normalized = tuple(action.strip() for action in value)
        if any(not action or not _SAFE_CODE_RE.match(action) for action in normalized):
            raise ValueError("customer pilot next actions must be bounded")
        return normalized


@dataclass(frozen=True, slots=True)
class ByocCustomerPilotPackageInputs:
    output_dir: Path = Path("tmp/byoc/customer-pilot")
    repo_root: Path = field(default_factory=Path.cwd)
    dataplane_manifest_path: Path = Path("deploy/byoc/dataplane.example.yaml")
    permissions_manifest_path: Path = Path("deploy/byoc/permissions.example.yaml")
    iam_template_path: Path = Path("deploy/byoc/aws/iam.bootstrap.template.yaml")
    iac_package_path: Path = Path("deploy/byoc/aws/iac-package.example.yaml")
    bootstrap_bundle_path: Path = Path("deploy/byoc/bootstrap-bundle.example.yaml")
    bootstrap_plan_path: Path = Path("deploy/byoc/bootstrap-plan.example.yaml")
    evidence_package_path: Path = Path("deploy/byoc/evidence-package.example.yaml")
    evidence_ledger_path: Path = Path("deploy/byoc/evidence-ledger.example.yaml")
    env_path: Path | None = Path(".env.production.example")
    control_plane_read_smoke_path: Path | None = None
    generated_at: datetime | None = None


def build_byoc_customer_pilot_package(
    inputs: ByocCustomerPilotPackageInputs,
) -> ByocCustomerPilotPackageManifest:
    generated_at = inputs.generated_at or datetime.now(tz=UTC)
    output_dir = _resolve_output_dir(inputs.output_dir, inputs.repo_root)
    output_dir.mkdir(parents=True, exist_ok=True)

    live_readiness = run_byoc_live_test_readiness(
        ByocLiveTestReadinessInputs(
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            iam_template_path=inputs.iam_template_path,
            repo_root=inputs.repo_root,
        )
    )
    live_path = output_dir / "byoc-live-test-readiness.json"
    live_path.write_text(render_live_test_readiness_json(live_readiness), "utf-8")

    handoff = run_byoc_customer_handoff(
        ByocCustomerHandoffInputs(
            dataplane_manifest_path=inputs.dataplane_manifest_path,
            permissions_manifest_path=inputs.permissions_manifest_path,
            iam_template_path=inputs.iam_template_path,
            iac_package_path=inputs.iac_package_path,
            bootstrap_bundle_path=inputs.bootstrap_bundle_path,
            bootstrap_plan_path=inputs.bootstrap_plan_path,
            evidence_package_path=inputs.evidence_package_path,
            evidence_ledger_path=inputs.evidence_ledger_path,
            env_path=inputs.env_path,
            repo_root=inputs.repo_root,
        )
    )
    handoff_path = output_dir / "byoc-customer-handoff-report.json"
    handoff_path.write_text(render_customer_handoff_json(handoff), "utf-8")

    smoke_summary = (
        build_byoc_control_plane_read_smoke_summary(
            ByocControlPlaneReadSmokeSummaryInputs(
                control_plane_read_smoke_path=inputs.control_plane_read_smoke_path,
                generated_at=generated_at,
            )
        )
        if inputs.control_plane_read_smoke_path is not None
        else _manual_control_plane_read_smoke_summary(
            deployment_id=handoff.deployment_id or live_readiness.deployment_id,
            customer_id=handoff.customer_id or live_readiness.customer_id,
            generated_at=generated_at,
        )
    )
    smoke_summary_path = output_dir / "byoc-control-plane-read-smoke-summary.json"
    smoke_summary_path.write_text(
        render_control_plane_read_smoke_summary_json(smoke_summary),
        "utf-8",
    )

    handoff_index = build_byoc_handoff_bundle_index(
        ByocHandoffBundleIndexInputs(
            evidence_package_path=inputs.evidence_package_path,
            evidence_ledger_path=inputs.evidence_ledger_path,
            repo_root=inputs.repo_root,
            customer_handoff_report_path=handoff_path,
            control_plane_read_smoke_report_path=smoke_summary_path,
            generated_at=generated_at,
        )
    )
    handoff_index_path = output_dir / "byoc-customer-handoff-bundle-index.json"
    handoff_index_path.write_text(
        render_handoff_bundle_index_json(handoff_index),
        "utf-8",
    )

    launch_summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=live_path,
            customer_handoff_report_path=handoff_path,
            handoff_bundle_index_path=handoff_index_path,
            control_plane_read_smoke_path=smoke_summary_path,
            generated_at=generated_at,
        )
    )
    launch_summary_path = output_dir / "byoc-launch-readiness-summary.json"
    launch_summary_path.write_text(
        render_launch_readiness_summary_json(launch_summary),
        "utf-8",
    )

    artifacts = (
        _artifact(
            "evidence_package",
            "evidence_package",
            inputs.evidence_package_path,
            inputs.repo_root,
            "fyralis.byoc.evidence_package.v1",
            False,
        ),
        _artifact(
            "evidence_ledger",
            "evidence_ledger",
            inputs.evidence_ledger_path,
            inputs.repo_root,
            "fyralis.byoc.evidence_ledger.v1",
            False,
        ),
        _artifact(
            "live_test_readiness",
            "live_test_readiness",
            live_path,
            inputs.repo_root,
            "fyralis.byoc.live_test_readiness.v1",
            True,
        ),
        _artifact(
            "customer_handoff_readiness",
            "customer_handoff_readiness",
            handoff_path,
            inputs.repo_root,
            "fyralis.byoc.customer_handoff_readiness.v1",
            True,
        ),
        _artifact(
            "control_plane_read_smoke_summary",
            "control_plane_read_smoke_summary",
            smoke_summary_path,
            inputs.repo_root,
            "fyralis.byoc.control_plane_read_smoke_summary.v1",
            True,
        ),
        _artifact(
            "handoff_bundle_index",
            "handoff_bundle_index",
            handoff_index_path,
            inputs.repo_root,
            "fyralis.byoc.customer_handoff_bundle_index.v1",
            True,
        ),
        _artifact(
            "launch_readiness_summary",
            "launch_readiness_summary",
            launch_summary_path,
            inputs.repo_root,
            "fyralis.byoc.launch_readiness_summary.v1",
            True,
        ),
    )
    manifest = ByocCustomerPilotPackageManifest(
        schema_version="fyralis.byoc.customer_pilot_package_manifest.v1",
        generated_at=generated_at,
        status=launch_summary.status,
        customer_pilot_ready=launch_summary.customer_pilot_ready,
        manual_actions_required=launch_summary.manual_actions_required,
        required_checks_passed=launch_summary.required_checks_passed,
        deployment_id=launch_summary.deployment_id,
        customer_id=launch_summary.customer_id,
        cloud_provider=launch_summary.cloud_provider,
        region=launch_summary.region,
        artifact_revision=launch_summary.artifact_revision,
        artifact_count=len(artifacts),
        output_dir=_relative_path(output_dir, inputs.repo_root),
        next_actions=launch_summary.next_actions,
        artifacts=artifacts,
        privacy=ByocCustomerPilotPackagePrivacyContract(),
        stored_scope="sanitized_customer_pilot_package_manifest_only",
    )
    manifest_path = output_dir / "byoc-customer-pilot-package-manifest.json"
    manifest_path.write_text(render_customer_pilot_package_manifest_json(manifest), "utf-8")
    return manifest


def render_customer_pilot_package_manifest_json(
    manifest: ByocCustomerPilotPackageManifest,
) -> str:
    return json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_customer_pilot_package_manifest_yaml(
    manifest: ByocCustomerPilotPackageManifest,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(
        manifest.model_dump(mode="json"),
        sort_keys=False,
        width=1_000_000,
    )


def _manual_control_plane_read_smoke_summary(
    *,
    deployment_id: str | None,
    customer_id: str | None,
    generated_at: datetime,
) -> ByocControlPlaneReadSmokeSummary:
    return ByocControlPlaneReadSmokeSummary(
        schema_version="fyralis.byoc.control_plane_read_smoke_summary.v1",
        generated_at=generated_at,
        status="manual_required",
        mode="unknown",
        hosted_read_executed=False,
        required_surfaces_present=False,
        manual_actions_required=True,
        deployment_id=deployment_id,
        customer_id=customer_id,
        surface_count=0,
        expected_surface_count=len(_EXPECTED_SMOKE_SURFACES),
        next_actions=("run_hosted_control_plane_read_smoke",),
        surfaces=tuple(
            ByocControlPlaneReadSmokeSurface(
                name=name,
                status="manual_required",
                required=True,
                details="Hosted control-plane read smoke has not been provided.",
            )
            for name in _EXPECTED_SMOKE_SURFACES
        ),
        privacy=ByocControlPlaneReadSmokePrivacyContract(),
        stored_scope="sanitized_control_plane_read_smoke_metadata_only",
    )


def _artifact(
    name: str,
    kind: PilotPackageArtifactKind,
    path: Path,
    repo_root: Path,
    schema_version: str,
    generated_by_builder: bool,
) -> ByocCustomerPilotPackageArtifact:
    return ByocCustomerPilotPackageArtifact(
        name=name,
        kind=kind,
        path=_relative_path(path, repo_root),
        digest=_file_digest(path),
        schema_version=schema_version,
        generated_by_builder=generated_by_builder,
        share_with_customer=True,
        contents_included=False,
    )


def _resolve_output_dir(output_dir: Path, repo_root: Path) -> Path:
    resolved = output_dir if output_dir.is_absolute() else repo_root / output_dir
    _relative_path(resolved, repo_root)
    return resolved


def _relative_path(path: Path, repo_root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = repo_root.resolve()
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("customer pilot artifact path must stay inside repo_root") from exc
    return relative.as_posix()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


__all__ = [
    "ByocCustomerPilotPackageArtifact",
    "ByocCustomerPilotPackageInputs",
    "ByocCustomerPilotPackageManifest",
    "ByocCustomerPilotPackagePrivacyContract",
    "build_byoc_customer_pilot_package",
    "render_customer_pilot_package_manifest_json",
    "render_customer_pilot_package_manifest_yaml",
]
