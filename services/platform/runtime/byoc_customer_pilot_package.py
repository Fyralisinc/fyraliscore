"""Local BYOC customer-pilot handoff artifact builder."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypeVar

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
    ByocCustomerHandoffReport,
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
    ByocLiveTestReadinessReport,
    render_live_test_readiness_json,
    run_byoc_live_test_readiness,
)


PilotPackageStatus = Literal["pass", "fail", "manual_required"]
PilotPackageValidationStatus = Literal["pass", "fail"]
PilotPackageStoredScope = Literal["sanitized_customer_pilot_package_manifest_only"]
PilotPackageValidationStoredScope = Literal[
    "sanitized_customer_pilot_package_validation_metadata_only"
]
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
    "control_panel_state",
    "evidence_packages",
    "preflight_reports",
    "runner_evidence",
)
_EXPECTED_ARTIFACT_NAMES = frozenset(
    {
        "evidence_package",
        "evidence_ledger",
        "live_test_readiness",
        "customer_handoff_readiness",
        "control_plane_read_smoke_summary",
        "handoff_bundle_index",
        "launch_readiness_summary",
    }
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
_ModelT = TypeVar("_ModelT", bound=BaseModel)


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


class ByocCustomerPilotPackageValidationResult(_StrictModel):
    schema_version: Literal["fyralis.byoc.customer_pilot_package_validation.v1"]
    generated_at: datetime
    status: PilotPackageValidationStatus
    package_status: PilotPackageStatus
    customer_pilot_ready: bool
    manual_actions_required: bool
    required_checks_passed: bool
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    manifest_path: str
    output_dir: str
    artifact_count: int = Field(ge=0)
    verified_artifact_count: int = Field(ge=0)
    failure_codes: tuple[str, ...]
    privacy: ByocCustomerPilotPackagePrivacyContract
    stored_scope: PilotPackageValidationStoredScope = (
        "sanitized_customer_pilot_package_validation_metadata_only"
    )

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
        "manifest_path",
        "output_dir",
    )
    @classmethod
    def _metadata_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if (
            not value
            or not _SAFE_CODE_RE.match(value)
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("customer pilot validation metadata must be bounded")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("customer pilot validation paths must stay under repo root")
        return value

    @field_validator("failure_codes")
    @classmethod
    def _failure_codes_must_be_bounded(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if len(value) > 50:
            raise ValueError("customer pilot validation failures must be bounded")
        normalized = tuple(code.strip() for code in value)
        if any(not code or not _SAFE_CODE_RE.match(code) for code in normalized):
            raise ValueError("customer pilot validation failures must be bounded")
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
    live_test_readiness_path: Path | None = None
    customer_handoff_report_path: Path | None = None
    control_plane_read_smoke_path: Path | None = None
    control_plane_read_smoke_summary_path: Path | None = None
    generated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ByocCustomerPilotPackageValidationInputs:
    manifest_path: Path = Path(
        "tmp/byoc/customer-pilot/byoc-customer-pilot-package-manifest.json"
    )
    repo_root: Path = field(default_factory=Path.cwd)
    generated_at: datetime | None = None


def build_byoc_customer_pilot_package(
    inputs: ByocCustomerPilotPackageInputs,
) -> ByocCustomerPilotPackageManifest:
    generated_at = inputs.generated_at or datetime.now(tz=UTC)
    output_dir = _resolve_output_dir(inputs.output_dir, inputs.repo_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    if (
        inputs.control_plane_read_smoke_path is not None
        and inputs.control_plane_read_smoke_summary_path is not None
    ):
        raise ValueError(
            "provide either control_plane_read_smoke_path or "
            "control_plane_read_smoke_summary_path"
        )

    live_readiness = (
        _load_model(inputs.live_test_readiness_path, ByocLiveTestReadinessReport)
        if inputs.live_test_readiness_path is not None
        else run_byoc_live_test_readiness(
            ByocLiveTestReadinessInputs(
                dataplane_manifest_path=inputs.dataplane_manifest_path,
                permissions_manifest_path=inputs.permissions_manifest_path,
                iam_template_path=inputs.iam_template_path,
                repo_root=inputs.repo_root,
            )
        )
    )
    live_path = output_dir / "byoc-live-test-readiness.json"
    live_path.write_text(render_live_test_readiness_json(live_readiness), "utf-8")

    handoff = (
        _load_model(inputs.customer_handoff_report_path, ByocCustomerHandoffReport)
        if inputs.customer_handoff_report_path is not None
        else run_byoc_customer_handoff(
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
    )
    handoff_path = output_dir / "byoc-customer-handoff-report.json"
    handoff_path.write_text(render_customer_handoff_json(handoff), "utf-8")

    smoke_summary = _control_plane_smoke_summary_for_package(
        inputs=inputs,
        generated_at=generated_at,
        fallback=_manual_control_plane_read_smoke_summary(
            deployment_id=handoff.deployment_id or live_readiness.deployment_id,
            customer_id=handoff.customer_id or live_readiness.customer_id,
            generated_at=generated_at,
        ),
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


def load_byoc_customer_pilot_package_manifest(
    path: Path,
) -> ByocCustomerPilotPackageManifest:
    parsed = _load_mapping(path)
    return ByocCustomerPilotPackageManifest.model_validate(parsed)


def validate_byoc_customer_pilot_package(
    inputs: ByocCustomerPilotPackageValidationInputs,
) -> ByocCustomerPilotPackageValidationResult:
    manifest = load_byoc_customer_pilot_package_manifest(inputs.manifest_path)
    failures = _package_validation_failures(
        manifest=manifest,
        manifest_path=inputs.manifest_path,
        repo_root=inputs.repo_root,
    )
    verified_count = sum(
        1
        for artifact in manifest.artifacts
        if _artifact_path(artifact, inputs.repo_root).exists()
        and _file_digest(_artifact_path(artifact, inputs.repo_root)) == artifact.digest
    )
    return ByocCustomerPilotPackageValidationResult(
        schema_version="fyralis.byoc.customer_pilot_package_validation.v1",
        generated_at=inputs.generated_at or datetime.now(tz=UTC),
        status="pass" if not failures else "fail",
        package_status=manifest.status,
        customer_pilot_ready=manifest.customer_pilot_ready,
        manual_actions_required=manifest.manual_actions_required,
        required_checks_passed=manifest.required_checks_passed,
        deployment_id=manifest.deployment_id,
        customer_id=manifest.customer_id,
        cloud_provider=manifest.cloud_provider,
        region=manifest.region,
        artifact_revision=manifest.artifact_revision,
        manifest_path=_relative_path(inputs.manifest_path, inputs.repo_root),
        output_dir=manifest.output_dir,
        artifact_count=manifest.artifact_count,
        verified_artifact_count=verified_count,
        failure_codes=tuple(failures),
        privacy=ByocCustomerPilotPackagePrivacyContract(),
        stored_scope="sanitized_customer_pilot_package_validation_metadata_only",
    )


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


def render_customer_pilot_package_validation_json(
    validation: ByocCustomerPilotPackageValidationResult,
) -> str:
    return json.dumps(validation.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_customer_pilot_package_validation_yaml(
    validation: ByocCustomerPilotPackageValidationResult,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(
        validation.model_dump(mode="json"),
        sort_keys=False,
        width=1_000_000,
    )


def _package_validation_failures(
    *,
    manifest: ByocCustomerPilotPackageManifest,
    manifest_path: Path,
    repo_root: Path,
) -> list[str]:
    failures: list[str] = []
    manifest_relative = _relative_path(manifest_path, repo_root)
    manifest_dir = _relative_path(manifest_path.parent, repo_root)
    if manifest.output_dir != manifest_dir:
        failures.append("manifest_output_dir_mismatch")
    if manifest_relative != f"{manifest.output_dir}/byoc-customer-pilot-package-manifest.json":
        failures.append("manifest_path_unexpected")
    if manifest.artifact_count != len(manifest.artifacts):
        failures.append("artifact_count_mismatch")

    names = [artifact.name for artifact in manifest.artifacts]
    if set(names) != _EXPECTED_ARTIFACT_NAMES:
        failures.append("artifact_set_mismatch")
    if len(names) != len(set(names)):
        failures.append("duplicate_artifact_name")
    paths = [artifact.path for artifact in manifest.artifacts]
    if len(paths) != len(set(paths)):
        failures.append("duplicate_artifact_path")

    for artifact in manifest.artifacts:
        artifact_path = _artifact_path(artifact, repo_root)
        if not artifact.share_with_customer:
            failures.append(f"{artifact.name}_not_shareable")
        if artifact.contents_included is not False:
            failures.append(f"{artifact.name}_contents_included")
        if artifact.generated_by_builder and not artifact.path.startswith(
            f"{manifest.output_dir}/"
        ):
            failures.append(f"{artifact.name}_outside_output_dir")
        if not artifact_path.exists():
            failures.append(f"{artifact.name}_missing")
            continue
        if _file_digest(artifact_path) != artifact.digest:
            failures.append(f"{artifact.name}_digest_mismatch")
        schema = _artifact_schema_version(artifact_path)
        if schema != artifact.schema_version:
            failures.append(f"{artifact.name}_schema_mismatch")

    privacy = manifest.privacy.model_dump(mode="json")
    if any(value is not False for value in privacy.values()):
        failures.append("privacy_flags_not_false")
    return sorted(set(failures))


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


def _control_plane_smoke_summary_for_package(
    *,
    inputs: ByocCustomerPilotPackageInputs,
    generated_at: datetime,
    fallback: ByocControlPlaneReadSmokeSummary,
) -> ByocControlPlaneReadSmokeSummary:
    if inputs.control_plane_read_smoke_summary_path is not None:
        return _load_model(
            inputs.control_plane_read_smoke_summary_path,
            ByocControlPlaneReadSmokeSummary,
        )
    if inputs.control_plane_read_smoke_path is not None:
        return build_byoc_control_plane_read_smoke_summary(
            ByocControlPlaneReadSmokeSummaryInputs(
                control_plane_read_smoke_path=inputs.control_plane_read_smoke_path,
                generated_at=generated_at,
            )
        )
    return fallback


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


def _artifact_path(
    artifact: ByocCustomerPilotPackageArtifact,
    repo_root: Path,
) -> Path:
    return repo_root / artifact.path


def _artifact_schema_version(path: Path) -> str | None:
    parsed = _load_mapping(path)
    schema = parsed.get("schema_version")
    return str(schema) if schema is not None else None


def _load_mapping(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        parsed = json.loads(text)
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            raise RuntimeError("YAML input requires PyYAML") from exc
        parsed = yaml.safe_load(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"{path} must contain a mapping")
    return parsed


def _load_model(path: Path, model: type[_ModelT]) -> _ModelT:
    return model.model_validate(_load_mapping(path))


__all__ = [
    "ByocCustomerPilotPackageArtifact",
    "ByocCustomerPilotPackageInputs",
    "ByocCustomerPilotPackageManifest",
    "ByocCustomerPilotPackagePrivacyContract",
    "ByocCustomerPilotPackageValidationInputs",
    "ByocCustomerPilotPackageValidationResult",
    "build_byoc_customer_pilot_package",
    "load_byoc_customer_pilot_package_manifest",
    "render_customer_pilot_package_manifest_json",
    "render_customer_pilot_package_manifest_yaml",
    "render_customer_pilot_package_validation_json",
    "render_customer_pilot_package_validation_yaml",
    "validate_byoc_customer_pilot_package",
]
