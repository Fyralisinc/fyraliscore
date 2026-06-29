"""BYOC bootstrap dry-run plan contract."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_bootstrap_bundle import (
    ArtifactRole,
    ByocBootstrapArtifact,
    ByocBootstrapBundleManifest,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    CloudProvider,
    DeploymentEnvironment,
    validate_byoc_manifest_contract,
)
from services.platform.runtime.byoc_permissions import (
    ByocPermissionsManifest,
    PermissionActor,
    validate_permissions_manifest_contract,
)


PlanPhase = Literal[
    "preflight",
    "verify_artifacts",
    "prepare_identity",
    "prepare_network",
    "prepare_stateful_services",
    "install_runtime",
    "enroll_agent",
    "post_deploy_validation",
    "handoff",
]
PlanOperation = Literal[
    "validate_contracts",
    "verify_artifact_signatures",
    "verify_local_artifact_hashes",
    "inspect_permission_boundaries",
    "plan_private_network",
    "plan_stateful_services",
    "render_runtime_release",
    "prepare_agent_enrollment",
    "run_post_deploy_validation",
    "emit_handoff_summary",
]
SourceManifestKind = Literal["dataplane", "permissions", "bootstrap_bundle"]
PlanSourcePaths = Mapping[SourceManifestKind, Path]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_STEP_ID_RE = re.compile(r"^\d{2}_[a-z][a-z0-9_]{2,80}$")
_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:/@+=,\-*]+$")
_PHASE_ORDER = {
    "preflight": 1,
    "verify_artifacts": 2,
    "prepare_identity": 3,
    "prepare_network": 4,
    "prepare_stateful_services": 5,
    "install_runtime": 6,
    "enroll_agent": 7,
    "post_deploy_validation": 8,
    "handoff": 9,
}
_REQUIRED_OPERATIONS: tuple[PlanOperation, ...] = (
    "validate_contracts",
    "verify_artifact_signatures",
    "verify_local_artifact_hashes",
    "inspect_permission_boundaries",
    "plan_private_network",
    "plan_stateful_services",
    "render_runtime_release",
    "prepare_agent_enrollment",
    "run_post_deploy_validation",
    "emit_handoff_summary",
)
_FORBIDDEN_COMMAND_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bterraform\s+apply\b", re.IGNORECASE),
    re.compile(r"\bkubectl\s+apply\b", re.IGNORECASE),
    re.compile(r"\bhelm\s+(?:install|upgrade|uninstall)\b", re.IGNORECASE),
    re.compile(
        r"\baws\s+cloudformation\s+"
        r"(?:create|update|delete|execute|deploy)\S*",
        re.IGNORECASE,
    ),
    re.compile(
        r"\baws\s+\S+\s+"
        r"(?:create|delete|update|put|attach|detach|pass-role|authorize|revoke)",
        re.IGNORECASE,
    ),
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocPlanSourceManifest(_StrictModel):
    kind: SourceManifestKind
    path: str
    digest: str

    @field_validator("path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        value = value.strip()
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise ValueError("source manifest path must be repository-relative")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not re.match(r"^sha256:[0-9a-f]{64}$", value):
            raise ValueError("source manifest digest must look like sha256:<64-hex>")
        return value


class ByocBootstrapPlanStep(_StrictModel):
    order: int = Field(ge=1)
    id: str
    phase: PlanPhase
    operation: PlanOperation
    title: str
    actor: PermissionActor
    mutates_cloud: Literal[False] = False
    requires_cloud_credentials: Literal[False] = False
    requires_inbound_connectivity: Literal[False] = False
    artifact_roles: tuple[ArtifactRole, ...] = ()
    role_names: tuple[str, ...] = ()
    component_names: tuple[str, ...] = ()
    checks: tuple[str, ...]
    dry_run_commands: tuple[str, ...] = ()
    notes: str | None = None

    @field_validator("id")
    @classmethod
    def _id_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _STEP_ID_RE.match(value):
            raise ValueError("step id must look like 01_bounded_code")
        return value

    @field_validator("title")
    @classmethod
    def _title_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("step title must not be empty")
        return value

    @field_validator("role_names", "component_names", "checks")
    @classmethod
    def _tokens_must_be_safe(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in value)
        if any(not item or not _SAFE_TOKEN_RE.match(item) for item in normalized):
            raise ValueError("plan tokens must be bounded identifiers")
        return normalized

    @field_validator("dry_run_commands")
    @classmethod
    def _commands_must_be_single_line(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(command.strip() for command in value)
        if any(not command or "\n" in command or "\r" in command for command in normalized):
            raise ValueError("dry-run commands must be non-empty single lines")
        return normalized


class ByocBootstrapPlanManifest(_StrictModel):
    schema_version: Literal["fyralis.byoc.bootstrap_plan.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    execution_mode: Literal["dry-run"] = "dry-run"
    generated_at: datetime
    generated_by: Literal["fyralis-core"] = "fyralis-core"
    source_manifests: tuple[ByocPlanSourceManifest, ...]
    steps: tuple[ByocBootstrapPlanStep, ...]

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
            raise ValueError("plan fields must not be empty")
        return value

    @field_validator("source_manifests")
    @classmethod
    def _source_manifests_must_be_present(
        cls,
        value: tuple[ByocPlanSourceManifest, ...],
    ) -> tuple[ByocPlanSourceManifest, ...]:
        if not value:
            raise ValueError("source_manifests must not be empty")
        return value

    @field_validator("steps")
    @classmethod
    def _steps_must_be_present(
        cls,
        value: tuple[ByocBootstrapPlanStep, ...],
    ) -> tuple[ByocBootstrapPlanStep, ...]:
        if not value:
            raise ValueError("steps must not be empty")
        return value


@dataclass(frozen=True, slots=True)
class ByocBootstrapPlanViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def generate_bootstrap_plan(
    *,
    dataplane_manifest: ByocDataPlaneManifest,
    permissions_manifest: ByocPermissionsManifest,
    bootstrap_bundle: ByocBootstrapBundleManifest,
    source_paths: PlanSourcePaths,
    generated_at: datetime | None = None,
    repo_root: Path | None = None,
) -> ByocBootstrapPlanManifest:
    repo_root = repo_root or Path.cwd()
    generated = generated_at or datetime.now(UTC)
    artifacts_by_role = {artifact.role: artifact for artifact in bootstrap_bundle.artifacts}
    local_artifacts = tuple(
        artifact.role for artifact in bootstrap_bundle.artifacts if artifact.local_path
    )
    runtime_roles = tuple(
        role.name for role in permissions_manifest.roles if role.actor.endswith("_runtime")
    )
    private_components = tuple(dataplane_manifest.network.private_service_endpoints)

    steps = (
        ByocBootstrapPlanStep(
            order=1,
            id="01_validate_contracts",
            phase="preflight",
            operation="validate_contracts",
            title="Validate BYOC manifests locally",
            actor="customer_bootstrap_runner",
            checks=(
                "dataplane_contract",
                "permissions_contract",
                "bootstrap_bundle_contract",
            ),
            dry_run_commands=(
                "python scripts/validate_byoc_dataplane_manifest.py "
                f"{_source_path(source_paths, 'dataplane')}",
                "python scripts/validate_byoc_permissions_manifest.py "
                f"{_source_path(source_paths, 'permissions')} "
                f"--dataplane-manifest {_source_path(source_paths, 'dataplane')}",
                "python scripts/verify_byoc_bootstrap_bundle.py "
                f"{_source_path(source_paths, 'bootstrap_bundle')} "
                f"--dataplane-manifest {_source_path(source_paths, 'dataplane')} "
                f"--permissions-manifest {_source_path(source_paths, 'permissions')} "
                "--verify-local-files",
            ),
        ),
        ByocBootstrapPlanStep(
            order=2,
            id="02_verify_artifact_signatures",
            phase="verify_artifacts",
            operation="verify_artifact_signatures",
            title="Verify signed bootstrap artifacts",
            actor="customer_bootstrap_runner",
            artifact_roles=tuple(artifact.role for artifact in bootstrap_bundle.artifacts),
            checks=("sigstore_identity", "oidc_issuer", "digest_pinning"),
            dry_run_commands=tuple(
                _cosign_command(artifact) for artifact in bootstrap_bundle.artifacts
            ),
        ),
        ByocBootstrapPlanStep(
            order=3,
            id="03_verify_local_artifact_hashes",
            phase="verify_artifacts",
            operation="verify_local_artifact_hashes",
            title="Verify local IaC artifact hashes",
            actor="customer_bootstrap_runner",
            artifact_roles=local_artifacts,
            checks=("local_sha256_digest",),
            dry_run_commands=(
                "python scripts/verify_byoc_bootstrap_bundle.py "
                f"{_source_path(source_paths, 'bootstrap_bundle')} "
                "--verify-local-files",
            ),
        ),
        ByocBootstrapPlanStep(
            order=4,
            id="04_inspect_permission_boundaries",
            phase="prepare_identity",
            operation="inspect_permission_boundaries",
            title="Inspect customer-cloud role boundaries",
            actor="customer_bootstrap_runner",
            role_names=tuple(role.name for role in permissions_manifest.roles),
            checks=(
                "permissions_boundary_required",
                "no_admin_managed_policy",
                "control_plane_readonly",
                "pass_role_service_scoped",
            ),
        ),
        ByocBootstrapPlanStep(
            order=5,
            id="05_plan_private_network",
            phase="prepare_network",
            operation="plan_private_network",
            title="Plan private networking and egress-only control plane",
            actor="customer_bootstrap_runner",
            component_names=private_components,
            checks=(
                "control_plane_egress_only",
                "private_service_endpoints",
                "no_public_data_services",
            ),
        ),
        ByocBootstrapPlanStep(
            order=6,
            id="06_plan_stateful_services",
            phase="prepare_stateful_services",
            operation="plan_stateful_services",
            title="Plan stateful data-plane services",
            actor="customer_bootstrap_runner",
            component_names=tuple(
                component
                for component in private_components
                if component in {"postgres", "broker", "object_storage", "redis"}
            ),
            checks=("customer_kms_refs", "secret_refs_only", "data_residency_local"),
        ),
        ByocBootstrapPlanStep(
            order=7,
            id="07_render_runtime_release",
            phase="install_runtime",
            operation="render_runtime_release",
            title="Render runtime release without applying it",
            actor="customer_bootstrap_runner",
            artifact_roles=tuple(
                role
                for role in (
                    "gateway_image",
                    "worker_image",
                    "data_plane_agent_image",
                    "helm_chart",
                )
                if role in artifacts_by_role
            ),
            role_names=runtime_roles,
            checks=("helm_template_only", "digest_pinned_images", "runtime_identities"),
            dry_run_commands=("helm template fyralis-byoc <signed-chart-ref>",),
        ),
        ByocBootstrapPlanStep(
            order=8,
            id="08_prepare_agent_enrollment",
            phase="enroll_agent",
            operation="prepare_agent_enrollment",
            title="Prepare egress-only data-plane agent enrollment",
            actor="data_plane_agent",
            artifact_roles=("data_plane_agent_image",),
            role_names=("data_plane_agent",),
            checks=("install_token_secret_ref_only", "mtls_required", "heartbeat_contract"),
        ),
        ByocBootstrapPlanStep(
            order=9,
            id="09_run_post_deploy_validation",
            phase="post_deploy_validation",
            operation="run_post_deploy_validation",
            title="Prepare live post-deploy validation",
            actor="data_plane_agent",
            checks=("gateway_health", "worker_health", "db_rls_probe", "telemetry_privacy"),
            dry_run_commands=(
                "python scripts/run_byoc_post_deploy_validation.py "
                f"--manifest {_source_path(source_paths, 'dataplane')} "
                "--require-live",
            ),
        ),
        ByocBootstrapPlanStep(
            order=10,
            id="10_emit_handoff_summary",
            phase="handoff",
            operation="emit_handoff_summary",
            title="Emit source-onboarding handoff summary",
            actor="customer_bootstrap_runner",
            checks=(
                "source_onboarding_paused_until_validation",
                "local_dashboard_links_only",
                "no_raw_logs_to_control_plane",
            ),
        ),
    )

    return ByocBootstrapPlanManifest(
        schema_version="fyralis.byoc.bootstrap_plan.v1",
        deployment_id=dataplane_manifest.deployment_id,
        customer_id=dataplane_manifest.customer_id,
        environment=dataplane_manifest.environment,
        cloud_provider=dataplane_manifest.cloud_provider,
        region=dataplane_manifest.region,
        artifact_revision=dataplane_manifest.artifact_revision,
        execution_mode="dry-run",
        generated_at=generated,
        generated_by="fyralis-core",
        source_manifests=tuple(
            _source_manifest(kind, source_paths[kind], repo_root=repo_root)
            for kind in ("dataplane", "permissions", "bootstrap_bundle")
        ),
        steps=steps,
    )


def validate_bootstrap_plan_contract(
    plan: ByocBootstrapPlanManifest,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    permissions_manifest: ByocPermissionsManifest | None = None,
    bootstrap_bundle: ByocBootstrapBundleManifest | None = None,
    source_paths: PlanSourcePaths | None = None,
    repo_root: Path | None = None,
) -> list[ByocBootstrapPlanViolation]:
    violations: list[ByocBootstrapPlanViolation] = []
    repo_root = repo_root or Path.cwd()
    if dataplane_manifest is not None:
        violations.extend(_compare_dataplane_manifest(plan, dataplane_manifest))
        violations.extend(
            _wrap_contract_violations(
                "dataplane",
                validate_byoc_manifest_contract(dataplane_manifest),
            )
        )
    if permissions_manifest is not None:
        violations.extend(_compare_permissions_manifest(plan, permissions_manifest))
        violations.extend(
            _wrap_contract_violations(
                "permissions",
                validate_permissions_manifest_contract(
                    permissions_manifest,
                    dataplane_manifest=dataplane_manifest,
                ),
            )
        )
    if bootstrap_bundle is not None:
        violations.extend(_compare_bundle_manifest(plan, bootstrap_bundle))
        violations.extend(
            _wrap_contract_violations(
                "bootstrap_bundle",
                validate_bootstrap_bundle_contract(
                    bootstrap_bundle,
                    dataplane_manifest=dataplane_manifest,
                    permissions_manifest=permissions_manifest,
                    verify_local_files=True,
                    repo_root=repo_root,
                ),
            )
        )

    violations.extend(_validate_plan_shape(plan))
    if source_paths is not None:
        violations.extend(
            _validate_source_manifest_digests(
                plan,
                source_paths=source_paths,
                repo_root=repo_root,
            )
        )
    if (
        permissions_manifest is not None
        or bootstrap_bundle is not None
        or dataplane_manifest is not None
    ):
        violations.extend(
            _validate_step_references(
                plan,
                dataplane_manifest=dataplane_manifest,
                permissions_manifest=permissions_manifest,
                bootstrap_bundle=bootstrap_bundle,
            )
        )
    return violations


def byoc_bootstrap_plan_json_schema() -> dict[str, Any]:
    return ByocBootstrapPlanManifest.model_json_schema()


def load_byoc_bootstrap_plan(path: Path) -> ByocBootstrapPlanManifest:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC bootstrap plan must be a JSON/YAML object")
    return ByocBootstrapPlanManifest.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _validate_plan_shape(
    plan: ByocBootstrapPlanManifest,
) -> list[ByocBootstrapPlanViolation]:
    violations: list[ByocBootstrapPlanViolation] = []
    step_ids = [step.id for step in plan.steps]
    duplicate_ids = sorted({step_id for step_id in step_ids if step_ids.count(step_id) > 1})
    for step_id in duplicate_ids:
        violations.append(
            _violation("steps", "duplicate_step_id", f"{step_id!r} is duplicated")
        )
    expected_orders = list(range(1, len(plan.steps) + 1))
    actual_orders = [step.order for step in plan.steps]
    if actual_orders != expected_orders:
        violations.append(
            _violation(
                "steps",
                "step_order_not_contiguous",
                "step order must be contiguous and start at 1",
            )
        )
    phase_numbers = [_PHASE_ORDER[step.phase] for step in plan.steps]
    if phase_numbers != sorted(phase_numbers):
        violations.append(
            _violation("steps", "phase_order_regression", "step phases are out of order")
        )
    operations = {step.operation for step in plan.steps}
    for operation in _REQUIRED_OPERATIONS:
        if operation not in operations:
            violations.append(
                _violation(
                    "steps",
                    "missing_required_operation",
                    f"{operation!r} is required in the dry-run plan",
                )
            )
    for step in plan.steps:
        for command in step.dry_run_commands:
            if any(pattern.search(command) for pattern in _FORBIDDEN_COMMAND_PATTERNS):
                violations.append(
                    _violation(
                        f"steps.{step.id}.dry_run_commands",
                        "mutating_command_forbidden",
                        "dry-run plan commands must not mutate cloud resources",
                    )
                )
    return violations


def _validate_step_references(
    plan: ByocBootstrapPlanManifest,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None,
    permissions_manifest: ByocPermissionsManifest | None,
    bootstrap_bundle: ByocBootstrapBundleManifest | None,
) -> list[ByocBootstrapPlanViolation]:
    violations: list[ByocBootstrapPlanViolation] = []
    artifact_roles = (
        {artifact.role for artifact in bootstrap_bundle.artifacts}
        if bootstrap_bundle is not None
        else set()
    )
    role_names = (
        {role.name for role in permissions_manifest.roles}
        if permissions_manifest is not None
        else set()
    )
    component_names = (
        set(dataplane_manifest.network.private_service_endpoints)
        | {endpoint.component for endpoint in dataplane_manifest.network.endpoint_exposure}
        if dataplane_manifest is not None
        else set()
    )
    for step in plan.steps:
        for role in step.artifact_roles:
            if artifact_roles and role not in artifact_roles:
                violations.append(
                    _violation(
                        f"steps.{step.id}.artifact_roles",
                        "unknown_artifact_role",
                        f"{role!r} is not present in the bootstrap bundle",
                    )
                )
        for role_name in step.role_names:
            if role_names and role_name not in role_names:
                violations.append(
                    _violation(
                        f"steps.{step.id}.role_names",
                        "unknown_permission_role",
                        f"{role_name!r} is not present in permissions manifest",
                    )
                )
        for component in step.component_names:
            if component_names and component not in component_names:
                violations.append(
                    _violation(
                        f"steps.{step.id}.component_names",
                        "unknown_dataplane_component",
                        f"{component!r} is not present in data-plane network contract",
                    )
                )
    return violations


def _validate_source_manifest_digests(
    plan: ByocBootstrapPlanManifest,
    *,
    source_paths: PlanSourcePaths,
    repo_root: Path,
) -> list[ByocBootstrapPlanViolation]:
    violations: list[ByocBootstrapPlanViolation] = []
    sources_by_kind = {source.kind: source for source in plan.source_manifests}
    for kind in ("dataplane", "permissions", "bootstrap_bundle"):
        source = sources_by_kind.get(kind)
        if source is None:
            violations.append(
                _violation(
                    "source_manifests",
                    "missing_source_manifest",
                    f"{kind!r} source manifest is required",
                )
            )
            continue
        expected_path = _relative_path(source_paths[kind], repo_root=repo_root)
        if source.path != expected_path:
            violations.append(
                _violation(
                    f"source_manifests.{kind}.path",
                    "source_manifest_path_mismatch",
                    f"{kind!r} source path does not match generator input",
                )
            )
        actual_digest = _file_digest(repo_root / expected_path)
        if source.digest != actual_digest:
            violations.append(
                _violation(
                    f"source_manifests.{kind}.digest",
                    "source_manifest_digest_mismatch",
                    f"{kind!r} source digest does not match current file",
                )
            )
    return violations


def _source_manifest(
    kind: SourceManifestKind,
    path: Path,
    *,
    repo_root: Path,
) -> ByocPlanSourceManifest:
    rel = _relative_path(path, repo_root=repo_root)
    return ByocPlanSourceManifest(
        kind=kind,
        path=rel,
        digest=_file_digest(repo_root / rel),
    )


def _source_path(source_paths: PlanSourcePaths, kind: SourceManifestKind) -> str:
    return str(source_paths[kind])


def _relative_path(path: Path, *, repo_root: Path) -> str:
    resolved = path if path.is_absolute() else repo_root / path
    try:
        rel = resolved.resolve().relative_to(repo_root.resolve())
    except ValueError:
        rel = path
    return rel.as_posix()


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _cosign_command(artifact: ByocBootstrapArtifact) -> str:
    identity = (
        f"--certificate-identity {artifact.signature.certificate_identity!r} "
        f"--certificate-oidc-issuer {artifact.signature.oidc_issuer!r} "
        f"--bundle {artifact.signature.bundle_ref!r}"
    )
    if artifact.local_path is not None:
        return f"cosign verify-blob {identity} {artifact.local_path!r}"
    return f"cosign verify {identity} {artifact.ref!r}"


def _compare_dataplane_manifest(
    plan: ByocBootstrapPlanManifest,
    dataplane_manifest: ByocDataPlaneManifest,
) -> list[ByocBootstrapPlanViolation]:
    return _compare_identity(plan, dataplane_manifest, "dataplane")


def _compare_permissions_manifest(
    plan: ByocBootstrapPlanManifest,
    permissions_manifest: ByocPermissionsManifest,
) -> list[ByocBootstrapPlanViolation]:
    return _compare_identity(plan, permissions_manifest, "permissions")


def _compare_bundle_manifest(
    plan: ByocBootstrapPlanManifest,
    bootstrap_bundle: ByocBootstrapBundleManifest,
) -> list[ByocBootstrapPlanViolation]:
    return _compare_identity(plan, bootstrap_bundle, "bootstrap_bundle")


def _compare_identity(
    plan: ByocBootstrapPlanManifest,
    source: Any,
    name: str,
) -> list[ByocBootstrapPlanViolation]:
    violations: list[ByocBootstrapPlanViolation] = []
    for field in (
        "deployment_id",
        "customer_id",
        "environment",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        if getattr(plan, field) != getattr(source, field):
            violations.append(
                _violation(
                    field,
                    f"{name}_manifest_mismatch",
                    f"bootstrap plan {field} does not match {name} manifest",
                )
            )
    return violations


def _wrap_contract_violations(
    prefix: str,
    violations: list[Any],
) -> list[ByocBootstrapPlanViolation]:
    return [
        _violation(
            f"{prefix}.{violation.path}",
            f"{prefix}_{violation.code}",
            violation.message,
        )
        for violation in violations
    ]


def _violation(path: str, code: str, message: str) -> ByocBootstrapPlanViolation:
    return ByocBootstrapPlanViolation(path=path, code=code, message=message)


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
    "ByocBootstrapPlanManifest",
    "ByocBootstrapPlanStep",
    "ByocBootstrapPlanViolation",
    "ByocPlanSourceManifest",
    "byoc_bootstrap_plan_json_schema",
    "generate_bootstrap_plan",
    "load_byoc_bootstrap_plan",
    "render_validation_errors",
    "validate_bootstrap_plan_contract",
]
