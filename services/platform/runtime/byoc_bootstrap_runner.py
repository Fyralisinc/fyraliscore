"""Local BYOC bootstrap-runner dry-run evidence report.

The real customer-side bootstrap runner will eventually verify signatures and
apply customer-cloud resources. This module is deliberately narrower: it
consumes the checked dry-run plan, performs only in-process local validations,
and emits a sanitized evidence report suitable for CI or customer handoff.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
    load_byoc_bootstrap_bundle,
    render_validation_errors as render_bundle_validation_errors,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_bootstrap_plan import (
    ByocBootstrapPlanManifest,
    ByocBootstrapPlanStep,
    PlanOperation,
    generate_bootstrap_plan,
    load_byoc_bootstrap_plan,
    render_validation_errors as render_plan_validation_errors,
    validate_bootstrap_plan_contract,
)
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    effective_runtime_processes,
    load_byoc_manifest,
    render_validation_errors as render_dataplane_validation_errors,
    validate_byoc_manifest_contract,
)
from services.platform.runtime.byoc_permissions import (
    ByocPermissionsManifest,
    load_byoc_permissions_manifest,
    render_validation_errors as render_permissions_validation_errors,
    validate_permissions_manifest_contract,
)
from services.platform.runtime.byoc_validation import (
    ByocValidationInputs,
    run_byoc_post_deploy_validation,
)


RunnerStatus = Literal["pass", "fail", "skipped"]

_PASS: RunnerStatus = "pass"
_FAIL: RunnerStatus = "fail"
_LOCAL_HASH_CODES = {
    "local_artifact_missing",
    "local_artifact_digest_mismatch",
    "local_template_path_required",
}
_STATEFUL_COMPONENTS = {"postgres", "broker", "object_storage", "redis"}


@dataclass(frozen=True, slots=True)
class ByocBootstrapRunnerCheck:
    name: str
    status: RunnerStatus
    required: bool
    details: str
    step_id: str | None = None
    operation: str | None = None
    metrics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ByocBootstrapRunnerReport:
    status: RunnerStatus
    required_checks_passed: bool
    plan_path: str
    dataplane_manifest_path: str
    permissions_manifest_path: str
    bootstrap_bundle_path: str
    elapsed_seconds: float
    deployment_id: str | None
    customer_id: str | None
    environment: str | None
    cloud_provider: str | None
    region: str | None
    artifact_revision: str | None
    execution_mode: str | None
    checks: list[ByocBootstrapRunnerCheck]

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ByocBootstrapRunnerInputs:
    plan_path: Path
    dataplane_manifest_path: Path
    permissions_manifest_path: Path
    bootstrap_bundle_path: Path
    repo_root: Path = field(default_factory=Path.cwd)
    env_path: Path | None = None


def run_byoc_bootstrap_runner(
    inputs: ByocBootstrapRunnerInputs,
) -> ByocBootstrapRunnerReport:
    started = time.monotonic()
    checks: list[ByocBootstrapRunnerCheck] = []

    plan, plan_checks = _load_plan(inputs.plan_path)
    dataplane, dataplane_checks = _load_dataplane(inputs.dataplane_manifest_path)
    permissions, permissions_checks = _load_permissions(
        inputs.permissions_manifest_path
    )
    bundle, bundle_checks = _load_bundle(inputs.bootstrap_bundle_path)
    checks.extend(plan_checks)
    checks.extend(dataplane_checks)
    checks.extend(permissions_checks)
    checks.extend(bundle_checks)

    if (
        plan is not None
        and dataplane is not None
        and permissions is not None
        and bundle is not None
    ):
        checks.extend(
            _evaluate_plan(
                plan,
                dataplane=dataplane,
                permissions=permissions,
                bundle=bundle,
                inputs=inputs,
            )
        )

    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    status: RunnerStatus = _PASS if required_checks_passed else _FAIL
    identity = _report_identity(plan, dataplane, permissions, bundle)
    return ByocBootstrapRunnerReport(
        status=status,
        required_checks_passed=required_checks_passed,
        plan_path=str(inputs.plan_path),
        dataplane_manifest_path=str(inputs.dataplane_manifest_path),
        permissions_manifest_path=str(inputs.permissions_manifest_path),
        bootstrap_bundle_path=str(inputs.bootstrap_bundle_path),
        elapsed_seconds=round(time.monotonic() - started, 3),
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        environment=identity.get("environment"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        execution_mode=identity.get("execution_mode"),
        checks=checks,
    )


def render_runner_report_json(report: ByocBootstrapRunnerReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_runner_report_yaml(report: ByocBootstrapRunnerReport) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(report.as_json(), sort_keys=False, width=1_000_000)


def _load_plan(
    path: Path,
) -> tuple[ByocBootstrapPlanManifest | None, list[ByocBootstrapRunnerCheck]]:
    try:
        plan = load_byoc_bootstrap_plan(path)
    except ValidationError as exc:
        return None, [_schema_failure("bootstrap_plan_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("bootstrap_plan_schema", exc)]
    return plan, [
        _check(
            "bootstrap_plan_schema",
            _PASS,
            required=True,
            details="BYOC bootstrap plan schema is valid.",
            metrics={"step_count": len(plan.steps)},
        )
    ]


def _load_dataplane(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocBootstrapRunnerCheck]]:
    try:
        manifest = load_byoc_manifest(path)
    except ValidationError as exc:
        return None, [_schema_failure("dataplane_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("dataplane_manifest_schema", exc)]
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
) -> tuple[ByocPermissionsManifest | None, list[ByocBootstrapRunnerCheck]]:
    try:
        manifest = load_byoc_permissions_manifest(path)
    except ValidationError as exc:
        return None, [_schema_failure("permissions_manifest_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("permissions_manifest_schema", exc)]
    return manifest, [
        _check(
            "permissions_manifest_schema",
            _PASS,
            required=True,
            details="BYOC permissions manifest schema is valid.",
            metrics={"role_count": len(manifest.roles)},
        )
    ]


def _load_bundle(
    path: Path,
) -> tuple[ByocBootstrapBundleManifest | None, list[ByocBootstrapRunnerCheck]]:
    try:
        bundle = load_byoc_bootstrap_bundle(path)
    except ValidationError as exc:
        return None, [_schema_failure("bootstrap_bundle_schema", exc)]
    except Exception as exc:  # noqa: BLE001
        return None, [_load_failure("bootstrap_bundle_schema", exc)]
    return bundle, [
        _check(
            "bootstrap_bundle_schema",
            _PASS,
            required=True,
            details="BYOC bootstrap bundle schema is valid.",
            metrics={"artifact_count": len(bundle.artifacts)},
        )
    ]


def _evaluate_plan(
    plan: ByocBootstrapPlanManifest,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
) -> list[ByocBootstrapRunnerCheck]:
    repo_root = inputs.repo_root.resolve()
    source_paths = _source_paths_from_inputs(inputs, repo_root=repo_root)
    plan_violations = validate_bootstrap_plan_contract(
        plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths=source_paths,
        repo_root=repo_root,
    )
    checks = [
        _violations_check(
            "bootstrap_plan_contract",
            plan_violations,
            details="BYOC bootstrap plan is local-only and matches source manifests.",
            metrics={"step_count": len(plan.steps)},
        )
    ]

    generated = generate_bootstrap_plan(
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths=source_paths,
        generated_at=plan.generated_at,
        repo_root=repo_root,
    )
    checks.append(
        _check(
            "generated_plan_drift",
            (
                _PASS
                if plan.model_dump(mode="json") == generated.model_dump(mode="json")
                else _FAIL
            ),
            required=True,
            details=(
                "Checked plan matches deterministic generator output."
                if plan.model_dump(mode="json") == generated.model_dump(mode="json")
                else "Checked plan drifted from deterministic generator output."
            ),
        )
    )

    previous_required_failed = any(
        check.status == _FAIL for check in checks if check.required
    )
    for step in plan.steps:
        step_checks = _evaluate_step(
            step,
            dataplane=dataplane,
            permissions=permissions,
            bundle=bundle,
            inputs=inputs,
            previous_required_failed=previous_required_failed,
        )
        checks.extend(step_checks)
        previous_required_failed = previous_required_failed or any(
            check.status == _FAIL for check in step_checks if check.required
        )
    return checks


def _source_paths_from_inputs(
    inputs: ByocBootstrapRunnerInputs,
    *,
    repo_root: Path,
) -> dict[str, Path]:
    return {
        "dataplane": _repo_relative_path(
            inputs.dataplane_manifest_path,
            repo_root=repo_root,
        ),
        "permissions": _repo_relative_path(
            inputs.permissions_manifest_path,
            repo_root=repo_root,
        ),
        "bootstrap_bundle": _repo_relative_path(
            inputs.bootstrap_bundle_path,
            repo_root=repo_root,
        ),
    }


def _repo_relative_path(path: Path, *, repo_root: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    try:
        return Path(resolved.resolve().relative_to(repo_root).as_posix())
    except ValueError:
        return path


def _evaluate_step(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> list[ByocBootstrapRunnerCheck]:
    evaluators = {
        "validate_contracts": _step_validate_contracts,
        "verify_artifact_signatures": _step_verify_artifact_signatures,
        "verify_local_artifact_hashes": _step_verify_local_artifact_hashes,
        "inspect_permission_boundaries": _step_inspect_permission_boundaries,
        "plan_private_network": _step_plan_private_network,
        "plan_stateful_services": _step_plan_stateful_services,
        "render_runtime_release": _step_render_runtime_release,
        "prepare_agent_enrollment": _step_prepare_agent_enrollment,
        "run_post_deploy_validation": _step_run_post_deploy_validation,
        "emit_handoff_summary": _step_emit_handoff_summary,
    }
    return [
        evaluators[step.operation](
            step,
            dataplane=dataplane,
            permissions=permissions,
            bundle=bundle,
            inputs=inputs,
            previous_required_failed=previous_required_failed,
        )
    ]


def _step_validate_contracts(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del previous_required_failed
    violations = [
        *validate_byoc_manifest_contract(dataplane),
        *validate_permissions_manifest_contract(
            permissions,
            dataplane_manifest=dataplane,
        ),
        *validate_bootstrap_bundle_contract(
            bundle,
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            verify_local_files=True,
            repo_root=inputs.repo_root.resolve(),
        ),
    ]
    return _step_violations_check(
        step,
        violations,
        details="Data-plane, permissions, and bundle contracts passed locally.",
        metrics={"contract_count": 3},
    )


def _step_verify_artifact_signatures(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del dataplane, permissions, inputs, previous_required_failed
    artifact_roles = {artifact.role for artifact in bundle.artifacts}
    missing = sorted(set(step.artifact_roles) - artifact_roles)
    command_count_ok = len(step.dry_run_commands) == len(step.artifact_roles)
    status = _PASS if not missing and command_count_ok else _FAIL
    details = "Signature verification commands are prepared but not executed locally."
    if missing:
        details = "Plan references artifact roles missing from the bundle."
    elif not command_count_ok:
        details = "Signature command count does not match planned artifact roles."
    return _step_check(
        step,
        status,
        details=details,
        metrics={
            "artifact_count": len(step.artifact_roles),
            "prepared_commands": len(step.dry_run_commands),
            "executed_commands": 0,
        },
    )


def _step_verify_local_artifact_hashes(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del dataplane, permissions, previous_required_failed
    violations = [
        violation
        for violation in validate_bootstrap_bundle_contract(
            bundle,
            verify_local_files=True,
            repo_root=inputs.repo_root.resolve(),
        )
        if violation.code in _LOCAL_HASH_CODES
    ]
    local_artifacts = [artifact for artifact in bundle.artifacts if artifact.local_path]
    return _step_violations_check(
        step,
        violations,
        details="Local checked-in artifact hashes match the bundle manifest.",
        metrics={"local_artifact_count": len(local_artifacts)},
    )


def _step_inspect_permission_boundaries(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del bundle, inputs, previous_required_failed
    violations = validate_permissions_manifest_contract(
        permissions,
        dataplane_manifest=dataplane,
    )
    grant_count = sum(len(role.grants) for role in permissions.roles)
    return _step_violations_check(
        step,
        violations,
        details="Permission boundaries and customer-data isolation passed locally.",
        metrics={"role_count": len(permissions.roles), "grant_count": grant_count},
    )


def _step_plan_private_network(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del permissions, bundle, inputs, previous_required_failed
    violations = validate_byoc_manifest_contract(dataplane)
    customer_ingress = [
        endpoint
        for endpoint in dataplane.network.endpoint_exposure
        if endpoint.exposure == "customer_ingress"
    ]
    return _step_violations_check(
        step,
        violations,
        details="Network contract is egress-only with private data-plane services.",
        metrics={
            "private_service_endpoint_count": len(
                dataplane.network.private_service_endpoints
            ),
            "customer_ingress_count": len(customer_ingress),
        },
    )


def _step_plan_stateful_services(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del permissions, bundle, inputs, previous_required_failed
    violations = validate_byoc_manifest_contract(dataplane)
    present_stateful = sorted(
        set(dataplane.network.private_service_endpoints) & _STATEFUL_COMPONENTS
    )
    return _step_violations_check(
        step,
        violations,
        details="Stateful services stay private and use managed secret refs.",
        metrics={
            "stateful_component_count": len(present_stateful),
            "raw_env_secrets_allowed": dataplane.secrets.raw_env_secrets_allowed,
        },
    )


def _step_render_runtime_release(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del bundle, inputs, previous_required_failed
    contract_violations = [
        *validate_byoc_manifest_contract(dataplane),
        *validate_permissions_manifest_contract(
            permissions,
            dataplane_manifest=dataplane,
        ),
    ]
    enabled_processes = effective_runtime_processes(dataplane)
    return _step_violations_check(
        step,
        contract_violations,
        details="Runtime release inputs are digest-pinned and template-only.",
        metrics={
            "runtime_process_count": len(enabled_processes),
            "artifact_count": len(step.artifact_roles),
            "prepared_commands": len(step.dry_run_commands),
            "executed_commands": 0,
        },
    )


def _step_prepare_agent_enrollment(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del permissions, bundle, inputs, previous_required_failed
    violations = validate_byoc_manifest_contract(dataplane)
    return _step_violations_check(
        step,
        violations,
        details="Agent enrollment uses mTLS and managed secret references only.",
        metrics={
            "agent_auth": dataplane.connectivity.auth,
            "raw_env_secrets_allowed": dataplane.secrets.raw_env_secrets_allowed,
        },
    )


def _step_run_post_deploy_validation(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del dataplane, permissions, bundle, previous_required_failed
    validation_report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=inputs.dataplane_manifest_path,
            env_path=inputs.env_path,
            require_live=False,
        )
    )
    status = _PASS if validation_report.required_checks_passed else _FAIL
    return _step_check(
        step,
        status,
        details=(
            "Offline post-deploy validation passed; live probes remain customer-side."
            if status == _PASS
            else "Offline post-deploy validation failed."
        ),
        metrics={
            "validation_checks": len(validation_report.checks),
            "live_probe_checks_executed": 0,
        },
    )


def _step_emit_handoff_summary(
    step: ByocBootstrapPlanStep,
    *,
    dataplane: ByocDataPlaneManifest,
    permissions: ByocPermissionsManifest,
    bundle: ByocBootstrapBundleManifest,
    inputs: ByocBootstrapRunnerInputs,
    previous_required_failed: bool,
) -> ByocBootstrapRunnerCheck:
    del dataplane, permissions, bundle, inputs
    status = _FAIL if previous_required_failed else _PASS
    return _step_check(
        step,
        status,
        details=(
            "Sanitized handoff report is ready for CI or customer evidence."
            if status == _PASS
            else "Handoff blocked by an earlier required local validation failure."
        ),
        metrics={"raw_customer_data_included": False},
    )


def _schema_failure(
    name: str,
    exc: ValidationError,
) -> ByocBootstrapRunnerCheck:
    renderers = {
        "bootstrap_plan_schema": render_plan_validation_errors,
        "dataplane_manifest_schema": render_dataplane_validation_errors,
        "permissions_manifest_schema": render_permissions_validation_errors,
        "bootstrap_bundle_schema": render_bundle_validation_errors,
    }
    return _check(
        name,
        _FAIL,
        required=True,
        details="; ".join(renderers[name](exc)),
    )


def _load_failure(name: str, exc: Exception) -> ByocBootstrapRunnerCheck:
    return _check(
        name,
        _FAIL,
        required=True,
        details=f"{type(exc).__name__}: {_sanitize_exception_message(exc)}",
    )


def _violations_check(
    name: str,
    violations: list[object],
    *,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocBootstrapRunnerCheck:
    if violations:
        return _check(
            name,
            _FAIL,
            required=True,
            details=_violation_codes(violations),
            metrics={**(metrics or {}), "violation_count": len(violations)},
        )
    return _check(name, _PASS, required=True, details=details, metrics=metrics or {})


def _step_violations_check(
    step: ByocBootstrapPlanStep,
    violations: list[object],
    *,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocBootstrapRunnerCheck:
    if violations:
        return _step_check(
            step,
            _FAIL,
            details=_violation_codes(violations),
            metrics={**(metrics or {}), "violation_count": len(violations)},
        )
    return _step_check(step, _PASS, details=details, metrics=metrics or {})


def _step_check(
    step: ByocBootstrapPlanStep,
    status: RunnerStatus,
    *,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocBootstrapRunnerCheck:
    return _check(
        f"step.{step.id}",
        status,
        required=True,
        details=details,
        step_id=step.id,
        operation=step.operation,
        metrics={
            "order": step.order,
            "phase": step.phase,
            "checks": len(step.checks),
            **(metrics or {}),
        },
    )


def _check(
    name: str,
    status: RunnerStatus,
    *,
    required: bool,
    details: str,
    step_id: str | None = None,
    operation: PlanOperation | str | None = None,
    metrics: dict[str, object] | None = None,
) -> ByocBootstrapRunnerCheck:
    return ByocBootstrapRunnerCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        step_id=step_id,
        operation=operation,
        metrics=metrics or {},
    )


def _violation_codes(violations: list[object], *, limit: int = 12) -> str:
    rendered: list[str] = []
    for violation in violations[:limit]:
        path = getattr(violation, "path", "<root>")
        code = getattr(violation, "code", type(violation).__name__)
        rendered.append(f"{path}:{code}")
    if len(violations) > limit:
        rendered.append(f"+{len(violations) - limit} more")
    return "; ".join(rendered)


def _sanitize_exception_message(exc: Exception) -> str:
    return str(exc).replace("\n", " ")[:500]


def _report_identity(*sources: object | None) -> dict[str, str | None]:
    for source in sources:
        if source is None:
            continue
        identity = {
            "deployment_id": getattr(source, "deployment_id", None),
            "customer_id": getattr(source, "customer_id", None),
            "environment": getattr(source, "environment", None),
            "cloud_provider": getattr(source, "cloud_provider", None),
            "region": getattr(source, "region", None),
            "artifact_revision": getattr(source, "artifact_revision", None),
            "execution_mode": getattr(source, "execution_mode", None),
        }
        return {key: None if value is None else str(value) for key, value in identity.items()}
    return {
        "deployment_id": None,
        "customer_id": None,
        "environment": None,
        "cloud_provider": None,
        "region": None,
        "artifact_revision": None,
        "execution_mode": None,
    }


__all__ = [
    "ByocBootstrapRunnerCheck",
    "ByocBootstrapRunnerInputs",
    "ByocBootstrapRunnerReport",
    "render_runner_report_json",
    "render_runner_report_yaml",
    "run_byoc_bootstrap_runner",
]
