"""Offline BYOC product-health collector install rehearsal.

The rehearsal validates the customer-side scheduling package before real cloud
credentials are available. It checks only local contracts, rendered artifacts,
and reference names; it never requires or serializes database URLs, signing-key
material, control-plane URL values, logs, payloads, prompts, model contents, or
PII.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_product_health_automation import (
    ByocProductHealthAutomation,
    load_product_health_automation,
    validate_product_health_automation_contract,
)


InstallMode = Literal["kubernetes", "systemd"]
InstallCheckStatus = Literal["pass", "fail", "skipped"]
InstallReportStatus = Literal["pass", "fail"]

_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,120}$")
_K8S_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,80}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:/@+=,<> -]{1,180}$")
_SYSTEMD_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,120}\.(service|timer)$")
_FORBIDDEN_RENDERED_FRAGMENTS = (
    "://customer",
    "arn:aws",
    "bearer ",
    "content_text",
    "error_context",
    "error_summary",
    "password=",
    "postgresql://",
    "raw_payload",
    "raw_prompt",
    "raw_s3_key",
    "secret=",
    "token=",
)
_FORBIDDEN_PLAN_FRAGMENTS = tuple(
    fragment
    for fragment in _FORBIDDEN_RENDERED_FRAGMENTS
    if fragment not in {"raw_payload", "raw_prompt"}
)
_KUBERNETES_INBOUND_MARKERS = (
    "\n  ports:",
    "\nports:",
    "hostNetwork: true",
    "hostPort:",
    "nodePort:",
    "type: LoadBalancer",
    "type: NodePort",
)
_SYSTEMD_INBOUND_MARKERS = (
    "Accept=true",
    "ListenDatagram=",
    "ListenStream=",
    "SocketMode=",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocProductHealthInstallRehearsalPrivacy(_StrictModel):
    raw_secret_values_included: Literal[False] = False
    endpoint_values_included: Literal[False] = False
    raw_payloads_included: Literal[False] = False
    raw_prompts_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    source_records_included: Literal[False] = False
    model_contents_included: Literal[False] = False
    vector_values_included: Literal[False] = False
    cloud_credentials_required: Literal[False] = False
    inbound_ports_required: Literal[False] = False


class ByocProductHealthKubernetesConfigKeys(_StrictModel):
    tenant_id_key: str = "tenant-id"
    control_plane_url_key: str = "control-plane-url"
    evidence_intake_key_ref_key: str = "evidence-intake-key-ref"

    @field_validator(
        "tenant_id_key",
        "control_plane_url_key",
        "evidence_intake_key_ref_key",
    )
    @classmethod
    def _key_must_be_safe(cls, value: str) -> str:
        return _bounded_key(value)


class ByocProductHealthKubernetesSecretKeys(_StrictModel):
    database_url_key: str = "database-url"
    evidence_intake_signing_key_key: str = "evidence-intake-signing-key"

    @field_validator("database_url_key", "evidence_intake_signing_key_key")
    @classmethod
    def _key_must_be_safe(cls, value: str) -> str:
        return _bounded_key(value)


class ByocProductHealthKubernetesInstall(_StrictModel):
    enabled: bool = True
    cronjob_manifest_path: str
    namespace: str
    cronjob_name: str
    config_map_name: str
    secret_name: str
    service_account_name: str
    config_map_keys: ByocProductHealthKubernetesConfigKeys = Field(
        default_factory=ByocProductHealthKubernetesConfigKeys
    )
    secret_keys: ByocProductHealthKubernetesSecretKeys = Field(
        default_factory=ByocProductHealthKubernetesSecretKeys
    )
    raw_values_included: Literal[False] = False
    inbound_ports_allowed: Literal[False] = False

    @field_validator("cronjob_manifest_path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)

    @field_validator(
        "namespace",
        "cronjob_name",
        "config_map_name",
        "secret_name",
        "service_account_name",
    )
    @classmethod
    def _k8s_name_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _K8S_NAME_RE.match(value):
            raise ValueError("Kubernetes names must be bounded DNS labels")
        return value


class ByocProductHealthSystemdInstall(_StrictModel):
    enabled: bool = True
    service_unit_path: str
    timer_unit_path: str
    service_unit: str
    timer_unit: str
    environment_file: str
    required_environment_names: tuple[str, ...]
    secret_environment_names: tuple[str, ...]
    raw_values_included: Literal[False] = False
    inbound_ports_allowed: Literal[False] = False

    @field_validator("service_unit_path", "timer_unit_path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)

    @field_validator("service_unit", "timer_unit")
    @classmethod
    def _unit_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _SYSTEMD_UNIT_RE.match(value):
            raise ValueError("systemd units must be bounded .service/.timer names")
        return value

    @field_validator("environment_file")
    @classmethod
    def _environment_file_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "environment_file")

    @field_validator("required_environment_names", "secret_environment_names")
    @classmethod
    def _env_names_must_be_safe(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("environment name lists must not be empty")
        normalized = tuple(item.strip() for item in value)
        if any(not _ENV_NAME_RE.match(item) for item in normalized):
            raise ValueError("environment names must be upper-case codes")
        if len(normalized) != len(set(normalized)):
            raise ValueError("environment name lists must not contain duplicates")
        return normalized


class ByocProductHealthInstallRehearsalPlan(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health_install_rehearsal.v1"]
    deployment_id: str
    customer_id: str
    artifact_revision: str
    automation_manifest_path: str
    target_modes: tuple[InstallMode, ...] = ("kubernetes", "systemd")
    kubernetes: ByocProductHealthKubernetesInstall
    systemd: ByocProductHealthSystemdInstall
    privacy: ByocProductHealthInstallRehearsalPrivacy = Field(
        default_factory=ByocProductHealthInstallRehearsalPrivacy
    )
    stored_scope: Literal[
        "customer_side_product_health_install_rehearsal_metadata_only"
    ] = "customer_side_product_health_install_rehearsal_metadata_only"

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

    @field_validator("artifact_revision")
    @classmethod
    def _artifact_revision_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "artifact_revision")

    @field_validator("automation_manifest_path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)

    @field_validator("target_modes")
    @classmethod
    def _target_modes_must_be_unique(
        cls,
        value: tuple[InstallMode, ...],
    ) -> tuple[InstallMode, ...]:
        if not value:
            raise ValueError("target_modes must not be empty")
        if len(value) != len(set(value)):
            raise ValueError("target_modes must not contain duplicates")
        return value


class ByocProductHealthInstallRehearsalCheck(_StrictModel):
    name: str
    status: InstallCheckStatus
    details: str
    required: bool = True

    @field_validator("name")
    @classmethod
    def _name_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "check name")

    @field_validator("details")
    @classmethod
    def _details_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "check details")


class ByocProductHealthInstallRehearsalReport(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health_install_rehearsal_report.v1"]
    generated_at: datetime
    status: InstallReportStatus
    deployment_id: str
    customer_id: str
    artifact_revision: str
    target_modes: tuple[InstallMode, ...]
    check_count: int = Field(ge=0)
    failed_check_count: int = Field(ge=0)
    checks: tuple[ByocProductHealthInstallRehearsalCheck, ...]
    next_actions: tuple[str, ...]
    privacy: ByocProductHealthInstallRehearsalPrivacy
    stored_scope: Literal[
        "customer_side_product_health_install_rehearsal_metadata_only"
    ] = "customer_side_product_health_install_rehearsal_metadata_only"

    @field_validator("next_actions")
    @classmethod
    def _next_actions_must_be_safe(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 20:
            raise ValueError("next_actions must be bounded")
        return tuple(_safe_code(action, "next action") for action in value)


@dataclass(frozen=True, slots=True)
class ByocProductHealthInstallRehearsalInputs:
    install_plan_path: Path = Path(
        "deploy/byoc/product-health-install-rehearsal.example.yaml"
    )
    repo_root: Path = field(default_factory=Path.cwd)
    generated_at: datetime | None = None


def run_product_health_install_rehearsal(
    inputs: ByocProductHealthInstallRehearsalInputs,
) -> ByocProductHealthInstallRehearsalReport:
    plan = load_product_health_install_rehearsal_plan(inputs.install_plan_path)
    repo_root = inputs.repo_root.resolve()
    automation = load_product_health_automation(repo_root / plan.automation_manifest_path)
    checks: list[ByocProductHealthInstallRehearsalCheck] = []

    checks.extend(_identity_checks(plan, automation))
    checks.extend(_automation_checks(automation, repo_root=repo_root))
    checks.extend(_plan_privacy_checks(plan))
    if "kubernetes" in plan.target_modes:
        checks.extend(_kubernetes_checks(plan, automation, repo_root=repo_root))
    else:
        checks.append(_check("kubernetes_target", "skipped", "Kubernetes target not selected."))
    if "systemd" in plan.target_modes:
        checks.extend(_systemd_checks(plan, automation, repo_root=repo_root))
    else:
        checks.append(_check("systemd_target", "skipped", "Systemd target not selected."))

    failed = [check for check in checks if check.status == "fail"]
    return ByocProductHealthInstallRehearsalReport(
        schema_version="fyralis.byoc.product_health_install_rehearsal_report.v1",
        generated_at=inputs.generated_at or datetime.now(tz=UTC),
        status="fail" if failed else "pass",
        deployment_id=plan.deployment_id,
        customer_id=plan.customer_id,
        artifact_revision=plan.artifact_revision,
        target_modes=plan.target_modes,
        check_count=len(checks),
        failed_check_count=len(failed),
        checks=tuple(checks),
        next_actions=_next_actions(failed),
        privacy=ByocProductHealthInstallRehearsalPrivacy(),
        stored_scope="customer_side_product_health_install_rehearsal_metadata_only",
    )


def load_product_health_install_rehearsal_plan(
    path: Path,
) -> ByocProductHealthInstallRehearsalPlan:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC product-health install rehearsal must be an object")
    return ByocProductHealthInstallRehearsalPlan.model_validate(data)


def product_health_install_rehearsal_json_schema() -> dict[str, Any]:
    return {
        "plan": ByocProductHealthInstallRehearsalPlan.model_json_schema(),
        "report": ByocProductHealthInstallRehearsalReport.model_json_schema(),
        "stored_scope": "customer_side_product_health_install_rehearsal_metadata_only",
    }


def render_product_health_install_rehearsal_json(
    report: ByocProductHealthInstallRehearsalReport,
) -> str:
    return json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_product_health_install_rehearsal_yaml(
    report: ByocProductHealthInstallRehearsalReport,
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


def _identity_checks(
    plan: ByocProductHealthInstallRehearsalPlan,
    automation: ByocProductHealthAutomation,
) -> list[ByocProductHealthInstallRehearsalCheck]:
    checks: list[ByocProductHealthInstallRehearsalCheck] = []
    for field_name in ("deployment_id", "customer_id", "artifact_revision"):
        checks.append(
            _check(
                f"{field_name}_matches_automation",
                (
                    "pass"
                    if getattr(plan, field_name) == getattr(automation, field_name)
                    else "fail"
                ),
                f"{field_name} matches automation manifest.",
            )
        )
    return checks


def _automation_checks(
    automation: ByocProductHealthAutomation,
    *,
    repo_root: Path,
) -> list[ByocProductHealthInstallRehearsalCheck]:
    violations = validate_product_health_automation_contract(
        automation,
        repo_root=repo_root,
    )
    return [
        _check(
            "automation_contract",
            "pass" if not violations else "fail",
            "Product-health automation contract is valid.",
        )
    ]


def _plan_privacy_checks(
    plan: ByocProductHealthInstallRehearsalPlan,
) -> list[ByocProductHealthInstallRehearsalCheck]:
    rendered = json.dumps(plan.model_dump(mode="json"), sort_keys=True)
    return [
        _check(
            "install_plan_raw_values_absent",
            "pass"
            if not _contains_forbidden_value(
                rendered,
                forbidden_fragments=_FORBIDDEN_PLAN_FRAGMENTS,
            )
            else "fail",
            "Install plan contains references only.",
        ),
        _check(
            "install_plan_privacy_flags_false",
            "pass"
            if all(value is False for value in plan.privacy.model_dump(mode="json").values())
            else "fail",
            "Install plan privacy flags are pinned false.",
        ),
    ]


def _kubernetes_checks(
    plan: ByocProductHealthInstallRehearsalPlan,
    automation: ByocProductHealthAutomation,
    *,
    repo_root: Path,
) -> list[ByocProductHealthInstallRehearsalCheck]:
    if not plan.kubernetes.enabled:
        return [_check("kubernetes_enabled", "fail", "Kubernetes target is disabled.")]
    path = repo_root / plan.kubernetes.cronjob_manifest_path
    if not path.exists():
        return [_check("kubernetes_cronjob_present", "fail", "Kubernetes CronJob is missing.")]
    text = path.read_text(encoding="utf-8")
    required_fragments = (
        plan.kubernetes.namespace,
        plan.kubernetes.cronjob_name,
        plan.kubernetes.config_map_name,
        plan.kubernetes.secret_name,
        plan.kubernetes.service_account_name,
        plan.kubernetes.config_map_keys.tenant_id_key,
        plan.kubernetes.config_map_keys.control_plane_url_key,
        plan.kubernetes.config_map_keys.evidence_intake_key_ref_key,
        plan.kubernetes.secret_keys.database_url_key,
        plan.kubernetes.secret_keys.evidence_intake_signing_key_key,
        automation.env.database_url_env,
        automation.env.signing_key_env,
        automation.control_plane.submit_path,
        "automountServiceAccountToken: false",
        "concurrencyPolicy: Forbid",
        "readOnlyRootFilesystem: true",
        "runAsNonRoot: true",
    )
    return [
        _check("kubernetes_cronjob_present", "pass", "Kubernetes CronJob is present."),
        _check(
            "kubernetes_required_refs_present",
            "pass" if all(fragment in text for fragment in required_fragments) else "fail",
            "Kubernetes CronJob contains required config and secret references.",
        ),
        _check(
            "kubernetes_no_inbound_ports",
            "pass" if not any(marker in text for marker in _KUBERNETES_INBOUND_MARKERS) else "fail",
            "Kubernetes CronJob exposes no inbound ports.",
        ),
        _check(
            "kubernetes_raw_values_absent",
            "pass" if not _contains_forbidden_value(text) else "fail",
            "Kubernetes CronJob contains references only.",
        ),
    ]


def _systemd_checks(
    plan: ByocProductHealthInstallRehearsalPlan,
    automation: ByocProductHealthAutomation,
    *,
    repo_root: Path,
) -> list[ByocProductHealthInstallRehearsalCheck]:
    if not plan.systemd.enabled:
        return [_check("systemd_enabled", "fail", "Systemd target is disabled.")]
    service_path = repo_root / plan.systemd.service_unit_path
    timer_path = repo_root / plan.systemd.timer_unit_path
    if not service_path.exists() or not timer_path.exists():
        return [_check("systemd_units_present", "fail", "Systemd units are missing.")]
    service_text = service_path.read_text(encoding="utf-8")
    timer_text = timer_path.read_text(encoding="utf-8")
    required_service_fragments = (
        f"EnvironmentFile={plan.systemd.environment_file}",
        f"User={automation.systemd.user}",
        f"Group={automation.systemd.group}",
        "ExecStart=/bin/sh -lc",
        automation.env.database_url_env,
        automation.env.signing_key_env,
        automation.control_plane.submit_path,
        "NoNewPrivileges=true",
        "ProtectSystem=strict",
        "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX",
    )
    env_present = all(
        name in service_text for name in plan.systemd.required_environment_names
    )
    secret_env_present = all(
        name in service_text for name in plan.systemd.secret_environment_names
    )
    return [
        _check("systemd_units_present", "pass", "Systemd service and timer are present."),
        _check(
            "systemd_required_refs_present",
            "pass"
            if all(fragment in service_text for fragment in required_service_fragments)
            and env_present
            and secret_env_present
            and f"Unit={plan.systemd.service_unit}" in timer_text
            else "fail",
            "Systemd units contain required env references.",
        ),
        _check(
            "systemd_no_inbound_sockets",
            "pass"
            if not any(
                marker in service_text + timer_text
                for marker in _SYSTEMD_INBOUND_MARKERS
            )
            else "fail",
            "Systemd units define no inbound sockets.",
        ),
        _check(
            "systemd_raw_values_absent",
            "pass" if not _contains_forbidden_value(service_text + timer_text) else "fail",
            "Systemd units contain references only.",
        ),
    ]


def _next_actions(
    failed: list[ByocProductHealthInstallRehearsalCheck],
) -> tuple[str, ...]:
    if not failed:
        return ()
    actions = []
    for check in failed:
        if "raw_values" in check.name:
            actions.append("remove_raw_values_from_install_plan")
        elif "inbound" in check.name:
            actions.append("remove_inbound_listener_from_install_artifact")
        elif "kubernetes" in check.name:
            actions.append("fix_kubernetes_product_health_install_refs")
        elif "systemd" in check.name:
            actions.append("fix_systemd_product_health_install_refs")
        elif "automation" in check.name:
            actions.append("regenerate_product_health_automation_package")
        else:
            actions.append("fix_product_health_install_rehearsal_contract")
    return tuple(sorted(set(actions)))


def _contains_forbidden_value(
    text: str,
    *,
    forbidden_fragments: tuple[str, ...] = _FORBIDDEN_RENDERED_FRAGMENTS,
) -> bool:
    lowered = text.lower()
    return any(fragment in lowered for fragment in forbidden_fragments)


def _check(
    name: str,
    status: InstallCheckStatus,
    details: str,
) -> ByocProductHealthInstallRehearsalCheck:
    return ByocProductHealthInstallRehearsalCheck(
        name=name,
        status=status,
        details=details,
        required=True,
    )


def _safe_code(value: str, label: str) -> str:
    value = value.strip()
    if not value or not _SAFE_CODE_RE.match(value):
        raise ValueError(f"{label} must be bounded metadata")
    if _contains_forbidden_value(value):
        raise ValueError(f"{label} must not include raw values")
    return value


def _bounded_key(value: str) -> str:
    value = value.strip()
    if not _KEY_RE.match(value) or _contains_forbidden_value(value):
        raise ValueError("install reference keys must be bounded metadata")
    return value


def _relative_path(value: str) -> str:
    value = value.strip()
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError("paths must be relative and stay inside the repo")
    if _contains_forbidden_value(value):
        raise ValueError("paths must not include raw values")
    return value


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(raw)
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            raise RuntimeError("YAML input requires PyYAML") from exc
        data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError("BYOC product-health install rehearsal must be a mapping")
    return dict(data)


__all__ = [
    "ByocProductHealthInstallRehearsalInputs",
    "ByocProductHealthInstallRehearsalPlan",
    "ByocProductHealthInstallRehearsalReport",
    "load_product_health_install_rehearsal_plan",
    "product_health_install_rehearsal_json_schema",
    "render_product_health_install_rehearsal_json",
    "render_product_health_install_rehearsal_yaml",
    "run_product_health_install_rehearsal",
]
