"""BYOC product-health collector automation contracts.

This module describes how a customer data plane should schedule
``scripts/run_byoc_product_health_collector.py``. The contract is intentionally
metadata-only: it carries environment variable names, customer-side secret
references, schedules, and artifact paths, but never raw DSNs, signing-key
material, endpoint values, logs, payloads, or model contents.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    DeploymentEnvironment,
)


_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:/@+=,<> -]{1,180}$")
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,120}$")
_K8S_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_SYSTEMD_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,120}\.(service|timer)$")
_FORBIDDEN_RENDERED_FRAGMENTS = (
    "://customer",
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
_FORBIDDEN_MANIFEST_FRAGMENTS = tuple(
    fragment
    for fragment in _FORBIDDEN_RENDERED_FRAGMENTS
    if fragment not in {"raw_payload", "raw_prompt"}
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocProductHealthAutomationReferences(_StrictModel):
    dataplane_manifest_path: str
    collector_script_path: str = "scripts/run_byoc_product_health_collector.py"

    @field_validator("dataplane_manifest_path", "collector_script_path")
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocProductHealthAutomationIdentity(_StrictModel):
    agent_id: str
    agent_version: str

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("agent_version")
    @classmethod
    def _version_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "agent_version")


class ByocProductHealthAutomationSchedule(_StrictModel):
    mode: Literal["customer_managed_schedule"] = "customer_managed_schedule"
    cron: str
    systemd_on_calendar: str
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    jitter_seconds: int = Field(default=30, ge=0, le=3600)
    concurrency_policy: Literal["forbid_overlap"] = "forbid_overlap"
    failure_retry_limit: int = Field(default=1, ge=0, le=5)

    @field_validator("cron")
    @classmethod
    def _cron_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if len(value.split()) != 5:
            raise ValueError("cron must use a five-field schedule")
        if not re.match(r"^[A-Za-z0-9_*/,\- ?]+$", value):
            raise ValueError("cron contains unsupported characters")
        return value

    @field_validator("systemd_on_calendar")
    @classmethod
    def _calendar_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or len(value) > 80:
            raise ValueError("systemd_on_calendar must be a bounded calendar value")
        if not re.match(r"^[A-Za-z0-9_*/:., -]+$", value):
            raise ValueError("systemd_on_calendar contains unsupported characters")
        return value


class ByocProductHealthAutomationRuntime(_StrictModel):
    working_directory: str = "/opt/fyralis/fyraliscore"
    python_executable: str = "/opt/fyralis/venv/bin/python"
    container_image_ref: str = "<customer-approved-fyralis-image-ref>"
    run_as_non_root: Literal[True] = True
    read_only_root_filesystem: Literal[True] = True
    no_inbound_ports: Literal[True] = True
    egress_only_control_plane: Literal[True] = True
    automount_service_account_token: Literal[False] = False

    @field_validator("working_directory", "python_executable", "container_image_ref")
    @classmethod
    def _runtime_strings_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "runtime field")


class ByocProductHealthAutomationEnv(_StrictModel):
    deployment_id_env: str = "FYRALIS_BYOC_DEPLOYMENT_ID"
    customer_id_env: str = "FYRALIS_BYOC_CUSTOMER_ID"
    tenant_id_env: str = "FYRALIS_TENANT_ID"
    agent_id_env: str = "FYRALIS_BYOC_AGENT_ID"
    agent_version_env: str = "FYRALIS_BYOC_AGENT_VERSION"
    artifact_revision_env: str = "FYRALIS_BYOC_ARTIFACT_REVISION"
    database_url_env: str = "DATABASE_URL"
    signing_key_env: str = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"
    signing_key_ref_env: str = "FYRALIS_BYOC_EVIDENCE_INTAKE_KEY_REF"
    control_plane_url_env: str = "FYRALIS_CONTROL_PLANE_URL"

    @field_validator(
        "deployment_id_env",
        "customer_id_env",
        "tenant_id_env",
        "agent_id_env",
        "agent_version_env",
        "artifact_revision_env",
        "database_url_env",
        "signing_key_env",
        "signing_key_ref_env",
        "control_plane_url_env",
    )
    @classmethod
    def _env_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _ENV_NAME_RE.match(value):
            raise ValueError("environment variable names must be upper-case codes")
        return value


class ByocProductHealthAutomationSecretRefs(_StrictModel):
    database_url_secret_ref: str
    signing_key_secret_ref: str

    @field_validator("database_url_secret_ref", "signing_key_secret_ref")
    @classmethod
    def _secret_refs_must_be_refs_only(cls, value: str) -> str:
        value = _safe_code(value, "secret reference")
        lowered = value.lower()
        if any(fragment in lowered for fragment in ("postgresql://", "token=", "secret=")):
            raise ValueError("secret references must not contain raw secret material")
        return value


class ByocProductHealthAutomationControlPlane(_StrictModel):
    submit_path: Literal[
        "/byoc/control-plane/product-health-snapshots"
    ] = "/byoc/control-plane/product-health-snapshots"
    control_plane_url_env: str = "FYRALIS_CONTROL_PLANE_URL"
    signing_key_ref_env: str = "FYRALIS_BYOC_EVIDENCE_INTAKE_KEY_REF"
    outbound_port: Literal[443] = 443

    @field_validator("control_plane_url_env", "signing_key_ref_env")
    @classmethod
    def _env_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _ENV_NAME_RE.match(value):
            raise ValueError("environment variable names must be upper-case codes")
        return value


class ByocProductHealthAutomationKubernetes(_StrictModel):
    namespace: str = "fyralis-byoc"
    cronjob_name: str = "fyralis-byoc-product-health"
    config_map_name: str = "fyralis-byoc-product-health"
    secret_name: str = "fyralis-byoc-product-health"
    service_account_name: str = "fyralis-byoc-product-health"
    successful_jobs_history_limit: int = Field(default=3, ge=0, le=10)
    failed_jobs_history_limit: int = Field(default=3, ge=0, le=10)
    active_deadline_seconds: int = Field(default=120, ge=1, le=1800)

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


class ByocProductHealthAutomationSystemd(_StrictModel):
    service_unit: str = "fyralis-byoc-product-health.service"
    timer_unit: str = "fyralis-byoc-product-health.timer"
    environment_file: str = "/etc/fyralis/byoc-product-health.env"
    user: str = "fyralis"
    group: str = "fyralis"

    @field_validator("service_unit", "timer_unit")
    @classmethod
    def _unit_must_be_safe(cls, value: str) -> str:
        value = value.strip()
        if not _SYSTEMD_UNIT_RE.match(value):
            raise ValueError("systemd units must be bounded .service/.timer names")
        return value

    @field_validator("environment_file", "user", "group")
    @classmethod
    def _systemd_strings_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "systemd field")


class ByocProductHealthAutomationArtifacts(_StrictModel):
    kubernetes_cronjob_path: str
    systemd_service_path: str
    systemd_timer_path: str

    @field_validator(
        "kubernetes_cronjob_path",
        "systemd_service_path",
        "systemd_timer_path",
    )
    @classmethod
    def _path_must_be_relative(cls, value: str) -> str:
        return _relative_path(value)


class ByocProductHealthAutomationPrivacyBoundary(_StrictModel):
    raw_payloads_included: Literal[False] = False
    raw_prompts_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    source_records_included: Literal[False] = False
    model_contents_included: Literal[False] = False
    vector_values_included: Literal[False] = False
    raw_secret_values_included: Literal[False] = False
    endpoint_values_included: Literal[False] = False


class ByocProductHealthAutomation(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health_automation.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    artifact_revision: str
    references: ByocProductHealthAutomationReferences
    identity: ByocProductHealthAutomationIdentity
    schedule: ByocProductHealthAutomationSchedule
    runtime: ByocProductHealthAutomationRuntime
    env: ByocProductHealthAutomationEnv
    secret_refs: ByocProductHealthAutomationSecretRefs
    control_plane: ByocProductHealthAutomationControlPlane
    kubernetes: ByocProductHealthAutomationKubernetes
    systemd: ByocProductHealthAutomationSystemd
    artifacts: ByocProductHealthAutomationArtifacts
    privacy_boundary: ByocProductHealthAutomationPrivacyBoundary = Field(
        default_factory=ByocProductHealthAutomationPrivacyBoundary
    )
    stored_scope: Literal[
        "customer_side_product_health_automation_metadata_only"
    ] = "customer_side_product_health_automation_metadata_only"

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


@dataclass(frozen=True, slots=True)
class ByocProductHealthAutomationViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def generate_product_health_automation(
    *,
    dataplane_manifest: ByocDataPlaneManifest,
    dataplane_manifest_path: Path = Path("deploy/byoc/dataplane.example.yaml"),
    collector_script_path: Path = Path("scripts/run_byoc_product_health_collector.py"),
) -> ByocProductHealthAutomation:
    return ByocProductHealthAutomation(
        schema_version="fyralis.byoc.product_health_automation.v1",
        deployment_id=dataplane_manifest.deployment_id,
        customer_id=dataplane_manifest.customer_id,
        environment=dataplane_manifest.environment,
        artifact_revision=dataplane_manifest.artifact_revision,
        references=ByocProductHealthAutomationReferences(
            dataplane_manifest_path=_path_string(dataplane_manifest_path),
            collector_script_path=_path_string(collector_script_path),
        ),
        identity=ByocProductHealthAutomationIdentity(
            agent_id="agt_producthealth01",
            agent_version=f"{dataplane_manifest.artifact_revision}.product-health",
        ),
        schedule=ByocProductHealthAutomationSchedule(
            cron="*/5 * * * *",
            systemd_on_calendar="*:0/5",
            timeout_seconds=30,
            jitter_seconds=30,
            concurrency_policy="forbid_overlap",
            failure_retry_limit=1,
        ),
        runtime=ByocProductHealthAutomationRuntime(),
        env=ByocProductHealthAutomationEnv(),
        secret_refs=ByocProductHealthAutomationSecretRefs(
            database_url_secret_ref=(
                f"prod/fyralis/{dataplane_manifest.deployment_id}/database-url"
            ),
            signing_key_secret_ref=(
                f"prod/fyralis/{dataplane_manifest.deployment_id}/"
                "byoc-evidence-intake-signing-key"
            ),
        ),
        control_plane=ByocProductHealthAutomationControlPlane(),
        kubernetes=ByocProductHealthAutomationKubernetes(),
        systemd=ByocProductHealthAutomationSystemd(),
        artifacts=ByocProductHealthAutomationArtifacts(
            kubernetes_cronjob_path=(
                "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml"
            ),
            systemd_service_path=(
                "deploy/byoc/systemd/product-health-collector.service.example"
            ),
            systemd_timer_path=(
                "deploy/byoc/systemd/product-health-collector.timer.example"
            ),
        ),
        privacy_boundary=ByocProductHealthAutomationPrivacyBoundary(),
        stored_scope="customer_side_product_health_automation_metadata_only",
    )


def validate_product_health_automation_contract(
    automation: ByocProductHealthAutomation,
    *,
    dataplane_manifest: ByocDataPlaneManifest | None = None,
    repo_root: Path | None = None,
) -> list[ByocProductHealthAutomationViolation]:
    violations: list[ByocProductHealthAutomationViolation] = []
    if dataplane_manifest is not None:
        expected = {
            "deployment_id": dataplane_manifest.deployment_id,
            "customer_id": dataplane_manifest.customer_id,
            "environment": dataplane_manifest.environment,
            "artifact_revision": dataplane_manifest.artifact_revision,
        }
        for field_name, expected_value in expected.items():
            actual = getattr(automation, field_name)
            if actual != expected_value:
                violations.append(
                    _violation(
                        field_name,
                        "dataplane_manifest_mismatch",
                        f"{field_name} does not match the data-plane manifest",
                    )
                )
        if dataplane_manifest.connectivity.direction != "egress_only":
            violations.append(
                _violation(
                    "dataplane.connectivity.direction",
                    "dataplane_not_egress_only",
                    "product-health automation requires egress-only control plane",
                )
            )

    if automation.env.control_plane_url_env != automation.control_plane.control_plane_url_env:
        violations.append(
            _violation(
                "env.control_plane_url_env",
                "control_plane_url_env_mismatch",
                "env and control_plane must reference the same URL variable",
            )
        )
    if automation.env.signing_key_ref_env != automation.control_plane.signing_key_ref_env:
        violations.append(
            _violation(
                "env.signing_key_ref_env",
                "signing_key_ref_env_mismatch",
                "env and control_plane must reference the same key-ref variable",
            )
        )

    rendered = render_product_health_automation_artifacts(automation)
    rendered_text = "\n".join(rendered.values())
    manifest_text = json.dumps(
        automation.model_dump(mode="json"),
        separators=(",", ":"),
        sort_keys=True,
    )
    violations.extend(
        _privacy_violations(
            "<rendered>",
            rendered_text,
            forbidden_fragments=_FORBIDDEN_RENDERED_FRAGMENTS,
        )
    )
    violations.extend(
        _privacy_violations(
            "<manifest>",
            manifest_text,
            forbidden_fragments=_FORBIDDEN_MANIFEST_FRAGMENTS,
        )
    )

    root = repo_root or Path.cwd()
    collector_script = root / automation.references.collector_script_path
    if not collector_script.exists():
        violations.append(
            _violation(
                "references.collector_script_path",
                "collector_script_missing",
                "referenced product-health collector script is missing",
            )
        )
    for rel_path, expected_text in rendered.items():
        path = root / rel_path
        if not path.exists():
            violations.append(
                _violation(
                    rel_path,
                    "automation_artifact_missing",
                    "rendered product-health automation artifact is missing",
                )
            )
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != expected_text:
            violations.append(
                _violation(
                    rel_path,
                    "automation_artifact_drift",
                    "checked-in artifact does not match generated automation output",
                )
            )

    return violations


def render_product_health_automation_artifacts(
    automation: ByocProductHealthAutomation,
) -> dict[str, str]:
    return {
        automation.artifacts.kubernetes_cronjob_path: _render_kubernetes_cronjob(
            automation
        ),
        automation.artifacts.systemd_service_path: _render_systemd_service(
            automation
        ),
        automation.artifacts.systemd_timer_path: _render_systemd_timer(automation),
    }


def product_health_automation_json_schema() -> dict[str, Any]:
    return ByocProductHealthAutomation.model_json_schema()


def load_product_health_automation(path: Path) -> ByocProductHealthAutomation:
    data = _load_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC product-health automation must be a JSON/YAML object")
    return ByocProductHealthAutomation.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


def _render_kubernetes_cronjob(
    automation: ByocProductHealthAutomation,
) -> str:
    env = automation.env
    k8s = automation.kubernetes
    schedule = automation.schedule
    runtime = automation.runtime
    command = _collector_shell_command(automation)
    return f"""apiVersion: batch/v1
kind: CronJob
metadata:
  name: {k8s.cronjob_name}
  namespace: {k8s.namespace}
  labels:
    app.kubernetes.io/name: {k8s.cronjob_name}
    app.kubernetes.io/component: product-health-collector
    fyralis.com/deployment-id: {automation.deployment_id}
    fyralis.com/customer-id: {automation.customer_id}
spec:
  schedule: "{schedule.cron}"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: {k8s.successful_jobs_history_limit}
  failedJobsHistoryLimit: {k8s.failed_jobs_history_limit}
  jobTemplate:
    spec:
      backoffLimit: {schedule.failure_retry_limit}
      activeDeadlineSeconds: {k8s.active_deadline_seconds}
      template:
        metadata:
          labels:
            app.kubernetes.io/name: {k8s.cronjob_name}
            app.kubernetes.io/component: product-health-collector
            fyralis.com/deployment-id: {automation.deployment_id}
        spec:
          automountServiceAccountToken: false
          restartPolicy: Never
          serviceAccountName: {k8s.service_account_name}
          containers:
            - name: product-health-collector
              image: "{runtime.container_image_ref}"
              imagePullPolicy: IfNotPresent
              workingDir: "{runtime.working_directory}"
              command:
                - /bin/sh
                - -lc
              args:
                - |-
{_indent(command, 18)}
              env:
                - name: {env.deployment_id_env}
                  value: "{automation.deployment_id}"
                - name: {env.customer_id_env}
                  value: "{automation.customer_id}"
                - name: {env.agent_id_env}
                  value: "{automation.identity.agent_id}"
                - name: {env.agent_version_env}
                  value: "{automation.identity.agent_version}"
                - name: {env.artifact_revision_env}
                  value: "{automation.artifact_revision}"
                - name: {env.tenant_id_env}
                  valueFrom:
                    configMapKeyRef:
                      name: {k8s.config_map_name}
                      key: tenant-id
                - name: {env.control_plane_url_env}
                  valueFrom:
                    configMapKeyRef:
                      name: {k8s.config_map_name}
                      key: control-plane-url
                - name: {env.signing_key_ref_env}
                  valueFrom:
                    configMapKeyRef:
                      name: {k8s.config_map_name}
                      key: evidence-intake-key-ref
                - name: {env.database_url_env}
                  valueFrom:
                    secretKeyRef:
                      name: {k8s.secret_name}
                      key: database-url
                - name: {env.signing_key_env}
                  valueFrom:
                    secretKeyRef:
                      name: {k8s.secret_name}
                      key: evidence-intake-signing-key
              securityContext:
                allowPrivilegeEscalation: false
                capabilities:
                  drop:
                    - ALL
                readOnlyRootFilesystem: true
                runAsNonRoot: true
"""


def _render_systemd_service(
    automation: ByocProductHealthAutomation,
) -> str:
    systemd = automation.systemd
    command = _collector_shell_command_one_line(automation)
    return f"""[Unit]
Description=Fyralis BYOC product-health collector
Documentation=man:systemd.timer(5)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User={systemd.user}
Group={systemd.group}
WorkingDirectory={automation.runtime.working_directory}
EnvironmentFile={systemd.environment_file}
ExecStart=/bin/sh -lc '{_systemd_escape(command)}'
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
CapabilityBoundingSet=
LockPersonality=true
MemoryDenyWriteExecute=true
"""


def _render_systemd_timer(
    automation: ByocProductHealthAutomation,
) -> str:
    return f"""[Unit]
Description=Run Fyralis BYOC product-health collector

[Timer]
OnCalendar={automation.schedule.systemd_on_calendar}
RandomizedDelaySec={automation.schedule.jitter_seconds}
Persistent=true
Unit={automation.systemd.service_unit}

[Install]
WantedBy=timers.target
"""


def _collector_shell_command(automation: ByocProductHealthAutomation) -> str:
    env = automation.env
    path = automation.control_plane.submit_path
    script = automation.references.collector_script_path
    python = automation.runtime.python_executable
    timeout = automation.schedule.timeout_seconds
    return f"""set -eu
CONTROL_PLANE_BASE="${{{env.control_plane_url_env}%/}}"
exec {python} {script} \\
  --deployment-id "${{{env.deployment_id_env}}}" \\
  --customer-id "${{{env.customer_id_env}}}" \\
  --agent-id "${{{env.agent_id_env}}}" \\
  --agent-version "${{{env.agent_version_env}}}" \\
  --artifact-revision "${{{env.artifact_revision_env}}}" \\
  --tenant-id "${{{env.tenant_id_env}}}" \\
  --database-url-env {env.database_url_env} \\
  --signing-secret-env {env.signing_key_env} \\
  --key-ref "${{{env.signing_key_ref_env}}}" \\
  --submit-url "${{CONTROL_PLANE_BASE}}{path}" \\
  --timeout-seconds {timeout}"""


def _collector_shell_command_one_line(
    automation: ByocProductHealthAutomation,
) -> str:
    env = automation.env
    path = automation.control_plane.submit_path
    script = automation.references.collector_script_path
    python = automation.runtime.python_executable
    timeout = automation.schedule.timeout_seconds
    return (
        f"set -eu; CONTROL_PLANE_BASE=\"${{{env.control_plane_url_env}%/}}\"; "
        f"exec {python} {script} "
        f"--deployment-id \"${{{env.deployment_id_env}}}\" "
        f"--customer-id \"${{{env.customer_id_env}}}\" "
        f"--agent-id \"${{{env.agent_id_env}}}\" "
        f"--agent-version \"${{{env.agent_version_env}}}\" "
        f"--artifact-revision \"${{{env.artifact_revision_env}}}\" "
        f"--tenant-id \"${{{env.tenant_id_env}}}\" "
        f"--database-url-env {env.database_url_env} "
        f"--signing-secret-env {env.signing_key_env} "
        f"--key-ref \"${{{env.signing_key_ref_env}}}\" "
        f"--submit-url \"${{CONTROL_PLANE_BASE}}{path}\" "
        f"--timeout-seconds {timeout}"
    )


def _privacy_violations(
    path: str,
    text: str,
    *,
    forbidden_fragments: tuple[str, ...],
) -> list[ByocProductHealthAutomationViolation]:
    lowered = text.lower()
    violations: list[ByocProductHealthAutomationViolation] = []
    for fragment in forbidden_fragments:
        if fragment in lowered:
            violations.append(
                _violation(
                    path,
                    "raw_or_sensitive_fragment_forbidden",
                    f"product-health automation must not include {fragment!r}",
                )
            )
    return violations


def _safe_code(value: str, label: str) -> str:
    value = value.strip()
    if not value or not _SAFE_CODE_RE.match(value):
        raise ValueError(f"{label} must be a bounded identifier")
    lowered = value.lower()
    if any(fragment in lowered for fragment in ("bearer ", "password=", "token=")):
        raise ValueError(f"{label} must not contain raw credential material")
    return value


def _relative_path(value: str) -> str:
    value = value.strip()
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError("paths must be relative and stay within the repo")
    return value


def _path_string(path: Path) -> str:
    return path.as_posix()


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
        raise ValueError("BYOC product-health automation must be a JSON/YAML object")
    return dict(data)


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else "" for line in text.splitlines())


def _systemd_escape(command: str) -> str:
    return command.replace("'", "'\\''")


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocProductHealthAutomationViolation:
    return ByocProductHealthAutomationViolation(
        path=path,
        code=code,
        message=message,
    )


__all__ = [
    "ByocProductHealthAutomation",
    "ByocProductHealthAutomationViolation",
    "generate_product_health_automation",
    "load_product_health_automation",
    "product_health_automation_json_schema",
    "render_product_health_automation_artifacts",
    "render_validation_errors",
    "validate_product_health_automation_contract",
]
