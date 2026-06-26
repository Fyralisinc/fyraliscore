"""BYOC data-plane deployment contract.

This module is intentionally contract-first. The control panel, cloud bootstrap
runner, and long-running data-plane agent can all converge on this schema, while
the core repo can already validate the safety properties locally.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from services.platform.runtime.process_manifest import (
    RuntimeProcess,
    production_processes,
)


CloudProvider = Literal["aws", "gcp", "azure", "customer-managed-kubernetes"]
DeploymentEnvironment = Literal["prod", "staging"]
SecretProvider = Literal[
    "aws-secrets-manager",
    "gcp-secret-manager",
    "azure-key-vault",
    "hashicorp-vault",
]
TelemetryMode = Literal["aggregate-only", "disabled"]
EndpointExposure = Literal["private", "customer_ingress", "public"]

_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_PROCESS_NAMES = frozenset(process.name for process in production_processes())
_PUBLIC_EXPOSURE_FORBIDDEN = "public endpoint exposure is not allowed for BYOC"
_CUSTOMER_INGRESS_ALLOWED = frozenset({"gateway"})
_PRIVATE_SERVICE_COMPONENTS = frozenset(
    {
        "postgres",
        "broker",
        "object_storage",
        "redis",
        "embedding",
        "observability",
        "metrics",
        "data_plane_agent",
    }
)
_FORBIDDEN_TELEMETRY_LABELS = frozenset(
    {
        "tenant",
        "tenant_id",
        "actor_id",
        "user_id",
        "installation_id",
        "account_id",
        "external_id",
        "email",
        "owner_email",
        "query",
        "prompt",
        "payload",
        "body",
        "channel",
        "channel_name",
        "path",
        "url",
        "object_key",
        "source_payload",
        "source_channel",
    }
)
_FORBIDDEN_TELEMETRY_LABEL_SUFFIXES = ("_id", "_email", "_url", "_path")
_SAFE_TELEMETRY_LABEL_SUFFIX_EXCEPTIONS = frozenset({"deployment_id"})


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocControlPlaneConnectivity(_StrictModel):
    direction: Literal["egress_only"] = "egress_only"
    protocol: Literal["https"] = "https"
    port: Literal[443] = 443
    auth: Literal["mtls"] = "mtls"
    control_plane_url: str
    agent_poll_interval_seconds: int = Field(default=30, ge=5, le=300)
    heartbeat_interval_seconds: int = Field(default=15, ge=5, le=300)
    fail_closed_for_new_config_after: str = "24h"
    continue_serving_local_traffic_when_disconnected: Literal[True] = True

    @field_validator("control_plane_url")
    @classmethod
    def _control_plane_url_must_be_https(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("control_plane_url must be an https URL")
        if parsed.username or parsed.password:
            raise ValueError("control_plane_url must not contain credentials")
        return value.rstrip("/")


class ByocEndpoint(_StrictModel):
    component: str
    exposure: EndpointExposure
    notes: str = ""

    @field_validator("component")
    @classmethod
    def _component_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("component must not be empty")
        return value


class ByocNetworkContract(_StrictModel):
    customer_ingress_components: tuple[str, ...] = ("gateway",)
    endpoint_exposure: tuple[ByocEndpoint, ...]
    private_service_endpoints: tuple[str, ...]
    control_plane_inbound_allowed: Literal[False] = False


class ByocIdentityContract(_StrictModel):
    runtime_identity: Literal["workload_identity"] = "workload_identity"
    provisioner_identity_ref: str
    agent_identity_ref: str

    @field_validator("provisioner_identity_ref", "agent_identity_ref")
    @classmethod
    def _identity_ref_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("identity references must not be empty")
        return value


class ByocSecretContract(_StrictModel):
    provider: SecretProvider
    region: str
    master_kek_secret_ref: str
    bootstrap_token_secret_ref: str
    agent_client_certificate_secret_ref: str
    raw_env_secrets_allowed: Literal[False] = False

    @field_validator(
        "region",
        "master_kek_secret_ref",
        "bootstrap_token_secret_ref",
        "agent_client_certificate_secret_ref",
    )
    @classmethod
    def _secret_fields_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("secret contract values must not be empty")
        return value


class ByocTelemetryContract(_StrictModel):
    mode: TelemetryMode = "aggregate-only"
    contract: str = "aggregate-only-v1"
    max_batch_bytes: int = Field(default=262_144, ge=1, le=1_048_576)
    raw_logs_allowed: Literal[False] = False
    raw_payloads_allowed: Literal[False] = False
    raw_prompts_allowed: Literal[False] = False
    pii_allowed: Literal[False] = False
    allowlisted_label_keys: tuple[str, ...] = (
        "deployment_id",
        "region",
        "component",
        "version",
        "route_template",
        "source_family",
        "status_class",
        "error_code",
        "resource_class",
        "phase",
    )


class ByocDataResidencyContract(_StrictModel):
    raw_payloads_leave_boundary: Literal[False] = False
    prompts_leave_boundary: Literal[False] = False
    embeddings_leave_boundary: Literal[False] = False
    logs_leave_boundary: Literal[False] = False
    pii_leaves_boundary: Literal[False] = False
    provider_secrets_leave_boundary: Literal[False] = False


class ByocDisabledRuntimeProcess(_StrictModel):
    name: str
    reason: str

    @field_validator("name", "reason")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("disabled process fields must not be empty")
        return value


class ByocRuntimeContract(_StrictModel):
    process_manifest: Literal["production"] = "production"
    per_source_isolation: Literal[True] = True
    allowed_source_families: tuple[str, ...]
    disabled_processes: tuple[ByocDisabledRuntimeProcess, ...] = ()

    @field_validator("allowed_source_families")
    @classmethod
    def _source_families_must_be_present(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if not value:
            raise ValueError("allowed_source_families must not be empty")
        normalized = tuple(source.strip() for source in value)
        if any(not source for source in normalized):
            raise ValueError("allowed_source_families must not contain blanks")
        return normalized


class ByocDataPlaneManifest(_StrictModel):
    schema_version: Literal["fyralis.byoc.dataplane.v1"]
    deployment_id: str
    customer_id: str
    environment: DeploymentEnvironment
    cloud_provider: CloudProvider
    region: str
    artifact_revision: str
    connectivity: ByocControlPlaneConnectivity
    network: ByocNetworkContract
    identity: ByocIdentityContract
    secrets: ByocSecretContract
    telemetry: ByocTelemetryContract
    data_residency: ByocDataResidencyContract
    runtime: ByocRuntimeContract

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
    def _top_level_strings_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be empty")
        return value


@dataclass(frozen=True, slots=True)
class ByocContractViolation:
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


def _label_key_is_forbidden(label: str) -> bool:
    if label in _SAFE_TELEMETRY_LABEL_SUFFIX_EXCEPTIONS:
        return False
    return label in _FORBIDDEN_TELEMETRY_LABELS or label.endswith(
        _FORBIDDEN_TELEMETRY_LABEL_SUFFIXES
    )


def validate_byoc_manifest_contract(
    manifest: ByocDataPlaneManifest,
) -> list[ByocContractViolation]:
    """Return semantic BYOC contract violations not covered by field types."""

    violations: list[ByocContractViolation] = []

    disabled_names = [process.name for process in manifest.runtime.disabled_processes]
    duplicates = sorted(
        {name for name in disabled_names if disabled_names.count(name) > 1}
    )
    for name in duplicates:
        violations.append(
            ByocContractViolation(
                path="runtime.disabled_processes",
                code="duplicate_disabled_process",
                message=f"{name!r} is listed more than once",
            )
        )

    for disabled in manifest.runtime.disabled_processes:
        if disabled.name not in _PROCESS_NAMES:
            violations.append(
                ByocContractViolation(
                    path=f"runtime.disabled_processes.{disabled.name}",
                    code="unknown_runtime_process",
                    message="disabled process is not in the production runtime manifest",
                )
            )
        if disabled.name == "gateway":
            violations.append(
                ByocContractViolation(
                    path="runtime.disabled_processes.gateway",
                    code="gateway_required",
                    message="the gateway is the required customer data-plane entrypoint",
                )
            )

    for endpoint in manifest.network.endpoint_exposure:
        if endpoint.exposure == "public":
            violations.append(
                ByocContractViolation(
                    path=f"network.endpoint_exposure.{endpoint.component}",
                    code="public_endpoint_forbidden",
                    message=_PUBLIC_EXPOSURE_FORBIDDEN,
                )
            )
        if (
            endpoint.exposure == "customer_ingress"
            and endpoint.component not in _CUSTOMER_INGRESS_ALLOWED
        ):
            violations.append(
                ByocContractViolation(
                    path=f"network.endpoint_exposure.{endpoint.component}",
                    code="customer_ingress_not_allowed",
                    message=(
                        "only gateway may use customer-approved ingress; "
                        "data services, metrics, and the agent must stay private"
                    ),
                )
            )

    private_components = frozenset(manifest.network.private_service_endpoints)
    for component in sorted(_PRIVATE_SERVICE_COMPONENTS - private_components):
        violations.append(
            ByocContractViolation(
                path="network.private_service_endpoints",
                code="missing_private_service_endpoint",
                message=f"{component!r} must be declared private",
            )
        )

    for label in manifest.telemetry.allowlisted_label_keys:
        if _label_key_is_forbidden(label):
            violations.append(
                ByocContractViolation(
                    path=f"telemetry.allowlisted_label_keys.{label}",
                    code="unsafe_telemetry_label",
                    message="customer identifiers and free-form payload labels must not leave the data plane",
                )
            )

    return violations


def effective_runtime_processes(
    manifest: ByocDataPlaneManifest,
) -> tuple[RuntimeProcess, ...]:
    disabled = {process.name for process in manifest.runtime.disabled_processes}
    return tuple(
        process for process in production_processes() if process.name not in disabled
    )


def byoc_manifest_json_schema() -> dict[str, Any]:
    return ByocDataPlaneManifest.model_json_schema()


def _load_manifest_mapping(path: Path) -> Any:
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


def load_byoc_manifest(path: Path) -> ByocDataPlaneManifest:
    data = _load_manifest_mapping(path)
    if not isinstance(data, dict):
        raise ValueError("BYOC manifest must be a JSON/YAML object")
    return ByocDataPlaneManifest.model_validate(data)


def render_validation_errors(exc: ValidationError) -> list[str]:
    rendered: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        rendered.append(f"{location}: {error['msg']}")
    return rendered


__all__ = [
    "ByocContractViolation",
    "ByocDataPlaneManifest",
    "byoc_manifest_json_schema",
    "effective_runtime_processes",
    "load_byoc_manifest",
    "render_validation_errors",
    "validate_byoc_manifest_contract",
]
