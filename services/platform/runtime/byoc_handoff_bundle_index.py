"""Sanitized BYOC customer handoff bundle index."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.platform.runtime.byoc_evidence_ledger import (
    load_byoc_evidence_ledger,
)
from services.platform.runtime.byoc_evidence_package import (
    load_byoc_evidence_package,
)


HandoffBundleArtifactKind = Literal[
    "evidence_package",
    "evidence_ledger",
    "customer_handoff_readiness_report",
    "preflight_report",
    "source_onboarding_gate_report",
    "control_plane_read_smoke_report",
    "control_plane_read_smoke_summary",
]
HandoffBundleStoredScope = Literal["sanitized_customer_handoff_bundle_index_only"]

_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_PATH_RE = re.compile(r"^[A-Za-z0-9_./+=,-]{1,240}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
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


class ByocHandoffBundleIndexPrivacyContract(_StrictModel):
    artifact_bodies_included: Literal[False] = False
    raw_reports_included: Literal[False] = False
    raw_payloads_included: Literal[False] = False
    request_bodies_included: Literal[False] = False
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


class ByocHandoffBundleArtifact(_StrictModel):
    name: str
    kind: HandoffBundleArtifactKind
    required: bool
    present: bool
    share_with_customer: Literal[True] = True
    path: str
    digest: str | None = None
    schema_version: str | None = None
    export_scope: str | None = None
    contents_included: Literal[False] = False

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff artifact name must be bounded metadata")
        return value

    @field_validator("path")
    @classmethod
    def _path_must_be_relative_safe(cls, value: str) -> str:
        value = value.strip()
        if not value or not _PATH_RE.match(value):
            raise ValueError("handoff artifact path must be bounded metadata")
        if any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS):
            raise ValueError("handoff artifact path must not contain raw material")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("handoff artifact path must stay inside the repository")
        return value

    @field_validator("digest")
    @classmethod
    def _digest_must_be_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("handoff artifact digest must look like sha256:<64-hex>")
        return value

    @field_validator("schema_version", "export_scope")
    @classmethod
    def _schema_fields_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff artifact schema metadata must be bounded")
        return value


class ByocHandoffBundleReadEndpoint(_StrictModel):
    name: str
    method: Literal["GET"] = "GET"
    path: str
    signed_read_required: Literal[True] = True
    required_query_params: tuple[Literal["deployment_id"], ...] = ("deployment_id",)
    optional_query_params: tuple[Literal["customer_id", "limit"], ...] = ()
    response_schema_version: str
    response_scope: str
    response_body_included: Literal[False] = False
    signed_headers_included: Literal[False] = False

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff endpoint name must be bounded metadata")
        return value

    @field_validator("path")
    @classmethod
    def _path_must_be_control_plane_path(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith("/byoc/control-plane/") or "://" in value:
            raise ValueError("handoff endpoint path must be a relative BYOC path")
        if any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS):
            raise ValueError("handoff endpoint path must not contain raw material")
        return value

    @field_validator("response_schema_version", "response_scope")
    @classmethod
    def _response_metadata_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff endpoint metadata must be bounded")
        return value


class ByocHandoffBundleIndex(_StrictModel):
    schema_version: Literal["fyralis.byoc.customer_handoff_bundle_index.v1"]
    generated_at: datetime
    deployment_id: str
    customer_id: str
    environment: str
    cloud_provider: str
    region: str
    artifact_revision: str
    artifact_count: int = Field(ge=0)
    signed_read_endpoint_count: int = Field(ge=0)
    artifacts: tuple[ByocHandoffBundleArtifact, ...]
    signed_read_endpoints: tuple[ByocHandoffBundleReadEndpoint, ...]
    excluded_material: tuple[str, ...]
    privacy: ByocHandoffBundleIndexPrivacyContract
    stored_scope: HandoffBundleStoredScope = "sanitized_customer_handoff_bundle_index_only"

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

    @field_validator("environment", "cloud_provider", "region", "artifact_revision")
    @classmethod
    def _identity_metadata_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("handoff identity metadata must be bounded")
        return value

    @field_validator("excluded_material")
    @classmethod
    def _excluded_material_must_be_bounded(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if len(value) > 50:
            raise ValueError("excluded material list must be bounded")
        normalized = tuple(item.strip() for item in value)
        if any(not item or not _SAFE_CODE_RE.match(item) for item in normalized):
            raise ValueError("excluded material entries must be bounded metadata")
        return normalized


@dataclass(frozen=True, slots=True)
class ByocHandoffBundleIndexInputs:
    evidence_package_path: Path
    evidence_ledger_path: Path
    repo_root: Path = field(default_factory=Path.cwd)
    customer_handoff_report_path: Path | None = None
    preflight_report_path: Path | None = None
    source_onboarding_gate_report_path: Path | None = None
    control_plane_read_smoke_report_path: Path | None = None
    generated_at: datetime | None = None


def build_byoc_handoff_bundle_index(
    inputs: ByocHandoffBundleIndexInputs,
) -> ByocHandoffBundleIndex:
    package = load_byoc_evidence_package(inputs.evidence_package_path)
    ledger = load_byoc_evidence_ledger(inputs.evidence_ledger_path)
    artifacts = _artifacts(inputs, package_scope=package.export_scope, ledger_scope=ledger.export_scope)
    endpoints = _signed_read_endpoints()
    return ByocHandoffBundleIndex(
        schema_version="fyralis.byoc.customer_handoff_bundle_index.v1",
        generated_at=inputs.generated_at or datetime.now(tz=UTC),
        deployment_id=package.deployment_id,
        customer_id=package.customer_id,
        environment=package.environment,
        cloud_provider=package.cloud_provider,
        region=package.region,
        artifact_revision=package.artifact_revision,
        artifact_count=len(artifacts),
        signed_read_endpoint_count=len(endpoints),
        artifacts=artifacts,
        signed_read_endpoints=endpoints,
        excluded_material=(
            "account_ids",
            "arns",
            "artifact_bodies",
            "credentials",
            "endpoint_urls",
            "logs",
            "pii",
            "prompts",
            "raw_payloads",
            "raw_reports",
            "request_bodies",
            "signed_headers",
        ),
        privacy=ByocHandoffBundleIndexPrivacyContract(),
        stored_scope="sanitized_customer_handoff_bundle_index_only",
    )


def render_handoff_bundle_index_json(index: ByocHandoffBundleIndex) -> str:
    return json.dumps(index.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_handoff_bundle_index_yaml(index: ByocHandoffBundleIndex) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(
        index.model_dump(mode="json"),
        sort_keys=False,
        width=1_000_000,
    )


def _artifacts(
    inputs: ByocHandoffBundleIndexInputs,
    *,
    package_scope: str,
    ledger_scope: str,
) -> tuple[ByocHandoffBundleArtifact, ...]:
    specs: tuple[tuple[str, HandoffBundleArtifactKind, Path | None, bool, str, str | None], ...] = (
        (
            "evidence_package",
            "evidence_package",
            inputs.evidence_package_path,
            True,
            "fyralis.byoc.evidence_package.v1",
            package_scope,
        ),
        (
            "evidence_ledger",
            "evidence_ledger",
            inputs.evidence_ledger_path,
            True,
            "fyralis.byoc.evidence_ledger.v1",
            ledger_scope,
        ),
        (
            "customer_handoff_readiness_report",
            "customer_handoff_readiness_report",
            inputs.customer_handoff_report_path,
            False,
            "fyralis.byoc.customer_handoff_readiness.v1",
            "sanitized_customer_handoff_metadata_only",
        ),
        (
            "preflight_report",
            "preflight_report",
            inputs.preflight_report_path,
            False,
            "fyralis.byoc.preflight_bundle.v1",
            "sanitized_metadata_only",
        ),
        (
            "source_onboarding_gate_report",
            "source_onboarding_gate_report",
            inputs.source_onboarding_gate_report_path,
            False,
            "fyralis.byoc.source_onboarding_gate.v1",
            "sanitized_metadata_only",
        ),
        (
            "control_plane_read_smoke_summary",
            "control_plane_read_smoke_summary",
            inputs.control_plane_read_smoke_report_path,
            False,
            "fyralis.byoc.control_plane_read_smoke_summary.v1",
            "sanitized_control_plane_read_smoke_metadata_only",
        ),
    )
    artifacts: list[ByocHandoffBundleArtifact] = []
    for name, kind, path, required, schema_version, scope in specs:
        if path is None and not required:
            continue
        if path is None:
            raise ValueError(f"{name} path is required")
        artifacts.append(
            ByocHandoffBundleArtifact(
                name=name,
                kind=kind,
                required=required,
                present=path.exists(),
                share_with_customer=True,
                path=_relative_path(path, inputs.repo_root),
                digest=_file_digest(path) if path.exists() else None,
                schema_version=schema_version,
                export_scope=scope,
                contents_included=False,
            )
        )
    return tuple(artifacts)


def _signed_read_endpoints() -> tuple[ByocHandoffBundleReadEndpoint, ...]:
    return (
        ByocHandoffBundleReadEndpoint(
            name="agent_fleet",
            path="/byoc/control-plane/agents",
            optional_query_params=("customer_id", "limit"),
            response_schema_version="fyralis.byoc.agent_fleet_list.v1",
            response_scope="sanitized_agent_metadata_only",
        ),
        ByocHandoffBundleReadEndpoint(
            name="deployment_overview",
            path="/byoc/control-plane/deployment-overview",
            optional_query_params=("customer_id",),
            response_schema_version="fyralis.byoc.deployment_overview.v1",
            response_scope="sanitized_deployment_metadata_only",
        ),
        ByocHandoffBundleReadEndpoint(
            name="evidence_package_receipts",
            path="/byoc/control-plane/evidence-packages",
            optional_query_params=("customer_id", "limit"),
            response_schema_version="fyralis.byoc.evidence_package_receipt_list.v1",
            response_scope="sanitized_metadata_only",
        ),
        ByocHandoffBundleReadEndpoint(
            name="preflight_report_receipts",
            path="/byoc/control-plane/preflight-reports",
            optional_query_params=("customer_id", "limit"),
            response_schema_version="fyralis.byoc.preflight_report_receipt_list.v1",
            response_scope="sanitized_metadata_only",
        ),
        ByocHandoffBundleReadEndpoint(
            name="runner_evidence_receipts",
            path="/byoc/control-plane/runner-evidence",
            optional_query_params=("customer_id", "limit"),
            response_schema_version="fyralis.byoc.runner_evidence_receipt_list.v1",
            response_scope="sanitized_metadata_only",
        ),
    )


def _relative_path(path: Path, repo_root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = repo_root.resolve()
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("handoff artifact path must stay inside repo_root") from exc
    return relative.as_posix()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


__all__ = [
    "ByocHandoffBundleArtifact",
    "ByocHandoffBundleIndex",
    "ByocHandoffBundleIndexInputs",
    "ByocHandoffBundleIndexPrivacyContract",
    "ByocHandoffBundleReadEndpoint",
    "build_byoc_handoff_bundle_index",
    "render_handoff_bundle_index_json",
    "render_handoff_bundle_index_yaml",
]
