"""Final metadata-only BYOC customer-pilot launch readiness summary."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


LaunchReadinessStatus = Literal["pass", "fail", "manual_required"]
LaunchStoredScope = Literal["sanitized_launch_readiness_metadata_only"]

_PASS: LaunchReadinessStatus = "pass"
_FAIL: LaunchReadinessStatus = "fail"
_MANUAL: LaunchReadinessStatus = "manual_required"
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")
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


class ByocLaunchReadinessPrivacyContract(_StrictModel):
    child_report_bodies_included: Literal[False] = False
    artifact_bodies_included: Literal[False] = False
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


class ByocLaunchReadinessCheck(_StrictModel):
    name: str
    status: LaunchReadinessStatus
    required: bool = True
    details: str
    source_schema_version: str | None = None
    metrics: dict[str, int | bool | str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("launch readiness check name must be bounded metadata")
        return value

    @field_validator("details", "source_schema_version")
    @classmethod
    def _details_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if (
            not value
            or len(value) > 240
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("launch readiness string fields must be bounded metadata")
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
                raise ValueError("launch readiness metric names must be bounded")
            if isinstance(metric, str):
                metric = metric.strip()
                if (
                    not metric
                    or len(metric) > 160
                    or any(
                        fragment in metric.lower()
                        for fragment in _FORBIDDEN_FRAGMENTS
                    )
                ):
                    raise ValueError("launch readiness metric values must be bounded")
            normalized[key] = metric
        return normalized


class ByocLaunchReadinessSummary(_StrictModel):
    schema_version: Literal["fyralis.byoc.launch_readiness_summary.v1"]
    generated_at: datetime
    status: LaunchReadinessStatus
    customer_pilot_ready: bool
    manual_actions_required: bool
    required_checks_passed: bool
    deployment_id: str | None = None
    customer_id: str | None = None
    cloud_provider: str | None = None
    region: str | None = None
    artifact_revision: str | None = None
    next_actions: tuple[str, ...]
    checks: tuple[ByocLaunchReadinessCheck, ...]
    privacy: ByocLaunchReadinessPrivacyContract
    stored_scope: LaunchStoredScope = "sanitized_launch_readiness_metadata_only"

    @field_validator(
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
    )
    @classmethod
    def _identity_fields_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("launch readiness identity fields must be bounded")
        return value

    @field_validator("next_actions")
    @classmethod
    def _next_actions_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 20:
            raise ValueError("launch readiness next actions must be bounded")
        normalized = tuple(action.strip() for action in value)
        if any(not action or not _SAFE_CODE_RE.match(action) for action in normalized):
            raise ValueError("launch readiness next actions must be bounded metadata")
        return normalized


@dataclass(frozen=True, slots=True)
class ByocLaunchReadinessSummaryInputs:
    live_test_readiness_path: Path
    customer_handoff_report_path: Path
    handoff_bundle_index_path: Path
    control_plane_read_smoke_path: Path
    generated_at: datetime | None = None


def build_byoc_launch_readiness_summary(
    inputs: ByocLaunchReadinessSummaryInputs,
) -> ByocLaunchReadinessSummary:
    live = _load_json(inputs.live_test_readiness_path)
    handoff = _load_json(inputs.customer_handoff_report_path)
    index = _load_json(inputs.handoff_bundle_index_path)
    smoke = _load_json(inputs.control_plane_read_smoke_path)
    checks = (
        _live_test_check(live),
        _customer_handoff_check(handoff),
        _handoff_bundle_index_check(index),
        _control_plane_read_smoke_check(smoke),
        _identity_consistency_check((live, handoff, index, smoke)),
    )
    required_checks_passed = all(
        check.status != _FAIL for check in checks if check.required
    )
    manual_actions_required = any(check.status == _MANUAL for check in checks)
    status: LaunchReadinessStatus
    if not required_checks_passed:
        status = _FAIL
    elif manual_actions_required:
        status = _MANUAL
    else:
        status = _PASS
    identity = _identity_from_payloads((live, handoff, index, smoke))
    return ByocLaunchReadinessSummary(
        schema_version="fyralis.byoc.launch_readiness_summary.v1",
        generated_at=inputs.generated_at or datetime.now(tz=UTC),
        status=status,
        customer_pilot_ready=status == _PASS,
        manual_actions_required=manual_actions_required,
        required_checks_passed=required_checks_passed,
        deployment_id=identity.get("deployment_id"),
        customer_id=identity.get("customer_id"),
        cloud_provider=identity.get("cloud_provider"),
        region=identity.get("region"),
        artifact_revision=identity.get("artifact_revision"),
        next_actions=_next_actions(checks),
        checks=checks,
        privacy=ByocLaunchReadinessPrivacyContract(),
        stored_scope="sanitized_launch_readiness_metadata_only",
    )


def render_launch_readiness_summary_json(
    summary: ByocLaunchReadinessSummary,
) -> str:
    return json.dumps(summary.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_launch_readiness_summary_yaml(
    summary: ByocLaunchReadinessSummary,
) -> str:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    return yaml.safe_dump(
        summary.model_dump(mode="json"),
        sort_keys=False,
        width=1_000_000,
    )


def _live_test_check(payload: dict[str, Any]) -> ByocLaunchReadinessCheck:
    schema = str(payload.get("schema_version") or "")
    if schema != "fyralis.byoc.live_test_readiness.v1":
        return _check(
            "live_test_readiness",
            _FAIL,
            "Live-test readiness artifact has an unexpected schema.",
            schema,
        )
    if payload.get("required_checks_passed") is not True:
        return _check(
            "live_test_readiness",
            _FAIL,
            "Live-test readiness has failing contract checks.",
            schema,
            metrics={"source_status": str(payload.get("status") or "unknown")},
        )
    if payload.get("live_aws_ready") is True and payload.get("status") == _PASS:
        return _check(
            "live_test_readiness",
            _PASS,
            "Live AWS readiness is satisfied.",
            schema,
            metrics={"live_aws_ready": True},
        )
    return _check(
        "live_test_readiness",
        _MANUAL,
        "Live AWS access or credential rehearsal still requires operator action.",
        schema,
        metrics={
            "live_aws_ready": bool(payload.get("live_aws_ready")),
            "next_required_action": str(
                payload.get("next_required_action") or "configure_aws_access"
            ),
        },
    )


def _customer_handoff_check(payload: dict[str, Any]) -> ByocLaunchReadinessCheck:
    schema = str(payload.get("schema_version") or "")
    if schema != "fyralis.byoc.customer_handoff_readiness.v1":
        return _check(
            "customer_handoff_readiness",
            _FAIL,
            "Customer handoff report has an unexpected schema.",
            schema,
        )
    ready = (
        payload.get("customer_handoff_ready") is True
        and payload.get("required_sections_passed") is True
    )
    return _check(
        "customer_handoff_readiness",
        _PASS if ready else _FAIL,
        "Customer handoff readiness is satisfied."
        if ready
        else "Customer handoff readiness is not satisfied.",
        schema,
        metrics={
            "customer_handoff_ready": bool(payload.get("customer_handoff_ready")),
            "source_onboarding_allowed": bool(
                payload.get("source_onboarding_allowed")
            ),
        },
    )


def _handoff_bundle_index_check(payload: dict[str, Any]) -> ByocLaunchReadinessCheck:
    schema = str(payload.get("schema_version") or "")
    if schema != "fyralis.byoc.customer_handoff_bundle_index.v1":
        return _check(
            "handoff_bundle_index",
            _FAIL,
            "Handoff bundle index has an unexpected schema.",
            schema,
        )
    artifacts = payload.get("artifacts")
    endpoints = payload.get("signed_read_endpoints")
    privacy = payload.get("privacy")
    required_missing = 0
    if isinstance(artifacts, list):
        required_missing = sum(
            1
            for artifact in artifacts
            if isinstance(artifact, dict)
            and artifact.get("required") is True
            and artifact.get("present") is not True
        )
    ok = (
        isinstance(artifacts, list)
        and len(artifacts) >= 2
        and required_missing == 0
        and isinstance(endpoints, list)
        and len(endpoints) >= 5
        and _privacy_flags_false(privacy)
    )
    return _check(
        "handoff_bundle_index",
        _PASS if ok else _FAIL,
        "Handoff bundle index is complete."
        if ok
        else "Handoff bundle index is incomplete or privacy flags are unsafe.",
        schema,
        metrics={
            "artifact_count": int(payload.get("artifact_count") or 0),
            "signed_read_endpoint_count": int(
                payload.get("signed_read_endpoint_count") or 0
            ),
            "required_missing_count": required_missing,
        },
    )


def _control_plane_read_smoke_check(
    payload: dict[str, Any],
) -> ByocLaunchReadinessCheck:
    schema = str(payload.get("schema_version") or "")
    if schema != "fyralis.byoc.control_plane_read_smoke.v1":
        return _check(
            "control_plane_read_smoke",
            _FAIL,
            "Control-plane read smoke artifact has an unexpected schema.",
            schema,
        )
    mode = str(payload.get("mode") or "")
    if mode == "signed_requests":
        return _check(
            "control_plane_read_smoke",
            _MANUAL,
            "Hosted control-plane read smoke still needs execution.",
            schema,
            metrics={"mode": mode},
        )
    responses = payload.get("responses")
    expected = {
        "agent_fleet",
        "deployment_overview",
        "evidence_packages",
        "preflight_reports",
        "runner_evidence",
    }
    ok = mode == "executed" and isinstance(responses, dict) and expected.issubset(
        set(responses)
    )
    return _check(
        "control_plane_read_smoke",
        _PASS if ok else _FAIL,
        "Hosted control-plane read smoke passed."
        if ok
        else "Hosted control-plane read smoke is missing expected surfaces.",
        schema,
        metrics={"mode": mode or "unknown", "surface_count": len(responses or {})},
    )


def _identity_consistency_check(
    payloads: tuple[dict[str, Any], ...],
) -> ByocLaunchReadinessCheck:
    mismatches = 0
    for field in (
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        values = {
            str(payload.get(field))
            for payload in payloads
            if payload.get(field) not in (None, "")
        }
        if len(values) > 1:
            mismatches += 1
    return _check(
        "identity_consistency",
        _PASS if mismatches == 0 else _FAIL,
        "Launch artifacts agree on deployment identity."
        if mismatches == 0
        else "Launch artifacts have inconsistent deployment identity.",
        None,
        metrics={"mismatch_count": mismatches},
    )


def _identity_from_payloads(
    payloads: tuple[dict[str, Any], ...],
) -> dict[str, str | None]:
    identity: dict[str, str | None] = {}
    for field in (
        "deployment_id",
        "customer_id",
        "cloud_provider",
        "region",
        "artifact_revision",
    ):
        value = next(
            (
                payload.get(field)
                for payload in payloads
                if payload.get(field) not in (None, "")
            ),
            None,
        )
        identity[field] = str(value) if value is not None else None
    return identity


def _next_actions(
    checks: tuple[ByocLaunchReadinessCheck, ...],
) -> tuple[str, ...]:
    actions: list[str] = []
    for check in checks:
        if check.status == _FAIL:
            actions.append(f"fix_{check.name}")
        elif check.status == _MANUAL:
            actions.append(f"complete_{check.name}")
    return tuple(actions or ["none"])


def _privacy_flags_false(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return all(flag is False for flag in value.values())


def _check(
    name: str,
    status: LaunchReadinessStatus,
    details: str,
    schema: str | None,
    *,
    metrics: dict[str, int | bool | str] | None = None,
) -> ByocLaunchReadinessCheck:
    return ByocLaunchReadinessCheck(
        name=name,
        status=status,
        required=True,
        details=details,
        source_schema_version=schema or None,
        metrics=metrics or {},
    )


def _load_json(path: Path) -> dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return parsed


__all__ = [
    "ByocLaunchReadinessCheck",
    "ByocLaunchReadinessPrivacyContract",
    "ByocLaunchReadinessSummary",
    "ByocLaunchReadinessSummaryInputs",
    "build_byoc_launch_readiness_summary",
    "render_launch_readiness_summary_json",
    "render_launch_readiness_summary_yaml",
]
