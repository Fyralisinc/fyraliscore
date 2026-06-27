"""Sanitized summary for BYOC control-plane read smoke output."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


SmokeSummaryStatus = Literal["pass", "fail", "manual_required"]
SmokeSummaryMode = Literal["executed", "signed_requests", "unknown"]
SmokeSummaryStoredScope = Literal["sanitized_control_plane_read_smoke_metadata_only"]

_PASS: SmokeSummaryStatus = "pass"
_FAIL: SmokeSummaryStatus = "fail"
_MANUAL: SmokeSummaryStatus = "manual_required"
_EXPECTED_SURFACES = (
    "agent_fleet",
    "deployment_overview",
    "control_panel_state",
    "evidence_packages",
    "preflight_reports",
    "runner_evidence",
)
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


class ByocControlPlaneReadSmokePrivacyContract(_StrictModel):
    request_bodies_included: Literal[False] = False
    response_bodies_included: Literal[False] = False
    signed_headers_included: Literal[False] = False
    endpoint_urls_included: Literal[False] = False
    endpoint_paths_included: Literal[False] = False
    query_strings_included: Literal[False] = False
    raw_auth_material_included: Literal[False] = False
    credentials_included: Literal[False] = False
    account_ids_included: Literal[False] = False
    arns_included: Literal[False] = False
    command_output_included: Literal[False] = False
    logs_included: Literal[False] = False
    prompts_included: Literal[False] = False
    embeddings_included: Literal[False] = False
    pii_included: Literal[False] = False


class ByocControlPlaneReadSmokeSurface(_StrictModel):
    name: str
    status: SmokeSummaryStatus
    required: bool = True
    details: str

    @field_validator("name")
    @classmethod
    def _name_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("control-plane read smoke surface name must be bounded")
        return value

    @field_validator("details")
    @classmethod
    def _details_must_be_bounded(cls, value: str) -> str:
        value = value.strip()
        if (
            not value
            or len(value) > 240
            or any(fragment in value.lower() for fragment in _FORBIDDEN_FRAGMENTS)
        ):
            raise ValueError("control-plane read smoke details must be bounded")
        return value


class ByocControlPlaneReadSmokeSummary(_StrictModel):
    schema_version: Literal["fyralis.byoc.control_plane_read_smoke_summary.v1"]
    generated_at: datetime
    status: SmokeSummaryStatus
    mode: SmokeSummaryMode
    hosted_read_executed: bool
    required_surfaces_present: bool
    manual_actions_required: bool
    deployment_id: str | None = None
    customer_id: str | None = None
    surface_count: int = Field(ge=0)
    expected_surface_count: int = Field(ge=1)
    next_actions: tuple[str, ...]
    surfaces: tuple[ByocControlPlaneReadSmokeSurface, ...]
    privacy: ByocControlPlaneReadSmokePrivacyContract
    stored_scope: SmokeSummaryStoredScope = (
        "sanitized_control_plane_read_smoke_metadata_only"
    )

    @field_validator("deployment_id", "customer_id")
    @classmethod
    def _identity_must_be_bounded(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or not _SAFE_CODE_RE.match(value):
            raise ValueError("control-plane read smoke identity must be bounded")
        return value

    @field_validator("next_actions")
    @classmethod
    def _next_actions_must_be_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 10:
            raise ValueError("control-plane read smoke next actions must be bounded")
        normalized = tuple(action.strip() for action in value)
        if any(not action or not _SAFE_CODE_RE.match(action) for action in normalized):
            raise ValueError("control-plane read smoke next actions must be bounded")
        return normalized


@dataclass(frozen=True, slots=True)
class ByocControlPlaneReadSmokeSummaryInputs:
    control_plane_read_smoke_path: Path
    generated_at: datetime | None = None


def build_byoc_control_plane_read_smoke_summary(
    inputs: ByocControlPlaneReadSmokeSummaryInputs,
) -> ByocControlPlaneReadSmokeSummary:
    payload = _load_json(inputs.control_plane_read_smoke_path)
    schema = str(payload.get("schema_version") or "")
    if schema != "fyralis.byoc.control_plane_read_smoke.v1":
        surfaces = tuple(
            _surface(
                name,
                _FAIL,
                "Control-plane read smoke artifact has an unexpected schema.",
            )
            for name in _EXPECTED_SURFACES
        )
        return _summary(
            status=_FAIL,
            mode="unknown",
            hosted_read_executed=False,
            deployment_id=_identity_value(payload, "deployment_id"),
            customer_id=_identity_value(payload, "customer_id"),
            surfaces=surfaces,
            generated_at=inputs.generated_at,
            next_actions=("fix_control_plane_read_smoke_schema",),
        )

    mode = str(payload.get("mode") or "")
    if mode == "signed_requests":
        requests = payload.get("requests")
        present = set(requests) if isinstance(requests, dict) else set()
        surfaces = tuple(
            _surface(
                name,
                _MANUAL if name in present else _FAIL,
                "Signed read request was generated but hosted read is pending."
                if name in present
                else "Signed read request is missing.",
            )
            for name in _EXPECTED_SURFACES
        )
        status: SmokeSummaryStatus = (
            _MANUAL if all(surface.status != _FAIL for surface in surfaces) else _FAIL
        )
        return _summary(
            status=status,
            mode="signed_requests",
            hosted_read_executed=False,
            deployment_id=_identity_value(payload, "deployment_id"),
            customer_id=_identity_value(payload, "customer_id"),
            surfaces=surfaces,
            generated_at=inputs.generated_at,
            next_actions=("run_hosted_control_plane_read_smoke",)
            if status == _MANUAL
            else ("fix_control_plane_read_smoke_requests",),
        )

    responses = payload.get("responses")
    present = set(responses) if isinstance(responses, dict) else set()
    surfaces = tuple(
        _surface(
            name,
            _PASS if mode == "executed" and name in present else _FAIL,
            "Hosted control-plane read surface responded."
            if mode == "executed" and name in present
            else "Hosted control-plane read surface is missing.",
        )
        for name in _EXPECTED_SURFACES
    )
    ok = mode == "executed" and all(surface.status == _PASS for surface in surfaces)
    return _summary(
        status=_PASS if ok else _FAIL,
        mode="executed" if mode == "executed" else "unknown",
        hosted_read_executed=mode == "executed",
        deployment_id=_identity_value(payload, "deployment_id"),
        customer_id=_identity_value(payload, "customer_id"),
        surfaces=surfaces,
        generated_at=inputs.generated_at,
        next_actions=("none",)
        if ok
        else ("fix_control_plane_read_smoke_surfaces",),
    )


def render_control_plane_read_smoke_summary_json(
    summary: ByocControlPlaneReadSmokeSummary,
) -> str:
    return json.dumps(summary.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def render_control_plane_read_smoke_summary_yaml(
    summary: ByocControlPlaneReadSmokeSummary,
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


def _summary(
    *,
    status: SmokeSummaryStatus,
    mode: SmokeSummaryMode,
    hosted_read_executed: bool,
    deployment_id: str | None,
    customer_id: str | None,
    surfaces: tuple[ByocControlPlaneReadSmokeSurface, ...],
    generated_at: datetime | None,
    next_actions: tuple[str, ...],
) -> ByocControlPlaneReadSmokeSummary:
    required_surfaces_present = all(surface.status != _FAIL for surface in surfaces)
    return ByocControlPlaneReadSmokeSummary(
        schema_version="fyralis.byoc.control_plane_read_smoke_summary.v1",
        generated_at=generated_at or datetime.now(tz=UTC),
        status=status,
        mode=mode,
        hosted_read_executed=hosted_read_executed,
        required_surfaces_present=required_surfaces_present,
        manual_actions_required=status == _MANUAL,
        deployment_id=deployment_id,
        customer_id=customer_id,
        surface_count=sum(surface.status != _FAIL for surface in surfaces),
        expected_surface_count=len(_EXPECTED_SURFACES),
        next_actions=next_actions,
        surfaces=surfaces,
        privacy=ByocControlPlaneReadSmokePrivacyContract(),
        stored_scope="sanitized_control_plane_read_smoke_metadata_only",
    )


def _surface(
    name: str,
    status: SmokeSummaryStatus,
    details: str,
) -> ByocControlPlaneReadSmokeSurface:
    return ByocControlPlaneReadSmokeSurface(
        name=name,
        status=status,
        required=True,
        details=details,
    )


def _identity_value(payload: dict[str, Any], field: str) -> str | None:
    value = payload.get(field)
    return str(value) if value not in (None, "") else None


def _load_json(path: Path) -> dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return parsed


__all__ = [
    "ByocControlPlaneReadSmokePrivacyContract",
    "ByocControlPlaneReadSmokeSummary",
    "ByocControlPlaneReadSmokeSummaryInputs",
    "ByocControlPlaneReadSmokeSurface",
    "build_byoc_control_plane_read_smoke_summary",
    "render_control_plane_read_smoke_summary_json",
    "render_control_plane_read_smoke_summary_yaml",
]
