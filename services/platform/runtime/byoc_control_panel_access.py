"""Tenant-scoped access contract for future BYOC control-panel proxy reads."""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, Iterable, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")

ControlPanelAccessRole = Literal["viewer", "operator", "admin"]
ControlPanelAccessDecisionCode = Literal[
    "allowed",
    "grant_missing",
    "customer_mismatch",
    "deployment_not_allowed",
    "grant_disabled",
    "grant_expired",
]
ControlPanelAccessStoredScope = Literal[
    "sanitized_control_panel_access_metadata_only"
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocControlPanelAccessGrant(_StrictModel):
    schema_version: Literal["fyralis.byoc.control_panel_access_grant.v1"]
    tenant_id: UUID
    customer_id: str
    deployment_ids: tuple[str, ...] = Field(min_length=1, max_length=50)
    role: ControlPanelAccessRole
    enabled: bool = True
    granted_at: datetime
    expires_at: datetime | None = None
    stored_scope: ControlPanelAccessStoredScope = (
        "sanitized_control_panel_access_metadata_only"
    )

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value

    @field_validator("deployment_ids")
    @classmethod
    def _deployment_ids_shape(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in value)
        if len(set(normalized)) != len(normalized):
            raise ValueError("deployment_ids must be unique")
        if any(not _DEPLOYMENT_ID_RE.match(item) for item in normalized):
            raise ValueError("deployment_ids must look like dep_<stable-id>")
        return normalized


class ByocControlPanelAccessQuery(_StrictModel):
    tenant_id: UUID
    deployment_id: str
    customer_id: str | None = None

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value


class ByocControlPanelAccessDecision(_StrictModel):
    schema_version: Literal["fyralis.byoc.control_panel_access_decision.v1"]
    tenant_id: UUID
    deployment_id: str
    customer_id: str | None = None
    allowed: bool
    reason_code: ControlPanelAccessDecisionCode
    role: ControlPanelAccessRole | None = None
    evaluated_at: datetime
    stored_scope: ControlPanelAccessStoredScope = (
        "sanitized_control_panel_access_metadata_only"
    )

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value


def evaluate_byoc_control_panel_access(
    *,
    query: ByocControlPanelAccessQuery,
    grants: Iterable[ByocControlPanelAccessGrant],
    evaluated_at: datetime | None = None,
) -> ByocControlPanelAccessDecision:
    """Evaluate whether a gateway tenant may read a BYOC deployment state.

    This is metadata-only authorization state for a future server-side browser
    proxy. It intentionally contains no read signing keys, endpoint URLs,
    customer data, logs, prompts, or raw evidence bodies.
    """

    observed = evaluated_at or datetime.now(tz=UTC)
    tenant_grants = tuple(grant for grant in grants if grant.tenant_id == query.tenant_id)
    if not tenant_grants:
        return _decision(query, observed, allowed=False, reason_code="grant_missing")

    if query.customer_id is None:
        customer_grants = tenant_grants
    else:
        customer_grants = tuple(
            grant for grant in tenant_grants if grant.customer_id == query.customer_id
        )
        if not customer_grants:
            return _decision(
                query,
                observed,
                allowed=False,
                reason_code="customer_mismatch",
            )

    deployment_grants = tuple(
        grant for grant in customer_grants if query.deployment_id in grant.deployment_ids
    )
    if not deployment_grants:
        return _decision(
            query,
            observed,
            allowed=False,
            reason_code="deployment_not_allowed",
        )

    grant = _strongest_grant(deployment_grants)
    customer_id = query.customer_id or grant.customer_id
    if not grant.enabled:
        return _decision(
            query,
            observed,
            allowed=False,
            reason_code="grant_disabled",
            customer_id=customer_id,
            role=grant.role,
        )
    if grant.expires_at is not None and grant.expires_at <= observed:
        return _decision(
            query,
            observed,
            allowed=False,
            reason_code="grant_expired",
            customer_id=customer_id,
            role=grant.role,
        )
    return _decision(
        query,
        observed,
        allowed=True,
        reason_code="allowed",
        customer_id=customer_id,
        role=grant.role,
    )


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.control_panel_access_bundle.v1",
        "grant": ByocControlPanelAccessGrant.model_json_schema(),
        "query": ByocControlPanelAccessQuery.model_json_schema(),
        "decision": ByocControlPanelAccessDecision.model_json_schema(),
        "stored_scope": "sanitized_control_panel_access_metadata_only",
    }


def render_control_panel_access_schema_bundle_json() -> str:
    return json.dumps(model_json_schema_bundle(), indent=2, sort_keys=True) + "\n"


def _decision(
    query: ByocControlPanelAccessQuery,
    evaluated_at: datetime,
    *,
    allowed: bool,
    reason_code: ControlPanelAccessDecisionCode,
    customer_id: str | None = None,
    role: ControlPanelAccessRole | None = None,
) -> ByocControlPanelAccessDecision:
    return ByocControlPanelAccessDecision(
        schema_version="fyralis.byoc.control_panel_access_decision.v1",
        tenant_id=query.tenant_id,
        deployment_id=query.deployment_id,
        customer_id=customer_id if customer_id is not None else query.customer_id,
        allowed=allowed,
        reason_code=reason_code,
        role=role,
        evaluated_at=evaluated_at,
        stored_scope="sanitized_control_panel_access_metadata_only",
    )


def _strongest_grant(
    grants: tuple[ByocControlPanelAccessGrant, ...],
) -> ByocControlPanelAccessGrant:
    priority = {"admin": 3, "operator": 2, "viewer": 1}
    return max(grants, key=lambda grant: priority[grant.role])


__all__ = [
    "ByocControlPanelAccessDecision",
    "ByocControlPanelAccessGrant",
    "ByocControlPanelAccessQuery",
    "evaluate_byoc_control_panel_access",
    "model_json_schema_bundle",
    "render_control_panel_access_schema_bundle_json",
]
