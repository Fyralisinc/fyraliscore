"""Tenant-scoped access contract for BYOC control-panel proxy reads."""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, Iterable, Literal, Protocol
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


class ByocControlPanelAccessGrantStore(Protocol):
    async def put(
        self,
        grant: ByocControlPanelAccessGrant,
    ) -> ByocControlPanelAccessGrant:
        ...

    async def list_grants(
        self,
        *,
        tenant_id: UUID,
        customer_id: str | None = None,
        deployment_id: str | None = None,
    ) -> tuple[ByocControlPanelAccessGrant, ...]:
        ...

    async def revoke(
        self,
        *,
        tenant_id: UUID,
        customer_id: str,
        deployment_id: str,
    ) -> ByocControlPanelAccessGrant | None:
        ...


class InMemoryByocControlPanelAccessGrantStore:
    """Local metadata-only control-panel access grant store."""

    def __init__(
        self,
        grants: Iterable[ByocControlPanelAccessGrant] = (),
    ) -> None:
        self._records: dict[
            tuple[UUID, str, str],
            ByocControlPanelAccessGrant,
        ] = {}
        for grant in grants:
            self._put_sync(grant)

    @property
    def records(self) -> tuple[ByocControlPanelAccessGrant, ...]:
        keys = sorted(
            self._records,
            key=lambda item: (str(item[0]), item[1], item[2]),
        )
        return tuple(self._records[key] for key in keys)

    async def put(
        self,
        grant: ByocControlPanelAccessGrant,
    ) -> ByocControlPanelAccessGrant:
        self._put_sync(grant)
        return grant

    async def list_grants(
        self,
        *,
        tenant_id: UUID,
        customer_id: str | None = None,
        deployment_id: str | None = None,
    ) -> tuple[ByocControlPanelAccessGrant, ...]:
        _validate_customer_id(customer_id)
        _validate_deployment_id(deployment_id)
        grants = tuple(
            grant
            for grant in self.records
            if grant.tenant_id == tenant_id
            and (customer_id is None or grant.customer_id == customer_id)
            and (
                deployment_id is None
                or deployment_id in grant.deployment_ids
            )
        )
        return grants

    async def revoke(
        self,
        *,
        tenant_id: UUID,
        customer_id: str,
        deployment_id: str,
    ) -> ByocControlPanelAccessGrant | None:
        _validate_customer_id(customer_id)
        _validate_deployment_id(deployment_id)
        key = (tenant_id, customer_id, deployment_id)
        existing = self._records.get(key)
        if existing is None:
            return None
        revoked = existing.model_copy(update={"enabled": False})
        self._records[key] = revoked
        return revoked

    def _put_sync(self, grant: ByocControlPanelAccessGrant) -> None:
        for deployment_id in grant.deployment_ids:
            row_grant = grant.model_copy(update={"deployment_ids": (deployment_id,)})
            self._records[
                (
                    grant.tenant_id,
                    grant.customer_id,
                    deployment_id,
                )
            ] = row_grant


class PostgresByocControlPanelAccessGrantStore:
    """Postgres-backed control-panel access grants.

    The table stores only tenant/customer/deployment authorization metadata. It
    intentionally does not store data-plane URLs, read signing keys, customer
    data, logs, prompts, evidence bodies, or cloud identifiers.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def put(
        self,
        grant: ByocControlPanelAccessGrant,
    ) -> ByocControlPanelAccessGrant:
        for deployment_id in grant.deployment_ids:
            await self._pool.fetchrow(
                """
                INSERT INTO byoc_control_panel_access_grants (
                    tenant_id,
                    customer_id,
                    deployment_id,
                    role,
                    enabled,
                    granted_at,
                    expires_at,
                    stored_scope
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8
                )
                ON CONFLICT (tenant_id, customer_id, deployment_id) DO UPDATE SET
                    role = EXCLUDED.role,
                    enabled = EXCLUDED.enabled,
                    granted_at = EXCLUDED.granted_at,
                    expires_at = EXCLUDED.expires_at,
                    stored_scope = EXCLUDED.stored_scope,
                    updated_at = now()
                RETURNING
                    tenant_id,
                    customer_id,
                    deployment_id,
                    role,
                    enabled,
                    granted_at,
                    expires_at,
                    stored_scope
                """,
                grant.tenant_id,
                grant.customer_id,
                deployment_id,
                grant.role,
                grant.enabled,
                grant.granted_at,
                grant.expires_at,
                grant.stored_scope,
            )
        return grant

    async def list_grants(
        self,
        *,
        tenant_id: UUID,
        customer_id: str | None = None,
        deployment_id: str | None = None,
    ) -> tuple[ByocControlPanelAccessGrant, ...]:
        _validate_customer_id(customer_id)
        _validate_deployment_id(deployment_id)
        clauses = ["tenant_id = $1"]
        args: list[Any] = [tenant_id]
        if customer_id is not None:
            args.append(customer_id)
            clauses.append(f"customer_id = ${len(args)}")
        if deployment_id is not None:
            args.append(deployment_id)
            clauses.append(f"deployment_id = ${len(args)}")
        rows = await self._pool.fetch(
            f"""
            SELECT
                tenant_id,
                customer_id,
                deployment_id,
                role,
                enabled,
                granted_at,
                expires_at,
                stored_scope
            FROM byoc_control_panel_access_grants
            WHERE {' AND '.join(clauses)}
            ORDER BY customer_id ASC, deployment_id ASC
            """,
            *args,
        )
        return tuple(_grant_from_row(row) for row in rows)

    async def revoke(
        self,
        *,
        tenant_id: UUID,
        customer_id: str,
        deployment_id: str,
    ) -> ByocControlPanelAccessGrant | None:
        _validate_customer_id(customer_id)
        _validate_deployment_id(deployment_id)
        row = await self._pool.fetchrow(
            """
            UPDATE byoc_control_panel_access_grants
            SET
                enabled = false,
                updated_at = now()
            WHERE tenant_id = $1
              AND customer_id = $2
              AND deployment_id = $3
            RETURNING
                tenant_id,
                customer_id,
                deployment_id,
                role,
                enabled,
                granted_at,
                expires_at,
                stored_scope
            """,
            tenant_id,
            customer_id,
            deployment_id,
        )
        if row is None:
            return None
        return _grant_from_row(row)


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


def _grant_from_row(row: Any) -> ByocControlPanelAccessGrant:
    return ByocControlPanelAccessGrant(
        schema_version="fyralis.byoc.control_panel_access_grant.v1",
        tenant_id=(
            row["tenant_id"]
            if isinstance(row["tenant_id"], UUID)
            else UUID(str(row["tenant_id"]))
        ),
        customer_id=row["customer_id"],
        deployment_ids=(row["deployment_id"],),
        role=row["role"],
        enabled=row["enabled"],
        granted_at=row["granted_at"],
        expires_at=row["expires_at"],
        stored_scope=row["stored_scope"],
    )


def _validate_customer_id(value: str | None) -> None:
    if value is not None and not _CUSTOMER_ID_RE.match(value.strip()):
        raise ValueError("customer_id must look like cus_<stable-id>")


def _validate_deployment_id(value: str | None) -> None:
    if value is not None and not _DEPLOYMENT_ID_RE.match(value.strip()):
        raise ValueError("deployment_id must look like dep_<stable-id>")


__all__ = [
    "ByocControlPanelAccessDecision",
    "ByocControlPanelAccessGrant",
    "ByocControlPanelAccessGrantStore",
    "ByocControlPanelAccessQuery",
    "InMemoryByocControlPanelAccessGrantStore",
    "PostgresByocControlPanelAccessGrantStore",
    "evaluate_byoc_control_panel_access",
    "model_json_schema_bundle",
    "render_control_panel_access_schema_bundle_json",
]
