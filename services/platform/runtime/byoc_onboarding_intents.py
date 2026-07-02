"""Hosted-portal onboarding intent contract for Design Partner BYOC."""
from __future__ import annotations

import json
import re
import secrets
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from lib.shared.ids import uuid7


_INTENT_ID_RE = re.compile(r"^ofi_[0-9a-f]{32}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

PlanCode = Literal["design_partner_byoc_pilot", "enterprise_byoc"]
SupportedPlanCode = Literal["design_partner_byoc_pilot"]
ProcurementChannel = Literal[
    "design_partner",
    "sales",
    "direct",
    "aws_marketplace",
    "private_offer",
]
OnboardingIntentStatus = Literal[
    "draft",
    "intake_submitted",
    "workspace_created",
    "commercial_review",
    "cancelled",
]
TargetCloud = Literal["aws"]
StoredScope = Literal["sanitized_onboarding_metadata_only"]


class OnboardingIntentNotFound(KeyError):
    """Raised when the hosted-portal onboarding intent does not exist."""


class UnsupportedOnboardingPlan(ValueError):
    """Raised when a requested commercial path is not implemented yet."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CreateOnboardingIntentRequest(_StrictModel):
    plan_code: PlanCode
    procurement_channel: ProcurementChannel = "design_partner"
    entrypoint: str = Field(default="get_fyralis", min_length=1, max_length=100)

    @field_validator("entrypoint")
    @classmethod
    def _entrypoint_shape(cls, value: str) -> str:
        value = value.strip()
        if not re.match(r"^[A-Za-z0-9_.:-]{1,100}$", value):
            raise ValueError("entrypoint must be a stable token")
        return value


class SubmitDesignPartnerIntakeRequest(_StrictModel):
    company_name: str = Field(min_length=2, max_length=200)
    setup_owner_email: str = Field(min_length=5, max_length=254)
    target_cloud: TargetCloud = "aws"

    @field_validator("company_name")
    @classmethod
    def _company_name(cls, value: str) -> str:
        value = " ".join(value.strip().split())
        if len(value) < 2:
            raise ValueError("company_name is required")
        return value

    @field_validator("setup_owner_email")
    @classmethod
    def _setup_owner_email(cls, value: str) -> str:
        value = value.strip().lower()
        if not _EMAIL_RE.match(value):
            raise ValueError("setup_owner_email must be a valid email address")
        return value


class OnboardingIntentRecord(_StrictModel):
    schema_version: Literal["fyralis.platform.onboarding_intent.v1"]
    intent_id: str
    plan_code: PlanCode
    procurement_channel: ProcurementChannel
    entrypoint: str
    status: OnboardingIntentStatus
    customer_id: str | None = None
    tenant_id: UUID | None = None
    deployment_id: str | None = None
    company_name: str | None = None
    setup_owner_email: str | None = None
    target_cloud: TargetCloud | None = None
    created_at: datetime
    updated_at: datetime
    stored_scope: StoredScope = "sanitized_onboarding_metadata_only"

    @field_validator("intent_id")
    @classmethod
    def _intent_id(cls, value: str) -> str:
        if not _INTENT_ID_RE.match(value):
            raise ValueError("intent_id must look like ofi_<32 lowercase hex chars>")
        return value

    @field_validator("customer_id")
    @classmethod
    def _customer_id(cls, value: str | None) -> str | None:
        if value is not None and not _CUSTOMER_ID_RE.match(value):
            raise ValueError("customer_id must look like cus_<stable-id>")
        return value

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id(cls, value: str | None) -> str | None:
        if value is not None and not _DEPLOYMENT_ID_RE.match(value):
            raise ValueError("deployment_id must look like dep_<stable-id>")
        return value


class OnboardingIntentStore(Protocol):
    async def create_intent(
        self,
        request: CreateOnboardingIntentRequest,
    ) -> OnboardingIntentRecord:
        ...

    async def submit_design_partner_intake(
        self,
        intent_id: str,
        request: SubmitDesignPartnerIntakeRequest,
    ) -> OnboardingIntentRecord:
        ...

    async def get_intent(self, intent_id: str) -> OnboardingIntentRecord | None:
        ...


class InMemoryOnboardingIntentStore:
    """Local metadata-only Design Partner BYOC onboarding store."""

    def __init__(
        self,
        intents: Iterable[OnboardingIntentRecord] = (),
    ) -> None:
        self._intents = {intent.intent_id: intent for intent in intents}
        self.events: list[dict[str, Any]] = []

    async def create_intent(
        self,
        request: CreateOnboardingIntentRequest,
    ) -> OnboardingIntentRecord:
        _ensure_supported_plan(request.plan_code)
        now = _now()
        record = OnboardingIntentRecord(
            schema_version="fyralis.platform.onboarding_intent.v1",
            intent_id=_new_intent_id(),
            plan_code=request.plan_code,
            procurement_channel=request.procurement_channel,
            entrypoint=request.entrypoint,
            status="draft",
            created_at=now,
            updated_at=now,
        )
        self._intents[record.intent_id] = record
        self._append_event(
            intent_id=record.intent_id,
            customer_id=None,
            deployment_id=None,
            event_type="plan_selected",
            metadata={
                "plan_code": request.plan_code,
                "procurement_channel": request.procurement_channel,
                "entrypoint": request.entrypoint,
            },
        )
        return record

    async def submit_design_partner_intake(
        self,
        intent_id: str,
        request: SubmitDesignPartnerIntakeRequest,
    ) -> OnboardingIntentRecord:
        _validate_intent_id(intent_id)
        existing = self._intents.get(intent_id)
        if existing is None:
            raise OnboardingIntentNotFound(intent_id)
        _ensure_supported_plan(existing.plan_code)
        now = _now()
        tenant_id = existing.tenant_id or uuid7()
        customer_id = existing.customer_id or _new_customer_id()
        deployment_id = existing.deployment_id or _new_deployment_id()
        updated = existing.model_copy(
            update={
                "status": "workspace_created",
                "customer_id": customer_id,
                "tenant_id": tenant_id,
                "deployment_id": deployment_id,
                "company_name": request.company_name,
                "setup_owner_email": request.setup_owner_email,
                "target_cloud": request.target_cloud,
                "updated_at": now,
            }
        )
        self._intents[intent_id] = updated
        self._append_event(
            intent_id=intent_id,
            customer_id=customer_id,
            deployment_id=deployment_id,
            event_type="design_partner_intake_submitted",
            metadata={
                "target_cloud": request.target_cloud,
                "company_name": request.company_name,
                "setup_owner_email": request.setup_owner_email,
            },
        )
        self._append_event(
            intent_id=intent_id,
            customer_id=customer_id,
            deployment_id=deployment_id,
            event_type="workspace_created",
            metadata={"tenant_id": str(tenant_id)},
        )
        return updated

    async def get_intent(self, intent_id: str) -> OnboardingIntentRecord | None:
        _validate_intent_id(intent_id)
        return self._intents.get(intent_id)

    def _append_event(
        self,
        *,
        intent_id: str,
        customer_id: str | None,
        deployment_id: str | None,
        event_type: str,
        metadata: dict[str, Any],
    ) -> None:
        self.events.append(
            {
                "event_id": uuid7(),
                "intent_id": intent_id,
                "customer_id": customer_id,
                "deployment_id": deployment_id,
                "event_type": event_type,
                "actor": "hosted_portal",
                "metadata": metadata,
                "stored_scope": "sanitized_onboarding_metadata_only",
                "created_at": _now(),
            }
        )


class PostgresOnboardingIntentStore:
    """Postgres-backed hosted onboarding intent store.

    This store writes only commercial/setup metadata and stable BYOC IDs. It
    intentionally does not write source credentials, cloud credentials, raw
    logs, prompts, payloads, private URLs, evidence bodies, or provider tokens.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def create_intent(
        self,
        request: CreateOnboardingIntentRequest,
    ) -> OnboardingIntentRecord:
        _ensure_supported_plan(request.plan_code)
        intent_id = _new_intent_id()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO fyralis_onboarding_intents (
                        intent_id,
                        plan_code,
                        procurement_channel,
                        entrypoint,
                        status
                    ) VALUES (
                        $1, $2, $3, $4, 'draft'
                    )
                    RETURNING
                        intent_id,
                        plan_code,
                        procurement_channel,
                        entrypoint,
                        status,
                        customer_id,
                        tenant_id,
                        deployment_id,
                        company_name,
                        setup_owner_email,
                        target_cloud,
                        created_at,
                        updated_at,
                        stored_scope
                    """,
                    intent_id,
                    request.plan_code,
                    request.procurement_channel,
                    request.entrypoint,
                )
                await self._insert_event(
                    conn,
                    intent_id=intent_id,
                    customer_id=None,
                    deployment_id=None,
                    event_type="plan_selected",
                    metadata={
                        "plan_code": request.plan_code,
                        "procurement_channel": request.procurement_channel,
                        "entrypoint": request.entrypoint,
                    },
                )
        return _record_from_row(row)

    async def submit_design_partner_intake(
        self,
        intent_id: str,
        request: SubmitDesignPartnerIntakeRequest,
    ) -> OnboardingIntentRecord:
        _validate_intent_id(intent_id)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    """
                    SELECT
                        intent_id,
                        plan_code,
                        procurement_channel,
                        entrypoint,
                        status,
                        customer_id,
                        tenant_id,
                        deployment_id,
                        company_name,
                        setup_owner_email,
                        target_cloud,
                        created_at,
                        updated_at,
                        stored_scope
                    FROM fyralis_onboarding_intents
                    WHERE intent_id = $1
                    FOR UPDATE
                    """,
                    intent_id,
                )
                if existing is None:
                    raise OnboardingIntentNotFound(intent_id)
                _ensure_supported_plan(existing["plan_code"])

                tenant_id = existing["tenant_id"] or uuid7()
                customer_id = existing["customer_id"] or _new_customer_id()
                deployment_id = existing["deployment_id"] or _new_deployment_id()

                await conn.execute(
                    """
                    INSERT INTO tenants (id, name, is_demo)
                    VALUES ($1, $2, false)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name
                    """,
                    tenant_id,
                    request.company_name,
                )
                await conn.execute(
                    """
                    INSERT INTO fyralis_customers (
                        customer_id,
                        company_name,
                        selected_plan_code,
                        status
                    ) VALUES (
                        $1, $2, 'design_partner_byoc_pilot', 'pilot'
                    )
                    ON CONFLICT (customer_id) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        selected_plan_code = EXCLUDED.selected_plan_code,
                        status = EXCLUDED.status,
                        updated_at = now()
                    """,
                    customer_id,
                    request.company_name,
                )
                await conn.execute(
                    """
                    INSERT INTO fyralis_byoc_deployments (
                        deployment_id,
                        customer_id,
                        tenant_id,
                        intent_id,
                        cloud_provider,
                        environment,
                        runtime,
                        status
                    ) VALUES (
                        $1, $2, $3, $4, $5, 'pilot', 'kubernetes', 'planned'
                    )
                    ON CONFLICT (deployment_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        tenant_id = EXCLUDED.tenant_id,
                        cloud_provider = EXCLUDED.cloud_provider,
                        updated_at = now()
                    """,
                    deployment_id,
                    customer_id,
                    tenant_id,
                    intent_id,
                    request.target_cloud,
                )
                row = await conn.fetchrow(
                    """
                    UPDATE fyralis_onboarding_intents
                    SET
                        status = 'workspace_created',
                        customer_id = $2,
                        tenant_id = $3,
                        deployment_id = $4,
                        company_name = $5,
                        setup_owner_email = $6,
                        target_cloud = $7,
                        updated_at = now()
                    WHERE intent_id = $1
                    RETURNING
                        intent_id,
                        plan_code,
                        procurement_channel,
                        entrypoint,
                        status,
                        customer_id,
                        tenant_id,
                        deployment_id,
                        company_name,
                        setup_owner_email,
                        target_cloud,
                        created_at,
                        updated_at,
                        stored_scope
                    """,
                    intent_id,
                    customer_id,
                    tenant_id,
                    deployment_id,
                    request.company_name,
                    request.setup_owner_email,
                    request.target_cloud,
                )
                await self._insert_event(
                    conn,
                    intent_id=intent_id,
                    customer_id=customer_id,
                    deployment_id=deployment_id,
                    event_type="design_partner_intake_submitted",
                    metadata={
                        "target_cloud": request.target_cloud,
                        "company_name": request.company_name,
                        "setup_owner_email": request.setup_owner_email,
                    },
                )
                await self._insert_event(
                    conn,
                    intent_id=intent_id,
                    customer_id=customer_id,
                    deployment_id=deployment_id,
                    event_type="workspace_created",
                    metadata={"tenant_id": str(tenant_id)},
                )
        return _record_from_row(row)

    async def get_intent(self, intent_id: str) -> OnboardingIntentRecord | None:
        _validate_intent_id(intent_id)
        row = await self._pool.fetchrow(
            """
            SELECT
                intent_id,
                plan_code,
                procurement_channel,
                entrypoint,
                status,
                customer_id,
                tenant_id,
                deployment_id,
                company_name,
                setup_owner_email,
                target_cloud,
                created_at,
                updated_at,
                stored_scope
            FROM fyralis_onboarding_intents
            WHERE intent_id = $1
            """,
            intent_id,
        )
        if row is None:
            return None
        return _record_from_row(row)

    async def _insert_event(
        self,
        conn: Any,
        *,
        intent_id: str,
        customer_id: str | None,
        deployment_id: str | None,
        event_type: str,
        metadata: dict[str, Any],
    ) -> None:
        await conn.execute(
            """
            INSERT INTO fyralis_onboarding_events (
                event_id,
                intent_id,
                customer_id,
                deployment_id,
                event_type,
                actor,
                metadata,
                stored_scope
            ) VALUES (
                $1, $2, $3, $4, $5, 'hosted_portal', $6, $7
            )
            """,
            uuid7(),
            intent_id,
            customer_id,
            deployment_id,
            event_type,
            json.dumps(metadata, separators=(",", ":"), sort_keys=True),
            "sanitized_onboarding_metadata_only",
        )


def _record_from_row(row: Any) -> OnboardingIntentRecord:
    return OnboardingIntentRecord(
        schema_version="fyralis.platform.onboarding_intent.v1",
        intent_id=row["intent_id"],
        plan_code=row["plan_code"],
        procurement_channel=row["procurement_channel"],
        entrypoint=row["entrypoint"],
        status=row["status"],
        customer_id=row["customer_id"],
        tenant_id=row["tenant_id"],
        deployment_id=row["deployment_id"],
        company_name=row["company_name"],
        setup_owner_email=row["setup_owner_email"],
        target_cloud=row["target_cloud"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        stored_scope=row["stored_scope"],
    )


def _ensure_supported_plan(plan_code: str) -> None:
    if plan_code != "design_partner_byoc_pilot":
        raise UnsupportedOnboardingPlan(
            "Only design_partner_byoc_pilot is implemented in this slice"
        )


def _validate_intent_id(intent_id: str) -> None:
    if not _INTENT_ID_RE.match(intent_id):
        raise ValueError("intent_id must look like ofi_<32 lowercase hex chars>")


def _new_intent_id() -> str:
    return f"ofi_{secrets.token_hex(16)}"


def _new_customer_id() -> str:
    return f"cus_{secrets.token_hex(8)}"


def _new_deployment_id() -> str:
    return f"dep_{secrets.token_hex(8)}"


def _now() -> datetime:
    return datetime.now(tz=UTC)


__all__ = [
    "CreateOnboardingIntentRequest",
    "InMemoryOnboardingIntentStore",
    "OnboardingIntentNotFound",
    "OnboardingIntentRecord",
    "OnboardingIntentStore",
    "PostgresOnboardingIntentStore",
    "SubmitDesignPartnerIntakeRequest",
    "UnsupportedOnboardingPlan",
]
