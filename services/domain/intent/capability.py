"""Live capable-principal checks for company-intent mutations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

import asyncpg

from lib.contracts.agency import IntentMutation, IntentObjectKind, IntentOperation
from lib.contracts.kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
)
from lib.shared.errors import InvariantViolation, ValidationError
from services.platform.access_control.authority import current_grant_epoch
from services.platform.access_control.roles import roles_for_actor


@dataclass(frozen=True)
class IntentCapabilityDecision:
    capability_ref: str
    processing_authority: ProcessingAuthorityContext
    consumption_authority: ConsumptionAuthorityContext


async def authorize_principal_intent_mutation(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    actor_id: UUID,
    mutation: IntentMutation,
    decided_at: datetime,
) -> IntentCapabilityDecision:
    if decided_at.tzinfo is None or decided_at.utcoffset() is None:
        raise ValidationError("intent capability decision time must be timezone-aware")
    actor_active = await conn.fetchval(
        "SELECT 1 FROM actors WHERE tenant_id = $1 AND id = $2 AND status = 'active'",
        tenant_id,
        actor_id,
    )
    if actor_active != 1:
        raise InvariantViolation(
            "INTENT_CAPABLE_PRINCIPAL", "intent actor is absent or inactive"
        )
    roles = await roles_for_actor(actor_id, conn=conn, tenant_id=tenant_id)
    tenant_roles = {
        str(row["role"])
        for row in roles
        if row["entity_type"] == "tenant" and row["entity_id"] is None
    }
    privileged = tenant_roles & {"admin", "leadership"}
    capability_ref: str | None = None
    if privileged:
        role = sorted(privileged)[0]
        capability_ref = f"tenant-role:{tenant_id}:{actor_id}:{role}"
    elif mutation.object_kind is IntentObjectKind.COMMITMENT:
        capability_ref = await _commitment_capability(
            conn=conn,
            tenant_id=tenant_id,
            actor_id=actor_id,
            mutation=mutation,
        )
    if capability_ref is None:
        raise InvariantViolation(
            "INTENT_CAPABLE_PRINCIPAL",
            "principal lacks live capability for this exact intent mutation",
            object_kind=mutation.object_kind.value,
            operation=mutation.operation.value,
        )
    object_ids = (
        RestrictionSet.only(str(mutation.target_aggregate_id))
        if mutation.target_aggregate_id
        else RestrictionSet.unrestricted()
    )
    fields = RestrictionSet.only(*sorted(mutation.payload))
    common = dict(
        tenant_id=tenant_id,
        principal_or_service_id=f"actor:{actor_id}",
        purpose="intent_mutation",
        operation=mutation.operation.value,
        object_types=RestrictionSet.only(mutation.object_kind.value),
        object_ids=object_ids,
        fields=fields,
        source_labels=RestrictionSet.only("product:recommendation"),
        authority_basis_refs=frozenset({capability_ref}),
        policy_version="principal-intent-capability-v1",
        authority_epoch=await current_grant_epoch(conn=conn, tenant_id=tenant_id),
        decision_time=decided_at,
        expires_at=decided_at + timedelta(minutes=15),
    )
    return IntentCapabilityDecision(
        capability_ref=capability_ref,
        processing_authority=ProcessingAuthorityContext(**common),
        consumption_authority=ConsumptionAuthorityContext(**common),
    )


async def _commitment_capability(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    actor_id: UUID,
    mutation: IntentMutation,
) -> str | None:
    if mutation.operation is IntentOperation.CREATE:
        owner = mutation.payload.get("owner_id")
        if owner is not None and str(owner) == str(actor_id):
            return f"self-commitment:{tenant_id}:{actor_id}"
        return None
    if mutation.target_aggregate_id is None:
        return None
    relation = await conn.fetchrow(
        """
        SELECT c.owner_id,
               EXISTS (
                 SELECT 1 FROM commitment_contributors cc
                 WHERE cc.commitment_id = c.id AND cc.actor_id = $3
               ) AS is_contributor
        FROM commitments c
        WHERE c.tenant_id = $1 AND c.id = $2
        """,
        tenant_id,
        mutation.target_aggregate_id,
        actor_id,
    )
    if relation is None:
        return None
    if relation["owner_id"] == actor_id:
        return f"commitment-owner:{mutation.target_aggregate_id}:{actor_id}"
    if relation["is_contributor"]:
        return f"commitment-contributor:{mutation.target_aggregate_id}:{actor_id}"
    return None


__all__ = ["IntentCapabilityDecision", "authorize_principal_intent_mutation"]
