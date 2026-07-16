"""Atomic materialization of governed canonical resource replacement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import InvariantViolation
from services.domain.canonical_referents.repo import CanonicalReferentLineage
from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentReplacementResult,
)
from services.domain.correction_propagation.projections import (
    ProjectionCorrectionAdapter,
    ProjectionCorrectionFenceReport,
)
from services.domain.entity_aliases.repo import (
    close_aliases_for_entity_with_connection,
)
from services.domain.resources import repo as resources_repo
from services.domain.source_identity_bindings.repo import (
    SourceIdentityBindingRepo,
)


@dataclass(frozen=True, slots=True)
class CanonicalResourceReplacementReport:
    """Observable result of one applied or idempotently reconciled replacement."""

    transition: CanonicalReferentReplacementResult
    predecessor_retired: bool
    closed_alias_count: int
    superseded_binding_lineages: tuple[str, ...]
    projection_fence: ProjectionCorrectionFenceReport
    lineage: CanonicalReferentLineage

    @property
    def state_changed(self) -> bool:
        return bool(
            self.transition.applied
            or self.predecessor_retired
            or self.closed_alias_count
            or self.superseded_binding_lineages
            or self.projection_fence.invalidated_subjects
        )


class CanonicalResourceReplacementOrchestrator:
    """Apply the first complete canonical lifecycle protocol.

    The registry remains the append-only lineage authority. This orchestrator
    materializes its operational consequences in the same database
    transaction: retire the predecessor, close its current aliases, supersede
    exact source bindings, and fail closed by removing projections derived
    from Models scoped to the predecessor. Canonical Models and historical
    evidence are never rewritten.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        registry: CanonicalReferentRegistryService | None = None,
        source_bindings: SourceIdentityBindingRepo | None = None,
        projection_adapter: ProjectionCorrectionAdapter | None = None,
    ) -> None:
        self._pool = pool
        self._registry = registry or CanonicalReferentRegistryService(pool)
        self._source_bindings = source_bindings or SourceIdentityBindingRepo(pool)
        self._projection_adapter = projection_adapter or ProjectionCorrectionAdapter()

    async def apply(
        self,
        command: CanonicalReferentReplacementCommand,
    ) -> CanonicalResourceReplacementReport:
        """Apply or reconcile one exact resource replacement atomically."""

        predecessor_id, successor_id = _validate_supported_command(command)
        cause_event_id = command.cause_event_id
        assert cause_event_id is not None

        async with self._pool.acquire() as conn, conn.transaction():
            transition = await self._registry.apply_replacement(
                command,
                conn=conn,
            )
            await _lock_and_validate_resource_endpoints(
                conn,
                command=command,
                predecessor_id=predecessor_id,
                successor_id=successor_id,
            )
            transaction_at = await conn.fetchval(
                "SELECT transaction_timestamp()"
            )
            bindings = await self._source_bindings.list_bindings_for_canonical_ref(
                tenant_id=command.tenant_id,
                canonical_referent_type=command.predecessor.type,
                canonical_referent_id=command.predecessor.id,
                canonical_referent_version=command.predecessor.version,
                valid_at=command.effective_at,
                known_at=transaction_at,
                conn=conn,
            )

            predecessor_before = await conn.fetchval(
                """
                SELECT archived_at
                FROM resources
                WHERE tenant_id=$1 AND id=$2
                """,
                command.tenant_id,
                predecessor_id,
            )
            await resources_repo.retire_non_customer_at(
                predecessor_id,
                tenant_id=command.tenant_id,
                canonical_referent_type=command.predecessor.type,
                effective_at=command.effective_at,
                reason=command.reason,
                cause_event_id=cause_event_id,
                conn=conn,
            )
            predecessor_retired = predecessor_before is None

            closed_alias_count = await close_aliases_for_entity_with_connection(
                conn,
                tenant_id=command.tenant_id,
                resolved_entity_ref=command.predecessor.model_dump(mode="json"),
                valid_until=command.effective_at,
                validity_event_id=cause_event_id,
                validity_reason="canonical_referent_replaced",
            )

            superseded_lineages: list[str] = []
            successor_ref = command.successor.model_dump(mode="json")
            for binding in bindings:
                lineage_id = binding.binding_lineage_id
                if not lineage_id:
                    raise InvariantViolation(
                        "SOURCE_BINDING_LINEAGE_REQUIRED",
                        "canonical replacement cannot repair an unversioned binding",
                        tenant_id=str(command.tenant_id),
                        binding_id=binding.binding_id,
                    )
                await self._source_bindings.supersede(
                    tenant_id=command.tenant_id,
                    binding_lineage_id=lineage_id,
                    expected_binding_version=binding.binding_version,
                    effective_at=command.effective_at,
                    operation_ref=(
                        f"{command.operation_ref}:source-binding:{lineage_id}"
                    ),
                    reason=command.reason,
                    evidence_refs=command.evidence_refs,
                    new_canonical_ref=successor_ref,
                    new_source_identity_authority_ref=(
                        binding.source_identity_authority_ref
                    ),
                    new_evidence_refs=_merge_evidence_refs(
                        binding.evidence_refs,
                        command.evidence_refs,
                    ),
                    conn=conn,
                )
                superseded_lineages.append(lineage_id)

            projection_fence = (
                await self._projection_adapter.invalidate_for_canonical_referent(
                    conn,
                    tenant_id=command.tenant_id,
                    canonical_referent_type=command.predecessor.type,
                    canonical_referent_id=command.predecessor.id,
                    cause_event_id=cause_event_id,
                )
            )
            lineage = await self._registry.lineage_at(
                tenant_id=command.tenant_id,
                referent=command.predecessor,
                valid_at=command.effective_at,
                known_at=transaction_at,
                conn=conn,
            )
            if lineage.head != command.successor:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_REPAIR_INCOMPLETE",
                    "replacement lineage does not expose the requested successor",
                    tenant_id=str(command.tenant_id),
                    operation_ref=command.operation_ref,
                )

            return CanonicalResourceReplacementReport(
                transition=transition,
                predecessor_retired=predecessor_retired,
                closed_alias_count=closed_alias_count,
                superseded_binding_lineages=tuple(superseded_lineages),
                projection_fence=projection_fence,
                lineage=lineage,
            )


def _validate_supported_command(
    command: CanonicalReferentReplacementCommand,
) -> tuple[UUID, UUID]:
    if command.predecessor.type != "resource":
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_TYPE_UNSUPPORTED",
            "the first materialized replacement protocol supports resources only",
            canonical_referent_type=command.predecessor.type,
        )
    if command.predecessor.version != 1 or command.successor.version != 1:
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_VERSION_UNSUPPORTED",
            "physical resource replacement currently supports version 1 refs",
            predecessor_version=command.predecessor.version,
            successor_version=command.successor.version,
        )
    if command.cause_event_id is None:
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_CAUSE_REQUIRED",
            "materialized replacement requires a durable cause event",
            operation_ref=command.operation_ref,
        )
    try:
        return UUID(command.predecessor.id), UUID(command.successor.id)
    except ValueError as exc:
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_RESOURCE_ID",
            "materialized resource replacement requires UUID physical IDs",
            predecessor_id=command.predecessor.id,
            successor_id=command.successor.id,
        ) from exc


async def _lock_and_validate_resource_endpoints(
    conn: asyncpg.Connection,
    *,
    command: CanonicalReferentReplacementCommand,
    predecessor_id: UUID,
    successor_id: UUID,
) -> None:
    rows = await conn.fetch(
        """
        SELECT id, archived_at, metadata
        FROM resources
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        ORDER BY id
        FOR UPDATE
        """,
        command.tenant_id,
        [predecessor_id, successor_id],
    )
    by_id = {row["id"]: row for row in rows}
    missing = [
        str(resource_id)
        for resource_id in (predecessor_id, successor_id)
        if resource_id not in by_id
    ]
    if missing:
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_ENDPOINT_MISSING",
            "replacement endpoints must be tenant-local physical resources",
            tenant_id=str(command.tenant_id),
            missing_resource_ids=missing,
        )
    successor = by_id[successor_id]
    if successor["archived_at"] is not None:
        raise InvariantViolation(
            "CANONICAL_REPLACEMENT_SUCCESSOR_INACTIVE",
            "replacement successor must be an active physical resource",
            tenant_id=str(command.tenant_id),
            successor_id=str(successor_id),
        )
    for role, resource_id in (
        ("predecessor", predecessor_id),
        ("successor", successor_id),
    ):
        semantic_kind = _json_obj(by_id[resource_id]["metadata"]).get(
            "semantic_kind"
        )
        if semantic_kind in {
            "actor",
            "customer",
            "human",
            "human_internal",
            "human_external",
            "person",
        }:
            raise InvariantViolation(
                "CANONICAL_REPLACEMENT_SPECIALIZED_LIFECYCLE",
                "actor and customer resources require specialized protocols",
                tenant_id=str(command.tenant_id),
                endpoint_role=role,
                semantic_kind=semantic_kind,
            )


def _merge_evidence_refs(
    first: tuple[str, ...],
    second: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*first, *second)))


def _json_obj(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


__all__ = [
    "CanonicalResourceReplacementOrchestrator",
    "CanonicalResourceReplacementReport",
]
