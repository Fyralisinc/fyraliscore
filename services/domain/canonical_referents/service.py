"""Transactional replacement registry for canonical referent lineage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg

from lib.shared.errors import InvariantViolation
from services.domain.canonical_referents.repo import (
    CanonicalReferentLineage,
    CanonicalReferentTransitionRepo,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentReplacementResult,
    CanonicalReferentVersionRef,
)


@dataclass(frozen=True, slots=True)
class CanonicalReferentReadResolution:
    """Bitemporal lineage resolution for one exact requested referent."""

    requested_ref: CanonicalReferentVersionRef
    effective_ref: CanonicalReferentVersionRef
    lineage: CanonicalReferentLineage

    @property
    def replaced(self) -> bool:
        return self.requested_ref != self.effective_ref


class CanonicalReferentRegistryService:
    """Apply exact 1->1 replacements without mutating physical entities."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        *,
        repo: CanonicalReferentTransitionRepo | None = None,
    ) -> None:
        self._pool = pool
        self._repo = repo or CanonicalReferentTransitionRepo(pool)

    async def apply_replacement(
        self,
        command: CanonicalReferentReplacementCommand,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentReplacementResult:
        """Append or replay one replacement under endpoint and operation locks."""

        async def apply(target: asyncpg.Connection) -> CanonicalReferentReplacementResult:
            await self._repo.lock_replacement_scope(target, command=command)
            replay = await self._repo.operation_result(
                target,
                tenant_id=command.tenant_id,
                operation_ref=command.operation_ref,
                applied=False,
            )
            if replay is not None:
                if replay.request_fingerprint != command.request_fingerprint:
                    raise InvariantViolation(
                        "CANONICAL_REFERENT_OPERATION_CONFLICT",
                        "operation_ref already names a different replacement request",
                        tenant_id=str(command.tenant_id),
                        operation_ref=command.operation_ref,
                    )
                return replay

            predecessor_successor = await self._repo.successor_ever(
                target,
                tenant_id=command.tenant_id,
                referent=command.predecessor,
            )
            if predecessor_successor is not None:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_STALE_HEAD",
                    "replacement predecessor is no longer a lineage head",
                    tenant_id=str(command.tenant_id),
                    predecessor=command.predecessor.model_dump(mode="json"),
                    current_successor=(
                        predecessor_successor.adjacent_ref.model_dump(mode="json")
                    ),
                )

            predecessor_parent = await self._repo.predecessor_ever(
                target,
                tenant_id=command.tenant_id,
                referent=command.predecessor,
            )
            if (
                predecessor_parent is not None
                and command.effective_at <= predecessor_parent.effective_at
            ):
                raise InvariantViolation(
                    "CANONICAL_REFERENT_EFFECTIVE_ORDER",
                    "replacement must become effective after its predecessor exists",
                    tenant_id=str(command.tenant_id),
                    predecessor=command.predecessor.model_dump(mode="json"),
                    predecessor_effective_at=(
                        predecessor_parent.effective_at.isoformat()
                    ),
                    requested_effective_at=command.effective_at.isoformat(),
                )

            successor_parent = await self._repo.predecessor_ever(
                target,
                tenant_id=command.tenant_id,
                referent=command.successor,
            )
            successor_child = await self._repo.successor_ever(
                target,
                tenant_id=command.tenant_id,
                referent=command.successor,
            )
            if successor_parent is not None or successor_child is not None:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_SUCCESSOR_NOT_FRESH",
                    "replacement successor already participates in another lineage",
                    tenant_id=str(command.tenant_id),
                    successor=command.successor.model_dump(mode="json"),
                )

            transaction_at = await target.fetchval(
                "SELECT transaction_timestamp()"
            )
            lineage = await self._repo.lineage_at(
                tenant_id=command.tenant_id,
                referent=command.predecessor,
                valid_at=command.effective_at,
                known_at=transaction_at,
                conn=target,
            )
            if command.successor in lineage.members:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_LINEAGE_CYCLE",
                    "replacement successor is already an ancestor of predecessor",
                    tenant_id=str(command.tenant_id),
                    predecessor=command.predecessor.model_dump(mode="json"),
                    successor=command.successor.model_dump(mode="json"),
                )
            return await self._repo.insert_replacement(
                target,
                command=command,
                transaction_at=transaction_at,
            )

        if conn is not None:
            async with conn.transaction():
                return await apply(conn)
        if self._pool is None:
            raise ValueError("canonical referent replacement requires a connection")
        async with self._pool.acquire() as owned, owned.transaction():
            return await apply(owned)

    async def current_successor(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        return await self._repo.current_successor(
            tenant_id=tenant_id,
            referent=referent,
            conn=conn,
        )

    async def current_predecessor(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        return await self._repo.current_predecessor(
            tenant_id=tenant_id,
            referent=referent,
            conn=conn,
        )

    async def lineage_at(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentLineage:
        return await self._repo.lineage_at(
            tenant_id=tenant_id,
            referent=referent,
            valid_at=valid_at,
            known_at=known_at,
            conn=conn,
        )

    async def resolve_at(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentReadResolution:
        """Resolve one exact ref to the head visible at both time cutoffs."""

        lineage = await self.lineage_at(
            tenant_id=tenant_id,
            referent=referent,
            valid_at=valid_at,
            known_at=known_at,
            conn=conn,
        )
        return CanonicalReferentReadResolution(
            requested_ref=referent,
            effective_ref=lineage.head,
            lineage=lineage,
        )
