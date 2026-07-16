"""Persistence for the append-only canonical referent transition registry."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentReplacementResult,
    CanonicalReferentVersionRef,
)


@dataclass(frozen=True)
class CanonicalReferentLineage:
    """One bitemporally visible, ordered replacement lineage."""

    tenant_id: UUID
    valid_at: datetime
    known_at: datetime
    members: tuple[CanonicalReferentVersionRef, ...]

    @property
    def root(self) -> CanonicalReferentVersionRef:
        return self.members[0]

    @property
    def head(self) -> CanonicalReferentVersionRef:
        return self.members[-1]


@dataclass(frozen=True)
class _AdjacentReplacement:
    transition_id: UUID
    adjacent_ref: CanonicalReferentVersionRef
    effective_at: datetime
    transaction_at: datetime


class CanonicalReferentTransitionRepo:
    """Read and append exact canonical-referent replacement edges."""

    def __init__(self, pool: asyncpg.Pool | None) -> None:
        self._pool = pool

    async def lock_replacement_scope(
        self,
        conn: asyncpg.Connection,
        *,
        command: CanonicalReferentReplacementCommand,
    ) -> None:
        """Serialize operation replay and both physical referent endpoints."""

        lock_keys = {
            (
                f"canonical-referent-operation:{command.tenant_id}:"
                f"{command.operation_ref}"
            ),
            _referent_lock_key(command.tenant_id, command.predecessor),
            _referent_lock_key(command.tenant_id, command.successor),
        }
        for lock_key in sorted(lock_keys):
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                lock_key,
            )

    async def operation_result(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        operation_ref: str,
        applied: bool,
    ) -> CanonicalReferentReplacementResult | None:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM canonical_referent_transitions
            WHERE tenant_id=$1
              AND operation_ref=$2
            """,
            tenant_id,
            operation_ref,
        )
        if row is None:
            return None
        if row["transition_kind"] != "replacement":
            raise InvariantViolation(
                "CANONICAL_REFERENT_OPERATION_KIND_CONFLICT",
                "operation_ref is already owned by another transition kind",
                tenant_id=str(tenant_id),
                operation_ref=operation_ref,
                transition_kind=row["transition_kind"],
            )
        return await self._replacement_result(conn, row=row, applied=applied)

    async def insert_replacement(
        self,
        conn: asyncpg.Connection,
        *,
        command: CanonicalReferentReplacementCommand,
        transaction_at: datetime,
    ) -> CanonicalReferentReplacementResult:
        transition_id = uuid7()
        row = await conn.fetchrow(
            """
            INSERT INTO canonical_referent_transitions (
                id, tenant_id, operation_ref, request_fingerprint,
                transition_kind, effective_at, transaction_at,
                expected_predecessor_version, authority_ref, reason,
                evidence_refs, cause_event_id
            ) VALUES (
                $1, $2, $3, $4, 'replacement', $5, $6, $7, $8, $9,
                $10, $11
            )
            RETURNING *
            """,
            transition_id,
            command.tenant_id,
            command.operation_ref,
            command.request_fingerprint,
            command.effective_at,
            transaction_at,
            command.expected_predecessor_version,
            command.authority_ref,
            command.reason,
            list(command.evidence_refs),
            command.cause_event_id,
        )
        await conn.executemany(
            """
            INSERT INTO canonical_referent_transition_members (
                tenant_id, transition_id, member_role, member_ordinal,
                canonical_ref
            ) VALUES ($1, $2, $3, 0, $4::jsonb)
            """,
            (
                (
                    command.tenant_id,
                    transition_id,
                    "predecessor",
                    _dump_ref(command.predecessor),
                ),
                (
                    command.tenant_id,
                    transition_id,
                    "successor",
                    _dump_ref(command.successor),
                ),
            ),
        )
        if row is None:
            raise RuntimeError("replacement transition insert returned no row")
        return await self._replacement_result(conn, row=row, applied=True)

    async def successor_ever(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
    ) -> _AdjacentReplacement | None:
        return await self._adjacent_replacement(
            conn,
            tenant_id=tenant_id,
            referent=referent,
            source_role="predecessor",
            target_role="successor",
        )

    async def predecessor_ever(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
    ) -> _AdjacentReplacement | None:
        return await self._adjacent_replacement(
            conn,
            tenant_id=tenant_id,
            referent=referent,
            source_role="successor",
            target_role="predecessor",
        )

    async def successor_at(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        adjacent = await self._read_adjacent(
            tenant_id=tenant_id,
            referent=referent,
            source_role="predecessor",
            target_role="successor",
            valid_at=valid_at,
            known_at=known_at,
            conn=conn,
        )
        return adjacent.adjacent_ref if adjacent else None

    async def predecessor_at(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        adjacent = await self._read_adjacent(
            tenant_id=tenant_id,
            referent=referent,
            source_role="successor",
            target_role="predecessor",
            valid_at=valid_at,
            known_at=known_at,
            conn=conn,
        )
        return adjacent.adjacent_ref if adjacent else None

    async def current_successor(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        now = datetime.now(timezone.utc)
        return await self.successor_at(
            tenant_id=tenant_id,
            referent=referent,
            valid_at=now,
            known_at=now,
            conn=conn,
        )

    async def current_predecessor(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        conn: asyncpg.Connection | None = None,
    ) -> CanonicalReferentVersionRef | None:
        now = datetime.now(timezone.utc)
        return await self.predecessor_at(
            tenant_id=tenant_id,
            referent=referent,
            valid_at=now,
            known_at=now,
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
        _require_aware(valid_at, field_name="valid_at")
        _require_aware(known_at, field_name="known_at")

        async def read(target: asyncpg.Connection) -> CanonicalReferentLineage:
            seen = {_ref_key(referent)}
            predecessors: list[CanonicalReferentVersionRef] = []
            cursor = referent
            for _ in range(1000):
                adjacent = await self._adjacent_replacement(
                    target,
                    tenant_id=tenant_id,
                    referent=cursor,
                    source_role="successor",
                    target_role="predecessor",
                    valid_at=valid_at,
                    known_at=known_at,
                )
                if adjacent is None:
                    break
                key = _ref_key(adjacent.adjacent_ref)
                if key in seen:
                    raise InvariantViolation(
                        "CANONICAL_REFERENT_LINEAGE_CYCLE",
                        "replacement lineage contains a predecessor cycle",
                        tenant_id=str(tenant_id),
                        referent=referent.model_dump(mode="json"),
                    )
                seen.add(key)
                predecessors.append(adjacent.adjacent_ref)
                cursor = adjacent.adjacent_ref
            else:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_LINEAGE_DEPTH",
                    "replacement lineage exceeds the supported traversal depth",
                    tenant_id=str(tenant_id),
                )

            successors: list[CanonicalReferentVersionRef] = []
            cursor = referent
            for _ in range(1000):
                adjacent = await self._adjacent_replacement(
                    target,
                    tenant_id=tenant_id,
                    referent=cursor,
                    source_role="predecessor",
                    target_role="successor",
                    valid_at=valid_at,
                    known_at=known_at,
                )
                if adjacent is None:
                    break
                key = _ref_key(adjacent.adjacent_ref)
                if key in seen:
                    raise InvariantViolation(
                        "CANONICAL_REFERENT_LINEAGE_CYCLE",
                        "replacement lineage contains a successor cycle",
                        tenant_id=str(tenant_id),
                        referent=referent.model_dump(mode="json"),
                    )
                seen.add(key)
                successors.append(adjacent.adjacent_ref)
                cursor = adjacent.adjacent_ref
            else:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_LINEAGE_DEPTH",
                    "replacement lineage exceeds the supported traversal depth",
                    tenant_id=str(tenant_id),
                )

            return CanonicalReferentLineage(
                tenant_id=tenant_id,
                valid_at=valid_at,
                known_at=known_at,
                members=(
                    *reversed(predecessors),
                    referent,
                    *successors,
                ),
            )

        if conn is not None:
            return await read(conn)
        if self._pool is None:
            raise ValueError("canonical referent lineage read requires a connection")
        async with self._pool.acquire() as owned:
            return await read(owned)

    async def _read_adjacent(
        self,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        source_role: Literal["predecessor", "successor"],
        target_role: Literal["predecessor", "successor"],
        valid_at: datetime,
        known_at: datetime,
        conn: asyncpg.Connection | None,
    ) -> _AdjacentReplacement | None:
        _require_aware(valid_at, field_name="valid_at")
        _require_aware(known_at, field_name="known_at")
        if conn is not None:
            return await self._adjacent_replacement(
                conn,
                tenant_id=tenant_id,
                referent=referent,
                source_role=source_role,
                target_role=target_role,
                valid_at=valid_at,
                known_at=known_at,
            )
        if self._pool is None:
            raise ValueError("canonical referent adjacency read requires a connection")
        async with self._pool.acquire() as owned:
            return await self._adjacent_replacement(
                owned,
                tenant_id=tenant_id,
                referent=referent,
                source_role=source_role,
                target_role=target_role,
                valid_at=valid_at,
                known_at=known_at,
            )

    async def _adjacent_replacement(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        referent: CanonicalReferentVersionRef,
        source_role: Literal["predecessor", "successor"],
        target_role: Literal["predecessor", "successor"],
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
    ) -> _AdjacentReplacement | None:
        temporal_sql = ""
        parameters: list[Any] = [
            tenant_id,
            referent.type,
            referent.id,
            referent.version,
        ]
        if valid_at is not None or known_at is not None:
            if valid_at is None or known_at is None:
                raise ValueError("valid_at and known_at must be provided together")
            temporal_sql = "AND transition.effective_at <= $5 AND transition.transaction_at <= $6"
            parameters.extend((valid_at, known_at))
        rows = await conn.fetch(
            f"""
            SELECT
              transition.id AS transition_id,
              adjacent.canonical_ref AS adjacent_ref,
              transition.effective_at,
              transition.transaction_at
            FROM canonical_referent_transition_members member
            JOIN canonical_referent_transitions transition
              ON transition.tenant_id=member.tenant_id
             AND transition.id=member.transition_id
            JOIN canonical_referent_transition_members adjacent
              ON adjacent.tenant_id=transition.tenant_id
             AND adjacent.transition_id=transition.id
             AND adjacent.member_role='{target_role}'
            WHERE member.tenant_id=$1
              AND member.member_role='{source_role}'
              AND member.canonical_ref ->> 'type'=$2
              AND member.canonical_ref ->> 'id'=$3
              AND (member.canonical_ref ->> 'version')::integer=$4
              AND transition.transition_kind='replacement'
              {temporal_sql}
            ORDER BY transition.effective_at, transition.transaction_at
            LIMIT 2
            """,
            *parameters,
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise InvariantViolation(
                "CANONICAL_REFERENT_REPLACEMENT_ONE_TO_ONE",
                "referent has more than one replacement adjacency",
                tenant_id=str(tenant_id),
                referent=referent.model_dump(mode="json"),
                direction=f"{source_role}_to_{target_role}",
            )
        row = rows[0]
        return _AdjacentReplacement(
            transition_id=row["transition_id"],
            adjacent_ref=_load_ref(row["adjacent_ref"]),
            effective_at=row["effective_at"],
            transaction_at=row["transaction_at"],
        )

    async def _replacement_result(
        self,
        conn: asyncpg.Connection,
        *,
        row: asyncpg.Record,
        applied: bool,
    ) -> CanonicalReferentReplacementResult:
        members = await conn.fetch(
            """
            SELECT member_role, member_ordinal, canonical_ref
            FROM canonical_referent_transition_members
            WHERE tenant_id=$1 AND transition_id=$2
            ORDER BY member_role, member_ordinal
            """,
            row["tenant_id"],
            row["id"],
        )
        by_role: dict[str, list[asyncpg.Record]] = {
            "predecessor": [],
            "successor": [],
        }
        for member in members:
            if member["member_role"] not in by_role:
                raise InvariantViolation(
                    "CANONICAL_REFERENT_REPLACEMENT_CARDINALITY",
                    "replacement transition has an unsupported member role",
                    transition_id=str(row["id"]),
                )
            by_role[member["member_role"]].append(member)
        if (
            len(by_role["predecessor"]) != 1
            or len(by_role["successor"]) != 1
            or int(by_role["predecessor"][0]["member_ordinal"]) != 0
            or int(by_role["successor"][0]["member_ordinal"]) != 0
        ):
            raise InvariantViolation(
                "CANONICAL_REFERENT_REPLACEMENT_CARDINALITY",
                "replacement transition must contain exactly one predecessor and successor",
                transition_id=str(row["id"]),
                predecessor_count=len(by_role["predecessor"]),
                successor_count=len(by_role["successor"]),
            )
        predecessor = _load_ref(by_role["predecessor"][0]["canonical_ref"])
        successor = _load_ref(by_role["successor"][0]["canonical_ref"])
        if predecessor.type != successor.type:
            raise InvariantViolation(
                "CANONICAL_REFERENT_REPLACEMENT_TYPE",
                "replacement transition crosses semantic referent types",
                transition_id=str(row["id"]),
            )
        if int(row["expected_predecessor_version"]) != predecessor.version:
            raise InvariantViolation(
                "CANONICAL_REFERENT_REPLACEMENT_VERSION",
                "replacement transition does not preserve predecessor CAS version",
                transition_id=str(row["id"]),
            )
        return CanonicalReferentReplacementResult(
            transition_id=row["id"],
            tenant_id=row["tenant_id"],
            operation_ref=row["operation_ref"],
            request_fingerprint=row["request_fingerprint"],
            predecessor=predecessor,
            successor=successor,
            effective_at=row["effective_at"],
            transaction_at=row["transaction_at"],
            applied=applied,
        )


def _dump_ref(referent: CanonicalReferentVersionRef) -> str:
    return json.dumps(
        referent.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )


def _load_ref(value: Any) -> CanonicalReferentVersionRef:
    if isinstance(value, str):
        value = json.loads(value)
    return CanonicalReferentVersionRef.model_validate(dict(value))


def _ref_key(referent: CanonicalReferentVersionRef) -> tuple[str, str, int]:
    return referent.type, referent.id, referent.version


def _referent_lock_key(
    tenant_id: UUID,
    referent: CanonicalReferentVersionRef,
) -> str:
    return (
        f"canonical-referent:{tenant_id}:{referent.type}:"
        f"{referent.id}:{referent.version}"
    )


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value
