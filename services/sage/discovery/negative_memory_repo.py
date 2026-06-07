"""services/sage/discovery/negative_memory_repo.py — Negative Memory repo (Phase 10).

Backing schema: db/migrations/0052_sage_discovery_and_negative_memory.sql.

Negative memory stores rejected hypotheses, noisy paths, failed
shortcuts, and low-value nodes (doc §14). It lives in the Discovery
Utility Layer — nothing here is canonical truth. The expiry contract
is load-bearing: doc §14 explicitly mandates that every negative
memory expires "because company reality changes". The SQL column is
NOT NULL to enforce this at the schema level; the repo additionally
exposes evidence-driven invalidation so a memory grounded in a
specific evidence snapshot is dropped the moment that evidence
shifts.

Public API (Phase 10):

  NegativeMemoryRepo(pool, *, tenant_id)

    .insert(memory, *, conn=None) -> NegativeMemory
        Insert a new negative memory row. Returns the hydrated row
        (with the DB-assigned id / created_at).

    .find_for_signature(signature, *, memory_type=None, conn=None)
        Returns non-expired negative memory whose `signature @>
        $signature`. When `memory_type` is supplied, filters to that
        kind only — typical use is "find rejected_hypotheses that
        share this inquiry signature so the deep reasoner does not
        re-derive them."

    .sweep_expired(*, conn=None) -> int
        Deletes expired rows for this tenant. Returns deletion count.

    .invalidate_by_evidence_change(signature, new_evidence_hash, *, conn=None)
        Deletes negative memory whose signature matches and whose
        stored `evidence_snapshot_hash` differs from `new_evidence_hash`.
        Rows with a NULL `evidence_snapshot_hash` are NOT touched —
        those negatives were not pinned to a specific evidence
        snapshot, so an evidence diff is not authoritative grounds to
        drop them; they will age out via the normal expiry sweep.

Tenant scoping: every method WHERE-clauses on `self.tenant_id`. RLS
(migration 0036 / 0052) is defense-in-depth — the repo NEVER relies on
RLS alone to keep tenants apart.

No mocks. Real Postgres. Callers may pass `conn=` to participate in a
larger transaction (e.g. the validation transaction that just
rejected the hypothesis being recorded).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7

from services.sage.discovery.types import (
    NegativeMemory,
    NegativeMemoryType,
    Signature,
)


_log = structlog.get_logger(__name__)


class NegativeMemoryRepoError(CompanyOSError):
    default_code = "negative_memory_repo_error"


_SELECT_COLS = (
    "id",
    "tenant_id",
    "memory_type",
    "signature",
    "rejected_claim",
    "rejected_path",
    "reason",
    "evidence_snapshot_hash",
    "confidence",
    "created_at",
    "expires_at",
)
_SELECT_COLS_SQL = ", ".join(_SELECT_COLS)


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string for params cast as ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _normalize_signature(signature: Signature | dict[str, Any]) -> dict[str, Any]:
    if isinstance(signature, Signature):
        return signature.to_jsonable()
    if not isinstance(signature, dict):
        raise NegativeMemoryRepoError(
            "signature must be a Signature or dict",
            got=type(signature).__name__,
        )
    return signature


def _hydrate(row: asyncpg.Record) -> NegativeMemory:
    sig = row["signature"]
    if isinstance(sig, str):
        sig = json.loads(sig)
    rejected_path = row["rejected_path"]
    if isinstance(rejected_path, str):
        rejected_path = json.loads(rejected_path)
    return NegativeMemory(
        id=row["id"],
        tenant_id=row["tenant_id"],
        memory_type=row["memory_type"],
        signature=dict(sig or {}),
        rejected_claim=row["rejected_claim"],
        rejected_path=rejected_path,
        reason=row["reason"],
        evidence_snapshot_hash=row["evidence_snapshot_hash"],
        confidence=(
            float(row["confidence"]) if row["confidence"] is not None else None
        ),
        created_at=row["created_at"],
        expires_at=row["expires_at"],
    )


class NegativeMemoryRepo:
    """Repository for `negative_memory` rows.

    Tenant-scoped — instantiate once per request / worker with the
    bound tenant. All read + write methods WHERE-clause on
    `self.tenant_id`; the `pool` argument is only used when the caller
    does not supply a `conn`.
    """

    def __init__(self, pool: asyncpg.Pool | None, *, tenant_id: UUID) -> None:
        self._pool = pool
        self.tenant_id = tenant_id

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    async def _acquire(self, conn: asyncpg.Connection | None):
        if conn is not None:
            return _NullCtx(conn)
        if self._pool is None:
            raise NegativeMemoryRepoError(
                "NegativeMemoryRepo has no pool and no conn was passed",
            )
        return self._pool.acquire()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def insert(
        self,
        memory: NegativeMemory,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> NegativeMemory:
        """Insert a new negative memory row.

        If `memory.id` collides with an existing row this raises
        (asyncpg.UniqueViolationError). Callers building a fresh
        memory typically construct with `id=uuid7()`; we also
        defensively generate one here if the caller passed a
        sentinel-equivalent.
        """
        if memory.tenant_id != self.tenant_id:
            raise NegativeMemoryRepoError(
                "memory.tenant_id does not match repo tenant",
                memory_tenant=str(memory.tenant_id),
                repo_tenant=str(self.tenant_id),
            )
        new_id = memory.id or uuid7()
        sql = f"""
            INSERT INTO negative_memory (
              id, tenant_id, memory_type, signature,
              rejected_claim, rejected_path,
              reason, evidence_snapshot_hash, confidence,
              expires_at
            ) VALUES (
              $1, $2, $3, $4::jsonb,
              $5, $6::jsonb,
              $7, $8, $9,
              $10
            )
            RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(
                sql,
                new_id,
                memory.tenant_id,
                memory.memory_type,
                _jsonb(memory.signature),
                memory.rejected_claim,
                _jsonb(memory.rejected_path) if memory.rejected_path is not None else None,
                memory.reason,
                memory.evidence_snapshot_hash,
                memory.confidence,
                memory.expires_at,
            )
        return _hydrate(row)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def find_for_signature(
        self,
        signature: Signature | dict[str, Any],
        *,
        memory_type: NegativeMemoryType | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[NegativeMemory]:
        """Return non-expired negative memory compatible with `signature`.

        Matching mirrors DiscoveryShortcutsRepo: both containment
        directions count. A stored broad dead-end for
        ``{signal_type: T1, question_primitive: DEPENDENCY}`` should
        match a later probe that carries extra cue entities, while a
        more specific stored dead-end should still match a broad probe.
        """
        sig = _normalize_signature(signature)
        params: list[Any] = [self.tenant_id, _jsonb(sig)]
        type_clause = ""
        if memory_type is not None:
            params.append(memory_type)
            type_clause = f" AND memory_type = ${len(params)}"
        sql = f"""
            SELECT {_SELECT_COLS_SQL},
                   CASE
                     WHEN signature @> $2::jsonb THEN 2
                     WHEN $2::jsonb @> signature THEN 1
                     ELSE 0
                   END AS match_rank
              FROM negative_memory
             WHERE tenant_id  = $1
               AND (
                 signature @> $2::jsonb
                 OR $2::jsonb @> signature
               )
               AND expires_at > now()
               {type_clause}
          ORDER BY match_rank DESC,
                   (
                     SELECT count(*)
                     FROM jsonb_object_keys(signature)
                   ) DESC,
                   created_at DESC
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(sql, *params)
        return [_hydrate(r) for r in rows]

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def sweep_expired(
        self,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Delete expired negative memory rows for this tenant.

        Returns deletion count. Intended for a nightly maintenance
        job — runs in tandem with DiscoveryShortcutsRepo.sweep_expired.
        """
        sql = """
            DELETE FROM negative_memory
             WHERE tenant_id  = $1
               AND expires_at <= now()
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            result = await c.execute(sql, self.tenant_id)
        try:
            return int(result.split()[-1])
        except (ValueError, AttributeError, IndexError):
            return 0

    async def invalidate_by_evidence_change(
        self,
        signature: Signature | dict[str, Any],
        new_evidence_hash: str,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Drop negatives whose evidence snapshot no longer matches.

        Deletes rows where:
          * `signature @> $signature` (the memory is keyed by this
            inquiry shape), AND
          * `evidence_snapshot_hash IS NOT NULL` (the memory was
            pinned to a specific snapshot), AND
          * `evidence_snapshot_hash <> new_evidence_hash` (the
            snapshot has since changed).

        Rows with NULL `evidence_snapshot_hash` are NOT touched —
        those negatives are not pinned to specific evidence and will
        instead age out via `sweep_expired`.

        Returns deletion count.
        """
        sig = _normalize_signature(signature)
        sql = """
            DELETE FROM negative_memory
             WHERE tenant_id  = $1
               AND signature  @> $2::jsonb
               AND evidence_snapshot_hash IS NOT NULL
               AND evidence_snapshot_hash <> $3
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            result = await c.execute(
                sql, self.tenant_id, _jsonb(sig), new_evidence_hash,
            )
        try:
            return int(result.split()[-1])
        except (ValueError, AttributeError, IndexError):
            return 0


# ---------------------------------------------------------------------
# Tiny helper for the conn-or-pool branch (mirrors affordances/repo.py)
# ---------------------------------------------------------------------


class _NullCtx:
    """Async context manager that yields a pre-acquired connection."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


__all__ = [
    "NegativeMemoryRepo",
    "NegativeMemoryRepoError",
]
