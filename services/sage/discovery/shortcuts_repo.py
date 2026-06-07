"""services/sage/discovery/shortcuts_repo.py — Discovery Shortcuts repo (Phase 10).

Backing schema: db/migrations/0052_sage_discovery_and_negative_memory.sql.

This repo manages the Discovery Utility Layer's shortcut store
(fyralis-sage-synthesis-self-evolution.md §11). A shortcut is a
learned retrieval utility — when an inquiry signature appears, the
target Model / region / affordance has historically been useful to
inspect. Nothing here is canonical truth; the doc §2 distinction is
load-bearing.

Public API (Phase 10):

  DiscoveryShortcutsRepo(pool, *, tenant_id)

    .find_for_signature(signature, *, limit=10, conn=None)
        Returns shortcuts whose `from_signature` is a *superset* of
        the probe signature (so a precise probe selects only narrowly
        matching shortcuts, while a partial probe surfaces every
        shortcut that shares the same fragment). Filters expired rows
        out so callers never act on stale utility. Sorted by
        `utility_score DESC` so the hottest shortcuts win first.

    .record_success(shortcut_id, *, conn=None)
        Bumps `success_count`, `utility_score`, and `last_success_at`.
        Utility additively rewards success.

    .record_failure(shortcut_id, *, conn=None)
        Bumps `failure_count`, decays `utility_score` multiplicatively,
        and stamps `last_failure_at`. Clamps at 0 to satisfy the
        `utility_score >= 0` CHECK.

    .upsert_from_outcome(signature, *, to_model_id|to_region_id|
        to_affordance, delta_utility, conn=None)
        Insert-or-find by (tenant, signature, target). On hit, bumps
        utility by `delta_utility`. On miss, inserts a fresh row with
        utility = max(0, delta_utility). The Synthesis post-commit
        evaluator calls this when a path appears in a valid diff
        (doc Phase 10 update rules).

    .sweep_expired(*, conn=None) -> int
        Deletes rows whose `expires_at <= now()`. Returns deletion
        count. Intended for a nightly job.

Tenant scoping: every method WHERE-clauses on `self.tenant_id`. RLS
(migration 0036 / 0052) is defense-in-depth — the repo NEVER relies on
RLS alone to keep tenants apart.

No mocks. Real Postgres. Callers may pass `conn=` to participate in a
larger transaction (e.g. the apply-layer transaction that records the
valid diff that justified the shortcut).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7

from services.sage.discovery.types import DiscoveryShortcut, Signature


_log = structlog.get_logger(__name__)


# Tunables for the success / failure utility update rule. Kept module-
# level so tests can monkey-patch and callers can read them when
# reasoning about behavior.
SUCCESS_UTILITY_BUMP = 0.1
FAILURE_DECAY_FACTOR = 0.5
UTILITY_FLOOR = 0.0


class DiscoveryShortcutsRepoError(CompanyOSError):
    default_code = "discovery_shortcuts_repo_error"


# Canonical SELECT order — keep stable so hydration never reorders.
_SELECT_COLS = (
    "id",
    "tenant_id",
    "from_signature",
    "to_model_id",
    "to_region_id",
    "to_affordance",
    "utility_score",
    "success_count",
    "failure_count",
    "last_success_at",
    "last_failure_at",
    "expires_at",
    "created_at",
    "updated_at",
)
_SELECT_COLS_SQL = ", ".join(_SELECT_COLS)


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string for params cast as ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _normalize_signature(signature: Signature | dict[str, Any]) -> dict[str, Any]:
    """Accept either a Pydantic Signature or a raw dict and return
    the JSONB-ready dict form."""
    if isinstance(signature, Signature):
        return signature.to_jsonable()
    if not isinstance(signature, dict):
        raise DiscoveryShortcutsRepoError(
            "signature must be a Signature or dict",
            got=type(signature).__name__,
        )
    return signature


def _hydrate(row: asyncpg.Record) -> DiscoveryShortcut:
    """Map an asyncpg row to a typed `DiscoveryShortcut`.

    JSONB columns may arrive as `str` if no codec is registered on
    the connection; we json.loads() defensively to accept both shapes.
    """
    sig = row["from_signature"]
    if isinstance(sig, str):
        sig = json.loads(sig)
    return DiscoveryShortcut(
        id=row["id"],
        tenant_id=row["tenant_id"],
        from_signature=dict(sig or {}),
        to_model_id=row["to_model_id"],
        to_region_id=row["to_region_id"],
        to_affordance=row["to_affordance"],
        utility_score=float(row["utility_score"]),
        success_count=int(row["success_count"]),
        failure_count=int(row["failure_count"]),
        last_success_at=row["last_success_at"],
        last_failure_at=row["last_failure_at"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class DiscoveryShortcutsRepo:
    """Repository for `discovery_shortcuts` rows.

    Tenant-scoped — instantiate once per request / worker with the
    bound tenant. All read + write methods WHERE-clause on
    `self.tenant_id`; the `pool` argument is only used when the caller
    does not supply a `conn`.
    """

    def __init__(self, pool: asyncpg.Pool | None, *, tenant_id: UUID) -> None:
        self._pool = pool
        self.tenant_id = tenant_id

    # ------------------------------------------------------------------
    # Connection helper — mirrors services/sage/affordances/repo.py
    # ------------------------------------------------------------------

    async def _acquire(self, conn: asyncpg.Connection | None):
        if conn is not None:
            return _NullCtx(conn)
        if self._pool is None:
            raise DiscoveryShortcutsRepoError(
                "DiscoveryShortcutsRepo has no pool and no conn was passed",
            )
        return self._pool.acquire()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def find_for_signature(
        self,
        signature: Signature | dict[str, Any],
        *,
        limit: int = 10,
        conn: asyncpg.Connection | None = None,
    ) -> list[DiscoveryShortcut]:
        """Return shortcuts compatible with the probe signature and
        whose `expires_at` (if set) is in the future, sorted by
        `utility_score DESC`.

        We accept both containment directions. If a
        shortcut was learned for
        ``{question_primitive: DEPENDENCY, entities: [SSO]}`` then a
        probe like ``{question_primitive: DEPENDENCY}`` will surface
        it. Conversely, if a shortcut was learned for the broader
        ``{signal_type: T1, question_primitive: DEPENDENCY}``, then a
        later probe with extra cue entities should also surface it.
        """
        sig = _normalize_signature(signature)
        sql = f"""
            SELECT {_SELECT_COLS_SQL},
                   CASE
                     WHEN from_signature @> $2::jsonb THEN 2
                     WHEN $2::jsonb @> from_signature THEN 1
                     ELSE 0
                   END AS match_rank
              FROM discovery_shortcuts
             WHERE tenant_id = $1
               AND (
                 from_signature @> $2::jsonb
                 OR $2::jsonb @> from_signature
               )
               AND (expires_at IS NULL OR expires_at > now())
          ORDER BY match_rank DESC,
                   (
                     SELECT count(*)
                     FROM jsonb_object_keys(from_signature)
                   ) DESC,
                   utility_score DESC,
                   updated_at DESC
             LIMIT $3
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(sql, self.tenant_id, _jsonb(sig), int(limit))
        return [_hydrate(r) for r in rows]

    # ------------------------------------------------------------------
    # Write — success / failure utility update
    # ------------------------------------------------------------------

    async def record_success(
        self,
        shortcut_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> DiscoveryShortcut | None:
        """Reward a shortcut that helped: bump `success_count`, add
        `SUCCESS_UTILITY_BUMP` to `utility_score`, stamp
        `last_success_at = now()`.

        Returns the updated shortcut, or None if no row matched.
        """
        sql = f"""
            UPDATE discovery_shortcuts
               SET success_count   = success_count + 1,
                   utility_score   = utility_score + $3,
                   last_success_at = now(),
                   updated_at      = now()
             WHERE id        = $1
               AND tenant_id = $2
         RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(
                sql, shortcut_id, self.tenant_id, float(SUCCESS_UTILITY_BUMP),
            )
        return _hydrate(row) if row else None

    async def record_failure(
        self,
        shortcut_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> DiscoveryShortcut | None:
        """Punish a shortcut that misfired: bump `failure_count`,
        multiply `utility_score` by `FAILURE_DECAY_FACTOR`, clamp to
        `UTILITY_FLOOR`, stamp `last_failure_at = now()`.

        The clamp is essential — the SQL CHECK forbids negative
        utility, and we want decay (not sign-flip) to be the failure
        signal. Returns the updated shortcut, or None if no row
        matched.
        """
        sql = f"""
            UPDATE discovery_shortcuts
               SET failure_count   = failure_count + 1,
                   utility_score   = GREATEST($4, utility_score * $3),
                   last_failure_at = now(),
                   updated_at      = now()
             WHERE id        = $1
               AND tenant_id = $2
         RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(
                sql,
                shortcut_id,
                self.tenant_id,
                float(FAILURE_DECAY_FACTOR),
                float(UTILITY_FLOOR),
            )
        return _hydrate(row) if row else None

    # ------------------------------------------------------------------
    # Write — upsert from a fresh inquiry outcome
    # ------------------------------------------------------------------

    async def upsert_from_outcome(
        self,
        signature: Signature | dict[str, Any],
        *,
        to_model_id: UUID | None = None,
        to_region_id: UUID | None = None,
        to_affordance: str | None = None,
        delta_utility: float,
        expires_at: Any = None,
        conn: asyncpg.Connection | None = None,
    ) -> DiscoveryShortcut:
        """Find-or-insert a shortcut for (tenant, signature, target);
        bump its utility by `delta_utility`.

        Target selection: exactly one of `to_model_id`, `to_region_id`,
        `to_affordance` should be supplied — this matches the SQL CHECK
        `discovery_shortcuts_has_target` which requires at least one
        non-null target. If multiple are supplied they are all stored,
        but the row-find query treats the triple as the natural key.

        Utility update: on hit, additive bump (mirrors record_success
        semantics but with a caller-controlled delta — the Synthesis
        outcome evaluator decides "how good was this path"). On miss,
        insert with utility = max(UTILITY_FLOOR, delta_utility) so the
        CHECK is satisfied even if a caller passes a negative delta
        for a new shortcut.
        """
        if to_model_id is None and to_region_id is None and to_affordance is None:
            raise DiscoveryShortcutsRepoError(
                "upsert_from_outcome requires at least one target "
                "(to_model_id, to_region_id, or to_affordance)",
            )
        sig = _normalize_signature(signature)

        # The natural key is (tenant_id, from_signature, to_model_id,
        # to_region_id, to_affordance). We do NOT have a UNIQUE
        # constraint on this composite (the SQL schema keeps the
        # target columns nullable + flexible), so we do a manual
        # SELECT-or-INSERT inside the caller's transaction. Callers
        # who care about strict idempotency should pass conn= so this
        # whole upsert runs in one transaction.
        # Explicit casts on $3/$4/$5: when any of these is None, asyncpg
        # can't infer the column type from `IS NOT DISTINCT FROM` alone
        # and raises IndeterminateDatatypeError (or coerces to text and
        # produces an empty-string uuid error). The casts pin the types.
        find_sql = f"""
            SELECT {_SELECT_COLS_SQL}
              FROM discovery_shortcuts
             WHERE tenant_id      = $1
               AND from_signature = $2::jsonb
               AND to_model_id    IS NOT DISTINCT FROM $3::uuid
               AND to_region_id   IS NOT DISTINCT FROM $4::uuid
               AND to_affordance  IS NOT DISTINCT FROM $5::text
             LIMIT 1
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            existing = await c.fetchrow(
                find_sql,
                self.tenant_id,
                _jsonb(sig),
                to_model_id,
                to_region_id,
                to_affordance,
            )
            if existing is not None:
                bump_sql = f"""
                    UPDATE discovery_shortcuts
                       SET utility_score = GREATEST($3, utility_score + $2),
                           updated_at    = now()
                     WHERE id = $1
                 RETURNING {_SELECT_COLS_SQL}
                """
                row = await c.fetchrow(
                    bump_sql,
                    existing["id"],
                    float(delta_utility),
                    float(UTILITY_FLOOR),
                )
                return _hydrate(row)

            # Insert a fresh shortcut.
            insert_sql = f"""
                INSERT INTO discovery_shortcuts (
                  id, tenant_id, from_signature,
                  to_model_id, to_region_id, to_affordance,
                  utility_score, expires_at
                ) VALUES (
                  $1, $2, $3::jsonb,
                  $4::uuid, $5::uuid, $6::text,
                  $7, $8
                )
                RETURNING {_SELECT_COLS_SQL}
            """
            new_id = uuid7()
            initial_utility = max(UTILITY_FLOOR, float(delta_utility))
            row = await c.fetchrow(
                insert_sql,
                new_id,
                self.tenant_id,
                _jsonb(sig),
                to_model_id,
                to_region_id,
                to_affordance,
                initial_utility,
                expires_at,
            )
        return _hydrate(row)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def sweep_expired(
        self,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Delete expired shortcuts for this tenant. Returns deletion count.

        Intended for a nightly maintenance job. Per-tenant scope keeps
        the sweep cheap and gives the operator a knob to schedule
        differently per tenant if needed later.
        """
        sql = """
            DELETE FROM discovery_shortcuts
             WHERE tenant_id  = $1
               AND expires_at IS NOT NULL
               AND expires_at <= now()
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            result = await c.execute(sql, self.tenant_id)
        # asyncpg .execute returns 'DELETE <n>' — parse the count off
        # the tag. Fall back to 0 if anything looks off.
        try:
            return int(result.split()[-1])
        except (ValueError, AttributeError, IndexError):
            return 0


# ---------------------------------------------------------------------
# Tiny helper for the conn-or-pool branch (mirrors affordances/repo.py)
# ---------------------------------------------------------------------


class _NullCtx:
    """Async context manager that yields a pre-acquired connection.

    Used when the caller passes their own `conn` — we must NOT release
    it back to the pool, only yield it back so the same `async with`
    pattern works either way.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


__all__ = [
    "DiscoveryShortcutsRepo",
    "DiscoveryShortcutsRepoError",
    "FAILURE_DECAY_FACTOR",
    "SUCCESS_UTILITY_BUMP",
    "UTILITY_FLOOR",
]
