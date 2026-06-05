"""services/reasoning/sage/affordances/repo.py — Retrieval affordance profiles repo.

Backing schema: db/migrations/0086_sage_retrieval_affordance_profiles.sql.

Public API (Phase 9):

  AffordanceProfilesRepo(pool, *, tenant_id)

    .upsert(profile, *, conn=None) -> RetrievalAffordanceProfile
        Insert-or-update on `model_id`. Bumps `last_updated_at` to now()
        and preserves `created_at` on update.

    .get(model_id, *, conn=None) -> RetrievalAffordanceProfile | None
        Single-row lookup. Returns None when no profile exists yet.

    .bulk_get(model_ids) -> dict[UUID, RetrievalAffordanceProfile]
        Batched fetch (one round-trip via ANY($1::uuid[])).

    .search_by_primitive(primitive, *, limit=50, min_utility=0.0)
        Uses the GIN index on `answers_question_primitives`. Sorted by
        utility DESC so retrieval planners see the most-useful profiles
        first.

    .search_by_hypothesis_type(htype, *, supports=True, limit=50)
        GIN-backed lookup on either `supports_hypothesis_types` or
        `weakens_hypothesis_types` depending on `supports`.

    .reinforce(model_id, delta_utility, *, conn=None)
        Additive reinforcement step. Bumps `utility_score` by
        `delta_utility`, sets `last_reinforced_at = now()`, also
        refreshes `last_updated_at`.

    .decay(model_id, factor, *, conn=None)
        Multiplicative decay (e.g. nightly job calls with 0.95). Updates
        `last_updated_at` but does NOT touch `last_reinforced_at`.

Tenant scoping: every method WHERE-clauses on `tenant_id`. RLS
(migration 0036 / 0086) is defense-in-depth — the repo NEVER relies on
RLS alone to keep tenants apart.

No mocks. Real Postgres. Callers may supply their own connection (so
the upsert can run inside a larger transaction with the Model insert);
otherwise the repo acquires from its pool.
"""
from __future__ import annotations

import json
from typing import Any, Iterable
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import CompanyOSError

from services.reasoning.sage.affordances.types import RetrievalAffordanceProfile


_log = structlog.get_logger(__name__)


class AffordanceProfilesRepoError(CompanyOSError):
    default_code = "affordance_profiles_repo_error"


# Canonical SELECT order — keep stable so Pydantic hydration never has
# to reorder columns.
_SELECT_COLS = (
    "model_id",
    "tenant_id",
    "answers_question_primitives",
    "supports_hypothesis_types",
    "weakens_hypothesis_types",
    "common_composition_types",
    "action_affordances",
    "activation_signatures",
    "projection_policy",
    "utility_score",
    "decay_after",
    "last_reinforced_at",
    "created_at",
    "last_updated_at",
)
_SELECT_COLS_SQL = ", ".join(_SELECT_COLS)


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string for params cast as ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _hydrate(row: asyncpg.Record) -> RetrievalAffordanceProfile:
    """Map an asyncpg row to a typed profile.

    JSONB columns come back as `str` from asyncpg unless a codec is
    registered; we json.loads() defensively to accept both shapes.
    """
    activation = row["activation_signatures"]
    if isinstance(activation, str):
        activation = json.loads(activation)
    projection = row["projection_policy"]
    if isinstance(projection, str):
        projection = json.loads(projection)
    return RetrievalAffordanceProfile(
        model_id=row["model_id"],
        tenant_id=row["tenant_id"],
        answers_question_primitives=list(row["answers_question_primitives"] or []),
        supports_hypothesis_types=list(row["supports_hypothesis_types"] or []),
        weakens_hypothesis_types=list(row["weakens_hypothesis_types"] or []),
        common_composition_types=list(row["common_composition_types"] or []),
        action_affordances=list(row["action_affordances"] or []),
        activation_signatures=dict(activation or {}),
        projection_policy=dict(projection or {}),
        utility_score=float(row["utility_score"]),
        decay_after=row["decay_after"],
        last_reinforced_at=row["last_reinforced_at"],
        created_at=row["created_at"],
        last_updated_at=row["last_updated_at"],
    )


class AffordanceProfilesRepo:
    """Repository for `retrieval_affordance_profiles` rows.

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
        """Return an async context manager that yields a connection.

        Supports both `conn` (caller-owned, just yield it back) and
        pool-acquired (acquire + release) without leaking the
        distinction to callers.
        """
        if conn is not None:
            return _NullCtx(conn)
        if self._pool is None:
            raise AffordanceProfilesRepoError(
                "AffordanceProfilesRepo has no pool and no conn was passed",
            )
        return self._pool.acquire()

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    async def upsert(
        self,
        profile: RetrievalAffordanceProfile,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RetrievalAffordanceProfile:
        """Insert-or-update on `model_id`.

        On conflict, every mutable column is overwritten with the new
        value and `last_updated_at` is bumped to now(). `created_at`
        is preserved on update (it is only written by the row default
        on insert).
        """
        if profile.tenant_id != self.tenant_id:
            raise AffordanceProfilesRepoError(
                "profile.tenant_id does not match repo tenant",
                profile_tenant=str(profile.tenant_id),
                repo_tenant=str(self.tenant_id),
            )
        sql = f"""
            INSERT INTO retrieval_affordance_profiles (
              model_id, tenant_id,
              answers_question_primitives, supports_hypothesis_types,
              weakens_hypothesis_types, common_composition_types,
              action_affordances,
              activation_signatures, projection_policy,
              utility_score, decay_after, last_reinforced_at,
              last_updated_at
            )
            VALUES (
              $1, $2,
              $3::text[], $4::text[],
              $5::text[], $6::text[],
              $7::text[],
              $8::jsonb, $9::jsonb,
              $10, $11, $12,
              now()
            )
            ON CONFLICT (model_id) DO UPDATE SET
              answers_question_primitives = EXCLUDED.answers_question_primitives,
              supports_hypothesis_types   = EXCLUDED.supports_hypothesis_types,
              weakens_hypothesis_types    = EXCLUDED.weakens_hypothesis_types,
              common_composition_types    = EXCLUDED.common_composition_types,
              action_affordances          = EXCLUDED.action_affordances,
              activation_signatures       = EXCLUDED.activation_signatures,
              projection_policy           = EXCLUDED.projection_policy,
              utility_score               = EXCLUDED.utility_score,
              decay_after                 = EXCLUDED.decay_after,
              last_reinforced_at          = EXCLUDED.last_reinforced_at,
              last_updated_at             = now()
            RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(
                sql,
                profile.model_id,
                profile.tenant_id,
                list(profile.answers_question_primitives),
                list(profile.supports_hypothesis_types),
                list(profile.weakens_hypothesis_types),
                list(profile.common_composition_types),
                list(profile.action_affordances),
                _jsonb(profile.activation_signatures),
                _jsonb(profile.projection_policy),
                float(profile.utility_score),
                profile.decay_after,
                profile.last_reinforced_at,
            )
        return _hydrate(row)

    async def reinforce(
        self,
        model_id: UUID,
        delta_utility: float,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RetrievalAffordanceProfile | None:
        """Add `delta_utility` to `utility_score`; stamp reinforcement time.

        Returns the updated profile, or None if no profile exists for
        `model_id`. Does NOT auto-create a profile — callers should
        upsert() a default first (typically via
        `policy.derive_default_profile_from_model`).
        """
        sql = f"""
            UPDATE retrieval_affordance_profiles
               SET utility_score      = utility_score + $3,
                   last_reinforced_at = now(),
                   last_updated_at    = now()
             WHERE model_id  = $1
               AND tenant_id = $2
         RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(sql, model_id, self.tenant_id, float(delta_utility))
        return _hydrate(row) if row else None

    async def decay(
        self,
        model_id: UUID,
        factor: float,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RetrievalAffordanceProfile | None:
        """Multiply `utility_score` by `factor` (e.g. 0.95 nightly).

        Does NOT update `last_reinforced_at` — decay is the opposite of
        reinforcement. `last_updated_at` is bumped so dirty-tracking
        downstream knows the row was touched.
        """
        sql = f"""
            UPDATE retrieval_affordance_profiles
               SET utility_score   = utility_score * $3,
                   last_updated_at = now()
             WHERE model_id  = $1
               AND tenant_id = $2
         RETURNING {_SELECT_COLS_SQL}
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(sql, model_id, self.tenant_id, float(factor))
        return _hydrate(row) if row else None

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    async def get(
        self,
        model_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RetrievalAffordanceProfile | None:
        sql = f"""
            SELECT {_SELECT_COLS_SQL}
              FROM retrieval_affordance_profiles
             WHERE model_id  = $1
               AND tenant_id = $2
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            row = await c.fetchrow(sql, model_id, self.tenant_id)
        return _hydrate(row) if row else None

    async def bulk_get(
        self,
        model_ids: Iterable[UUID],
        *,
        conn: asyncpg.Connection | None = None,
    ) -> dict[UUID, RetrievalAffordanceProfile]:
        ids = list(model_ids)
        if not ids:
            return {}
        sql = f"""
            SELECT {_SELECT_COLS_SQL}
              FROM retrieval_affordance_profiles
             WHERE tenant_id = $1
               AND model_id  = ANY($2::uuid[])
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(sql, self.tenant_id, ids)
        return {row["model_id"]: _hydrate(row) for row in rows}

    async def search_by_primitive(
        self,
        primitive: str,
        *,
        limit: int = 50,
        min_utility: float = 0.0,
        conn: asyncpg.Connection | None = None,
    ) -> list[RetrievalAffordanceProfile]:
        """Return profiles whose `answers_question_primitives` contains
        `primitive`, ordered by utility DESC.

        Uses the GIN index on `answers_question_primitives` (the `@>`
        contains operator is GIN-friendly).
        """
        sql = f"""
            SELECT {_SELECT_COLS_SQL}
              FROM retrieval_affordance_profiles
             WHERE tenant_id = $1
               AND answers_question_primitives @> ARRAY[$2]::text[]
               AND utility_score >= $3
          ORDER BY utility_score DESC, last_updated_at DESC
             LIMIT $4
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(
                sql, self.tenant_id, primitive, float(min_utility), int(limit),
            )
        return [_hydrate(r) for r in rows]

    async def search_by_primitive_context(
        self,
        primitive: str,
        *,
        entities: list[str],
        limit: int = 50,
        min_utility: float = 0.0,
        fallback_limit: int | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[RetrievalAffordanceProfile]:
        """Return primitive matches, preferring activation-signature overlap.

        Large tenants can accumulate many high-utility affordances for a
        primitive. A plain utility sort can crowd out the right Model
        when the signal mentions a specific customer/system. This query
        first retrieves profiles whose JSONB activation_signatures contain
        at least one current entity, then fills any remaining budget with
        global utility-ranked profiles.
        """
        clean_entities = []
        for entity in entities:
            text = str(entity).strip()
            if text and text not in clean_entities:
                clean_entities.append(text)
        if not clean_entities:
            return await self.search_by_primitive(
                primitive,
                limit=limit,
                min_utility=min_utility,
                conn=conn,
            )

        fallback = (
            max(0, int(fallback_limit))
            if fallback_limit is not None
            else max(0, int(limit // 3))
        )
        entity_clauses: list[str] = []
        params: list[Any] = [
            self.tenant_id,
            primitive,
            float(min_utility),
            int(limit),
            int(fallback),
        ]
        for entity in clean_entities[:12]:
            params.append(_jsonb({"entities": [entity]}))
            entity_clauses.append(
                f"activation_signatures @> ${len(params)}::jsonb"
            )
        entity_sql = " OR ".join(entity_clauses)
        sql = f"""
            WITH contextual AS (
                SELECT {_SELECT_COLS_SQL}, 1 AS context_rank
                  FROM retrieval_affordance_profiles
                 WHERE tenant_id = $1
                   AND answers_question_primitives @> ARRAY[$2]::text[]
                   AND utility_score >= $3
                   AND ({entity_sql})
              ORDER BY utility_score DESC, last_updated_at DESC
                 LIMIT $4
            ),
            fallback AS (
                SELECT {_SELECT_COLS_SQL}, 0 AS context_rank
                  FROM retrieval_affordance_profiles
                 WHERE tenant_id = $1
                   AND answers_question_primitives @> ARRAY[$2]::text[]
                   AND utility_score >= $3
              ORDER BY utility_score DESC, last_updated_at DESC
                 LIMIT $5
            ),
            combined AS (
                SELECT * FROM contextual
                UNION ALL
                SELECT * FROM fallback
            )
            SELECT DISTINCT ON (model_id) {_SELECT_COLS_SQL}
              FROM combined
          ORDER BY model_id, context_rank DESC, utility_score DESC
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(sql, *params)
        profiles = [_hydrate(r) for r in rows]
        profiles.sort(
            key=lambda p: (
                -_profile_entity_overlap(p.activation_signatures, clean_entities),
                -float(p.utility_score or 0.0),
                str(p.model_id),
            )
        )
        return profiles[: int(limit)]

    async def search_by_hypothesis_type(
        self,
        htype: str,
        *,
        supports: bool = True,
        limit: int = 50,
        conn: asyncpg.Connection | None = None,
    ) -> list[RetrievalAffordanceProfile]:
        """Return profiles whose support/weaken list contains `htype`.

        `supports=True` (default) hits the GIN index on
        `supports_hypothesis_types`; `supports=False` scans
        `weakens_hypothesis_types` (also GIN'd implicitly if added
        later — currently no explicit index, falls back to seq scan
        on tiny tables, but per-tenant filter keeps it cheap).
        """
        column = "supports_hypothesis_types" if supports else "weakens_hypothesis_types"
        sql = f"""
            SELECT {_SELECT_COLS_SQL}
              FROM retrieval_affordance_profiles
             WHERE tenant_id = $1
               AND {column} @> ARRAY[$2]::text[]
          ORDER BY utility_score DESC, last_updated_at DESC
             LIMIT $3
        """
        ctx = await self._acquire(conn)
        async with ctx as c:
            rows = await c.fetch(sql, self.tenant_id, htype, int(limit))
        return [_hydrate(r) for r in rows]


# ---------------------------------------------------------------------
# Tiny helper for the conn-or-pool branch
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


def _profile_entity_overlap(
    activation_signatures: dict[str, Any],
    entities: list[str],
) -> int:
    raw = activation_signatures.get("entities")
    if not isinstance(raw, list) or not entities:
        return 0
    profile_entities = {str(item).casefold() for item in raw if item is not None}
    return sum(1 for entity in entities if entity.casefold() in profile_entities)


__all__ = [
    "AffordanceProfilesRepo",
    "AffordanceProfilesRepoError",
]
