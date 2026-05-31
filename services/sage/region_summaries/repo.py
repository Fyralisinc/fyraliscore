"""services.sage.region_summaries.repo — asyncpg surface for region summaries.

Tenant-bound repo over `region_sufficient_state` (migration 0053).
The repo is constructed with the tenant up-front so callers cannot
accidentally cross tenants — every public method scopes its WHERE
clause to `self._tenant_id`. This mirrors the pattern used by the
predictions / inquiry repos (post-0036 RLS rollout) and is a
deliberate divergence from `services.models.repo.ModelsRepo`, which
predates per-instance tenant binding.

All write methods accept an optional `conn=` for callers already
inside a transaction; otherwise the repo acquires from the pool.

Spec reference: fyralis-sage-synthesis-self-evolution.md §12 / Phase 11.
"""
from __future__ import annotations

import json
from typing import Any, Sequence
from uuid import UUID

import asyncpg

from lib.shared.db import RowHydrationError
from lib.shared.errors import CompanyOSError
from services.sage.region_summaries.types import (
    RegionSufficientState,
)


class RegionSummariesRepoError(CompanyOSError):
    default_code = "region_summaries_repo_error"


# Canonical read order — keep all SELECTs in one shape so hydration
# never has to reorder columns.
_SELECT_COLS = (
    "region_id",
    "tenant_id",
    "region_label",
    "summary",
    "active_hypotheses",
    "active_constraints",
    "known_counterevidence",
    "unresolved_unknowns",
    "affected_goals",
    "affected_commitments",
    "member_model_ids",
    "priority_score",
    "prediction_error_score",
    "next_best_frontiers",
    "falsification_watch",
    "last_refreshed_reason",
    "created_at",
    "updated_at",
)
_SELECT_COLS_SQL = ", ".join(_SELECT_COLS)


def _jsonb(value: Any) -> str:
    """asyncpg requires a JSON string when the param is cast ::jsonb.

    `default=str` lets nested UUID/datetime values serialize without
    forcing the caller to pre-stringify them; `sort_keys=True` keeps
    diffs deterministic, matching `services.models.repo._jsonb`.
    """
    return json.dumps(value, sort_keys=True, default=str)


def _hydrate_row(record: asyncpg.Record) -> RegionSufficientState:
    """asyncpg Record -> RegionSufficientState.

    Tolerates the JSONB string codec (asyncpg returns text for jsonb
    when no codec is registered) by parsing every json column on the
    way in. Numeric columns and UUID arrays are passed through.
    """
    raw = dict(record)
    for key in (
        "active_hypotheses",
        "active_constraints",
        "known_counterevidence",
        "unresolved_unknowns",
        "next_best_frontiers",
        "falsification_watch",
    ):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                # Leave the string in place; Pydantic will surface the
                # validation error with the offending column name.
                pass
    try:
        return RegionSufficientState.model_validate(raw)
    except Exception as e:
        raise RowHydrationError(
            f"could not hydrate region_sufficient_state row: {e}",
            row_keys=list(record.keys()),
        ) from e


def _serialize_jsonb_list(items: Sequence[Any]) -> str:
    """Pydantic model list -> JSONB text via per-item model_dump."""
    return _jsonb([
        item.model_dump(mode="json") if hasattr(item, "model_dump") else item
        for item in items
    ])


class RegionSummariesRepo:
    """Tenant-scoped repo for `region_sufficient_state`.

    Construction:
        RegionSummariesRepo(pool, *, tenant_id=...)

    The tenant is bound at construction time. Pool may be `None` only
    if every caller passes an explicit `conn=` — methods that need a
    pool when conn is None raise via `_require_pool()`.
    """

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RegionSummariesRepoError(
                "RegionSummariesRepo was constructed without a pool; "
                "callers in conn-only mode must pass conn= on every call"
            )
        return self._pool

    # =================================================================
    # upsert
    # =================================================================
    async def upsert(
        self,
        region: RegionSufficientState,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RegionSufficientState:
        """Insert or update a region summary keyed by `region_id`.

        The row's tenant is taken from `self._tenant_id`, NOT from
        `region.tenant_id`, so a misaligned argument cannot leak
        cross-tenant. On conflict (same region_id) every mutable
        column is overwritten and `updated_at` is bumped to `now()`.
        `created_at` is preserved on update.
        """
        if region.tenant_id != self._tenant_id:
            raise RegionSummariesRepoError(
                "region.tenant_id does not match repo tenant_id",
                region_tenant=str(region.tenant_id),
                repo_tenant=str(self._tenant_id),
            )

        async def _run(c: asyncpg.Connection) -> RegionSufficientState:
            row = await c.fetchrow(
                f"""
                INSERT INTO region_sufficient_state (
                    region_id, tenant_id, region_label, summary,
                    active_hypotheses, active_constraints,
                    known_counterevidence, unresolved_unknowns,
                    affected_goals, affected_commitments, member_model_ids,
                    priority_score, prediction_error_score,
                    next_best_frontiers, falsification_watch,
                    last_refreshed_reason
                ) VALUES (
                    $1, $2, $3, $4,
                    $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb,
                    $9::uuid[], $10::uuid[], $11::uuid[],
                    $12, $13,
                    $14::jsonb, $15::jsonb,
                    $16
                )
                ON CONFLICT (region_id) DO UPDATE SET
                    region_label = EXCLUDED.region_label,
                    summary = EXCLUDED.summary,
                    active_hypotheses = EXCLUDED.active_hypotheses,
                    active_constraints = EXCLUDED.active_constraints,
                    known_counterevidence = EXCLUDED.known_counterevidence,
                    unresolved_unknowns = EXCLUDED.unresolved_unknowns,
                    affected_goals = EXCLUDED.affected_goals,
                    affected_commitments = EXCLUDED.affected_commitments,
                    member_model_ids = EXCLUDED.member_model_ids,
                    priority_score = EXCLUDED.priority_score,
                    prediction_error_score = EXCLUDED.prediction_error_score,
                    next_best_frontiers = EXCLUDED.next_best_frontiers,
                    falsification_watch = EXCLUDED.falsification_watch,
                    last_refreshed_reason = EXCLUDED.last_refreshed_reason,
                    updated_at = now()
                RETURNING {_SELECT_COLS_SQL}
                """,
                region.region_id,
                self._tenant_id,
                region.region_label,
                region.summary,
                _serialize_jsonb_list(region.active_hypotheses),
                _serialize_jsonb_list(region.active_constraints),
                _serialize_jsonb_list(region.known_counterevidence),
                _serialize_jsonb_list(region.unresolved_unknowns),
                list(region.affected_goals),
                list(region.affected_commitments),
                list(region.member_model_ids),
                float(region.priority_score),
                float(region.prediction_error_score),
                _serialize_jsonb_list(region.next_best_frontiers),
                _serialize_jsonb_list(region.falsification_watch),
                region.last_refreshed_reason,
            )
            if row is None:
                raise RegionSummariesRepoError(
                    "upsert returned no row",
                    region_id=str(region.region_id),
                )
            return _hydrate_row(row)

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # get
    # =================================================================
    async def get(
        self,
        region_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RegionSufficientState | None:
        async def _run(c: asyncpg.Connection) -> RegionSufficientState | None:
            row = await c.fetchrow(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM region_sufficient_state
                WHERE region_id = $1 AND tenant_id = $2
                """,
                region_id,
                self._tenant_id,
            )
            if row is None:
                return None
            return _hydrate_row(row)

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # bulk_get
    # =================================================================
    async def bulk_get(
        self,
        region_ids: Sequence[UUID],
        *,
        conn: asyncpg.Connection | None = None,
    ) -> dict[UUID, RegionSufficientState]:
        """Bulk fetch keyed by region_id. Missing ids are simply absent
        from the result — callers can compute the diff themselves."""
        if not region_ids:
            return {}

        async def _run(c: asyncpg.Connection) -> dict[UUID, RegionSufficientState]:
            rows = await c.fetch(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM region_sufficient_state
                WHERE tenant_id = $1
                  AND region_id = ANY($2::uuid[])
                """,
                self._tenant_id,
                list(region_ids),
            )
            return {r["region_id"]: _hydrate_row(r) for r in rows}

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # find_by_member_model — GIN-backed reverse lookup
    # =================================================================
    async def find_by_member_model(
        self,
        model_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[RegionSufficientState]:
        """Return every region whose member_model_ids contains `model_id`.

        Uses the GIN index from migration 0053 — `@> ARRAY[$1]::uuid[]`
        is the indexable operator (array-contains). This is the hot
        path for the validated_model_update refresh trigger: when a
        Model commits, the refresher walks every region that includes
        it and decides whether to bump.
        """
        async def _run(c: asyncpg.Connection) -> list[RegionSufficientState]:
            rows = await c.fetch(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM region_sufficient_state
                WHERE tenant_id = $1
                  AND member_model_ids @> ARRAY[$2]::uuid[]
                ORDER BY priority_score DESC, region_id
                """,
                self._tenant_id,
                model_id,
            )
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # top_by_priority / top_by_prediction_error — leaderboards
    # =================================================================
    async def top_by_priority(
        self,
        *,
        limit: int = 20,
        conn: asyncpg.Connection | None = None,
    ) -> list[RegionSufficientState]:
        return await self._top_by(
            "priority_score",
            limit=limit,
            conn=conn,
        )

    async def top_by_prediction_error(
        self,
        *,
        limit: int = 20,
        conn: asyncpg.Connection | None = None,
    ) -> list[RegionSufficientState]:
        return await self._top_by(
            "prediction_error_score",
            limit=limit,
            conn=conn,
        )

    async def _top_by(
        self,
        column: str,
        *,
        limit: int,
        conn: asyncpg.Connection | None,
    ) -> list[RegionSufficientState]:
        # `column` is selected from a closed set (priority_score /
        # prediction_error_score) by the public callers above, so the
        # f-string interpolation is safe — no untrusted input reaches
        # the SQL string.
        async def _run(c: asyncpg.Connection) -> list[RegionSufficientState]:
            rows = await c.fetch(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM region_sufficient_state
                WHERE tenant_id = $1
                ORDER BY {column} DESC, region_id
                LIMIT $2
                """,
                self._tenant_id,
                int(limit),
            )
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)


__all__ = [
    "RegionSummariesRepo",
    "RegionSummariesRepoError",
]
