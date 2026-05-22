"""
services/workers/calibration_updater/worker.py — Wave 4-C entry point.

Weekly scheduled job. Single public entry: `run_once(pool, *, tenant_id=None,
embedder=None)`. The argument list mirrors the pattern used by the
Deadline resolver and Entity resolver workers.

Behaviour
---------
1. `_harvest_stats(conn, tenant_id)` — upserts one `calibration_stats`
   row per resolved prediction since the last harvest. Idempotent; a
   second call in the same minute is a no-op.
2. `_recompute_all_offsets(conn, tenant_id)` — iterates the distinct
   (tenant, actor, proposition_kind) tuples in `calibration_stats`,
   computes offsets via `compute.compute_offsets_for_tuple`, upserts
   into `calibration_offsets`. Deletes rows that no longer have any
   supporting offset (keeps the table from growing unbounded).
3. `_apply_offsets_to_active_models(conn, repo, tenant_id)` — for
   every active Model in scope, compute a freshly-calibrated
   confidence and bulk-update via `ModelsRepo.bulk_confidence_update`.
   Clipping to [0.05, 0.95] is handled by the repo.

All three steps run inside a single `conn.transaction()` — if any
step fails the whole run rolls back.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from services.workers.calibration_updater.compute import (
    COLD_START_BUCKET_HIGH,
    COLD_START_BUCKET_LOW,
    OffsetRow,
    Stat,
    compute_offsets_for_tuple,
)


# Spec §9: "resolved_at > now() - interval '180 days'".
HARVEST_LOOKBACK_DAYS = 180


@dataclass
class RunResult:
    """Bookkeeping returned by `run_once` for observability + tests."""
    tenant_id: UUID | None
    harvested_stats: int
    offsets_written: int
    models_recalibrated: int


# ---------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    models_repo=None,
) -> RunResult:
    """
    Execute one calibration update pass.

    Parameters
    ----------
    pool
        asyncpg pool.
    tenant_id
        Restrict the run to a single tenant. Pass None to process all
        tenants in a single transaction — useful for small deployments
        and for tests; production deployments will schedule one
        per-tenant run at a time.
    models_repo
        Optional pre-built `ModelsRepo`. Pass one when the caller
        already has a configured repo (e.g. with an embedder). Tests
        wire a pool-only repo when the re-calibration step doesn't
        need embeddings.
    """
    # Lazy import to avoid circular imports (ModelsRepo imports
    # calibration.apply_calibration, which now runs against the
    # calibration_offsets table this worker writes).
    if models_repo is None:
        from services.models.repo import ModelsRepo
        models_repo = ModelsRepo(pool)

    async with pool.acquire() as conn:
        async with conn.transaction():
            harvested = await _harvest_stats(conn, tenant_id=tenant_id)
            offsets_written = await _recompute_all_offsets(
                conn, tenant_id=tenant_id
            )
            recalibrated = await _apply_offsets_to_active_models(
                conn, models_repo=models_repo, tenant_id=tenant_id
            )

    return RunResult(
        tenant_id=tenant_id,
        harvested_stats=harvested,
        offsets_written=offsets_written,
        models_recalibrated=recalibrated,
    )


# ---------------------------------------------------------------------
# Step 1 — harvest
# ---------------------------------------------------------------------


async def _harvest_stats(
    conn: asyncpg.Connection, *, tenant_id: UUID | None
) -> int:
    """
    Upsert one `calibration_stats` row per resolved Model that doesn't
    already have a stat. A Model is "resolved" when `resolved_at IS NOT
    NULL AND resolution_outcome IS NOT NULL` (enforced by the CHECK in
    migration 0002).

    Returns the number of new rows inserted.
    """
    params: list = []
    filters = [
        "m.resolved_at IS NOT NULL",
        "m.resolution_outcome IS NOT NULL",
        "cardinality(m.scope_actors) > 0",
    ]
    if tenant_id is not None:
        params.append(tenant_id)
        filters.append(f"m.tenant_id = ${len(params)}")

    # NOT EXISTS clause avoids double-inserting the same stat across
    # weekly runs; the spec's calibration_stats schema is an
    # append-only log (no UNIQUE on source_model_id is required), so
    # we guard at the application layer.
    rows = await conn.fetch(
        f"""
        SELECT m.tenant_id, m.scope_actors[1] AS actor_id,
               m.proposition_kind, m.confidence_at_assertion,
               m.resolution_outcome, m.resolved_at, m.id AS source_model_id
        FROM models m
        WHERE {' AND '.join(filters)}
          AND NOT EXISTS (
              SELECT 1 FROM calibration_stats cs
              WHERE cs.source_model_id = m.id
          )
        """,
        *params,
    )
    if not rows:
        return 0

    records = [
        (
            uuid7(),
            r["tenant_id"],
            r["actor_id"],
            r["proposition_kind"],
            float(r["confidence_at_assertion"]),
            r["resolution_outcome"],
            r["resolved_at"],
            r["source_model_id"],
        )
        for r in rows
    ]
    await conn.executemany(
        """
        INSERT INTO calibration_stats (
            id, tenant_id, actor_id, proposition_kind,
            asserted_confidence, outcome, resolved_at, source_model_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        records,
    )
    return len(records)


# ---------------------------------------------------------------------
# Step 2 — recompute offsets
# ---------------------------------------------------------------------


async def _recompute_all_offsets(
    conn: asyncpg.Connection, *, tenant_id: UUID | None
) -> int:
    """
    For every distinct (tenant, actor, proposition_kind) tuple in
    `calibration_stats`, compute offsets and upsert into
    `calibration_offsets`. Returns total rows written.
    """
    params: list = [HARVEST_LOOKBACK_DAYS]
    filters = [
        "outcome IS NOT NULL",
        f"resolved_at > now() - make_interval(days => $1)",
    ]
    if tenant_id is not None:
        params.append(tenant_id)
        filters.append(f"tenant_id = ${len(params)}")

    tuples = await conn.fetch(
        f"""
        SELECT DISTINCT tenant_id, actor_id, proposition_kind
        FROM calibration_stats
        WHERE {' AND '.join(filters)}
        """,
        *params,
    )

    total_written = 0
    for t in tuples:
        t_tenant = t["tenant_id"]
        actor_id = t["actor_id"]
        kind = t["proposition_kind"]
        # Re-query stats for this tuple.
        stat_rows = await conn.fetch(
            """
            SELECT asserted_confidence, outcome
            FROM calibration_stats
            WHERE tenant_id = $1 AND actor_id = $2
              AND proposition_kind = $3
              AND outcome IS NOT NULL
              AND resolved_at > now() - make_interval(days => $4)
            """,
            t_tenant, actor_id, kind, HARVEST_LOOKBACK_DAYS,
        )
        stats = [
            Stat(
                asserted_confidence=float(r["asserted_confidence"]),
                outcome=r["outcome"],
            )
            for r in stat_rows
        ]
        offsets = compute_offsets_for_tuple(stats, kind)
        await _upsert_offsets(
            conn,
            tenant_id=t_tenant,
            actor_id=actor_id,
            proposition_kind=kind,
            offsets=offsets,
        )
        total_written += len(offsets)

    return total_written


async def _upsert_offsets(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    proposition_kind: str,
    offsets: Iterable[OffsetRow],
) -> None:
    """
    Wipe-and-rewrite for this (tenant, actor, kind) triple, so stale
    buckets (e.g. a bucket that used to have samples but doesn't
    anymore) don't linger in the offsets table.
    """
    await conn.execute(
        """
        DELETE FROM calibration_offsets
        WHERE tenant_id = $1 AND actor_id = $2 AND proposition_kind = $3
        """,
        tenant_id, actor_id, proposition_kind,
    )
    rows = [
        (
            tenant_id,
            actor_id,
            proposition_kind,
            float(o.bucket_low),
            float(o.bucket_high),
            float(o.offset),
            int(o.sample_size),
        )
        for o in offsets
    ]
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO calibration_offsets (
            tenant_id, actor_id, proposition_kind,
            bucket_low, bucket_high, "offset", sample_size
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant_id, actor_id, proposition_kind, bucket_low)
        DO UPDATE SET
            bucket_high = EXCLUDED.bucket_high,
            "offset" = EXCLUDED."offset",
            sample_size = EXCLUDED.sample_size,
            last_updated = now()
        """,
        rows,
    )


# ---------------------------------------------------------------------
# Step 3 — apply offsets to active Models
# ---------------------------------------------------------------------


async def _apply_offsets_to_active_models(
    conn: asyncpg.Connection,
    *,
    models_repo,
    tenant_id: UUID | None,
) -> int:
    """
    For every active Model whose scope_actors[1] has a calibration
    offset row, compute new_confidence = clip(confidence_at_assertion
    * offset, 0.05, 0.95) and batch-update via
    `ModelsRepo.bulk_confidence_update`.

    This is the step that makes a fresh offset immediately visible in
    retrieval ranking — otherwise new offsets would only take effect on
    Models inserted after the run.

    Returns the number of Models whose confidence actually changed.
    """
    params: list = []
    filters = [
        "m.status = 'active'",
        "cardinality(m.scope_actors) > 0",
    ]
    if tenant_id is not None:
        params.append(tenant_id)
        filters.append(f"m.tenant_id = ${len(params)}")

    rows = await conn.fetch(
        f"""
        SELECT m.id, m.tenant_id, m.scope_actors[1] AS primary_actor,
               m.proposition_kind, m.confidence, m.confidence_at_assertion
        FROM models m
        WHERE {' AND '.join(filters)}
        """,
        *params,
    )

    updates: dict[UUID, float] = {}
    for r in rows:
        # Look up the offset for this (tenant, primary_actor, kind)
        # at confidence_at_assertion bucket.
        offset_row = await conn.fetchrow(
            """
            SELECT "offset"
            FROM calibration_offsets
            WHERE tenant_id = $1
              AND actor_id = $2
              AND proposition_kind = $3
              AND bucket_low <= $4
              AND bucket_high > $4
            LIMIT 1
            """,
            r["tenant_id"], r["primary_actor"], r["proposition_kind"],
            float(r["confidence_at_assertion"]),
        )
        if offset_row is None:
            continue
        offset = float(offset_row["offset"])
        new_conf = float(r["confidence_at_assertion"]) * offset
        # Only update when the delta is material (avoids a state_change
        # per Model every run when nothing's really moved). Threshold:
        # 0.001 absolute.
        if abs(new_conf - float(r["confidence"])) < 1e-3:
            continue
        updates[r["id"]] = new_conf

    if not updates:
        return 0
    # bulk_confidence_update re-enters the same connection via its
    # own conn parameter — passing `conn` so the update runs in the
    # current transaction.
    applied = await models_repo.bulk_confidence_update(updates, conn=conn)
    return len(applied)


__all__ = ["run_once", "RunResult"]
