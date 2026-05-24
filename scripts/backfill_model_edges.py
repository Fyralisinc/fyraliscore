#!/usr/bin/env python3
"""
backfill_model_edges.py — one-shot backfill from legacy array columns
into the unified model_edges table (S1 of self-organizing-substrate
plan, migration 0031).

Reads every active Model and inserts the corresponding edges:

  - For each `m.supporting_model_ids[i]`:
      INSERT (m.supporting_model_ids[i], m.id, 'supports')
    Direction: array element supports m.

  - For each `m.contributing_models[i]`:
      INSERT (m.contributing_models[i], m.id, 'contributes_to_resolution')
    Direction: array element contributes to m's prediction resolution.

  - For each pattern_candidates row with promoted_pattern_model_id set:
      For each constituent c in constituent_model_ids:
        INSERT (c, promoted_pattern_model_id, 'instance_of')
    Direction: constituent is an instance of the promoted pattern.

  - superseded_by: NOT backfilled. The pre-S1 schema records archives
    with reason='superseded' but never the replacement Model id —
    there is no clean way to recover the chain. New supersessions in
    Stage 1 onward populate the edge cleanly. Audit gaps are
    accepted as a known limitation in the plan.

Idempotent: every INSERT uses the (tenant, source, target, kind)
UNIQUE constraint with ON CONFLICT DO NOTHING (via EdgesRepo.link).
Safe to re-run; duplicates collapse silently.

Usage:
    python scripts/backfill_model_edges.py                    # all tenants
    python scripts/backfill_model_edges.py --tenant <UUID>    # one tenant
    python scripts/backfill_model_edges.py --dry-run          # report only

Reports:
    counts per kind
    elapsed time
    any errors per tenant

Exits 0 on success; 1 if any tenant errored.

This script is part of Stage 0.5 acceptance: after migration 0031
ships, run this once per tenant, then start the drift detector
(services/workers/edge_drift) and watch for 14 consecutive days of
zero drift before considering Stage 2.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections import defaultdict
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


async def _backfill_supports_for_tenant(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    dry_run: bool,
) -> int:
    """For each active Model in tenant, INSERT a `supports` edge for
    every entry in supporting_model_ids. Returns rows inserted (or
    would-have-been-inserted in dry-run).
    """
    rows = await conn.fetch(
        """
        SELECT id, supporting_model_ids
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND array_length(supporting_model_ids, 1) > 0
        """,
        tenant_id,
    )
    inserted = 0
    for row in rows:
        target = row["id"]
        for source in row["supporting_model_ids"]:
            if dry_run:
                inserted += 1
                continue
            # Idempotent INSERT via the unique constraint.
            res = await conn.execute(
                """
                INSERT INTO model_edges
                  (id, tenant_id, source_model_id, target_model_id,
                   edge_kind, status, detected_by, metadata)
                VALUES ($1, $2, $3, $4, 'supports', 'active',
                        'backfill', '{}'::jsonb)
                ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
                """,
                uuid7(), tenant_id, source, target,
            )
            # asyncpg returns "INSERT 0 N" — N is rows affected.
            if res.endswith(" 1"):
                inserted += 1
    return inserted


async def _backfill_contributes_for_tenant(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    dry_run: bool,
) -> int:
    """For each active Model in tenant, INSERT a
    `contributes_to_resolution` edge for every entry in
    contributing_models. Direction: contributor → predictor."""
    rows = await conn.fetch(
        """
        SELECT id, contributing_models
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND array_length(contributing_models, 1) > 0
        """,
        tenant_id,
    )
    inserted = 0
    for row in rows:
        target = row["id"]
        for source in row["contributing_models"]:
            if dry_run:
                inserted += 1
                continue
            res = await conn.execute(
                """
                INSERT INTO model_edges
                  (id, tenant_id, source_model_id, target_model_id,
                   edge_kind, status, detected_by, metadata)
                VALUES ($1, $2, $3, $4, 'contributes_to_resolution',
                        'active', 'backfill', '{}'::jsonb)
                ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
                """,
                uuid7(), tenant_id, source, target,
            )
            if res.endswith(" 1"):
                inserted += 1
    return inserted


async def _backfill_instance_of_for_tenant(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    dry_run: bool,
) -> int:
    """For each promoted pattern_candidates row, INSERT an
    `instance_of` edge from each constituent to the promoted pattern.
    Direction: constituent → pattern."""
    rows = await conn.fetch(
        """
        SELECT promoted_pattern_model_id, constituent_model_ids
        FROM pattern_candidates
        WHERE tenant_id = $1
          AND promoted_pattern_model_id IS NOT NULL
          AND promoted_at IS NOT NULL
        """,
        tenant_id,
    )
    inserted = 0
    for row in rows:
        pattern_id = row["promoted_pattern_model_id"]
        for constituent in row["constituent_model_ids"]:
            if dry_run:
                inserted += 1
                continue
            res = await conn.execute(
                """
                INSERT INTO model_edges
                  (id, tenant_id, source_model_id, target_model_id,
                   edge_kind, status, detected_by, metadata)
                VALUES ($1, $2, $3, $4, 'instance_of', 'active',
                        'backfill', '{}'::jsonb)
                ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
                """,
                uuid7(), tenant_id, constituent, pattern_id,
            )
            if res.endswith(" 1"):
                inserted += 1
    return inserted


async def _backfill_one_tenant(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    counts: dict[str, int] = defaultdict(int)
    err: str | None = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                counts["supports"] = await _backfill_supports_for_tenant(
                    conn, tenant_id, dry_run=dry_run
                )
                counts["contributes_to_resolution"] = (
                    await _backfill_contributes_for_tenant(
                        conn, tenant_id, dry_run=dry_run
                    )
                )
                counts["instance_of"] = await _backfill_instance_of_for_tenant(
                    conn, tenant_id, dry_run=dry_run
                )
                if dry_run:
                    # Roll back so dry-run truly changes nothing.
                    raise asyncpg.exceptions._base.InterfaceError(
                        "dry-run rollback"
                    )
            except asyncpg.exceptions._base.InterfaceError as e:
                if dry_run and "dry-run rollback" in str(e):
                    pass  # expected; transaction will rollback
                else:
                    err = str(e)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
    return {
        "tenant_id": str(tenant_id),
        "counts": dict(counts),
        "elapsed_s": round(time.monotonic() - started, 3),
        "error": err,
    }


async def _list_tenants(pool: asyncpg.Pool) -> list[UUID]:
    """Distinct tenant_ids that have at least one active Model."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT tenant_id FROM models WHERE status = 'active'"
        )
    return [r["tenant_id"] for r in rows]


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill model_edges from legacy array columns."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN (default: $DATABASE_URL)",
    )
    parser.add_argument(
        "--tenant",
        type=str,
        default=None,
        help="If set, backfill only this tenant_id; otherwise all tenants.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count edges that would be inserted; rollback at end.",
    )
    args = parser.parse_args()
    if not args.dsn:
        print("ERROR: --dsn or $DATABASE_URL required", file=sys.stderr)
        return 2

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    try:
        if args.tenant:
            tenants = [UUID(args.tenant)]
        else:
            tenants = await _list_tenants(pool)

        print(
            f"Backfilling model_edges for {len(tenants)} tenant(s) "
            f"(dry_run={args.dry_run})"
        )
        any_error = False
        total_counts: dict[str, int] = defaultdict(int)
        for tid in tenants:
            result = await _backfill_one_tenant(
                pool, tid, dry_run=args.dry_run
            )
            for k, v in result["counts"].items():
                total_counts[k] += v
            print(
                f"  tenant={result['tenant_id']} "
                f"counts={result['counts']} "
                f"elapsed={result['elapsed_s']}s "
                f"error={result['error']}"
            )
            if result["error"]:
                any_error = True
        print(f"\nTotals: {dict(total_counts)}")
        return 1 if any_error else 0
    finally:
        await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
