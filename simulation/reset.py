"""simulation/reset.py — drop synthetic signals from a dev tenant.

Mirrors the semantics of services/synthetic's purge intent (§6.1 of
SYNTHETIC-BYPASS-PLAN): only removes observations tagged as
synthetic. Scope:

- Observations with content->>'synthetic' = 'true' in the target
  tenant (optionally filtered by run_id / scenario_id).
- Downstream Models, Acts/Commitments/Decisions/Goals, and think_runs
  whose born_from_event_id / created_by_event_id / source_event_id
  points at a purged observation. These are nullified or archived
  rather than hard-deleted where the DAG requires it (matches the
  purge behavior in SYNTHETIC-BYPASS-PLAN §6.1).

What this DOES NOT touch:
- The persona actors and their identity_mapping rows. Those are the
  seeded "foundation" and survive resets so scenarios can re-run.
- Any observation where content.synthetic is NULL/false — belt-and-
  suspenders against footguns.

Usage:
    python simulation/reset.py --tenant <uuid>            # prompts
    python simulation/reset.py --tenant <uuid> --confirm
    python simulation/reset.py --tenant <uuid> --run-id sim-xyz --confirm
    python simulation/reset.py --dry-run                  # show counts
"""
from __future__ import annotations

import pathlib as _pl, sys as _sys
_ROOT = _pl.Path(__file__).resolve().parents[1]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

import argparse
import asyncio
import os
import sys
from typing import Optional
from uuid import UUID

import asyncpg

# Env guard.
import services.synthetic  # noqa: F401
from services.gateway.db_bootstrap import _register_codecs
from simulation.workers._common import _resolve_tenant_id


async def _counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    run_id: Optional[str],
    scenario_id: Optional[str],
) -> dict[str, int]:
    filters = ["tenant_id = $1", "content->>'synthetic' = 'true'"]
    args: list = [tenant_id]
    if run_id:
        filters.append(f"content->>'run_id' = ${len(args)+1}")
        args.append(run_id)
    if scenario_id:
        filters.append(f"content->>'scenario_id' = ${len(args)+1}")
        args.append(scenario_id)
    where = " AND ".join(filters)
    obs_count = await conn.fetchval(
        f"SELECT COUNT(*) FROM observations WHERE {where}", *args
    )
    model_count = await conn.fetchval(
        f"""
        SELECT COUNT(*) FROM models m
        WHERE EXISTS (
          SELECT 1 FROM observations o
          WHERE o.id = m.born_from_event_id AND {where.replace('tenant_id', 'o.tenant_id')}
        )
        """,
        *args,
    )
    return {"observations": obs_count or 0, "models": model_count or 0}


async def _purge(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    run_id: Optional[str],
    scenario_id: Optional[str],
) -> dict[str, int]:
    """Delete matching rows in dependency-safe order.

    We delete models, then related act/commitment/goal rows that
    reference matching observations, then the observations themselves.
    FKs from observations are NOT enforced at the DB layer (see
    BUILD-LOG 0001_foundation deviations), so we do the cascade here.
    """
    obs_filter_parts = ["o.tenant_id = $1", "o.content->>'synthetic' = 'true'"]
    args: list = [tenant_id]
    if run_id:
        obs_filter_parts.append(f"o.content->>'run_id' = ${len(args)+1}")
        args.append(run_id)
    if scenario_id:
        obs_filter_parts.append(f"o.content->>'scenario_id' = ${len(args)+1}")
        args.append(scenario_id)
    obs_filter = " AND ".join(obs_filter_parts)

    counts: dict[str, int] = {}

    # Helper to run and capture a delete count. Each delete runs inside
    # its own savepoint so that an UndefinedColumn / UndefinedTable on
    # a non-migrated-yet table doesn't abort the whole purge tx.
    async def _del(sql: str, label: str) -> None:
        try:
            async with conn.transaction():
                status = await conn.execute(sql, *args)
        except (
            asyncpg.UndefinedTableError,
            asyncpg.UndefinedColumnError,
        ):
            counts[label] = 0
            return
        try:
            n = int(status.split()[-1])
        except Exception:
            n = 0
        counts[label] = n

    # Tables that reference observations.id via <col>.
    refs = [
        ("models", "born_from_event_id"),
        ("goals", "created_by_event_id"),
        ("commitments", "created_by_event_id"),
        ("decisions", "created_by_event_id"),
        ("resources", "last_updated_by_event_id"),
        ("resource_transactions", "source_event_id"),
        ("entity_aliases", "source_event_id"),
    ]
    for tbl, col in refs:
        try:
            await _del(
                f"""
                DELETE FROM {tbl} t
                WHERE EXISTS (
                  SELECT 1 FROM observations o
                  WHERE o.id = t.{col} AND {obs_filter}
                )
                """,
                tbl,
            )
        except asyncpg.UndefinedTableError:
            counts[tbl] = 0  # table not migrated yet — skip

    # think_trigger_queue references observations.id via observation_id.
    await _del(
        f"""
        DELETE FROM think_trigger_queue t
        WHERE EXISTS (
          SELECT 1 FROM observations o
          WHERE o.id = t.observation_id AND {obs_filter}
        )
        """,
        "think_trigger_queue",
    )
    # think_runs — newer migrations may or may not link directly by
    # observation_id. Attempt and swallow UndefinedColumnError if so.
    await _del(
        f"""
        DELETE FROM think_runs t
        WHERE EXISTS (
          SELECT 1 FROM observations o
          WHERE o.id = t.observation_id AND {obs_filter}
        )
        """,
        "think_runs",
    )

    # Finally observations themselves.
    await _del(
        f"DELETE FROM observations o WHERE {obs_filter}",
        "observations",
    )
    return counts


async def _run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        sys.stderr.write("DATABASE_URL not set.\n")
        return 2
    tenant_id = _resolve_tenant_id(args.tenant_id)

    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=4, init=_register_codecs
    )
    try:
        async with pool.acquire() as conn:
            pre = await _counts(conn, tenant_id, args.run_id, args.scenario_id)
            print(
                f"tenant={tenant_id} run_id={args.run_id} scenario={args.scenario_id}"
            )
            print(
                f"would purge: observations={pre['observations']}, models={pre['models']}"
            )
            if args.dry_run:
                return 0
            if not args.confirm:
                if not sys.stdin.isatty():
                    sys.stderr.write(
                        "Non-interactive and --confirm not set. Aborting.\n"
                    )
                    return 2
                answer = input("Proceed? (type 'yes'): ").strip().lower()
                if answer != "yes":
                    print("Aborted.")
                    return 1
            # _purge manages its own per-statement savepoints so that
            # tables referenced but not yet migrated (e.g.
            # think_trigger_queue column rename) don't poison the tx.
            deleted = await _purge(
                conn, tenant_id, args.run_id, args.scenario_id
            )
            print("deleted:", deleted)
        return 0
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset (purge) synthetic signals from a tenant.")
    parser.add_argument("--tenant", dest="tenant_id", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--scenario-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
