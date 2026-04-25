"""simulation/inspect.py — peek at the current tenant's substrate.

Prints a compact summary of what Think made of the authored narrative:
- observations count (synthetic vs real)
- Models count + the most recent N (with natural-language + confidence)
- Acts count (goals, commitments, decisions if tables are populated)
- Resources count with health breakdown (if resources table exists)

Usage:
    python simulation/inspect.py
    python simulation/inspect.py --tenant <uuid>
    python simulation/inspect.py --run-id sim-xyz --json

No writes. Safe to run at any point.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Optional
from uuid import UUID

import asyncpg

# Env guard.
import services.synthetic  # noqa: F401
from services.gateway.db_bootstrap import _register_codecs
from simulation.workers._common import _resolve_tenant_id


async def _has_table(conn: asyncpg.Connection, name: str) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT 1 FROM pg_tables WHERE tablename = $1 LIMIT 1", name
        )
    )


async def _summarize(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    run_id: Optional[str],
    scenario_id: Optional[str],
    top_n: int,
) -> dict[str, Any]:
    filters = ["tenant_id = $1"]
    args: list = [tenant_id]

    synth_filters = filters + ["content->>'synthetic' = 'true'"]
    if run_id:
        synth_filters.append(f"content->>'run_id' = ${len(args)+1}")
        args.append(run_id)
    if scenario_id:
        synth_filters.append(f"content->>'scenario_id' = ${len(args)+1}")
        args.append(scenario_id)
    where_synth = " AND ".join(synth_filters)

    # Observations
    total_obs = await conn.fetchval(
        "SELECT COUNT(*) FROM observations WHERE tenant_id = $1", tenant_id
    )
    synth_obs = await conn.fetchval(
        f"SELECT COUNT(*) FROM observations WHERE {where_synth}", *args
    )
    channels = await conn.fetch(
        f"""
        SELECT source_channel, COUNT(*) AS n
        FROM observations
        WHERE {where_synth}
        GROUP BY source_channel
        ORDER BY n DESC
        """,
        *args,
    )

    summary: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "filter": {"run_id": run_id, "scenario_id": scenario_id},
        "observations": {
            "total": total_obs or 0,
            "synthetic": synth_obs or 0,
            "by_channel": [
                {"source_channel": r["source_channel"], "count": r["n"]}
                for r in channels
            ],
        },
    }

    # Models
    if await _has_table(conn, "models"):
        models_total = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1", tenant_id
        )
        models_from_synth = await conn.fetchval(
            f"""
            SELECT COUNT(*) FROM models m
            WHERE EXISTS (
              SELECT 1 FROM observations o
              WHERE o.id = m.born_from_event_id
                AND {where_synth.replace('tenant_id', 'o.tenant_id')}
            )
            """,
            *args,
        )
        recent = await conn.fetch(
            f"""
            SELECT m.id, m."natural", m.confidence, m.created_at
            FROM models m
            WHERE EXISTS (
              SELECT 1 FROM observations o
              WHERE o.id = m.born_from_event_id
                AND {where_synth.replace('tenant_id', 'o.tenant_id')}
            )
            ORDER BY m.created_at DESC
            LIMIT {int(top_n)}
            """,
            *args,
        )
        summary["models"] = {
            "total": models_total or 0,
            "from_synthetic": models_from_synth or 0,
            "recent": [
                {
                    "id": str(r["id"]),
                    "natural": r["natural"],
                    "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in recent
            ],
        }
    else:
        summary["models"] = {"total": 0, "from_synthetic": 0, "recent": []}

    # Acts / Goals / Commitments / Decisions / Resources (if migrated)
    for tbl in ("goals", "commitments", "decisions", "resources"):
        if await _has_table(conn, tbl):
            n = await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} WHERE tenant_id = $1", tenant_id
            )
            summary[tbl] = {"total": n or 0}
        else:
            summary[tbl] = {"total": 0, "note": "table not migrated"}

    return summary


def _format_text(s: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"tenant_id : {s['tenant_id']}")
    f = s["filter"]
    lines.append(
        f"filter    : run_id={f['run_id']!r} scenario_id={f['scenario_id']!r}"
    )
    obs = s["observations"]
    lines.append(
        f"observations: {obs['total']} total ({obs['synthetic']} synthetic)"
    )
    for row in obs["by_channel"][:10]:
        lines.append(f"  {row['source_channel']:32s} {row['count']:>4d}")
    m = s["models"]
    lines.append(
        f"models      : {m['total']} total ({m['from_synthetic']} from synthetic)"
    )
    for row in m["recent"]:
        conf = f"{row['confidence']:.2f}" if row["confidence"] is not None else "—"
        lines.append(f"  [{conf}] {row['natural']}")
    for t in ("goals", "commitments", "decisions", "resources"):
        lines.append(f"{t:12s}: {s[t]['total']} total")
    return "\n".join(lines)


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
            s = await _summarize(
                conn, tenant_id, args.run_id, args.scenario_id, args.top
            )
        if args.json:
            print(json.dumps(s, indent=2, default=str))
        else:
            print(_format_text(s))
        return 0
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect tenant substrate state.")
    parser.add_argument("--tenant", dest="tenant_id", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--scenario-id", default=None)
    parser.add_argument("--top", type=int, default=15, help="Recent models to show.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
