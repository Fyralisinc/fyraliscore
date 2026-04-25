"""services/workers/maintenance/monthly.py — Wave 4-D monthly
maintenance.

Jobs:

* ``vacuum_analyze_foundation`` — VACUUM ANALYZE on each foundation
  table. Partition parents are skipped; each child partition of
  observations / resource_transactions is visited individually (a
  VACUUM on the parent covers children in modern Postgres, but spec
  language says "except partitioned parents — vacuum each child"; we
  honor the spec).
* ``old_partition_migration_notes`` — log which partitions SHOULD
  migrate to warm storage (older than 90 days). Phase 5 actually moves
  them.
* ``activation_histogram_report`` — p10 / p50 / p90 activation per
  (tenant, proposition_kind) — log only.
* ``uncontested_high_confidence_report`` — Models with confidence > 0.85
  AND contested_count = 0 AND age > 90 days. Log ids for founder review.

Important: VACUUM cannot run inside a transaction and is one statement
per target. The worker therefore uses a dedicated connection with
autocommit (asyncpg executes `VACUUM` outside any transaction when we
don't open one).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import get_pool
from lib.shared.ids import uuid7


log = logging.getLogger(__name__)


# Tables to VACUUM ANALYZE. We explicitly list them rather than
# enumerating pg_class so a new table accidentally left out of SCHEMA-
# LOCK doesn't silently skip vacuum review. Partition children added
# dynamically.
FOUNDATION_NON_PARTITIONED_TABLES = [
    "actors",
    "actor_identity_mappings",
    "entity_aliases",
    "models",
    "goals",
    "commitments",
    "commitment_contributors",
    "decisions",
    "contributes_to",
    "depends_on",
    "constrained_by",
    "resources",
    "resource_deployments",
    "customer_commitments",
    "model_status_notes",
]


UNCONTESTED_CONFIDENCE_THRESHOLD = 0.85
UNCONTESTED_AGE_DAYS = 90
OLD_PARTITION_DAYS = 90


@dataclass
class MonthlyReport:
    run_id: UUID
    run_started_at: datetime
    vacuumed_tables: list[str] = field(default_factory=list)
    old_partitions: list[str] = field(default_factory=list)
    activation_histogram: dict[str, dict[str, float]] = field(
        default_factory=dict
    )
    uncontested_high_confidence_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------


async def _list_partition_children(
    conn: asyncpg.Connection,
    parent: str,
) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE p.relname = $1
        ORDER BY c.relname
        """,
        parent,
    )
    return [r["relname"] for r in rows]


async def vacuum_analyze_foundation(
    *, pool: asyncpg.Pool | None = None
) -> list[str]:
    """VACUUM ANALYZE each listed foundation table + every partition
    child of ``observations`` and ``resource_transactions``. Returns the
    list of tables actually vacuumed.

    VACUUM cannot run inside a transaction, so we acquire a dedicated
    connection with ``transaction()`` not used — each statement commits
    implicitly.
    """
    the_pool = pool or get_pool()
    vacuumed: list[str] = []
    async with the_pool.acquire() as conn:
        children = (
            await _list_partition_children(conn, "observations")
            + await _list_partition_children(conn, "resource_transactions")
        )
        targets = FOUNDATION_NON_PARTITIONED_TABLES + children
        for t in targets:
            try:
                # VACUUM can't be parameterized — safe because `targets`
                # comes from a known-fixed list + catalog lookup.
                await conn.execute(f'VACUUM ANALYZE "{t}"')
                vacuumed.append(t)
            except asyncpg.PostgresError as e:
                log.warning("vacuum failed on %s: %s", t, e)
    return vacuumed


async def old_partition_migration_notes(
    *,
    conn: asyncpg.Connection | None = None,
    old_days: int = OLD_PARTITION_DAYS,
) -> list[str]:
    """Log (by returning the list; scheduler logs elsewhere) partitions
    whose upper bound is older than `old_days`. Actual cold-storage
    migration is Phase 5 work — this is notes only.
    """
    runner: Any = conn if conn is not None else get_pool()
    children = []
    for parent in ("observations", "resource_transactions"):
        children += await runner.fetch(
            """
            SELECT c.relname,
                   pg_get_expr(c.relpartbound, c.oid) AS bounds
            FROM pg_inherits i
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE p.relname = $1
            ORDER BY c.relname
            """,
            parent,
        )
    # Heuristic: parse "FOR VALUES FROM ('YYYY-MM-DD') TO ('YYYY-MM-DD')"
    # and compare the upper bound to `now() - old_days`.
    import re
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(days=old_days)
    old: list[str] = []
    pat = re.compile(r"TO \('(\d{4}-\d{2}-\d{2})'")
    for r in children:
        m = pat.search(r["bounds"] or "")
        if not m:
            continue
        try:
            upper = datetime.strptime(m.group(1), "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue
        if upper < cutoff:
            old.append(r["relname"])
    return old


async def activation_histogram_report(
    *, conn: asyncpg.Connection | None = None
) -> dict[str, dict[str, float]]:
    """Return {"<tenant>:<proposition_kind>": {"p10", "p50", "p90"}}.
    """
    runner: Any = conn if conn is not None else get_pool()
    rows = await runner.fetch(
        """
        SELECT tenant_id,
               COALESCE(proposition_kind, 'unknown') AS kind,
               percentile_cont(0.10) WITHIN GROUP (
                 ORDER BY activation
               ) AS p10,
               percentile_cont(0.50) WITHIN GROUP (
                 ORDER BY activation
               ) AS p50,
               percentile_cont(0.90) WITHIN GROUP (
                 ORDER BY activation
               ) AS p90,
               COUNT(*) AS n
        FROM models
        WHERE status = 'active'
        GROUP BY tenant_id, COALESCE(proposition_kind, 'unknown')
        """
    )
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        key = f"{r['tenant_id']}:{r['kind']}"
        out[key] = {
            "p10": float(r["p10"] or 0.0),
            "p50": float(r["p50"] or 0.0),
            "p90": float(r["p90"] or 0.0),
            "n": int(r["n"] or 0),
        }
    return out


async def uncontested_high_confidence_report(
    *,
    conn: asyncpg.Connection | None = None,
    conf_threshold: float = UNCONTESTED_CONFIDENCE_THRESHOLD,
    age_days: int = UNCONTESTED_AGE_DAYS,
) -> list[str]:
    """Return ids of Models worth founder review (never contested,
    high confidence, old enough).
    """
    runner: Any = conn if conn is not None else get_pool()
    rows = await runner.fetch(
        """
        SELECT id FROM models
        WHERE status = 'active'
          AND confidence > $1
          AND contested_count = 0
          AND created_at < (now() - ($2 || ' days')::interval)
        ORDER BY created_at ASC
        LIMIT 500
        """,
        float(conf_threshold),
        str(int(age_days)),
    )
    return [str(r["id"]) for r in rows]


# ---------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------


async def run_monthly(
    *,
    pool: asyncpg.Pool | None = None,
) -> MonthlyReport:
    the_pool = pool or get_pool()
    report = MonthlyReport(
        run_id=uuid7(),
        run_started_at=datetime.now(timezone.utc),
    )
    try:
        report.vacuumed_tables = await vacuum_analyze_foundation(
            pool=the_pool
        )
    except Exception as e:
        report.errors.append(f"vacuum:{type(e).__name__}:{e}")
    try:
        async with the_pool.acquire() as conn:
            report.old_partitions = await old_partition_migration_notes(
                conn=conn
            )
    except Exception as e:
        report.errors.append(f"old_partitions:{type(e).__name__}:{e}")
    try:
        async with the_pool.acquire() as conn:
            report.activation_histogram = (
                await activation_histogram_report(conn=conn)
            )
    except Exception as e:
        report.errors.append(f"activation_histogram:{type(e).__name__}:{e}")
    try:
        async with the_pool.acquire() as conn:
            report.uncontested_high_confidence_ids = (
                await uncontested_high_confidence_report(conn=conn)
            )
    except Exception as e:
        report.errors.append(f"uncontested:{type(e).__name__}:{e}")
    log.info(
        "monthly maintenance complete",
        extra={
            "run_id": str(report.run_id),
            "vacuumed_n": len(report.vacuumed_tables),
            "old_partitions_n": len(report.old_partitions),
            "activation_histogram_keys": list(
                report.activation_histogram.keys()
            ),
            "uncontested_n": len(report.uncontested_high_confidence_ids),
            "errors": report.errors,
        },
    )
    return report


__all__ = [
    "MonthlyReport",
    "vacuum_analyze_foundation",
    "old_partition_migration_notes",
    "activation_histogram_report",
    "uncontested_high_confidence_report",
    "run_monthly",
    "FOUNDATION_NON_PARTITIONED_TABLES",
]
