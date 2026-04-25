"""services/resources/partitions.py — monthly partition management for
`resource_transactions`.

BUILD-PLAN.md §3 Prompt 2.C: "services/resources/partitions.py —
`ensure_partition_for(occurred_at)`, `ensure_next_n_months(n=3)`,
`list_existing_partitions()`. Idempotent via CREATE TABLE IF NOT
EXISTS ... PARTITION OF resource_transactions. Pattern mirrors Agent
1-A's `services/observations/partitions.py` — do NOT import it (that
file targets the `observations` parent)."

Rationale — why mirror rather than import:
- Agent 1-A's module hard-codes `OBSERVATIONS_PARENT`. A swap to a
  `parent` arg would work but bloats their public API mid-wave.
- Keeping a separate module here lets us vary defaults (e.g. number
  of months) without coordinating cross-agent.
- Both modules use the same deterministic naming scheme
  `<parent>_YYYY_MM` so ad-hoc catalog queries look consistent.

SCHEMA-LOCK.md S4.2 note: `resource_transactions` is PARTITIONED BY
`occurred_at`; PK is `(id, occurred_at)`. Wave 0 created the current
month + next 3 partitions. This module guarantees idempotent extension
when tests span multiple months.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import asyncpg


RESOURCE_TX_PARENT = "resource_transactions"
DEFAULT_MONTHS_AHEAD = 3  # current month + next 3 = 4 partitions total


# ---------------------------------------------------------------------
# Date helpers — calendar month boundaries.
# ---------------------------------------------------------------------

def _first_of_month(d: date) -> date:
    return d.replace(day=1)


def _next_month(d: date) -> date:
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1, day=1)
    return d.replace(month=d.month + 1, day=1)


def partition_name(parent: str, month_start: date) -> str:
    return f"{parent}_{month_start.strftime('%Y_%m')}"


@dataclass(frozen=True)
class PartitionSpec:
    parent: str
    month_start: date
    month_end: date  # exclusive

    @property
    def name(self) -> str:
        return partition_name(self.parent, self.month_start)


def compute_partitions(
    as_of: date | None = None,
    *,
    parent: str = RESOURCE_TX_PARENT,
    months_ahead: int = DEFAULT_MONTHS_AHEAD,
) -> list[PartitionSpec]:
    """Pure: current month + next `months_ahead` months."""
    if months_ahead < 0:
        raise ValueError("months_ahead must be >= 0")
    if as_of is None:
        as_of = datetime.now(timezone.utc).date()
    start = _first_of_month(as_of)
    specs: list[PartitionSpec] = []
    for _ in range(months_ahead + 1):
        end = _next_month(start)
        specs.append(PartitionSpec(parent=parent, month_start=start, month_end=end))
        start = end
    return specs


# ---------------------------------------------------------------------
# DDL execution — idempotent creation.
# ---------------------------------------------------------------------

async def _create_one(conn: asyncpg.Connection, spec: PartitionSpec) -> bool:
    """Create one partition; return True if newly created."""
    existed = await conn.fetchval("SELECT to_regclass($1)", spec.name)
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{spec.name}" '
        f'PARTITION OF "{spec.parent}" '
        f"FOR VALUES FROM ('{spec.month_start.isoformat()}') "
        f"TO ('{spec.month_end.isoformat()}')"
    )
    await conn.execute(sql)
    return existed is None


async def ensure_partition_for(
    occurred_at: datetime | date,
    *,
    pool_or_conn: asyncpg.Pool | asyncpg.Connection | None = None,
    parent: str = RESOURCE_TX_PARENT,
) -> str:
    """
    Ensure the single partition covering `occurred_at` exists.
    Returns the partition name. Accepts a pool or a connection; if
    None, imports `lib.shared.db.get_pool()` lazily.
    """
    if isinstance(occurred_at, datetime):
        d = occurred_at.astimezone(timezone.utc).date() if occurred_at.tzinfo else occurred_at.date()
    else:
        d = occurred_at
    month_start = _first_of_month(d)
    month_end = _next_month(month_start)
    spec = PartitionSpec(parent=parent, month_start=month_start, month_end=month_end)

    if pool_or_conn is None:
        from lib.shared.db import get_pool
        pool_or_conn = get_pool()

    if isinstance(pool_or_conn, asyncpg.Connection):
        await _create_one(pool_or_conn, spec)
        return spec.name
    async with pool_or_conn.acquire() as conn:
        await _create_one(conn, spec)
    return spec.name


async def ensure_partitions(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    as_of: date | None = None,
    parent: str = RESOURCE_TX_PARENT,
    months_ahead: int = DEFAULT_MONTHS_AHEAD,
) -> list[str]:
    """Ensure current month + N ahead exist. Returns newly-created."""
    specs = compute_partitions(as_of, parent=parent, months_ahead=months_ahead)
    created: list[str] = []

    if isinstance(pool_or_conn, asyncpg.Connection):
        for spec in specs:
            if await _create_one(pool_or_conn, spec):
                created.append(spec.name)
        return created

    async with pool_or_conn.acquire() as conn:
        async with conn.transaction():
            for spec in specs:
                if await _create_one(conn, spec):
                    created.append(spec.name)
    return created


async def ensure_next_n_months(
    pool: asyncpg.Pool,
    n: int = DEFAULT_MONTHS_AHEAD,
    *,
    as_of: date | None = None,
    parent: str = RESOURCE_TX_PARENT,
) -> list[str]:
    return await ensure_partitions(
        pool, as_of=as_of, parent=parent, months_ahead=n
    )


async def list_existing_partitions(
    conn_or_pool: asyncpg.Connection | asyncpg.Pool,
    *,
    parent: str = RESOURCE_TX_PARENT,
) -> list[str]:
    q = (
        """
        SELECT c.relname AS name
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE p.relname = $1
        ORDER BY c.relname
        """
    )
    if isinstance(conn_or_pool, asyncpg.Pool):
        async with conn_or_pool.acquire() as conn:
            rows = await conn.fetch(q, parent)
    else:
        rows = await conn_or_pool.fetch(q, parent)
    return [r["name"] for r in rows]


__all__ = [
    "RESOURCE_TX_PARENT",
    "DEFAULT_MONTHS_AHEAD",
    "PartitionSpec",
    "compute_partitions",
    "partition_name",
    "ensure_partition_for",
    "ensure_partitions",
    "ensure_next_n_months",
    "list_existing_partitions",
]
