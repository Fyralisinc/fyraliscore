"""services/observations/partitions.py — monthly partition management.

BUILD-PLAN.md §2 Prompt 1.A item 4:
    "Partitioning management: services/observations/partitions.py —
     monthly partition creator, runs at service startup and via cron,
     creates next-3-months partitions. Attaches partition to parent
     table atomically."

SCHEMA-LOCK.md "Partition creation" note (§ after S22):
    Current calendar month + next three calendar months are attached
    at migration time. A maintenance worker (Wave 4-D) extends the
    window. Partition creation must be idempotent via CREATE TABLE
    IF NOT EXISTS ... PARTITION OF ..., and must attach atomically.

Design:
- One module, no long-lived state. Each call opens a transaction,
  creates up to N monthly partitions starting from the current
  month, and commits. The IF NOT EXISTS clause makes every call
  idempotent — re-running this against an up-to-date DB is a no-op
  aside from the BEGIN/COMMIT round-trip.
- Creates partitions on the `observations` parent only. Wave 0 also
  partitioned `resource_transactions`, but that table belongs to
  Agent 2-C; partition upkeep for it is not in Wave 1-A scope.
- Attachment is atomic because CREATE TABLE ... PARTITION OF is a
  single DDL statement inside a transaction. There is no detach/
  attach dance — the partition springs into existence already
  attached.
"""
from __future__ import annotations

import asyncpg
from dataclasses import dataclass
from datetime import date, datetime, timezone


OBSERVATIONS_PARENT = "observations"
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
    """Canonical partition name: `<parent>_YYYY_MM`."""
    return f"{parent}_{month_start.strftime('%Y_%m')}"


@dataclass(frozen=True)
class PartitionSpec:
    parent: str
    month_start: date
    month_end: date  # exclusive upper bound (first of the next month)

    @property
    def name(self) -> str:
        return partition_name(self.parent, self.month_start)


def compute_partitions(
    as_of: date | None = None,
    *,
    parent: str = OBSERVATIONS_PARENT,
    months_ahead: int = DEFAULT_MONTHS_AHEAD,
) -> list[PartitionSpec]:
    """
    Return the list of PartitionSpec covering the current month and
    the next `months_ahead` months. Pure function; no DB side effects.
    """
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
    """
    Create a single monthly partition if it doesn't exist. Returns
    True if the partition was newly created, False if it already
    existed.

    `CREATE TABLE ... PARTITION OF` with `IF NOT EXISTS` is a single
    DDL statement — attachment is atomic under the surrounding
    transaction. No DETACH/ATTACH gymnastics required.
    """
    existed = await conn.fetchval(
        "SELECT to_regclass($1)", spec.name
    )
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{spec.name}" '
        f'PARTITION OF "{spec.parent}" '
        f"FOR VALUES FROM ('{spec.month_start.isoformat()}') "
        f"TO ('{spec.month_end.isoformat()}')"
    )
    await conn.execute(sql)
    return existed is None


async def ensure_partitions(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    as_of: date | None = None,
    parent: str = OBSERVATIONS_PARENT,
    months_ahead: int = DEFAULT_MONTHS_AHEAD,
) -> list[str]:
    """
    Ensure monthly partitions exist for the current month plus the
    next `months_ahead` months. Returns the list of partition names
    that were newly created (empty list if all already existed).

    Accepts either a pool (one connection is acquired, one transaction
    is opened) or an existing connection (caller owns the transaction).
    """
    specs = compute_partitions(as_of, parent=parent, months_ahead=months_ahead)
    created: list[str] = []

    if isinstance(pool_or_conn, asyncpg.Connection):
        # Caller owns the transaction; do not open our own.
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
    n: int,
    *,
    as_of: date | None = None,
    parent: str = OBSERVATIONS_PARENT,
) -> list[str]:
    """Convenience wrapper for the Wave-4-D maintenance worker."""
    return await ensure_partitions(
        pool, as_of=as_of, parent=parent, months_ahead=n
    )


async def list_existing_partitions(
    conn: asyncpg.Connection,
    *,
    parent: str = OBSERVATIONS_PARENT,
) -> list[str]:
    """
    Inspect the catalog and return the names of tables attached as
    partitions of `parent`. Ordered by name (which is
    lexicographically equivalent to chronological for our YYYY_MM
    scheme).
    """
    rows = await conn.fetch(
        """
        SELECT c.relname AS name
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE p.relname = $1
        ORDER BY c.relname
        """,
        parent,
    )
    return [r["name"] for r in rows]


__all__ = [
    "OBSERVATIONS_PARENT",
    "DEFAULT_MONTHS_AHEAD",
    "PartitionSpec",
    "compute_partitions",
    "partition_name",
    "ensure_partitions",
    "ensure_next_n_months",
    "list_existing_partitions",
]
