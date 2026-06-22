#!/usr/bin/env python3
"""Audit the live Postgres schema for database surface area debt.

This is intentionally diagnostic: it does not mutate the database. It combines
catalog facts with a light repository scan so cleanup candidates are grounded in
both schema shape and code use.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import re
from dataclasses import dataclass
from typing import Iterable

import asyncpg

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is present in normal dev envs.
    load_dotenv = None  # type: ignore[assignment]


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SCAN_ROOTS = ("services", "lib", "scripts")
SCAN_SUFFIXES = {".py", ".sh", ".sql"}
SKIP_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "tests",
}


@dataclass(frozen=True)
class TableRefCount:
    table: str
    references: int


def _database_url(cli_dsn: str | None) -> str:
    if load_dotenv is not None:
        load_dotenv(REPO_ROOT / ".env")
    dsn = cli_dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; pass --dsn or load .env")
    return dsn


def _iter_scan_files() -> Iterable[pathlib.Path]:
    for root_name in SCAN_ROOTS:
        root = REPO_ROOT / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in SCAN_SUFFIXES:
                continue
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            yield path


def _repo_text() -> str:
    chunks: list[str] = []
    for path in _iter_scan_files():
        try:
            chunks.append(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            continue
    return "\n".join(chunks)


def _count_table_refs(table_names: Iterable[str]) -> list[TableRefCount]:
    text = _repo_text()
    counts: list[TableRefCount] = []
    for table in table_names:
        refs = len(re.findall(rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])", text))
        counts.append(TableRefCount(table=table, references=refs))
    return sorted(counts, key=lambda item: (item.references, item.table))


async def _base_tables(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
        ORDER BY c.relname
        """
    )
    return [row["relname"] for row in rows]


async def _print_catalog_summary(conn: asyncpg.Connection) -> None:
    row = await conn.fetchrow(
        """
        SELECT
          count(*) FILTER (WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition)::int AS base_tables,
          count(*) FILTER (WHERE c.relkind = 'p' AND NOT c.relispartition)::int AS partitioned_tables,
          count(*) FILTER (WHERE c.relispartition)::int AS partitions,
          (SELECT count(*)::int FROM pg_index) AS indexes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
        """
    )
    print("Catalog")
    print(f"  base_tables:        {row['base_tables']}")
    print(f"  partitioned_tables: {row['partitioned_tables']}")
    print(f"  partitions:         {row['partitions']}")
    print(f"  indexes:            {row['indexes']}")


async def _print_partition_summary(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT parent.relname AS parent, count(*)::int AS partitions
        FROM pg_inherits inh
        JOIN pg_class child ON child.oid = inh.inhrelid
        JOIN pg_class parent ON parent.oid = inh.inhparent
        JOIN pg_namespace n ON n.oid = parent.relnamespace
        WHERE n.nspname = 'public'
          AND parent.relkind IN ('r', 'p')
          AND child.relkind IN ('r', 'p')
        GROUP BY parent.relname
        ORDER BY partitions DESC, parent.relname
        """
    )
    print("\nPartitions")
    for row in rows:
        print(f"  {row['parent']}: {row['partitions']}")


async def _print_hnsw_indexes(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT
          tbl.relname AS table_name,
          idx.relname AS index_name,
          pg_size_pretty(pg_relation_size(idx.oid)) AS size
        FROM pg_index ix
        JOIN pg_class idx ON idx.oid = ix.indexrelid
        JOIN pg_class tbl ON tbl.oid = ix.indrelid
        JOIN pg_am am ON am.oid = idx.relam
        JOIN pg_namespace n ON n.oid = tbl.relnamespace
        WHERE n.nspname = 'public'
          AND am.amname = 'hnsw'
        ORDER BY tbl.relname, idx.relname
        """
    )
    print("\nHNSW Indexes")
    if not rows:
        print("  none")
        return
    for row in rows:
        print(f"  {row['table_name']}.{row['index_name']} ({row['size']})")


async def _print_tenant_rls_gaps(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND a.attname = 'tenant_id'
          AND NOT a.attisdropped
          AND NOT c.relrowsecurity
        ORDER BY c.relname
        """
    )
    print("\nTenant Tables Without RLS")
    if not rows:
        print("  none")
        return
    for row in rows:
        print(f"  {row['relname']}")


async def _print_tenant_policy_drift(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT
          c.relname,
          c.relrowsecurity,
          c.relforcerowsecurity,
          p.polname,
          COALESCE(pg_get_expr(p.polqual, p.polrelid), '') AS using_expr,
          COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '') AS check_expr
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_policy p
          ON p.polrelid = c.oid
         AND p.polname = 'tenant_isolation'
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND a.attname = 'tenant_id'
          AND NOT a.attisdropped
          AND c.relname <> 'tenants'
        ORDER BY c.relname
        """
    )
    needle = "NULLIF(current_setting('app.current_tenant'::text, true)"
    drift = [
        row
        for row in rows
        if (
            not row["relrowsecurity"]
            or not row["relforcerowsecurity"]
            or row["polname"] != "tenant_isolation"
            or needle not in row["using_expr"]
            or needle not in row["check_expr"]
        )
    ]
    print("\nTenant Policy Drift")
    if not drift:
        print("  none")
        return
    for row in drift:
        print(f"  {row['relname']}")


async def _print_partition_window_gaps(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        WITH bounds AS (
          SELECT
            (date_trunc('month', CURRENT_DATE)::date - INTERVAL '36 months')::date AS keep_start,
            (date_trunc('month', CURRENT_DATE)::date + INTERVAL '7 months')::date AS keep_end
        ),
        parts AS (
          SELECT
            parent.relname AS parent_name,
            child.relname AS partition_name,
            to_date(substring(child.relname FROM '_(\\d{4}_\\d{2})$'), 'YYYY_MM') AS month_start
          FROM pg_inherits inh
          JOIN pg_class child ON child.oid = inh.inhrelid
          JOIN pg_class parent ON parent.oid = inh.inhparent
          JOIN pg_namespace n ON n.oid = parent.relnamespace
          WHERE n.nspname = 'public'
            AND parent.relname IN ('observations', 'resource_transactions')
            AND child.relkind = 'r'
            AND child.relname ~ '_(\\d{4}_\\d{2})$'
        )
        SELECT parent_name, count(*)::int AS partitions
        FROM parts, bounds
        WHERE month_start < keep_start OR month_start >= keep_end
        GROUP BY parent_name
        ORDER BY parent_name
        """
    )
    print("\nOut-Of-Window Monthly Partitions")
    if not rows:
        print("  none")
        return
    for row in rows:
        print(f"  {row['parent_name']}: {row['partitions']}")


async def _print_low_reference_tables(conn: asyncpg.Connection) -> None:
    tables = await _base_tables(conn)
    counts = _count_table_refs(tables)
    print("\nLowest Production Reference Counts")
    for item in counts[:30]:
        print(f"  {item.table}: {item.references}")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="Postgres DSN. Defaults to DATABASE_URL.")
    args = parser.parse_args()

    conn = await asyncpg.connect(_database_url(args.dsn))
    try:
        await _print_catalog_summary(conn)
        await _print_partition_summary(conn)
        await _print_partition_window_gaps(conn)
        await _print_hnsw_indexes(conn)
        await _print_tenant_rls_gaps(conn)
        await _print_tenant_policy_drift(conn)
        await _print_low_reference_tables(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
