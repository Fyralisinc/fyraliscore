#!/usr/bin/env python3
"""Validate database table contracts with a 5k-observation run.

This is a focused alternative to replaying a full large-company run. It reuses the
single-company synthetic probe's production ingestion path, but skips Think/LLM
drain and topology work. The goal is table-contract evidence:

* which tables are touched by 5k production-shaped observation inserts;
* which low-reference tables remain untouched by this validation lane;
* whether the current schema still has RLS/policy/partition/index drift.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import pathlib
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg
from dotenv import load_dotenv

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from lib.embeddings.ollama import EMBEDDING_DIM  # noqa: E402
from lib.shared.migrations import apply_migrations_dir  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.domain.actors.repo import ActorRepo  # noqa: E402
from services.domain.entity_aliases.repo import EntityAliasRepo  # noqa: E402

from scripts.run_1000_signal_model_layer_probe import (  # noqa: E402
    COMPANY_NAME,
    _insert_extra_aliases,
    build_scenario,
    inject_generated_signals,
)


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
class TableSnapshot:
    rows: int
    approx_live_rows: int
    total_bytes: int


class DeterministicEmbedder:
    class _Config:
        model = "deterministic-db-contract-validator"
        expected_dim = EMBEDDING_DIM

    config = _Config()

    async def embed(self, text: str) -> list[float]:
        seed = int.from_bytes(hashlib.sha256((text or "").encode()).digest()[:8], "big")
        rng = random.Random(seed)
        vec = [rng.gauss(0.0, 1.0) for _ in range(EMBEDDING_DIM)]
        norm = sum(value * value for value in vec) ** 0.5
        if norm == 0:
            return vec
        return [value / norm for value in vec]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [await self.embed(text) for text in texts]

    async def close(self) -> None:
        return None


def _database_url(cli_dsn: str | None) -> str:
    load_dotenv(REPO_ROOT / ".env", override=False)
    dsn = cli_dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; pass --dsn or load .env")
    return dsn


def _iter_scan_files() -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for root_name in SCAN_ROOTS:
        root = REPO_ROOT / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in SCAN_SUFFIXES:
                continue
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            files.append(path)
    return files


def _repo_text() -> str:
    chunks: list[str] = []
    for path in _iter_scan_files():
        try:
            chunks.append(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            continue
    return "\n".join(chunks)


def _count_refs(table_names: list[str]) -> dict[str, int]:
    text = _repo_text()
    return {
        table: len(
            re.findall(
                rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])",
                text,
            )
        )
        for table in table_names
    }


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


async def _snapshot_tables(conn: asyncpg.Connection) -> dict[str, TableSnapshot]:
    rows = await conn.fetch(
        """
        SELECT
          c.relname AS table_name,
          COALESCE(s.n_live_tup, 0)::bigint AS approx_live_rows,
          pg_total_relation_size(c.oid)::bigint AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
        ORDER BY c.relname
        """
    )
    snapshots: dict[str, TableSnapshot] = {}
    for row in rows:
        table = row["table_name"]
        count = await conn.fetchval(f'SELECT count(*)::bigint FROM "{table}"')
        snapshots[table] = TableSnapshot(
            rows=int(count or 0),
            approx_live_rows=int(row["approx_live_rows"] or 0),
            total_bytes=int(row["total_bytes"] or 0),
        )
    return snapshots


async def _catalog_health(conn: asyncpg.Connection) -> dict[str, Any]:
    catalog = await conn.fetchrow(
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
    out_of_window = await conn.fetch(
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
        SELECT parent_name, partition_name
        FROM parts, bounds
        WHERE month_start < keep_start OR month_start >= keep_end
        ORDER BY parent_name, partition_name
        """
    )
    rls_gaps = await conn.fetch(
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
    policy_drift = await conn.fetch(
        """
        SELECT c.relname
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
          AND (
            NOT c.relrowsecurity
            OR NOT c.relforcerowsecurity
            OR p.polname IS NULL
            OR COALESCE(pg_get_expr(p.polqual, p.polrelid), '')
               NOT LIKE '%NULLIF(current_setting(''app.current_tenant''%'
            OR COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
               NOT LIKE '%NULLIF(current_setting(''app.current_tenant''%'
          )
        ORDER BY c.relname
        """
    )
    hnsw = await conn.fetch(
        """
        SELECT tbl.relname AS table_name, idx.relname AS index_name
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
    return {
        "base_tables": catalog["base_tables"],
        "partitioned_tables": catalog["partitioned_tables"],
        "partitions": catalog["partitions"],
        "indexes": catalog["indexes"],
        "out_of_window_partitions": [dict(row) for row in out_of_window],
        "tenant_tables_without_rls": [row["relname"] for row in rls_gaps],
        "tenant_policy_drift": [row["relname"] for row in policy_drift],
        "hnsw_indexes": [dict(row) for row in hnsw],
    }


def _classify_table(table: str) -> str:
    if table.startswith("extension_"):
        return "extension-owned/control"
    if any(token in table for token in ("queue", "pending", "dead_letter", "lock")):
        return "queue/control"
    if any(token in table for token in ("audit", "trace", "log", "cost", "artifact")):
        return "trace/audit"
    if any(token in table for token in ("cache", "view_", "summary", "index")):
        return "derived/cache"
    if table.endswith("_state") or table.endswith("_runs") or table.endswith("_events"):
        return "trace/control"
    if table in {
        "tenants",
        "actors",
        "entity_aliases",
        "observations",
        "models",
        "model_edges",
        "goals",
        "commitments",
        "decisions",
        "resources",
        "resource_transactions",
    }:
        return "canonical"
    return "unclassified"


def _deltas(
    before: dict[str, TableSnapshot],
    after: dict[str, TableSnapshot],
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for table in sorted(set(before) | set(after)):
        b = before.get(table, TableSnapshot(0, 0, 0))
        a = after.get(table, TableSnapshot(0, 0, 0))
        result[table] = {
            "rows_before": b.rows,
            "rows_after": a.rows,
            "row_delta": a.rows - b.rows,
            "bytes_before": b.total_bytes,
            "bytes_after": a.total_bytes,
            "byte_delta": a.total_bytes - b.total_bytes,
        }
    return result


def _render_markdown(report: dict[str, Any]) -> str:
    active = [
        (table, data)
        for table, data in report["table_deltas"].items()
        if data["row_delta"] != 0
    ]
    active.sort(key=lambda item: (-abs(item[1]["row_delta"]), item[0]))
    low_refs = report["low_reference_tables"]
    lines = [
        "# Database Contract Validation - 5k Observations",
        "",
        f"- Run id: `{report['run_id']}`",
        f"- Tenant id: `{report['tenant_id']}`",
        f"- Signals requested: `{report['signals_requested']}`",
        f"- Observations inserted: `{report['observations_inserted']}`",
        f"- Elapsed seconds: `{report['elapsed_seconds']}`",
        "",
        "## Catalog Health",
        "",
        f"- Base tables: `{report['catalog_health']['base_tables']}`",
        f"- Partitions: `{report['catalog_health']['partitions']}`",
        f"- Indexes: `{report['catalog_health']['indexes']}`",
        f"- Out-of-window partitions: `{len(report['catalog_health']['out_of_window_partitions'])}`",
        f"- Tenant RLS gaps: `{len(report['catalog_health']['tenant_tables_without_rls'])}`",
        f"- Tenant policy drift: `{len(report['catalog_health']['tenant_policy_drift'])}`",
        "",
        "## Tables Touched By 5k Observation Ingest",
        "",
        "| Table | Row Delta | Contract Class | Prod Refs |",
        "|---|---:|---|---:|",
    ]
    for table, data in active:
        lines.append(
            f"| `{table}` | {data['row_delta']} | "
            f"{report['table_contracts'][table]} | "
            f"{report['production_references'][table]} |"
        )
    lines.extend(
        [
            "",
            "## Lowest-Reference Tables",
            "",
            "| Table | Prod Refs | Row Delta | Contract Class | Validation Read |",
            "|---|---:|---:|---|---|",
        ]
    )
    for item in low_refs:
        if item["row_delta"] != 0:
            read = "active in 5k ingest"
        elif item["contract_class"] in {"derived/cache", "trace/audit"}:
            read = "candidate for regeneration/archive proof"
        else:
            read = "not proven by observation-only lane"
        lines.append(
            f"| `{item['table']}` | {item['production_references']} | "
            f"{item['row_delta']} | {item['contract_class']} | {read} |"
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="Postgres DSN. Defaults to DATABASE_URL.")
    parser.add_argument("--signals", type=int, default=5000)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument(
        "--skip-migrations",
        action="store_true",
        help="Skip db/migrations replay when the target DB is already current.",
    )
    parser.add_argument(
        "--report-root",
        type=pathlib.Path,
        default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    if args.signals <= 0:
        raise SystemExit("--signals must be positive")
    dsn = _database_url(args.dsn)
    run_id = args.run_id or f"db-contract-5k-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    report_dir = args.report_root / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    started = time.monotonic()
    try:
        async with pool.acquire() as conn:
            if not args.skip_migrations:
                await apply_migrations_dir(
                    conn,
                    REPO_ROOT / "db" / "migrations",
                    on_error="warn",
                )
            before = await _snapshot_tables(conn)

        print(f"building {args.signals}-signal scenario for {COMPANY_NAME}", flush=True)
        scenario = build_scenario(args.signals, namespace=run_id)
        from tests.real_llm.infrastructure.scenario_loader import materialize

        await materialize(scenario, pool=pool)
        assert scenario.tenant_id is not None
        print(f"tenant={scenario.tenant_id} run_id={run_id}", flush=True)

        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        await _insert_extra_aliases(scenario, alias_repo)
        observation_ids = await inject_generated_signals(
            scenario,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=DeterministicEmbedder(),
            run_id=run_id,
            progress_every=args.progress_every,
        )

        async with pool.acquire() as conn:
            after = await _snapshot_tables(conn)
            catalog_health = await _catalog_health(conn)
            tables = await _base_tables(conn)

        refs = _count_refs(tables)
        deltas = _deltas(before, after)
        contracts = {table: _classify_table(table) for table in tables}
        low_refs = [
            {
                "table": table,
                "production_references": refs[table],
                "row_delta": deltas[table]["row_delta"],
                "contract_class": contracts[table],
            }
            for table in sorted(tables, key=lambda name: (refs[name], name))[:40]
        ]
        elapsed = round(time.monotonic() - started, 3)
        report = {
            "run_id": run_id,
            "tenant_id": str(scenario.tenant_id),
            "signals_requested": args.signals,
            "observations_inserted": len(observation_ids),
            "elapsed_seconds": elapsed,
            "catalog_health": catalog_health,
            "production_references": refs,
            "table_contracts": contracts,
            "table_deltas": deltas,
            "low_reference_tables": low_refs,
        }
        (report_dir / "database_contract_validation.json").write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str)
        )
        (report_dir / "database_contract_validation.md").write_text(
            _render_markdown(report)
        )
        print(f"report_dir={report_dir}", flush=True)
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "tenant_id": str(scenario.tenant_id),
                    "observations_inserted": len(observation_ids),
                    "elapsed_seconds": elapsed,
                    "touched_tables": sum(
                        1 for data in deltas.values() if data["row_delta"] != 0
                    ),
                    "catalog_health": catalog_health,
                },
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
