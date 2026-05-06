"""End-to-end synthesis harness — black-box tests for the memory layer.

Usage:
    python -m tests.synthesis_harness                     # run all stages
    python -m tests.synthesis_harness retrieval scope     # run subset
    HARNESS_SKIP_LLM=1 python -m tests.synthesis_harness  # skip LLM cases
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import time

import asyncpg
from dotenv import load_dotenv

# Make the repo root importable when running as a script.
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

# Load .env so DATABASE_URL / DEEPSEEK_API_KEY / LLM_PROVIDER are present.
load_dotenv(REPO_ROOT / ".env")

# Migrations are idempotent; we run them once at startup so the harness
# is self-bootstrapping in a clean DB.
MIGRATIONS_DIR = REPO_ROOT / "db" / "migrations"


async def _ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            try:
                await conn.execute(path.read_text())
            except Exception as exc:  # noqa: BLE001
                print(f"  schema warn ({path.name}): {exc}", file=sys.stderr)


async def main(stages_filter: list[str] | None = None) -> int:
    from tests.synthesis_harness._runner import render_report, run_cases
    from tests.synthesis_harness import cases_cascade  # noqa: WPS433
    from tests.synthesis_harness import cases_contest
    from tests.synthesis_harness import cases_falsifier
    from tests.synthesis_harness import cases_reconcile
    from tests.synthesis_harness import cases_retrieval
    from tests.synthesis_harness import cases_scope

    all_cases = (
        cases_retrieval.CASES
        + cases_scope.CASES
        + cases_contest.CASES
        + cases_falsifier.CASES
        + cases_cascade.CASES
        + cases_reconcile.CASES
    )
    if stages_filter:
        all_cases = [c for c in all_cases if c.stage in stages_filter]
        print(f"Filter: {stages_filter} → {len(all_cases)} cases")

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        return 2

    # Register pgvector codec on every pooled connection. Production code
    # paths (services/models/repo.py:189) call register_vector lazily on
    # the connections they touch, which mutates the connection-level codec
    # map. Without doing the same in setup, fixture writes that pass
    # vectors as `'[…]'::vector` SQL casts collide with retrieval reads
    # that expect a list/bytes. Setting it once on init eliminates that
    # race entirely — every connection in the pool always expects a list.
    from pgvector.asyncpg import register_vector
    from services.models.repo import _VECTOR_REGISTERED_IDS

    async def _init_conn(conn: asyncpg.Connection) -> None:
        await register_vector(conn)
        # Production code uses an id(conn)-keyed set to remember which
        # connections have the codec; pathway B branches on this. Without
        # the entry, retrieval thinks the codec is missing and reformats
        # the vector as a string — which then fails because the codec
        # *was* registered. Mirror what services/models/repo does.
        _VECTOR_REGISTERED_IDS.add(id(conn))
        inner = getattr(conn, "_con", None)
        if inner is not None:
            _VECTOR_REGISTERED_IDS.add(id(inner))

    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20, init=_init_conn)
    try:
        await _ensure_schema(pool)
        # Stage names → concurrency. LLM-using cases get lower concurrency
        # so we don't hammer the provider rate limit.
        concurrency = 8
        if any(c.stage == "reconciliation" for c in all_cases) and not os.environ.get("HARNESS_SKIP_LLM"):
            concurrency = 4
        t0 = time.monotonic()
        results = await run_cases(pool, all_cases, concurrency=concurrency)
        elapsed = time.monotonic() - t0
        report = render_report(results)
        print(report)
        print(f"\nTotal wall time: {elapsed:.1f}s | Concurrency: {concurrency}")

        # Write JSON results next to the harness for diffing across runs.
        outpath = pathlib.Path(__file__).parent / "_last_run.json"
        outpath.write_text(json.dumps(
            [{
                "stage": r.stage, "name": r.name, "intent": r.intent,
                "passed": r.passed, "elapsed_ms": r.elapsed_ms,
                "diff": r.diff, "error": r.error,
                "actual": r.actual, "expected": r.expected,
            } for r in results],
            indent=2,
            default=str,
        ))
        print(f"JSON: {outpath.relative_to(REPO_ROOT)}")
        failed = sum(1 for r in results if not r.passed)
        return 0 if failed == 0 else 1
    finally:
        await pool.close()


if __name__ == "__main__":
    stages = sys.argv[1:] if len(sys.argv) > 1 else None
    rc = asyncio.run(main(stages))
    sys.exit(rc)
