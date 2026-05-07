"""End-to-end synthesis harness — black-box tests for the memory layer.

Usage:
    python -m tests.synthesis_harness                     # run all stages
    python -m tests.synthesis_harness retrieval scope     # run subset
    HARNESS_SKIP_LLM=1 python -m tests.synthesis_harness  # skip LLM cases
    python -m tests.synthesis_harness --calibration       # produce ECE table
    python -m tests.synthesis_harness --adversarial       # include adversarial suite
    python -m tests.synthesis_harness --adversarial-only  # adversarial only
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
    # T3: each migration runs inside its own transaction so a single
    # failure (typically "table already exists" against a long-lived
    # dev DB) doesn't poison the connection for the next migration.
    # `on_error="warn"` matches the harness's previous behavior of
    # logging and continuing — fresh-DB bootstrap should use the
    # default `on_error="stop"` instead.
    from lib.shared.migrations import apply_migrations_dir
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, MIGRATIONS_DIR, on_error="warn")


async def main(
    stages_filter: list[str] | None = None,
    *,
    do_calibration: bool = False,
    include_adversarial: bool = False,
    adversarial_only: bool = False,
) -> int:
    from tests.synthesis_harness._runner import render_report, run_cases
    from tests.synthesis_harness import cases_cascade  # noqa: WPS433
    from tests.synthesis_harness import cases_contest
    from tests.synthesis_harness import cases_falsifier
    from tests.synthesis_harness import cases_reconcile
    from tests.synthesis_harness import cases_reconciliation
    from tests.synthesis_harness import cases_retrieval
    from tests.synthesis_harness import cases_scope

    base_cases = (
        cases_retrieval.CASES
        + cases_scope.CASES
        + cases_contest.CASES
        + cases_falsifier.CASES
        + cases_cascade.CASES
        + cases_reconcile.CASES
        + cases_reconciliation.CASES
    )

    adversarial_cases = []
    if include_adversarial or adversarial_only:
        from tests.synthesis_harness.adversarial import (
            cases_boundary,
            cases_cascade_pressure,
            cases_falsifier_adversarial,
            cases_linguistic,
            cases_multitenant,
            cases_reconciliation_pressure,
            cases_sequencing,
            concurrency_harness,
            failure_injection_harness,
            slow_burn_harness,
        )
        adversarial_cases = (
            cases_linguistic.CASES
            + cases_boundary.CASES
            + cases_sequencing.CASES
            + cases_reconciliation_pressure.CASES
            + cases_falsifier_adversarial.CASES
            + cases_cascade_pressure.CASES
            + concurrency_harness.CASES
            + failure_injection_harness.CASES
            + cases_multitenant.CASES
            + slow_burn_harness.CASES
        )

    if adversarial_only:
        all_cases = adversarial_cases
    else:
        all_cases = base_cases + adversarial_cases
    if stages_filter:
        all_cases = [c for c in all_cases if c.stage in stages_filter]
        print(f"Filter: {stages_filter} → {len(all_cases)} cases")

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        return 2

    # Pool init callback registers pgvector codec on every connection
    # the pool ever produces. See services/models/PGVECTOR_REGISTRY.md
    # for the contract. Any new pool that reads via Pathway B (the
    # gateway, the Think worker, this harness) must do this.
    from services.models.repo import pgvector_pool_init

    pool = await asyncpg.create_pool(
        dsn, min_size=2, max_size=20, init=pgvector_pool_init,
    )
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

        # T4: optional calibration report.
        rc = 0 if all(r.passed for r in results) else 1
        if do_calibration:
            from tests.synthesis_harness.calibration import (
                compute_calibration,
                diff_against_baseline,
                render_calibration_table,
                save_run_artifact,
            )
            cal_report = compute_calibration(results)
            print()
            print(render_calibration_table(cal_report))

            runs_dir = pathlib.Path(__file__).parent / "runs"
            artifact = save_run_artifact(cal_report, runs_dir)
            print(f"\nCalibration artifact: {artifact.relative_to(REPO_ROOT)}")

            baseline_path = (
                pathlib.Path(__file__).parent
                / "baselines"
                / "calibration.json"
            )
            regressed, msg = diff_against_baseline(cal_report, baseline_path)
            print(f"Baseline check: {msg}")
            if regressed:
                print(
                    "REGRESSION: ECE rose by more than "
                    f"{0.05:.2f} since baseline.",
                    file=sys.stderr,
                )
                rc = max(rc, 1)
        return rc
    finally:
        await pool.close()


if __name__ == "__main__":
    raw = sys.argv[1:]
    do_calibration = "--calibration" in raw
    include_adversarial = "--adversarial" in raw
    adversarial_only = "--adversarial-only" in raw
    stages = [a for a in raw if not a.startswith("--")] or None
    rc = asyncio.run(main(
        stages,
        do_calibration=do_calibration,
        include_adversarial=include_adversarial,
        adversarial_only=adversarial_only,
    ))
    sys.exit(rc)
