#!/usr/bin/env python3
"""Run the sealed 3-world x 5-arm real-provider PostgreSQL P7 lane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.evaluation.epistemic_repair.p7_real_runner import run_p7_real_provider
from lib.llm.provider import build_provider


def _commit_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or "working-tree"


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    expected = {
        "LLM_PROVIDER": "codex",
        "CODEX_TRANSPORT": "cli",
        "CODEX_MODEL": "gpt-5.4",
    }
    mismatches = {
        key: os.environ.get(key) for key, value in expected.items()
        if os.environ.get(key) != value
    }
    if mismatches:
        raise SystemExit(f"P7 real lane requires exact provider env {expected}; got {mismatches}")
    provider = build_provider()
    conn = await asyncpg.connect(args.database_url)
    tx = conn.transaction()
    await tx.start()
    progress_path = args.progress_output or args.output.with_suffix(".progress.jsonl")
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    skip_completed: set[tuple[str, str, int]] = set()
    if args.resume_progress and progress_path.exists():
        for line in progress_path.read_text().splitlines():
            event = json.loads(line)
            if event.get("event") == "call_completed":
                skip_completed.add((
                    str(event["world_id"]),
                    str(event["arm_id"]),
                    int(event["stage_batch"]),
                ))
    else:
        progress_path.write_text("")

    def progress(event: dict[str, object]) -> None:
        line = json.dumps(event, sort_keys=True)
        with progress_path.open("a") as handle:
            handle.write(line + "\n")
        print(line, flush=True)

    try:
        artifact = await run_p7_real_provider(
            conn,
            provider=provider,
            commit_sha=_commit_sha(),
            transport="cli",
            progress=progress,
            skip_completed=skip_completed,
            parallel_arms=args.parallel_arms,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True)
            + "\n"
        )
        if args.commit_results:
            await tx.commit()
        else:
            await tx.rollback()
    except BaseException:
        await tx.rollback()
        raise
    finally:
        await conn.close()
    print(json.dumps({
        "phase_exit_ready": artifact.phase_exit_ready,
        "strategic_verdict": artifact.strategic_verdict,
        "failed_paired_units": len(artifact.failed_paired_units),
        "calls": len(artifact.call_receipts),
        "hard_gates": artifact.hard_gates,
        "output": str(args.output),
        "database_committed": args.commit_results,
    }))
    return 0 if artifact.phase_exit_ready else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--progress-output", type=Path)
    parser.add_argument("--resume-progress", action="store_true")
    parser.add_argument("--parallel-arms", type=int, default=5)
    parser.add_argument("--commit-results", action="store_true")
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
