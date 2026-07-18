#!/usr/bin/env python3
"""Run P7's five isolated arms through production Think in 12 batches."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterator

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p6_think_runner import _write_checkpoint
from lib.evaluation.epistemic_repair.p7_production_runner import (
    run_p7_production_staged,
    run_p7_production_worlds,
    seal_execution_stream,
)
from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import (
    evaluate_frozen_worlds,
)
from lib.evaluation.epistemic_repair.p7_population import (
    P7_INITIAL_WORLD_COUNT,
    build_p7_population,
)
from lib.evaluation.epistemic_repair.p7_real_runner import _variant_population


ROOT = Path(__file__).resolve().parents[1]


def _clean_cli_provenance(repository: Path) -> dict[str, object]:
    repository = repository.resolve()
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repository, text=True,
    ).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=repository, text=True,
    ).strip()
    if dirty:
        raise SystemExit("P7 provider run requires an isolated clean worktree")
    if os.environ.get("CODEX_TRANSPORT") != "cli":
        raise SystemExit("P7 provider run requires CODEX_TRANSPORT=cli")
    return {
        "git_commit": commit, "worktree_clean": True,
        "worktree_path": str(repository), "codex_transport": "cli",
    }


@contextmanager
def _exclusive_run_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise SystemExit(f"P7 run lock already exists: {path}") from exc
    try:
        os.write(descriptor, f"pid={os.getpid()}\n".encode())
        os.close(descriptor)
        yield
    finally:
        path.unlink(missing_ok=True)


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    run_provenance = _clean_cli_provenance(args.repository)
    preregistered = build_p7_population()
    variants = tuple(
        (world.world_id, _variant_population(world, index))
        for index, world in enumerate(preregistered.worlds)
    )
    streams = tuple(
        (world_id, seal_execution_stream(population))
        for world_id, population in variants
    )
    initial_streams = streams[:P7_INITIAL_WORLD_COUNT]
    del variants, preregistered
    artifact = await run_p7_production_worlds(
        database_url=args.database_url,
        worlds=initial_streams,
        per_batch_timeout_s=args.batch_timeout,
    )
    artifact["run_provenance"] = run_provenance
    _write_checkpoint(args.output, artifact)
    executed = P7_INITIAL_WORLD_COUNT
    while True:
        preregistered = build_p7_population()
        variants = tuple(
            (world.world_id, _variant_population(world, index))
            for index, world in enumerate(preregistered.worlds)
        )
        sealed = dict(variants[:executed])
        scores = evaluate_frozen_worlds(
            execution_artifact=artifact, sealed_worlds=sealed,
        )
        _write_checkpoint(args.score_output, scores)
        should_continue = scores["stopping_rule"]["continue"] and executed < len(streams)
        del variants, preregistered, sealed
        if not should_continue:
            break
        world_id, stream = streams[executed]
        next_world = await run_p7_production_staged(
            database_url=args.database_url,
            population=stream,
            per_batch_timeout_s=args.batch_timeout,
            world_id=world_id,
        )
        world_results = [*artifact["world_results"], next_world]
        tenant_ids = [
            arm["tenant_id"] for world in world_results for arm in world["arm_results"]
        ]
        artifact = {
            **artifact,
            "world_count": len(world_results),
            "arm_execution_count": len(tenant_ids),
            "isolated_tenant_count": len(set(tenant_ids)),
            "world_results": world_results,
            "complete": (
                len(tenant_ids) == len(set(tenant_ids))
                and all(world["complete"] for world in world_results)
            ),
        }
        executed += 1
        _write_checkpoint(args.output, artifact)
    print(
        f"complete={str(artifact['complete']).lower()} "
        f"worlds={artifact['world_count']} arms={artifact['arm_execution_count']} "
        f"output={args.output} scores={args.score_output}"
    )
    return 0 if artifact["complete"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("/tmp/p7-production.json"))
    parser.add_argument(
        "--score-output", type=Path, default=Path("/tmp/p7-production-scores.json")
    )
    parser.add_argument("--batch-timeout", type=float, default=180.0)
    parser.add_argument("--repository", type=Path, default=ROOT)
    parser.add_argument(
        "--lock-file", type=Path, default=Path("/tmp/fyralis-p7-production.lock"),
    )
    args = parser.parse_args()
    with _exclusive_run_lock(args.lock_file):
        return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
